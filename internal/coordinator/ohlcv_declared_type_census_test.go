package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// EVERY BAR FIELD DECLARES THE SAME (TYPE, PRECISION, SCALE) ON EVERY ARM
// (#965 round 2, B1).
//
// The values were right on every arm from the first round; the DECLARATION was
// not, and nothing in the tree observed it at the END of a path. The value
// census renders through `na2Run`, which shows a DECIMAL as its own exact text
// and cannot see `(p,s)` at all, and `exec.TestTheBarsDeclaredFieldsFollowIts
// Inputs` calls `OhlcvOutputFields` directly — a pure unit table, no plan and
// no arm. So `(b).open` over a DECIMAL(18,4) price declared DECIMAL(18,4) in
// process and DECIMAL(0,4) on the DAG, `(b).volume` (38,2) against (0,2),
// `(b).vwap` (38,8) against (0,8) — and over an EMPTY input DECIMAL(18,4)
// against FLOAT64, which is OID 1700 against 701 in the RowDescription.
//
// Three mechanisms, one rule between them: a bar's ROW is declared ONCE, from
// the input columns' declared types, at PLAN time, and every consumer reads
// that declaration rather than deriving one of its own.
//
//   - `physical.aggOhlcvOutputFields` now types a COMPUTED price through
//     `aggComputedInputExprDecl`, so the plan declines no answerable bar. A
//     declaration the plan declines is one each consumer invents differently.
//   - `physical.inputColFields` publishes what an AGGREGATE declares, instead
//     of returning nil for the whole map at the first aggregate projection —
//     so `(b).open` resolves at plan time and the stage's ProjectExprSpec
//     carries the real `(p,s)`.
//   - the worker's projection carries that spec even on a pass-through, and
//     `exec.Project` fills the ONE thing a decoded batch cannot carry: a WSHF
//     chunk records a ROW child's SCALE and has no room for its PRECISION.
//   - `worker.applyOhlcvFold` takes the declaration from the spec and no
//     longer substitutes FLOAT64 for a column of empty states.
//
// Five arms, because a declaration can be lost in a place only one of them
// reaches: `TestNumericArc2`'s own set, and for its reason.
func TestTheBarsDeclaredTypeIsTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM, func(w *worker.Config) { w.MorselWorkers = 4 })

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return odcDecls(tmdRunSingleDeclAll(ctx, single, sql)) }},
		{"single+budget", func(sql string) ([]string, error) { return odcDecls(tmdRunSingleDeclAll(ctx, spilled, sql)) }},
		{"dag", func(sql string) ([]string, error) { return odcDecls(tmdRunDAGDeclAll(ctx, coord, sql)) }},
		{"dag+shuffled", func(sql string) ([]string, error) { return odcDecls(tmdRunDAGDeclAll(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return odcDecls(tmdRunDAGDeclAll(ctx, coordM, sql)) }},
	}

	for _, tc := range odcCells() {
		t.Run(tc.name, func(t *testing.T) {
			for _, shape := range []string{"ungrouped", "grouped"} {
				for _, empty := range []bool{false, true} {
					sql := tc.sql(shape, empty)
					label := shape + "/rows"
					if empty {
						label = shape + "/empty"
					}
					for _, arm := range arms {
						got, err := arm.run(sql)
						if err != nil {
							t.Errorf("%s arm (%s): %v\n  SQL: %s", arm.name, label, err, sql)
							continue
						}
						if strings.Join(got, " | ") != strings.Join(tc.want, " | ") {
							t.Errorf("%s arm (%s)\n  got  %s\n  want %s\n  PostgreSQL: %s\n  SQL: %s",
								arm.name, label, strings.Join(got, " | "), strings.Join(tc.want, " | "),
								tc.pgSays, sql)
						}
					}
				}
			}
		})
	}
}

type odcCell struct {
	name   string
	price  string
	volume string
	want   []string
	pgSays string
}

// odcFields is the SELECT list every shape publishes: all six bar fields, so
// no field goes unwatched.
const odcFields = `(b).open AS o, (b).high AS h, (b).low AS l, (b).close AS c,
	                 (b).volume AS v, (b).vwap AS w`

// sql builds one cell in one SHAPE. Both shapes matter and only one of them
// was ever gated: round 2's census was UNGROUPED in all eleven cells, and the
// GROUPED spelling — the bucket published beside the bar, which is what every
// README, release-note and sql-reference example writes — kept round 1's
// divergence on all three DAG arms (#965 round 3).
//
// The mechanism was one more site of round 2's own class: `inputColFields`
// returned nil for the WHOLE block at a COMPUTED projection, and the bucket is
// one, so `(b).open` reached the stage with no declared (p,s) and a ROW child
// that crossed a WSHF boundary — where a container carries its scale and has
// no room for its precision — declared DECIMAL(0,s). A gate whose corpus never
// groups cannot see that, which is why the shape is a dimension here now
// rather than a cell.
func (c odcCell) sql(shape string, empty bool) string {
	where := ""
	if empty {
		where = " WHERE id < 0"
	}
	switch shape {
	case "ungrouped":
		return fmt.Sprintf(`SELECT %s
		 FROM (SELECT ohlcv(ts, %s, %s) AS b FROM %s%s) t`,
			odcFields, c.price, c.volume, ohlcvTable, where)
	case "grouped":
		// The headline shape: a computed group key PUBLISHED beside the bar.
		return fmt.Sprintf(`SELECT %s
		 FROM (SELECT time_bucket(INTERVAL '1' MINUTE, ts) AS g, ohlcv(ts, %s, %s) AS b
		       FROM %s%s GROUP BY 1) t`,
			odcFields, c.price, c.volume, ohlcvTable, where)
	}
	panic("odcCell.sql: unknown shape " + shape)
}

// odcCells is every bar field's declaration over the price × volume matrix the
// brief names. The expectations are PostgreSQL's declarations for the
// aggregates each field IS, measured on 17.11 — with the two rules ADR-0024
// records as this engine's own: AVG's fixed +4 scale increment (the server's
// division scale is magnitude-dependent, which ADR-0024 item 2 rejects), and a
// DECIMAL SUM widening to the carrier's full 38 digits.
func odcCells() []odcCell {
	return []odcCell{
		{name: "int32_price_int32_volume", price: "px_i32", volume: "vol_i32",
			want:   []string{"o=INT32", "h=INT32", "l=INT32", "c=INT32", "v=INT64", "w=DECIMAL(38,4)"},
			pgSays: "min(int4)=integer, sum(int4)=bigint, avg(int4)=numeric"},
		{name: "int64_price_int64_volume", price: "px_i64", volume: "vol_i64",
			want:   []string{"o=INT64", "h=INT64", "l=INT64", "c=INT64", "v=DECIMAL(38,0)", "w=DECIMAL(38,4)"},
			pgSays: "min(int8)=bigint, sum(int8)=numeric, avg(int8)=numeric"},
		{name: "float64_price_float64_volume", price: "px_f64", volume: "vol_f64",
			want:   []string{"o=FLOAT64", "h=FLOAT64", "l=FLOAT64", "c=FLOAT64", "v=FLOAT64", "w=FLOAT64"},
			pgSays: "min/sum/avg of double precision are all double precision"},
		{name: "float32_price_int32_volume", price: "px_f32", volume: "vol_i32",
			want:   []string{"o=FLOAT32", "h=FLOAT32", "l=FLOAT32", "c=FLOAT32", "v=INT64", "w=FLOAT64"},
			pgSays: "min(real)=real; one approximate operand makes the quotient double"},
		{name: "decimal92_price_int32_volume", price: "px_d92", volume: "vol_i32",
			want:   []string{"o=DECIMAL(9,2)", "h=DECIMAL(9,2)", "l=DECIMAL(9,2)", "c=DECIMAL(9,2)", "v=INT64", "w=DECIMAL(38,6)"},
			pgSays: "min(numeric(9,2)) keeps (9,2); sum(int4)=bigint; avg adds ADR-0024's +4"},
		{name: "decimal184_price_int64_volume", price: "px_d184", volume: "vol_i64",
			want:   []string{"o=DECIMAL(18,4)", "h=DECIMAL(18,4)", "l=DECIMAL(18,4)", "c=DECIMAL(18,4)", "v=DECIMAL(38,0)", "w=DECIMAL(38,8)"},
			pgSays: "min(numeric(18,4)) keeps (18,4); sum(int8)=numeric"},
		{name: "decimal184_price_decimal92_volume", price: "px_d184", volume: "vol_d92",
			want:   []string{"o=DECIMAL(18,4)", "h=DECIMAL(18,4)", "l=DECIMAL(18,4)", "c=DECIMAL(18,4)", "v=DECIMAL(38,2)", "w=DECIMAL(38,8)"},
			pgSays: "sum(numeric(9,2)) keeps scale 2 at the carrier's width"},
		{name: "decimal3810_price_int32_volume", price: "px_d3810", volume: "vol_i32",
			want:   []string{"o=DECIMAL(38,10)", "h=DECIMAL(38,10)", "l=DECIMAL(38,10)", "c=DECIMAL(38,10)", "v=INT64", "w=DECIMAL(38,14)"},
			pgSays: "the widest DECIMAL the carrier holds; avg's +4 on scale 10"},
		{name: "decimal184_price_float64_volume", price: "px_d184", volume: "vol_f64",
			want:   []string{"o=DECIMAL(18,4)", "h=DECIMAL(18,4)", "l=DECIMAL(18,4)", "c=DECIMAL(18,4)", "v=FLOAT64", "w=FLOAT64"},
			pgSays: "the carrier is per field GROUP: an exact price keeps its digits beside a float volume"},
		{name: "float64_price_int64_volume", price: "px_f64", volume: "vol_i64",
			want:   []string{"o=FLOAT64", "h=FLOAT64", "l=FLOAT64", "c=FLOAT64", "v=DECIMAL(38,0)", "w=FLOAT64"},
			pgSays: "the mirror: an exact volume still sums exactly beside a float price"},
		// A COMPUTED price: the plan types the EXPRESSION, so the bar's four
		// price fields declare what MIN of that expression declares.
		// DECIMAL(18,4) * 2 is DECIMAL(20,4) by ADR-0024's multiply rule.
		{name: "computed_decimal_price", price: "px_d184*2", volume: "vol_i64",
			want:   []string{"o=DECIMAL(20,4)", "h=DECIMAL(20,4)", "l=DECIMAL(20,4)", "c=DECIMAL(20,4)", "v=DECIMAL(38,0)", "w=DECIMAL(38,8)"},
			pgSays: "min(numeric(18,4)*2) is numeric(20,4); the aggregate declares its argument's type"},
	}
}

// odcDecls renders one result's DECLARED types, which is the whole point: the
// value census renders a DECIMAL as its text and cannot see (p,s).
func odcDecls(cols []parquet.Column, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, c.Name+"="+odcType(c))
	}
	return out, nil
}

func odcType(c parquet.Column) string {
	if c.Type == parquet.TypeDecimal {
		return fmt.Sprintf("DECIMAL(%d,%d)", c.Precision, c.Scale)
	}
	return c.Type.String()
}

// tmdRunSingleDeclAll is tmdRunSingleDecl for EVERY column rather than the
// first: a bar has six fields and five of them would otherwise go unwatched.
func tmdRunSingleDeclAll(ctx context.Context, db *wadjet.DB, sql string) (cols []parquet.Column, err error) {
	defer func() {
		if r := recover(); r != nil {
			cols, err = nil, fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := db.Query(ctx, sql)
	if qerr != nil {
		return nil, qerr
	}
	for _, m := range out.ColumnMetas {
		cols = append(cols, parquet.Column{
			Name: m.Name, Type: m.TypeID, Precision: m.Precision, Scale: m.Scale,
		})
	}
	return cols, nil
}

func tmdRunDAGDeclAll(ctx context.Context, coord *Coordinator, sql string) (cols []parquet.Column, err error) {
	defer func() {
		if r := recover(); r != nil {
			cols, err = nil, fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := coord.ExecuteSQL(ctx, sql)
	if qerr != nil {
		return nil, qerr
	}
	if out.Error != "" {
		return nil, fmt.Errorf("%s", out.Error)
	}
	// Before Rows(): materializing the result detaches the batches the schema
	// would otherwise be read from.
	schema := append([]parquet.Column(nil), out.OutputSchema()...)
	if _, rerr := out.Rows(); rerr != nil {
		return nil, fmt.Errorf("materializing distributed rows: %w", rerr)
	}
	return schema, nil
}

// EVERY GROUPED SPELLING DECLARES THE SAME THING (#965 round 3).
//
// The census above holds the price × volume MATRIX in two shapes. This holds
// the SHAPE space at one point of that matrix — a DECIMAL(18,4) price with a
// DECIMAL(9,2) volume, which is the pairing whose three field groups declare
// three different things (prices (18,4), volume (38,2), vwap (38,8)), so a
// spelling that loses one of them cannot hide behind another.
//
// Why a second gate rather than more cells: the divergence round 3 found was
// not about a TYPE at all. It was about a SHAPE — a computed group key
// published beside the bar — and the matrix census would have needed all
// eleven cells re-run per spelling to say the same thing. The two gates split
// the axes: types there, spellings here.
//
// Every spelling below is one a user writes. `GROUP BY 1`, the CTE, the key
// dropped, two keys, the bar beside COUNT(*) and SUM, a filter and an ORDER BY
// on a bar FIELD above the block — the review's list, plus the two the
// original defect was localized with (the key dropped and a BARE column as the
// key, both of which always agreed and which are the controls that say the
// trigger is the COMPUTED key and not grouping as such).
func TestTheBarsDeclaredTypeSurvivesEveryGroupedSpelling(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM, func(w *worker.Config) { w.MorselWorkers = 4 })

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return odcDecls(tmdRunSingleDeclAll(ctx, single, sql)) }},
		{"single+budget", func(sql string) ([]string, error) { return odcDecls(tmdRunSingleDeclAll(ctx, spilled, sql)) }},
		{"dag", func(sql string) ([]string, error) { return odcDecls(tmdRunDAGDeclAll(ctx, coord, sql)) }},
		{"dag+shuffled", func(sql string) ([]string, error) { return odcDecls(tmdRunDAGDeclAll(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return odcDecls(tmdRunDAGDeclAll(ctx, coordM, sql)) }},
	}

	// The bar over px_d184 × vol_d92, in the census's own spelling.
	const bar = `ohlcv(ts, px_d184, vol_d92)`
	const bkt = `time_bucket(INTERVAL '1' MINUTE, ts)`
	// The six fields' declarations at this point of the matrix. Identical to
	// the census's `decimal184_price_decimal92_volume` cell, and deliberately
	// spelled out again: a shared constant would let the two gates agree by
	// construction rather than by measurement.
	sixFields := []string{
		"o=DECIMAL(18,4)", "h=DECIMAL(18,4)", "l=DECIMAL(18,4)", "c=DECIMAL(18,4)",
		"v=DECIMAL(38,2)", "w=DECIMAL(38,8)",
	}
	withKey := append([]string{"g=TIMESTAMP"}, sixFields...)

	for _, tc := range []struct {
		name string
		sql  func(where string) string
		want []string
	}{
		{"key_published_beside_the_bar", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT %s AS g, %s AS b FROM %s%s GROUP BY 1) t`,
				odcFields, bkt, bar, ohlcvTable, w)
		}, sixFields},
		{"key_and_bar_both_selected", func(w string) string {
			return fmt.Sprintf(`SELECT g, %s FROM (SELECT %s AS g, %s AS b FROM %s%s GROUP BY 1) t ORDER BY g`,
				odcFields, bkt, bar, ohlcvTable, w)
		}, withKey},
		// CONTROL: the key DROPPED from the block's select list. Always
		// agreed, before the fix and after — it never reaches the computed
		// projection at all, which is how the trigger was localized.
		{"ctl_key_dropped", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT %s AS b FROM %s%s GROUP BY %s) t`,
				odcFields, bar, ohlcvTable, w, bkt)
		}, sixFields},
		// CONTROL: a BARE column as the group key. Also always agreed: a
		// column reference is not a computed projection.
		{"ctl_bare_column_key", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT c_str AS g, %s AS b FROM %s%s GROUP BY 1) t`,
				odcFields, bar, ohlcvTable, w)
		}, sixFields},
		{"group_by_the_expression_not_the_ordinal", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT %s AS g, %s AS b FROM %s%s GROUP BY %s) t`,
				odcFields, bkt, bar, ohlcvTable, w, bkt)
		}, sixFields},
		{"cte_spelling", func(w string) string {
			return fmt.Sprintf(`WITH bars AS (SELECT %s AS g, %s AS b FROM %s%s GROUP BY 1)
			                    SELECT %s FROM bars`, bkt, bar, ohlcvTable, w, odcFields)
		}, sixFields},
		{"two_group_keys", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT %s AS g, c_str AS s, %s AS b
			                    FROM %s%s GROUP BY 1, 2) t`,
				odcFields, bkt, bar, ohlcvTable, w)
		}, sixFields},
		{"bar_beside_count", func(w string) string {
			return fmt.Sprintf(`SELECT %s, n FROM (SELECT %s AS g, %s AS b, COUNT(*) AS n
			                    FROM %s%s GROUP BY 1) t`,
				odcFields, bkt, bar, ohlcvTable, w)
		}, append(append([]string{}, sixFields...), "n=INT64")},
		{"bar_beside_sum", func(w string) string {
			return fmt.Sprintf(`SELECT %s, s FROM (SELECT %s AS g, %s AS b, SUM(vol_i64) AS s
			                    FROM %s%s GROUP BY 1) t`,
				odcFields, bkt, bar, ohlcvTable, w)
		}, append(append([]string{}, sixFields...), "s=DECIMAL(38,0)")},
		// A bar FIELD used above the block, which is where round 2's repair
		// runs and where the review found two more divergent spellings.
		{"field_filtered_above", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT %s AS g, %s AS b FROM %s%s GROUP BY 1) t
			                    WHERE (b).high > 12`, odcFields, bkt, bar, ohlcvTable, w)
		}, sixFields},
		{"field_ordered_above", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT %s AS g, %s AS b FROM %s%s GROUP BY 1) t
			                    ORDER BY (b).vwap DESC`, odcFields, bkt, bar, ohlcvTable, w)
		}, sixFields},
		// A COMPUTED price under a computed group key: the two mechanisms
		// round 2 and round 3 fixed, in one statement. DECIMAL(18,4) * 2 is
		// DECIMAL(20,4) by ADR-0024's multiply rule, and vwap follows the
		// argument's scale.
		{"computed_price_under_a_computed_key", func(w string) string {
			return fmt.Sprintf(`SELECT %s FROM (SELECT %s AS g,
			                    ohlcv(ts, px_d184*2, vol_d92) AS b FROM %s%s GROUP BY 1) t`,
				odcFields, bkt, ohlcvTable, w)
		}, []string{
			"o=DECIMAL(20,4)", "h=DECIMAL(20,4)", "l=DECIMAL(20,4)", "c=DECIMAL(20,4)",
			"v=DECIMAL(38,2)", "w=DECIMAL(38,8)",
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, empty := range []bool{false, true} {
				where, label := "", "rows"
				if empty {
					where, label = " WHERE id < 0", "empty"
				}
				sql := tc.sql(where)
				for _, arm := range arms {
					got, err := arm.run(sql)
					if err != nil {
						t.Errorf("%s arm (%s): %v\n  SQL: %s", arm.name, label, err, sql)
						continue
					}
					if strings.Join(got, " | ") != strings.Join(tc.want, " | ") {
						t.Errorf("%s arm (%s)\n  got  %s\n  want %s\n  SQL: %s",
							arm.name, label, strings.Join(got, " | "),
							strings.Join(tc.want, " | "), sql)
					}
				}
			}
		})
	}
}
