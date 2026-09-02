package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// The numeric/decimal arc-2 census: every shape on FOUR arms, each answer
// anchored to live PostgreSQL 17 rather than to the other arm.
//
// The arms are the four an answer can differ between (ADR-0018 §3, ADR-0027):
//
//	single             the embedded engine, no memory budget
//	single+spill       the same engine at 512 KiB, so every pipeline breaker
//	                   drains — a spill is a CONDITION, not a shape, and it is
//	                   the arm a shape corpus never reaches on purpose
//	dag                the stage DAG over an embedded NATS cluster
//	dag+broadcast      the DAG with BroadcastBytesOverride = 1, which forces
//	                   the shuffle rather than the broadcast join
//
// `want` is what `psql` printed on the oracle container (127.0.0.1:55432,
// --locale=C, PostgreSQL 17) for the SAME rows, recorded before any of these
// fixes was written. A DECIMAL is compared DIGIT FOR DIGIT: the whole point of
// these issues is a value that is right to six digits and wrong after them.
//
// Issues: #727 (a CTE's TEXT column re-typed from its first row's VALUE),
// #728 (an aggregate's output declared FLOAT64 through a rename), #786/#781
// (a derived GROUP BY key typed against a scope that stops at a Project),
// #749 (an exact operator's scale reduced to buy integer digits), #703
// (DISTINCT dropped for every aggregate but COUNT), #704 (an integer column
// compared against a non-integral literal by truncating the literal).
func TestNumericArc2ShapesMatchPostgres(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

	for _, tc := range []struct {
		issue, name, sql string
		want             []string
	}{
		// ------------------------------------------------------------------
		// #727 — a CTE materialization used to read the FIRST ROW of every
		// STRING column and, when that one value parsed as a number, re-type
		// the whole column. decpair.s is TEXT holding "1.50","1.5","abc",
		// "1.500": the column came back double precision, `s = '1.50'`
		// compared NUMBERS, and "abc" became NULL and then a hard
		// `invalid input syntax for type double precision`.
		{"#727", "cte_text_column_stays_text",
			`WITH c AS (SELECT s, id FROM decpair) SELECT COUNT(*) AS n FROM c WHERE s = '1.50'`,
			[]string{"n=int64:1"}},
		{"#727", "derived_table_twin_unchanged",
			`SELECT COUNT(*) AS n FROM (SELECT s, id FROM decpair) c WHERE s = '1.50'`,
			[]string{"n=int64:1"}},
		{"#727", "cte_text_in_a_case_arm",
			`WITH c AS (SELECT s, a*2 AS v FROM decpair) SELECT SUM(CASE WHEN s='abc' THEN v ELSE 0 END) AS sm FROM c`,
			[]string{"sm=25.50"}},
		{"#727", "cte_text_column_reads_back_whole",
			`WITH c AS (SELECT s, id FROM decpair) SELECT s AS v FROM c WHERE id IN (1,3,6)`,
			[]string{"v=1.50", "v=1.500", "v=abc"}},
		// The type used to depend on the DATA: the same CTE body restricted
		// to the row holding "abc" kept the string, and without the
		// restriction did not. Both spellings must now answer as TEXT.
		{"#727", "cte_text_type_does_not_depend_on_the_first_row",
			`WITH c AS (SELECT s, id FROM decpair WHERE id = 3) SELECT s AS v FROM c`,
			[]string{"v=abc"}},

		// ------------------------------------------------------------------
		// #728 — SUM/AVG/MIN/MAX resolved a bare column argument by searching
		// the SCANS below for that NAME, which cannot see a rename. Over
		// `(SELECT dw AS v FROM decwin) x` nothing carries `v`, so the
		// aggregate's OUTPUT declared FLOAT64 and the projection above it
		// computed in float: two spellings of one question, two numbers.
		{"#728", "sum_times_two_over_a_rename",
			`SELECT SUM(v*2) AS s FROM (SELECT dw AS v FROM decwin) x`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "sum_times_two_over_the_base_column",
			`SELECT SUM(dw*2) AS s FROM decwin`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "sum_times_two_through_a_cte",
			`WITH c AS (SELECT dw AS v FROM decwin) SELECT SUM(v*2) AS s FROM c`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "max_times_two_over_a_rename",
			`SELECT MAX(v*2) AS m FROM (SELECT dw AS v FROM decwin) x`,
			[]string{"m=1994666666891066.6258890908"}},

		// ------------------------------------------------------------------
		// #786/#781 — a derived GROUP BY key was typed against inputColDecls,
		// which stops at a Project, and the arithmetic above the unresolved
		// column still answered expr.Decided FLOAT64. The key therefore
		// declared FLOAT64 over a derived table and died at the #361 store
		// guard. The delimited sibling column in #786's own spelling is not
		// the trigger: a parsed BinaryOp asks for `c_dec`, never for the name
		// "c_dec + 1" (ADR-0026 §2c).
		{"#786", "derived_decimal_key_over_a_derived_table",
			`SELECT c_dec + 1 AS k FROM (SELECT c_dec FROM typemx WHERE id < 4) s GROUP BY c_dec + 1 ORDER BY k`,
			[]string{"k=1.0000", "k=2.0001", "k=3.0002", "k=4.0003"}},
		{"#786", "derived_decimal_key_beside_a_delimited_column_of_that_text",
			`SELECT c_dec + 1 AS k, MAX("c_dec + 1") AS m FROM ` +
				`(SELECT c_dec, c_i32 AS "c_dec + 1" FROM typemx WHERE id < 4) s GROUP BY c_dec + 1 ORDER BY k`,
			[]string{"k=1.0000|m=int64:0", "k=2.0001|m=int64:3", "k=3.0002|m=int64:6", "k=4.0003|m=int64:9"}},

		// ------------------------------------------------------------------
		// #749 — item 3's p>38 reduction spent an EXACT operator's fraction
		// digits to buy integer digits: over DECIMAL(38,10), `dw + 1` came
		// back at scale 9 and `dw * 2` at scale 8, correctly rounded and so
		// indistinguishable from exact. PostgreSQL keeps all ten.
		{"#749", "wide_decimal_plus_one_keeps_its_scale",
			`SELECT dw + 1 AS p FROM decwin WHERE id = 199`,
			[]string{"p=997333333445534.3129445454"}},
		{"#749", "wide_decimal_times_two_keeps_its_scale",
			`SELECT dw * 2 AS t FROM decwin WHERE id = 199`,
			[]string{"t=1994666666891066.6258890908"}},
		{"#749", "wide_decimal_minus_a_fraction_keeps_the_wider_scale",
			`SELECT dw - 0.5 AS d FROM decwin WHERE id = 199`,
			[]string{"d=997333333445532.8129445454"}},

		// ------------------------------------------------------------------
		// #703 — DISTINCT was mapped onto COUNT's own AggFunc and dropped for
		// every other aggregate, so SUM(DISTINCT a) was a plain SUM wearing
		// the DISTINCT spelling.
		{"#703", "distinct_over_a_decimal_column",
			`SELECT SUM(DISTINCT a) AS sd, AVG(DISTINCT a) AS ad, MIN(DISTINCT a) AS md, ` +
				`MAX(DISTINCT a) AS xd, COUNT(DISTINCT a) AS cd FROM decpair`,
			[]string{"sd=14.74|ad=3.685000|md=-0.01|xd=12.75|cd=int64:4"}},
		{"#703", "distinct_over_the_wider_decimal_column",
			`SELECT SUM(DISTINCT b) AS sb FROM decpair`,
			[]string{"sb=49.2400"}},
		// UNGROUPED and ALONE is the shape the first cut missed: an ungrouped
		// aggregate whose every function has a BATCH kernel takes the scalar
		// fast path, which folds a whole vector at a time and never consults
		// the group's set. resolveBatchAggKernel answers by FUNC, so it
		// declined COUNT(DISTINCT) — its own AggFunc — and returned SUM's for
		// SUM(DISTINCT). Each of these must therefore stand ALONE: putting a
		// COUNT(DISTINCT) beside them declines the fast path for its own
		// reason and hides the defect.
		{"#703", "distinct_sum_alone_is_ungrouped",
			`SELECT SUM(DISTINCT a) AS sd FROM decpair`, []string{"sd=14.74"}},
		{"#703", "distinct_avg_alone_is_ungrouped",
			`SELECT AVG(DISTINCT a) AS ad FROM decpair`, []string{"ad=3.685000"}},
		{"#703", "distinct_min_alone_is_ungrouped",
			`SELECT MIN(DISTINCT a) AS md FROM decpair`, []string{"md=-0.01"}},
		{"#703", "distinct_sum_over_integers_alone",
			`SELECT SUM(DISTINCT c_i32) AS s FROM typemx`, []string{"s=float:3.61986e+07"}},
		{"#703", "distinct_grouped_over_integers",
			`SELECT g AS k, SUM(DISTINCT c_i32) AS s, COUNT(DISTINCT c_i32) AS c ` +
				`FROM typemx WHERE id < 40 GROUP BY g ORDER BY k`,
			// Sorted as TEXT by na2Run, so the NULL key leads.
			[]string{
				"k=NULL|s=float:225|c=int64:3",
				"k=int32:0|s=float:231|c=int64:5",
				"k=int32:1|s=float:333|c=int64:6",
				"k=int32:2|s=float:351|c=int64:6",
				"k=int32:3|s=float:255|c=int64:5",
				"k=int32:4|s=float:312|c=int64:5",
				"k=int32:5|s=float:249|c=int64:4",
				"k=int32:6|s=float:300|c=int64:5",
			}},
		{"#703", "distinct_over_a_float_column",
			`SELECT SUM(DISTINCT f) AS sf, AVG(DISTINCT f) AS af FROM decpair`,
			[]string{"sf=float:138.75|af=float:17.3438"}},
		{"#703", "distinct_over_text",
			`SELECT COUNT(DISTINCT s) AS cs, MIN(DISTINCT s) AS ms, MAX(DISTINCT s) AS xs FROM decpair`,
			[]string{"cs=int64:8|ms=-1|xs=abc"}},

		// ------------------------------------------------------------------
		// #704 — an integer column against a NON-INTEGRAL numeric literal.
		// The filter kernels read the constant with `int64(float)`, which
		// TRUNCATES toward zero, so `= 3.5` matched the row holding 3 and
		// `= -0.5` matched the row holding 0. The typemx measurement in the
		// arc brief read 0 for the INT64 column only because no row of that
		// column holds 3; `c_i64 = 1000003.5` matched one.
		{"#704", "int32_eq_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 = 3.5`, []string{"n=int64:0"}},
		{"#704", "int32_ne_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 <> 3.5`, []string{"n=int64:4828"}},
		{"#704", "int32_ne_a_negative_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 <> -0.5`, []string{"n=int64:4828"}},
		{"#704", "int64_not_in_a_fraction_list",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 NOT IN (3.5, 99.5)`, []string{"n=int64:4839"}},
		{"#704", "int32_in_a_fraction_list",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 IN (3.5, 99.5)`, []string{"n=int64:0"}},
		{"#704", "int32_eq_a_negative_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 = -0.5`, []string{"n=int64:0"}},
		{"#704", "int32_gt_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 > 3.5`, []string{"n=int64:4826"}},
		{"#704", "int32_le_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 <= 3.5`, []string{"n=int64:2"}},
		{"#704", "int32_lt_a_negative_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 < -0.5`, []string{"n=int64:0"}},
		{"#704", "int64_eq_a_fraction_a_row_would_truncate_onto",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 = 1000003.5`, []string{"n=int64:0"}},
		{"#704", "int64_ge_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 >= 3.5`, []string{"n=int64:4838"}},
		{"#704", "int32_eq_an_integral_literal_still_matches",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 = 3.0`, []string{"n=int64:1"}},
	} {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				run  func(string) ([]string, error)
			}{
				{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
				{"single+spill", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, spilled, sql)) }},
				{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
				{"dag+broadcast", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
			} {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17: %v", arm.name, err, tc.sql, tc.want)
					continue
				}
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17)\n  SQL: %s",
						arm.name, len(got), len(tc.want), got, tc.want, tc.sql)
					continue
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
						break
					}
				}
			}
		})
	}
}

// na2Standalone is tmdStandalone with a per-query memory budget, so every
// pipeline breaker in the shape drains to disk. A budget alone is not proof
// that it did (ADR-0027 §5); what this arm proves is that the shapes above
// answer the same under pressure, and the exec-level spill suites carry the
// engagement assertions.
func na2Standalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open budgeted standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tmdTables() {
		if tmdStoresAReservedName(tbl) {
			continue // the reserved-name fixtures need the catalog door; no shape here reads them
		}
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: typematrix.RowGroup,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// na2Run renders a result to comparable text.
//
// A DECIMAL is rendered VERBATIM — it arrives as its own text and every issue
// here is about a digit past the sixth, so rounding it would hide the defect.
// A FLOAT is rendered to six significant digits, which is ADR-0013's
// nondeterminism class 9: a float sum's last digits move with the order three
// workers hand batches to the aggregate. The Go TYPE is printed for every
// non-string box, because a float64 holding an exact integer and an int64
// holding it print identically under %v — and "the right number under the
// wrong Go type" is exactly what #728 and #784 are.
func na2Run(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			v := r[c]
			switch t := v.(type) {
			case nil:
				parts = append(parts, c+"=NULL")
			case string:
				parts = append(parts, c+"="+t)
			case float64:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, t))
			case float32:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, float64(t)))
			default:
				parts = append(parts, fmt.Sprintf("%s=%T:%v", c, v, v))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	// Every shape above either carries an ORDER BY or returns one row, so the
	// sort only makes an unordered multiset comparison total.
	sort.Strings(out)
	return out, nil
}
