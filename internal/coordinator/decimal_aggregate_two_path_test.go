package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The distributed half of #455.
//
// MIN/MAX/SUM/AVG over a DECIMAL answer in DECIMAL now, which changes what
// the stage DAG carries end to end: the partial aggregate's output column
// type, the WSHF shuffle encoding of it, the partial→final merge, and — for
// AVG — the worker's fold of (__avg_sum#X, __avg_count#X) back into one
// column. Every one of those is a place the two paths could disagree, and the
// two-path contract is that they do not (ADR-0018 §3).
//
// The expectation is computed in big.Int from the fixture generator, so this
// is not only "the arms agree" but "both are right": two paths through the
// same wrong accumulator would agree with each other and fail here.

const (
	dtpTable  = "decagg"
	dtpRows   = 400
	dtpGroups = 4
	// 23 digits: every non-zero value needs more than 64 bits, so a
	// truncation to int64 or a round-trip through float64 cannot hide inside
	// a right answer.
	dtpWideStep = "97777777788777775778877"
)

func dtpSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "d4", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "dw", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
}

// dtpUnscaled is the fixture generator: row i's unscaled value, or nil where
// the column is NULL.
func dtpUnscaled(col string, i int) *big.Int {
	switch col {
	case "d4":
		if i%7 == 6 {
			return nil
		}
		return big.NewInt(int64(i+1) * 1234567890123)
	case "dw":
		if i%11 == 10 {
			return nil
		}
		step, _ := new(big.Int).SetString(dtpWideStep, 10)
		return step.Mul(step, big.NewInt(int64(i-137)))
	}
	panic("unknown column " + col)
}

func dtpScale(col string) int {
	if col == "d4" {
		return 4
	}
	return 10
}

func dtpData() []map[string]any {
	rows := make([]map[string]any, dtpRows)
	for i := range rows {
		r := map[string]any{"k": int64(i % dtpGroups)}
		for _, col := range []string{"d4", "dw"} {
			if u := dtpUnscaled(col, i); u != nil {
				r[col] = dtpDecimal128(u)
			} else {
				r[col] = nil
			}
		}
		rows[i] = r
	}
	return rows
}

// dtpDecimal128 renders a signed big.Int as the halves parquet.Decimal128
// carries. An integer box IS the unscaled value at the column's scale
// (ADR-0018's writer corollary).
func dtpDecimal128(n *big.Int) parquet.Decimal128 {
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	var b [16]byte
	m.FillBytes(b[:])
	hi := new(big.Int).Rsh(new(big.Int).SetBytes(b[:]), 64).Uint64()
	lo := new(big.Int).And(new(big.Int).SetBytes(b[:]), new(big.Int).SetUint64(^uint64(0))).Uint64()
	return parquet.Decimal128{Hi: int64(hi), Lo: lo}
}

// dtpText renders an unscaled integer at a scale the way a DECIMAL cell
// reaches a client: split at the scale, the fraction EXACTLY scale digits
// wide (#453 — a numeric(p,s) renders at its declared scale).
func dtpText(unscaled *big.Int, scale int) string {
	neg := unscaled.Sign() < 0
	digits := new(big.Int).Abs(unscaled).String()
	if scale <= 0 {
		if neg {
			return "-" + digits
		}
		return digits
	}
	intPart, frac := "0", digits
	if len(digits) > scale {
		intPart, frac = digits[:len(digits)-scale], digits[len(digits)-scale:]
	} else {
		frac = strings.Repeat("0", scale-len(digits)) + digits
	}
	if neg {
		return "-" + intPart + "." + frac
	}
	return intPart + "." + frac
}

type dtpExpect struct {
	min, max, sum *big.Int
	count         int64
}

func dtpCompute(col string, group int) dtpExpect {
	var e dtpExpect
	e.sum = big.NewInt(0)
	for i := 0; i < dtpRows; i++ {
		if group >= 0 && i%dtpGroups != group {
			continue
		}
		u := dtpUnscaled(col, i)
		if u == nil {
			continue
		}
		if e.count == 0 {
			e.min, e.max = new(big.Int).Set(u), new(big.Int).Set(u)
		} else {
			if u.Cmp(e.min) < 0 {
				e.min = new(big.Int).Set(u)
			}
			if u.Cmp(e.max) > 0 {
				e.max = new(big.Int).Set(u)
			}
		}
		e.sum.Add(e.sum, u)
		e.count++
	}
	return e
}

// dtpAvgText is the AVG contract: exact division at scale+4, half away from
// zero (batch.AvgScale). Computed here in big.Int so the expectation does not
// come from the code under test.
func dtpAvgText(sum *big.Int, count int64, scale int) string {
	outScale := scale + 4
	if outScale > 38 {
		outScale = 38
	}
	shift := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(outScale-scale)), nil)
	num := new(big.Int).Mul(sum, shift)
	den := big.NewInt(count)
	q, rem := new(big.Int).QuoRem(num, den, new(big.Int))
	rem.Abs(rem)
	if rem.Lsh(rem, 1).Cmp(den) >= 0 {
		if num.Sign() < 0 {
			q.Sub(q, big.NewInt(1))
		} else {
			q.Add(q, big.NewInt(1))
		}
	}
	return dtpText(q, outScale)
}

func TestDecimalAggregatesTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, col := range []string{"d4", "dw"} {
		scale := dtpScale(col)

		t.Run("scalar_"+col, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT MIN(%s) AS lo, MAX(%s) AS hi, SUM(%s) AS s, AVG(%s) AS a FROM %s",
				col, col, col, col, dtpTable)
			want := dtpCompute(col, -1)
			rowFor := func(arm string, rows []map[string]any) map[string]any {
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm, len(rows))
				}
				return rows[0]
			}
			a := rowFor("single", dtpRun(t, ctx, single, coord, sql, false))
			b := rowFor("dag", dtpRun(t, ctx, single, coord, sql, true))
			for _, arm := range []struct {
				name string
				row  map[string]any
			}{{"single", a}, {"dag", b}} {
				dtpCell(t, arm.name+" MIN", arm.row["lo"], dtpText(want.min, scale))
				dtpCell(t, arm.name+" MAX", arm.row["hi"], dtpText(want.max, scale))
				dtpCell(t, arm.name+" SUM", arm.row["s"], dtpText(want.sum, scale))
				dtpCell(t, arm.name+" AVG", arm.row["a"], dtpAvgText(want.sum, want.count, scale))
			}
		})

		t.Run("grouped_"+col, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT k, MIN(%s) AS lo, MAX(%s) AS hi, SUM(%s) AS s, AVG(%s) AS a "+
				"FROM %s GROUP BY k ORDER BY k", col, col, col, col, dtpTable)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != dtpGroups {
					t.Fatalf("%s: %d groups, want %d", arm.name, len(rows), dtpGroups)
				}
				for g, r := range rows {
					want := dtpCompute(col, g)
					pre := fmt.Sprintf("%s group %d ", arm.name, g)
					dtpCell(t, pre+"MIN", r["lo"], dtpText(want.min, scale))
					dtpCell(t, pre+"MAX", r["hi"], dtpText(want.max, scale))
					dtpCell(t, pre+"SUM", r["s"], dtpText(want.sum, scale))
					dtpCell(t, pre+"AVG", r["a"], dtpAvgText(want.sum, want.count, scale))
				}
			}
		})
	}

	// A predicate that empties some scan tasks: a partial aggregate that
	// consumed no rows emits an identity row whose DECIMAL column carries no
	// scale, and a merge that adopted that as the answer's scale would render
	// every value 10^scale too large.
	t.Run("sparse_predicate", func(t *testing.T) {
		sql := "SELECT SUM(dw) AS s, COUNT(dw) AS n FROM " + dtpTable + " WHERE k = 3 AND dw IS NOT NULL"
		var want dtpExpect
		want.sum = big.NewInt(0)
		for i := 0; i < dtpRows; i++ {
			if i%dtpGroups != 3 {
				continue
			}
			if u := dtpUnscaled("dw", i); u != nil {
				want.sum.Add(want.sum, u)
				want.count++
			}
		}
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 1 {
				t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
			}
			dtpCell(t, arm.name+" SUM", rows[0]["s"], dtpText(want.sum, 10))
			if n, _ := rows[0]["n"].(int64); n != want.count {
				t.Errorf("%s COUNT = %v, want %d", arm.name, rows[0]["n"], want.count)
			}
		}
	})
}

func dtpRun(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator, sql string, dag bool) []map[string]any {
	t.Helper()
	if dag {
		res, err := tmdRunDAG(ctx, coord, sql)
		if err != nil {
			t.Fatalf("stage DAG refused %q: %v", sql, err)
		}
		return res.Rows
	}
	res, err := tmdRunSingle(ctx, single, sql)
	if err != nil {
		t.Fatalf("single-process engine refused %q: %v", sql, err)
	}
	return res.Rows
}

func dtpCell(t *testing.T, what string, got any, want string) {
	t.Helper()
	s, ok := got.(string)
	if !ok {
		t.Errorf("%s = %#v (%T), want the DECIMAL text %q — a non-string box is the float64 "+
			"answer #455 is about", what, got, got, want)
		return
	}
	if s != want {
		t.Errorf("%s = %q, want %q", what, s, want)
	}
}

// The overflow fixture: values chosen so the EXACT answer leaves the Int128
// carrier, which is the one case a DECIMAL aggregate must refuse rather than
// answer. One row per file, so no single task overflows and the condition is
// only reachable in the partial→final merge — the DAG shape that could
// otherwise wrap where the single process errors.
const dtpOvfTable = "decovf"

func dtpOvfSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		// 9 x 10^37 is the widest a DECIMAL(38,0) holds and still fits the
		// carrier (ceiling 1.70 x 10^38); three of them do not.
		{Name: "big", Type: parquet.TypeDecimal, Precision: 38, Scale: 0, Nullable: true},
		// 5 x 10^24 at scale 10: the SUM fits, but AVG scales it by 10^4
		// before dividing and that product does not — the condition SUM's
		// check does NOT cover.
		{Name: "wide", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
}

func dtpOvfData() []map[string]any {
	nine37, _ := new(big.Int).SetString("90000000000000000000000000000000000000", 10)
	wide, _ := new(big.Int).SetString("50000000000000000000000000000000000", 10)
	rows := make([]map[string]any, 3)
	for i := range rows {
		rows[i] = map[string]any{
			"k":    int64(i),
			"big":  dtpDecimal128(nine37),
			"wide": dtpDecimal128(wide),
		}
	}
	return rows
}

// TestDecimalAggregateOverflowTwoPath: a DECIMAL aggregate with no exact
// 128-bit answer fails on BOTH paths.
//
// The two-path contract covers refusals, not only values (ADR-0018 §3). AVG
// is the arm that caught this: the single-process engine used to answer NULL
// where the DAG's avg-fold raised, so the same query returned "no value" in
// one deployment and an error in another — and a NULL is indistinguishable
// from an empty group, which is the silent-wrong-answer shape #455 is about.
func TestDecimalAggregateOverflowTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct{ name, sql, want string }{
		{"sum_scalar", "SELECT SUM(big) AS s FROM " + dtpOvfTable, "overflow"},
		{"sum_grouped", "SELECT 1 AS g, SUM(big) AS s FROM " + dtpOvfTable + " GROUP BY g", "overflow"},
		{"avg_scalar", "SELECT AVG(wide) AS a FROM " + dtpOvfTable, "no exact 128-bit value"},
		{"avg_grouped", "SELECT 1 AS g, AVG(wide) AS a FROM " + dtpOvfTable + " GROUP BY g", "no exact 128-bit value"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, serr := tmdRunSingle(ctx, single, tc.sql)
			_, derr := tmdRunDAG(ctx, coord, tc.sql)
			if serr == nil {
				t.Errorf("single-process answered %q; the exact value has no Int128 and the "+
					"wrapped or NULL one is not the answer", tc.sql)
			} else if !strings.Contains(serr.Error(), tc.want) {
				t.Errorf("single-process error does not name the condition (%q): %v", tc.want, serr)
			}
			if derr == nil {
				t.Errorf("the stage DAG answered %q where the single process refused it", tc.sql)
			} else if !strings.Contains(derr.Error(), tc.want) {
				t.Errorf("DAG error does not name the condition (%q): %v", tc.want, derr)
			}
		})
	}
}
