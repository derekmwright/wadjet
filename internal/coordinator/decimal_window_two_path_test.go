package coordinator

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The distributed half of #586/#475: a windowed SUM/AVG over a DECIMAL
// answers the same exact DECIMAL the GROUPED SUM/AVG answer, on BOTH
// execution paths.
//
// The DAG is the arm nothing else covers. It executes window stages (#349),
// and it reaches the operator by a different route than the single-process
// pipeline: `physical.walkStages` ships a `WindowColSpec` carrying a bare
// TypeID, the stage's input arrives from S3 rather than from the scan's own
// vectors, and `worker.buildFragmentWindow` rebuilds `exec.WindowColumn`
// with no catalog and no logical plan. The (p,s) of the output column is
// therefore resolved on the worker from the input schema — so a declaration
// that carried the accumulator's scale on only one path would show up here
// as an arm disagreement rather than as a wrong answer on both.
//
// Expectations are computed in big.Int from the fixture generator, so this is
// not only "the arms agree" but "both are right".

const (
	dwpTable  = "decwin"
	dwpRows   = 200
	dwpGroups = 4
	// The same 23-digit step dtpTable uses: every non-zero value needs more
	// than 64 bits, so a truncation to int64 or a round-trip through float64
	// cannot hide inside a right answer.
	dwpWideStep = "97777777788777775778877"
)

func dwpSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "dw", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
}

// dwpUnscaled is the fixture generator: row i's unscaled value, or nil where
// the column is NULL. Every 11th row is NULL, so every partition carries some
// and none carries only them — the shape AVG's denominator turns on.
func dwpUnscaled(i int) *big.Int {
	if i%11 == 10 {
		return nil
	}
	step, _ := new(big.Int).SetString(dwpWideStep, 10)
	return step.Mul(step, big.NewInt(int64(i-97)))
}

func dwpData() []map[string]any {
	rows := make([]map[string]any, dwpRows)
	for i := range rows {
		r := map[string]any{"id": int64(i), "k": int64(i % dwpGroups)}
		if u := dwpUnscaled(i); u != nil {
			r["dw"] = dtpDecimal128(u)
		} else {
			r["dw"] = nil
		}
		rows[i] = r
	}
	return rows
}

// dwpFrameRows lists the row indices row i's frame sees, in the order the
// window walks them: the partition (or the whole table for a global window)
// filtered to the frame's bounds.
//
// partition < 0 means "every row"; back < 0 means "unbounded preceding".
func dwpFrameRows(i, back int, partitioned, ordered bool) []int {
	var part []int
	for j := 0; j < dwpRows; j++ {
		if partitioned && j%dwpGroups != i%dwpGroups {
			continue
		}
		part = append(part, j)
	}
	if !ordered {
		return part
	}
	pos := -1
	for p, j := range part {
		if j == i {
			pos = p
			break
		}
	}
	lo := 0
	if back >= 0 {
		lo = max(pos-back, 0)
	}
	return part[lo : pos+1]
}

// dwpWant is the exact SUM and AVG over a set of rows, rendered the way a
// DECIMAL cell reaches a client. Both are "" for a frame with no non-NULL
// row, which is SQL's NULL.
func dwpWant(rows []int) (sum, avg string) {
	total := big.NewInt(0)
	var n int64
	for _, j := range rows {
		u := dwpUnscaled(j)
		if u == nil {
			continue // a NULL is not part of an aggregate's input
		}
		total.Add(total, u)
		n++
	}
	if n == 0 {
		return "", ""
	}
	return dtpText(total, 10), dtpAvgText(total, n, 10)
}

func TestDecimalWindowAggregatesTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, tc := range []struct {
		name string
		over string
		// frame describes the same window to the expectation side.
		partitioned bool
		ordered     bool
		back        int // rows preceding; -1 = unbounded
	}{
		// The whole partition: every row of a group sees the group's total,
		// which is exactly what `SUM(dw) ... GROUP BY k` answers. The two
		// spellings of one question, held to one answer.
		{"partition", "PARTITION BY k", true, false, -1},
		// The default frame with an ORDER BY: the running total through the
		// row's ORDER-BY peer group. id is unique, so each row is its own.
		{"running", "PARTITION BY k ORDER BY id", true, true, -1},
		// The SLIDING frame, where the accumulator RETRACTS a row. That
		// subtraction has to be exact and checked (Int128.SubChecked); a
		// float running total additionally loses associativity there.
		{"sliding", "PARTITION BY k ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW", true, true, 2},
		// An empty PARTITION BY: one partition spanning the input, which on
		// the DAG needs every row in one place.
		{"global", "", false, false, -1},
		{"global-running", "ORDER BY id", false, true, -1},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			over := tc.over
			sql := fmt.Sprintf(
				"SELECT id, SUM(dw) OVER (%s) AS s, AVG(dw) OVER (%s) AS a FROM %s ORDER BY id",
				over, over, dwpTable)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != dwpRows {
					t.Fatalf("%s: %d rows, want %d", arm.name, len(rows), dwpRows)
				}
				for _, r := range rows {
					id, _ := r["id"].(int64)
					wantSum, wantAvg := dwpWant(dwpFrameRows(int(id), tc.back, tc.partitioned, tc.ordered))
					dwpCell(t, fmt.Sprintf("%s id=%d SUM", arm.name, id), r["s"], wantSum)
					dwpCell(t, fmt.Sprintf("%s id=%d AVG", arm.name, id), r["a"], wantAvg)
				}
			}
		})
	}
}

// dwpCell asserts one window cell. want == "" means SQL NULL.
func dwpCell(t *testing.T, what string, got any, want string) {
	t.Helper()
	if want == "" {
		if got != nil {
			t.Errorf("%s = %#v, want NULL", what, got)
		}
		return
	}
	s, ok := got.(string)
	if !ok {
		t.Errorf("%s = %#v (%T), want the DECIMAL text %q — a non-string box is the float64 "+
			"answer #586 is about", what, got, got, want)
		return
	}
	if s != want {
		t.Errorf("%s = %q, want %q", what, s, want)
	}
}
