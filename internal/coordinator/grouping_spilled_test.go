package coordinator

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// gsDrainDB is a budgeted embedded DB over one purpose-made fixture: 2400
// rows, g = i%3 (800 each), h = i%4 (600 each), no NULLs. The budget alone
// does not pressure it — the caller arms exec.ForceAggDrainEvery(1) — so the
// fixture only has to be big enough that a forced drain has state to move.
func gsDrainDB(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 512 * 1024, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open budgeted db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt64},
		{Name: "h", Type: parquet.TypeInt64},
	}}
	if err := db.CreateTable(ctx, "gsdrain", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := make([]map[string]any, 0, 2400)
	for i := 0; i < 2400; i++ {
		rows = append(rows, map[string]any{
			"id": int64(i), "g": int64(i % 3), "h": int64(i % 4),
		})
	}
	ing := db.NewIngester("gsdrain", schema, nil,
		ingest.Config{MaxBufferRows: len(rows) + 1, RowGroupSize: 512})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestGroupingBitmaskSurvivesADrain is the SPILLED arm for GROUPING(...) — the
// fifth execution arm no shape corpus reaches on purpose, because a spill is a
// condition and not a query shape (ADR-0027).
//
// It exists because the bitmask is written by the EMIT path and not by
// aggregate_partial_spill.go's writeMergedRow, which fills group-key and
// aggregate columns only. A plain GROUP BY carrying a GROUPING call was
// admitted to that path and answered correctly by ACCIDENT: the column is
// non-nullable Int32, its unwritten zero value is 0, and 0 is the right
// bitmask when there are no grouping sets. canUseExternalMerge now refuses on
// len(GroupingCalls) > 0 so the answer stops depending on that coincidence,
// and the CUBE/ROLLUP cases below are where a lost bitmask would be VISIBLE —
// their super-aggregate rows carry non-zero masks.
//
// Every `want` is the same answer the unspilled arm gives, which is itself
// transcribed from PostgreSQL 17.11.
func TestGroupingBitmaskSurvivesADrain(t *testing.T) {
	ctx := context.Background()
	db := gsDrainDB(t, ctx)

	cases := []struct {
		name string
		sql  string
		cols []string
		want string
		// partial says whether this shape may engage the partial-state
		// external-merge path (aggregate_partial_spill.go). It is asserted
		// beside the rows because the ROWS cannot tell: writeMergedRow leaves
		// the GROUPING column unwritten and its zero value happens to be the
		// right bitmask for a plain GROUP BY, so a shape wrongly admitted to
		// that path answers correctly and silently. Rule 11: where the fix is
		// a REFUSAL, the fixture asserts the routing, not only the answer.
		partial bool
	}{
		{
			// Non-zero masks on 8 of the 20 rows.
			name: "cube/two-arguments",
			sql: "SELECT g, h, GROUPING(g) AS a, GROUPING(h) AS b, GROUPING(g,h) AS ab, COUNT(*) AS n " +
				"FROM gsdrain GROUP BY CUBE(g,h) ORDER BY ab, g, h",
			cols: []string{"g", "h", "a", "b", "ab", "n"},
			// Grouping sets have always been refused this path.
			partial: false,
			want: "20 rows: " +
				"0|0|0|0|0|200;0|1|0|0|0|200;0|2|0|0|0|200;0|3|0|0|0|200;" +
				"1|0|0|0|0|200;1|1|0|0|0|200;1|2|0|0|0|200;1|3|0|0|0|200;" +
				"2|0|0|0|0|200;2|1|0|0|0|200;2|2|0|0|0|200;2|3|0|0|0|200;" +
				"0||0|1|1|800;1||0|1|1|800;2||0|1|1|800;" +
				"|0|1|0|2|600;|1|1|0|2|600;|2|1|0|2|600;|3|1|0|2|600;" +
				"||1|1|3|2400;",
		},
		{
			name: "rollup/one-argument",
			sql: "SELECT g, GROUPING(g) AS gg, COUNT(*) AS n FROM gsdrain " +
				"GROUP BY ROLLUP(g) ORDER BY gg, g",
			cols:    []string{"g", "gg", "n"},
			want:    "4 rows: 0|0|800;1|0|800;2|0|800;|1|2400;",
			partial: false,
		},
		{
			// The shape the partial-merge refusal is about: a plain GROUP BY
			// carrying a GROUPING call.
			name: "plain-group-by",
			sql: "SELECT g, GROUPING(g) AS gg, COUNT(*) AS n FROM gsdrain " +
				"GROUP BY g ORDER BY g",
			cols: []string{"g", "gg", "n"},
			want: "3 rows: 0|0|800;1|0|800;2|0|800;",
			// The refusal under test: without it this engaged the path.
			partial: false,
		},
		{
			// The CONTROL: the same plain GROUP BY without a GROUPING call is
			// still admitted to the partial-merge path, so the refusal above
			// is bounded to the shape that needs it.
			name: "plain-group-by/no-grouping-call",
			sql:  "SELECT g, COUNT(*) AS n FROM gsdrain GROUP BY g ORDER BY g",
			cols: []string{"g", "n"},
			want: "3 rows: 0|800;1|800;2|800;",
			// Unchanged: the refusal is bounded to shapes that need it.
			partial: true,
		},
	}

	var fired bool
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := exec.ForcedAggDrains.Load()
			beforePartial := exec.AggregatePartialDrains.Load()
			restore := exec.ForceAggDrainEvery(1)
			res, err := tmdRunSingle(ctx, db, tc.sql)
			exec.ForceAggDrainEvery(restore)
			if exec.ForcedAggDrains.Load() > before {
				fired = true
			}
			partialMoved := exec.AggregatePartialDrains.Load() > beforePartial
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			if got := dajDigest(res, tc.cols); got != tc.want {
				t.Errorf("%s\n got: %s\nwant: %s", tc.sql, got, tc.want)
			}
			if partialMoved != tc.partial {
				t.Errorf("%s: partial-merge path engaged = %v, want %v — "+
					"writeMergedRow does not write the GROUPING column, so a shape "+
					"admitted here answers from an unwritten zero", tc.sql, partialMoved, tc.partial)
			}
		})
	}
	if !fired {
		t.Fatal("ForceAggDrainEvery(1) forced no drain on any case: the arm proves nothing")
	}
}
