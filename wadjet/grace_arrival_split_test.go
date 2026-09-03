package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The end-to-end arm of #598, reached the way production reaches it.
//
// The scan hands a join build one batch per parquet ROW GROUP, not one per
// batch.DefaultBatchSize, so a fat row group is a fat arrival batch — no exec
// harness is needed to build one bigger than the pool. Below the fix, a build
// whose first arrival batch does not fit the budget failed outright
// (`used=0, requested=…`) even though the same rows in smaller batches build,
// so the query a user could run and the query they could not differed only in
// how the file was written.
//
// The answer is asserted, not just the absence of an error: MIN(z.pad) reads a
// value out of the BUILD side, so a split that dropped a chunk, replayed one
// twice or stitched a chunk out of two rows changes it.
func wideJoinDB(t *testing.T, rows int, padLen int, rowGroup int, budget int64) *DB {
	t.Helper()
	ctx := context.Background()
	cfg := Config{Store: objstore.NewMemStore(), Bucket: "test", MemoryBudget: budget}
	if budget > 0 {
		cfg.SpillDir = t.TempDir()
	}
	db, err := Open(ctx, cfg)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "pad", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "wide", schema, nil); err != nil {
		t.Fatalf("create wide: %v", err)
	}
	data := make([]map[string]any, rows)
	for i := range data {
		// Distinct per row so MIN(pad) has exactly one right answer and a
		// dropped or duplicated chunk cannot hide behind a shared value.
		data[i] = map[string]any{
			"k":   int64(i),
			"pad": fmt.Sprintf("%08d", i) + strings.Repeat("x", padLen-8),
		}
	}
	ing := db.NewIngester("wide", schema, nil, ingest.Config{
		MaxBufferRows: rows + 1, RowGroupSize: rowGroup,
	})
	if err := ing.Ingest(ctx, data); err != nil {
		t.Fatalf("ingest wide: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush wide: %v", err)
	}
	return db
}

const wideJoinSQL = `SELECT COUNT(*) AS n, MIN(z.pad) AS m FROM wide x JOIN wide z ON x.k = z.k`

func TestGraceBuildAnswersWhenOneRowGroupExceedsTheBudget(t *testing.T) {
	const rows, padLen = 4000, 512
	ctx := context.Background()

	// The reference: the same rows, the same query, no budget.
	want, err := tmRun(ctx, wideJoinDB(t, rows, padLen, rows, 0), wideJoinSQL)
	if err != nil {
		t.Fatalf("unbudgeted: %v", err)
	}
	wantMin := want.Rows[0]["m"]

	// One row group holding every row: the build's FIRST arrival batch is
	// ~2.3 MB against a 2 MiB budget, which is #598 exactly.
	db := wideJoinDB(t, rows, padLen, rows, 2<<20)
	for run := 0; run < 5; run++ {
		got, err := tmRun(ctx, db, wideJoinSQL)
		if err != nil {
			t.Fatalf("run %d: the join refused a build whose ONE arrival batch is bigger than the "+
				"pool, though the same rows in smaller row groups build: %v", run, err)
		}
		if n := got.Rows[0]["n"]; fmt.Sprint(n) != fmt.Sprint(want.Rows[0]["n"]) {
			t.Fatalf("run %d: COUNT(*)=%v, want %v — the split changed the row set", run, n, want.Rows[0]["n"])
		}
		if m := got.Rows[0]["m"]; fmt.Sprint(m) != fmt.Sprint(wantMin) {
			t.Fatalf("run %d: MIN(z.pad)=%v, want %v — the split lost a build row or stitched one "+
				"out of two", run, m, wantMin)
		}
	}
}
