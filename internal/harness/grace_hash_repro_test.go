package harness

import (
	"context"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/wadjet"
)

// TestGraceHashJoinReproStandalone exercises the exact data shape used by
// the harness's micro_grace_hash_join (500K build × 50K probe → 500K out)
// against a single-process wadjet.DB, with no pgx / harness cluster /
// embedded NATS in the loop. If THIS hangs, the bug is in Wadjet itself.
// If THIS completes quickly, the harness's cluster/coordinator setup is
// what is timing out.
//
// Skip-gated by env so it doesn't run on every package test invocation:
// generating the 500K-row table takes ~250 ms and the join itself is meant
// to push memory pressure.
func TestGraceHashJoinReproStandalone(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping heavy join repro under -short")
	}

	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "g"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()

	micros := generateMicroData()
	for _, name := range []string{"micro_build", "micro_probe"} {
		schema := microSchemas[name]
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		rows := micros[name].rows
		ing := db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1,
			RowGroupSize:  max(1000, len(rows)/4),
		})
		t0 := time.Now()
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", name, err)
		}
		t.Logf("ingest %s: %d rows in %v", name, len(rows), time.Since(t0))
	}

	queryCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	t0 := time.Now()
	res, err := db.Query(queryCtx, `SELECT b.build_key, b.build_val, p.probe_val
FROM micro_build b JOIN micro_probe p ON b.build_key = p.probe_key`)
	if err != nil {
		t.Fatalf("query (after %v): %v", time.Since(t0), err)
	}
	t.Logf("query: %d rows in %v", len(res.Rows), time.Since(t0))
	if len(res.Rows) == 0 {
		t.Fatal("expected non-zero rows")
	}
}
