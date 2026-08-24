package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// TestOrderByLimitZeroBoundsDistributedResult is the distributed/stage-DAG
// regression test for #481: `ORDER BY ... LIMIT 0` returned every row
// instead of zero. The root cause was a sentinel collision — exec.Sort.Limit,
// sortSourceAdapter's Top-K guard, physical.Stage.Limit, MergeInfo.KeepRows,
// and the OpSpec.SortLimit wire field all used 0 to mean "no limit",
// colliding with a real `LIMIT 0`. Mirrors
// TestBareLimitBoundsDistributedResult's scaffolding (same fixture, same
// forced-DAG setup via LocalFastPathBytes = 0) — the local fast path
// already applied a real LIMIT 0 correctly, which is exactly what hid this
// on the distributed path.
func TestOrderByLimitZeroBoundsDistributedResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("nats: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)
	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("js: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("streams: %v", err)
	}
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("cat init: %v", err)
	}

	// Several chunks so the scan really fans out across tasks and the sort
	// stage's Top-K compaction actually runs across worker-produced runs —
	// a single-task plan could bound the result by accident.
	data := tpch.Generate(tpch.SF001)
	schema := tpch.AllTables["orders"]
	rows := data["orders"]
	if err := cat.CreateTable(ctx, "orders", schema, nil); err != nil {
		t.Fatal(err)
	}
	const chunks = 4
	per := (len(rows) + chunks - 1) / chunks
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, c*per+per
		if hi > len(rows) {
			hi = len(rows)
		}
		if lo >= hi {
			break
		}
		var buf bytes.Buffer
		pw, _ := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
		if err := pw.WriteRows(rows[lo:hi]); err != nil {
			t.Fatal(err)
		}
		pw.Close()
		fp := fmt.Sprintf("tables/orders/chunk_%04d.parquet", c)
		pd := buf.Bytes()
		store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream")
		cat.AddFiles(ctx, "orders", map[string]string{}, "tables/orders/", []catalog.FileEntry{{
			Path: fp, SizeBytes: int64(len(pd)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
		}})
	}
	if len(rows) < 50 {
		t.Fatalf("fixture has only %d rows; the shapes below would not be meaningful", len(rows))
	}

	for i := 0; i < 3; i++ {
		w := worker.New(worker.Config{NATSUrl: embeddedNATS.ClientURL(), MaxConcurrent: 4, CacheBytes: 64 << 20}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
	}
	coord := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	// Force the DAG: the local fast path already applies a real LIMIT 0
	// correctly, which is exactly what hid this bug in every hand test on a
	// small table.
	coord.config.LocalFastPathBytes = 0
	for i := 0; i < 3; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
	}

	for _, tt := range []struct {
		name string
		sql  string
	}{
		{"order by limit zero", "SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 0"},
		{"order by desc limit zero", "SELECT o_orderkey FROM orders ORDER BY o_orderkey DESC LIMIT 0"},
		{"order by limit zero with filter", "SELECT o_orderkey FROM orders WHERE o_orderstatus = 'O' ORDER BY o_orderkey LIMIT 0"},
		{"order by limit zero offset n", "SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 0 OFFSET 5"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, err := coord.ExecuteSQL(ctx, tt.sql)
			if err != nil {
				t.Fatalf("%s: %v", tt.sql, err)
			}
			defer res.Close()
			if res.TotalRows != 0 {
				t.Errorf("%s: TotalRows = %d, want 0", tt.sql, res.TotalRows)
			}
			var got int64
			for _, b := range res.Batches {
				got += int64(b.ActiveLen())
			}
			if got != 0 {
				t.Errorf("%s: materialized %d rows, want 0", tt.sql, got)
			}
		})
	}

	// A real, positive LIMIT over the same ORDER BY must still bound
	// correctly — guards against an off-by-one in the fix.
	t.Run("order by limit five still bounds", func(t *testing.T) {
		res, err := coord.ExecuteSQL(ctx, "SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 5")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer res.Close()
		if res.TotalRows != 5 {
			t.Errorf("TotalRows = %d, want 5", res.TotalRows)
		}
	})

	// A derived table's own `ORDER BY ... LIMIT 0` produces a sort stage
	// (unlike a bare, no-ORDER-BY derived-table LIMIT, which #478 tracks
	// separately as unstaged on the DAG entirely) — walkStages attaches
	// the same Limit/HasLimit bound to that stage this fix carries for a
	// top-level ORDER BY, so the outer COUNT(*) must see zero input rows.
	t.Run("derived table order by limit zero bounds the outer count", func(t *testing.T) {
		res, err := coord.ExecuteSQL(ctx, "SELECT COUNT(*) AS c FROM (SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 0) u")
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer res.Close()
		if res.TotalRows != 1 {
			t.Fatalf("TotalRows = %d, want 1 (scalar COUNT)", res.TotalRows)
		}
		var got int64
		for _, b := range res.Batches {
			for i := 0; i < b.ActiveLen(); i++ {
				row := i
				if b.Sel != nil {
					row = int(b.Sel[i])
				}
				got = b.Columns[0].Int64Data[row]
			}
		}
		if got != 0 {
			t.Errorf("COUNT(*) over a derived table's ORDER BY ... LIMIT 0 = %d, want 0", got)
		}
	})
}
