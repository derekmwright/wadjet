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

// A LIMIT with no ORDER BY must bound the result on the distributed path.
//
// walkStages attaches a stage limit only to sort/merge_sort, so `ORDER BY x
// LIMIT n` was bounded while a bare `LIMIT n` landed on no stage at all and
// every row came back: `SELECT n_name FROM nation LIMIT 3` returned all 25.
// It stayed hidden because small scans take the local fast path (which applies
// the limit correctly) and because every TPC-H query with a LIMIT also has an
// ORDER BY — so the benchmark suite only ever exercised the working shape.
//
// A client felt it as a hang: DataGrip opening a 15M-row table read all of it,
// 425 seconds, for the 501 rows it asked for.
func TestBareLimitBoundsDistributedResult(t *testing.T) {
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

	// Several chunks so the scan really fans out across tasks — a single-task
	// plan could bound the result by accident.
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
		t.Fatalf("fixture has only %d rows; the limits below would not bind", len(rows))
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
	// Force the DAG: the local fast path applies limits correctly, which is
	// what masked this in every hand test on a small table.
	coord.config.LocalFastPathBytes = 0
	for i := 0; i < 3; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
	}

	for _, tt := range []struct {
		name  string
		sql   string
		limit int64
	}{
		{"bare limit", "SELECT o_orderkey FROM orders LIMIT 5", 5},
		{"limit with filter", "SELECT o_orderkey FROM orders WHERE o_orderstatus = 'O' LIMIT 3", 3},
		{"limit with order by", "SELECT o_orderkey FROM orders ORDER BY o_orderkey LIMIT 4", 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, err := coord.ExecuteSQL(ctx, tt.sql)
			if err != nil {
				t.Fatalf("%s: %v", tt.sql, err)
			}
			defer res.Close()
			if res.TotalRows > tt.limit {
				t.Errorf("%s returned %d rows, want at most %d", tt.sql, res.TotalRows, tt.limit)
			}
			var got int64
			for _, b := range res.Batches {
				got += int64(b.ActiveLen())
			}
			if got > tt.limit {
				t.Errorf("%s materialized %d rows, want at most %d", tt.sql, got, tt.limit)
			}
			if got == 0 {
				t.Errorf("%s returned nothing; the limit must bound, not empty, the result", tt.sql)
			}
		})
	}
}
