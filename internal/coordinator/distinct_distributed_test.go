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

// TestDistributedDistinctDedup is the regression for #163: distributed
// SELECT DISTINCT must deduplicate across scan tasks. Before the fix the
// native-DAG path passed NodeDistinct through with no dedup stage, so
// SELECT DISTINCT returned every input row. Ground truth is computed from the
// source rows. Data is split into multiple files so several scan tasks each
// see overlapping values — the condition a missing cross-task dedup needs.
func TestDistributedDistinctDedup(t *testing.T) {
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

	data := tpch.Generate(tpch.SF001)
	schema := tpch.AllTables["lineitem"]
	rows := data["lineitem"]
	if err := cat.CreateTable(ctx, "lineitem", schema, nil); err != nil {
		t.Fatal(err)
	}
	const chunks = 3
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
		fp := fmt.Sprintf("tables/lineitem/chunk_%04d.parquet", c)
		pd := buf.Bytes()
		store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream")
		cat.AddFiles(ctx, "lineitem", map[string]string{}, "tables/lineitem/", []catalog.FileEntry{{
			Path: fp, SizeBytes: int64(len(pd)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
		}})
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
	for i := 0; i < 3; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
	}

	// Ground truth from source rows.
	rf := map[string]struct{}{}
	rfls := map[string]struct{}{}
	for _, r := range rows {
		a := fmt.Sprint(r["l_returnflag"])
		rf[a] = struct{}{}
		rfls[a+"|"+fmt.Sprint(r["l_linestatus"])] = struct{}{}
	}

	cases := []struct {
		sql  string
		want int
	}{
		{"SELECT DISTINCT l_returnflag FROM lineitem", len(rf)},
		{"SELECT DISTINCT l_returnflag, l_linestatus FROM lineitem", len(rfls)},
		// DISTINCT + ORDER BY: dedup must run before the sort is re-applied.
		{"SELECT DISTINCT l_returnflag FROM lineitem ORDER BY l_returnflag", len(rf)},
		// Control: the aggregate path was already correct.
		{"SELECT l_returnflag, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag", len(rf)},
		// #166: aggregate-free GROUP BY must dedup across scan tasks.
		// Before the fix the fused scan (group cols, zero agg specs)
		// dispatched as a plain scan — no partial dedup, no hash-partition
		// exchange — so the N final tasks re-emitted every group (3×3=9).
		{"SELECT l_returnflag FROM lineitem GROUP BY l_returnflag", len(rf)},
		{"SELECT l_returnflag, l_linestatus FROM lineitem GROUP BY l_returnflag, l_linestatus", len(rfls)},
		// Aliased bare column still takes the sharded GROUP BY rewrite.
		{"SELECT DISTINCT l_returnflag AS flag FROM lineitem", len(rf)},
		// DISTINCT over an expression is NOT rewritten — it exercises the
		// coordinator-side dedup fallback (MergeInfo.HasDistinct).
		{"SELECT DISTINCT lower(l_returnflag) AS flag FROM lineitem", len(rf)},
		// Control for the expression case: the equivalent explicit GROUP BY.
		{"SELECT lower(l_returnflag) AS flag FROM lineitem GROUP BY lower(l_returnflag)", len(rf)},
	}
	for _, tc := range cases {
		res, err := coord.ExecuteSQL(ctx, tc.sql)
		if err != nil {
			t.Fatalf("%s: %v", tc.sql, err)
		}
		if res.Error != "" {
			t.Fatalf("%s: %s", tc.sql, res.Error)
		}
		got := len(mustRows(t, res))
		if got != tc.want {
			t.Errorf("%s: got %d rows, want %d", tc.sql, got, tc.want)
		} else {
			t.Logf("%s: %d rows OK", tc.sql, got)
		}
	}
}
