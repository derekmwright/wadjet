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
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// TestExchangeSubsumeQ21Shape is the e2e regression for exchange
// subsumption dedup: the Q21 EXISTS/NOT-EXISTS pair whose NOT-EXISTS build
// (filtered lineitem shuffle) is subsumed by the EXISTS build's raw shuffle
// of the same table. Both arms (subsume on / kill switch off) must return
// identical rows; ground truth pins the semantics. Multi-chunk data ensures
// every group's rows span several scan/shuffle tasks.
func TestExchangeSubsumeQ21Shape(t *testing.T) {
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
	for _, tbl := range []string{"lineitem", "supplier"} {
		schema := tpch.AllTables[tbl]
		rows := data[tbl]
		if err := cat.CreateTable(ctx, tbl, schema, nil); err != nil {
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
			fp := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tbl, c)
			pd := buf.Bytes()
			store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream")
			cat.AddFiles(ctx, tbl, map[string]string{}, "tables/"+tbl+"/", []catalog.FileEntry{{
				Path: fp, SizeBytes: int64(len(pd)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
			}})
		}
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

	// Ground truth from source rows: suppliers with a "waiting" line
	// (receipt > commit) on a multi-supplier order where no OTHER supplier
	// also has a late line on that order — the Q21 core.
	type line struct {
		order, supp int64
		late        bool
	}
	var lines []line
	for _, r := range data["lineitem"] {
		lines = append(lines, line{
			order: toInt64(r["l_orderkey"]),
			supp:  toInt64(r["l_suppkey"]),
			late:  fmt.Sprint(r["l_receiptdate"]) > fmt.Sprint(r["l_commitdate"]),
		})
	}
	byOrder := map[int64][]line{}
	for _, l := range lines {
		byOrder[l.order] = append(byOrder[l.order], l)
	}
	truthCount := 0
	for _, l := range lines {
		if !l.late {
			continue
		}
		otherSupp, otherLate := false, false
		for _, o := range byOrder[l.order] {
			if o.supp != l.supp {
				otherSupp = true
				if o.late {
					otherLate = true
				}
			}
		}
		if otherSupp && !otherLate {
			truthCount++
		}
	}

	sql := `SELECT COUNT(*) AS numwait
FROM supplier
JOIN lineitem l1 ON s_suppkey = l1.l_suppkey
WHERE l1.l_receiptdate > l1.l_commitdate
AND EXISTS (
  SELECT 1 FROM lineitem l2
  WHERE l2.l_orderkey = l1.l_orderkey AND l2.l_suppkey != l1.l_suppkey
)
AND NOT EXISTS (
  SELECT 1 FROM lineitem l3
  WHERE l3.l_orderkey = l1.l_orderkey AND l3.l_suppkey != l1.l_suppkey
    AND l3.l_receiptdate > l3.l_commitdate
)`

	prev := physical.ExchangeSubsume.Load()
	t.Cleanup(func() { physical.ExchangeSubsume.Store(prev) })
	for _, arm := range []struct {
		name string
		on   bool
	}{
		{"subsume", true},
		{"kill-switch", false},
	} {
		t.Run(arm.name, func(t *testing.T) {
			physical.ExchangeSubsume.Store(arm.on)
			res, err := coord.ExecuteSQL(ctx, sql)
			if err != nil {
				t.Fatalf("ExecuteSQL: %v", err)
			}
			if res.Error != "" {
				t.Fatalf("query error: %s", res.Error)
			}
			rows := mustRows(t, res)
			if len(rows) != 1 {
				t.Fatalf("got %d rows, want 1", len(rows))
			}
			got := toInt64(rows[0]["numwait"])
			if got != int64(truthCount) {
				t.Errorf("numwait = %d, want ground truth %d", got, truthCount)
			}
		})
	}
}
