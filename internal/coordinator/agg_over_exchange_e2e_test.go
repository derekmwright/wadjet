package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/planner/physical"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// TestAggOverExchangeQ18Shape is the e2e regression for the
// aggregate-over-exchange rewrite: Q18's IN + HAVING subquery re-scans
// lineitem as a fused scan-agg while the join leg already shuffles the raw
// table on l_orderkey — the rewrite feeds the subquery's final_aggregate
// from the raw exchange's partitions (raw mode, exact per-partition groups)
// and drops the second scan. Both arms (rewrite on / kill switch off) must
// return identical rows; ground truth pins the semantics. Multi-chunk data
// ensures every order's lines span several scan/shuffle tasks.
func TestAggOverExchangeQ18Shape(t *testing.T) {
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
	for _, tbl := range []string{"customer", "orders", "lineitem"} {
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

	// Ground truth from source rows: total quantity per order; orders over
	// the HAVING threshold survive. The threshold is picked below the SF001
	// maximum so the qualifying set is non-empty — a 0-row pass would prove
	// nothing.
	qtyByOrder := map[int64]int64{}
	for _, r := range data["lineitem"] {
		qtyByOrder[toInt64(r["l_orderkey"])] += toInt64(r["l_quantity"])
	}
	const havingThreshold = 250
	truth := map[int64]int64{} // qualifying o_orderkey -> total qty
	for ok, q := range qtyByOrder {
		if q > havingThreshold {
			truth[ok] = q
		}
	}
	if len(truth) == 0 {
		t.Fatalf("ground truth empty at threshold %d — lower it so the test exercises real rows", havingThreshold)
	}
	if len(truth) > 90 {
		t.Fatalf("ground truth has %d orders; LIMIT 100 would truncate — raise the threshold", len(truth))
	}

	sql := fmt.Sprintf(`SELECT
  c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
  SUM(l_quantity) as total_qty
FROM customer
JOIN orders ON c_custkey = o_custkey
JOIN lineitem ON o_orderkey = l_orderkey
WHERE o_orderkey IN (
  SELECT l_orderkey
  FROM lineitem
  GROUP BY l_orderkey
  HAVING SUM(l_quantity) > %d
)
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100`, havingThreshold)

	prev := physical.AggOverExchange.Load()
	t.Cleanup(func() { physical.AggOverExchange.Store(prev) })
	for _, arm := range []struct {
		name string
		on   bool
	}{
		{"agg-over-exchange", true},
		{"kill-switch", false},
	} {
		t.Run(arm.name, func(t *testing.T) {
			physical.AggOverExchange.Store(arm.on)
			res, err := coord.ExecuteSQL(ctx, sql)
			if err != nil {
				t.Fatalf("ExecuteSQL: %v", err)
			}
			if res.Error != "" {
				t.Fatalf("query error: %s", res.Error)
			}
			rows := mustRows(t, res)
			if len(rows) != len(truth) {
				t.Fatalf("got %d rows, want %d", len(rows), len(truth))
			}
			for _, r := range rows {
				ok := toInt64(r["o_orderkey"])
				want, present := truth[ok]
				if !present {
					t.Errorf("order %d in result but not in ground truth", ok)
					continue
				}
				if got := toInt64(r["total_qty"]); got != want {
					t.Errorf("order %d total_qty = %d, want %d", ok, got, want)
				}
			}
		})
	}
}
