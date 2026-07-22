package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
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

// TestShardedSortLimitFinals is the regression for sharded sort/limit
// finals (the Q10/Q13 single-task final-aggregate tail): a grouped
// final_aggregate under ORDER BY + LIMIT fans out across group-key shards,
// each shard sorts and limits its own disjoint groups, and the surviving
// Singleton sort merges the per-shard top-K lists. Both arms (sharded and
// the WADJET_SHARDED_FINALS=0 Singleton collapse) must return exactly the
// ground-truth top-K rows, in order. Data is split into multiple files so
// every group's partials span several scan tasks — the condition a broken
// cross-shard merge or a non-exact shard aggregate would need to diverge.
func TestShardedSortLimitFinals(t *testing.T) {
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

	// Ground truth: SUM(l_quantity) per l_suppkey, top-K by (sum DESC,
	// suppkey ASC). l_suppkey at SF0.01 has ~100 distinct values spread
	// across every chunk, so shard finals genuinely merge partials from
	// all three scan tasks.
	sums := map[int64]float64{}
	for _, r := range rows {
		k := toInt64(r["l_suppkey"])
		sums[k] += sslToFloat64(r["l_quantity"])
	}
	type kv2 struct {
		key int64
		sum float64
	}
	truth := make([]kv2, 0, len(sums))
	for k, s := range sums {
		truth = append(truth, kv2{k, s})
	}
	sort.Slice(truth, func(i, j int) bool {
		if truth[i].sum != truth[j].sum {
			return truth[i].sum > truth[j].sum
		}
		return truth[i].key < truth[j].key
	})
	const limit = 7
	if len(truth) <= limit {
		t.Fatalf("need > %d groups for a meaningful top-K, got %d", limit, len(truth))
	}
	want := truth[:limit]

	sql := fmt.Sprintf(`SELECT l_suppkey, SUM(l_quantity) AS total
FROM lineitem GROUP BY l_suppkey
ORDER BY total DESC, l_suppkey LIMIT %d`, limit)

	prev := physical.ShardedSortFinals.Load()
	t.Cleanup(func() { physical.ShardedSortFinals.Store(prev) })
	for _, arm := range []struct {
		name    string
		sharded bool
	}{
		{"sharded", true},
		{"singleton-collapse", false},
	} {
		t.Run(arm.name, func(t *testing.T) {
			physical.ShardedSortFinals.Store(arm.sharded)
			res, err := coord.ExecuteSQL(ctx, sql)
			if err != nil {
				t.Fatalf("ExecuteSQL: %v", err)
			}
			if res.Error != "" {
				t.Fatalf("query error: %s", res.Error)
			}
			got := mustRows(t, res)
			if len(got) != limit {
				t.Fatalf("got %d rows, want %d", len(got), limit)
			}
			for i, r := range got {
				k := toInt64(r["l_suppkey"])
				s := sslToFloat64(r["total"])
				if k != want[i].key || !floatClose(s, want[i].sum) {
					t.Errorf("row %d: got (%d, %v), want (%d, %v)", i, k, s, want[i].key, want[i].sum)
				}
			}
		})
	}
}

func sslToFloat64(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	default:
		return 0
	}
}

func floatClose(a, b float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= 1e-6*(1+absF(a)+absF(b))
}

func absF(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}
