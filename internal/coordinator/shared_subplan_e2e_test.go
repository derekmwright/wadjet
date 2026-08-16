package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
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

// TestSharedSubplanDedupE2E is the e2e regression for the shared-subplan
// dedup pass: Q11 (exact clone leg — the scalar subquery re-plans the whole
// partsupp⋈supplier⋈nation pipeline) and Q17 (semi≡inner — the decorrelated
// AVG leg re-joins lineitem⋈part). Both arms (dedup on / kill switch off)
// must return the same rows, pinned against ground truth computed from the
// source data. Multi-chunk files spread every table across several tasks so
// the multi-consumer read of the shared join output is actually exercised.
func TestSharedSubplanDedupE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
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
	for _, tbl := range []string{"partsupp", "supplier", "nation", "lineitem", "part"} {
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

	// ---- Q11 ground truth: German suppliers' partsupp value per part,
	// HAVING value > fraction * total.
	germanSupp := map[int64]bool{}
	var germanyKey int64 = -1
	for _, n := range data["nation"] {
		if fmt.Sprint(n["n_name"]) == "GERMANY" {
			germanyKey = toInt64(n["n_nationkey"])
		}
	}
	if germanyKey < 0 {
		t.Fatal("no GERMANY nation in generated data")
	}
	for _, s := range data["supplier"] {
		if toInt64(s["s_nationkey"]) == germanyKey {
			germanSupp[toInt64(s["s_suppkey"])] = true
		}
	}
	q11Value := map[int64]float64{}
	total := 0.0
	for _, ps := range data["partsupp"] {
		if !germanSupp[toInt64(ps["ps_suppkey"])] {
			continue
		}
		v := toFloat64E2E(ps["ps_supplycost"]) * toFloat64E2E(ps["ps_availqty"])
		q11Value[toInt64(ps["ps_partkey"])] += v
		total += v
	}
	const q11Fraction = 0.0001
	q11Truth := map[int64]float64{}
	for pk, v := range q11Value {
		if v > total*q11Fraction {
			q11Truth[pk] = v
		}
	}
	if len(q11Truth) == 0 {
		t.Fatal("q11 ground truth empty — fixture broken")
	}

	q11SQL := fmt.Sprintf(`SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
FROM partsupp
JOIN supplier ON ps_suppkey = s_suppkey
JOIN nation ON s_nationkey = n_nationkey
WHERE n_name = 'GERMANY'
GROUP BY ps_partkey
HAVING SUM(ps_supplycost * ps_availqty) > (
  SELECT SUM(ps_supplycost * ps_availqty) * %v
  FROM partsupp
  JOIN supplier ON ps_suppkey = s_suppkey
  JOIN nation ON s_nationkey = n_nationkey
  WHERE n_name = 'GERMANY'
)
ORDER BY value DESC`, q11Fraction)

	// ---- Q17: pick the (brand, container) pair with the most parts so the
	// filtered join is guaranteed non-empty at SF0.01.
	type bc struct{ brand, container string }
	bcCount := map[bc]int{}
	for _, p := range data["part"] {
		bcCount[bc{fmt.Sprint(p["p_brand"]), fmt.Sprint(p["p_container"])}]++
	}
	var pick bc
	for k, n := range bcCount {
		if n > bcCount[pick] {
			pick = k
		}
	}
	matchPart := map[int64]bool{}
	for _, p := range data["part"] {
		if fmt.Sprint(p["p_brand"]) == pick.brand && fmt.Sprint(p["p_container"]) == pick.container {
			matchPart[toInt64(p["p_partkey"])] = true
		}
	}
	sumQty := map[int64]float64{}
	cntQty := map[int64]float64{}
	for _, l := range data["lineitem"] {
		pk := toInt64(l["l_partkey"])
		sumQty[pk] += toFloat64E2E(l["l_quantity"])
		cntQty[pk]++
	}
	q17Truth := 0.0
	q17Rows := 0
	for _, l := range data["lineitem"] {
		pk := toInt64(l["l_partkey"])
		if !matchPart[pk] || cntQty[pk] == 0 {
			continue
		}
		if toFloat64E2E(l["l_quantity"]) < 0.2*(sumQty[pk]/cntQty[pk]) {
			q17Truth += toFloat64E2E(l["l_extendedprice"])
			q17Rows++
		}
	}
	if q17Rows == 0 {
		t.Fatalf("q17 ground truth empty for %v — fixture broken", pick)
	}
	q17Truth /= 7.0

	q17SQL := fmt.Sprintf(`SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
FROM lineitem
JOIN part ON p_partkey = l_partkey
WHERE p_brand = '%s' AND p_container = '%s'
AND l_quantity < (
  SELECT 0.2 * AVG(l_quantity) FROM lineitem WHERE l_partkey = p_partkey
)`, pick.brand, pick.container)

	prev := physical.SharedSubplanDedup.Load()
	t.Cleanup(func() { physical.SharedSubplanDedup.Store(prev) })
	for _, arm := range []struct {
		name string
		on   bool
	}{
		{"dedup", true},
		{"kill-switch", false},
	} {
		t.Run(arm.name+"/q11", func(t *testing.T) {
			physical.SharedSubplanDedup.Store(arm.on)
			res, err := coord.ExecuteSQL(ctx, q11SQL)
			if err != nil {
				t.Fatalf("ExecuteSQL: %v", err)
			}
			if res.Error != "" {
				t.Fatalf("query error: %s", res.Error)
			}
			rows := mustRows(t, res)
			if len(rows) != len(q11Truth) {
				t.Fatalf("q11 rows = %d, want %d", len(rows), len(q11Truth))
			}
			var prevVal float64 = math.Inf(1)
			for i, r := range rows {
				pk := toInt64(r["ps_partkey"])
				got := toFloat64E2E(r["value"])
				want, ok := q11Truth[pk]
				if !ok {
					t.Fatalf("row %d: unexpected ps_partkey %d", i, pk)
				}
				if !closeE2E(got, want) {
					t.Errorf("ps_partkey %d: value = %v, want %v", pk, got, want)
				}
				if got > prevVal*(1+1e-9) {
					t.Errorf("row %d: value %v out of DESC order (prev %v)", i, got, prevVal)
				}
				prevVal = got
			}
		})
		t.Run(arm.name+"/q17", func(t *testing.T) {
			physical.SharedSubplanDedup.Store(arm.on)
			res, err := coord.ExecuteSQL(ctx, q17SQL)
			if err != nil {
				t.Fatalf("ExecuteSQL: %v", err)
			}
			if res.Error != "" {
				t.Fatalf("query error: %s", res.Error)
			}
			rows := mustRows(t, res)
			if len(rows) != 1 {
				t.Fatalf("q17 rows = %d, want 1", len(rows))
			}
			got := toFloat64E2E(rows[0]["avg_yearly"])
			if !closeE2E(got, q17Truth) {
				t.Errorf("avg_yearly = %v, want %v", got, q17Truth)
			}
		})
	}
}

func toFloat64E2E(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int64:
		return float64(x)
	case int32:
		return float64(x)
	case int:
		return float64(x)
	}
	var f float64
	fmt.Sscan(fmt.Sprint(v), &f)
	return f
}

func closeE2E(got, want float64) bool {
	if got == want {
		return true
	}
	denom := math.Max(math.Abs(got), math.Abs(want))
	if denom == 0 {
		return true
	}
	return math.Abs(got-want)/denom < 1e-6
}
