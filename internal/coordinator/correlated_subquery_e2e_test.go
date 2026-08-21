package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// A per-row correlated subquery must never answer wrongly on a distributed
// coordinator (#359).
//
// The stage DAG has no distributed algorithm for a correlation that survives
// decorrelation (a NON-EQUI correlation — equality correlations are rewritten
// into joins and never reach execution per row). A worker compiles its
// fragment's filter with no SubqueryRunner, so before the fix a correlated
// EXISTS failed the task outright while a correlated SCALAR was mis-deferred
// to a producer stage whose dangling outer reference evaluated to NULL: the
// query answered 0, silently, on a distributed deployment and correctly
// single-process.
//
// The fix: PlanDistributed REFUSES the shape with a typed error
// (physical.ErrCorrelatedSubqueryDistributed) and the coordinator routes the
// refused query onto its local single-process pipeline — the engine that owns
// correlated-subquery semantics — regardless of the fast-path byte threshold.
// A local failure is reported to the client; there is no DAG to fall back to.
//
// Every subtest here runs against a DAG-forced coordinator
// (LocalFastPathBytes=0) and asserts the ABSOLUTE answer recomputed from the
// rows loaded, so "0 rows" can never pass.
func TestCorrelatedSubqueryDistributedCoordinator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	t.Cleanup(cancel)

	coord, custRows := setupCorrelatedCluster(t, ctx)

	// Reference implementations over the loaded rows. NULL semantics matter:
	// an empty inner set makes AVG NULL and every comparison against it
	// UNKNOWN, so nationkey 0 contributes nothing to the scalar counts.
	nkey := func(r map[string]any) float64 { return float64(r["c_nationkey"].(int32)) }
	bal := func(r map[string]any) float64 { return r["c_acctbal"].(float64) }
	avgBelow := func(k float64) (float64, bool) {
		sum, n := 0.0, 0
		for _, o := range custRows {
			if nkey(o) < k {
				sum += bal(o)
				n++
			}
		}
		if n == 0 {
			return 0, false
		}
		return sum / float64(n), true
	}

	scalarWant := 0
	for _, r := range custRows {
		if avg, ok := avgBelow(nkey(r)); ok && bal(r) > avg {
			scalarWant++
		}
	}
	existsWant, notExistsWant := 0, 0
	for _, r := range custRows {
		found := false
		for _, o := range custRows {
			if nkey(o) < nkey(r) && bal(o) > 9000 {
				found = true
				break
			}
		}
		if found {
			existsWant++
		} else {
			notExistsWant++
		}
	}
	// Nested two-deep: c1 is bound by the outermost query and read inside the
	// inner-inner SELECT.
	nestedWant := 0
	for _, r := range custRows {
		threshold, ok := avgBelow(nkey(r))
		if !ok {
			continue
		}
		sum, n := 0.0, 0
		for _, o := range custRows {
			if bal(o) > threshold {
				sum += bal(o)
				n++
			}
		}
		if n > 0 && bal(r) > sum/float64(n) {
			nestedWant++
		}
	}
	if scalarWant == 0 || existsWant == 0 || notExistsWant == 0 || nestedWant == 0 {
		t.Fatalf("fixture produced degenerate expectations (scalar=%d exists=%d notExists=%d nested=%d) — the assertions below would not distinguish the bug",
			scalarWant, existsWant, notExistsWant, nestedWant)
	}

	countCases := []struct {
		name string
		sql  string
		want int
	}{
		{"ScalarInWhere", `SELECT COUNT(*) AS n FROM customer c1
			WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
				WHERE c2.c_nationkey < c1.c_nationkey)`, scalarWant},
		{"Exists", `SELECT COUNT(*) AS n FROM customer c1
			WHERE EXISTS (SELECT 1 FROM customer c2
				WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`, existsWant},
		{"NotExists", `SELECT COUNT(*) AS n FROM customer c1
			WHERE NOT EXISTS (SELECT 1 FROM customer c2
				WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`, notExistsWant},
		{"NestedTwoDeep", `SELECT COUNT(*) AS n FROM customer c1
			WHERE c1.c_acctbal > (SELECT AVG(c2.c_acctbal) FROM customer c2
				WHERE c2.c_acctbal > (SELECT AVG(c3.c_acctbal) FROM customer c3
					WHERE c3.c_nationkey < c1.c_nationkey))`, nestedWant},
	}
	for _, tc := range countCases {
		t.Run(tc.name, func(t *testing.T) {
			rows := runToRows(t, ctx, coord, tc.sql)
			if len(rows) != 1 {
				t.Fatalf("%d rows, want 1", len(rows))
			}
			got := toInt(t, rows[0]["n"])
			if got != int64(tc.want) {
				t.Errorf("n = %d, want %d (recomputed over the %d loaded rows)\n  SQL: %s",
					got, tc.want, len(custRows), tc.sql)
			}
		})
	}

	t.Run("ScalarInSelectList", func(t *testing.T) {
		rows := runToRows(t, ctx, coord, `SELECT c_custkey, c_nationkey,
			(SELECT AVG(c_acctbal) FROM customer c2
				WHERE c2.c_nationkey < c1.c_nationkey) AS below_avg
			FROM customer c1 WHERE c1.c_custkey <= 25 ORDER BY c_custkey`)
		if len(rows) != 25 {
			t.Fatalf("%d rows, want 25", len(rows))
		}
		for i, r := range rows {
			v, present := r["below_avg"]
			if !present {
				t.Fatalf("row %d carries no below_avg column — the correlated projection was dropped silently: %v", i, r)
			}
			k := toInt(t, r["c_nationkey"])
			want, ok := avgBelow(float64(k))
			if !ok {
				if v != nil {
					t.Errorf("row %d (nationkey %d): below_avg = %v, want NULL (empty inner set)", i, k, v)
				}
				continue
			}
			if v == nil {
				t.Errorf("row %d (nationkey %d): below_avg = NULL, want %v", i, k, want)
				continue
			}
			got := toFloat64(t, v)
			if math.Abs(got-want) > 1e-6*math.Max(1, math.Abs(want)) {
				t.Errorf("row %d (nationkey %d): below_avg = %v, want %v", i, k, got, want)
			}
		}
	})

	// The routing proof: every query above must have been answered by the
	// coordinator-local pipeline via the typed refusal — NOT by the fast
	// path (disabled here) and NOT by the DAG (which cannot run the shape).
	// An exact count also proves the refusal fires ONLY for these shapes.
	t.Run("RoutedViaRefusal", func(t *testing.T) {
		if n := coord.LocalFastPathHits(); n != 0 {
			t.Errorf("local fast path served %d queries — LocalFastPathBytes=0 must keep it off", n)
		}
		if n := coord.CorrelatedLocalRoutes(); n != 5 {
			t.Errorf("correlated-local routes = %d, want 5 (one per query above)", n)
		}
	})

	// A control that must KEEP running on the DAG: an uncorrelated scalar
	// subquery defers to a producer stage and must not trip the refusal.
	t.Run("UncorrelatedScalarStaysDistributed", func(t *testing.T) {
		before := coord.CorrelatedLocalRoutes()
		rows := runToRows(t, ctx, coord, `SELECT COUNT(*) AS n FROM customer c1
			WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2)`)
		if len(rows) != 1 {
			t.Fatalf("%d rows, want 1", len(rows))
		}
		sum, cnt := 0.0, 0
		for _, r := range custRows {
			sum += bal(r)
			cnt++
		}
		avg := sum / float64(cnt)
		want := 0
		for _, r := range custRows {
			if bal(r) > avg {
				want++
			}
		}
		if got := toInt(t, rows[0]["n"]); got != int64(want) {
			t.Errorf("n = %d, want %d", got, want)
		}
		if coord.CorrelatedLocalRoutes() != before {
			t.Errorf("uncorrelated scalar subquery tripped the correlated refusal — it must stay on the DAG's producer-stage path")
		}
	})

	// Past the local result budget the query must FAIL LOUDLY: there is no
	// DAG plan to fall back to, and the old behavior — a silent 0 — is the
	// defect. The error names the budget and the remedy.
	t.Run("OverBudgetFailsLoudly", func(t *testing.T) {
		coord.localResultBudgetOverride = 1
		t.Cleanup(func() { coord.localResultBudgetOverride = 0 })
		_, err := coord.ExecuteSQL(ctx, `SELECT c_custkey,
			(SELECT AVG(c_acctbal) FROM customer c2
				WHERE c2.c_nationkey < c1.c_nationkey) AS below_avg
			FROM customer c1`)
		if err == nil {
			t.Fatal("over-budget correlated query returned a result; it must fail loudly")
		}
		if !strings.Contains(err.Error(), "exceeded the local budget") {
			t.Errorf("error does not name the budget: %v", err)
		}
	})
}

// setupCorrelatedCluster stands up an embedded-NATS cluster (3 workers, one
// DAG-forced coordinator) with the first 300 SF0.01 customers split over 3
// parquet chunks, and returns the coordinator plus the loaded rows.
func setupCorrelatedCluster(t *testing.T, ctx context.Context) (*Coordinator, []map[string]any) {
	t.Helper()
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
	schema := tpch.AllTables["customer"]
	rows := data["customer"]
	if len(rows) > 300 {
		rows = rows[:300]
	}
	if err := cat.CreateTable(ctx, "customer", schema, nil); err != nil {
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
		fp := fmt.Sprintf("tables/customer/chunk_%04d.parquet", c)
		pd := buf.Bytes()
		store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream")
		cat.AddFiles(ctx, "customer", map[string]string{}, "tables/customer/", []catalog.FileEntry{{
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
	// Force the DAG: with the fast path enabled this table routes local and
	// the defect never fires — exactly how it stayed hidden.
	coord.config.LocalFastPathBytes = 0
	for i := 0; i < 3; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
	}
	return coord, rows
}

// runToRows executes sql and boxes the columnar result into row maps.
func runToRows(t *testing.T, ctx context.Context, coord *Coordinator, sql string) []map[string]any {
	t.Helper()
	res, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("ExecuteSQL: %v\n  SQL: %s", err, sql)
	}
	defer res.Close()
	var rows []map[string]any
	for _, b := range res.Batches {
		rows = append(rows, batchRows(b)...)
	}
	return rows
}

func batchRows(b *batch.RecordBatch) []map[string]any {
	if b == nil {
		return nil
	}
	n := b.ActiveLen()
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		row := i
		if b.Sel != nil {
			row = int(b.Sel[i])
		}
		m := make(map[string]any, len(b.Schema))
		for ci, col := range b.Schema {
			m[col.Name] = b.Columns[ci].GetValue(row)
		}
		rows = append(rows, m)
	}
	return rows
}

func toInt(t *testing.T, v any) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int32:
		return int64(n)
	case int:
		return int64(n)
	case float64:
		return int64(n)
	}
	t.Fatalf("value %v (%T) is not numeric", v, v)
	return 0
}

func toFloat64(t *testing.T, v any) float64 {
	t.Helper()
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case string:
		// The single-process pipeline types a computed correlated projection
		// as a string column (exec.Project's undecided-type default) — a
		// pre-existing arm-A behavior the routing reproduces faithfully. The
		// VALUE is what this test asserts.
		f, err := strconv.ParseFloat(n, 64)
		if err == nil {
			return f
		}
	}
	t.Fatalf("value %v (%T) is not numeric", v, v)
	return 0
}
