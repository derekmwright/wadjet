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

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// setupEagerE2E is the shared scaffolding for the eager-dispatch
// end-to-end tests: embedded NATS, MemStore + NATS-KV catalog with the
// requested TPC-H SF001 tables (each split into 3 files so shuffles fan
// out to multiple producer tasks — multiple manifests, staggered arrival),
// and 3 in-process workers. Returns a factory so each arm gets a fresh
// coordinator against the shared cluster.
func setupEagerE2E(t *testing.T, tables []string) (context.Context, func(eager bool) *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	logLevel := slog.LevelWarn
	if os.Getenv("WADJET_TEST_LOG") == "info" {
		logLevel = slog.LevelInfo
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))

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
	for _, table := range tables {
		schema := tpch.AllTables[table]
		rows := data[table]
		if err := cat.CreateTable(ctx, table, schema, nil); err != nil {
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
			fp := fmt.Sprintf("tables/%s/chunk_%04d.parquet", table, c)
			pd := buf.Bytes()
			store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream")
			cat.AddFiles(ctx, table, map[string]string{}, "tables/"+table+"/", []catalog.FileEntry{{
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

	newCoord := func(eager bool) *Coordinator {
		coord := New(Config{
			NATSUrl:           embeddedNATS.ClientURL(),
			ResultBucket:      "test",
			StreamingExchange: true,
			EagerDispatch:     eager,
			SkewSplit:         true, // exercise the early-skew decision path
			// SF001 build sides are tiny — without this the planner
			// broadcasts every join and no exchange-repartition (the
			// eager producer) ever exists. 1 byte forces the shuffle path.
			BroadcastBytesOverride: 1,
		}, cat, nc, js, logger)
		// The planner needs workerCount > 1 to insert exchanges at all
		// (everything collapses to Singleton otherwise).
		for i := 0; i < 3; i++ {
			coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
		}
		return coord
	}
	return ctx, newCoord
}

// runEagerE2EQuery executes sql and returns a full-row multiset (row
// content → count): eager and barrier arms must agree on exact row content
// including duplicates.
func runEagerE2EQuery(ctx context.Context, t *testing.T, coord *Coordinator, sql string, label string) map[string]int {
	t.Helper()
	res, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if res.Error != "" {
		t.Fatalf("%s: %s", label, res.Error)
	}
	rows := mustRows(t, res)
	out := make(map[string]int, len(rows))
	for _, r := range rows {
		cols := make([]string, 0, len(r))
		for k := range r {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		var row string
		for _, k := range cols {
			row += fmt.Sprintf("%s=%v|", k, r[k])
		}
		out[row]++
	}
	return out
}

func assertEagerArmsIdentical(t *testing.T, control, treatment map[string]int) {
	t.Helper()
	if len(treatment) != len(control) {
		t.Fatalf("distinct rows: eager=%d control=%d", len(treatment), len(control))
	}
	for k, n := range control {
		if treatment[k] != n {
			t.Fatalf("row %q: eager ×%d != control ×%d", k, treatment[k], n)
		}
	}
}

// TestEagerDispatchE2E runs the Phase C1 eager path end-to-end (real NATS,
// 3 in-process workers): TPC-H Q02 is the one TPC-H plan carrying the C1
// edge — the decorrelated MIN(ps_supplycost) subquery's final_aggregate
// consumes a standalone exchange-repartition AND feeds a join (not the
// gather, so it isn't gather-fused; fusion disables the task retry that
// fencing needs). Asserts flag-on results are identical to flag-off and
// that the mechanism markers engaged (memo §8).
func TestEagerDispatchE2E(t *testing.T) {
	// SF001 producers complete in milliseconds, so the projected-tail
	// gate (eager-consumer-dispatch.md §10/§11) would keep the barrier
	// and the engagement assertions below could never fire. Disable the
	// floor: this test gates clearance MECHANISM correctness, not the
	// SF100 conversion policy (TestEagerTailGate covers the gate).
	restoreTail := eagerMinTailSeconds
	eagerMinTailSeconds = 0
	t.Cleanup(func() { eagerMinTailSeconds = restoreTail })

	ctx, newCoord := setupEagerE2E(t, []string{"part", "supplier", "partsupp", "nation", "region"})
	sql := tpch.TPCHQueries[2].SQL

	control := runEagerE2EQuery(ctx, t, newCoord(false), sql, "control")
	if len(control) == 0 {
		t.Fatal("control returned no rows")
	}
	edgesBefore := EagerEdgesPlanned.Load()
	manifestsBefore := EagerManifestsPublished.Load()

	// execute_stage_dag.go races each eager-capable dependency's real
	// completion (done[depID]) against its task dispatch (f.dispatched) --
	// real work, so dispatch almost always wins. SF001 producers finish in
	// single-digit milliseconds, so under CPU contention (e.g. a parallel
	// `go test -p 4 ./internal/...` load) the consumer's own
	// per-dependency loop can be scheduled late enough that BOTH channels
	// are already closed by the time it checks; Go's select then picks
	// between two ready cases at random, so the eager path can lose even
	// though nothing was actually wrong (#298: "eager engaged: edges=0
	// manifests=0" reproduced under exactly this load).
	//
	// Holding EVERY stage's completion open (tried first) does not fix
	// this: it also delays how long the consumer's own loop takes to walk
	// its OTHER, non-eager dependencies before reaching the one that
	// matters, so the producer gets the same extra time back and the race
	// just moves, not resolves (confirmed empirically -- even a 1s
	// across-the-board hold still lost the race under sustained
	// contention). Gating on f.dispatched's own readiness targets only
	// stages that are actually eager-feed producers (dispatch always
	// happens before done for those, so the hook safely reads it after
	// the fact) and leaves every other stage -- including the ones the
	// consumer's loop visits first -- at full speed, so only the specific
	// race this test cares about gets pushed, not the whole DAG's timing.
	eagerDispatchStageCompletionHook = func(c *Coordinator, queryID, stageID string) {
		f := c.eagerFeedHandle(queryID, stageID)
		if f == nil {
			return
		}
		select {
		case <-f.dispatched:
		default:
			return // not an eager-feed producer stage -- nothing races here
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Cleanup(func() { eagerDispatchStageCompletionHook = nil })
	treatment := runEagerE2EQuery(ctx, t, newCoord(true), sql, "eager")

	assertEagerArmsIdentical(t, control, treatment)
	if got := EagerManifestsPublished.Load() - manifestsBefore; got == 0 {
		t.Error("eager run published no manifests — producer wiring did not engage")
	}
	if got := EagerEdgesPlanned.Load() - edgesBefore; got == 0 {
		t.Error("eager run cleared no consumer early — clearance did not engage")
	}
	t.Logf("C1 eager engaged: edges=%d manifests=%d, %d distinct rows identical",
		EagerEdgesPlanned.Load()-edgesBefore, EagerManifestsPublished.Load()-manifestsBefore, len(control))
}

// TestEagerJoinDispatchE2E runs the Phase C2 slice-1 join path: a shuffled
// hash join (both sides standalone exchange-repartitions) whose consumer
// clears at the early-skew decision point and feeds BOTH the probe stream
// and the hash-table build from producer-task manifests, published in
// governed waves (join numTasks = partition count ≫ cluster capacity).
// SF001 data is uniform, so the projected decision is no-split and eager
// clearance proceeds; results must be row-identical to the barrier arm.
func TestEagerJoinDispatchE2E(t *testing.T) {
	// SF001 producers complete in milliseconds, so the projected-tail
	// gate (eager-consumer-dispatch.md §10/§11) would keep the barrier
	// and the engagement assertions below could never fire. Disable the
	// floor: this test gates clearance MECHANISM correctness, not the
	// SF100 conversion policy (TestEagerTailGate covers the gate).
	restoreTail := eagerMinTailSeconds
	eagerMinTailSeconds = 0
	t.Cleanup(func() { eagerMinTailSeconds = restoreTail })

	ctx, newCoord := setupEagerE2E(t, []string{"lineitem", "orders"})
	const sql = `SELECT l_orderkey, COUNT(*) AS cnt, SUM(l_quantity) AS qty
		FROM lineitem JOIN orders ON l_orderkey = o_orderkey
		GROUP BY l_orderkey`

	control := runEagerE2EQuery(ctx, t, newCoord(false), sql, "control")
	if len(control) == 0 {
		t.Fatal("control returned no rows")
	}
	edgesBefore := EagerEdgesPlanned.Load()
	treatment := runEagerE2EQuery(ctx, t, newCoord(true), sql, "eager")

	assertEagerArmsIdentical(t, control, treatment)
	if got := EagerEdgesPlanned.Load() - edgesBefore; got == 0 {
		t.Error("eager join run cleared no consumer early — C2 clearance did not engage")
	}
	t.Logf("C2 join eager engaged: edges=%d, %d groups identical",
		EagerEdgesPlanned.Load()-edgesBefore, len(control))
}

// TestEagerChainedJoinDispatchE2E runs the A2 fused-chain path: a
// three-table join whose two shuffled hash joins fuse into one chained
// stage (stage-chain fusion, #269), which then clears dispatch eagerly on
// its primary probe/build feeds while the chained build dep completes at
// the barrier. Results must be row-identical to the barrier arm.
func TestEagerChainedJoinDispatchE2E(t *testing.T) {
	restoreTail := eagerMinTailSeconds
	eagerMinTailSeconds = 0
	t.Cleanup(func() { eagerMinTailSeconds = restoreTail })

	// Pin fusion ON so the chain exists regardless of environment.
	restoreFusion := physical.StageFusion.Load()
	physical.StageFusion.Store(true)
	t.Cleanup(func() { physical.StageFusion.Store(restoreFusion) })

	ctx, newCoord := setupEagerE2E(t, []string{"lineitem", "orders"})
	// Q18's chain shape at test scale: both joins key on l_orderkey /
	// o_orderkey, so the decorrelated semi-join consumes the first join as
	// its probe with the identical hash partitioning — the 1:1 link
	// fuseStageChains absorbs (threshold lowered so SF001 returns rows).
	const sql = `SELECT o_orderkey, SUM(l_quantity) AS qty
		FROM lineitem
		JOIN orders ON l_orderkey = o_orderkey
		WHERE l_orderkey IN (SELECT l_orderkey FROM lineitem
		     GROUP BY l_orderkey HAVING SUM(l_quantity) > 5)
		GROUP BY o_orderkey`

	control := runEagerE2EQuery(ctx, t, newCoord(false), sql, "control")
	if len(control) == 0 {
		t.Fatal("control returned no rows")
	}
	edgesBefore := EagerEdgesPlanned.Load()
	chainedBefore := EagerChainedEdgesPlanned.Load()
	treatment := runEagerE2EQuery(ctx, t, newCoord(true), sql, "eager-chained")

	assertEagerArmsIdentical(t, control, treatment)
	if got := EagerChainedEdgesPlanned.Load() - chainedBefore; got == 0 {
		t.Error("eager chained run cleared no fused-chain consumer — A2 clearance did not engage")
	}
	t.Logf("chained eager engaged: edges=%d chained=%d, %d groups identical",
		EagerEdgesPlanned.Load()-edgesBefore,
		EagerChainedEdgesPlanned.Load()-chainedBefore, len(control))
}

// TestEagerComputeProducerE2E runs the A3 path: with stage-chain fusion
// pinned OFF, the Q18-shaped plan keeps join-4 → join-9 as separate
// stages, so join-9's probe is a hash-partitioned COMPUTE producer
// (join-4) and its build another (final_aggregate-6). Both back eager
// feeds whose manifests carry plain per-task .wshf files — the consumer's
// manifest source maps them to partitions by producer-task dispatch
// ordinal. The v1 eager slot serializes chained clearances, so the test
// widens it to let the cascade engage deterministically. Results must be
// row-identical to the barrier arm.
func TestEagerComputeProducerE2E(t *testing.T) {
	restoreTail := eagerMinTailSeconds
	eagerMinTailSeconds = 0
	t.Cleanup(func() { eagerMinTailSeconds = restoreTail })

	restoreFusion := physical.StageFusion.Load()
	physical.StageFusion.Store(false)
	t.Cleanup(func() { physical.StageFusion.Store(restoreFusion) })

	ctx, newCoord := setupEagerE2E(t, []string{"lineitem", "orders"})
	const sql = `SELECT o_orderkey, SUM(l_quantity) AS qty
		FROM lineitem
		JOIN orders ON l_orderkey = o_orderkey
		WHERE l_orderkey IN (SELECT l_orderkey FROM lineitem
		     GROUP BY l_orderkey HAVING SUM(l_quantity) > 5)
		GROUP BY o_orderkey`

	control := runEagerE2EQuery(ctx, t, newCoord(false), sql, "control")
	if len(control) == 0 {
		t.Fatal("control returned no rows")
	}
	edgesBefore := EagerEdgesPlanned.Load()
	eagerCoord := newCoord(true)
	eagerCoord.eagerStageSlot = make(chan struct{}, 2)
	treatment := runEagerE2EQuery(ctx, t, eagerCoord, sql, "eager-compute")

	assertEagerArmsIdentical(t, control, treatment)
	// join-4 (C2 over two repartitions) AND join-9 (A3 over two compute
	// producers) must both clear — join-9 clearing proves manifest-fed
	// consumption of plain compute outputs end-to-end.
	if got := EagerEdgesPlanned.Load() - edgesBefore; got < 2 {
		t.Errorf("expected >=2 eager clearances (C2 join-4 + A3 join-9), got %d", got)
	}
	t.Logf("compute-producer eager engaged: edges=%d, %d groups identical",
		EagerEdgesPlanned.Load()-edgesBefore, len(control))
}

// TestEagerTailGate: with the projected-tail floor in force (set high so
// SF001's millisecond producers can never satisfy it), an eager-flagged
// run must keep the barrier on every edge — zero early clearances — and
// stay row-identical. This is the §11 gate contract: below-floor tails
// degrade to exactly the flag-off plan, never to a wrong clearance.
func TestEagerTailGate(t *testing.T) {
	ctx, newCoord := setupEagerE2E(t, []string{"part", "supplier", "partsupp", "nation", "region"})
	sql := tpch.TPCHQueries[2].SQL

	restoreTail := eagerMinTailSeconds
	eagerMinTailSeconds = 1e9
	t.Cleanup(func() { eagerMinTailSeconds = restoreTail })

	control := runEagerE2EQuery(ctx, t, newCoord(false), sql, "control")
	if len(control) == 0 {
		t.Fatal("control returned no rows")
	}
	edgesBefore := EagerEdgesPlanned.Load()
	manifestsBefore := EagerManifestsPublished.Load()
	treatment := runEagerE2EQuery(ctx, t, newCoord(true), sql, "eager-gated")

	assertEagerArmsIdentical(t, control, treatment)
	if got := EagerEdgesPlanned.Load() - edgesBefore; got != 0 {
		t.Errorf("gate floor in force but %d consumers cleared early — gate not applied", got)
	}
	// Clearance-driven activation (§13 precondition 2): with every
	// consumer declined, no feed activates, so the flag-on run must
	// publish ZERO manifests — the always-on NATS fan-out §12 charged to
	// the flag no longer exists without a cleared consumer.
	if got := EagerManifestsPublished.Load() - manifestsBefore; got != 0 {
		t.Errorf("no consumer cleared but %d manifests were published — activation gating not applied", got)
	}
}
