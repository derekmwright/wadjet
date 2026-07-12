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
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// TestEagerDispatchE2E runs the Phase C1 eager path end-to-end (real NATS,
// 3 in-process workers): a GROUP BY over a shuffled join plans to the one
// non-join repartition edge C1 targets (standalone exchange-repartition →
// final_aggregate, the Q02 shape), so with the flag on the aggregate stage
// clears dispatch on the shuffle's feed and consumes manifests while the
// shuffle drains. Asserts flag-on results are identical to flag-off and
// that the mechanism markers (EagerEdgesPlanned, EagerManifestsPublished)
// engaged — the same markers the SF100 pair will be judged on (memo §8).
func TestEagerDispatchE2E(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

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

	// Q02's tables, each split into 3 files so shuffles fan out to
	// multiple producer tasks (multiple manifests, staggered arrival —
	// the condition the manifest feed exists for).
	data := tpch.Generate(tpch.SF001)
	for _, table := range []string{"part", "supplier", "partsupp", "nation", "region"} {
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

	// TPC-H Q02 is the one query whose plan carries the C1 edge: the
	// decorrelated MIN(ps_supplycost) subquery's final_aggregate consumes
	// a standalone exchange-repartition AND feeds a join (not the gather,
	// so it isn't gather-fused — fusion disables task retry, which fencing
	// needs, so eligibility excludes fused stages). Simple GROUP BY
	// queries don't exercise C1: their final aggregate either fuses into
	// the gather or (with ORDER BY folded in) loses the exchange.
	sql := tpch.TPCHQueries[2].SQL

	run := func(eager bool) map[string]int {
		coord := New(Config{
			NATSUrl:           embeddedNATS.ClientURL(),
			ResultBucket:      "test",
			StreamingExchange: true,
			EagerDispatch:     eager,
			// SF001 build sides are tiny — without this the planner
			// broadcasts every join and no exchange-repartition (the C1
			// producer) ever exists. 1 byte forces the shuffle path.
			BroadcastBytesOverride: 1,
		}, cat, nc, js, logger)
		// The planner needs workerCount > 1 to insert exchanges at all
		// (everything collapses to Singleton otherwise) — same fake-
		// heartbeat bootstrap the other distributed e2e tests use.
		for i := 0; i < 3; i++ {
			coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
		}
		res, err := coord.ExecuteSQL(ctx, sql)
		if err != nil {
			t.Fatalf("eager=%v: %v", eager, err)
		}
		if res.Error != "" {
			t.Fatalf("eager=%v: %s", eager, res.Error)
		}
		rows := mustRows(t, res)
		// Full-row multiset: Q02 has no single-column key, and eager vs
		// barrier must agree on exact row content including duplicates.
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

	control := run(false)
	if len(control) == 0 {
		t.Fatal("control returned no rows")
	}

	edgesBefore := EagerEdgesPlanned.Load()
	manifestsBefore := EagerManifestsPublished.Load()
	treatment := run(true)

	if len(treatment) != len(control) {
		t.Fatalf("distinct rows: eager=%d control=%d", len(treatment), len(control))
	}
	for k, n := range control {
		if treatment[k] != n {
			t.Fatalf("row %q: eager ×%d != control ×%d", k, treatment[k], n)
		}
	}
	if got := EagerManifestsPublished.Load() - manifestsBefore; got == 0 {
		t.Error("eager run published no manifests — producer wiring did not engage")
	}
	if got := EagerEdgesPlanned.Load() - edgesBefore; got == 0 {
		t.Error("eager run cleared no consumer early — clearance did not engage")
	}
	t.Logf("eager engaged: edges=%d manifests=%d, %d groups row-identical",
		EagerEdgesPlanned.Load()-edgesBefore, EagerManifestsPublished.Load()-manifestsBefore, len(control))
}
