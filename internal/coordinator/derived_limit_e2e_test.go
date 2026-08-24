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

// A LIMIT that is not the query's top-level one must still bound the stream
// on the DAG.
//
// Before #478 nothing applied it. The coordinator's post-gather pass reads
// logical.ExtractMergeInfo, which inspects the PLAN ROOT and nothing below
// it, and walkStages handed a bound only to a sort stage — so a LIMIT inside
// a derived table with no ORDER BY under it landed on no stage at all. The
// derived table yielded every row and the outer aggregate counted them:
// `SELECT COUNT(*) FROM (SELECT DISTINCT n_regionkey FROM nation LIMIT 2) u`
// answered 5, its plain twin 25, and neither errored.
//
// The sorted shape was half-broken in a way the issue did not name: the sort
// stage truncates to limit+OFFSET, because skipping the OFFSET is the
// coordinator's job in the top-level shape it was written for. One level
// down nothing skipped, so `ORDER BY n LIMIT 3 OFFSET 5` inside a derived
// table answered 8.
//
// Expectations read off PostgreSQL 17 over the same 25-row nation.
func TestDerivedLimitBoundsDistributedResult(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	t.Cleanup(cancel)
	coord := derivedLimitCluster(t, ctx)

	for _, tt := range []struct {
		name string
		sql  string
		want int64
	}{
		// The four shapes in #478's own repro table.
		{"distinct_limit", `SELECT COUNT(*) AS c FROM (SELECT DISTINCT n_regionkey FROM nation LIMIT 2) u`, 2},
		{"plain_limit", `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 3) u`, 3},
		{"plain_limit_offset", `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 3 OFFSET 5) u`, 3},
		{"group_by_limit", `SELECT COUNT(*) AS c FROM (SELECT n_regionkey FROM nation GROUP BY n_regionkey LIMIT 2) u`, 2},
		// An ORDER BY inside the derived table masked the bug — the sort
		// stage carried the bound. It masked it only for OFFSET 0.
		{"order_by_limit", `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) u`, 3},
		{"order_by_limit_offset", `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5) u`, 3},
		// LIMIT 0 is a bound, not an absence (#481's convention), and an
		// OFFSET alone bounds nothing below but still has to skip.
		{"limit_zero", `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation LIMIT 0) u`, 0},
		{"offset_alone", `SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation OFFSET 20) u`, 5},
		// The bounded derived table as a JOIN input and as a WINDOW input:
		// two consumers that are not an aggregate, so neither is covered by
		// the shapes above. nation keys 0..2 all match a region key.
		{"limit_feeds_join",
			`SELECT COUNT(*) AS c FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3) u
				JOIN region r ON u.n_nationkey = r.r_regionkey`, 3},
		{"limit_feeds_window",
			`SELECT MAX(rn) AS c FROM (SELECT n_nationkey, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn
				FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 4) v) w`, 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, err := tmdRunDAG(ctx, coord, tt.sql)
			if err != nil {
				t.Fatalf("%s: %v", tt.sql, err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s: got %d rows, want 1 (scalar aggregate)", tt.sql, len(res.Rows))
			}
			got, ok := numericCell(res.Rows[0]["c"])
			if !ok {
				t.Fatalf("%s: column c is %#v, not a number", tt.sql, res.Rows[0]["c"])
			}
			if got != tt.want {
				t.Errorf("%s\n  = %v, want %v (PostgreSQL 17)", tt.sql, got, tt.want)
			}
		})
	}

	// The OFFSET has to skip the right rows, not just the right COUNT of
	// them: a stage that truncated to limit+offset and never skipped
	// answered with the first page again (#337's shape, one level down).
	t.Run("order_by_limit_offset_values", func(t *testing.T) {
		const sql = `SELECT n_nationkey FROM (SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5) u
			ORDER BY n_nationkey`
		res, err := tmdRunDAG(ctx, coord, sql)
		if err != nil {
			t.Fatalf("%v", err)
		}
		want := []int64{5, 6, 7}
		if len(res.Rows) != len(want) {
			t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
		}
		for i, w := range want {
			got, ok := numericCell(res.Rows[i]["n_nationkey"])
			if !ok || got != w {
				t.Errorf("row %d = %#v, want %v (PostgreSQL 17 returns 5,6,7)", i, res.Rows[i]["n_nationkey"], w)
			}
		}
	})

	// The top-level shapes the coordinator's post-gather pass already owned.
	// They must not double-apply now that a stage can carry a bound too —
	// an OFFSET applied twice skips 2n rows.
	t.Run("top_level_limit_offset_unchanged", func(t *testing.T) {
		res, err := tmdRunDAG(ctx, coord, `SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5`)
		if err != nil {
			t.Fatalf("%v", err)
		}
		want := []int64{5, 6, 7}
		if len(res.Rows) != len(want) {
			t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
		}
		for i, w := range want {
			got, ok := numericCell(res.Rows[i]["n_nationkey"])
			if !ok || got != w {
				t.Errorf("row %d = %#v, want %v", i, res.Rows[i]["n_nationkey"], w)
			}
		}
	})
}

// derivedLimitCluster stands up embedded NATS + MemStore + NATS-KV catalog +
// three workers over a multi-chunk SF0.01 fixture, and a coordinator with
// LocalFastPathBytes=0 so every query takes the stage DAG. Several chunks per
// table so the scan really fans out — a single-task plan could bound a result
// by accident.
func derivedLimitCluster(t *testing.T, ctx context.Context) *Coordinator {
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
	for _, tbl := range []string{"nation", "region"} {
		schema := tpch.AllTables[tbl]
		rows := data[tbl]
		if err := cat.CreateTable(ctx, tbl, schema, nil); err != nil {
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
			pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatal(err)
			}
			if err := pw.WriteRows(rows[lo:hi]); err != nil {
				t.Fatal(err)
			}
			if err := pw.Close(); err != nil {
				t.Fatal(err)
			}
			fp := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tbl, c)
			pd := buf.Bytes()
			if _, err := store.Put(ctx, "test", fp, bytes.NewReader(pd), int64(len(pd)), "application/octet-stream"); err != nil {
				t.Fatal(err)
			}
			if err := cat.AddFiles(ctx, tbl, map[string]string{}, "tables/"+tbl+"/", []catalog.FileEntry{{
				Path: fp, SizeBytes: int64(len(pd)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
			}}); err != nil {
				t.Fatal(err)
			}
		}
	}

	for i := 0; i < 3; i++ {
		w := worker.New(worker.Config{
			NATSUrl: embeddedNATS.ClientURL(), MaxConcurrent: 4, CacheBytes: 64 << 20, SpillDir: t.TempDir(),
		}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
	}
	coord := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	// 0 disables the small-query fast path outright, which is what forces
	// every query onto the stage DAG regardless of how small the scan is.
	// The fast path applies a derived LIMIT correctly — that is what hid
	// this from every hand test.
	coord.config.LocalFastPathBytes = 0
	for i := 0; i < 3; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{WorkerID: fmt.Sprintf("fake-%d", i), Timestamp: time.Now()})
	}
	return coord
}
