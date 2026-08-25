package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The stage-DAG half of #550.
//
// The single-process arm of this gate lives in
// wadjet.TestOuterJoinOverASpilledBuildAnswersLikeAResidentOne. The DAG is a
// different driver for the same operator: the worker drains a spilled probe
// through drainFlushableOps rather than through physical.joinFlushSource, and
// the memory budget that forces eviction is the WORKER's shared pool rather
// than a per-query one. Both used to reach the same nil dereference, because
// both end at HashJoinProbe.FlushUnmatched walking an arena whose entries
// point at h.buildBatches slots the eviction nil'd.
//
// The comparison is DAG vs single process over the same fixture, and the
// single-process arm runs UNBUDGETED so it is also the resident reference: an
// agreement here is "the spilled distributed answer equals the resident
// in-process one", not "two spilled paths agree with each other".

const (
	ojdBuild = "ojd_build"
	ojdProbe = "ojd_probe"
	ojdRows  = 40000
	// Coprime with the partition count so matched rows land in resident and
	// evicted partitions alike; every seventh build key is NULL.
	ojdProbeEvery = 977
	ojdNullEvery  = 7
	// A payload wide enough that the projected build side (id, kk, pad) does
	// not fit the worker's 4 MiB shared pool: ~40000 x 112 B is over 4 MiB,
	// so the grace build has to evict. Narrower and the fixture stays
	// resident and this gate silently tests nothing, which is what the
	// eviction-counter assertion below refuses to allow.
	ojdPad = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" +
		"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)

func ojdBuildSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "kk", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		// The payload that makes the build too big for the worker pool.
		{Name: "pad", Type: parquet.TypeString},
	}}
}

func ojdProbeSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "pid", Type: parquet.TypeInt64},
		{Name: "pk", Type: parquet.TypeInt64, Nullable: true},
		{Name: "ps", Type: parquet.TypeString, Nullable: true},
	}}
}

func ojdBuildData() []map[string]any {
	rows := make([]map[string]any, ojdRows)
	for i := range rows {
		r := map[string]any{
			"id":  int64(i),
			"kk":  int64(i),
			"s":   fmt.Sprintf("key-%08d", i),
			"pad": ojdPad,
		}
		if i%ojdNullEvery == 0 {
			r["k"] = nil
		} else {
			r["k"] = int64(i)
		}
		rows[i] = r
	}
	return rows
}

func ojdProbeData() []map[string]any {
	var rows []map[string]any
	for i := 0; i < ojdRows; i += ojdProbeEvery {
		rows = append(rows, map[string]any{
			"pid": int64(i), "pk": int64(i), "ps": fmt.Sprintf("key-%08d", i),
		})
	}
	return rows
}

func TestOuterJoinOverASpilledBuildTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	single := ojdStandalone(t, ctx)
	coord := ojdCluster(t, ctx)

	for _, q := range []struct {
		name string
		sql  string
	}{
		{"right_int_key", fmt.Sprintf(
			"SELECT b.id AS id, b.pad AS pad, p.pid AS pid FROM %s p RIGHT JOIN %s b ON b.kk = p.pk ORDER BY id", ojdProbe, ojdBuild)},
		{"right_nullable_int_key", fmt.Sprintf(
			"SELECT b.id AS id, b.pad AS pad, p.pid AS pid FROM %s p RIGHT JOIN %s b ON b.k = p.pk ORDER BY id", ojdProbe, ojdBuild)},
		{"full_outer_int_key", fmt.Sprintf(
			"SELECT b.id AS id, b.pad AS pad, p.pid AS pid FROM %s p FULL OUTER JOIN %s b ON b.kk = p.pk ORDER BY id, pid", ojdProbe, ojdBuild)},
		{"right_string_key", fmt.Sprintf(
			"SELECT b.id AS id, b.pad AS pad, p.pid AS pid FROM %s p RIGHT JOIN %s b ON b.s = p.ps ORDER BY id", ojdProbe, ojdBuild)},
		// The LEFT control: its build side is the one a distributed plan
		// REPLICATES, where flushing unmatched build rows is unsound (#348).
		{"left_control", fmt.Sprintf(
			"SELECT p.pid AS pid, b.id AS id FROM %s p LEFT JOIN %s b ON b.kk = p.pk ORDER BY pid", ojdProbe, ojdBuild)},
	} {
		t.Run(q.name, func(t *testing.T) {
			want, err := tmdRunSingle(ctx, single, q.sql)
			if err != nil {
				t.Fatalf("single-process engine refused %q: %v", q.sql, err)
			}
			evictedBefore := exec.JoinPartitionsEvicted.Load()
			got, err := tmdRunDAG(ctx, coord, q.sql)
			if err != nil {
				t.Fatalf("stage DAG refused %q: %v", q.sql, err)
			}
			evicted := exec.JoinPartitionsEvicted.Load() - evictedBefore
			if evicted == 0 {
				t.Fatalf("no grace partition was evicted on the DAG arm over %d build rows — this "+
					"gate is not exercising the spilled build it exists for", ojdRows)
			}
			if len(got.Rows) != len(want.Rows) {
				t.Fatalf("DAG returned %d rows, single process %d (%d partitions evicted)",
					len(got.Rows), len(want.Rows), evicted)
			}
			for i := range want.Rows {
				a, b := want.Rows[i], got.Rows[i]
				for _, c := range []string{"id", "pid", "pad"} {
					if _, ok := a[c]; !ok {
						continue
					}
					if fmt.Sprintf("%v", a[c]) != fmt.Sprintf("%v", b[c]) {
						t.Fatalf("row %d column %q: DAG %v, single process %v (%d partitions evicted)",
							i, c, b[c], a[c], evicted)
					}
				}
			}
		})
	}
}

// ojdStandalone is the reference arm: the embedded engine with NO memory
// budget, so its build stays resident and never partitions on arrival.
func ojdStandalone(t *testing.T, ctx context.Context) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{ojdBuild, ojdBuildSchema(), ojdBuildData()},
		{ojdProbe, ojdProbeSchema(), ojdProbeData()},
	} {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 4096,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// ojdCluster stands up the DAG arm: one coordinator with the small-query fast
// path disabled and ONE worker on a shared pool small enough that the build
// has to evict partitions to fit.
//
// One worker, not three, and that is the point rather than a shortcut: with
// the build hash-shuffled across three, each worker holds a third of it and
// the same pool no longer forces an eviction — the gate would pass without
// ever reaching the path it exists for. One worker holds the whole build, so
// the budget decides deterministically. The eviction-counter assertion in the
// test is what keeps that honest if the planner's placement changes.
func ojdCluster(t *testing.T, ctx context.Context) *Coordinator {
	t.Helper()
	infra := tmdInfra(t, ctx)

	const chunks = 4
	for _, tbl := range []struct {
		name   string
		schema parquet.Schema
		rows   []map[string]any
	}{
		{ojdBuild, ojdBuildSchema(), ojdBuildData()},
		{ojdProbe, ojdProbeSchema(), ojdProbeData()},
	} {
		if err := infra.cat.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		n := len(tbl.rows)
		per := (n + chunks - 1) / chunks
		var entries []catalog.FileEntry
		for c := 0; c < chunks; c++ {
			lo, hi := c*per, min(c*per+per, n)
			if lo >= hi {
				break
			}
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, tbl.schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatalf("parquet writer %s: %v", tbl.name, err)
			}
			if err := pw.WriteRows(tbl.rows[lo:hi]); err != nil {
				t.Fatalf("write %s: %v", tbl.name, err)
			}
			if err := pw.Close(); err != nil {
				t.Fatalf("close %s: %v", tbl.name, err)
			}
			path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tbl.name, c)
			payload := buf.Bytes()
			if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(payload),
				int64(len(payload)), "application/octet-stream"); err != nil {
				t.Fatalf("put %s: %v", path, err)
			}
			entries = append(entries, catalog.FileEntry{
				Path: path, SizeBytes: int64(len(payload)),
				NumRows: int64(hi - lo), CreatedAt: time.Now(),
			})
		}
		if err := infra.cat.AddFiles(ctx, tbl.name, map[string]string{},
			"tables/"+tbl.name+"/", entries); err != nil {
			t.Fatalf("add files %s: %v", tbl.name, err)
		}
	}

	coord := New(Config{
		NATSUrl: infra.clientURL, ResultBucket: "test",
		LocalFastPathBytes: 0, // every query on the stage DAG
	}, infra.cat, infra.nc, infra.js, infra.logger)

	const workers = 1
	ids := make([]string, workers)
	for i := range ids {
		ids[i] = fmt.Sprintf("ojd-worker-%d", i)
		w := worker.New(worker.Config{
			WorkerID: ids[i], NATSUrl: infra.clientURL,
			MaxConcurrent: 1, CacheBytes: 8 << 20, SpillDir: t.TempDir(),
			// The whole point: a pool this small cannot hold the build, so
			// the grace partitioning evicts.
			MemoryBudget: 4 << 20, SharedPoolBudget: 4 << 20,
		}, infra.store, infra.nc, infra.js, infra.logger)
		wctx, wcancel := context.WithCancel(context.Background())
		t.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			t.Fatalf("worker start: %v", err)
		}
		t.Cleanup(w.Stop)
	}

	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, id := range ids {
			hb, err := distributed.Marshal(distributed.WorkerHeartbeat{
				WorkerID: id, MaxConcurrent: 1, Timestamp: time.Now(),
			})
			if err != nil {
				t.Fatalf("marshal heartbeat: %v", err)
			}
			if err := infra.nc.Publish(distributed.SubjectHeartbeat, hb); err != nil {
				t.Fatalf("publish heartbeat: %v", err)
			}
		}
		if err := infra.nc.Flush(); err != nil {
			t.Fatalf("nats flush: %v", err)
		}
		if coord.Workers().Count() >= workers {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workers never registered: %d, want %d", coord.Workers().Count(), workers)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return coord
}
