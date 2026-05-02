//go:build !race

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
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
)

// setupQ17MinDistributed brings up a distributed cluster with SF0.01 data
// chunked into the given number of files. Used to localise whether the
// row-loss bug is per-task (within one scan + partial-aggregate) or
// inter-task (between partial → shuffle → final).
func setupQ17MinDistributed(t *testing.T, chunksPerTable int, numWorkers int) (context.Context, *Coordinator) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("starting NATS: %v", err)
	}
	t.Cleanup(embeddedNATS.Shutdown)

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		t.Fatalf("connecting to NATS: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatalf("creating JetStream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatalf("setting up streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatalf("creating NATS KV: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}

	data := tpch.Generate(tpch.SF001)
	for tableName, schema := range tpch.AllTables {
		if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatal(err)
		}
		rows := data[tableName]
		if len(rows) == 0 {
			continue
		}
		nc := chunksPerTable
		if len(rows) < nc {
			nc = len(rows)
		}
		chunkSize := (len(rows) + nc - 1) / nc
		var entries []catalog.FileEntry
		for cIdx := 0; cIdx < nc; cIdx++ {
			start := cIdx * chunkSize
			end := start + chunkSize
			if end > len(rows) {
				end = len(rows)
			}
			if start >= end {
				break
			}
			chunkRows := rows[start:end]
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatalf("parquet writer: %v", err)
			}
			if err := pw.WriteRows(chunkRows); err != nil {
				t.Fatalf("write: %v", err)
			}
			if err := pw.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tableName, cIdx+1)
			pdata := buf.Bytes()
			if _, err := store.Put(ctx, "test", path, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
				t.Fatal(err)
			}
			entries = append(entries, catalog.FileEntry{
				Path:      path,
				SizeBytes: int64(len(pdata)),
				NumRows:   int64(len(chunkRows)),
				CreatedAt: time.Now(),
			})
		}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", entries); err != nil {
			t.Fatal(err)
		}
	}

	coord := New(Config{NATSUrl: embeddedNATS.ClientURL(), ResultBucket: "test"}, cat, nc, js, logger)
	for i := 0; i < numWorkers; i++ {
		w := worker.New(worker.Config{
			NATSUrl: embeddedNATS.ClientURL(), MaxConcurrent: 4, CacheBytes: 64 * 1024 * 1024,
			SpillDir: t.TempDir(),
		}, store, nc, js, logger)
		if err := w.Start(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(w.Stop)
		time.Sleep(50 * time.Millisecond)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if coord.workers.Count() >= numWorkers {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return ctx, coord
}

// TestQ17MinRepro is the smallest-possible reproducer for the distributed
// GROUP BY row-loss bug. Iterates over (chunks-per-table × workers) to
// localise where the loss kicks in.
//
// Findings from this session (gated on WADJET_Q17_MIN=1):
//
//	          | sum_via_pk | sum_via_sk | sum_via_ok
//	chunks=1, workers=1 | 60000 ✓     | 60000 ✓     | 60000 ✓
//	chunks=1, workers=3 | 60000 ✓     | 60000 ✓     | 57470 ✗
//	chunks=8, workers=1 | 60000 ✓     | 60000 ✓     | 60000 ✓
//	chunks=8, workers=3 | ~56500 ✗   | 60000 ✓     | ~58100 ✗
//
// Bug scales with #distinct keys: l_suppkey has 100 keys (no loss),
// l_partkey has 2000 (~6% loss), l_orderkey has 15000 (~3% loss).
//
// Single-worker is always correct → the loss only happens when the inner
// aggregate stage (`SELECT key, COUNT(*) GROUP BY key`) shuffles partial
// outputs across multiple workers, then the final_aggregate merges. With
// per-stage instrumentation the loss localises to inside the final-stage
// HashAggregate: input rows to the final task carry the full count
// (sumOfC = total lineitem rows in this partition), but the merged
// output's per-group SUM(c) is short. Pre-existing on main, not
// introduced by today's fixes.
//
// What was ruled out:
//   - Scan / shuffle row count: traced and matches (15,609 partial rows
//     in, 15,609 shuffle batches out, 15,609 reaching the final tasks).
//   - In-process HashAggregate with the same merge shape (SUM(c)
//     GROUP BY l_partkey over (key, c) pairs): see
//     internal/engine/exec/agg_merge_repro_test.go — passes with the same
//     2000 keys × 8 partials = 16000 row pattern, even with explicit
//     Workers=4. The loss is specific to the distributed input path
//     (.wshf reader / coordinator-side fan-out / merge-mode rewrite), not
//     the in-process aggregate kernel.
//
// Next-session leads:
//   - Compare batch-by-batch: dump each .wshf chunk's expected vs actual
//     SUM(c) when read by cachedFileStreamSource.
//   - Check whether the rewrite mergeMode path (fn=COUNT→SUM, InputCol=
//     OutputCol "c") interacts oddly with the int-group SoA scatter when
//     batches arrive at final-task batch boundaries < 2048 rows.
//   - Audit whether scatterSumInt skips rows when the `c` column carries
//     a (possibly stale) selection vector inherited from the shuffle
//     write path.
func TestQ17MinRepro(t *testing.T) {
	if os.Getenv("WADJET_Q17_MIN") != "1" {
		t.Skip("set WADJET_Q17_MIN=1 to enable")
	}

	probesSQL := []struct {
		name string
		sql  string
	}{
		{"li_count", `SELECT COUNT(*) AS n FROM lineitem`},
		{"sum_via_pk", `SELECT SUM(c) AS s FROM (SELECT l_partkey, COUNT(*) AS c FROM lineitem GROUP BY l_partkey) sub`},
		{"sum_via_sk", `SELECT SUM(c) AS s FROM (SELECT l_suppkey, COUNT(*) AS c FROM lineitem GROUP BY l_suppkey) sub`},
		{"sum_via_ok", `SELECT SUM(c) AS s FROM (SELECT l_orderkey, COUNT(*) AS c FROM lineitem GROUP BY l_orderkey) sub`},
	}
	cases := []struct {
		chunks  int
		workers int
	}{
		{1, 1},  // single-task: no shuffle parallelism
		{1, 3},  // single-task with 3 workers (still 1 scan task)
		{8, 1},  // 8 chunks, 1 worker — chunks serialize but final shuffles
		{8, 3},  // matches prod path
	}
	for _, c := range cases {
		t.Run(fmt.Sprintf("chunks%d_workers%d", c.chunks, c.workers), func(t *testing.T) {
			ctx, coord := setupQ17MinDistributed(t, c.chunks, c.workers)
			for _, p := range probesSQL {
				res, err := coord.ExecuteSQL(ctx, p.sql)
				if err != nil || res.Error != "" {
					t.Logf("ERR %s: %v %s", p.name, err, res.Error)
					continue
				}
				rows := res.Rows()
				if len(rows) > 0 {
					t.Logf("%s: %v", p.name, rows[0])
				}
			}
		})
	}
}
