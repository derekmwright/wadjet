package coordinator

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// TestShuffleProjectionNarrowsAbsorbedScans is the end-to-end regression
// test for docs/design/exchange-reuse.md §2 A2: a repartition whose
// dependency is a pass-through leaf scan (unfiltered, file count >
// workerCount so fuseScanShuffle skips it) must shuffle only the scan's
// required columns, not the full table width. Before the fix the
// synthetic source stage handed to runShuffleSide carried no column
// list, and Q21/Q18's raw lineitem legs shipped all 16 columns
// (85.69 GB vs ~12 GB needed at SF100).
//
// The assertion reads the WSHF partition headers the shuffle tasks
// actually wrote: no repartition output may carry lineitem's full width.
func TestShuffleProjectionNarrowsAbsorbedScans(t *testing.T) {
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

	// Write each table; lineitem in 5 chunks so its scan stays above the
	// fuseScanShuffle workerCount cap and the repartition absorbs it (the
	// pass-through + dispatchShuffleStage path under test).
	data := tpch.Generate(tpch.SF001)
	for tableName, schema := range tpch.AllTables {
		if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("creating table %s: %v", tableName, err)
		}
		rows := data[tableName]
		if len(rows) == 0 {
			continue
		}
		chunks := 1
		if tableName == "lineitem" {
			chunks = 5
		}
		per := (len(rows) + chunks - 1) / chunks
		for ci := 0; ci < chunks; ci++ {
			lo, hi := ci*per, (ci+1)*per
			if hi > len(rows) {
				hi = len(rows)
			}
			if lo >= hi {
				break
			}
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
			if err != nil {
				t.Fatalf("parquet writer for %s: %v", tableName, err)
			}
			if err := pw.WriteRows(rows[lo:hi]); err != nil {
				t.Fatalf("writing %s rows: %v", tableName, err)
			}
			if err := pw.Close(); err != nil {
				t.Fatalf("closing %s writer: %v", tableName, err)
			}
			filePath := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tableName, ci+1)
			pdata := buf.Bytes()
			if _, err := store.Put(ctx, "test", filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
				t.Fatalf("storing %s parquet: %v", tableName, err)
			}
			if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", []catalog.FileEntry{{
				Path:      filePath,
				SizeBytes: int64(len(pdata)),
				NumRows:   int64(hi - lo),
				CreatedAt: time.Now(),
			}}); err != nil {
				t.Fatalf("adding %s to manifest: %v", tableName, err)
			}
		}
	}

	for i := 0; i < 3; i++ {
		w := worker.New(worker.Config{
			NATSUrl:       embeddedNATS.ClientURL(),
			MaxConcurrent: 4,
			CacheBytes:    64 * 1024 * 1024,
		}, store, nc, js, logger)
		workerCtx, workerCancel := context.WithCancel(context.Background())
		t.Cleanup(workerCancel)
		if err := w.Start(workerCtx); err != nil {
			t.Fatalf("starting worker %d: %v", i, err)
		}
		t.Cleanup(w.Stop)
	}

	coord := New(Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: "test",
		// Force hash joins with real repartition exchanges: at SF0.01
		// every table fits the broadcast threshold and the path under
		// test (scan-absorbed repartition) would never run.
		BroadcastBytesOverride: 1024,
	}, cat, nc, js, logger)
	for i := 0; i < 3; i++ {
		coord.workers.record(distributed.WorkerHeartbeat{
			WorkerID:  fmt.Sprintf("fake-worker-%d", i),
			Timestamp: time.Now(),
		})
	}

	// Q21: two of its lineitem legs shuffle straight from base parquet.
	q := tpch.TPCHQueries[21]
	result, err := coord.ExecuteSQL(ctx, q.SQL)
	if err != nil {
		t.Fatalf("Q21 ExecuteSQL: %v", err)
	}
	if result.Error != "" {
		t.Fatalf("Q21 error: %s", result.Error)
	}
	if rows := mustRows(t, result); len(rows) != 1 {
		t.Fatalf("Q21 rows = %d, want 1 (SF0.01 reference)", len(rows))
	}

	fullWidth := len(tpch.AllTables["lineitem"].Columns)
	infos, err := store.List(ctx, "test", objstore.ListOptions{Prefix: "queries/"})
	if err != nil {
		t.Fatalf("listing scratch: %v", err)
	}
	checked := 0
	seen := map[string]int{}
	for _, info := range infos {
		if i := strings.Index(info.Key, "/"); i > 0 {
			if j := strings.Index(info.Key[i+1:], "/"); j > 0 {
				seen[info.Key[i+1:i+1+j]]++
			}
		}
		if !strings.Contains(info.Key, "exchange-repartition") || !strings.Contains(info.Key, ".wshf") {
			continue
		}
		cols, err := readWSHFColCount(ctx, store, "test", info.Key)
		if err != nil {
			t.Fatalf("reading %s: %v", info.Key, err)
		}
		if cols == 0 {
			continue // empty partition file with no header
		}
		checked++
		if cols >= fullWidth {
			t.Errorf("shuffle partition %s has %d columns (full lineitem width %d) — absorbed-scan projection lost", info.Key, cols, fullWidth)
		}
	}
	if checked == 0 {
		t.Fatalf("no repartition partition files found in scratch — the path under test did not run (plan shape changed or scratch purged?); scratch dirs: %v (total %d keys); keys: %v", seen, len(infos), keyNames(infos))
	}
	t.Logf("checked %d repartition partitions, all narrower than %d columns", checked, fullWidth)
}

func keyNames(infos []objstore.ObjectInfo) []string {
	out := make([]string, 0, len(infos))
	for _, i := range infos {
		out = append(out, i.Key)
	}
	return out
}

// readWSHFColCount parses a WSHF header's NumCols, transparently
// decompressing s2-compressed bodies.
func readWSHFColCount(ctx context.Context, store objstore.Store, bucket, key string) (int, error) {
	rc, _, err := store.Get(ctx, bucket, key)
	if err != nil {
		return 0, err
	}
	defer rc.Close()
	raw, err := io.ReadAll(rc)
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		return 0, nil
	}
	if !bytes.HasPrefix(raw, []byte("WSHF")) {
		if raw, err = worker.DecompressShuffleData(raw); err != nil {
			return 0, fmt.Errorf("neither raw WSHF nor s2: %w", err)
		}
		if !bytes.HasPrefix(raw, []byte("WSHF")) {
			return 0, fmt.Errorf("decompressed body is not WSHF")
		}
	}
	if len(raw) < 10 {
		return 0, fmt.Errorf("truncated WSHF header (%d bytes)", len(raw))
	}
	return int(binary.LittleEndian.Uint16(raw[8:10])), nil
}
