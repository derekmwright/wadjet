package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/skew"
	"github.com/derekmwright/wadjet/internal/benchnotify"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Skew-suite mode (docs/design/skew-aware-shuffle.md Phase 3): instead of
// the 22 TPC-H queries, run the hot-key events⋈dims fixture from
// benchmarks/skew. Enabled by --skew-suite or WADJET_SKEW_SUITE=1 (the
// terraform deploy exports the env var, mirroring WADJET_SKEW_SPLIT).
//
// Data staging follows the TPC-H flags: without --skip-load the fixture is
// generated and uploaded under --data-prefix (any stale objects there are
// deleted first); with --skip-load the tables are discovered like TPC-H
// ones. The A/B deploy stages once (GENERATE_DATA=1 on the first arm) and
// discovers on later arms.

// skewSuiteEnabled reports the env-var opt-in used by the terraform deploy.
func skewSuiteEnabled() bool {
	v := os.Getenv("WADJET_SKEW_SUITE")
	return v == "1" || v == "true" || v == "TRUE"
}

// skewDeployConfig returns the deploy fixture config with the same env
// overrides the harness honors, so undersized smoke deploys are possible.
func skewDeployConfig() (skew.Config, error) {
	cfg := skew.DefaultDeployConfig()
	if v := os.Getenv("WADJET_SKEW_EVENTS_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("WADJET_SKEW_EVENTS_ROWS: %w", err)
		}
		cfg.EventsRows = n
	}
	if v := os.Getenv("WADJET_SKEW_DIMS_ROWS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return cfg, fmt.Errorf("WADJET_SKEW_DIMS_ROWS: %w", err)
		}
		cfg.DimsRows = n
		if ks := int64(n) * 4 / 3; cfg.KeySpace <= int64(n) || cfg.KeySpace > ks {
			cfg.KeySpace = ks
		}
		if cfg.HotKey >= int64(n) {
			cfg.HotKey = int64(n) / 2
		}
	}
	return cfg, cfg.Validate()
}

// loadSkewData generates the skew fixture and uploads it under
// bucket/dataPrefix, registering files + row counts in the catalog. Any
// existing objects under the fixture tables' prefixes are deleted first so
// a re-generation can never double the dataset (stale files would silently
// double row counts AND change every parity invariant).
func loadSkewData(ctx context.Context, db *wadjet.DB, endpoint, region, bucket string, ssl bool, dataPrefix string) {
	cfg, err := skewDeployConfig()
	if err != nil {
		fatalf("skew config: %v", err)
	}
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		fatalf("creating S3 store for skew load: %v", err)
	}

	for name := range skew.Tables {
		prefix := dataPrefix + name + "/"
		objects, err := store.List(ctx, bucket, objstore.ListOptions{Prefix: prefix})
		if err != nil {
			fatalf("listing stale %s files: %v", name, err)
		}
		for _, obj := range objects {
			if err := store.Delete(ctx, bucket, obj.Key); err != nil {
				fatalf("deleting stale %s: %v", obj.Key, err)
			}
		}
		if len(objects) > 0 {
			log.Printf("Deleted %d stale objects under %s", len(objects), prefix)
		}
	}

	for name, schema := range skew.Tables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			fatalf("create table %s: %v", name, err)
		}
	}

	log.Printf("Generating skew fixture: events=%d dims=%d hot_pct=%d pad=%dB",
		cfg.EventsRows, cfg.DimsRows, cfg.HotPct, cfg.PadBytes)
	start := time.Now()
	chunkIdx := make(map[string]int)
	entries := make(map[string][]catalog.FileEntry)
	emit := func(table string, rows []map[string]any) error {
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, skew.Tables[table], parquet.DefaultWriterConfig())
		if err != nil {
			return fmt.Errorf("parquet writer for %s: %w", table, err)
		}
		if err := pw.WriteRows(rows); err != nil {
			return fmt.Errorf("writing %s: %w", table, err)
		}
		if err := pw.Close(); err != nil {
			return fmt.Errorf("closing %s: %w", table, err)
		}
		idx := chunkIdx[table]
		chunkIdx[table] = idx + 1
		key := fmt.Sprintf("%s%s/chunk_%04d.parquet", dataPrefix, table, idx+1)
		data := buf.Bytes()
		if _, err := store.Put(ctx, bucket, key, bytes.NewReader(data), int64(len(data)), "application/octet-stream"); err != nil {
			return fmt.Errorf("uploading %s: %w", key, err)
		}
		entries[table] = append(entries[table], catalog.FileEntry{
			Path:      key,
			SizeBytes: int64(len(data)),
			NumRows:   int64(len(rows)),
			CreatedAt: time.Now(),
		})
		return nil
	}
	if err := skew.GenerateChunked(cfg, emit); err != nil {
		fatalf("generating skew fixture: %v", err)
	}
	for table, files := range entries {
		if err := db.Catalog().AddFiles(ctx, table, nil, "", files); err != nil {
			fatalf("registering %s files: %v", table, err)
		}
		var rows, bytesTotal int64
		for _, f := range files {
			rows += f.NumRows
			bytesTotal += f.SizeBytes
		}
		log.Printf("Uploaded %s: %d files, %d rows, %.1f MB", table, len(files), rows, float64(bytesTotal)/(1024*1024))
	}
	log.Printf("Skew fixture staged in %s", time.Since(start).Round(time.Second))

	for name := range skew.Tables {
		analyzed, err := db.Catalog().AnalyzeTable(ctx, name)
		if err != nil {
			log.Printf("WARNING: ANALYZE %s failed (%v); planner falls back to heuristic NDV", name, err)
			continue
		}
		if analyzed > 0 {
			log.Printf("ANALYZE %s: HLL collected on %d files", name, analyzed)
		}
	}
}

// runSkewSuite executes the skew queries through the coordinator, printing
// wall time, row count, a full-row checksum (the cross-arm parity signal),
// and the SkewSplitsPlanned delta (the mechanism marker: wall deltas without
// this counter moving are window drift, not skew-split signal).
func runSkewSuite(ctx context.Context, coord *coordinator.Coordinator, expectedWorkers, runs int, timeout time.Duration) {
	if coord == nil {
		fatalf("--skew-suite requires distributed mode (the mechanism is coordinator task dispatch)")
	}
	for run := 1; run <= runs; run++ {
		log.Printf("=== Skew suite run %d/%d ===", run, runs)
		for _, name := range skew.QueryOrder {
			for retries := 0; retries < 30 && coord.Workers().Count() < expectedWorkers; retries++ {
				if retries == 0 {
					log.Printf("  waiting for workers (%d/%d)...", coord.Workers().Count(), expectedWorkers)
				}
				time.Sleep(2 * time.Second)
			}
			qctx := ctx
			var cancel context.CancelFunc
			if timeout > 0 {
				qctx, cancel = context.WithTimeout(ctx, timeout)
			}
			splitsBefore := coordinator.SkewSplitsPlanned.Load()
			startQ := time.Now()
			result, err := coord.ExecuteSQL(qctx, skew.Queries[name])
			if err == nil && result.Error != "" {
				err = fmt.Errorf("%s", result.Error)
			}
			var rows []map[string]any
			if err == nil {
				rows, err = result.Rows()
			}
			wall := time.Since(startQ)
			splits := coordinator.SkewSplitsPlanned.Load() - splitsBefore
			if cancel != nil {
				cancel()
			}
			if err != nil {
				log.Printf("SKEW RESULT run=%d query=%s wall_ms=%d skew_splits=%d ERROR: %v",
					run, name, wall.Milliseconds(), splits, err)
				notifier.Send(benchnotify.Event{
					Event: benchnotify.EventQueryCompleted, Query: name,
					WallSeconds: benchnotify.Seconds(wall), OK: benchnotify.OK(false),
					RunIndex: run, TotalRuns: runs,
				})
				continue
			}
			rendered := make([]string, len(rows))
			for i, r := range rows {
				rendered[i] = renderRow(r)
			}
			sort.Strings(rendered)
			hash := sha256.New()
			for _, r := range rendered {
				fmt.Fprintln(hash, r)
			}
			log.Printf("SKEW RESULT run=%d query=%s wall_ms=%d rows=%d checksum=%s skew_splits=%d",
				run, name, wall.Milliseconds(), len(rows), hex.EncodeToString(hash.Sum(nil))[:16], splits)
			for _, r := range rendered {
				log.Printf("  row: %s", r)
			}
			notifier.Send(benchnotify.Event{
				Event: benchnotify.EventQueryCompleted, Query: name,
				WallSeconds: benchnotify.Seconds(wall), Rows: benchnotify.Rows(int64(len(rows))),
				OK: benchnotify.OK(true), RunIndex: run, TotalRuns: runs,
			})
		}
		notifier.Send(benchnotify.Event{
			Event: benchnotify.EventRunCompleted, RunIndex: run, TotalRuns: runs,
		})
	}
	notifier.Send(benchnotify.Event{Event: benchnotify.EventSuiteCompleted, TotalRuns: runs})
}

// renderRow renders a result row with sorted column names so checksums are
// stable across map iteration order.
func renderRow(r map[string]any) string {
	cols := make([]string, 0, len(r))
	for c := range r {
		cols = append(cols, c)
	}
	sort.Strings(cols)
	var b bytes.Buffer
	for i, c := range cols {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s=%v", c, r[c])
	}
	return b.String()
}
