package harness

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
)

// Run is the top-level entry point. Reads cfg, sets up the run dir,
// starts the cluster (local mode only), runs the query suite, compares
// against the baseline, writes result.json, and returns RunResult.
func Run(ctx context.Context, cfg Config, logger *slog.Logger) (RunResult, error) {
	started := time.Now()
	result := RunResult{
		Mode:         cfg.Mode,
		Slice:        cfg.Slice,
		StartedAt:    started,
		BaselinePath: cfg.BaselinePath,
	}

	// Load baseline (unless --no-compare).
	var baseline *BaselineFile
	if !cfg.NoCompare {
		bf, err := LoadBaseline(cfg.BaselinePath)
		if err != nil && !os.IsNotExist(err) {
			return result, fmt.Errorf("loading baseline: %w", err)
		}
		baseline = bf
	}

	// Create run dir.
	runDir := filepath.Join("/tmp/wadjet-harness", fmt.Sprintf("run-%d", started.Unix()))
	if err := os.MkdirAll(runDir, 0755); err != nil {
		return result, fmt.Errorf("creating run dir: %w", err)
	}

	// Local mode: preflight + spawn cluster.
	var cluster *Cluster
	var coordURL string
	switch cfg.Mode {
	case ModeLocal:
		sliceCfg := SliceConfigs[cfg.Slice]
		const numWorkers = 2
		pf := CheckPreflight(sliceCfg, runDir, numWorkers)
		if !pf.OK {
			return result, fmt.Errorf("preflight failed:\n  - %s", pf.Error())
		}

		clusterCfg := ClusterConfig{
			WadjetBin:    cfg.WadjetBin,
			RunDir:       runDir,
			NumWorkers:   numWorkers,
			GoMemLimit:   sliceCfg.GoMemLimit,
			PgAddr:       cfg.PgAddr,
			DataDir:      cfg.DataDir,
			Logger:       logger,
		}
		if cfg.Source == "s3" {
			clusterCfg.StorageType = "s3"
			clusterCfg.Bucket = cfg.Bucket
			clusterCfg.Region = cfg.Region
			clusterCfg.Endpoint = cfg.Endpoint
			clusterCfg.SSL = cfg.SSL
			clusterCfg.DataDir = "" // ignore — S3 is the source
		}
		cluster = NewCluster(clusterCfg)

		// Two-phase startup: coordinator (owns NATS) first, seed data, then workers.
		if err := cluster.StartCoordinator(ctx); err != nil {
			return result, fmt.Errorf("starting coordinator: %w", err)
		}
		defer cluster.Shutdown(context.Background())

		switch cfg.Source {
		case "s3":
			if err := primeS3Catalog(ctx, cluster, cfg.Endpoint, cfg.Region, cfg.Bucket, cfg.SSL, cfg.DataPrefix, logger); err != nil {
				return result, fmt.Errorf("priming S3 catalog: %w", err)
			}
		default: // "local" or empty
			if err := loadSampleData(ctx, cluster, cfg.DataDir, sliceCfg, logger); err != nil {
				return result, fmt.Errorf("loading sample data: %w", err)
			}
		}

		if err := cluster.StartWorkers(ctx); err != nil {
			return result, fmt.Errorf("starting workers: %w", err)
		}

		coordURL = fmt.Sprintf("postgres://wadjet@localhost%s/wadjet?sslmode=disable", cluster.PgAddr())

	case ModeGolden:
		coordURL = cfg.CoordURL

	default:
		return result, fmt.Errorf("unknown mode %q", cfg.Mode)
	}

	// Subscribe to heartbeats for measurement collection.
	collector := NewCollector()
	hangDetector := NewHangDetector(30 * time.Second)
	stopHB := startHeartbeatSubscriber(ctx, cluster, collector, hangDetector)
	defer stopHB()

	// Run the query suite.
	queries := SelectQueries(cfg.Queries)
	sliceCfg := SliceConfigs[cfg.Slice]
	for _, qname := range queries {
		m, err := runOneQuery(ctx, coordURL, qname, collector, hangDetector, baseline, sliceCfg, cluster, runDir, logger)
		if err != nil {
			logger.Error("query failed", "q", qname, "err", err)
			m.Hung = true
		}
		result.Queries = append(result.Queries, m)
		if m.Hung {
			result.Hangs = append(result.Hangs, qname)
		}
	}

	// Compare against baseline.
	if baseline != nil {
		for _, m := range result.Queries {
			if m.Hung {
				continue
			}
			projected, err := baseline.Project(string(cfg.Slice)+"_slice", m)
			if err != nil {
				continue
			}
			projected.Query = m.Query
			projected.RowCount = m.RowCount
			projected.RowChecksum = m.RowChecksum
			deltas := baseline.Compare(projected)
			for _, d := range deltas {
				if d.Status == "REGRESS" {
					result.Regressions = append(result.Regressions, d)
				}
			}
		}
	}

	// ExpectSpill assertion for large slice.
	if cfg.Mode == ModeLocal && cfg.Slice == SliceLarge {
		var totalSpill int64
		for _, m := range result.Queries {
			totalSpill += m.SpillBytes
		}
		if totalSpill == 0 {
			result.Regressions = append(result.Regressions, QueryDelta{
				Query:  "<run>",
				Metric: "spill_paths_exercised",
				Status: "REGRESS",
			})
		}
	}

	result.DurationMs = time.Since(started).Milliseconds()
	result.Passed = len(result.Regressions) == 0 && len(result.Hangs) == 0
	result.ExitCode = computeExitCode(result)

	// Write result.json.
	if cfg.OutPath != "" {
		if err := writeResult(cfg.OutPath, result); err != nil {
			logger.Error("writing result", "err", err)
		}
	}

	preserveRunDirOnFailure(runDir, result)

	return result, nil
}

func computeExitCode(r RunResult) int {
	exit := ExitOK
	for _, d := range r.Regressions {
		if d.Metric == "row_count" || d.Metric == "row_checksum" {
			if exit < ExitCorrectness {
				exit = ExitCorrectness
			}
			continue
		}
		if exit < ExitRegression {
			exit = ExitRegression
		}
	}
	if len(r.Hangs) > 0 && exit < ExitRegression {
		exit = ExitRegression
	}
	return exit
}

func writeResult(path string, r RunResult) error {
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

func preserveRunDirOnFailure(runDir string, r RunResult) {
	if r.Passed {
		_ = os.RemoveAll(runDir)
		return
	}
	manifest := fmt.Sprintf(`Wadjet harness run preserved due to non-PASS result.

mode:     %s
slice:    %s
exit:     %d
hangs:    %v

Layout:
  logs/coord.log         — coordinator stdout/stderr
  logs/worker-N.log      — per-worker stdout/stderr
  spill/<role>/          — spill files (preserved for inspection)
  nats/                  — embedded NATS jetstream store
  result.json            — structured run result

To clean up: rm -rf %s
`, r.Mode, r.Slice, r.ExitCode, r.Hangs, runDir)
	_ = os.WriteFile(filepath.Join(runDir, "MANIFEST.txt"), []byte(manifest), 0644)
}

// captureGoroutineDumps fetches /debug/pprof/goroutine?debug=2 from every
// process in the cluster and writes each dump to a file in runDir/logs/.
// Returns the directory containing the dumps. Errors are logged, not fatal —
// a missing dump is acceptable, a harness hang is not.
func captureGoroutineDumps(cluster *Cluster, query string, runDir string, logger *slog.Logger) string {
	if cluster == nil {
		return ""
	}
	dumpDir := filepath.Join(runDir, "logs")
	ports := cluster.DebugPorts()
	client := &http.Client{Timeout: 5 * time.Second}
	for role, port := range ports {
		url := fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/goroutine?debug=2", port)
		resp, err := client.Get(url)
		if err != nil {
			logger.Warn("pprof fetch failed", "role", role, "query", query, "err", err)
			continue
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			logger.Warn("pprof non-200", "role", role, "query", query, "status", resp.StatusCode)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		dumpPath := filepath.Join(dumpDir, fmt.Sprintf("hang-%s-%s.txt", query, role))
		if err := os.WriteFile(dumpPath, body, 0644); err != nil {
			logger.Warn("writing dump", "path", dumpPath, "err", err)
		} else {
			logger.Info("goroutine dump captured", "path", dumpPath, "bytes", len(body))
		}
	}
	return dumpDir
}

func runOneQuery(
	ctx context.Context,
	coordURL string,
	name string,
	collector *MeasurementCollector,
	hangDetector *HangDetector,
	baseline *BaselineFile,
	_ SliceConfig,
	cluster *Cluster,
	runDir string,
	logger *slog.Logger,
) (QueryMeasurement, error) {
	collector.StartWindow(name)
	hangDetector.Reset()

	sql, err := LoadQuery(name)
	if err != nil {
		switch name {
		case "micro_reverse_bloom":
			return RunMicroReverseBloom(ctx, coordURL, collector)
		case "micro_grace_hash_join":
			return RunMicroGraceHashJoin(ctx, coordURL, collector)
		case "micro_hash_agg_high_card":
			return RunMicroHashAggHighCard(ctx, coordURL, collector)
		default:
			return collector.EndWindow(name), err
		}
	}

	// Hard timeout: 10x baseline projection if known, else 5 min.
	// Override via WADJET_HARNESS_QUERY_TIMEOUT (Go duration string) for
	// scenarios like SF10 with WADJET_GOGC=100 where GC overhead extends
	// wall time well past the default — Q18 SF10 needed >3 min just to
	// finish the IN-subquery aggregate (project_q18_sf10_native_dag_oom_2026-04-24).
	timeout := 5 * time.Minute
	if baseline != nil {
		if qb, ok := baseline.Queries[name]; ok && qb.WallMsP50 > 0 {
			timeout = time.Duration(qb.WallMsP50) * 10 * time.Millisecond
		}
	}
	if override := os.Getenv("WADJET_HARNESS_QUERY_TIMEOUT"); override != "" {
		if d, err := time.ParseDuration(override); err == nil {
			timeout = d
		}
	}
	queryCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, err := pgx.Connect(queryCtx, coordURL)
	if err != nil {
		return collector.EndWindow(name), fmt.Errorf("pgx connect: %w", err)
	}
	defer conn.Close(context.Background())

	rows, err := conn.Query(queryCtx, sql)
	if err != nil {
		return collector.EndWindow(name), err
	}
	defer rows.Close()

	hash := sha256.New()
	var rowCount int64
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return collector.EndWindow(name), err
		}
		fmt.Fprintf(hash, "%v\n", vals)
		rowCount++
	}
	if err := rows.Err(); err != nil {
		// Check if this was a timeout — if so, capture goroutine dumps.
		if queryCtx.Err() != nil {
			dumpPath := captureGoroutineDumps(cluster, name, runDir, logger)
			m := collector.EndWindow(name)
			m.Hung = true
			m.HangDumpPath = dumpPath
			return m, err
		}
		return collector.EndWindow(name), err
	}

	m := collector.EndWindow(name)
	m.RowCount = rowCount
	m.RowChecksum = hex.EncodeToString(hash.Sum(nil))
	return m, nil
}

// loadSampleData generates TPC-H SF0.01 data, writes parquet files into the
// FileStore directory, and registers them in the NATS KV catalog. This must
// be called after cluster.StartNATS() and before cluster.StartProcesses()
// so that when the coordinator and workers boot, the catalog already has
// all tables and files registered.
func loadSampleData(ctx context.Context, cluster *Cluster, dataDir string, _ SliceConfig, logger *slog.Logger) error {
	// Connect to the coordinator's NATS for catalog access.
	nc, err := cluster.ConnectNATS()
	if err != nil {
		return fmt.Errorf("connecting to NATS for catalog: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	// Set up FileStore at the data dir (same path the coordinator will use).
	if dataDir == "" {
		dataDir = "/tmp/wadjet-harness/data"
	}
	store, err := objstore.NewFileStore(dataDir)
	if err != nil {
		return fmt.Errorf("creating FileStore: %w", err)
	}

	const bucketName = "wadjet"
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		return fmt.Errorf("creating NATS KV: %w", err)
	}
	cat := catalog.New(kv, store, bucketName)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("catalog init: %w", err)
	}

	// Generate SF0.01 data and write as parquet files.
	const chunksPerTable = 8
	data := tpch.Generate(tpch.SF001)
	for tableName, schema := range tpch.AllTables {
		if err := cat.CreateTable(ctx, tableName, schema, nil); err != nil {
			return fmt.Errorf("creating table %s: %w", tableName, err)
		}
		rows := data[tableName]
		if len(rows) == 0 {
			continue
		}

		numChunks := chunksPerTable
		if len(rows) < numChunks {
			numChunks = len(rows)
		}
		chunkSize := (len(rows) + numChunks - 1) / numChunks
		var entries []catalog.FileEntry
		for cIdx := 0; cIdx < numChunks; cIdx++ {
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
				return fmt.Errorf("parquet writer for %s: %w", tableName, err)
			}
			if err := pw.WriteRows(chunkRows); err != nil {
				return fmt.Errorf("writing %s chunk %d: %w", tableName, cIdx, err)
			}
			if err := pw.Close(); err != nil {
				return fmt.Errorf("closing %s chunk %d: %w", tableName, cIdx, err)
			}

			filePath := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tableName, cIdx+1)
			pdata := buf.Bytes()
			if _, err := store.Put(ctx, bucketName, filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
				return fmt.Errorf("storing %s chunk %d: %w", tableName, cIdx, err)
			}
			entries = append(entries, catalog.FileEntry{
				Path:      filePath,
				SizeBytes: int64(len(pdata)),
				NumRows:   int64(len(chunkRows)),
				CreatedAt: time.Now(),
			})
		}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", entries); err != nil {
			return fmt.Errorf("adding %s files to catalog: %w", tableName, err)
		}
		logger.Info("loaded table", "table", tableName, "chunks", len(entries), "rows", len(rows))
	}

	// Seed synthetic micro tables for micro-benchmarks.
	microData := generateMicroData()
	for tableName, mt := range microData {
		if err := cat.CreateTable(ctx, tableName, mt.schema, nil); err != nil {
			return fmt.Errorf("creating micro table %s: %w", tableName, err)
		}
		if len(mt.rows) == 0 {
			continue
		}
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, mt.schema, parquet.DefaultWriterConfig())
		if err != nil {
			return fmt.Errorf("parquet writer for %s: %w", tableName, err)
		}
		if err := pw.WriteRows(mt.rows); err != nil {
			return fmt.Errorf("writing %s: %w", tableName, err)
		}
		if err := pw.Close(); err != nil {
			return fmt.Errorf("closing %s: %w", tableName, err)
		}
		filePath := fmt.Sprintf("tables/%s/chunk_0001.parquet", tableName)
		pdata := buf.Bytes()
		if _, err := store.Put(ctx, bucketName, filePath, bytes.NewReader(pdata), int64(len(pdata)), "application/octet-stream"); err != nil {
			return fmt.Errorf("storing %s: %w", tableName, err)
		}
		entries := []catalog.FileEntry{{
			Path:      filePath,
			SizeBytes: int64(len(pdata)),
			NumRows:   int64(len(mt.rows)),
			CreatedAt: time.Now(),
		}}
		if err := cat.AddFiles(ctx, tableName, map[string]string{}, "tables/"+tableName+"/", entries); err != nil {
			return fmt.Errorf("adding %s files to catalog: %w", tableName, err)
		}
		logger.Info("loaded micro table", "table", tableName, "rows", len(mt.rows))
	}

	return nil
}

// startHeartbeatSubscriber spawns a goroutine that subscribes to the
// embedded NATS heartbeat subject and feeds samples into the collector
// and hang detector. Returns a stop function.
func startHeartbeatSubscriber(
	ctx context.Context,
	cluster *Cluster,
	collector *MeasurementCollector,
	hangDetector *HangDetector,
) func() {
	if cluster == nil {
		return func() {}
	}

	nc, err := cluster.ConnectNATS()
	if err != nil {
		return func() {}
	}

	sub, err := nc.Subscribe(distributed.SubjectHeartbeat, func(msg *nats.Msg) {
		var hb distributed.WorkerHeartbeat
		if err := distributed.Unmarshal(msg.Data, &hb); err != nil {
			return
		}
		collector.Observe(hb)
		hangDetector.Observe(hb.Timestamp, hb.NumGoroutines)
	})
	if err != nil {
		nc.Close()
		return func() {}
	}

	stopC := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-stopC:
		case <-ctx.Done():
		}
		sub.Unsubscribe()
		nc.Close()
	}()

	return func() {
		close(stopC)
		wg.Wait()
	}
}
