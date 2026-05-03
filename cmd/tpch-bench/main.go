// tpch-bench runs the TPC-H benchmark against a Wadjet database.
//
// Standalone mode (default): in-memory storage, single process.
// Distributed mode: S3 storage, embedded NATS, external workers.
//
// Usage:
//
//	tpch-bench --scale=1                                    # Standalone SF1
//	tpch-bench --scale=1 --workers=3 --endpoint=s3.us-east-2.amazonaws.com --ssl --bucket=wadjet-bench-sf1 --region=us-east-2
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	tpch "github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/coordinator"
	distrib "github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/worker"
	"github.com/citc-tech/wadjet/wadjet"
	"github.com/nats-io/nats.go"
)

func main() {
	var (
		configPath  = flag.String("config", "", "Path to benchmark profile YAML (values override defaults; CLI flags override profile)")
		scale       = flag.Int("scale", 1, "TPC-H scale factor (1, 10, 100)")
		s3Endpoint  = flag.String("endpoint", "", "S3 endpoint (empty = in-memory standalone)")
		s3Bucket    = flag.String("bucket", "wadjet-bench-sf1", "S3 bucket name")
		s3Region    = flag.String("region", "", "S3 region")
		ssl         = flag.Bool("ssl", false, "Use TLS for S3")
		workers     = flag.Int("workers", 0, "Expected external workers (0 = standalone in-memory)")
		natsPort    = flag.Int("nats-port", 4222, "NATS listen port (distributed mode)")
		runs        = flag.Int("runs", 1, "Number of benchmark runs")
		dataOnly    = flag.Bool("data-only", false, "Generate and upload data only, skip benchmark queries")
		skipLoad    = flag.Bool("skip-load", false, "Skip data generation; discover existing parquet files from S3")
		skipQueries = flag.String("skip-queries", "", "Comma-separated query numbers to skip (e.g. 2,17)")
		queryTimeout = flag.Duration("query-timeout", 10*time.Minute, "Per-query timeout (0 = no timeout)")
		cpuProf     = flag.String("cpuprofile", "", "Write CPU profile to file")
		memProf     = flag.String("memprofile", "", "Write memory profile to file")
		profDir     = flag.String("profdir", "", "Directory for per-query profiles")
		dataPrefix  = flag.String("data-prefix", "tables/", "S3 prefix for table data (e.g. 'tables/' or '' for root)")
	)
	flag.Parse()

	// Apply profile: values from YAML override flag defaults, but
	// explicitly-set CLI flags take precedence over the profile.
	if *configPath != "" {
		profile, err := loadProfile(*configPath)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Loaded profile: %s (%s)", profile.Name, *configPath)

		setFlags := make(map[string]bool)
		flag.Visit(func(f *flag.Flag) { setFlags[f.Name] = true })

		if !setFlags["scale"] && profile.Benchmark.ScaleFactor > 0 {
			*scale = profile.Benchmark.ScaleFactor
		}
		if !setFlags["endpoint"] && profile.Storage.Region != "" {
			*s3Endpoint = profile.s3Endpoint()
		}
		if !setFlags["bucket"] && profile.Storage.Bucket != "" {
			*s3Bucket = profile.Storage.Bucket
		}
		if !setFlags["region"] && profile.Storage.Region != "" {
			*s3Region = profile.Storage.Region
		}
		if !setFlags["ssl"] && profile.Storage.Region != "" {
			*ssl = true // S3 always uses TLS
		}
		if !setFlags["workers"] && profile.Cluster.Workers > 0 {
			*workers = profile.Cluster.Workers
		}
		if !setFlags["runs"] && profile.Benchmark.Runs > 0 {
			*runs = profile.Benchmark.Runs
		}
		if !setFlags["skip-load"] && profile.Benchmark.SkipLoad {
			*skipLoad = true
		}
		// data-prefix: always apply from profile (even "") unless explicitly set.
		// This is the field that caused the SF100 deploy failure — the default
		// "tables/" is wrong for buckets with data at root level.
		if !setFlags["data-prefix"] {
			*dataPrefix = profile.Storage.DataPrefix
		}
		if !setFlags["query-timeout"] && profile.Benchmark.QueryTimeout != "" {
			d, err := time.ParseDuration(profile.Benchmark.QueryTimeout)
			if err != nil {
				log.Fatalf("invalid query_timeout in profile: %v", err)
			}
			*queryTimeout = d
		}
		if !setFlags["skip-queries"] && len(profile.Benchmark.SkipQueries) > 0 {
			parts := make([]string, len(profile.Benchmark.SkipQueries))
			for i, q := range profile.Benchmark.SkipQueries {
				parts[i] = fmt.Sprintf("%d", q)
			}
			*skipQueries = strings.Join(parts, ",")
		}

		log.Printf("  scale=%d bucket=%s region=%s prefix=%q workers=%d timeout=%v",
			*scale, *s3Bucket, *s3Region, *dataPrefix, *workers, *queryTimeout)
	}

	if *cpuProf != "" {
		f, err := os.Create(*cpuProf)
		if err != nil {
			log.Fatal(err)
		}
		pprof.StartCPUProfile(f)
		defer func() {
			pprof.StopCPUProfile()
			f.Close()
		}()
	}

	// Set GOMEMLIMIT to prevent OOM on bare metal / EC2 instances.
	// GOGC=off disables percentage-based GC triggering — GOMEMLIMIT alone
	// acts as the safety net. This avoids GC assist overhead when the LRU
	// file cache creates a large long-lived heap.
	if memLimit := memory.DetectMemoryLimit(); memLimit > 0 {
		goMemLimit := memLimit * 9 / 10
		debug.SetMemoryLimit(goMemLimit)
		debug.SetGCPercent(-1)
		log.Printf("Set GOMEMLIMIT=%d (%.1f GB), GOGC=off from detected limit %d",
			goMemLimit, float64(goMemLimit)/(1024*1024*1024), memLimit)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var db *wadjet.DB
	var coord *coordinator.Coordinator
	var nc *nats.Conn
	isDistributed := *workers > 0 && *s3Endpoint != ""
	useS3 := *s3Endpoint != ""

	if isDistributed {
		db, coord, nc = setupDistributed(ctx, logger, *s3Endpoint, *s3Region, *s3Bucket, *ssl, *natsPort, *workers)
	} else if useS3 {
		db = setupS3Standalone(ctx, *s3Endpoint, *s3Region, *s3Bucket, *ssl)
	} else {
		db = setupStandalone(ctx)
	}

	sf := tpch.ScaleFactor(float64(*scale))
	if *skipLoad {
		discoverData(ctx, db, *s3Endpoint, *s3Region, *s3Bucket, *ssl, *dataPrefix)
	} else {
		loadData(ctx, db, sf)
	}
	// Start worker CPU profiling before benchmark
	if nc != nil && *profDir != "" {
		startWorkerProfiling(nc)
	}

	if !*dataOnly {
		var qf queryFn
		if coord != nil {
			// Distributed: route queries through coordinator → workers.
			// Coordinator only handles inline results (<256KB); large
			// results stay on S3 and are counted without reading bytes.
			expectedWorkers := *workers
			qf = func(ctx context.Context, sql string) (int64, error) {
				// Wait for workers to recover if previous query crashed them.
				for retries := 0; retries < 30 && coord.Workers().Count() < expectedWorkers; retries++ {
					if retries == 0 {
						log.Printf("  waiting for workers (%d/%d)...", coord.Workers().Count(), expectedWorkers)
					}
					time.Sleep(2 * time.Second)
				}
				r, err := coord.ExecuteSQL(ctx, sql)
				if err != nil {
					return 0, err
				}
				return r.TotalRows, nil
			}
		} else {
			// Standalone: local execution
			qf = func(ctx context.Context, sql string) (int64, error) {
				r, err := db.Query(ctx, sql)
				if err != nil {
					return 0, err
				}
				return int64(len(r.Rows)), nil
			}
		}
		// Parse skip-queries flag
		skip := make(map[int]bool)
		if *skipQueries != "" {
			for _, s := range strings.Split(*skipQueries, ",") {
				s = strings.TrimSpace(s)
				if n, err := fmt.Sscanf(s, "%d", new(int)); n == 1 && err == nil {
					var qn int
					fmt.Sscanf(s, "%d", &qn)
					skip[qn] = true
				}
			}
		}

		runBenchmark(ctx, qf, sf, *runs, *profDir, skip, *queryTimeout)
	}

	// Collect worker profiles after benchmark
	if nc != nil && *profDir != "" {
		collectWorkerProfiles(nc, *workers, *profDir)
	}

	if *memProf != "" {
		f, err := os.Create(*memProf)
		if err != nil {
			log.Fatal(err)
		}
		runtime.GC()
		pprof.WriteHeapProfile(f)
		f.Close()
	}
}

// queryFn executes a SQL query and returns (row count, error).
type queryFn func(ctx context.Context, sql string) (int64, error)

func setupStandalone(ctx context.Context) *wadjet.DB {
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "tpch"})
	if err != nil {
		log.Fatalf("opening DB: %v", err)
	}
	return db
}

func setupS3Standalone(ctx context.Context, endpoint, region, bucket string, ssl bool) *wadjet.DB {
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		log.Fatalf("creating S3 store: %v", err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: bucket})
	if err != nil {
		log.Fatalf("opening DB: %v", err)
	}
	return db
}

func setupDistributed(ctx context.Context, logger *slog.Logger, endpoint, region, bucket string, ssl bool, nPort, workerCount int) (*wadjet.DB, *coordinator.Coordinator, *nats.Conn) {
	// S3 store (IAM credentials auto-detected)
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		log.Fatalf("creating S3 store: %v", err)
	}

	// Embedded NATS for coordination — bind to 0.0.0.0 so remote workers can connect
	natsCfg := distrib.DefaultNATSConfig()
	natsCfg.Host = "0.0.0.0"
	natsCfg.Port = nPort
	embeddedNATS, err := distrib.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		log.Fatalf("starting NATS: %v", err)
	}

	nc, err := distrib.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		log.Fatalf("connecting NATS: %v", err)
	}

	js, err := distrib.NewJetStream(nc)
	if err != nil {
		log.Fatalf("JetStream: %v", err)
	}

	if err := distrib.SetupStreams(ctx, js); err != nil {
		log.Fatalf("streams: %v", err)
	}

	// Catalog backed by NATS KV
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		log.Fatalf("KV: %v", err)
	}
	cat := catalog.NewWithCluster(kv, store, bucket, "local")
	if err := cat.Init(ctx); err != nil {
		log.Fatalf("catalog init: %v", err)
	}

	// Coordinator — no embedded worker so it stays out of the data path.
	// All data tasks run on the remote workers.
	coord := coordinator.New(coordinator.Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: bucket,
	}, cat, nc, js, logger)
	coord.Workers().StartReaper(ctx)
	coord.Workers().StartSubStatsLogger(ctx)
	coord.StartQueryReaper(ctx)
	coord.StartQueryActiveHandler()

	// Wait for remote workers
	log.Printf("Waiting for %d remote workers to connect...", workerCount)
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		total := coord.Workers().Count()
		if total >= workerCount {
			log.Printf("All %d remote workers connected", workerCount)
			break
		}
		log.Printf("  %d/%d remote workers...", total, workerCount)
		time.Sleep(5 * time.Second)
	}

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: bucket,
		MetaKV: kv,
	})
	if err != nil {
		log.Fatalf("opening DB: %v", err)
	}

	return db, coord, nc
}

func discoverData(ctx context.Context, db *wadjet.DB, endpoint, region, bucket string, ssl bool, dataPrefix string) {
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		log.Fatalf("creating S3 store for discovery: %v", err)
	}

	// Discover existing parquet files for each table, then probe the first
	// file to learn the actual column types. The hardcoded benchmarks/tpch
	// schemas use TypeInt32/TypeString throughout, which matches the local
	// datagen but NOT the Polars-generated SF100 parquet (which uses INT64
	// and DATE). Registering the hardcoded schema against Polars data leads
	// to silent type-truncation in the cache pre-scan path: o_orderkey
	// truncates from int64 → int32 (works by luck for low keys, fails for
	// hash joins like Q05's c_custkey = o_custkey), and o_orderdate gets
	// represented as a string column even though the bytes on disk are
	// int32 days. Detecting the real schema once at startup costs one S3
	// round-trip per table (range read of the parquet footer) and removes
	// an entire class of correctness bugs.
	totalFiles := 0
	for name := range tpch.AllTables {
		prefix := dataPrefix + name + "/"
		objects, err := store.List(ctx, bucket, objstore.ListOptions{Prefix: prefix})
		if err != nil {
			log.Fatalf("listing %s files: %v", name, err)
		}
		if len(objects) == 0 {
			log.Printf("WARNING: no files found for table %s (prefix: %s)", name, prefix)
			continue
		}

		// Pre-collect parquet objects so we can probe each for both schema
		// (first file) AND row counts (every file). Without real row counts
		// in the catalog, the worker's planner sees ScanRowEstimate=0 for
		// every table — including lineitem at 600M rows — and the cost-based
		// join reorder picks an arbitrary order. At SF100 that surfaced as
		// Q05 returning 0 rows because lineitem ended up on the BUILD side
		// of a join with batches=0 (the build pipeline's first scan call
		// somehow returned no rows under the bad ordering).
		var pqObjects []objstore.ObjectInfo
		for _, obj := range objects {
			if strings.HasSuffix(obj.Key, ".parquet") {
				pqObjects = append(pqObjects, obj)
			}
		}
		if len(pqObjects) == 0 {
			log.Printf("WARNING: no parquet files for table %s, using hardcoded schema", name)
			if err := db.CreateTable(ctx, name, tpch.AllTables[name], nil); err != nil && !strings.Contains(err.Error(), "already exists") {
				log.Fatalf("create table %s: %v", name, err)
			}
			continue
		}

		schema, err := probeParquetSchema(ctx, store, bucket, pqObjects[0].Key)
		if err != nil {
			log.Printf("WARNING: probing %s/%s failed (%v); falling back to hardcoded schema", name, pqObjects[0].Key, err)
			schema = tpch.AllTables[name]
		}
		if err := db.CreateTable(ctx, name, schema, nil); err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Fatalf("create table %s: %v", name, err)
		}

		// Probe the parquet footer of every file to populate NumRows in the
		// catalog. The footer is small (KBs) so this is one quick range read
		// per file. With real NumRows, ScanRowEstimate flows through the
		// optimizer and the join reorder picks the right build/probe sides.
		files := make([]catalog.FileEntry, 0, len(pqObjects))
		var totalRows int64
		for _, obj := range pqObjects {
			numRows, perr := probeParquetRowCount(ctx, store, bucket, obj.Key)
			if perr != nil {
				log.Printf("WARNING: row count probe failed for %s/%s: %v", name, obj.Key, perr)
				numRows = 0
			}
			totalRows += numRows
			files = append(files, catalog.FileEntry{
				Path:      obj.Key,
				SizeBytes: obj.Size,
				NumRows:   numRows,
				CreatedAt: obj.LastModified,
			})
		}
		if err := db.Catalog().AddFiles(ctx, name, nil, "", files); err != nil {
			log.Fatalf("registering %s files: %v", name, err)
		}
		totalFiles += len(files)
		log.Printf("Discovered %d files for table %s (%d cols, %d rows)", len(files), name, len(schema.Columns), totalRows)
	}
	log.Printf("Data discovery complete: %d total files across %d tables", totalFiles, len(tpch.AllTables))
}

// probeParquetRowCount returns the total row count of a parquet file by
// reading just the footer via GetReaderAt. Used at startup so the catalog
// has real NumRows for cost-based join reordering.
func probeParquetRowCount(ctx context.Context, store objstore.Store, bucket, key string) (int64, error) {
	if ras, ok := store.(objstore.ReaderAtStore); ok {
		ra, sz, err := ras.GetReaderAt(ctx, bucket, key)
		if err != nil {
			return 0, fmt.Errorf("opening reader-at: %w", err)
		}
		defer ra.Close()
		r, err := parquet.NewReader(ra, sz)
		if err != nil {
			return 0, fmt.Errorf("parquet reader: %w", err)
		}
		return r.NumRows(), nil
	}
	rc, _, err := store.Get(ctx, bucket, key)
	if err != nil {
		return 0, fmt.Errorf("get: %w", err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil {
		return 0, fmt.Errorf("read: %w", err)
	}
	r, err := parquet.NewReaderFromBytes(buf)
	if err != nil {
		return 0, fmt.Errorf("parquet reader: %w", err)
	}
	return r.NumRows(), nil
}

// probeParquetSchema reads a single parquet file's schema directly from the
// object store. Uses GetReaderAt so the parquet reader can read just the
// trailer (~8 bytes) and the footer (~KB) instead of the whole file.
func probeParquetSchema(ctx context.Context, store objstore.Store, bucket, key string) (parquet.Schema, error) {
	if ras, ok := store.(objstore.ReaderAtStore); ok {
		ra, sz, err := ras.GetReaderAt(ctx, bucket, key)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("opening reader-at: %w", err)
		}
		defer ra.Close()
		r, err := parquet.NewReader(ra, sz)
		if err != nil {
			return parquet.Schema{}, fmt.Errorf("parquet reader: %w", err)
		}
		return r.Schema(), nil
	}
	// Fallback for stores without ReaderAt support — full download.
	rc, _, err := store.Get(ctx, bucket, key)
	if err != nil {
		return parquet.Schema{}, fmt.Errorf("get: %w", err)
	}
	defer rc.Close()
	buf, err := io.ReadAll(rc)
	if err != nil {
		return parquet.Schema{}, fmt.Errorf("read: %w", err)
	}
	r, err := parquet.NewReaderFromBytes(buf)
	if err != nil {
		return parquet.Schema{}, fmt.Errorf("parquet reader: %w", err)
	}
	return r.Schema(), nil
}

func loadData(ctx context.Context, db *wadjet.DB, sf tpch.ScaleFactor) {
	// Create tables
	for name, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			log.Fatalf("create table %s: %v", name, err)
		}
	}

	// Set up ingesters
	ingesters := make(map[string]*ingest.Ingester)
	for name, schema := range tpch.AllTables {
		ingesters[name] = db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: 100_000,
			RowGroupSize:  65_536,
		})
	}

	counts := sf.RowCounts()
	total := counts.Region + counts.Nation + counts.Supplier + counts.Part +
		counts.PartSupp + counts.Customer + counts.Orders + counts.LineItem
	log.Printf("Generating TPC-H SF%.0f data (~%dM rows)...", float64(sf), total/1_000_000)

	start := time.Now()
	rows := 0
	lastLog := time.Now()

	err := tpch.GenerateChunked(sf, 50_000, func(table string, chunk []map[string]any) error {
		rows += len(chunk)
		if time.Since(lastLog) > 3*time.Second {
			pct := float64(rows) / float64(total) * 100
			log.Printf("  %.0f%% (%d/%d rows, %.0f rows/s)", pct, rows, total, float64(rows)/time.Since(start).Seconds())
			lastLog = time.Now()
		}
		return ingesters[table].Ingest(ctx, chunk)
	})
	if err != nil {
		log.Fatalf("data generation: %v", err)
	}

	for name, ing := range ingesters {
		if err := ing.FlushAll(ctx); err != nil {
			log.Fatalf("flushing %s: %v", name, err)
		}
	}

	log.Printf("Data loaded: %d rows in %v (%.0f rows/s)", rows, time.Since(start), float64(rows)/time.Since(start).Seconds())
}

func runBenchmark(ctx context.Context, qf queryFn, sf tpch.ScaleFactor, runs int, profDir string, skipSet map[int]bool, perQueryTimeout time.Duration) {
	queryNums := make([]int, 0, len(tpch.TPCHQueries))
	for n := range tpch.TPCHQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	if profDir != "" {
		os.MkdirAll(profDir, 0755)
	}

	type result struct {
		Query    int
		Name     string
		Elapsed  time.Duration
		Rows     int
		HeapMB   int64
		DeltaMB  int64
		GCPauseNs uint64
		Err      error
	}

	for run := 1; run <= runs; run++ {
		fmt.Printf("\n=== Run %d/%d ===\n", run, runs)
		var results []result
		var totalElapsed time.Duration

		for _, qNum := range queryNums {
			if skipSet[qNum] {
				fmt.Printf("Q%02d %-30s  SKIPPED\n", qNum, tpch.GetQuery(qNum, sf).Name)
				continue
			}

			q := tpch.GetQuery(qNum, sf)
			runtime.GC()

			var memBefore, memAfter runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			// Per-query CPU profile (if profDir set)
			var cpuFile *os.File
			if profDir != "" {
				path := fmt.Sprintf("%s/cpu-q%02d-run%d.prof", profDir, qNum, run)
				cpuFile, _ = os.Create(path)
				pprof.StartCPUProfile(cpuFile)
			}

			// Per-query timeout
			qCtx := ctx
			var qCancel context.CancelFunc
			if perQueryTimeout > 0 {
				qCtx, qCancel = context.WithTimeout(ctx, perQueryTimeout)
			}

			start := time.Now()
			rowCount, err := qf(qCtx, q.SQL)
			elapsed := time.Since(start)

			// Force GC to collect previous query's result batches before
			// measuring heap delta. With GOGC=off, GC only runs under
			// GOMEMLIMIT pressure — previous batches stay allocated.
			runtime.GC()

			if qCancel != nil {
				qCancel()
			}
			totalElapsed += elapsed

			if cpuFile != nil {
				pprof.StopCPUProfile()
				cpuFile.Close()
			}

			runtime.ReadMemStats(&memAfter)
			heapDelta := int64(memAfter.HeapAlloc) - int64(memBefore.HeapAlloc)
			gcPause := memAfter.PauseTotalNs - memBefore.PauseTotalNs

			r := result{
				Query:     qNum,
				Name:      q.Name,
				Elapsed:   elapsed,
				Rows:      int(rowCount),
				HeapMB:    int64(memAfter.HeapAlloc) / (1024 * 1024),
				DeltaMB:   heapDelta / (1024 * 1024),
				GCPauseNs: gcPause,
				Err:       err,
			}
			results = append(results, r)

			status := "OK"
			if err != nil {
				status = fmt.Sprintf("FAIL: %v", err)
			}
			fmt.Printf("Q%02d %-30s %8s  %5d rows  heap %+4dMB  gc %4.1fms  %s\n",
				qNum, q.Name, elapsed.Round(time.Millisecond), r.Rows,
				r.DeltaMB, float64(gcPause)/1e6, status)
		}

		// Summary
		fmt.Printf("\n--- Summary (Run %d) ---\n", run)
		fmt.Printf("%-5s %-30s %10s %8s %10s %10s\n", "Query", "Description", "Time", "Rows", "Heap Delta", "GC Pause")
		fmt.Printf("%-5s %-30s %10s %8s %10s %10s\n", "-----", "-----", "-----", "-----", "-----", "-----")
		for _, r := range results {
			status := r.Elapsed.Round(time.Millisecond).String()
			if r.Err != nil {
				status = "FAILED"
			}
			fmt.Printf("Q%02d   %-30s %10s %8d %+8dMB %8.1fms\n",
				r.Query, r.Name, status, r.Rows, r.DeltaMB, float64(r.GCPauseNs)/1e6)
		}
		fmt.Printf("%-5s %-30s %10s\n", "", "TOTAL", totalElapsed.Round(time.Millisecond))

		// Per-query memory profile (if profDir set, at end of run)
		if profDir != "" {
			path := fmt.Sprintf("%s/mem-run%d.prof", profDir, run)
			f, err := os.Create(path)
			if err == nil {
				runtime.GC()
				pprof.WriteHeapProfile(f)
				f.Close()
			}
		}
	}
}

// startWorkerProfiling tells all workers to begin CPU profiling.
func startWorkerProfiling(nc *nats.Conn) {
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		log.Printf("warning: could not subscribe for profile start responses: %v", err)
		return
	}
	defer sub.Unsubscribe()

	nc.PublishRequest(distrib.SubjectProfileStart, inbox, nil)

	// Wait for at least one acknowledgment
	for {
		_, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			break
		}
	}
	log.Println("Worker CPU profiling started")
}

// collectWorkerProfiles stops CPU profiling on all workers and saves their
// CPU + heap profiles to profDir.
func collectWorkerProfiles(nc *nats.Conn, workerCount int, profDir string) {
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		log.Printf("warning: could not subscribe for profile collection: %v", err)
		return
	}
	defer sub.Unsubscribe()

	nc.PublishRequest(distrib.SubjectProfileCollect, inbox, nil)

	collected := 0
	for collected < workerCount {
		msg, err := sub.NextMsg(30 * time.Second)
		if err != nil {
			break
		}
		var wp worker.WorkerProfile
		if err := json.Unmarshal(msg.Data, &wp); err != nil {
			log.Printf("warning: could not unmarshal worker profile: %v", err)
			continue
		}
		if len(wp.CPU) > 0 {
			path := filepath.Join(profDir, fmt.Sprintf("worker-%s-cpu.prof", wp.WorkerID))
			os.WriteFile(path, wp.CPU, 0644)
			log.Printf("Saved worker CPU profile: %s (%d bytes)", path, len(wp.CPU))
		}
		if len(wp.Heap) > 0 {
			path := filepath.Join(profDir, fmt.Sprintf("worker-%s-heap.prof", wp.WorkerID))
			os.WriteFile(path, wp.Heap, 0644)
			log.Printf("Saved worker heap profile: %s (%d bytes)", path, len(wp.Heap))
		}
		collected++
	}
	log.Printf("Collected profiles from %d/%d workers", collected, workerCount)
}
