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
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	tpch "github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/coordinator"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	distrib "github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/worker"
	"github.com/citc-tech/wadjet/wadjet"
)

func main() {
	var (
		scale      = flag.Int("scale", 1, "TPC-H scale factor (1, 10, 100)")
		s3Endpoint = flag.String("endpoint", "", "S3 endpoint (empty = in-memory standalone)")
		s3Bucket   = flag.String("bucket", "wadjet-bench-sf1", "S3 bucket name")
		s3Region   = flag.String("region", "", "S3 region")
		ssl        = flag.Bool("ssl", false, "Use TLS for S3")
		workers    = flag.Int("workers", 0, "Expected external workers (0 = standalone in-memory)")
		natsPort   = flag.Int("nats-port", 4222, "NATS listen port (distributed mode)")
		runs       = flag.Int("runs", 1, "Number of benchmark runs")
		dataOnly   = flag.Bool("data-only", false, "Generate and upload data only, skip benchmark queries")
		skipLoad   = flag.Bool("skip-load", false, "Skip data generation; discover existing parquet files from S3")
		cpuProf    = flag.String("cpuprofile", "", "Write CPU profile to file")
		memProf    = flag.String("memprofile", "", "Write memory profile to file")
		profDir    = flag.String("profdir", "", "Directory for per-query profiles")
	)
	flag.Parse()

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

	// Set GOMEMLIMIT to prevent OOM on bare metal / EC2 instances
	if memLimit := memory.DetectMemoryLimit(); memLimit > 0 {
		goMemLimit := memLimit * 9 / 10
		debug.SetMemoryLimit(goMemLimit)
		log.Printf("Set GOMEMLIMIT=%d (%.1f GB) from detected limit %d",
			goMemLimit, float64(goMemLimit)/(1024*1024*1024), memLimit)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	var db *wadjet.DB
	isDistributed := *workers > 0 && *s3Endpoint != ""
	useS3 := *s3Endpoint != ""

	if isDistributed {
		db = setupDistributed(ctx, logger, *s3Endpoint, *s3Region, *s3Bucket, *ssl, *natsPort, *workers)
	} else if useS3 {
		db = setupS3Standalone(ctx, *s3Endpoint, *s3Region, *s3Bucket, *ssl)
	} else {
		db = setupStandalone(ctx)
	}

	sf := tpch.ScaleFactor(float64(*scale))
	if *skipLoad {
		discoverData(ctx, db, *s3Endpoint, *s3Region, *s3Bucket, *ssl)
	} else {
		loadData(ctx, db, sf)
	}
	if !*dataOnly {
		runBenchmark(ctx, db, *runs, *profDir)
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

func setupDistributed(ctx context.Context, logger *slog.Logger, endpoint, region, bucket string, ssl bool, nPort, workerCount int) *wadjet.DB {
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

	// Local worker on coordinator node
	w := worker.New(worker.Config{
		NATSUrl:          embeddedNATS.ClientURL(),
		ClusterID:        "local",
		MaxConcurrent:    4,
		CacheBytes:       256 * 1024 * 1024,
		ResultStoreBytes: 512 * 1024 * 1024,
	}, store, nc, js, logger)
	if err := w.Start(ctx); err != nil {
		log.Fatalf("worker start: %v", err)
	}

	// Coordinator
	coord := coordinator.New(coordinator.Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: bucket,
	}, cat, nc, js, logger)
	coord.Workers().StartReaper(ctx)

	// Wait for remote workers
	log.Printf("Waiting for %d remote workers to connect...", workerCount)
	deadline := time.Now().Add(10 * time.Minute)
	for time.Now().Before(deadline) {
		// Count() includes local worker
		total := coord.Workers().Count()
		remote := total - 1
		if remote >= workerCount {
			log.Printf("All %d remote workers connected (%d total)", workerCount, total)
			break
		}
		log.Printf("  %d/%d remote workers...", remote, workerCount)
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

	return db
}

func discoverData(ctx context.Context, db *wadjet.DB, endpoint, region, bucket string, ssl bool) {
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		log.Fatalf("creating S3 store for discovery: %v", err)
	}

	// Register table schemas in catalog
	for name, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				continue
			}
			log.Fatalf("create table %s: %v", name, err)
		}
	}

	// Discover existing parquet files for each table
	totalFiles := 0
	for name := range tpch.AllTables {
		prefix := "tables/" + name + "/"
		objects, err := store.List(ctx, bucket, objstore.ListOptions{Prefix: prefix})
		if err != nil {
			log.Fatalf("listing %s files: %v", name, err)
		}
		if len(objects) == 0 {
			log.Printf("WARNING: no files found for table %s (prefix: %s)", name, prefix)
			continue
		}

		files := make([]catalog.FileEntry, 0, len(objects))
		for _, obj := range objects {
			if !strings.HasSuffix(obj.Key, ".parquet") {
				continue
			}
			files = append(files, catalog.FileEntry{
				Path:      obj.Key,
				SizeBytes: obj.Size,
				NumRows:   0, // unknown, scanner reads from parquet footer
				CreatedAt: obj.LastModified,
			})
		}

		if err := db.Catalog().AddFiles(ctx, name, nil, "", files); err != nil {
			log.Fatalf("registering %s files: %v", name, err)
		}
		totalFiles += len(files)
		log.Printf("Discovered %d files for table %s", len(files), name)
	}
	log.Printf("Data discovery complete: %d total files across %d tables", totalFiles, len(tpch.AllTables))
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

func runBenchmark(ctx context.Context, db *wadjet.DB, runs int, profDir string) {
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
			q := tpch.TPCHQueries[qNum]
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

			start := time.Now()
			qResult, err := db.Query(ctx, q.SQL)
			elapsed := time.Since(start)
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
				HeapMB:    int64(memAfter.HeapAlloc) / (1024 * 1024),
				DeltaMB:   heapDelta / (1024 * 1024),
				GCPauseNs: gcPause,
				Err:       err,
			}
			if err == nil {
				r.Rows = len(qResult.Rows)
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
