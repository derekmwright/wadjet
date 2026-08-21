// security-bench runs the security analytics benchmark against a Wadjet database.
//
// Standalone mode (default): in-memory storage, single process.
// Distributed mode: S3 storage, embedded NATS, external workers.
//
// Usage:
//
//	security-bench --scale=1                                    # Standalone SF1
//	security-bench --scale=10 --endpoint=s3.us-east-2.amazonaws.com --ssl --bucket=wadjet-security-sf10 --region=us-east-2
//	security-bench --scale=10 --workers=3 --endpoint=s3.us-east-2.amazonaws.com --ssl --bucket=wadjet-security-sf10 --region=us-east-2
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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

	security "github.com/derekmwright/wadjet/benchmarks/security"
	"github.com/derekmwright/wadjet/internal/coordinator"
	distrib "github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
	"github.com/nats-io/nats.go"
)

func main() {
	var (
		scale        = flag.Int("scale", 1, "Scale factor (1, 10, 100)")
		s3Endpoint   = flag.String("endpoint", "", "S3 endpoint (empty = in-memory standalone)")
		s3Bucket     = flag.String("bucket", "wadjet-security", "S3 bucket name")
		s3Region     = flag.String("region", "", "S3 region")
		ssl          = flag.Bool("ssl", false, "Use TLS for S3")
		workers      = flag.Int("workers", 0, "Expected external workers (0 = standalone)")
		natsPort     = flag.Int("nats-port", 4222, "NATS listen port (distributed mode)")
		runs         = flag.Int("runs", 1, "Number of benchmark runs")
		dataOnly     = flag.Bool("data-only", false, "Generate and upload data only, skip queries")
		skipLoad     = flag.Bool("skip-load", false, "Skip data generation; discover existing parquet files")
		skipQueries  = flag.String("skip-queries", "", "Comma-separated query numbers to skip")
		queryTimeout = flag.Duration("query-timeout", 10*time.Minute, "Per-query timeout")
		cpuProf      = flag.String("cpuprofile", "", "Write CPU profile to file")
		memProf      = flag.String("memprofile", "", "Write memory profile to file")
		profDir      = flag.String("profdir", "", "Directory for per-query profiles")
		dataPrefix   = flag.String("data-prefix", "tables/", "S3 prefix for table data")
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

	sf := security.ScaleFactor(float64(*scale))
	if *skipLoad {
		discoverData(ctx, db, *s3Endpoint, *s3Region, *s3Bucket, *ssl, *dataPrefix)
	} else {
		loadData(ctx, db, sf)
	}

	if nc != nil && *profDir != "" {
		startWorkerProfiling(nc)
	}

	if !*dataOnly {
		var qf queryFn
		if coord != nil {
			qf = func(ctx context.Context, sql string) (int64, error) {
				r, err := coord.ExecuteSQL(ctx, sql)
				if err != nil {
					return 0, err
				}
				r.Close() // row count only; release batches / spill stream
				return r.TotalRows, nil
			}
		} else {
			qf = func(ctx context.Context, sql string) (int64, error) {
				r, err := db.Query(ctx, sql)
				if err != nil {
					return 0, err
				}
				return int64(len(r.Rows)), nil
			}
		}

		skip := make(map[int]bool)
		if *skipQueries != "" {
			for _, s := range strings.Split(*skipQueries, ",") {
				s = strings.TrimSpace(s)
				var qn int
				if _, err := fmt.Sscanf(s, "%d", &qn); err == nil {
					skip[qn] = true
				}
			}
		}

		runBenchmark(ctx, qf, *runs, *profDir, skip, *queryTimeout)
	}

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

type queryFn func(ctx context.Context, sql string) (int64, error)

func setupStandalone(ctx context.Context) *wadjet.DB {
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "security"})
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
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   ssl,
		Region:   region,
	})
	if err != nil {
		log.Fatalf("creating S3 store: %v", err)
	}

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

	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		log.Fatalf("KV: %v", err)
	}
	cat := catalog.NewWithCluster(kv, store, bucket, "local")
	if err := cat.Init(ctx); err != nil {
		log.Fatalf("catalog init: %v", err)
	}

	coord := coordinator.New(coordinator.Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: bucket,
	}, cat, nc, js, logger)
	coord.Workers().StartReaper(ctx)
	coord.Workers().StartSubStatsLogger(ctx)
	coord.StartQueryReaper(ctx)
	coord.StartQueryActiveHandler()

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

	for name, schema := range security.AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			if strings.Contains(err.Error(), "already exists") {
				continue
			}
			log.Fatalf("create table %s: %v", name, err)
		}
	}

	totalFiles := 0
	for name := range security.AllTables {
		prefix := dataPrefix + name + "/"
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
				NumRows:   0,
				CreatedAt: obj.LastModified,
			})
		}

		if err := db.Catalog().AddFiles(ctx, name, nil, "", files); err != nil {
			log.Fatalf("registering %s files: %v", name, err)
		}
		totalFiles += len(files)
		log.Printf("Discovered %d files for table %s", len(files), name)
	}
	log.Printf("Data discovery complete: %d total files across %d tables", totalFiles, len(security.AllTables))
}

func loadData(ctx context.Context, db *wadjet.DB, sf security.ScaleFactor) {
	for name, schema := range security.AllTables {
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			log.Fatalf("create table %s: %v", name, err)
		}
	}

	ingesters := make(map[string]*ingest.Ingester)
	for name, schema := range security.AllTables {
		ingesters[name] = db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: 100_000,
			RowGroupSize:  65_536,
		})
	}

	counts := sf.RowCounts()
	total := counts.Total()
	log.Printf("Generating security data SF%.0f (~%dM rows across 5 tables)...", float64(sf), total/1_000_000)

	start := time.Now()
	rows := 0
	lastLog := time.Now()

	err := security.GenerateChunked(sf, 50_000, func(table string, chunk []map[string]any) error {
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

	log.Printf("Data loaded: %d rows in %v (%.0f rows/s)", rows, time.Since(start).Round(time.Millisecond), float64(rows)/time.Since(start).Seconds())
}

func runBenchmark(ctx context.Context, qf queryFn, runs int, profDir string, skipSet map[int]bool, perQueryTimeout time.Duration) {
	queryNums := make([]int, 0, len(security.SecurityQueries))
	for n := range security.SecurityQueries {
		queryNums = append(queryNums, n)
	}
	sort.Ints(queryNums)

	if profDir != "" {
		os.MkdirAll(profDir, 0755)
	}

	type result struct {
		Query     int
		Name      string
		Elapsed   time.Duration
		Rows      int
		HeapMB    int64
		DeltaMB   int64
		GCPauseNs uint64
		Err       error
	}

	for run := 1; run <= runs; run++ {
		fmt.Printf("\n=== Run %d/%d ===\n", run, runs)
		var results []result
		var totalElapsed time.Duration

		for _, qNum := range queryNums {
			if skipSet[qNum] {
				fmt.Printf("Q%02d %-35s  SKIPPED\n", qNum, security.SecurityQueries[qNum].Name)
				continue
			}

			q := security.SecurityQueries[qNum]
			runtime.GC()

			var memBefore, memAfter runtime.MemStats
			runtime.ReadMemStats(&memBefore)

			var cpuFile *os.File
			if profDir != "" {
				path := fmt.Sprintf("%s/cpu-q%02d-run%d.prof", profDir, qNum, run)
				cpuFile, _ = os.Create(path)
				pprof.StartCPUProfile(cpuFile)
			}

			qCtx := ctx
			var qCancel context.CancelFunc
			if perQueryTimeout > 0 {
				qCtx, qCancel = context.WithTimeout(ctx, perQueryTimeout)
			}

			start := time.Now()
			rowCount, err := qf(qCtx, q.SQL)
			elapsed := time.Since(start)

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
			fmt.Printf("Q%02d %-35s %8s  %5d rows  heap %+4dMB  gc %4.1fms  %s\n",
				qNum, q.Name, elapsed.Round(time.Millisecond), r.Rows,
				r.DeltaMB, float64(gcPause)/1e6, status)
		}

		fmt.Printf("\n--- Summary (Run %d) ---\n", run)
		fmt.Printf("%-5s %-35s %10s %8s %10s %10s\n", "Query", "Description", "Time", "Rows", "Heap Delta", "GC Pause")
		fmt.Printf("%-5s %-35s %10s %8s %10s %10s\n", "-----", "-----", "-----", "-----", "-----", "-----")
		for _, r := range results {
			status := r.Elapsed.Round(time.Millisecond).String()
			if r.Err != nil {
				status = "FAILED"
			}
			fmt.Printf("Q%02d   %-35s %10s %8d %+8dMB %8.1fms\n",
				r.Query, r.Name, status, r.Rows, r.DeltaMB, float64(r.GCPauseNs)/1e6)
		}
		fmt.Printf("%-5s %-35s %10s\n", "", "TOTAL", totalElapsed.Round(time.Millisecond))

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

func startWorkerProfiling(nc *nats.Conn) {
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		log.Printf("warning: could not subscribe for profile start responses: %v", err)
		return
	}
	defer sub.Unsubscribe()

	nc.PublishRequest(distrib.SubjectProfileStart, inbox, nil)
	for {
		_, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			break
		}
	}
	log.Println("Worker CPU profiling started")
}

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
		if len(wp.Block) > 0 {
			path := filepath.Join(profDir, fmt.Sprintf("worker-%s-block.prof", wp.WorkerID))
			os.WriteFile(path, wp.Block, 0644)
			log.Printf("Saved worker block profile: %s (%d bytes)", path, len(wp.Block))
		}
		if len(wp.Mutex) > 0 {
			path := filepath.Join(profDir, fmt.Sprintf("worker-%s-mutex.prof", wp.WorkerID))
			os.WriteFile(path, wp.Mutex, 0644)
			log.Printf("Saved worker mutex profile: %s (%d bytes)", path, len(wp.Mutex))
		}
		if len(wp.Goroutine) > 0 {
			path := filepath.Join(profDir, fmt.Sprintf("worker-%s-goroutine.prof", wp.WorkerID))
			os.WriteFile(path, wp.Goroutine, 0644)
			log.Printf("Saved worker goroutine profile: %s (%d bytes)", path, len(wp.Goroutine))
		}
		collected++
	}
	log.Printf("Collected profiles from %d/%d workers", collected, workerCount)
}
