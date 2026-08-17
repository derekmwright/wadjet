// clickbench-bench runs the ClickBench 43-query suite against an embedded
// wadjet DB over local hits parquet parts, following the official
// methodology (https://github.com/ClickHouse/ClickBench):
//
//   - per query: drop the OS page cache, then run 3 tries; the first try
//     is the cold run, tries 2-3 are warm
//   - emit the official results JSON (system/date/machine/tags/load_time/
//     data_size/result), one [cold, warm, warm] triple per query
//
// Data layout: --data-dir points at a directory of hits_*.parquet parts
// (athena_partitioned). With --s3-bucket set, parts are first downloaded
// there from S3 (the download is NOT part of load_time; load_time covers
// registration, plus ANALYZE when --analyze is set — wadjet queries
// parquet in place, there is no import step).
//
// Usage (local smoke):
//
//	clickbench-bench --data-dir ~/hits-parts --out results.json
//
// Usage (EC2 official run):
//
//	clickbench-bench --s3-bucket wadjet-bench-clickbench-use2 \
//	  --s3-region us-east-2 --s3-prefix tables/hits/ \
//	  --data-dir /data/hits --machine "c6a.4xlarge, 500gb gp2" \
//	  --out results.json
package main

import (
	"context"
	_ "embed"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	runtimepprof "runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/benchnotify"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

//go:embed queries.sql
var embeddedQueries string

type resultsJSON struct {
	System      string      `json:"system"`
	Date        string      `json:"date"`
	Machine     string      `json:"machine"`
	ClusterSize int         `json:"cluster_size"`
	Comment     string      `json:"comment"`
	Tags        []string    `json:"tags"`
	LoadTime    float64     `json:"load_time"`
	DataSize    int64       `json:"data_size"`
	Result      [][]float64 `json:"result"`
}

// notifier is the process-wide push-event sink, nil (disabled) unless
// --notify-sqs-url is set. Only the PARENT builds one: isolate children
// inherit the parent's argv, and the parent already knows every child's
// timings, so a child-side emitter would only duplicate events.
var notifier *benchnotify.Notifier

// fatalf emits a terminal fatal event, then aborts like log.Fatalf. Inline,
// because log.Fatal's os.Exit skips deferred flushes.
func fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	notifier.Fatal("%s", msg)
	log.Fatal(msg)
}

func main() {
	var (
		dataDir         = flag.String("data-dir", "", "Directory containing hits_*.parquet parts (required)")
		s3Bucket        = flag.String("s3-bucket", "", "S3 bucket to download parts from (optional; skips files already present)")
		s3Region        = flag.String("s3-region", "us-east-2", "S3 region")
		s3Endpoint      = flag.String("s3-endpoint", "", "S3 endpoint override (empty = AWS)")
		s3Prefix        = flag.String("s3-prefix", "tables/hits/", "S3 key prefix for the parts")
		tries           = flag.Int("tries", 3, "Runs per query (first is cold)")
		dropCaches      = flag.Bool("drop-caches", true, "Drop the OS page cache before each query's first try (needs root; silently skipped if not possible)")
		queryTimeout    = flag.Duration("query-timeout", 10*time.Minute, "Per-query timeout")
		out             = flag.String("out", "results.json", "Results JSON output path")
		machine         = flag.String("machine", "", "Machine description for the results JSON (e.g. \"c6a.4xlarge, 500gb gp2\")")
		comment         = flag.String("comment", "", "Comment for the results JSON")
		queriesPath     = flag.String("queries", "", "Query file override (default: embedded queries.sql)")
		pprofAddr       = flag.String("pprof-addr", "", "Start a net/http/pprof server on this address (e.g. localhost:6060) for live profiling")
		spillDir        = flag.String("spill-dir", "", "Spill directory (default: <data-dir>/../wadjet-spill). MUST be real disk: the os temp dir default of the engine is tmpfs on AL2023, so spill runs consumed RAM and helped OOM-kill the c6a on high-cardinality aggregation")
		memBudget       = flag.Int64("mem-budget", 0, "Per-query memory budget in bytes (0 = auto-detect: 75% of the memory limit)")
		isolate         = flag.Bool("isolate", true, "Run each query in a fresh child process (the duckdb-entry pattern: one invocation per query). A query that OOMs or crashes becomes a null result instead of aborting the suite; per-process registration costs ~0.05s (footer reads)")
		childQuery      = flag.String("child-query", "", "internal: run this single query (isolate child mode) and print per-try seconds as JSON on the last stdout line")
		profileQueries  = flag.String("profile-queries", "", "comma-separated query numbers to CPU/heap-profile in an EXTRA isolate pass after the timed suite (profiles never contaminate the official triples); output under --profile-dir")
		profileDir      = flag.String("profile-dir", "/root/profiles", "directory for per-query pprof output from --profile-queries")
		cpuProfile      = flag.String("cpuprofile", "", "internal: child writes a CPU profile of its tries here")
		memProfile      = flag.String("memprofile", "", "internal: child writes a heap profile after its tries here")
		childQNum       = flag.Int("child-qnum", 0, "internal: real query number for the child's per-try log lines — without it every isolate child logs itself as Q00, which misreads as Q01 when tailing the live log")
		analyze         = flag.Bool("analyze", false, "Run ANALYZE (per-column HLL) after registration. Off by default: ClickBench has no joins, planner NDV buys little, and HLL over 105 columns dominates load_time (~24s per 1M-row part)")
		notifySQSURL    = flag.String("notify-sqs-url", "", "SQS queue URL for push lifecycle events (run/query/suite/fatal). Empty = disabled. See internal/benchnotify for the event schema and deploy/benchmark/watch-events.sh for the consumer.")
		notifySQSRegion = flag.String("notify-sqs-region", "", "Region for --notify-sqs-url (default: derived from the queue URL)")
		notifyRunID     = flag.String("notify-run-id", "", "run_id stamped on every event (default: a UTC timestamp). The terraform userdata passes the results timestamp so events and s3://<bucket>/results/clickbench/<run_id>/ share an id.")
	)
	flag.Parse()

	// Parent only: an isolate child inherits argv, and duplicate per-try
	// events from 43 children would be worse than none.
	if *childQuery == "" {
		notifier = benchnotify.New(context.Background(), benchnotify.Config{
			QueueURL: *notifySQSURL,
			Region:   *notifySQSRegion,
			RunID:    *notifyRunID,
			Logf:     log.Printf,
		})
		notifier.Send(benchnotify.Event{Event: benchnotify.EventRunStarted, TotalRuns: *tries})
	}

	if *dataDir == "" {
		fatalf("--data-dir is required")
	}

	// GOMEMLIMIT safety net. An explicit GOMEMLIMIT env (runtime already
	// honors it) wins — needed for reduced-scale memory repros.
	//
	// GOGC stays at its DEFAULT (on), deliberately diverging from
	// tpch-bench's GOGC=off policy (which serves a large long-lived LRU
	// file-cache heap). ClickBench aggregation is allocation-heavy on 16
	// cores; with GOGC off, garbage accumulates until the GOMEMLIMIT
	// cliff, where every cycle scans a ~limit-sized heap under assist
	// storms — Q13-Q15 ran 163s/157s/145s on the c6a against 3s/2s/3s
	// with default GC locally, and on Q19 the allocation rate overshot
	// GOMEMLIMIT past physical RAM and the kernel killed the process
	// (recon runs #2-#4, 2026-08-16).
	if memLimit := memory.DetectMemoryLimit(); memLimit > 0 && os.Getenv("GOMEMLIMIT") == "" {
		goMemLimit := memLimit * 9 / 10
		debug.SetMemoryLimit(goMemLimit)
		log.Printf("GOMEMLIMIT=%.1f GB, GOGC=default", float64(goMemLimit)/(1<<30))
	}

	if *pprofAddr != "" {
		go func() {
			log.Printf("pprof listening on %s", *pprofAddr)
			log.Println(http.ListenAndServe(*pprofAddr, nil))
		}()
	}

	ctx := context.Background()

	if *s3Bucket != "" && *childQuery == "" {
		if err := downloadParts(ctx, *s3Endpoint, *s3Region, *s3Bucket, *s3Prefix, *dataDir); err != nil {
			fatalf("downloading parts: %v", err)
		}
	}

	parts, dataSize, err := listParts(*dataDir)
	if err != nil {
		fatalf("%v", err)
	}
	if len(parts) == 0 {
		fatalf("no .parquet parts under %s", *dataDir)
	}
	log.Printf("%d parts, %.2f GB", len(parts), float64(dataSize)/(1<<30))

	queries := loadQueries(*queriesPath)
	log.Printf("%d queries", len(queries))

	// Register the hits table. load_time = registration + ANALYZE (wadjet
	// queries the parquet in place; there is no import step).
	loadStart := time.Now()
	sd := *spillDir
	if sd == "" {
		sd = filepath.Join(filepath.Dir(strings.TrimRight(*dataDir, "/")), "wadjet-spill")
	}
	if err := os.MkdirAll(sd, 0o755); err != nil {
		fatalf("spill dir %s: %v", sd, err)
	}
	log.Printf("spill dir: %s", sd)

	db, err := openHitsDB(ctx, *dataDir, parts, *analyze, *memBudget, sd)
	if err != nil {
		fatalf("opening db: %v", err)
	}
	defer db.Close()
	loadTime := time.Since(loadStart)
	log.Printf("hits registered in %.2fs", loadTime.Seconds())

	if *childQuery == "" {
		// Sanity: COUNT(*) must be positive before timing anything.
		res, err := db.Query(ctx, "SELECT COUNT(*) FROM hits")
		if err != nil || len(res.Rows) != 1 {
			fatalf("sanity COUNT(*): rows=%v err=%v", res, err)
		}
		for _, v := range res.Rows[0] {
			log.Printf("hits rows: %v", v)
		}
	}

	if *childQuery != "" {
		// Isolate child: one query, N tries, JSON triple on the last line.
		if *cpuProfile != "" {
			f, err := os.Create(*cpuProfile)
			if err != nil {
				fatalf("cpuprofile: %v", err)
			}
			if err := runtimepprof.StartCPUProfile(f); err != nil {
				fatalf("cpuprofile start: %v", err)
			}
			defer func() { runtimepprof.StopCPUProfile(); f.Close() }()
		}
		triesOut := runQueryTries(ctx, db, *childQuery, *tries, *queryTimeout, *childQNum)
		if *memProfile != "" {
			f, err := os.Create(*memProfile)
			if err == nil {
				runtime.GC()
				_ = runtimepprof.WriteHeapProfile(f)
				f.Close()
			}
		}
		buf, _ := json.Marshal(triesOut)
		fmt.Println(string(buf))
		return
	}

	results := make([][]float64, 0, len(queries))
	for qi, q := range queries {
		if *dropCaches {
			dropPageCache()
		}
		var triesOut []float64
		if *isolate {
			triesOut = runQueryIsolated(qi+1, q, *tries)
		} else {
			triesOut = runQueryTries(ctx, db, q, *tries, *queryTimeout, qi+1)
		}
		results = append(results, triesOut)
		notifyTries(qi+1, triesOut)
	}

	// Terminal event, sent before the optional profile pass so a slow
	// profiling rerun cannot delay the watcher's exit. cold/hot follow the
	// official scoring (benchmarks/clickbench/rank.py): cold = try 1,
	// hot = min(try 2, try 3).
	cold, hot, total := coldHotTotals(results)
	notifier.Send(benchnotify.Event{
		Event:        benchnotify.EventSuiteCompleted,
		TotalRuns:    *tries,
		ColdSeconds:  benchnotify.SecondsFloat(cold),
		HotSeconds:   benchnotify.SecondsFloat(hot),
		TotalSeconds: benchnotify.SecondsFloat(total),
	})

	// Profile pass: rerun the requested queries in fresh isolate children
	// with pprof enabled, AFTER the timed suite so profiling overhead never
	// touches the official triples. Warm-oriented (no cache drop): the
	// remaining levers are hot-side.
	if *profileQueries != "" {
		if err := os.MkdirAll(*profileDir, 0o755); err != nil {
			log.Printf("profile dir: %v", err)
		}
		for _, s := range strings.Split(*profileQueries, ",") {
			qn, err := strconv.Atoi(strings.TrimSpace(s))
			if err != nil || qn < 1 || qn > len(queries) {
				log.Printf("profile-queries: skipping %q", s)
				continue
			}
			log.Printf("profiling Q%02d", qn)
			exe, err := os.Executable()
			if err != nil {
				break
			}
			args := append([]string(nil), os.Args[1:]...)
			args = append(args,
				"--child-query", queries[qn-1],
				"--child-qnum", strconv.Itoa(qn),
				"--isolate=false", "--drop-caches=false",
				"--cpuprofile", fmt.Sprintf("%s/q%02d.cpu.prof", *profileDir, qn),
				"--memprofile", fmt.Sprintf("%s/q%02d.heap.prof", *profileDir, qn),
			)
			cmd := exec.Command(exe, args...)
			cmd.Stderr = os.Stderr
			if _, err := cmd.Output(); err != nil {
				log.Printf("Q%02d profile child: %v", qn, err)
			}
		}
	}

	rj := resultsJSON{
		System:      "Wadjet",
		Date:        time.Now().UTC().Format("2006-01-02"),
		Machine:     *machine,
		ClusterSize: 1,
		Comment:     *comment,
		Tags:        []string{"Go", "column-oriented", "embedded", "Parquet"},
		LoadTime:    loadTime.Seconds(),
		DataSize:    dataSize,
		Result:      results,
	}
	buf, err := marshalResults(rj)
	if err != nil {
		fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(*out, buf, 0o644); err != nil {
		fatalf("write %s: %v", *out, err)
	}
	log.Printf("wrote %s", *out)
}

// notifyTries pushes one query_completed event per try. Called after the
// query's tries are complete — never between the timing calls — so a slow
// queue cannot land inside a measured region. The -1 sentinel (failed try,
// null in the results JSON) becomes ok:false with no wall_seconds.
func notifyTries(qNum int, tries []float64) {
	for i, secs := range tries {
		ev := benchnotify.Event{
			Event: benchnotify.EventQueryCompleted,
			Query: fmt.Sprintf("Q%02d", qNum),
			Try:   i + 1,
			OK:    benchnotify.OK(secs >= 0),
		}
		if secs >= 0 {
			ev.WallSeconds = benchnotify.SecondsFloat(secs)
		}
		notifier.Send(ev)
	}
}

// coldHotTotals sums the suite the way the official ranking reads it
// (benchmarks/clickbench/rank.py): cold = try 1, hot = min(try 2, try 3).
// Failed tries (-1) are skipped rather than counted as zero, so a partial
// suite's numbers stay comparable to the same queries elsewhere; total is
// every successful try, i.e. the timed wall of the suite itself.
func coldHotTotals(results [][]float64) (cold, hot, total float64) {
	for _, tries := range results {
		for _, secs := range tries {
			if secs >= 0 {
				total += secs
			}
		}
		if len(tries) > 0 && tries[0] >= 0 {
			cold += tries[0]
		}
		best := -1.0
		for _, secs := range tries[min(1, len(tries)):] {
			if secs >= 0 && (best < 0 || secs < best) {
				best = secs
			}
		}
		if best >= 0 {
			hot += best
		}
	}
	return cold, hot, total
}

// runQueryTries runs one query N times in-process. qNum 0 suppresses the
// per-try log prefix (child mode logs go to stderr and the parent relogs).
func runQueryTries(ctx context.Context, db *wadjet.DB, q string, tries int, timeout time.Duration, qNum int) []float64 {
	out := make([]float64, 0, tries)
	for t := 0; t < tries; t++ {
		qctx, cancel := context.WithTimeout(ctx, timeout)
		start := time.Now()
		res, err := db.Query(qctx, q)
		elapsed := time.Since(start).Seconds()
		cancel()
		if err != nil {
			log.Printf("Q%02d try %d FAILED after %.3fs: %v", qNum, t+1, elapsed, err)
			out = append(out, -1)
			continue
		}
		log.Printf("Q%02d try %d: %.3fs (%d rows)", qNum, t+1, elapsed, len(res.Rows))
		out = append(out, elapsed)
	}
	return out
}

// runQueryIsolated re-execs this binary in child mode for one query. A
// child that dies (OOM kill, panic) yields nulls for all its tries; the
// suite continues — the duckdb-entry one-process-per-query pattern.
func runQueryIsolated(qNum int, q string, tries int) []float64 {
	nulls := make([]float64, tries)
	for i := range nulls {
		nulls[i] = -1
	}
	exe, err := os.Executable()
	if err != nil {
		log.Printf("Q%02d: cannot resolve executable: %v", qNum, err)
		return nulls
	}
	args := append([]string(nil), os.Args[1:]...)
	args = append(args, "--child-query", q, "--child-qnum", strconv.Itoa(qNum), "--isolate=false", "--drop-caches=false")
	cmd := exec.Command(exe, args...)
	cmd.Stderr = os.Stderr
	outBuf, err := cmd.Output()
	if err != nil {
		log.Printf("Q%02d CHILD DIED: %v", qNum, err)
		return nulls
	}
	lines := strings.Split(strings.TrimSpace(string(outBuf)), "\n")
	var triesOut []float64
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &triesOut); err != nil || len(triesOut) != tries {
		log.Printf("Q%02d: bad child output %q: %v", qNum, lines[len(lines)-1], err)
		return nulls
	}
	for t, v := range triesOut {
		if v >= 0 {
			log.Printf("Q%02d try %d: %.3fs", qNum, t+1, v)
		} else {
			log.Printf("Q%02d try %d FAILED (child)", qNum, t+1)
		}
	}
	return triesOut
}

// marshalResults renders the official JSON, encoding failed tries (-1) as
// null per the ClickBench convention.
func marshalResults(rj resultsJSON) ([]byte, error) {
	type rawResults struct {
		System      string           `json:"system"`
		Date        string           `json:"date"`
		Machine     string           `json:"machine"`
		ClusterSize int              `json:"cluster_size"`
		Comment     string           `json:"comment"`
		Tags        []string         `json:"tags"`
		LoadTime    float64          `json:"load_time"`
		DataSize    int64            `json:"data_size"`
		Result      [][]*json.Number `json:"result"`
	}
	raw := rawResults{
		System: rj.System, Date: rj.Date, Machine: rj.Machine,
		ClusterSize: rj.ClusterSize, Comment: rj.Comment, Tags: rj.Tags,
		LoadTime: rj.LoadTime, DataSize: rj.DataSize,
	}
	for _, triple := range rj.Result {
		row := make([]*json.Number, len(triple))
		for i, v := range triple {
			if v >= 0 {
				n := json.Number(fmt.Sprintf("%.3f", v))
				row[i] = &n
			}
		}
		raw.Result = append(raw.Result, row)
	}
	return json.MarshalIndent(raw, "", "  ")
}

func loadQueries(path string) []string {
	text := embeddedQueries
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			fatalf("reading %s: %v", path, err)
		}
		text = string(b)
	}
	var queries []string
	for _, line := range strings.Split(text, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			queries = append(queries, line)
		}
	}
	return queries
}

func listParts(dir string) ([]string, int64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, 0, fmt.Errorf("reading %s: %w", dir, err)
	}
	var parts []string
	var total int64
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".parquet") {
			continue
		}
		info, err := e.Info()
		if err != nil {
			return nil, 0, err
		}
		parts = append(parts, e.Name())
		total += info.Size()
	}
	return parts, total, nil
}

// openHitsDB opens an embedded DB over a FileStore rooted at the parent of
// dataDir (bucket = its basename) and registers the hits table with the
// arc's catalog type mapping: BYTE_ARRAY columns register as String (the
// athena parquet carries no UTF8 annotation but every byte-array column is
// text), EventDate as DATE (int32 days, same storage class as the file's
// INT16-annotated INT32 — the catalog-level equivalent of the official
// DuckDB view's make_date rewrite). EventTime stays INT64 epoch-seconds:
// wadjet's temporal scalar functions take int64 seconds directly.
func openHitsDB(ctx context.Context, dataDir string, parts []string, analyze bool, memBudget int64, spillDir string) (*wadjet.DB, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	root, bucket := filepath.Dir(abs), filepath.Base(abs)
	store, err := objstore.NewFileStore(root)
	if err != nil {
		return nil, fmt.Errorf("filestore: %w", err)
	}

	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: bucket, MemoryBudget: memBudget, SpillDir: spillDir})
	if err != nil {
		return nil, err
	}

	// Probe schema from the first part.
	ra, sz, err := store.GetReaderAt(ctx, bucket, parts[0])
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", parts[0], err)
	}
	pr, err := parquet.NewReaderAt(ra, sz)
	if err != nil {
		ra.Close()
		return nil, fmt.Errorf("probing schema: %w", err)
	}
	schema := pr.Schema()
	ra.Close()
	for i := range schema.Columns {
		if schema.Columns[i].Type == parquet.TypeBytes {
			schema.Columns[i].Type = parquet.TypeString
		}
		if schema.Columns[i].Name == "EventDate" {
			schema.Columns[i].Type = parquet.TypeDate
		}
	}
	if err := db.CreateTable(ctx, "hits", schema, nil); err != nil && !strings.Contains(err.Error(), "already exists") {
		return nil, fmt.Errorf("create table: %w", err)
	}

	// Footer-probe every part for real row counts (planner estimates).
	files := make([]catalog.FileEntry, 0, len(parts))
	var totalRows int64
	for _, name := range parts {
		ra, sz, err := store.GetReaderAt(ctx, bucket, name)
		if err != nil {
			return nil, fmt.Errorf("opening %s: %w", name, err)
		}
		r, err := parquet.NewReaderAt(ra, sz)
		if err != nil {
			ra.Close()
			return nil, fmt.Errorf("probing %s: %w", name, err)
		}
		numRows := r.NumRows()
		ra.Close()
		totalRows += numRows
		files = append(files, catalog.FileEntry{
			Path:      name,
			SizeBytes: sz,
			NumRows:   numRows,
			CreatedAt: time.Now(),
		})
	}
	if err := db.Catalog().AddFiles(ctx, "hits", nil, "", files); err != nil {
		return nil, fmt.Errorf("registering files: %w", err)
	}
	log.Printf("registered %d parts, %d rows", len(files), totalRows)

	// ANALYZE: per-column HLL sketches for planner NDV. Opt-in — see flag.
	if analyze {
		if analyzed, err := db.Catalog().AnalyzeTable(ctx, "hits"); err != nil {
			log.Printf("WARNING: ANALYZE hits failed (%v); planner falls back to heuristic NDV", err)
		} else {
			log.Printf("ANALYZE hits: HLL collected on %d files", analyzed)
		}
	}
	return db, nil
}

// downloadParts fetches every object under prefix into dataDir, skipping
// files that already exist with the right size.
func downloadParts(ctx context.Context, endpoint, region, bucket, prefix, dataDir string) error {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	if endpoint == "" {
		// The MinIO client needs a concrete endpoint even for AWS
		// (same convention as tpch-bench / run-benchmark.sh).
		endpoint = "s3." + region + ".amazonaws.com"
	}
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint: endpoint,
		UseSSL:   true,
		Region:   region,
	})
	if err != nil {
		return fmt.Errorf("s3 store: %w", err)
	}
	objects, err := store.List(ctx, bucket, objstore.ListOptions{Prefix: prefix})
	if err != nil {
		return fmt.Errorf("listing s3://%s/%s: %w", bucket, prefix, err)
	}
	start := time.Now()
	var fetched, skipped int
	var bytes int64
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, ".parquet") {
			continue
		}
		dst := filepath.Join(dataDir, filepath.Base(obj.Key))
		if st, err := os.Stat(dst); err == nil && st.Size() == obj.Size {
			skipped++
			continue
		}
		rc, _, err := store.Get(ctx, bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("get %s: %w", obj.Key, err)
		}
		f, err := os.Create(dst)
		if err != nil {
			rc.Close()
			return err
		}
		n, err := io.Copy(f, rc)
		rc.Close()
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		if err != nil {
			return fmt.Errorf("downloading %s: %w", obj.Key, err)
		}
		bytes += n
		fetched++
	}
	log.Printf("downloaded %d parts (%.2f GB) in %.1fs, %d already present",
		fetched, float64(bytes)/(1<<30), time.Since(start).Seconds(), skipped)
	return nil
}

// dropPageCache drops the OS page cache (official methodology: before each
// query's first try). Needs root; failures are logged once and skipped.
var dropCacheWarned bool

func dropPageCache() {
	if err := exec.Command("sync").Run(); err != nil {
		warnDropCache(err)
		return
	}
	if err := os.WriteFile("/proc/sys/vm/drop_caches", []byte("3"), 0o200); err != nil {
		warnDropCache(err)
		return
	}
}

func warnDropCache(err error) {
	if !dropCacheWarned {
		dropCacheWarned = true
		log.Printf("WARNING: cannot drop page cache (%v) — cold tries are not OS-cold", err)
	}
}
