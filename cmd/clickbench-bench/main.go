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
	"runtime/debug"
	"strings"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
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

func main() {
	var (
		dataDir      = flag.String("data-dir", "", "Directory containing hits_*.parquet parts (required)")
		s3Bucket     = flag.String("s3-bucket", "", "S3 bucket to download parts from (optional; skips files already present)")
		s3Region     = flag.String("s3-region", "us-east-2", "S3 region")
		s3Endpoint   = flag.String("s3-endpoint", "", "S3 endpoint override (empty = AWS)")
		s3Prefix     = flag.String("s3-prefix", "tables/hits/", "S3 key prefix for the parts")
		tries        = flag.Int("tries", 3, "Runs per query (first is cold)")
		dropCaches   = flag.Bool("drop-caches", true, "Drop the OS page cache before each query's first try (needs root; silently skipped if not possible)")
		queryTimeout = flag.Duration("query-timeout", 10*time.Minute, "Per-query timeout")
		out          = flag.String("out", "results.json", "Results JSON output path")
		machine      = flag.String("machine", "", "Machine description for the results JSON (e.g. \"c6a.4xlarge, 500gb gp2\")")
		comment      = flag.String("comment", "", "Comment for the results JSON")
		queriesPath  = flag.String("queries", "", "Query file override (default: embedded queries.sql)")
		pprofAddr    = flag.String("pprof-addr", "", "Start a net/http/pprof server on this address (e.g. localhost:6060) for live profiling")
		memBudget    = flag.Int64("mem-budget", 0, "Per-query memory budget in bytes (0 = auto-detect: 75% of the memory limit)")
		analyze      = flag.Bool("analyze", false, "Run ANALYZE (per-column HLL) after registration. Off by default: ClickBench has no joins, planner NDV buys little, and HLL over 105 columns dominates load_time (~24s per 1M-row part)")
	)
	flag.Parse()

	if *dataDir == "" {
		log.Fatal("--data-dir is required")
	}

	// GOMEMLIMIT safety net, same policy as tpch-bench. An explicit
	// GOMEMLIMIT env (runtime already honors it) wins — needed for
	// reduced-scale memory repros.
	if memLimit := memory.DetectMemoryLimit(); memLimit > 0 && os.Getenv("GOMEMLIMIT") == "" {
		goMemLimit := memLimit * 9 / 10
		debug.SetMemoryLimit(goMemLimit)
		debug.SetGCPercent(-1)
		log.Printf("GOMEMLIMIT=%.1f GB, GOGC=off", float64(goMemLimit)/(1<<30))
	}

	if *pprofAddr != "" {
		go func() {
			log.Printf("pprof listening on %s", *pprofAddr)
			log.Println(http.ListenAndServe(*pprofAddr, nil))
		}()
	}

	ctx := context.Background()

	if *s3Bucket != "" {
		if err := downloadParts(ctx, *s3Endpoint, *s3Region, *s3Bucket, *s3Prefix, *dataDir); err != nil {
			log.Fatalf("downloading parts: %v", err)
		}
	}

	parts, dataSize, err := listParts(*dataDir)
	if err != nil {
		log.Fatal(err)
	}
	if len(parts) == 0 {
		log.Fatalf("no .parquet parts under %s", *dataDir)
	}
	log.Printf("%d parts, %.2f GB", len(parts), float64(dataSize)/(1<<30))

	queries := loadQueries(*queriesPath)
	log.Printf("%d queries", len(queries))

	// Register the hits table. load_time = registration + ANALYZE (wadjet
	// queries the parquet in place; there is no import step).
	loadStart := time.Now()
	db, err := openHitsDB(ctx, *dataDir, parts, *analyze, *memBudget)
	if err != nil {
		log.Fatalf("opening db: %v", err)
	}
	defer db.Close()
	loadTime := time.Since(loadStart)
	log.Printf("hits registered in %.2fs", loadTime.Seconds())

	// Sanity: COUNT(*) must be positive before timing anything.
	res, err := db.Query(ctx, "SELECT COUNT(*) FROM hits")
	if err != nil || len(res.Rows) != 1 {
		log.Fatalf("sanity COUNT(*): rows=%v err=%v", res, err)
	}
	for _, v := range res.Rows[0] {
		log.Printf("hits rows: %v", v)
	}

	results := make([][]float64, 0, len(queries))
	for qi, q := range queries {
		if *dropCaches {
			dropPageCache()
		}
		triesOut := make([]float64, 0, *tries)
		for t := 0; t < *tries; t++ {
			qctx, cancel := context.WithTimeout(ctx, *queryTimeout)
			start := time.Now()
			res, err := db.Query(qctx, q)
			elapsed := time.Since(start).Seconds()
			cancel()
			if err != nil {
				log.Printf("Q%02d try %d FAILED after %.3fs: %v", qi+1, t+1, elapsed, err)
				triesOut = append(triesOut, -1) // null-ed below in JSON
				continue
			}
			log.Printf("Q%02d try %d: %.3fs (%d rows)", qi+1, t+1, elapsed, len(res.Rows))
			triesOut = append(triesOut, elapsed)
		}
		results = append(results, triesOut)
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
		log.Fatalf("marshal: %v", err)
	}
	if err := os.WriteFile(*out, buf, 0o644); err != nil {
		log.Fatalf("write %s: %v", *out, err)
	}
	log.Printf("wrote %s", *out)
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
			log.Fatalf("reading %s: %v", path, err)
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
func openHitsDB(ctx context.Context, dataDir string, parts []string, analyze bool, memBudget int64) (*wadjet.DB, error) {
	abs, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, err
	}
	root, bucket := filepath.Dir(abs), filepath.Base(abs)
	store, err := objstore.NewFileStore(root)
	if err != nil {
		return nil, fmt.Errorf("filestore: %w", err)
	}

	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: bucket, MemoryBudget: memBudget})
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
