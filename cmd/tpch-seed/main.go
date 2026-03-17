// Command tpch-seed generates TPC-H data and loads it into an S3-compatible
// object store for reusable benchmarking. The data persists in the bucket so
// subsequent wadjet benchmark runs can skip generation.
//
// Usage:
//
//	go run ./cmd/tpch-seed --scale=1 --endpoint=s3.amazonaws.com --bucket=wadjet-bench
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/citc-tech/wadjet/benchmarks/tpch"
	"github.com/citc-tech/wadjet/internal/storage/ingest"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/wadjet"
)

func main() {
	var (
		scale      float64
		endpoint   string
		accessKey  string
		secretKey  string
		bucket     string
		useSSL     bool
		region     string
		chunkSize  int
		rgSize     int
		skipExists bool
	)

	flag.Float64Var(&scale, "scale", 1.0, "TPC-H scale factor (0.01, 0.1, 1, 10, 100)")
	flag.StringVar(&endpoint, "endpoint", envOr("S3_ENDPOINT", ""), "S3-compatible endpoint (e.g., s3.amazonaws.com, localhost:9000)")
	flag.StringVar(&accessKey, "access-key", envOr("AWS_ACCESS_KEY_ID", ""), "S3 access key (or AWS_ACCESS_KEY_ID)")
	flag.StringVar(&secretKey, "secret-key", envOr("AWS_SECRET_ACCESS_KEY", ""), "S3 secret key (or AWS_SECRET_ACCESS_KEY)")
	flag.StringVar(&bucket, "bucket", envOr("S3_BUCKET", "wadjet-bench"), "S3 bucket name")
	flag.BoolVar(&useSSL, "ssl", true, "use TLS for S3 connection")
	flag.StringVar(&region, "region", envOr("AWS_REGION", "us-east-1"), "S3 region")
	flag.IntVar(&chunkSize, "chunk-size", 50_000, "rows per generation chunk (controls memory)")
	flag.IntVar(&rgSize, "row-group-size", 131_072, "Parquet row group size")
	flag.BoolVar(&skipExists, "skip-if-exists", false, "exit successfully if bucket already contains TPC-H data")
	flag.Parse()

	if endpoint == "" {
		fmt.Fprintln(os.Stderr, "error: --endpoint is required (or set S3_ENDPOINT)")
		flag.Usage()
		os.Exit(1)
	}
	if accessKey == "" || secretKey == "" {
		fmt.Fprintln(os.Stderr, "error: --access-key and --secret-key are required (or set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY)")
		flag.Usage()
		os.Exit(1)
	}

	sf := toScaleFactor(scale)
	counts := sf.RowCounts()
	totalRows := counts.Region + counts.Nation + counts.Supplier + counts.Part +
		counts.PartSupp + counts.Customer + counts.Orders + counts.LineItem

	log.Printf("TPC-H seed: SF=%.2f (~%dM rows), bucket=%s, endpoint=%s",
		scale, totalRows/1_000_000, bucket, endpoint)

	ctx := context.Background()

	// Connect to S3
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    useSSL,
		Region:    region,
	})
	if err != nil {
		log.Fatalf("connecting to S3: %v", err)
	}

	// Create bucket if needed
	if err := store.MakeBucket(ctx, bucket); err != nil {
		log.Fatalf("creating bucket %q: %v", bucket, err)
	}
	log.Printf("Bucket %q ready", bucket)

	// Check if data already exists
	existing, err := store.List(ctx, bucket, objstore.ListOptions{
		Prefix:  "tables/",
		MaxKeys: 1,
	})
	if err != nil {
		log.Fatalf("listing bucket: %v", err)
	}
	if len(existing) > 0 {
		if skipExists {
			log.Printf("Bucket already contains TPC-H data — skipping (--skip-if-exists)")
			os.Exit(0)
		}
		log.Printf("WARNING: bucket already contains data under tables/")
		log.Printf("  Existing data will be overwritten by new table creation.")
		log.Printf("  Use --skip-if-exists to make this a no-op when data is present.")
	}

	// Open wadjet DB against S3
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: bucket,
	})
	if err != nil {
		log.Fatalf("opening DB: %v", err)
	}

	// Create all 8 TPC-H tables
	for tableName, schema := range tpch.AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			log.Fatalf("creating table %s: %v", tableName, err)
		}
		log.Printf("Created table: %s (%d columns)", tableName, len(schema.Columns))
	}

	// Create ingesters with auto-flush thresholds sized for streaming
	ingesters := make(map[string]*ingest.Ingester)
	for tableName, schema := range tpch.AllTables {
		ingesters[tableName] = db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: 200_000,
			RowGroupSize:  rgSize,
		})
	}

	// Stream-generate data in bounded-memory chunks
	start := time.Now()
	rowsSoFar := 0
	lastLog := time.Now()
	tableRows := make(map[string]int)

	log.Printf("Generating and uploading TPC-H SF=%.2f data...", scale)

	err = tpch.GenerateChunked(sf, chunkSize, func(table string, rows []map[string]any) error {
		rowsSoFar += len(rows)
		tableRows[table] += len(rows)

		if time.Since(lastLog) > 3*time.Second {
			pct := float64(rowsSoFar) / float64(totalRows) * 100
			elapsed := time.Since(start).Round(time.Second)
			rate := float64(rowsSoFar) / time.Since(start).Seconds()
			log.Printf("  %dM / %dM rows (%.0f%%) in %v [%.0f rows/s] — current: %s",
				rowsSoFar/1_000_000, totalRows/1_000_000, pct, elapsed, rate, table)
			lastLog = time.Now()
		}

		return ingesters[table].Ingest(ctx, rows)
	})
	if err != nil {
		log.Fatalf("generating data: %v", err)
	}

	// Flush remaining buffered data
	log.Printf("Flushing remaining buffers to S3...")
	for tableName, ing := range ingesters {
		if err := ing.FlushAll(ctx); err != nil {
			log.Fatalf("flushing %s: %v", tableName, err)
		}
	}

	elapsed := time.Since(start).Round(time.Second)
	rate := float64(rowsSoFar) / time.Since(start).Seconds()

	log.Printf("Done! TPC-H SF=%.2f seeded in %v (%.0f rows/s)", scale, elapsed, rate)
	log.Printf("")
	log.Printf("Table summary:")
	for _, name := range []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"} {
		log.Printf("  %-12s %10d rows", name, tableRows[name])
	}

	// Memory report
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	log.Printf("")
	log.Printf("Peak memory: heap=%dMB, sys=%dMB", mem.HeapAlloc/1024/1024, mem.Sys/1024/1024)
	log.Printf("")
	log.Printf("To run benchmarks against this data:")
	log.Printf("  wadjet serve --mode=standalone --endpoint=%s --bucket=%s --access-key=... --secret-key=...", endpoint, bucket)
}

func toScaleFactor(f float64) tpch.ScaleFactor {
	switch {
	case f <= 0.01:
		return tpch.SF001
	case f <= 0.1:
		return tpch.SF01
	case f <= 1.0:
		return tpch.SF1
	case f <= 10.0:
		return tpch.SF10
	default:
		return tpch.SF100
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
