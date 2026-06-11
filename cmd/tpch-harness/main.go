package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/citc-tech/wadjet/internal/harness"
)

func main() {
	var (
		mode           = flag.String("mode", "", "run mode: local or golden (required)")
		slice          = flag.String("slice", "small", "data slice for local mode: small or large")
		coordURL       = flag.String("coord-url", "", "pgwire URL of an existing coordinator (golden mode)")
		dataDir        = flag.String("data-dir", "/tmp/sf100-sample", "directory containing TPC-H sample files (local mode)")
		baselinePath   = flag.String("baseline", "benchmarks/tpch/baseline-sf100.json", "path to baseline JSON")
		outPath        = flag.String("out", "./harness-result.json", "path to write result JSON")
		queries        = flag.String("queries", "", "comma-separated query names (default: all 22 + micros)")
		updateBaseline = flag.Bool("update-baseline", false, "(golden only) write the result directly to --baseline")
		noCompare      = flag.Bool("no-compare", false, "skip baseline comparison, just emit measurements")
		wadjetBin      = flag.String("wadjet-bin", "", "path to wadjet binary (default: $WADJET_BIN or ./wadjet)")
		pgAddr         = flag.String("pg-addr", ":15433", "pgwire listen address for the spawned coordinator")
		source         = flag.String("source", "local", "data source: local (files under --data-dir) or s3 (coordinator reads from --bucket)")
		bucket         = flag.String("bucket", "wadjet-bench-sf10-use2", "S3 bucket name (source=s3)")
		region         = flag.String("region", "us-east-2", "S3 region (source=s3)")
		endpoint       = flag.String("endpoint", "s3.us-east-2.amazonaws.com", "S3 endpoint (source=s3)")
		ssl            = flag.Bool("ssl", true, "use SSL for S3 (source=s3)")
		dataPrefix     = flag.String("data-prefix", "tables/", "S3 prefix under --bucket containing table data (source=s3)")
		numWorkers     = flag.Int("workers", 0, "number of worker processes to spawn (local mode); 0 = default of 2. Set to 1 to avoid 2-process memory overcommit on small dev boxes when measuring SF10+.")
		scaleFactor    = flag.Float64("scale-factor", 0, "TPC-H scale factor for local-mode generated data (0=SF0.01 default, 0.1, 1, 10). Larger values exercise per-stage round-trip overhead and memory paths at meaningful scale without EC2 spend.")
		dataPlane      = flag.String("data-plane", "", "Worker↔coord transport: empty/nats (default) or grpc (Phase B+)")
		serveArgs      = flag.String("serve-args", "", "comma-separated extra flags appended to every spawned `wadjet serve` (coord + workers), e.g. --bounded-dirty-writes")
		spawnWrapper   = flag.String("spawn-wrapper", "", "space-separated command prefix wrapped around every spawned wadjet process, e.g. 'deploy/edge/cap-wrapper.sh' for docker memory-cap edge simulation")
	)
	flag.Parse()

	if *mode == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --mode is required (local or golden)")
		flag.Usage()
		os.Exit(harness.ExitSetup)
	}
	if *source != "local" && *source != "s3" {
		fmt.Fprintln(os.Stderr, "ERROR: --source must be 'local' or 's3'")
		os.Exit(harness.ExitSetup)
	}

	cfg := harness.Config{
		Mode:           harness.Mode(*mode),
		Slice:          harness.Slice(*slice),
		CoordURL:       *coordURL,
		DataDir:        *dataDir,
		BaselinePath:   *baselinePath,
		OutPath:        *outPath,
		UpdateBaseline: *updateBaseline,
		NoCompare:      *noCompare,
		WadjetBin:      *wadjetBin,
		PgAddr:         *pgAddr,
		NumWorkers:     *numWorkers,
		ScaleFactor:    *scaleFactor,
		Source:         *source,
		Bucket:         *bucket,
		Region:         *region,
		Endpoint:       *endpoint,
		SSL:            *ssl,
		DataPrefix:     *dataPrefix,
		DataPlane:      *dataPlane,
	}
	if *queries != "" {
		cfg.Queries = strings.Split(*queries, ",")
	}
	if *serveArgs != "" {
		cfg.ExtraServeArgs = strings.Split(*serveArgs, ",")
	}
	if *spawnWrapper != "" {
		cfg.SpawnWrapper = strings.Fields(*spawnWrapper)
	}
	if cfg.WadjetBin == "" {
		cfg.WadjetBin = os.Getenv("WADJET_BIN")
	}
	if cfg.WadjetBin == "" {
		cfg.WadjetBin = "./wadjet"
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigC := make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigC
		logger.Info("signal received, cancelling")
		cancel()
	}()

	result, err := harness.Run(ctx, cfg, logger)
	if err != nil {
		logger.Error("harness run failed", "err", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(harness.ExitSetup)
	}

	printSummary(result)
	os.Exit(result.ExitCode)
}

func printSummary(r harness.RunResult) {
	fmt.Printf("\n=== harness %s/%s — %s ===\n", r.Mode, r.Slice, statusWord(r.Passed))
	fmt.Printf("ran %d queries in %d ms\n", len(r.Queries), r.DurationMs)
	for _, q := range r.Queries {
		marker := " "
		if q.Hung {
			marker = "H"
		}
		fmt.Printf("  [%s] %-20s wall=%6d ms peak=%5d MB rows=%d\n",
			marker, q.Query, q.WallMs, q.PeakHeapMB, q.RowCount)
	}
	if len(r.Regressions) > 0 {
		fmt.Println("regressions:")
		for _, d := range r.Regressions {
			fmt.Printf("  %s.%s drift=%.1f%% (tol=%.0f%%)\n", d.Query, d.Metric, d.DriftPct, d.TolerancePct)
		}
	}
}

func statusWord(passed bool) string {
	if passed {
		return "PASS"
	}
	return "FAIL"
}
