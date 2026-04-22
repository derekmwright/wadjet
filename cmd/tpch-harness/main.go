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
	)
	flag.Parse()

	if *mode == "" {
		fmt.Fprintln(os.Stderr, "ERROR: --mode is required (local or golden)")
		flag.Usage()
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
	}
	if *queries != "" {
		cfg.Queries = strings.Split(*queries, ",")
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
