package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	nethttppprof "net/http/pprof"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/peterh/liner"

	"github.com/citc-tech/wadjet/internal/alerts"
	"github.com/citc-tech/wadjet/internal/embedding"
	"github.com/citc-tech/wadjet/internal/engine/memory"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/config"
	"github.com/citc-tech/wadjet/internal/coordinator"
	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/format"
	"github.com/citc-tech/wadjet/internal/geoip"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/server"
	"github.com/citc-tech/wadjet/internal/server/mcp"
	"github.com/citc-tech/wadjet/internal/server/pgwire"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/compaction"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/telemetry"
	"github.com/citc-tech/wadjet/internal/worker"
	"github.com/citc-tech/wadjet/wadjet"
	"github.com/spf13/cobra"
)

var (
	mode                string
	storageType         string
	dataDir             string
	endpoint            string
	accessKey           string
	secretKey           string
	bucket              string
	httpAddr            string
	natsPort            int
	natsURL             string
	configFile          string
	clusterID           string
	leafRemotes         []string
	grpcAddr            string
	memoryBudget        int64
	sharedPoolBudget    int64
	spillFloatingBudget bool
	spillDir            string
	resultStoreBytes    int64
	pgAddr              string
	pgTLSCert           string
	pgTLSKey            string
	queryTimeout        string
	maxConcurrentQry    int
	natsStoreDir        string
	geoipCityDB         string
	geoipASNDB          string
	useSSL              bool
	s3Region            string
	maxConcurrent       int
	cacheBytes          int64
	logLevel            string
	natsTLSCert         string
	natsTLSKey          string
	natsTLSCA           string
	otelEndpoint        string
	otelInsecure        bool
	metricsAddr         string
	enableAlerts        bool
	dataPlane           string
	dataPlaneAddr       string
	coordDataPlane      string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "wadjet",
		Short: "Wadjet — lightweight distributed analytical query engine",
		Long:  "A distributed analytical query engine that uses embedded NATS for coordination and object storage for results.",
	}

	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "YAML config file path")
	rootCmd.PersistentFlags().StringVar(&mode, "mode", "standalone", "Run mode: standalone, coordinator, or worker")
	rootCmd.PersistentFlags().StringVar(&storageType, "storage-type", "s3", "Storage backend: s3 or file")
	rootCmd.PersistentFlags().StringVar(&dataDir, "data-dir", "", "Local data directory (for --storage-type=file)")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "localhost:9000", "S3-compatible endpoint")
	rootCmd.PersistentFlags().StringVar(&accessKey, "access-key", "", "S3 access key (empty = auto-detect from env/IAM)")
	rootCmd.PersistentFlags().StringVar(&secretKey, "secret-key", "", "S3 secret key (empty = auto-detect from env/IAM)")
	rootCmd.PersistentFlags().BoolVar(&useSSL, "ssl", false, "Use TLS for S3 connections")
	rootCmd.PersistentFlags().StringVar(&s3Region, "region", "", "S3 region (for IAM credential signing)")
	rootCmd.PersistentFlags().StringVar(&bucket, "bucket", "wadjet", "Storage bucket name")
	rootCmd.PersistentFlags().StringVar(&httpAddr, "http-addr", ":8080", "HTTP API listen address")
	rootCmd.PersistentFlags().StringVar(&grpcAddr, "grpc-addr", ":9090", "gRPC API listen address")
	rootCmd.PersistentFlags().IntVar(&natsPort, "nats-port", 4222, "Embedded NATS port")
	rootCmd.PersistentFlags().StringVar(&natsURL, "nats-url", "", "NATS URL (for worker mode)")
	rootCmd.PersistentFlags().StringVar(&natsStoreDir, "nats-store-dir", "", "NATS JetStream storage directory (default: ~/.wadjet/nats)")
	rootCmd.PersistentFlags().StringVar(&clusterID, "cluster-id", "local", "Cluster identifier for federation")
	rootCmd.PersistentFlags().StringSliceVar(&leafRemotes, "leaf-remote", nil, "Remote NATS URLs for leaf node connections (repeatable)")
	rootCmd.PersistentFlags().StringVar(&natsTLSCert, "nats-tls-cert", "", "TLS certificate file for NATS mTLS")
	rootCmd.PersistentFlags().StringVar(&natsTLSKey, "nats-tls-key", "", "TLS private key file for NATS mTLS")
	rootCmd.PersistentFlags().StringVar(&natsTLSCA, "nats-tls-ca", "", "CA certificate file for NATS mTLS peer verification")
	rootCmd.PersistentFlags().StringVar(&otelEndpoint, "otel-endpoint", "", "OTLP gRPC endpoint for tracing (e.g., localhost:4317)")
	rootCmd.PersistentFlags().BoolVar(&otelInsecure, "otel-insecure", false, "Use plaintext gRPC for OTLP exporter")
	rootCmd.PersistentFlags().Int64Var(&memoryBudget, "memory-budget", 0, "Per-task memory budget in bytes (0 = auto-detect from cgroup, or unlimited)")
	rootCmd.PersistentFlags().Int64Var(&sharedPoolBudget, "shared-pool-budget", 0, "Worker-wide shared memory pool in bytes (0 = auto-detect: envelope minus cache). All concurrent tasks Reserve against this pool.")
	rootCmd.PersistentFlags().BoolVar(&spillFloatingBudget, "spill-floating-budget", false, "Activate the floating-budget spill threshold (deploy-gated; requires Phase-4 mmap RSS accounting). Default false = tuned static 40%/90% thresholds.")
	rootCmd.PersistentFlags().StringVar(&spillDir, "spill-dir", "", "Directory for spill files (default: OS temp dir)")
	rootCmd.PersistentFlags().Int64Var(&cacheBytes, "cache-bytes", 0, "LRU file cache size in bytes (0 = auto-detect: 20% of memory)")

	rootCmd.PersistentFlags().Int64Var(&resultStoreBytes, "result-store", 512*1024*1024, "In-memory result store capacity in bytes (0 = disabled, results pass through S3)")
	rootCmd.PersistentFlags().StringVar(&pgAddr, "pg-addr", ":5433", "PostgreSQL wire protocol listen address")
	rootCmd.PersistentFlags().StringVar(&pgTLSCert, "pg-tls-cert", "", "TLS certificate file for PostgreSQL wire protocol")
	rootCmd.PersistentFlags().StringVar(&pgTLSKey, "pg-tls-key", "", "TLS private key file for PostgreSQL wire protocol")
	rootCmd.PersistentFlags().StringVar(&queryTimeout, "query-timeout", "0", "Default query timeout (e.g. 30s, 5m, 0=unlimited)")
	rootCmd.PersistentFlags().IntVar(&maxConcurrentQry, "max-concurrent-queries", 0, "Maximum concurrent queries (0=unlimited)")
	rootCmd.PersistentFlags().StringVar(&metricsAddr, "metrics-addr", ":9100", "Prometheus metrics listen address (worker mode)")
	rootCmd.PersistentFlags().IntVar(&maxConcurrent, "max-concurrent", 4, "Maximum concurrent tasks per worker")
	rootCmd.PersistentFlags().StringVar(&dataPlane, "data-plane", "nats", "Worker↔coord data-plane transport: nats (default) or grpc. See project_split_plane_design_2026-05-20.")
	rootCmd.PersistentFlags().StringVar(&dataPlaneAddr, "data-plane-addr", ":9091", "Data-plane gRPC listen address (coord/standalone)")
	rootCmd.PersistentFlags().StringVar(&coordDataPlane, "coord-data-plane", "", "Coord's data-plane host:port (worker only; defaults to coord-host + 9091)")
	rootCmd.PersistentFlags().StringVar(&geoipCityDB, "geoip-city", "", "Path to MaxMind GeoIP City database (GeoLite2-City.mmdb)")
	rootCmd.PersistentFlags().StringVar(&geoipASNDB, "geoip-asn", "", "Path to MaxMind GeoIP ASN database (GeoLite2-ASN.mmdb)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")
	rootCmd.PersistentFlags().BoolVar(&enableAlerts, "enable-alerts", false, "enable CREATE ALERT DDL and scheduler (default: disabled)")

	rootCmd.AddCommand(serveCmd())
	rootCmd.AddCommand(queryCmd())
	rootCmd.AddCommand(tablesCmd())
	rootCmd.AddCommand(createTableCmd())
	rootCmd.AddCommand(dropTableCmd())
	rootCmd.AddCommand(shellCmd())
	rootCmd.AddCommand(clustersCmd())
	rootCmd.AddCommand(mcpCmd())
	rootCmd.AddCommand(catalogCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func serveCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the Wadjet server",
	}

	catalogSnapshotPrefix := cmd.Flags().String("catalog-snapshot-s3-prefix", "", "S3 URL prefix (s3://bucket/path/) for catalog snapshots. Unset disables.")
	catalogSnapshotInterval := cmd.Flags().Duration("catalog-snapshot-interval", 5*time.Minute, "Periodic catalog snapshot cadence. 0 disables periodic (explicit CREATE SNAPSHOT still works).")
	forceRestoreCatalog := cmd.Flags().String("force-restore-catalog", "", "Restore catalog from S3 regardless of KV state. Value: 'latest' or a specific timestamp.")

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()

		logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: parseLogLevel(logLevel)}))

		if memLimit := memory.DetectMemoryLimit(); memLimit > 0 {
			maxConc := int64(maxConcurrent)
			if maxConc < 1 {
				maxConc = 1
			}

			// Memory envelope: 75% of detected limit. Leaves 25% headroom
			// for OS page cache, kernel buffers, and non-Go allocations.
			// (Previous 90% left only 3 GB on 32 GB machines — not enough.)
			//
			// Honour an explicit GOMEMLIMIT env var when set — the harness
			// passes a tight per-process limit (4 GB / 8 GB) to reproduce
			// constrained-memory paths locally, and overriding it with the
			// 75%-of-physical default would silently mask the very
			// workloads we're testing.
			goMemLimit := memLimit * 3 / 4
			if envLim := os.Getenv("GOMEMLIMIT"); envLim != "" {
				if parsed, ok := parseGoMemLimit(envLim); ok {
					goMemLimit = parsed
				} else {
					logger.Warn("ignoring unparseable GOMEMLIMIT env",
						"value", envLim, "fallback_bytes", goMemLimit)
				}
			}
			debug.SetMemoryLimit(goMemLimit)
			// GC mode is overridable via WADJET_GOGC env var:
			//   "off" / unset (default): rely on GOMEMLIMIT only — best for
			//     workloads with large stable live data (LRU cache pattern)
			//     because GC assist tax with GOGC=100 caused 2-3x query
			//     slowdowns when the cache was populated.
			//   "<int>" (e.g. "100"): set debug.SetGCPercent to that value —
			//     useful for catalog-priming-heavy workloads where transient
			//     garbage accumulates pre-query (Q18 SF10 baseline 11.5 GB
			//     before query starts on a freshly primed coord).
			gcMode := os.Getenv("WADJET_GOGC")
			if gcMode == "" || strings.EqualFold(gcMode, "off") {
				debug.SetGCPercent(-1)
				gcMode = "off"
			} else if pct, perr := strconv.Atoi(gcMode); perr == nil && pct > 0 {
				debug.SetGCPercent(pct)
				gcMode = strconv.Itoa(pct)
			} else {
				debug.SetGCPercent(-1)
				gcMode = "off (invalid WADJET_GOGC)"
			}
			logger.Info("set GOMEMLIMIT", "detected_limit", memLimit, "go_mem_limit", goMemLimit, "gogc", gcMode)

			if cacheBytes == 0 {
				if memoryBudget > 0 {
					// With explicit memory budget, scale cache to fit alongside
					// task memory. Each task uses ~3x its tracked budget in total
					// RSS (tracked + hash table arenas + intermediate batches).
					taskFootprint := memoryBudget * maxConc * 3
					headroom := goMemLimit / 10
					available := goMemLimit - taskFootprint - headroom
					if available < 256*1024*1024 {
						available = 256 * 1024 * 1024
					}
					if maxCache := goMemLimit / 5; available > maxCache {
						available = maxCache
					}
					cacheBytes = available
				} else {
					// Default: 10% of envelope for cross-query S3 file cache.
					cacheBytes = goMemLimit / 10
				}
				logger.Info("auto-detected file cache size", "cache_bytes", cacheBytes)
			}
			if memoryBudget == 0 {
				// Per-task spill budget. Reduced from 5x to 4x: hash table
				// reconciliation now uses ForceReserve for accurate tracking,
				// and spill threshold lowered to 60%, enabling tighter budgets
				// without OOM risk on multi-join queries.
				// Formula: (envelope - cache) / (4 * maxConcurrent)
				//
				// Note: with the shared worker memory pool (Trino MemoryPool /
				// Spark ExecutionMemoryPool model), the per-task budget is only
				// consulted by the planner for operator sizing — actual
				// allocation tracking flows through `sharedPoolBudget` below.
				// We keep this calculation for planner sizing and as a fallback
				// for legacy callers, but spill triggers are pool-driven.
				memoryBudget = (goMemLimit - cacheBytes) / (4 * maxConc)

				// Auto-tune maxConc DOWN when the per-task budget would be
				// too small to fit a SF100-class join. The 4x factor models
				// task overhead but cannot rescue a worker whose hash tables
				// alone need 8 GB at SF100 from a 1.4 GB budget — at SF100
				// the original maxConc=4 / 30GB-machine combo gives each
				// task ~1.4 GB and the worker OOMs at 31 GB anon-rss when
				// multiple tasks pick up the same query in parallel.
				//
				// Minimum target: 2 GB per task. If we'd be under, reduce
				// maxConc until we hit it (or until maxConc=1). Each query
				// in distributed/probe-split mode dispatches one task per
				// worker, so maxConc above ~2 only helps with concurrent
				// queries from different sessions — which is rare in
				// benchmarks and tunable upward via --max-concurrent when
				// the workload actually needs it.
				const minBudgetPerTask int64 = 2 * 1024 * 1024 * 1024
				if memoryBudget < minBudgetPerTask && maxConc > 1 {
					origConc := maxConc
					for memoryBudget < minBudgetPerTask && maxConc > 1 {
						maxConc--
						memoryBudget = (goMemLimit - cacheBytes) / (4 * maxConc)
					}
					maxConcurrent = int(maxConc)
					logger.Info("auto-tuned max_concurrent down to fit memory budget",
						"orig_max_concurrent", origConc,
						"new_max_concurrent", maxConc,
						"budget_bytes", memoryBudget,
						"min_budget_target", minBudgetPerTask)
				}

				logger.Info("auto-detected memory budget", "budget_bytes", memoryBudget, "max_concurrent", maxConc)
			}
			if sharedPoolBudget == 0 {
				// Pool is the full envelope minus the file cache. All
				// concurrent tasks Reserve from it; operators
				// cooperatively spill when the pool fills. NOT divided
				// by maxConc — the pool's whole point is to share a
				// single budget across tasks instead of statically
				// carving N slices.
				sharedPoolBudget = goMemLimit - cacheBytes
				if sharedPoolBudget < 256*1024*1024 {
					sharedPoolBudget = 256 * 1024 * 1024
				}
				logger.Info("auto-detected shared pool budget",
					"pool_bytes", sharedPoolBudget,
					"go_mem_limit", goMemLimit, "cache_bytes", cacheBytes)
			}
			// Phase 3 note: the system-reservoir registry is built per
			// run-function (buildReservoirs) and threaded into worker.Config
			// so it reaches a real worker's SpillManager. The Phase-1 prelude
			// that built+logged a throwaway registry here was removed.
		}

		store, err := newStore()
		if err != nil {
			return err
		}

		// Wrap store with circuit breaker for S3 resilience
		store = objstore.NewCircuitStore(store, objstore.DefaultCircuitConfig(), logger)

		if v := os.Getenv("WADJET_ENABLE_ALERTS"); v == "1" || strings.EqualFold(v, "true") {
			enableAlerts = true
		}
		if v := os.Getenv("WADJET_CATALOG_SNAPSHOT_PREFIX"); v != "" {
			*catalogSnapshotPrefix = v
		}
		if v := os.Getenv("WADJET_CATALOG_SNAPSHOT_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				*catalogSnapshotInterval = d
			}
		}

		switch mode {
		case "standalone":
			return runStandalone(ctx, store, logger, enableAlerts, *catalogSnapshotPrefix, *catalogSnapshotInterval, *forceRestoreCatalog)
		case "coordinator":
			return runCoordinator(ctx, store, logger, enableAlerts, *catalogSnapshotPrefix, *catalogSnapshotInterval, *forceRestoreCatalog)
		case "worker":
			return runWorker(ctx, store, logger)
		default:
			return fmt.Errorf("unknown mode: %s", mode)
		}
	}
	return cmd
}

func queryCmd() *cobra.Command {
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "query [sql]",
		Short: "Execute a SQL query (standalone mode)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

			f, err := format.ParseFormat(outputFormat)
			if err != nil {
				return err
			}

			// Load GeoIP databases if configured
			if err := loadGeoIP(nil, logger); err != nil {
				return fmt.Errorf("loading GeoIP: %w", err)
			}
			defer geoip.Close()

			store, err := newStore()
			if err != nil {
				store = objstore.NewMemStore()
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			result, err := db.Query(ctx, args[0])
			if err != nil {
				return err
			}

			return format.Write(os.Stdout, f, result.Columns, result.Rows)
		},
	}

	cmd.Flags().StringVarP(&outputFormat, "format", "f", "json", "Output format: table, json, csv")
	return cmd
}

func tablesCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "tables",
		Short: "List all tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				return err
			}

			cat := catalog.NewWithStore(store, bucket)
			tables, err := cat.ListTables(ctx)
			if err != nil {
				return err
			}

			for _, t := range tables {
				fmt.Println(t)
			}
			return nil
		},
	}
}

func createTableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "create-table [sql]",
		Short: "Create a table via SQL (e.g., CREATE TABLE events (id BIGINT, name VARCHAR) PARTITION BY (date))",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				store = objstore.NewMemStore()
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			result, err := db.Query(ctx, args[0])
			if err != nil {
				return err
			}

			if len(result.Rows) > 0 {
				if r, ok := result.Rows[0]["result"]; ok {
					fmt.Println(r)
				}
			}
			return nil
		},
	}
}

func dropTableCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "drop-table [name]",
		Short: "Drop a table by name",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				store = objstore.NewMemStore()
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			if err := db.DropTable(ctx, args[0]); err != nil {
				return err
			}
			fmt.Printf("Table %q dropped\n", args[0])
			return nil
		},
	}
}

func clustersCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "clusters",
		Short: "List all federated clusters and their tables",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			natsAddr := natsURL
			if natsAddr == "" {
				natsAddr = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
			}

			nc, err := distributed.Connect(natsAddr, nil)
			if err != nil {
				return fmt.Errorf("connecting to NATS: %w", err)
			}
			defer nc.Close()

			js, err := distributed.NewJetStream(nc)
			if err != nil {
				return fmt.Errorf("creating JetStream: %w", err)
			}

			kv, err := catalog.NewNATSKV(js)
			if err != nil {
				return fmt.Errorf("creating catalog KV: %w", err)
			}

			store, storeErr := newStore()
			if storeErr != nil {
				store = objstore.NewMemStore()
			}

			cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
			_ = cat.Init(ctx)

			clusters, err := cat.ListClusters()
			if err != nil {
				return err
			}

			if len(clusters) == 0 {
				fmt.Println("No clusters found.")
				return nil
			}

			for _, c := range clusters {
				marker := ""
				if c.ClusterID == clusterID {
					marker = " (local)"
				}
				fmt.Printf("Cluster: %s%s\n", c.ClusterID, marker)
				if len(c.Tables) == 0 {
					fmt.Println("  (no tables)")
				}
				for _, t := range c.Tables {
					fmt.Printf("  - %s\n", t)
				}
			}
			return nil
		},
	}
}

func shellCmd() *cobra.Command {
	var outputFormat string

	cmd := &cobra.Command{
		Use:   "shell",
		Short: "Interactive SQL shell (standalone mode)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

			f, err := format.ParseFormat(outputFormat)
			if err != nil {
				return err
			}

			// Load GeoIP databases if configured
			if err := loadGeoIP(nil, logger); err != nil {
				return fmt.Errorf("loading GeoIP: %w", err)
			}
			defer geoip.Close()

			store, err := newStore()
			if err != nil {
				store = objstore.NewMemStore()
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			return runShell(ctx, db, f)
		},
	}

	cmd.Flags().StringVarP(&outputFormat, "format", "f", "table", "Output format: table, json, csv")
	return cmd
}

func historyPath() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	dir := filepath.Join(home, ".wadjet")
	os.MkdirAll(dir, 0o700)
	return filepath.Join(dir, "history")
}

func runShell(ctx context.Context, db *wadjet.DB, f format.Format) error {
	line := liner.NewLiner()
	defer line.Close()

	line.SetCtrlCAborts(true)

	// Load history
	if path := historyPath(); path != "" {
		if fh, err := os.Open(path); err == nil {
			line.ReadHistory(fh)
			fh.Close()
		}
	}

	// Save history on exit
	defer func() {
		if path := historyPath(); path != "" {
			if fh, err := os.Create(path); err == nil {
				line.WriteHistory(fh)
				fh.Close()
			}
		}
	}()

	fmt.Println("Wadjet SQL Shell. Type 'exit' to quit.")
	fmt.Println("  Supports: SELECT, EXPLAIN, DESCRIBE, SHOW COLUMNS FROM")
	fmt.Println()

	var buf strings.Builder
	prompt := "wadjet> "

	for {
		input, err := line.Prompt(prompt)
		if err == liner.ErrPromptAborted {
			// Ctrl-C: clear current buffer
			buf.Reset()
			prompt = "wadjet> "
			continue
		}
		if err == io.EOF {
			fmt.Println()
			break
		}
		if err != nil {
			return err
		}

		trimmed := strings.TrimSpace(input)

		// Exit commands (only when not in multi-line mode)
		if buf.Len() == 0 && (trimmed == "exit" || trimmed == "quit" || trimmed == `\q`) {
			break
		}

		if trimmed == "" {
			if buf.Len() > 0 {
				buf.WriteString("\n")
			}
			continue
		}

		// Accumulate multi-line input
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		buf.WriteString(trimmed)

		// Check if statement is complete (ends with ;)
		current := buf.String()
		if !strings.HasSuffix(strings.TrimSpace(current), ";") {
			prompt = "     -> "
			continue
		}

		// Strip trailing semicolon and execute
		sql := strings.TrimRight(strings.TrimSpace(current), ";")
		buf.Reset()
		prompt = "wadjet> "

		if sql == "" {
			continue
		}

		line.AppendHistory(current)

		result, err := db.Query(ctx, sql)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			continue
		}

		if len(result.Rows) == 0 {
			fmt.Println("(0 rows)")
			continue
		}

		format.Write(os.Stdout, f, result.Columns, result.Rows)
	}

	return nil
}

func newStore() (objstore.Store, error) {
	switch storageType {
	case "file":
		dir := dataDir
		if dir == "" {
			dir = "/var/lib/wadjet/data"
		}
		return objstore.NewFileStore(dir)
	default:
		return objstore.NewMinIOStore(objstore.MinIOConfig{
			Endpoint:  endpoint,
			AccessKey: accessKey,
			SecretKey: secretKey,
			UseSSL:    useSSL,
			Region:    s3Region,
		})
	}
}

func runStandalone(ctx context.Context, store objstore.Store, logger *slog.Logger, alertsEnabled bool, snapshotPrefix string, snapshotInterval time.Duration, forceRestoreTS string) error {
	// Opt-in heap profile dumper for OOM debugging. No-op unless
	// WADJET_HEAP_DUMP_INTERVAL is set. See cmd/wadjet/heap_dumper.go.
	startHeapDumper(ctx, logger)

	// Periodic background GC: with the default gogc=off, large transient
	// garbage (catalog priming, NATS message buffers) accumulates and
	// can push baseline heap to 11+ GB before any query runs (Q18 SF10
	// 2026-04-25, project_q18_sf10_native_dag_oom_2026-04-24). One GC
	// every 30s reclaims this cheaply (~50ms per call) without the
	// per-allocation GC assist tax that GOGC=100 imposes. Override via
	// WADJET_BG_GC_INTERVAL ("off" to disable, "<duration>" otherwise).
	startBackgroundGC(ctx, logger)

	// Start embedded NATS (with optional leaf node connections)
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = natsPort
	natsCfg.ClusterID = clusterID
	natsCfg.LeafRemotes = leafRemotes
	if natsStoreDir != "" {
		natsCfg.StoreDir = natsStoreDir
	}
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		return fmt.Errorf("starting NATS: %w", err)
	}
	defer embeddedNATS.Shutdown()

	// Connect to NATS via in-process (zero-copy, no TCP overhead)
	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		return fmt.Errorf("setting up streams: %w", err)
	}

	// Create catalog with NATS KV metadata store and cluster identity
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		return fmt.Errorf("creating catalog KV: %w", err)
	}
	cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Restore persisted UDFs and wire persistence callback
	wireUDFPersistence(cat, logger)

	// Load GeoIP databases if configured
	var fileCfg *config.Config
	if configFile != "" {
		fileCfg, _ = config.Load(configFile)
	}
	if err := loadGeoIP(fileCfg, logger); err != nil {
		return fmt.Errorf("loading GeoIP: %w", err)
	}
	defer geoip.Close()

	// Construct worker (deferred Start; the gRPC data-plane wiring below
	// must complete first so Start can see the dpClient and skip the
	// JetStream Fetch loop in --data-plane=grpc mode).
	w := worker.New(worker.Config{
		NATSUrl:              embeddedNATS.ClientURL(),
		ClusterID:            clusterID,
		MaxConcurrent:        maxConcurrent,
		CacheBytes:           cacheBytes,
		MemoryBudget:         memoryBudget,
		SharedPoolBudget:     sharedPoolBudget,
		SpillDir:             spillDir,
		ResultStoreBytes:     resultStoreBytes,
		Reservoirs:           memory.NewReservoirRegistry(),
		FloatingBudgetActive: spillFloatingBudget,
	}, store, nc, js, logger)

	// Initialize Prometheus metrics (before worker.Start so spill metrics are wired)
	m := metrics.New()
	m.Registry.MustRegister(alerts.Collectors()...)
	w.SetMetrics(m)

	// Start coordinator
	coord := coordinator.New(coordinator.Config{
		NATSUrl:        embeddedNATS.ClientURL(),
		ResultBucket:   bucket,
		DynamicFilters: dynamicFiltersFromEnv(),
	}, cat, nc, js, logger)

	// Phase A: same-process data-plane server + client when enabled.
	// Worker dials localhost:dataPlaneAddr. Phase B: coord registers
	// gather receivers; worker streams results via dpClient.
	// Phase C: coord pushes TaskDispatch over the gRPC stream; worker.Start
	// (below) sees a non-nil dpClient and skips its JetStream Fetch loop.
	var dpSrv *dataplane.Server
	var dpClient *dataplane.Client
	if dataPlane == "grpc" {
		dpSrv = dataplane.NewServer(dataplane.ServerConfig{
			Addr:      dataPlaneAddr,
			ClusterID: clusterID,
		}, logger)
		if err := dpSrv.Start(); err != nil {
			return fmt.Errorf("dataplane server: %w", err)
		}
		defer dpSrv.Stop(3 * time.Second)
		coord.SetDataPlaneServer(dpSrv)

		dpClient = dataplane.NewClient(dataplane.ClientConfig{
			CoordAddr: dpSrv.Addr(),
			WorkerID:  "standalone-worker",
			BuildSHA:  buildSHA(),
		}, logger)
		dpClient.Start(ctx)
		defer dpClient.Stop()
		w.SetDataPlaneClient(dpClient)
	}

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}
	defer w.Stop()

	// Start heartbeat monitoring, query reaping, active check, and result cleanup
	coord.Workers().StartReaper(ctx)
	coord.Workers().StartSubStatsLogger(ctx)
	coord.StartQueryReaper(ctx)
	coord.StartQueryActiveHandler()
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, 0)

	// Enable alerts feature flag; in standalone mode StartLeaderWatch is a
	// no-op so we start the scheduler directly here if enabled.
	coord.SetAlertsEnabled(alertsEnabled)
	if alertsEnabled {
		coord.StartAlertScheduler(ctx)
	}

	// Wire CLI-driven catalog snapshot options.
	if snapshotPrefix != "" {
		snapBucket, snapPath, err := parseS3URL(snapshotPrefix)
		if err != nil {
			return fmt.Errorf("parsing --catalog-snapshot-s3-prefix: %w", err)
		}
		coord.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
			Store: store, Bucket: snapBucket, Prefix: snapPath,
		})
		coord.SetCatalogSnapshotInterval(snapshotInterval)
		if err := coord.MaybeRestoreCatalog(ctx, forceRestoreTS); err != nil {
			return fmt.Errorf("restoring catalog: %w", err)
		}
		// Standalone mode skips leader-election, so start the loop directly.
		coord.StartCatalogSnapshotLoop(ctx)
	}

	// Start background compaction
	compactor := compaction.NewBackgroundCompactor(cat, compaction.BackgroundConfig{
		Enabled:    true,
		Compaction: compaction.DefaultConfig(),
	}, logger)
	compactor.Start(ctx)

	// Build config manager and auth provider for hot-reload
	srvCfg := server.Config{
		Addr:        httpAddr,
		Catalog:     cat,
		Coordinator: coord,
		Metrics:     m,
	}

	var cfgMgr *config.Manager
	var provider *auth.Provider

	if configFile != "" {
		cfg, loadErr := config.Load(configFile)
		if loadErr == nil {
			cfgMgr = config.NewManager(cfg, logger)

			// Build initial auth
			authn, authz := buildAuth(cfg.Auth)
			var policies *auth.PolicySet
			if len(cfg.Auth.Policies) > 0 {
				policies = buildPolicies(cfg.Auth.Policies)
			}
			provider = auth.NewProvider(authn, authz, policies, logger)
			// Wire ABAC evaluator (explicit policies or auto-migrate from RBAC)
			if len(cfg.Auth.ABACPolicies) > 0 {
				abac := buildABACPolicies(cfg.Auth.ABACPolicies)
				authCfg := buildAuthConfig(cfg.Auth)
				policyCfgs := buildPolicyConfigs(cfg.Auth.Policies)
				provider.UpdateFromConfig(authCfg, policyCfgs, abac...)
			} else if len(cfg.Auth.Roles) > 0 {
				authCfg := buildAuthConfig(cfg.Auth)
				policyCfgs := buildPolicyConfigs(cfg.Auth.Policies)
				provider.UpdateFromConfig(authCfg, policyCfgs)
			}
			srvCfg.Provider = provider

			// Subscribe to config changes to rebuild auth
			cfgMgr.Subscribe(func(event config.ChangeEvent) {
				authCfg := buildAuthConfig(event.New.Auth)
				policyCfgs := buildPolicyConfigs(event.New.Auth.Policies)
				abacPolicies := buildABACPolicies(event.New.Auth.ABACPolicies)
				provider.UpdateFromConfig(authCfg, policyCfgs, abacPolicies...)
				logger.Info("auth hot-reloaded",
					"enabled", event.New.Auth.Enabled,
					"api_keys", len(event.New.Auth.APIKeys),
					"roles", len(event.New.Auth.Roles),
					"abac_policies", len(event.New.Auth.ABACPolicies),
				)
			})

			if cfg.Auth.MTLS.Enabled {
				tlsCfg, err := buildTLSConfig(cfg.Auth.MTLS)
				if err != nil {
					return fmt.Errorf("configuring mTLS: %w", err)
				}
				srvCfg.TLSConfig = tlsCfg
			}

			logger.Info("authentication enabled (hot-reloadable)",
				"api_keys", len(cfg.Auth.APIKeys),
				"jwt", cfg.Auth.JWT.Enabled,
				"mtls", cfg.Auth.MTLS.Enabled,
				"roles", len(cfg.Auth.Roles),
				"policies", len(cfg.Auth.Policies),
			)

			// Start file watcher for config hot-reload
			watcher := config.NewWatcher(config.WatcherConfig{Path: configFile}, cfgMgr, logger)
			go watcher.Watch(ctx)
		}
	}

	// Configure embedding provider if API key is set
	if apiKey := os.Getenv("WADJET_OPENAI_API_KEY"); apiKey != "" {
		embedModel := os.Getenv("WADJET_EMBED_MODEL")
		if embedModel == "" {
			embedModel = "text-embedding-3-small"
		}
		embedCache := embedding.NewCache(50000)
		embedProvider := embedding.NewOpenAI(embedding.OpenAIConfig{
			APIKey: apiKey,
			Model:  embedModel,
		}, embedCache)
		embedding.SetProvider(embedProvider)
		embedding.RegisterFunctions()
		logger.Info("embedding provider configured", "model", embedModel, "dim", embedProvider.Dimension())
	}

	// Start HTTP server
	srv := server.New(srvCfg, logger)

	// Register admin API if config manager is available
	if cfgMgr != nil && provider != nil {
		admin := server.NewAdminAPI(cfgMgr, provider, logger)
		admin.RegisterRoutes(srv.Mux())
	}

	// Register ops API (workers, cleanup)
	ops := server.NewOpsAPI(coord)
	ops.RegisterRoutes(srv.Mux())

	// Start gRPC server
	grpcSrv := server.NewGRPCServer(server.GRPCConfig{
		Addr:         grpcAddr,
		Catalog:      cat,
		Coord:        coord,
		AuthProvider: provider,
	}, logger)

	// Start PostgreSQL wire protocol server
	pgDB, err := wadjet.Open(ctx, wadjet.Config{
		Store:        store,
		Bucket:       bucket,
		MetaKV:       kv,
		AuthProvider: provider,
	})
	if err != nil {
		return fmt.Errorf("opening DB for pgwire: %w", err)
	}
	pgQueryTimeout, _ := time.ParseDuration(queryTimeout)
	pgCfg := pgwire.Config{
		AuthProvider:     provider,
		QueryTimeout:     pgQueryTimeout,
		MaxConcurrentQry: maxConcurrentQry,
	}
	if pgTLSCert != "" && pgTLSKey != "" {
		cert, err := tls.LoadX509KeyPair(pgTLSCert, pgTLSKey)
		if err != nil {
			return fmt.Errorf("loading pgwire TLS cert: %w", err)
		}
		pgCfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	pgSrv := pgwire.NewServer(pgDB, pgCfg, logger)
	// Route SELECT/WITH through coord.ExecuteSQL (native-DAG executor) when
	// available — bypasses the legacy db.Query CollectSink materialization
	// path that OOMed on Q18 SF10 (project_q18_sf10_native_dag_oom_2026-04-24).
	pgSrv.SetCoordinator(coord)

	errCh := make(chan error, 3)
	go func() {
		errCh <- srv.Start()
	}()
	go func() {
		errCh <- grpcSrv.Start()
	}()
	go func() {
		if err := pgSrv.Start(pgAddr); err != nil {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down...")
		// Reap query intermediates before exit. In-flight queries are
		// dying with us, so their queries/<id>/* prefix on the data
		// store would otherwise leak — the per-query cleanupQuery hook
		// only fires on graceful completion. Best-effort with a 3 s cap
		// so SIGTERM->SIGKILL still completes within the harness's 5 s
		// shutdown deadline.
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 3*time.Second)
		if cleaner := coord.Cleaner(store, bucket); cleaner != nil {
			_, _ = cleaner.CleanAll(cleanupCtx)
		}
		cancelCleanup()
		coord.Workers().Close()
		pgSrv.Shutdown()
		grpcSrv.Shutdown()
		srv.Shutdown(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}

func runCoordinator(ctx context.Context, store objstore.Store, logger *slog.Logger, alertsEnabled bool, snapshotPrefix string, snapshotInterval time.Duration, forceRestoreTS string) error {
	// Start embedded NATS (with optional leaf node connections)
	// Bind to 0.0.0.0 so remote workers can connect.
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Host = "0.0.0.0"
	natsCfg.Port = natsPort
	natsCfg.ClusterID = clusterID
	natsCfg.LeafRemotes = leafRemotes
	if natsStoreDir != "" {
		natsCfg.StoreDir = natsStoreDir
	}
	// Apply NATS mTLS config from CLI flags or env overrides
	applyNATSTLS(&natsCfg, logger)
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		return fmt.Errorf("starting NATS: %w", err)
	}
	defer embeddedNATS.Shutdown()

	nc, err := distributed.ConnectInProcess(embeddedNATS.Server())
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	if err := distributed.SetupStreams(ctx, js); err != nil {
		return fmt.Errorf("setting up streams: %w", err)
	}

	// Create catalog with NATS KV metadata store and cluster identity
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		return fmt.Errorf("creating catalog KV: %w", err)
	}
	cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Restore persisted UDFs and wire persistence callback
	wireUDFPersistence(cat, logger)

	coord := coordinator.New(coordinator.Config{
		NATSUrl:        embeddedNATS.ClientURL(),
		ResultBucket:   bucket,
		DynamicFilters: dynamicFiltersFromEnv(),
	}, cat, nc, js, logger)

	// Phase A: start data-plane gRPC server alongside coord when enabled.
	// Phase B: coord registers gather receivers as ResultHandlers so
	// workers can stream results over gRPC instead of NATS.
	var dpSrv *dataplane.Server
	if dataPlane == "grpc" {
		dpSrv = dataplane.NewServer(dataplane.ServerConfig{
			Addr:      dataPlaneAddr,
			ClusterID: clusterID,
		}, logger)
		if err := dpSrv.Start(); err != nil {
			return fmt.Errorf("dataplane server: %w", err)
		}
		defer dpSrv.Stop(3 * time.Second)
		coord.SetDataPlaneServer(dpSrv)
	}

	// Initialize OTel tracing if configured
	otelTP := initTelemetry(ctx, logger)
	if otelTP != nil {
		coord.SetTelemetry(otelTP)
		defer otelTP.Shutdown(context.Background())
	}

	coord.Workers().StartReaper(ctx)
	coord.Workers().StartSubStatsLogger(ctx)
	coord.StartQueryReaper(ctx)
	coord.StartQueryActiveHandler()
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, 0)

	// Enable alerts feature flag; in coordinator mode StartLeaderWatch manages
	// the scheduler lifecycle on leader transitions.
	coord.SetAlertsEnabled(alertsEnabled)

	// Wire CLI-driven catalog snapshot options.
	if snapshotPrefix != "" {
		snapBucket, snapPath, err := parseS3URL(snapshotPrefix)
		if err != nil {
			return fmt.Errorf("parsing --catalog-snapshot-s3-prefix: %w", err)
		}
		coord.SetCatalogSnapshotOptions(catalog.SnapshotOptions{
			Store: store, Bucket: snapBucket, Prefix: snapPath,
		})
		coord.SetCatalogSnapshotInterval(snapshotInterval)
		if err := coord.MaybeRestoreCatalog(ctx, forceRestoreTS); err != nil {
			return fmt.Errorf("restoring catalog: %w", err)
		}
		// In coordinator mode, StartLeaderWatch fires StartCatalogSnapshotLoop
		// on leader election, so we do NOT call it here.
	}

	// Start background compaction
	coordCompactor := compaction.NewBackgroundCompactor(cat, compaction.BackgroundConfig{
		Enabled:    true,
		Compaction: compaction.DefaultConfig(),
	}, logger)
	coordCompactor.Start(ctx)

	m := metrics.New()
	m.Registry.MustRegister(alerts.Collectors()...)
	dlq := coordinator.NewDLQ(js)

	srvCfg := server.Config{
		Addr:        httpAddr,
		Catalog:     cat,
		Coordinator: coord,
		DLQ:         dlq,
		Metrics:     m,
	}

	var cfgMgr *config.Manager
	var provider *auth.Provider

	if configFile != "" {
		if cfg, loadErr := config.Load(configFile); loadErr == nil {
			cfgMgr = config.NewManager(cfg, logger)

			authn, authz := buildAuth(cfg.Auth)
			var policies *auth.PolicySet
			if len(cfg.Auth.Policies) > 0 {
				policies = buildPolicies(cfg.Auth.Policies)
			}
			provider = auth.NewProvider(authn, authz, policies, logger)
			if len(cfg.Auth.ABACPolicies) > 0 {
				abac := buildABACPolicies(cfg.Auth.ABACPolicies)
				authCfg := buildAuthConfig(cfg.Auth)
				policyCfgs := buildPolicyConfigs(cfg.Auth.Policies)
				provider.UpdateFromConfig(authCfg, policyCfgs, abac...)
			} else if len(cfg.Auth.Roles) > 0 {
				authCfg := buildAuthConfig(cfg.Auth)
				policyCfgs := buildPolicyConfigs(cfg.Auth.Policies)
				provider.UpdateFromConfig(authCfg, policyCfgs)
			}
			srvCfg.Provider = provider

			cfgMgr.Subscribe(func(event config.ChangeEvent) {
				authCfg := buildAuthConfig(event.New.Auth)
				policyCfgs := buildPolicyConfigs(event.New.Auth.Policies)
				abacPolicies := buildABACPolicies(event.New.Auth.ABACPolicies)
				provider.UpdateFromConfig(authCfg, policyCfgs, abacPolicies...)
			})

			if cfg.Auth.MTLS.Enabled {
				tlsCfg, err := buildTLSConfig(cfg.Auth.MTLS)
				if err != nil {
					return fmt.Errorf("configuring mTLS: %w", err)
				}
				srvCfg.TLSConfig = tlsCfg
			}

			watcher := config.NewWatcher(config.WatcherConfig{Path: configFile}, cfgMgr, logger)
			go watcher.Watch(ctx)
		}
	}

	// Configure embedding provider for coordinator mode
	if apiKey := os.Getenv("WADJET_OPENAI_API_KEY"); apiKey != "" {
		embedModel := os.Getenv("WADJET_EMBED_MODEL")
		if embedModel == "" {
			embedModel = "text-embedding-3-small"
		}
		embedCache := embedding.NewCache(50000)
		embedProvider := embedding.NewOpenAI(embedding.OpenAIConfig{
			APIKey: apiKey,
			Model:  embedModel,
		}, embedCache)
		embedding.SetProvider(embedProvider)
		embedding.RegisterFunctions()
		logger.Info("embedding provider configured", "model", embedModel, "dim", embedProvider.Dimension())
	}

	srv := server.New(srvCfg, logger)

	if cfgMgr != nil && provider != nil {
		admin := server.NewAdminAPI(cfgMgr, provider, logger)
		admin.RegisterRoutes(srv.Mux())
	}

	// Register ops API
	ops := server.NewOpsAPI(coord)
	ops.RegisterRoutes(srv.Mux())

	// Start gRPC server
	grpcSrv := server.NewGRPCServer(server.GRPCConfig{
		Addr:         grpcAddr,
		Catalog:      cat,
		Coord:        coord,
		AuthProvider: provider,
	}, logger)

	errCh := make(chan error, 2)
	go func() {
		errCh <- srv.Start()
	}()
	go func() {
		errCh <- grpcSrv.Start()
	}()

	select {
	case <-ctx.Done():
		logger.Info("coordinator shutting down...")
		// Same shutdown reap as standalone (see standalone path comment).
		// In-flight queries die with us; their queries/<id>/* prefix would
		// otherwise leak.
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 3*time.Second)
		if cleaner := coord.Cleaner(store, bucket); cleaner != nil {
			_, _ = cleaner.CleanAll(cleanupCtx)
		}
		cancelCleanup()
		coord.Workers().Close()
		grpcSrv.Shutdown()
		srv.Shutdown(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}

func runWorker(ctx context.Context, store objstore.Store, logger *slog.Logger) error {
	natsAddr := natsURL
	if natsAddr == "" {
		natsAddr = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
	}

	// Build NATS client TLS config if mTLS is configured
	var natsTLSCfg *tls.Config
	tlsCert, tlsKey, tlsCA := resolveNATSTLSPaths()
	if tlsCert != "" && tlsKey != "" && tlsCA != "" {
		var err error
		natsTLSCfg, err = distributed.BuildNATSClientTLS(tlsCert, tlsKey, tlsCA)
		if err != nil {
			return fmt.Errorf("building NATS TLS config: %w", err)
		}
		logger.Info("NATS mTLS enabled", "ca", tlsCA, "cert", tlsCert)
	}

	nc, err := distributed.Connect(natsAddr, natsTLSCfg)
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	// Dedicated control-plane connection for the heartbeat publish path.
	// Separate from `nc` (data plane) so heartbeat traffic can't share fate
	// with bursty gather/result publishes that wedged the data connection
	// past JetStream AckWait on the 2026-05-02 SF10 EC2 deploy.
	controlNC, err := distributed.Connect(natsAddr, natsTLSCfg)
	if err != nil {
		return fmt.Errorf("connecting control-plane NATS: %w", err)
	}
	defer controlNC.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	// Load GeoIP databases if configured
	var fileCfg *config.Config
	if configFile != "" {
		fileCfg, _ = config.Load(configFile)
	}
	if err := loadGeoIP(fileCfg, logger); err != nil {
		return fmt.Errorf("loading GeoIP: %w", err)
	}
	defer geoip.Close()

	// Generate a worker id once so both the existing NATS worker code and
	// the new data-plane client share the same identity.
	workerID := "worker-" + uuid.New().String()[:8]

	w := worker.New(worker.Config{
		WorkerID:             workerID,
		NATSUrl:              natsAddr,
		ClusterID:            clusterID,
		MaxConcurrent:        maxConcurrent,
		CacheBytes:           cacheBytes,
		MemoryBudget:         memoryBudget,
		SharedPoolBudget:     sharedPoolBudget,
		SpillDir:             spillDir,
		ResultStoreBytes:     resultStoreBytes,
		Reservoirs:           memory.NewReservoirRegistry(),
		FloatingBudgetActive: spillFloatingBudget,
	}, store, nc, js, logger)
	w.SetControlConn(controlNC)

	// Phase A: open the data-plane stream to coord when enabled. Heartbeats,
	// cancellation, KV stay on NATS. Phases B–E migrate task dispatch,
	// results, gather, progress onto this stream.
	var dpClient *dataplane.Client
	if dataPlane == "grpc" {
		addr := coordDataPlane
		if addr == "" {
			// Derive from natsAddr host: keep host, swap port to data-plane.
			addr = deriveDataPlaneAddr(natsAddr, dataPlaneAddr)
		}
		dpClient = dataplane.NewClient(dataplane.ClientConfig{
			CoordAddr: addr,
			WorkerID:  workerID,
			BuildSHA:  buildSHA(),
		}, logger)
		dpClient.Start(ctx)
		defer dpClient.Stop()
		w.SetDataPlaneClient(dpClient)
	}

	// Opt-in heap+goroutine pprof dumper (env-gated). Workers are silent
	// to journald under buffered cloud-init pipes, so disk-snapshot pprof
	// is the only signal that survives across a stall window.
	startHeapDumper(ctx, logger)

	// Initialize Prometheus metrics
	m := metrics.New()
	w.SetMetrics(m)

	// Start /metrics HTTP endpoint for Prometheus scraping
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", m.Handler())
	metricsMux.HandleFunc("/debug/pprof/", nethttppprof.Index)
	metricsMux.HandleFunc("/debug/pprof/cmdline", nethttppprof.Cmdline)
	metricsMux.HandleFunc("/debug/pprof/profile", nethttppprof.Profile)
	metricsMux.HandleFunc("/debug/pprof/symbol", nethttppprof.Symbol)
	metricsMux.HandleFunc("/debug/pprof/trace", nethttppprof.Trace)
	metricsMux.Handle("/debug/pprof/goroutine", nethttppprof.Handler("goroutine"))
	metricsMux.Handle("/debug/pprof/heap", nethttppprof.Handler("heap"))
	metricsMux.Handle("/debug/pprof/allocs", nethttppprof.Handler("allocs"))
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsMux}
	go func() {
		logger.Info("worker metrics server listening", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()
	defer metricsSrv.Shutdown(context.Background())

	// Initialize OTel tracing on worker
	workerOtelTP := initTelemetry(ctx, logger)
	if workerOtelTP != nil {
		w.SetTelemetry(workerOtelTP)
		defer workerOtelTP.Shutdown(context.Background())
	}

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}

	// SIGQUIT triggers graceful drain: stop accepting new tasks, finish
	// in-flight work, then exit. SIGINT/SIGTERM still do a hard stop.
	drainCh := make(chan os.Signal, 1)
	signal.Notify(drainCh, syscall.SIGQUIT)

	select {
	case <-ctx.Done():
		w.Stop()
	case <-drainCh:
		logger.Info("SIGQUIT received, draining worker...")
		w.Drain()
	}
	return nil
}

// wireUDFPersistence loads persisted UDFs from the catalog and sets up
// a callback so future UDF changes are automatically saved to KV.
func catalogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Catalog management commands",
	}
	cmd.AddCommand(catalogSnapshotCmd())
	return cmd
}

func catalogSnapshotCmd() *cobra.Command {
	var coordAddr string
	cmd := &cobra.Command{
		Use:   "snapshot",
		Short: "Take a catalog snapshot via a running coordinator",
		RunE: func(cmd *cobra.Command, args []string) error {
			if coordAddr == "" {
				return fmt.Errorf("snapshots require a running coordinator; pass --coord-addr=host:port")
			}

			ctx := context.Background()
			conn, err := pgx.Connect(ctx, "postgres://wadjet@"+coordAddr+"/wadjet?sslmode=disable")
			if err != nil {
				return fmt.Errorf("connect to coordinator: %w", err)
			}
			defer conn.Close(ctx)

			rows, err := conn.Query(ctx, "CREATE SNAPSHOT")
			if err != nil {
				return fmt.Errorf("executing CREATE SNAPSHOT: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				vals, err := rows.Values()
				if err != nil {
					return err
				}
				fmt.Println(vals)
			}
			return rows.Err()
		},
	}
	cmd.Flags().StringVar(&coordAddr, "coord-addr", "", "coordinator pgwire address (host:port)")
	return cmd
}

// dynamicFiltersFromEnv reads WADJET_DYNAMIC_FILTERS and reports whether
// the Trino-style semi-join dynamic-filter optimization should be enabled
// on the coordinator. Accepts "1", "true" (case-insensitive). Off otherwise
// — v1 default until validated at SF10/SF100.
func dynamicFiltersFromEnv() bool {
	v := os.Getenv("WADJET_DYNAMIC_FILTERS")
	return v == "1" || strings.EqualFold(v, "true")
}

func wireUDFPersistence(cat *catalog.Catalog, logger *slog.Logger) {
	// Load existing UDFs from KV
	kvDefs, err := cat.LoadUDFs()
	if err != nil {
		logger.Warn("failed to load persisted UDFs", "error", err)
	} else if len(kvDefs) > 0 {
		exprDefs := make([]expr.UDFDef, len(kvDefs))
		for i, d := range kvDefs {
			exprDefs[i] = expr.UDFDef{
				Name:   d.Name,
				Params: d.Params,
				Body:   d.Body,
				Owner:  d.Owner,
				Locked: d.Locked,
			}
		}
		loaded := expr.DefaultUDFs.LoadDefs(exprDefs)
		logger.Info("restored persisted UDFs", "count", loaded)
	}

	// Wire persistence callback for future mutations
	expr.DefaultUDFs.SetPersister(func(udfs []expr.UDFDef) error {
		catDefs := make([]catalog.UDFDef, len(udfs))
		for i, d := range udfs {
			catDefs[i] = catalog.UDFDef{
				Name:   d.Name,
				Params: d.Params,
				Body:   d.Body,
				Owner:  d.Owner,
				Locked: d.Locked,
			}
		}
		return cat.SaveUDFs(catDefs)
	})
}

// buildAuthConfig converts config.Auth to auth.Config without creating the Authenticator.
func buildAuthConfig(cfg config.Auth) auth.Config {
	authCfg := auth.Config{
		Enabled: cfg.Enabled,
		Roles:   make([]auth.RoleConfig, len(cfg.Roles)),
		APIKeys: make([]auth.APIKeyDef, len(cfg.APIKeys)),
	}
	for i, r := range cfg.Roles {
		authCfg.Roles[i] = auth.RoleConfig{Name: r.Name, Tables: r.Tables, Allow: r.Allow}
	}
	for i, k := range cfg.APIKeys {
		authCfg.APIKeys[i] = auth.APIKeyDef{Key: k.Key, Name: k.Name, Role: k.Role}
	}
	if cfg.JWT.Enabled {
		authCfg.JWT = auth.JWTConfig{
			Enabled:       true,
			Secret:        cfg.JWT.Secret,
			PublicKeyFile: cfg.JWT.PublicKeyFile,
			RoleClaim:     cfg.JWT.RoleClaim,
			Issuer:        cfg.JWT.Issuer,
		}
	}
	if cfg.MTLS.Enabled {
		authCfg.MTLS = auth.MTLSConfig{
			Enabled:     true,
			CAFile:      cfg.MTLS.CAFile,
			RoleMap:     cfg.MTLS.RoleMap,
			DefaultRole: cfg.MTLS.DefaultRole,
		}
	}
	return authCfg
}

// buildPolicyConfigs converts config.AuthPolicy to auth.PolicyConfig slices.
func buildPolicyConfigs(cfgs []config.AuthPolicy) []auth.PolicyConfig {
	policyCfgs := make([]auth.PolicyConfig, len(cfgs))
	for i, c := range cfgs {
		policyCfgs[i] = auth.PolicyConfig{
			Table:     c.Table,
			Role:      c.Role,
			Columns:   c.Columns,
			RowFilter: c.RowFilter,
		}
	}
	return policyCfgs
}

func buildAuth(cfg config.Auth) (*auth.Authenticator, *auth.Authorizer) {
	authCfg := auth.Config{
		Enabled: cfg.Enabled,
		Roles:   make([]auth.RoleConfig, len(cfg.Roles)),
		APIKeys: make([]auth.APIKeyDef, len(cfg.APIKeys)),
	}
	for i, r := range cfg.Roles {
		authCfg.Roles[i] = auth.RoleConfig{Name: r.Name, Tables: r.Tables, Allow: r.Allow}
	}
	for i, k := range cfg.APIKeys {
		authCfg.APIKeys[i] = auth.APIKeyDef{Key: k.Key, Name: k.Name, Role: k.Role}
	}
	if cfg.JWT.Enabled {
		authCfg.JWT = auth.JWTConfig{
			Enabled:       true,
			Secret:        cfg.JWT.Secret,
			PublicKeyFile: cfg.JWT.PublicKeyFile,
			RoleClaim:     cfg.JWT.RoleClaim,
			Issuer:        cfg.JWT.Issuer,
		}
	}
	if cfg.MTLS.Enabled {
		authCfg.MTLS = auth.MTLSConfig{
			Enabled:     true,
			CAFile:      cfg.MTLS.CAFile,
			RoleMap:     cfg.MTLS.RoleMap,
			DefaultRole: cfg.MTLS.DefaultRole,
		}
	}
	return auth.New(authCfg)
}

func buildPolicies(cfgs []config.AuthPolicy) *auth.PolicySet {
	policyCfgs := make([]auth.PolicyConfig, len(cfgs))
	for i, c := range cfgs {
		policyCfgs[i] = auth.PolicyConfig{
			Table:     c.Table,
			Role:      c.Role,
			Columns:   c.Columns,
			RowFilter: c.RowFilter,
		}
	}
	return auth.ParsePolicies(policyCfgs)
}

// buildABACPolicies converts config ABAC policies to auth ABAC policies.
func buildABACPolicies(cfgs []config.ABACPolicy) []auth.AccessControlPolicy {
	policies := make([]auth.AccessControlPolicy, len(cfgs))
	for i, p := range cfgs {
		enabled := true
		if p.Enabled != nil {
			enabled = *p.Enabled
		}
		rules := make([]auth.PolicyRule, len(p.Rules))
		for j, r := range p.Rules {
			effect := auth.EffectAllow
			if r.Effect == "deny" {
				effect = auth.EffectDeny
			}
			// Categorize conditions into subject/resource/environment
			var subjects, resources, envConds []auth.Condition
			for _, c := range r.Conditions {
				cond := auth.Condition{Attribute: c.Attribute, Op: c.Operator, Value: c.Value}
				switch {
				case len(c.Attribute) > 8 && c.Attribute[:8] == "subject.":
					cond.Attribute = c.Attribute[8:]
					subjects = append(subjects, cond)
				case len(c.Attribute) > 9 && c.Attribute[:9] == "resource.":
					cond.Attribute = c.Attribute[9:]
					resources = append(resources, cond)
				case len(c.Attribute) > 4 && c.Attribute[:4] == "env.":
					cond.Attribute = c.Attribute[4:]
					envConds = append(envConds, cond)
				default:
					// Put unprefixed conditions in subjects by default
					subjects = append(subjects, cond)
				}
			}
			obligs := make([]auth.Obligation, len(r.Obligations))
			for k, o := range r.Obligations {
				obligs[k] = auth.Obligation{
					Type:   o.Type,
					Target: o.Target,
					Value:  o.Value,
				}
			}
			rules[j] = auth.PolicyRule{
				Description: p.Description,
				EffectStr:   r.Effect,
				Effect:      effect,
				Priority:    p.Priority,
				Subjects:    subjects,
				Resources:   resources,
				Environment: envConds,
				Obligations: obligs,
			}
		}
		policies[i] = auth.AccessControlPolicy{
			Name:    p.Name,
			Enabled: enabled,
			Rules:   rules,
		}
	}
	return policies
}

// loadGeoIP loads MaxMind GeoIP databases from CLI flags or config file.
func loadGeoIP(cfg *config.Config, logger *slog.Logger) error {
	cityDB, asnDB := geoipCityDB, geoipASNDB
	if cfg != nil {
		if cfg.GeoIP.CityDB != "" && cityDB == "" {
			cityDB = cfg.GeoIP.CityDB
		}
		if cfg.GeoIP.ASNDB != "" && asnDB == "" {
			asnDB = cfg.GeoIP.ASNDB
		}
	}
	if cityDB == "" && asnDB == "" {
		return nil
	}
	if err := geoip.Load(cityDB, asnDB); err != nil {
		return err
	}
	logger.Info("GeoIP databases loaded", "city", cityDB, "asn", asnDB)
	return nil
}

func parseLogLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// parseGoMemLimit accepts the same humanized formats the Go runtime
// recognizes for GOMEMLIMIT (e.g. "2GiB", "2GB", "2G", "2147483648") and
// returns the value in bytes. Returns ok=false if the input doesn't parse.
// Plain integer fast path matches the historical behavior callers depended
// on (the harness writes raw bytes).
func parseGoMemLimit(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		if n > 0 {
			return n, true
		}
		return 0, false
	}
	// Strip a trailing "B" — both "GiB" and "GB" end in "B" and the suffix
	// is informational; the multiplier is determined by the i-vs-no-i below.
	t := s
	if strings.HasSuffix(t, "B") || strings.HasSuffix(t, "b") {
		t = t[:len(t)-1]
	}
	binary := false
	if l := len(t); l >= 1 && (t[l-1] == 'i' || t[l-1] == 'I') {
		binary = true
		t = t[:l-1]
	}
	if len(t) == 0 {
		return 0, false
	}
	mult := int64(1)
	switch t[len(t)-1] {
	case 'K', 'k':
		if binary {
			mult = 1024
		} else {
			mult = 1000
		}
	case 'M', 'm':
		if binary {
			mult = 1024 * 1024
		} else {
			mult = 1000 * 1000
		}
	case 'G', 'g':
		if binary {
			mult = 1024 * 1024 * 1024
		} else {
			mult = 1000 * 1000 * 1000
		}
	case 'T', 't':
		if binary {
			mult = 1024 * 1024 * 1024 * 1024
		} else {
			mult = 1000 * 1000 * 1000 * 1000
		}
	default:
		return 0, false
	}
	num, err := strconv.ParseInt(t[:len(t)-1], 10, 64)
	if err != nil || num <= 0 {
		return 0, false
	}
	return num * mult, true
}

func buildTLSConfig(cfg config.AuthMTLS) (*tls.Config, error) {
	clientCA, err := auth.LoadClientCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return auth.NewTLSConfig(cfg.CertFile, cfg.KeyFile, clientCA)
}

// resolveNATSTLSPaths returns TLS cert/key/CA paths from CLI flags, env vars, or config file.
// CLI flags take priority, then env vars, then config file values.
func resolveNATSTLSPaths() (cert, key, ca string) {
	cert, key, ca = natsTLSCert, natsTLSKey, natsTLSCA
	// Env vars override CLI flags (already handled by applyEnvOverrides on config),
	// but CLI flags are direct — check env only if flag is empty.
	if cert == "" {
		cert = os.Getenv("WADJET_NATS_TLS_CERT")
	}
	if key == "" {
		key = os.Getenv("WADJET_NATS_TLS_KEY")
	}
	if ca == "" {
		ca = os.Getenv("WADJET_NATS_TLS_CA")
	}
	return
}

// initTelemetry creates an OTel TracerProvider if an OTLP endpoint is configured.
// Returns nil if no endpoint is set (tracing disabled).
func initTelemetry(ctx context.Context, logger *slog.Logger) *telemetry.Provider {
	endpoint := otelEndpoint
	insecure := otelInsecure
	var sampleRate float64

	// CLI flags take precedence; fall back to env vars / config file
	if endpoint == "" {
		endpoint = os.Getenv("WADJET_OTEL_ENDPOINT")
	}
	if endpoint == "" {
		return nil
	}
	if !insecure {
		insecure = os.Getenv("WADJET_OTEL_INSECURE") == "true" || os.Getenv("WADJET_OTEL_INSECURE") == "1"
	}
	if v := os.Getenv("WADJET_OTEL_SAMPLE_RATE"); v != "" {
		fmt.Sscanf(v, "%f", &sampleRate)
	}
	if sampleRate <= 0 {
		sampleRate = 1.0
	}

	tp, err := telemetry.Init(ctx, telemetry.Config{
		Endpoint:   endpoint,
		Insecure:   insecure,
		SampleRate: sampleRate,
	}, logger)
	if err != nil {
		logger.Error("failed to initialize OpenTelemetry", "error", err)
		return nil
	}
	return tp
}

// applyNATSTLS sets TLS fields on a NATSConfig from CLI flags/env vars.
// Used by runCoordinator to configure mTLS on the embedded NATS server.
func applyNATSTLS(cfg *distributed.NATSConfig, logger *slog.Logger) {
	cert, key, ca := resolveNATSTLSPaths()
	if cert != "" && key != "" && ca != "" {
		cfg.TLSCert = cert
		cfg.TLSKey = key
		cfg.TLSCA = ca
		logger.Info("NATS mTLS enabled on server", "ca", ca, "cert", cert)
	}
}

func mcpCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "mcp",
		Short: "Start MCP (Model Context Protocol) server on stdio for AI agent integration",
		Long: `Start a Model Context Protocol server that communicates over stdin/stdout.
This allows AI agents (Claude Desktop, Claude Code, Cursor, etc.) to discover
tables, inspect schemas, and execute SQL queries against Wadjet.

Configure in Claude Desktop's claude_desktop_config.json:

  {
    "mcpServers": {
      "wadjet": {
        "command": "wadjet",
        "args": ["mcp", "--endpoint", "localhost:9000"]
      }
    }
  }`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

			store, err := newStore()
			if err != nil {
				return fmt.Errorf("initializing storage: %w", err)
			}

			db, err := wadjet.Open(ctx, wadjet.Config{
				Store:  store,
				Bucket: bucket,
				Logger: logger,
			})
			if err != nil {
				return fmt.Errorf("opening database: %w", err)
			}

			srv := mcp.NewServer(db, logger)
			return srv.ServeStdio(ctx, os.Stdin, os.Stdout)
		},
	}
}

// parseS3URL splits "s3://bucket/path/..." into (bucket, path).
// path is empty or ends with "/".
func parseS3URL(s string) (bucket, path string, err error) {
	rest, ok := strings.CutPrefix(s, "s3://")
	if !ok {
		return "", "", fmt.Errorf("not an s3:// URL: %s", s)
	}
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return rest, "", nil
	}
	bucket = rest[:slash]
	path = rest[slash+1:]
	if path != "" && !strings.HasSuffix(path, "/") {
		path += "/"
	}
	return bucket, path, nil
}

// deriveDataPlaneAddr builds host:port for the data plane from the
// NATS URL host and the data-plane listen port. natsAddr is something
// like "nats://10.0.1.2:4222" or "10.0.1.2:4222"; portFromFlag is the
// listen address used by coord (":9091" → port 9091).
func deriveDataPlaneAddr(natsAddr, portFromFlag string) string {
	host := natsAddr
	host = strings.TrimPrefix(host, "nats://")
	host = strings.TrimPrefix(host, "tls://")
	if i := strings.LastIndex(host, ":"); i >= 0 {
		host = host[:i]
	}
	port := strings.TrimPrefix(portFromFlag, ":")
	return host + ":" + port
}

// buildSHA returns the VCS revision short SHA from the build info, or
// "unknown" if unavailable.
func buildSHA() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, s := range info.Settings {
		if s.Key == "vcs.revision" {
			if len(s.Value) >= 7 {
				return s.Value[:7]
			}
			return s.Value
		}
	}
	return "unknown"
}
