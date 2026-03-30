package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/peterh/liner"

	"github.com/citc-tech/wadjet/internal/engine/memory"

	"github.com/citc-tech/wadjet/internal/auth"
	"github.com/citc-tech/wadjet/internal/config"
	"github.com/citc-tech/wadjet/internal/geoip"
	"github.com/citc-tech/wadjet/internal/coordinator"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/expr"
	"github.com/citc-tech/wadjet/internal/format"
	"github.com/citc-tech/wadjet/internal/metrics"
	"github.com/citc-tech/wadjet/internal/server"
	"github.com/citc-tech/wadjet/internal/server/mcp"
	"github.com/citc-tech/wadjet/internal/telemetry"
	"github.com/citc-tech/wadjet/internal/server/pgwire"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/compaction"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/worker"
	"github.com/citc-tech/wadjet/wadjet"
	"github.com/spf13/cobra"
)

var (
	mode             string
	storageType      string
	dataDir          string
	endpoint         string
	accessKey        string
	secretKey        string
	bucket           string
	httpAddr         string
	natsPort         int
	natsURL          string
	configFile       string
	clusterID        string
	leafRemotes      []string
	grpcAddr         string
	memoryBudget     int64
	spillDir         string
	resultStoreBytes int64
	pgAddr           string
	pgTLSCert        string
	pgTLSKey         string
	queryTimeout     string
	maxConcurrentQry int
	natsStoreDir     string
	geoipCityDB      string
	geoipASNDB       string
	useSSL           bool
	s3Region         string
	maxConcurrent    int
	cacheBytes       int64
	logLevel         string
	natsTLSCert      string
	natsTLSKey       string
	natsTLSCA        string
	otelEndpoint     string
	otelInsecure     bool
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
	rootCmd.PersistentFlags().StringVar(&spillDir, "spill-dir", "", "Directory for spill files (default: OS temp dir)")
	rootCmd.PersistentFlags().Int64Var(&cacheBytes, "cache-bytes", 0, "LRU file cache size in bytes (0 = auto-detect: 20% of memory)")

	rootCmd.PersistentFlags().Int64Var(&resultStoreBytes, "result-store", 512*1024*1024, "In-memory result store capacity in bytes (0 = disabled, results pass through S3)")
	rootCmd.PersistentFlags().StringVar(&pgAddr, "pg-addr", ":5433", "PostgreSQL wire protocol listen address")
	rootCmd.PersistentFlags().StringVar(&pgTLSCert, "pg-tls-cert", "", "TLS certificate file for PostgreSQL wire protocol")
	rootCmd.PersistentFlags().StringVar(&pgTLSKey, "pg-tls-key", "", "TLS private key file for PostgreSQL wire protocol")
	rootCmd.PersistentFlags().StringVar(&queryTimeout, "query-timeout", "0", "Default query timeout (e.g. 30s, 5m, 0=unlimited)")
	rootCmd.PersistentFlags().IntVar(&maxConcurrentQry, "max-concurrent-queries", 0, "Maximum concurrent queries (0=unlimited)")
	rootCmd.PersistentFlags().IntVar(&maxConcurrent, "max-concurrent", 4, "Maximum concurrent tasks per worker")
	rootCmd.PersistentFlags().StringVar(&geoipCityDB, "geoip-city", "", "Path to MaxMind GeoIP City database (GeoLite2-City.mmdb)")
	rootCmd.PersistentFlags().StringVar(&geoipASNDB, "geoip-asn", "", "Path to MaxMind GeoIP ASN database (GeoLite2-ASN.mmdb)")
	rootCmd.PersistentFlags().StringVar(&logLevel, "log-level", "info", "Log level: debug, info, warn, error")

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
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the Wadjet server",
		RunE: func(cmd *cobra.Command, args []string) error {
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
				goMemLimit := memLimit * 3 / 4
				debug.SetMemoryLimit(goMemLimit)
				// Disable percentage-based GC trigger and rely solely on GOMEMLIMIT.
				// Workers keep an LRU file cache as long-lived heap. With GOGC=100,
				// GC assist forces marking tax on every allocation proportional to
				// live data — causing 2-3x query slowdowns when the cache is populated.
				// GOGC=off + GOMEMLIMIT is Go's recommended configuration for
				// workloads with large stable live data.
				debug.SetGCPercent(-1)
				logger.Info("set GOMEMLIMIT", "detected_limit", memLimit, "go_mem_limit", goMemLimit, "gogc", "off")

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
					// Per-task spill budget. Each task consumes ~5x its tracked
					// budget in total RSS due to hash table overhead (grow()
					// doubles entries), SoA accumulator arrays, Go GC working
					// set, scanner buffers, and Parquet decompression buffers.
					// Formula: (envelope - cache) / (5 * maxConcurrent)
					memoryBudget = (goMemLimit - cacheBytes) / (5 * maxConc)
					logger.Info("auto-detected memory budget", "budget_bytes", memoryBudget, "max_concurrent", maxConc)
				}
			}

			store, err := newStore()
			if err != nil {
				return err
			}

			// Wrap store with circuit breaker for S3 resilience
			store = objstore.NewCircuitStore(store, objstore.DefaultCircuitConfig(), logger)

			switch mode {
			case "standalone":
				return runStandalone(ctx, store, logger)
			case "coordinator":
				return runCoordinator(ctx, store, logger)
			case "worker":
				return runWorker(ctx, store, logger)
			default:
				return fmt.Errorf("unknown mode: %s", mode)
			}
		},
	}
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

func runStandalone(ctx context.Context, store objstore.Store, logger *slog.Logger) error {
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

	// Start worker
	w := worker.New(worker.Config{
		NATSUrl:          embeddedNATS.ClientURL(),
		ClusterID:        clusterID,
		MaxConcurrent:    maxConcurrent,
		CacheBytes:       cacheBytes,
		MemoryBudget:     memoryBudget,
		SpillDir:         spillDir,
		ResultStoreBytes: resultStoreBytes,
	}, store, nc, js, logger)

	// Initialize Prometheus metrics (before worker.Start so spill metrics are wired)
	m := metrics.New()
	w.SetMetrics(m)

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}
	defer w.Stop()

	// Start coordinator
	coord := coordinator.New(coordinator.Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: bucket,
	}, cat, nc, js, logger)

	// Start heartbeat monitoring, query reaping, and result cleanup
	coord.Workers().StartReaper(ctx)
	coord.StartQueryReaper(ctx)
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, 0)

	// Start catalog snapshotter
	snapCfg := buildSnapshotConfig(fileCfg)
	snapshotter := coordinator.NewCatalogSnapshotter(cat, store, bucket, snapCfg, logger)
	snapshotter.Start(ctx)

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
		coord.Workers().Close()
		pgSrv.Shutdown()
		grpcSrv.Shutdown()
		srv.Shutdown(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}

func runCoordinator(ctx context.Context, store objstore.Store, logger *slog.Logger) error {
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
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: bucket,
	}, cat, nc, js, logger)

	// Initialize OTel tracing if configured
	otelTP := initTelemetry(ctx, logger)
	if otelTP != nil {
		coord.SetTelemetry(otelTP)
		defer otelTP.Shutdown(context.Background())
	}

	coord.Workers().StartReaper(ctx)
	coord.StartQueryReaper(ctx)
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, 0)

	// Start catalog snapshotter (leader-only in distributed mode)
	var coordFileCfg *config.Config
	if configFile != "" {
		coordFileCfg, _ = config.Load(configFile)
	}
	coordSnapCfg := buildSnapshotConfig(coordFileCfg)
	coordSnap := coordinator.NewCatalogSnapshotter(cat, store, bucket, coordSnapCfg, logger)
	coordSnap.Start(ctx)

	// Start background compaction
	coordCompactor := compaction.NewBackgroundCompactor(cat, compaction.BackgroundConfig{
		Enabled:    true,
		Compaction: compaction.DefaultConfig(),
	}, logger)
	coordCompactor.Start(ctx)

	m := metrics.New()
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

	w := worker.New(worker.Config{
		NATSUrl:          natsAddr,
		ClusterID:        clusterID,
		MaxConcurrent:    maxConcurrent,
		CacheBytes:       cacheBytes,
		MemoryBudget:     memoryBudget,
		SpillDir:         spillDir,
		ResultStoreBytes: resultStoreBytes,
	}, store, nc, js, logger)

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
// buildSnapshotConfig converts the YAML/env CatalogSnapshot config into the
// coordinator's SnapshotConfig. Returns defaults when no config file is loaded.
func catalogCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "catalog",
		Short: "Catalog management commands",
	}
	cmd.AddCommand(catalogSnapshotCmd())
	cmd.AddCommand(catalogRestoreCmd())
	cmd.AddCommand(catalogListSnapshotsCmd())
	return cmd
}

func catalogSnapshotCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshot",
		Short: "Take a catalog snapshot now",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := newStore()
			if err != nil {
				return err
			}
			cat := catalog.NewWithStore(store, bucket)
			if err := cat.Init(ctx); err != nil {
				return fmt.Errorf("initializing catalog: %w", err)
			}

			var fileCfg *config.Config
			if configFile != "" {
				fileCfg, _ = config.Load(configFile)
			}
			snapCfg := buildSnapshotConfig(fileCfg)
			s := coordinator.NewCatalogSnapshotter(cat, store, bucket, snapCfg, nil)

			snap, err := s.TakeSnapshot(ctx)
			if err != nil {
				return err
			}
			fmt.Printf("Snapshot saved: %s (%d entries)\n", snap.Timestamp.Format(time.RFC3339), len(snap.Entries))
			return nil
		},
	}
}

func catalogRestoreCmd() *cobra.Command {
	var snapshotKey string
	cmd := &cobra.Command{
		Use:   "restore",
		Short: "Restore catalog from a snapshot (latest by default)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := newStore()
			if err != nil {
				return err
			}
			cat := catalog.NewWithStore(store, bucket)
			if err := cat.Init(ctx); err != nil {
				return fmt.Errorf("initializing catalog: %w", err)
			}

			var fileCfg *config.Config
			if configFile != "" {
				fileCfg, _ = config.Load(configFile)
			}
			snapCfg := buildSnapshotConfig(fileCfg)
			s := coordinator.NewCatalogSnapshotter(cat, store, bucket, snapCfg, nil)

			if snapshotKey != "" {
				snap, err := s.LoadSnapshot(ctx, snapshotKey)
				if err != nil {
					return err
				}
				if err := s.Restore(ctx, snap); err != nil {
					return err
				}
				fmt.Printf("Restored from %s (%d entries, %s)\n", snapshotKey, len(snap.Entries), snap.Timestamp.Format(time.RFC3339))
			} else {
				snap, err := s.RestoreLatest(ctx)
				if err != nil {
					return err
				}
				fmt.Printf("Restored latest snapshot (%d entries, %s)\n", len(snap.Entries), snap.Timestamp.Format(time.RFC3339))
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&snapshotKey, "from", "", "Specific snapshot key to restore (default: latest)")
	return cmd
}

func catalogListSnapshotsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "snapshots",
		Short: "List available catalog snapshots",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			store, err := newStore()
			if err != nil {
				return err
			}
			cat := catalog.NewWithStore(store, bucket)

			var fileCfg *config.Config
			if configFile != "" {
				fileCfg, _ = config.Load(configFile)
			}
			snapCfg := buildSnapshotConfig(fileCfg)
			s := coordinator.NewCatalogSnapshotter(cat, store, bucket, snapCfg, nil)

			snapshots, err := s.ListSnapshots(ctx)
			if err != nil {
				return err
			}
			if len(snapshots) == 0 {
				fmt.Println("No snapshots found.")
				return nil
			}
			for _, obj := range snapshots {
				fmt.Printf("  %s  (%d bytes)\n", obj.Key, obj.Size)
			}
			return nil
		},
	}
}

func buildSnapshotConfig(fileCfg *config.Config) coordinator.SnapshotConfig {
	cfg := coordinator.SnapshotConfig{
		Enabled:    true,
		LeaderOnly: true,
	}

	if fileCfg == nil {
		return cfg
	}
	snap := fileCfg.CatalogSnapshot

	if snap.Enabled != nil {
		cfg.Enabled = *snap.Enabled
	}
	if snap.Interval != "" {
		if d, err := time.ParseDuration(snap.Interval); err == nil {
			cfg.Interval = d
		}
	}
	if snap.Retention > 0 {
		cfg.Retention = snap.Retention
	}
	if snap.Prefix != "" {
		cfg.Prefix = snap.Prefix
	}
	if snap.Debounce != "" {
		if d, err := time.ParseDuration(snap.Debounce); err == nil {
			cfg.Debounce = d
		}
	}
	if snap.LeaderOnly != nil {
		cfg.LeaderOnly = *snap.LeaderOnly
	}
	return cfg
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
