package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/derekmwright/caelum/internal/auth"
	"github.com/derekmwright/caelum/internal/config"
	"github.com/derekmwright/caelum/internal/coordinator"
	"github.com/derekmwright/caelum/internal/distributed"
	"github.com/derekmwright/caelum/internal/metrics"
	"github.com/derekmwright/caelum/internal/server"
	"github.com/derekmwright/caelum/internal/storage/catalog"
	"github.com/derekmwright/caelum/internal/storage/objstore"
	"github.com/derekmwright/caelum/internal/worker"
	"github.com/derekmwright/caelum/pkg/caelum"
	"github.com/spf13/cobra"
)

var (
	mode       string
	endpoint   string
	accessKey  string
	secretKey  string
	bucket     string
	httpAddr   string
	natsPort   int
	natsURL    string
	configFile string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "caelum",
		Short: "Caelum — lightweight distributed analytical query engine",
		Long:  "A distributed analytical query engine that uses embedded NATS for coordination and object storage for results.",
	}

	rootCmd.PersistentFlags().StringVar(&configFile, "config", "", "YAML config file path")
	rootCmd.PersistentFlags().StringVar(&mode, "mode", "standalone", "Run mode: standalone, coordinator, or worker")
	rootCmd.PersistentFlags().StringVar(&endpoint, "endpoint", "localhost:9000", "S3-compatible endpoint")
	rootCmd.PersistentFlags().StringVar(&accessKey, "access-key", "minioadmin", "S3 access key")
	rootCmd.PersistentFlags().StringVar(&secretKey, "secret-key", "minioadmin", "S3 secret key")
	rootCmd.PersistentFlags().StringVar(&bucket, "bucket", "caelum", "Storage bucket name")
	rootCmd.PersistentFlags().StringVar(&httpAddr, "http-addr", ":8080", "HTTP API listen address")
	rootCmd.PersistentFlags().IntVar(&natsPort, "nats-port", 4222, "Embedded NATS port")
	rootCmd.PersistentFlags().StringVar(&natsURL, "nats-url", "", "NATS URL (for worker mode)")

	rootCmd.AddCommand(serveCmd())
	rootCmd.AddCommand(queryCmd())
	rootCmd.AddCommand(tablesCmd())
	rootCmd.AddCommand(shellCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func serveCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "serve",
		Short: "Start the Caelum server",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

			store, err := newStore()
			if err != nil {
				return err
			}

			cat := catalog.New(store, bucket)
			if err := cat.Init(ctx); err != nil {
				return fmt.Errorf("initializing catalog: %w", err)
			}

			switch mode {
			case "standalone":
				return runStandalone(ctx, store, cat, logger)
			case "coordinator":
				return runCoordinator(ctx, store, cat, logger)
			case "worker":
				return runWorker(ctx, store, logger)
			default:
				return fmt.Errorf("unknown mode: %s", mode)
			}
		},
	}
}

func queryCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "query [sql]",
		Short: "Execute a SQL query (standalone mode)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				// Fall back to memstore for testing
				store = objstore.NewMemStore()
			}

			db, err := caelum.Open(ctx, caelum.Config{
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

			// Print results as JSON
			enc := json.NewEncoder(os.Stdout)
			enc.SetIndent("", "  ")
			return enc.Encode(result)
		},
	}
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

			cat := catalog.New(store, bucket)
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

func shellCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "shell",
		Short: "Interactive SQL shell (standalone mode)",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()

			store, err := newStore()
			if err != nil {
				// Fall back to memstore for interactive testing
				store = objstore.NewMemStore()
			}

			db, err := caelum.Open(ctx, caelum.Config{
				Store:  store,
				Bucket: bucket,
			})
			if err != nil {
				return err
			}

			fmt.Println("Caelum SQL Shell. Type 'exit' to quit.")
			scanner := bufio.NewScanner(os.Stdin)
			for {
				fmt.Print("caelum> ")
				if !scanner.Scan() {
					break
				}
				line := strings.TrimSpace(scanner.Text())
				if line == "" {
					continue
				}
				if line == "exit" || line == "quit" || line == "\\q" {
					break
				}

				result, err := db.Query(ctx, line)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error: %v\n", err)
					continue
				}

				if len(result.Rows) == 0 {
					fmt.Println("(0 rows)")
					continue
				}

				// Print as JSON
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				enc.Encode(result.Rows)
				fmt.Printf("(%d rows)\n", len(result.Rows))
			}

			return nil
		},
	}
}

func newStore() (objstore.Store, error) {
	return objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint:  endpoint,
		AccessKey: accessKey,
		SecretKey: secretKey,
		UseSSL:    false,
	})
}

func runStandalone(ctx context.Context, store objstore.Store, cat *catalog.Catalog, logger *slog.Logger) error {
	// Start embedded NATS
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = natsPort
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		return fmt.Errorf("starting NATS: %w", err)
	}
	defer embeddedNATS.Shutdown()

	// Connect to NATS
	nc, err := distributed.Connect(embeddedNATS.ClientURL())
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

	// Start worker
	w := worker.New(worker.Config{
		NATSUrl:       embeddedNATS.ClientURL(),
		MaxConcurrent: 4,
		CacheBytes:    256 * 1024 * 1024,
	}, store, nc, js, logger)

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}
	defer w.Stop()

	// Start coordinator
	coord := coordinator.New(coordinator.Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: bucket,
	}, cat, nc, js, logger)

	// Start heartbeat monitoring and result cleanup
	coord.Workers().StartReaper(ctx)
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, 0)

	// Initialize Prometheus metrics
	m := metrics.New()

	// Build config manager and auth provider for hot-reload
	srvCfg := server.Config{
		Addr:    httpAddr,
		Catalog: cat,
		Metrics: m,
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
			srvCfg.Provider = provider

			// Subscribe to config changes to rebuild auth
			cfgMgr.Subscribe(func(event config.ChangeEvent) {
				authCfg := buildAuthConfig(event.New.Auth)
				policyCfgs := buildPolicyConfigs(event.New.Auth.Policies)
				provider.UpdateFromConfig(authCfg, policyCfgs)
				logger.Info("auth hot-reloaded",
					"enabled", event.New.Auth.Enabled,
					"api_keys", len(event.New.Auth.APIKeys),
					"roles", len(event.New.Auth.Roles),
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down...")
		coord.Workers().Close()
		srv.Shutdown(context.Background())
		return nil
	case err := <-errCh:
		return err
	}
}

func runCoordinator(ctx context.Context, store objstore.Store, cat *catalog.Catalog, logger *slog.Logger) error {
	// Start embedded NATS
	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = natsPort
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		return fmt.Errorf("starting NATS: %w", err)
	}
	defer embeddedNATS.Shutdown()

	nc, err := distributed.Connect(embeddedNATS.ClientURL())
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

	coord := coordinator.New(coordinator.Config{
		NATSUrl:      embeddedNATS.ClientURL(),
		ResultBucket: bucket,
	}, cat, nc, js, logger)
	coord.Workers().StartReaper(ctx)
	coord.Cleaner(store, bucket).StartPeriodicCleanup(ctx, 0)

	m := metrics.New()

	srvCfg := server.Config{
		Addr:    httpAddr,
		Catalog: cat,
		Metrics: m,
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
			srvCfg.Provider = provider

			cfgMgr.Subscribe(func(event config.ChangeEvent) {
				authCfg := buildAuthConfig(event.New.Auth)
				policyCfgs := buildPolicyConfigs(event.New.Auth.Policies)
				provider.UpdateFromConfig(authCfg, policyCfgs)
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

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start()
	}()

	select {
	case <-ctx.Done():
		logger.Info("coordinator shutting down...")
		coord.Workers().Close()
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

	nc, err := distributed.Connect(natsAddr)
	if err != nil {
		return fmt.Errorf("connecting to NATS: %w", err)
	}
	defer nc.Close()

	js, err := distributed.NewJetStream(nc)
	if err != nil {
		return fmt.Errorf("creating JetStream: %w", err)
	}

	w := worker.New(worker.Config{
		NATSUrl:       natsAddr,
		MaxConcurrent: 4,
		CacheBytes:    256 * 1024 * 1024,
	}, store, nc, js, logger)

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}

	<-ctx.Done()
	w.Stop()
	return nil
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

func buildTLSConfig(cfg config.AuthMTLS) (*tls.Config, error) {
	clientCA, err := auth.LoadClientCA(cfg.CAFile)
	if err != nil {
		return nil, err
	}
	return auth.NewTLSConfig(cfg.CertFile, cfg.KeyFile, clientCA)
}
