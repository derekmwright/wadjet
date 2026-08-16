package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/derekmwright/wadjet/wadjet"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/server"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/spf13/cobra"
)

var (
	unifiHost string
	unifiUser string
	unifiPass string
	unifiSite string

	s3Endpoint string
	s3Access   string
	s3Secret   string
	s3Bucket   string

	natsURL      string
	pollInterval time.Duration
	httpAddr     string
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "unifi-ingest",
		Short: "Poll UniFi Controller API and ingest network telemetry into Wadjet",
		RunE:  run,
	}

	rootCmd.Flags().StringVar(&unifiHost, "unifi-host", envOrDefault("UNIFI_HOST", "192.168.1.1"), "UniFi Controller host")
	rootCmd.Flags().StringVar(&unifiUser, "unifi-user", envOrDefault("UNIFI_USER", ""), "UniFi username (or set UNIFI_USER)")
	rootCmd.Flags().StringVar(&unifiPass, "unifi-pass", envOrDefault("UNIFI_PASS", ""), "UniFi password (or set UNIFI_PASS)")
	rootCmd.Flags().StringVar(&unifiSite, "unifi-site", envOrDefault("UNIFI_SITE", "default"), "UniFi site name")

	rootCmd.Flags().StringVar(&s3Endpoint, "endpoint", "localhost:9000", "S3-compatible endpoint")
	rootCmd.Flags().StringVar(&s3Access, "access-key", "minioadmin", "S3 access key")
	rootCmd.Flags().StringVar(&s3Secret, "secret-key", "minioadmin", "S3 secret key")
	rootCmd.Flags().StringVar(&s3Bucket, "bucket", "wadjet", "S3 bucket name")

	rootCmd.Flags().StringVar(&natsURL, "nats-url", "", "NATS URL for shared catalog (e.g., nats://localhost:4333)")
	rootCmd.Flags().DurationVar(&pollInterval, "poll-interval", 30*time.Second, "How often to poll the UniFi API")
	rootCmd.Flags().StringVar(&httpAddr, "http-addr", ":8080", "HTTP API listen address for querying ingested data")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func run(cmd *cobra.Command, args []string) error {
	if unifiUser == "" || unifiPass == "" {
		return fmt.Errorf("UniFi credentials required: set --unifi-user/--unifi-pass or UNIFI_USER/UNIFI_PASS")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// Connect to MinIO
	store, err := objstore.NewMinIOStore(objstore.MinIOConfig{
		Endpoint:  s3Endpoint,
		AccessKey: s3Access,
		SecretKey: s3Secret,
		UseSSL:    false,
	})
	if err != nil {
		return fmt.Errorf("connecting to S3: %w", err)
	}

	// Connect to shared NATS catalog if configured
	var metaKV catalog.MetaKV
	if natsURL != "" {
		nc, err := distributed.Connect(natsURL, nil)
		if err != nil {
			return fmt.Errorf("connecting to NATS at %s: %w", natsURL, err)
		}
		defer nc.Close()

		js, err := distributed.NewJetStream(nc)
		if err != nil {
			return fmt.Errorf("creating JetStream: %w", err)
		}

		kv, err := catalog.NewNATSKV(js)
		if err != nil {
			return fmt.Errorf("creating NATS KV catalog: %w", err)
		}
		metaKV = kv
		logger.Info("using shared NATS catalog", "url", natsURL)
	}

	// Open Wadjet DB
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: s3Bucket,
		Logger: logger,
		MetaKV: metaKV,
	})
	if err != nil {
		return fmt.Errorf("opening Wadjet: %w", err)
	}

	// Create tables if they don't exist
	tables := map[string]parquet.Schema{
		"client_traffic": ClientTrafficSchema,
		"switch_ports":   SwitchPortsSchema,
		"device_health":  DeviceHealthSchema,
	}

	existing, err := db.ListTables(ctx)
	if err != nil {
		return fmt.Errorf("listing tables: %w", err)
	}
	existingSet := make(map[string]bool, len(existing))
	for _, t := range existing {
		existingSet[t] = true
	}

	for name, schema := range tables {
		if existingSet[name] {
			logger.Info("table exists", "table", name)
			continue
		}
		if err := db.CreateTable(ctx, name, schema, []string{"date"}); err != nil {
			return fmt.Errorf("creating table %s: %w", name, err)
		}
		logger.Info("created table", "table", name)
	}

	// Create ingesters
	ingestCfg := ingest.Config{
		FlushInterval: 30 * time.Second,
		MaxBufferRows: 10_000,
		RowGroupSize:  1024,
	}

	clientIng := db.NewIngester("client_traffic", ClientTrafficSchema, []string{"date"}, ingestCfg)
	portIng := db.NewIngester("switch_ports", SwitchPortsSchema, []string{"date"}, ingestCfg)
	healthIng := db.NewIngester("device_health", DeviceHealthSchema, []string{"date"}, ingestCfg)

	clientIng.Start()
	portIng.Start()
	healthIng.Start()

	defer func() {
		logger.Info("flushing remaining data...")
		_ = clientIng.Stop(ctx)
		_ = portIng.Stop(ctx)
		_ = healthIng.Stop(ctx)
		logger.Info("shutdown complete")
	}()

	// Start HTTP API server for querying ingested data
	srv := server.New(server.Config{
		Addr:    httpAddr,
		Catalog: db.Catalog(),
	}, logger)

	go func() {
		if err := srv.Start(); err != nil && err != http.ErrServerClosed {
			logger.Error("HTTP server error", "error", err)
		}
	}()
	defer srv.Shutdown(context.Background())

	// Connect to UniFi
	unifi := NewUnifiClient(unifiHost, unifiUser, unifiPass, unifiSite, logger)
	if err := unifi.Login(ctx); err != nil {
		return fmt.Errorf("UniFi login: %w", err)
	}
	defer unifi.Logout(context.Background())

	// Initial poll
	if err := poll(ctx, unifi, clientIng, portIng, healthIng, logger); err != nil {
		logger.Error("initial poll failed", "error", err)
	}

	// Poll loop
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	logger.Info("ingestion running", "interval", pollInterval, "host", unifiHost)

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutting down...")
			return nil
		case <-ticker.C:
			if err := poll(ctx, unifi, clientIng, portIng, healthIng, logger); err != nil {
				logger.Error("poll failed", "error", err)
			}
		}
	}
}

func poll(ctx context.Context, unifi *UnifiClient, clientIng, portIng, healthIng *ingest.Ingester, logger *slog.Logger) error {
	now := time.Now().UTC()

	// Fetch both endpoints
	clients, err := unifi.FetchClients(ctx)
	if err != nil {
		return fmt.Errorf("fetching clients: %w", err)
	}

	devices, err := unifi.FetchDevices(ctx)
	if err != nil {
		return fmt.Errorf("fetching devices: %w", err)
	}

	// Build AP name map for client enrichment
	apNames := BuildAPNameMap(devices)

	// Transform and ingest
	clientRows := TransformClients(now, clients, apNames)
	if len(clientRows) > 0 {
		if err := clientIng.Ingest(ctx, clientRows); err != nil {
			return fmt.Errorf("ingesting client_traffic: %w", err)
		}
	}

	portRows := TransformSwitchPorts(now, devices)
	if len(portRows) > 0 {
		if err := portIng.Ingest(ctx, portRows); err != nil {
			return fmt.Errorf("ingesting switch_ports: %w", err)
		}
	}

	healthRows := TransformDeviceHealth(now, devices)
	if len(healthRows) > 0 {
		if err := healthIng.Ingest(ctx, healthRows); err != nil {
			return fmt.Errorf("ingesting device_health: %w", err)
		}
	}

	logger.Info("poll complete",
		"clients", len(clientRows),
		"ports", len(portRows),
		"devices", len(healthRows),
	)

	return nil
}
