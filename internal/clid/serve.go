// SPDX-License-Identifier: AGPL-3.0-only

// Package clid is the distributed server: the run modes `wadjetd serve`
// carries, standalone, coordinator and worker.
//
// It is the AGPL-3.0 side of the command line. The command tree, the flags
// and the configuration precedence that resolves them live in internal/cli
// (MIT) and are imported from here; nothing in internal/cli imports this
// package, which is the direction the import-boundary gate holds. See
// LICENSING.md.
package clid

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	nethttppprof "net/http/pprof"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"

	"github.com/derekmwright/wadjet/internal/alerts"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/cli"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/geoip"
	"github.com/derekmwright/wadjet/internal/metrics"
	"github.com/derekmwright/wadjet/internal/server"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// ServeCmd is `wadjetd serve`: the distributed server, in every mode.
//
// The prologue — logger, memory envelope, object store — is the embedded
// binary's (internal/cli), because a server process is a server process
// whichever engine it runs. What is here is the part that is not: the run
// modes that build a coordinator, a worker, or both in one process.
func ServeCmd() *cobra.Command {
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

		logger, closeSink := cli.ServeLogger()
		defer closeSink()

		cli.ApplyServeRuntimeEnvelope(logger)

		store, err := cli.OpenServeStore(logger)
		if err != nil {
			return err
		}

		// Read AFTER the envelope: the auto-detected file cache, per-task
		// budget, shared pool, result store and mmap ceiling are resolved
		// there, and a snapshot taken before it would carry the zeros that
		// mean "auto-detect".
		o := cli.ServeOptionsNow()

		// WADJET_ENABLE_ALERTS used to be read here, and it beat the flag
		// unconditionally — the opposite of every other tier. It is an
		// ordinary resolved key now (alerts.enabled), so `alerts:` in the
		// config file works and an explicit --enable-alerts=false wins
		// (ADR-0029).
		if v := os.Getenv("WADJET_CATALOG_SNAPSHOT_PREFIX"); v != "" {
			*catalogSnapshotPrefix = v
		}
		if v := os.Getenv("WADJET_CATALOG_SNAPSHOT_INTERVAL"); v != "" {
			if d, err := time.ParseDuration(v); err == nil {
				*catalogSnapshotInterval = d
			}
		}

		switch cli.ServeMode() {
		case "standalone":
			return runStandalone(ctx, o, store, logger, o.EnableAlerts, *catalogSnapshotPrefix, *catalogSnapshotInterval, *forceRestoreCatalog)
		case "coordinator":
			return runCoordinator(ctx, o, store, logger, o.EnableAlerts, *catalogSnapshotPrefix, *catalogSnapshotInterval, *forceRestoreCatalog)
		case "worker":
			return runWorker(ctx, o, store, logger)
		default:
			return fmt.Errorf("unknown mode: %s", cli.ServeMode())
		}
	}
	return cmd
}

func runStandalone(ctx context.Context, o cli.ServeOptions, store objstore.Store, logger *slog.Logger, alertsEnabled bool, snapshotPrefix string, snapshotInterval time.Duration, forceRestoreTS string) error {
	// Opt-in heap profile dumper for OOM debugging. No-op unless
	// WADJET_HEAP_DUMP_INTERVAL is set. See cmd/wadjet/heap_dumper.go.
	cli.StartHeapDumper(ctx, logger)

	// Periodic background GC: with the default gogc=off, large transient
	// garbage (catalog priming, NATS message buffers) accumulates and
	// can push baseline heap to 11+ GB before any query runs (Q18 SF10
	// 2026-04-25, project_q18_sf10_native_dag_oom_2026-04-24). One GC
	// every 30s reclaims this cheaply (~50ms per call) without the
	// per-allocation GC assist tax that GOGC=100 imposes. Override via
	// WADJET_BG_GC_INTERVAL ("off" to disable, "<duration>" otherwise).
	cli.StartBackgroundGC(ctx, logger)

	// Start embedded NATS (with optional leaf node connections)
	natsCfg := cli.NATSServerConfig()
	// The same advisory lock the CLI's embedded fallback takes (#842).
	// nats-server does not lock its own store directory, so without this a
	// second process opening the same JetStream file store writes over this
	// one's catalog metadata — and since `tables`, `query`, `create-table`,
	// `drop-table` and `shell` can now run an embedded server of their own,
	// a lock only one side took would be no lock at all.
	storeLock, err := cli.LockCatalogStoreDir(natsCfg.StoreDir)
	if err != nil {
		return fmt.Errorf("the catalog store directory %s is held by another wadjet process "+
			"(%w); stop it, or give this one its own --nats-store-dir", natsCfg.StoreDir, err)
	}
	defer storeLock.Release()
	embeddedNATS, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		return fmt.Errorf("starting NATS: %w", err)
	}
	defer embeddedNATS.Shutdown()
	// Publish where this server can be reached, so a CLI command that loses
	// the lock race reaches THIS catalog rather than whatever answers a
	// well-known port — which, with two data directories in play, is how a
	// command against one of them read and wrote the other's (round-2 B1).
	if err := storeLock.Publish(embeddedNATS.ClientURL()); err != nil {
		return fmt.Errorf("recording the catalog holder in %s: %w",
			cli.CatalogLockPath(natsCfg.StoreDir), err)
	}

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
	cat := catalog.NewWithCluster(kv, store, o.Bucket, o.ClusterID)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Restore persisted UDFs and wire persistence callback
	cli.WireUDFPersistence(cat, logger)

	// Load GeoIP databases if configured. The two dropped-error
	// `config.Load(o.ConfigFile)` reads that used to sit here are gone: the
	// file is loaded once, in the root command's PersistentPreRunE, and a
	// parse failure stops the process there instead of silently yielding a
	// nil config here (#802's doctrine, #808's loader).
	if err := cli.LoadGeoIP(logger); err != nil {
		return fmt.Errorf("loading GeoIP: %w", err)
	}
	defer geoip.Close()

	// Construct worker (deferred Start; the gRPC data-plane wiring below
	// must complete first so Start can see the dpClient and skip the
	// JetStream Fetch loop in --data-plane=grpc mode).
	w := worker.New(worker.Config{
		NATSUrl:               embeddedNATS.ClientURL(),
		ClusterID:             o.ClusterID,
		MaxConcurrent:         o.MaxConcurrent,
		MorselWorkers:         morselWorkersConfig(o.MorselWorkers),
		DrainTimeout:          o.DrainTimeout,
		CacheBytes:            o.CacheBytes,
		MemoryBudget:          o.MemoryBudget,
		SharedPoolBudget:      o.SharedPoolBudget,
		SpillDir:              o.SpillDir,
		ResultStoreBytes:      o.ResultStoreBytes,
		Reservoirs:            memory.NewReservoirRegistry(),
		FloatingBudgetActive:  o.SpillFloatingBudget,
		MmapRelief:            o.MmapRelief,
		MmapReliefThresholdMB: o.MmapReliefThresholdMB,
		BoundedDirtyWrites:    o.BoundedDirtyWrites,
		PeerListenAddr:        peerListenAddr(o),
		PeerAdvertiseAddr:     o.PeerExchangeAdvertise,
		StreamingShuffleRead:  o.StreamingShuffleRead,
		AsyncScratchPurge:     o.AsyncScratchPurge,
		PeerWireCompression:   o.PeerWireCompression,
		ScanDecodeAhead:       o.ScanDecodeAhead,
		ScanDecodeAheadBytes:  o.ScanDecodeAheadBytes,
		ShuffleDecodeAhead:    o.ShuffleDecodeAhead,
		DecodedCacheBytes:     o.DecodedCacheBytes,
	}, store, nc, js, logger)

	// Initialize Prometheus metrics (before worker.Start so spill metrics are wired)
	m := metrics.New()
	m.Registry.MustRegister(alerts.Collectors()...)
	w.SetMetrics(m)
	if cb := objstore.FindCircuitStore(store); cb != nil {
		cb.SetOnOpen(func(class objstore.OpClass) {
			m.CircuitBreakerOpened.WithLabelValues(class.String()).Inc()
		})
	}

	// Start coordinator
	durability, err := parseShuffleDurability(o.ShuffleDurability)
	if err != nil {
		return err
	}
	coord := coordinator.New(coordinator.Config{
		NATSUrl:                embeddedNATS.ClientURL(),
		ResultBucket:           o.Bucket,
		DynamicFilters:         dynamicFiltersFromEnv(),
		LocalFastPathBytes:     o.LocalFastPathBytes,
		IntermediateTTL:        o.QueryIntermediateTTL,
		BroadcastBytesOverride: o.BroadcastBytes,
		SortMergeJoinBytes:     o.SortMergeJoinBytes,
		LateMaterialization:    o.LateMaterialization,
		BushyJoinReorder:       o.BushyJoinReorder,
		SkewSplit:              o.SkewSplit,
		AggPartialSplit:        o.AggPartialSplit,
		StreamingExchange:      o.StreamingExchange,
		EagerDispatch:          o.EagerDispatch,
		ShuffleDurability:      durability,
		LocalityPlacement:      o.LocalityPlacement,
	}, cat, nc, js, logger)

	// Phase A: same-process data-plane server + client when enabled.
	// Worker dials localhost:o.DataPlaneAddr. Phase B: coord registers
	// gather receivers; worker streams results via dpClient.
	// Phase C: coord pushes TaskDispatch over the gRPC stream; worker.Start
	// (below) sees a non-nil dpClient and skips its JetStream Fetch loop.
	var dpSrv *dataplane.Server
	var dpClient *dataplane.Client
	if o.DataPlane == "grpc" {
		dpSrv = dataplane.NewServer(dataplane.ServerConfig{
			Addr:      o.DataPlaneAddr,
			ClusterID: o.ClusterID,
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
	coord.Cleaner(store, o.Bucket).StartPeriodicCleanup(ctx, o.QueryIntermediateGC)

	// OpenTelemetry. runStandalone did not call this at all, so the
	// `telemetry:` section and WADJET_OTEL_* reached nothing in the DEFAULT
	// run mode while working in the other two — "the config reaches runtime"
	// has to mean every mode that has the consumer, not two of three.
	if otelTP := cli.InitTelemetry(ctx, logger); otelTP != nil {
		coord.SetTelemetry(otelTP)
		defer otelTP.Shutdown(context.Background())
	}

	// Enable alerts feature flag; in standalone mode StartLeaderWatch is a
	// no-op so we start the scheduler directly here if enabled.
	coord.SetAlertsEnabled(alertsEnabled)
	if alertsEnabled {
		coord.StartAlertScheduler(ctx)
	}

	// Wire CLI-driven catalog snapshot options.
	if snapshotPrefix != "" {
		snapBucket, snapPath, err := cli.ParseS3URL(snapshotPrefix)
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
		Enabled:              o.BackgroundCompaction,
		Compaction:           compaction.DefaultConfig(),
		ReclaimDroppedTables: o.ReclaimDroppedTables,
	}, logger)
	compactor.Start(ctx)

	// Build config manager and auth provider for hot-reload
	srvCfg := server.Config{
		Addr:                o.HTTPAddr,
		Catalog:             cat,
		Coordinator:         coord,
		Metrics:             m,
		SortMergeJoinBytes:  o.SortMergeJoinBytes,
		LateMaterialization: o.LateMaterialization,
		BushyJoinReorder:    o.BushyJoinReorder,
	}

	var cfgMgr *config.Manager
	var provider *auth.Provider
	// Hoisted out of the block below: the pgwire DB is opened further down
	// and needs the same limits.
	// The cost guard reaches every planner a served query can meet: the
	// HTTP server's own (embedded, no-coordinator) path, the coordinator's
	// four, and — below — the embedded DB that pgwire falls back to for any
	// statement its routing gate declines and for every statement when a
	// provider is present but disabled (#803).
	//
	// It is wired from the RESOLVED config and OUTSIDE the `--config` block.
	// Both assignments used to live inside it, so a deployment that exported
	// WADJET_QUERY_MAX_SCAN_BYTES and passed no config file resolved the key,
	// reported it through GET /v1/admin/config, and ran with no cost guard at
	// all — #808's own shape surviving in one corner. Per-role limits still
	// come from the file, because roles do.
	globalLimits, roleLimits := cli.EffectiveConfig().EffectiveQueryLimits()
	srvCfg.QueryLimits, srvCfg.RoleLimits = globalLimits, roleLimits
	coord.SetQueryLimits(globalLimits, roleLimits)

	if o.ConfigFile != "" {
		fileCfg, mgr, prov, wireErr := cli.WireAuthFromConfig(ctx, o.ConfigFile, logger)
		if wireErr != nil {
			return wireErr
		}
		cfgMgr, provider = mgr, prov
		srvCfg.Provider = provider
		// Bind the policy set's NAMES to the catalog, once, now that it
		// exists. A relation or a policed column that does not resolve
		// refuses STARTUP: before #882 such a policy loaded quietly and its
		// scoped rules simply never matched, which beside a broad allow is a
		// grant, not a refusal. ADR-0033: a policy that cannot be enforced
		// does not load.
		if err := provider.BindToCatalog(ctx, cat); err != nil {
			return fmt.Errorf("binding auth policies to the catalog: %w", err)
		}

		if fileCfg.Auth.MTLS.Enabled {
			tlsCfg, err := cli.BuildTLSConfig(fileCfg.Auth.MTLS)
			if err != nil {
				return fmt.Errorf("configuring mTLS: %w", err)
			}
			srvCfg.TLSConfig = tlsCfg
		}
	}

	// Configure embedding provider for embed() if one is requested.
	cli.ConfigureEmbeddingProvider(logger)

	// Start HTTP server
	srv := server.New(srvCfg, logger)

	// Register admin API if config manager is available
	// Coordinator-side ABAC: with the provider wired, ExecuteSQL enforces
	// table/row/column policies itself, which lets pgwire route authed
	// connections through the native-DAG executor and local fast path.
	if provider != nil {
		coord.SetAuthProvider(provider)
	}

	if cfgMgr != nil && provider != nil {
		admin := server.NewAdminAPI(cfgMgr, provider, logger)
		admin.RegisterRoutes(srv.Mux())
	}

	// Register ops API (workers, cleanup)
	ops := server.NewOpsAPI(coord, provider)
	ops.RegisterRoutes(srv.Mux())

	// Start gRPC server
	grpcSrv := server.NewGRPCServer(server.GRPCConfig{
		Addr:         o.GRPCAddr,
		Catalog:      cat,
		Coord:        coord,
		AuthProvider: provider,
	}, logger)

	// Start PostgreSQL wire protocol server
	// The pgwire fallback DB is a planner of this server's too — every
	// statement the routing gate declines is planned here (a leading
	// comment, TABLE, VALUES, and every statement when a provider is
	// present but routing is disabled) — so it carries the server's OWN
	// planner and engine options rather than inheriting one from whichever
	// other instance happened to be opened first (#1223), and rather than
	// running the other four on their zero values while the coordinator and
	// worker it sits beside run on the resolved ones — which is what made
	// --late-materialization false here and true there (#1226).
	pgDB, err := wadjet.Open(ctx, wadjet.Config{
		Store:               store,
		Bucket:              o.Bucket,
		MetaKV:              kv,
		AuthProvider:        provider,
		QueryLimits:         globalLimits,
		RoleLimits:          roleLimits,
		SortMergeJoinBytes:  o.SortMergeJoinBytes,
		LateMaterialization: o.LateMaterialization,
		BushyJoinReorder:    o.BushyJoinReorder,
		MemoryBudget:        o.MemoryBudget,
		SpillDir:            o.SpillDir,
	})
	if err != nil {
		return fmt.Errorf("opening DB for pgwire: %w", err)
	}
	pgQueryTimeout, _ := time.ParseDuration(o.QueryTimeout)
	pgCfg := pgwire.Config{
		AuthProvider:     provider,
		QueryTimeout:     pgQueryTimeout,
		MaxConcurrentQry: o.MaxConcurrentQry,
	}
	if o.PGTLSCert != "" && o.PGTLSKey != "" {
		cert, err := tls.LoadX509KeyPair(o.PGTLSCert, o.PGTLSKey)
		if err != nil {
			return fmt.Errorf("loading pgwire TLS cert: %w", err)
		}
		pgCfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
	}
	pgSrv := pgwire.NewServer(pgDB, pgCfg, logger)
	// Route SELECT/WITH through coord.ExecuteSQL (native-DAG executor) when
	// available — bypasses the legacy db.Query CollectSink materialization
	// path that OOMed on Q18 SF10 (project_q18_sf10_native_dag_oom_2026-04-24).
	pgSrv.SetRouter(coordinator.NewQueryRouter(coord))

	errCh := make(chan error, 3)
	go func() {
		errCh <- srv.Start()
	}()
	go func() {
		errCh <- grpcSrv.Start()
	}()
	go func() {
		if err := pgSrv.Start(o.PGAddr); err != nil {
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
		if cleaner := coord.Cleaner(store, o.Bucket); cleaner != nil {
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
func runCoordinator(ctx context.Context, o cli.ServeOptions, store objstore.Store, logger *slog.Logger, alertsEnabled bool, snapshotPrefix string, snapshotInterval time.Duration, forceRestoreTS string) error {
	// Start embedded NATS (with optional leaf node connections)
	// Bind to 0.0.0.0 so remote workers can connect.
	natsCfg := cli.NATSServerConfig()
	natsCfg.Host = "0.0.0.0"
	// Apply NATS mTLS config: CLI flag, then env var, then the config file
	// (#827). A config file that will not PARSE is a startup error, and
	// partially-specified material is a startup error.
	natsFileCfg, err := cli.LoadConfigForNATSTLS()
	if err != nil {
		return err
	}
	if err := cli.ApplyNATSTLS(&natsCfg, natsFileCfg, logger); err != nil {
		return err
	}
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
	cat := catalog.NewWithCluster(kv, store, o.Bucket, o.ClusterID)
	if err := cat.Init(ctx); err != nil {
		return fmt.Errorf("initializing catalog: %w", err)
	}

	// Restore persisted UDFs and wire persistence callback
	cli.WireUDFPersistence(cat, logger)

	durability, err := parseShuffleDurability(o.ShuffleDurability)
	if err != nil {
		return err
	}
	coord := coordinator.New(coordinator.Config{
		NATSUrl:                embeddedNATS.ClientURL(),
		ResultBucket:           o.Bucket,
		DynamicFilters:         dynamicFiltersFromEnv(),
		LocalFastPathBytes:     o.LocalFastPathBytes,
		IntermediateTTL:        o.QueryIntermediateTTL,
		BroadcastBytesOverride: o.BroadcastBytes,
		SortMergeJoinBytes:     o.SortMergeJoinBytes,
		LateMaterialization:    o.LateMaterialization,
		BushyJoinReorder:       o.BushyJoinReorder,
		SkewSplit:              o.SkewSplit,
		AggPartialSplit:        o.AggPartialSplit,
		StreamingExchange:      o.StreamingExchange,
		EagerDispatch:          o.EagerDispatch,
		ShuffleDurability:      durability,
		LocalityPlacement:      o.LocalityPlacement,
	}, cat, nc, js, logger)

	// Phase A: start data-plane gRPC server alongside coord when enabled.
	// Phase B: coord registers gather receivers as ResultHandlers so
	// workers can stream results over gRPC instead of NATS.
	var dpSrv *dataplane.Server
	if o.DataPlane == "grpc" {
		dpSrv = dataplane.NewServer(dataplane.ServerConfig{
			Addr:      o.DataPlaneAddr,
			ClusterID: o.ClusterID,
		}, logger)
		if err := dpSrv.Start(); err != nil {
			return fmt.Errorf("dataplane server: %w", err)
		}
		defer dpSrv.Stop(3 * time.Second)
		coord.SetDataPlaneServer(dpSrv)
	}

	// Initialize OTel tracing if configured
	otelTP := cli.InitTelemetry(ctx, logger)
	if otelTP != nil {
		coord.SetTelemetry(otelTP)
		defer otelTP.Shutdown(context.Background())
	}

	coord.Workers().StartReaper(ctx)
	coord.Workers().StartSubStatsLogger(ctx)
	coord.StartQueryReaper(ctx)
	coord.StartQueryActiveHandler()
	coord.Cleaner(store, o.Bucket).StartPeriodicCleanup(ctx, o.QueryIntermediateGC)

	// Enable alerts feature flag; in coordinator mode StartLeaderWatch manages
	// the scheduler lifecycle on leader transitions.
	coord.SetAlertsEnabled(alertsEnabled)

	// Wire CLI-driven catalog snapshot options.
	if snapshotPrefix != "" {
		snapBucket, snapPath, err := cli.ParseS3URL(snapshotPrefix)
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
		Enabled:              o.BackgroundCompaction,
		Compaction:           compaction.DefaultConfig(),
		ReclaimDroppedTables: o.ReclaimDroppedTables,
	}, logger)
	coordCompactor.Start(ctx)

	m := metrics.New()
	m.Registry.MustRegister(alerts.Collectors()...)
	if cb := objstore.FindCircuitStore(store); cb != nil {
		cb.SetOnOpen(func(class objstore.OpClass) {
			m.CircuitBreakerOpened.WithLabelValues(class.String()).Inc()
		})
	}
	dlq := coordinator.NewDLQ(js)

	srvCfg := server.Config{
		Addr:                o.HTTPAddr,
		Catalog:             cat,
		Coordinator:         coord,
		DLQ:                 dlq,
		Metrics:             m,
		SortMergeJoinBytes:  o.SortMergeJoinBytes,
		LateMaterialization: o.LateMaterialization,
		BushyJoinReorder:    o.BushyJoinReorder,
	}

	var cfgMgr *config.Manager
	var provider *auth.Provider

	// The cost guard, from the resolved config and outside the `--config`
	// block — see runStandalone. This mode serves no pgwire listener (#803).
	globalLimits, roleLimits := cli.EffectiveConfig().EffectiveQueryLimits()
	srvCfg.QueryLimits, srvCfg.RoleLimits = globalLimits, roleLimits
	coord.SetQueryLimits(globalLimits, roleLimits)

	if o.ConfigFile != "" {
		fileCfg, mgr, prov, wireErr := cli.WireAuthFromConfig(ctx, o.ConfigFile, logger)
		if wireErr != nil {
			return wireErr
		}
		cfgMgr, provider = mgr, prov
		srvCfg.Provider = provider
		// Bind the policy set's NAMES to the catalog, once, now that it
		// exists. A relation or a policed column that does not resolve
		// refuses STARTUP: before #882 such a policy loaded quietly and its
		// scoped rules simply never matched, which beside a broad allow is a
		// grant, not a refusal. ADR-0033: a policy that cannot be enforced
		// does not load.
		if err := provider.BindToCatalog(ctx, cat); err != nil {
			return fmt.Errorf("binding auth policies to the catalog: %w", err)
		}

		if fileCfg.Auth.MTLS.Enabled {
			tlsCfg, err := cli.BuildTLSConfig(fileCfg.Auth.MTLS)
			if err != nil {
				return fmt.Errorf("configuring mTLS: %w", err)
			}
			srvCfg.TLSConfig = tlsCfg
		}
	}

	// Configure embedding provider for coordinator mode
	cli.ConfigureEmbeddingProvider(logger)

	srv := server.New(srvCfg, logger)

	// Coordinator-side ABAC: with the provider wired, ExecuteSQL enforces
	// table/row/column policies itself, which lets pgwire route authed
	// connections through the native-DAG executor and local fast path.
	if provider != nil {
		coord.SetAuthProvider(provider)
	}

	if cfgMgr != nil && provider != nil {
		admin := server.NewAdminAPI(cfgMgr, provider, logger)
		admin.RegisterRoutes(srv.Mux())
	}

	// Register ops API
	ops := server.NewOpsAPI(coord, provider)
	ops.RegisterRoutes(srv.Mux())

	// Start gRPC server
	grpcSrv := server.NewGRPCServer(server.GRPCConfig{
		Addr:         o.GRPCAddr,
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
		if cleaner := coord.Cleaner(store, o.Bucket); cleaner != nil {
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
func runWorker(ctx context.Context, o cli.ServeOptions, store objstore.Store, logger *slog.Logger) error {
	natsAddr := o.NATSURL
	if natsAddr == "" {
		natsAddr = fmt.Sprintf("nats://127.0.0.1:%d", o.NATSPort)
	}

	// Build NATS client TLS config if mTLS is configured. The config file is
	// a tier here, and material named but unusable is a startup error rather
	// than a silent plaintext connection (#827).
	natsFileCfg, err := cli.LoadConfigForNATSTLS()
	if err != nil {
		return err
	}
	var natsTLSCfg *tls.Config
	tlsCert, tlsKey, tlsCA, err := cli.ResolveNATSTLSPaths(natsFileCfg)
	if err != nil {
		return err
	}
	if tlsCert != "" && tlsKey != "" && tlsCA != "" {
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

	// Load GeoIP databases if configured (from the resolved config; see
	// runStandalone).
	if err := cli.LoadGeoIP(logger); err != nil {
		return fmt.Errorf("loading GeoIP: %w", err)
	}
	defer geoip.Close()

	// Generate a worker id once so both the existing NATS worker code and
	// the new data-plane client share the same identity.
	workerID := "worker-" + uuid.New().String()[:8]

	w := worker.New(worker.Config{
		WorkerID:              workerID,
		NATSUrl:               natsAddr,
		ClusterID:             o.ClusterID,
		MaxConcurrent:         o.MaxConcurrent,
		MorselWorkers:         morselWorkersConfig(o.MorselWorkers),
		DrainTimeout:          o.DrainTimeout,
		CacheBytes:            o.CacheBytes,
		MemoryBudget:          o.MemoryBudget,
		SharedPoolBudget:      o.SharedPoolBudget,
		SpillDir:              o.SpillDir,
		ResultStoreBytes:      o.ResultStoreBytes,
		Reservoirs:            memory.NewReservoirRegistry(),
		FloatingBudgetActive:  o.SpillFloatingBudget,
		MmapRelief:            o.MmapRelief,
		MmapReliefThresholdMB: o.MmapReliefThresholdMB,
		BoundedDirtyWrites:    o.BoundedDirtyWrites,
		PeerListenAddr:        peerListenAddr(o),
		PeerAdvertiseAddr:     o.PeerExchangeAdvertise,
		StreamingShuffleRead:  o.StreamingShuffleRead,
		AsyncScratchPurge:     o.AsyncScratchPurge,
		PeerWireCompression:   o.PeerWireCompression,
		ScanDecodeAhead:       o.ScanDecodeAhead,
		ScanDecodeAheadBytes:  o.ScanDecodeAheadBytes,
		ShuffleDecodeAhead:    o.ShuffleDecodeAhead,
		DecodedCacheBytes:     o.DecodedCacheBytes,
	}, store, nc, js, logger)
	w.SetControlConn(controlNC)

	// Phase A: open the data-plane stream to coord when enabled. Heartbeats,
	// cancellation, KV stay on NATS. Phases B–E migrate task dispatch,
	// results, gather, progress onto this stream.
	var dpClient *dataplane.Client
	if o.DataPlane == "grpc" {
		addr := o.CoordDataPlane
		if addr == "" {
			// Derive from natsAddr host: keep host, swap port to data-plane.
			addr = deriveDataPlaneAddr(natsAddr, o.DataPlaneAddr)
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
	cli.StartHeapDumper(ctx, logger)

	// Initialize Prometheus metrics
	m := metrics.New()
	w.SetMetrics(m)
	if cb := objstore.FindCircuitStore(store); cb != nil {
		cb.SetOnOpen(func(class objstore.OpClass) {
			m.CircuitBreakerOpened.WithLabelValues(class.String()).Inc()
		})
	}

	// Start /metrics HTTP endpoint for Prometheus scraping, plus the
	// Kubernetes lifecycle surface: liveness, readiness (false once
	// draining, so the pod drops out of any Service while it finishes),
	// and a POST /drain admin hook (preStop-hook alternative to SIGTERM).
	metricsMux := http.NewServeMux()
	metricsMux.HandleFunc("/healthz", func(rw http.ResponseWriter, _ *http.Request) {
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("ok"))
	})
	metricsMux.HandleFunc("/readyz", func(rw http.ResponseWriter, _ *http.Request) {
		if w.Draining() {
			rw.WriteHeader(http.StatusServiceUnavailable)
			rw.Write([]byte("draining"))
			return
		}
		rw.WriteHeader(http.StatusOK)
		rw.Write([]byte("ok"))
	})
	metricsMux.HandleFunc("/drain", func(rw http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			rw.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.BeginDrain()
		rw.WriteHeader(http.StatusAccepted)
		rw.Write([]byte("draining"))
	})
	metricsMux.Handle("/metrics", m.Handler())
	metricsMux.HandleFunc("/debug/pprof/", nethttppprof.Index)
	metricsMux.HandleFunc("/debug/pprof/cmdline", nethttppprof.Cmdline)
	metricsMux.HandleFunc("/debug/pprof/profile", nethttppprof.Profile)
	metricsMux.HandleFunc("/debug/pprof/symbol", nethttppprof.Symbol)
	metricsMux.HandleFunc("/debug/pprof/trace", nethttppprof.Trace)
	metricsMux.Handle("/debug/pprof/goroutine", nethttppprof.Handler("goroutine"))
	metricsMux.Handle("/debug/pprof/heap", nethttppprof.Handler("heap"))
	metricsMux.Handle("/debug/pprof/allocs", nethttppprof.Handler("allocs"))
	metricsSrv := &http.Server{Addr: o.MetricsAddr, Handler: metricsMux}
	go func() {
		logger.Info("worker metrics server listening", "addr", o.MetricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("metrics server error", "error", err)
		}
	}()
	defer metricsSrv.Shutdown(context.Background())

	// Initialize OTel tracing on worker
	workerOtelTP := cli.InitTelemetry(ctx, logger)
	if workerOtelTP != nil {
		w.SetTelemetry(workerOtelTP)
		defer workerOtelTP.Shutdown(context.Background())
	}

	if err := w.Start(ctx); err != nil {
		return fmt.Errorf("starting worker: %w", err)
	}

	// Signal contract for workers (Kubernetes-compatible):
	//   SIGTERM, SIGQUIT  -> graceful drain: stop taking tasks, finish
	//                        in-flight work, flush stage-output uploads,
	//                        exit. K8s sends SIGTERM on pod termination;
	//                        --drain-timeout (or the pod's grace period)
	//                        bounds it.
	//   SIGINT            -> hard stop (interactive Ctrl-C).
	// The serve command's NotifyContext claimed SIGINT+SIGTERM for a hard
	// cancel before we got here; detach both so SIGTERM cannot race into
	// the hard path and cancel in-flight task contexts mid-drain.
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
	hardCh := make(chan os.Signal, 1)
	signal.Notify(hardCh, syscall.SIGINT)
	drainSigCh := make(chan os.Signal, 1)
	signal.Notify(drainSigCh, syscall.SIGTERM, syscall.SIGQUIT)

	select {
	case <-hardCh:
		logger.Info("SIGINT received, stopping worker...")
		w.Stop()
	case sig := <-drainSigCh:
		logger.Info("drain signal received, draining worker...", "signal", sig.String())
		w.Drain()
	case <-w.DrainRequested():
		// NATS drain subject (coordinator reap) or POST /drain.
		logger.Info("drain requested via control plane, draining worker...")
		w.Drain()
	}
	return nil
}

// dynamicFiltersFromEnv reads WADJET_DYNAMIC_FILTERS and reports whether
// the Trino-style semi-join dynamic-filter optimization should be enabled
// on the coordinator. Accepts "1", "true" (case-insensitive). Off otherwise
// — v1 default until validated at SF10/SF100.
// parseShuffleDurability maps the --shuffle-durability flag value to the
// wire policy ("eager" is the zero value).
func parseShuffleDurability(s string) (distributed.UploadPolicy, error) {
	switch s {
	case "", "eager":
		return distributed.UploadEager, nil
	case "lazy":
		return distributed.UploadLazy, nil
	case "off":
		return distributed.UploadOff, nil
	default:
		return "", fmt.Errorf("invalid --shuffle-durability %q (want eager, lazy, or off)", s)
	}
}
func dynamicFiltersFromEnv() bool {
	v := os.Getenv("WADJET_DYNAMIC_FILTERS")
	return v == "1" || strings.EqualFold(v, "true")
}

// peerListenAddr resolves the worker's peer-exchange listen address:
// --peer-exchange-addr when --streaming-exchange is set, else "" (no peer
// server, no advertised address — the coordinator never hints at this
// worker).
func peerListenAddr(o cli.ServeOptions) string {
	if !o.StreamingExchange {
		return ""
	}
	return o.PeerExchangeAddr
}

// morselWorkersConfig maps the --morsel-workers flag to worker.Config
// semantics: flag 0 (auto) becomes Config -1, because the Config zero value
// must stay serial/dormant for programmatic callers that never set it.
func morselWorkersConfig(flagVal int) int {
	if flagVal == 0 {
		return -1
	}
	return flagVal
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
