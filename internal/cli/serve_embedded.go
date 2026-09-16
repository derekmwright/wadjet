// SPDX-License-Identifier: MIT

package cli

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/geoip"
	"github.com/derekmwright/wadjet/internal/natsconn"
	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/wadjet"
)

// EmbeddedServeCmd is `wadjet serve`: the PostgreSQL wire protocol over the
// engine in this process.
//
// It is the same wadjet.DB the Go API hands an embedded caller, with the
// same catalog the other commands share, published on the wire psql, JDBC,
// DataGrip and Superset speak. No coordinator, no worker, no task queues:
// pgwire is opened WITHOUT a query router, so every statement — SELECT
// included — is answered by this process's planner and executor, which is
// the path TestTwoPathInvariance holds to the same answers as the DAG.
//
// `wadjetd serve --mode=standalone|coordinator|worker` is the distributed
// server. This command refuses those two modes by name rather than pretending
// to be them.
func EmbeddedServeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Start the embedded server (PostgreSQL wire protocol over the in-process engine)",
		Long: "Start the embedded server: the PostgreSQL wire protocol over the engine in this\n" +
			"process, against the catalog and object store the storage flags name.\n\n" +
			"Distributed serving — a coordinator planning and dispatching to workers — is the\n" +
			"wadjetd binary (`wadjetd serve --mode=standalone|coordinator|worker`).",
	}

	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer cancel()

		logger, closeSink := ServeLogger()
		defer closeSink()

		// Anything that is not this binary's mode is refused BY NAME, the way
		// wadjetd refuses an unknown one. Refusing only the two exact strings
		// "coordinator" and "worker" let every typo through — `--mode=coordnator`
		// started a single-process server and answered queries, where the same
		// typo on wadjetd is `unknown mode: coordnator`, exit 1. That is the
		// failure this refusal exists to prevent, one character away (LS review P4).
		switch m := serveMode(); m {
		case "", "embedded", "standalone":
			// The embedded server. "standalone" is accepted because it is the
			// root command's default and means "one process" to a reader.
		case "coordinator", "worker":
			return fmt.Errorf("--mode=%s is the distributed server and this binary does not carry it: "+
				"run `wadjetd serve --mode=%s`. `wadjet serve` is the embedded server — pgwire over "+
				"the engine in this process, with no NATS task queues and no workers", m, m)
		default:
			return fmt.Errorf("unknown mode: %s (this binary runs the embedded server: "+
				"--mode=embedded, or standalone; --mode=coordinator and --mode=worker are wadjetd's)", m)
		}

		ApplyServeRuntimeEnvelope(logger)
		StartHeapDumper(ctx, logger)
		StartBackgroundGC(ctx, logger)

		store, err := OpenServeStore(logger)
		if err != nil {
			return err
		}

		if err := LoadGeoIP(logger); err != nil {
			return fmt.Errorf("loading GeoIP: %w", err)
		}
		defer geoip.Close()

		kv, releaseCatalog, err := openServerCatalogKV(logger)
		if err != nil {
			return err
		}
		defer releaseCatalog()

		cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
		if err := cat.Init(ctx); err != nil {
			return fmt.Errorf("initializing catalog: %w", err)
		}
		WireUDFPersistence(cat, logger)
		ConfigureEmbeddingProvider(logger)

		var provider *auth.Provider
		if configFile != "" {
			fileCfg, _, prov, wireErr := WireAuthFromConfig(ctx, configFile, logger)
			if wireErr != nil {
				return wireErr
			}
			provider = prov
			// A policy set that names a relation the catalog does not hold
			// refuses STARTUP, exactly as it does under `wadjetd serve`
			// (ADR-0033, #882).
			if err := provider.BindToCatalog(ctx, cat); err != nil {
				return fmt.Errorf("binding auth policies to the catalog: %w", err)
			}
			if fileCfg.Auth.MTLS.Enabled {
				logger.Warn("auth.mtls is configured but the embedded server has no HTTP or gRPC listener; " +
					"it applies to neither the PostgreSQL wire protocol (use --pg-tls-cert/--pg-tls-key) nor this process")
			}
		}

		// The cost guard reaches the only planner this server has (#803).
		globalLimits, roleLimits := EffectiveConfig().EffectiveQueryLimits()

		db, err := wadjet.Open(ctx, wadjet.Config{
			Store:               store,
			Bucket:              bucket,
			Logger:              logger,
			MetaKV:              kv,
			MemoryBudget:        memoryBudget,
			SpillDir:            spillDir,
			AuthProvider:        provider,
			SortMergeJoinBytes:  sortMergeJoinBytes,
			LateMaterialization: lateMaterialization,
			BushyJoinReorder:    bushyJoinReorder,
			EnableAlerts:        enableAlerts,
			QueryLimits:         globalLimits,
			RoleLimits:          roleLimits,
		})
		if err != nil {
			return fmt.Errorf("opening the embedded database: %w", err)
		}
		defer db.Close()

		pgQueryTimeout, _ := time.ParseDuration(queryTimeout)
		pgCfg := pgwire.Config{
			AuthProvider:     provider,
			QueryTimeout:     pgQueryTimeout,
			MaxConcurrentQry: maxConcurrentQry,
		}
		if pgTLSCert != "" && pgTLSKey != "" {
			cert, certErr := tls.LoadX509KeyPair(pgTLSCert, pgTLSKey)
			if certErr != nil {
				return fmt.Errorf("loading pgwire TLS cert: %w", certErr)
			}
			pgCfg.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}}
		}
		// No SetRouter: the embedded engine answers every statement.
		pgSrv := pgwire.NewServer(db, pgCfg, logger)

		logger.Info("embedded server ready", "pg_addr", pgAddr, "bucket", bucket, "cluster_id", clusterID)
		errCh := make(chan error, 1)
		go func() {
			if err := pgSrv.Start(pgAddr); err != nil {
				errCh <- err
			}
		}()

		select {
		case <-ctx.Done():
			logger.Info("shutting down...")
			pgSrv.Shutdown()
			return nil
		case err := <-errCh:
			return err
		}
	}
	return cmd
}

// openServerCatalogKV opens the catalog metadata store a server process
// holds for as long as it runs: the one a named --nats-url server already
// has, or an embedded NATS over the JetStream file store under the same
// advisory lock every other command takes (#842).
//
// It is deliberately not sharedCatalogKV, which the short-lived commands
// use: that one releases on SIGINT/SIGTERM by exiting the process, which for
// a server would take the wire listener down without draining it. Here the
// release is an ordinary defer and the signal is the shutdown path.
func openServerCatalogKV(logger *slog.Logger) (catalog.MetaKV, func(), error) {
	if effectiveResolution().Source("nats.url") != config.SourceDefault {
		// An operator who named a server means it: serve that catalog
		// rather than quietly running one of our own beside it.
		return dialCatalogKV(natsURL)
	}

	cfg := NATSServerConfig()
	lock, err := LockCatalogStoreDir(cfg.StoreDir)
	if err != nil {
		return nil, nil, fmt.Errorf("the catalog store directory %s is held by another wadjet process "+
			"(%w); stop it, or give this one its own --nats-store-dir", cfg.StoreDir, err)
	}
	embedded, err := natsconn.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		lock.Release()
		return nil, nil, fmt.Errorf("opening the catalog under %s: %w", cfg.StoreDir, err)
	}
	// Publish where this catalog can be reached, so a CLI command that loses
	// the lock race reaches THIS one rather than whatever answers a
	// well-known port.
	if err := lock.Publish(embedded.ClientURL()); err != nil {
		embedded.Shutdown()
		lock.Release()
		return nil, nil, fmt.Errorf("recording the catalog holder in %s: %w", CatalogLockPath(cfg.StoreDir), err)
	}
	nc, err := natsconn.ConnectInProcess(embedded.Server())
	if err != nil {
		embedded.Shutdown()
		lock.Release()
		return nil, nil, fmt.Errorf("connecting to the catalog: %w", err)
	}
	js, err := natsconn.NewJetStream(nc)
	if err != nil {
		nc.Close()
		embedded.Shutdown()
		lock.Release()
		return nil, nil, fmt.Errorf("creating JetStream: %w", err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		nc.Close()
		embedded.Shutdown()
		lock.Release()
		return nil, nil, fmt.Errorf("creating catalog KV: %w", err)
	}
	return kv, func() { nc.Close(); embedded.Shutdown(); lock.Release() }, nil
}
