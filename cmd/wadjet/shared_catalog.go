package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"

	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/wadjet"
)

// sharedCatalogKV opens the ONE catalog every wadjet command shares with
// `serve`, and returns it with the function that releases it.
//
// #842: `tables` built its catalog with catalog.NewWithStore and never called
// Init, so it failed with "reading catalog meta: key not found" on every
// invocation — and calling Init would not have helped, because NewWithStore's
// KV is a fresh in-memory map despite the name. `query`, `create-table`,
// `drop-table` and `shell` had the same map by another route (wadjet.Open
// with no Config.MetaKV), so a table one invocation created was invisible to
// the next and to a running server, while its parquet files accumulated under
// the data directory.
//
// Catalog metadata lives in a MetaKV, and there are exactly two
// implementations: NATS JetStream KV and the in-memory map. There is no
// object-store-backed one — Catalog.Init touches the store only to MakeBucket,
// and every metadata read and write goes through c.kv — so "persist the
// catalog beside the data files" is not a thing this engine can do today. The
// place the metadata actually lives is JetStream, which is what
// `serve --mode=standalone` runs embedded.
//
// So the commands go there, and WHICH catalog is decided before who is
// listening:
//
//  1. The operator NAMED a server (--nats-url or --nats-port at any tier):
//     dial it, and report a failure rather than quietly opening a catalog of
//     our own beside it. This is also the only route to a
//     `--mode=coordinator` deployment, which runs no embedded server.
//  2. Otherwise the catalog is this deployment's own directory
//     (natsServerConfig's StoreDir — under --data-dir for a file-backed
//     deployment, see derivedCatalogStoreDir; ~/.wadjet/nats for S3). Take
//     its lock. Holding it means nobody else has this catalog, so run an
//     embedded JetStream server over it for the lifetime of the command: the
//     metadata lands on disk, and the next invocation — or a `serve` started
//     afterwards — reads it back. The port is ephemeral and the connection is
//     in-process, so nothing binds 4222.
//  3. The lock is held: a wadjet process already has THIS catalog, so dial to
//     reach it. Failing that, refuse, naming the directory, the holder and
//     the flags that resolve it.
//
// Step 2 before step 3 is load-bearing. Dialing first asked "who is listening
// on the default address", which a `serve` on a DIFFERENT --data-dir answers
// just as readily as the right one (round-1 B3).
func sharedCatalogKV(ctx context.Context, logger *slog.Logger) (catalog.MetaKV, func(), error) {
	natsAddr := natsURL
	if natsAddr == "" {
		natsAddr = fmt.Sprintf("nats://127.0.0.1:%d", natsPort)
	}
	// --nats-url NAMES a server; --nats-port does not — its flag help is
	// "Embedded NATS port", so it sets where a server of ours would listen
	// and, below, where to look for the holder of a catalog we could not
	// lock. Only the URL means "use that one".
	if effectiveResolution().Source("nats.url") != config.SourceDefault {
		// An operator who named a server means it: dial, and report the
		// failure rather than quietly running a catalog of our own beside
		// it. This is also the only way to reach a `--mode=coordinator`
		// deployment, which runs no embedded server and locks no directory.
		kv, release, err := dialCatalogKV(natsAddr)
		if err != nil {
			return nil, nil, err
		}
		return kv, release, nil
	}

	cfg := natsServerConfig()
	// Ephemeral: nothing outside this process connects to it.
	cfg.Port = -1

	// LOCK FIRST, dial second, and only when THIS catalog is already held.
	//
	// Dialing first asked the wrong question. With no --nats-url the address
	// is the machine's default, so a `serve` on a DIFFERENT --data-dir — or
	// any other cluster on the box — answered it, and the command read a
	// catalog belonging to other data (round-1 B3). Taking the lock names the
	// exact catalog this command's --data-dir owns; a failure to take it
	// means a wadjet process already holds THAT catalog, and dialing is then
	// the way to reach the holder rather than a guess about who answers.
	unlock, lockErr := lockCatalogStoreDir(cfg.StoreDir)
	if lockErr != nil {
		kv, release, err := dialCatalogKV(natsAddr)
		if err != nil {
			return nil, nil, fmt.Errorf("the catalog store directory %s is held by another process "+
				"(%w) and no wadjet server answered at %s (%v).\n"+
				"That is a `wadjet serve` on a different address, or another wadjet command "+
				"running right now. Point this one at that server with --nats-url (or "+
				"--nats-port), or wait for the other command to finish",
				cfg.StoreDir, lockErr, natsAddr, err)
		}
		return kv, release, nil
	}
	embedded, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		unlock()
		return nil, nil, fmt.Errorf("opening the catalog under %s: %w", cfg.StoreDir, err)
	}
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		embedded.Shutdown()
		unlock()
		return nil, nil, fmt.Errorf("connecting to the embedded catalog server: %w", err)
	}
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		nc.Close()
		embedded.Shutdown()
		unlock()
		return nil, nil, fmt.Errorf("creating JetStream: %w", err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		nc.Close()
		embedded.Shutdown()
		unlock()
		return nil, nil, fmt.Errorf("opening the catalog under %s: %w", cfg.StoreDir, err)
	}
	return kv, func() { nc.Close(); embedded.Shutdown(); unlock() }, nil
}

// dialCatalogKV opens the catalog of a running server, and returns it with the
// function that closes the connection.
//
// The dial is short and non-retrying: distributed.Connect's reconnect options
// would make a miss cost seconds on every command run without a server.
func dialCatalogKV(natsAddr string) (catalog.MetaKV, func(), error) {
	nc, err := nats.Connect(natsAddr, nats.Timeout(2*time.Second), nats.MaxReconnects(0))
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to the catalog server at %s: %w", natsAddr, err)
	}
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("creating JetStream on %s: %w", natsAddr, err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("opening the catalog on %s: %w", natsAddr, err)
	}
	return kv, nc.Close, nil
}

// lockCatalogStoreDir takes an exclusive, non-blocking advisory lock on the
// JetStream store directory, and returns the function that drops it.
//
// Nothing else does. nats-server does not lock its store directory, so two
// processes can open the same JetStream file store and write over each other's
// metadata — which is a thing this engine could not reach before #842, when
// only `serve` ran an embedded server, and which the CLI's embedded fallback
// makes reachable the moment two commands run at once. A refusal naming the
// other process is the answer; corrupting a catalog silently is not.
//
// It is advisory (flock), so it binds only wadjet processes, and it is taken
// by BOTH the CLI fallback and `serve` — a lock one of the two skipped would
// be no lock at all.
func lockCatalogStoreDir(dir string) (func(), error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", dir, err)
	}
	path := filepath.Join(dir, "wadjet.lock")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s is locked: %w", path, err)
	}
	return func() {
		unix.Flock(int(f.Fd()), unix.LOCK_UN)
		f.Close()
	}, nil
}

// sharedCatalog is sharedCatalogKV with the Catalog built and INITIALIZED over
// the given object store. Init is the half `tables` was missing.
func sharedCatalog(ctx context.Context, store objstore.Store, logger *slog.Logger) (*catalog.Catalog, func(), error) {
	kv, release, err := sharedCatalogKV(ctx, logger)
	if err != nil {
		return nil, nil, err
	}
	cat := catalog.NewWithCluster(kv, store, bucket, clusterID)
	if err := cat.Init(ctx); err != nil {
		release()
		return nil, nil, fmt.Errorf("initializing catalog: %w", err)
	}
	return cat, release, nil
}

// openSharedDB opens an embedded engine over the shared catalog and the
// object store the storage flags name — the pair `query`, `create-table`,
// `drop-table` and `shell` run statements against, so that a table one of
// them creates is a table the next one, and a `serve`, can read.
//
// It is wadjet.Open WITH Config.MetaKV, which is the whole difference from
// what those commands did before: without it Open falls back to
// catalog.NewWithStore's in-memory map and every invocation starts from an
// empty catalog while its data files pile up in the store (#842).
func openSharedDB(ctx context.Context, logger *slog.Logger) (*wadjet.DB, func(), error) {
	store, err := newStore()
	if err != nil {
		return nil, nil, fmt.Errorf("opening object store: %w", err)
	}
	kv, release, err := sharedCatalogKV(ctx, logger)
	if err != nil {
		return nil, nil, err
	}
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:  store,
		Bucket: bucket,
		MetaKV: kv,
		Logger: logger,
	})
	if err != nil {
		release()
		return nil, nil, err
	}
	return db, func() { db.Close(); release() }, nil
}
