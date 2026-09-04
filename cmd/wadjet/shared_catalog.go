package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

	// LOCK FIRST, and when the lock is held reach the HOLDER — never an
	// address.
	//
	// Round 1 dialed first, so with no --nats-url a `serve` on a DIFFERENT
	// --data-dir answered the machine's default address and the command read
	// another deployment's catalog. Round 2 locked first but still dialed
	// `127.0.0.1:<--nats-port>` when the lock was held, which is the same
	// mistake one step later: with a server on data dir A and a shell holding
	// B's catalog, `--data-dir=B tables` listed A's table and `--data-dir=B
	// create-table` WROTE INTO A's catalog (round-2 B1).
	//
	// A lock holder now records its own reachable client URL in the lock
	// file, and a second process dials THAT and nothing else. No fixed port,
	// no "whoever answers": the URL came from the process that holds this
	// exact catalog. A lock whose URL is missing or unreachable — a stale
	// lock, a holder that died — is refused, naming the file and the pid.
	lock, lockErr := lockCatalogStoreDir(cfg.StoreDir)
	if lockErr != nil {
		holder, readErr := awaitCatalogLockHolder(cfg.StoreDir)
		if readErr != nil {
			return nil, nil, fmt.Errorf("the catalog store directory %s is held by another process, "+
				"and it published no address to reach it at (%w).\n"+
				"If no wadjet process is running, %s is stale and can be removed; otherwise wait "+
				"for that command to finish, or use --nats-url to name a server directly",
				cfg.StoreDir, readErr, catalogLockPath(cfg.StoreDir))
		}
		kv, release, err := dialCatalogKV(holder.url)
		if err != nil {
			return nil, nil, fmt.Errorf("the catalog store directory %s is held by process %d, "+
				"which is not answering at the address it published, %s (%w).\n"+
				"If that process is gone, %s is stale and can be removed; otherwise wait for it "+
				"to finish, or use --nats-url to name a server directly",
				cfg.StoreDir, holder.pid, holder.url, err, catalogLockPath(cfg.StoreDir))
		}
		return kv, release, nil
	}
	unlock := lock.release
	embedded, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		unlock()
		return nil, nil, fmt.Errorf("opening the catalog under %s: %w", cfg.StoreDir, err)
	}
	// Published BEFORE this command does any work, so a second process that
	// loses the lock race has an address to reach as soon as there is one to
	// reach. The port is ephemeral, which is exactly why it has to be
	// written down: no other process could guess it.
	if err := lock.publish(embedded.ClientURL()); err != nil {
		embedded.Shutdown()
		unlock()
		return nil, nil, fmt.Errorf("recording the catalog holder in %s: %w",
			catalogLockPath(cfg.StoreDir), err)
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
func lockCatalogStoreDir(dir string) (*catalogLock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", dir, err)
	}
	path := catalogLockPath(dir)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("%s is locked: %w", path, err)
	}
	// Any address a DEAD holder left is a lie the moment we take the lock,
	// so clear it before anyone can read it as ours.
	if err := f.Truncate(0); err != nil {
		unix.Flock(int(f.Fd()), unix.LOCK_UN)
		f.Close()
		return nil, fmt.Errorf("clearing %s: %w", path, err)
	}
	return &catalogLock{f: f, path: path}, nil
}

// catalogLock is a held catalog-store lock, and the place its holder publishes
// the address other processes can reach it at.
type catalogLock struct {
	f    *os.File
	path string
}

// publish records this process's pid and the client URL of the catalog server
// it is running, so a process that loses the lock race can reach THE HOLDER
// rather than whatever answers a well-known port.
func (l *catalogLock) publish(url string) error {
	if _, err := l.f.WriteAt([]byte(fmt.Sprintf("%d\n%s\n", os.Getpid(), url)), 0); err != nil {
		return err
	}
	return l.f.Sync()
}

// release clears the published address and drops the lock.
//
// It TRUNCATES rather than unlinks. Removing the file would break the flock
// rendezvous itself: a third process would create a fresh inode, flock that,
// and believe it held a lock the survivor also holds. An emptied file says
// "nobody is publishing an address here", which is what a reader needs, and a
// process killed outright leaves a stale address instead — which is why a
// reader must dial before trusting it.
func (l *catalogLock) release() {
	l.f.Truncate(0)
	unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	l.f.Close()
}

func catalogLockPath(dir string) string { return filepath.Join(dir, "wadjet.lock") }

// catalogLockHolder is what a lock file says about the process holding it.
type catalogLockHolder struct {
	pid int
	url string
}

// awaitCatalogLockHolder reads the holder's published address, waiting briefly
// for it to appear.
//
// The wait is not padding. A holder takes the flock and only then starts its
// server and learns the ephemeral port it must publish, so a process that
// loses the race by microseconds would otherwise read an empty file and refuse
// a catalog that is about to be perfectly reachable. Bounded, because a file
// that stays empty means the holder is not publishing one at all — a stale
// lock, or a process killed between the flock and the write — and that is a
// refusal, not something to wait out.
func awaitCatalogLockHolder(dir string) (catalogLockHolder, error) {
	path := catalogLockPath(dir)
	deadline := time.Now().Add(5 * time.Second)
	for {
		data, err := os.ReadFile(path)
		if err == nil {
			lines := strings.SplitN(strings.TrimSpace(string(data)), "\n", 2)
			if len(lines) == 2 && strings.TrimSpace(lines[1]) != "" {
				pid, _ := strconv.Atoi(strings.TrimSpace(lines[0]))
				return catalogLockHolder{pid: pid, url: strings.TrimSpace(lines[1])}, nil
			}
		}
		if time.Now().After(deadline) {
			return catalogLockHolder{}, fmt.Errorf("%s names no reachable address", path)
		}
		time.Sleep(50 * time.Millisecond)
	}
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
