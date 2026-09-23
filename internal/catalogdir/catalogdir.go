// SPDX-License-Identifier: MIT

// Package catalogdir opens a catalog DIRECTORY: the JetStream file store a
// `wadjet serve`, a `wadjet` command and an embedded program keep their
// table metadata in, under one advisory lock.
//
// It is the ONE mechanism behind `--storage-type=file --data-dir=D` (the
// catalog lives at `D/_catalog`, #842) and behind `wadjet.Config.DataDir`
// (#1255). The three doors share it rather than each running a JetStream
// store of their own, because a catalog two doors open differently is two
// catalogs: a table one of them created that the other cannot see, over the
// SAME Parquet files.
//
// A directory is held by exactly one process at a time. nats-server does
// not lock its store directory, so two processes can open one JetStream file
// store and write over each other's metadata; the flock here is what stops
// that, and it binds every door because every door takes it. The holder
// publishes its pid and the URL its embedded server answers on INTO the lock
// file, so a short-lived command that loses the race can reach the holder's
// catalog instead of whatever answers a well-known port. See LICENSING.md:
// this package is MIT, as is everything it imports.
package catalogdir

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/nats-io/nats.go"
	"golang.org/x/sys/unix"

	"github.com/derekmwright/wadjet/internal/natsconn"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// ErrHeld is the refusal a second opener of a held directory gets — and
// ONLY that: a lock another holder has, another DB in this process or
// another process (flock binds per open file description). A directory that cannot be
// created, opened or written (permissions, a regular file where the
// directory should be) is reported with its own cause, so
// errors.Is(err, os.ErrPermission) still answers (arc EC review P1). The
// error that wraps it names the directory and, when the holder has
// published, its pid.
var ErrHeld = errors.New("catalog directory is held by another process")

// HeldError is the refusal `wadjet.Open` and `wadjet serve` raise for a held
// directory: ErrHeld, the directory, the holder's pid when it has published,
// and the lock error (arc EC review B4). The short-lived CLI commands dial
// the holder instead, and refuse with their own message when that fails.
func HeldError(dir string, lockErr error) error {
	if holder, ok := ReadHolder(dir); ok {
		return fmt.Errorf("%w: %s is held by process %d (%w)", ErrHeld, dir, holder.PID, lockErr)
	}
	return fmt.Errorf("%w: %s (%w)", ErrHeld, dir, lockErr)
}

// Handle is an opened catalog directory: the KV the catalog reads and
// writes, and everything that has to be released with it.
type Handle struct {
	KV   catalog.MetaKV
	lock *Lock
	nc   *nats.Conn
	srv  *natsconn.EmbeddedNATS
}

// ClientURL is the address the embedded server answers on — the one the
// lock file publishes.
func (h *Handle) ClientURL() string { return h.srv.ClientURL() }

// Close releases the connection, stops the server and drops the lock. It is
// safe to call more than once.
func (h *Handle) Close() {
	if h.nc != nil {
		h.nc.Close()
		h.nc = nil
	}
	if h.srv != nil {
		h.srv.Shutdown()
		h.srv = nil
	}
	if h.lock != nil {
		h.lock.Release()
		h.lock = nil
	}
}

// Open takes the directory's lock and opens the catalog store in it: an
// embedded nats-server with JetStream over cfg.StoreDir, on cfg.Port (-1
// for an ephemeral port), the holder published, an in-process connection
// and the catalog KV bucket.
//
// A held directory is refused with ErrHeld — Open never dials the holder.
// A short-lived CLI command dials (internal/cli.sharedCatalogKV, with its
// own retry loop over TakeLock and ReadHolder), because it is gone in a second;
// a program that dialed would lose its catalog the moment the holder
// exited, which is a worse contract than a refusal at Open.
func Open(cfg natsconn.NATSConfig, logger *slog.Logger) (*Handle, error) {
	lock, err := TakeLock(cfg.StoreDir)
	if err != nil {
		if errors.Is(err, ErrHeld) {
			return nil, HeldError(cfg.StoreDir, err)
		}
		return nil, fmt.Errorf("opening the catalog directory %s: %w", cfg.StoreDir, err)
	}
	return OpenLocked(lock, cfg, logger)
}

// OpenLocked opens the catalog store under a lock the caller already holds,
// and owns the lock from here on: it is released on every error path and
// by Handle.Close.
//
// The holder's address is published BEFORE the caller does any work, so a
// process that loses the lock race has an address to reach as soon as
// there is one. The port is ephemeral when cfg.Port is -1, which is exactly
// why it has to be written down: no other process could guess it.
func OpenLocked(lock *Lock, cfg natsconn.NATSConfig, logger *slog.Logger) (*Handle, error) {
	srv, err := natsconn.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		lock.Release()
		return nil, fmt.Errorf("opening the catalog under %s: %w", cfg.StoreDir, err)
	}
	if err := lock.Publish(srv.ClientURL()); err != nil {
		srv.Shutdown()
		lock.Release()
		return nil, fmt.Errorf("recording the catalog holder in %s: %w", LockPath(cfg.StoreDir), err)
	}
	nc, err := natsconn.ConnectInProcess(srv.Server())
	if err != nil {
		srv.Shutdown()
		lock.Release()
		return nil, fmt.Errorf("connecting to the catalog under %s: %w", cfg.StoreDir, err)
	}
	js, err := natsconn.NewJetStream(nc)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		lock.Release()
		return nil, fmt.Errorf("creating JetStream under %s: %w", cfg.StoreDir, err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		nc.Close()
		srv.Shutdown()
		lock.Release()
		return nil, fmt.Errorf("opening the catalog under %s: %w", cfg.StoreDir, err)
	}
	return &Handle{KV: kv, lock: lock, nc: nc, srv: srv}, nil
}

// TakeLock takes an exclusive, non-blocking advisory lock on the catalog store
// directory, creating it if needed. A lock another holder has is ErrHeld;
// any other failure is the file error itself.
//
// It is advisory (flock), so it binds only wadjet processes, and it is
// taken by every door — a lock one of them skipped would be no lock at all.
// The kernel drops it when the holder dies, so a lock file left behind by a
// killed process is not a lock.
func TakeLock(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating %s: %w", dir, err)
	}
	path := LockPath(dir)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s is locked", ErrHeld, path)
		}
		return nil, fmt.Errorf("locking %s: %w", path, err)
	}
	// Any address a DEAD holder left is a lie the moment we take the lock,
	// so clear it before anyone can read it as ours.
	if err := f.Truncate(0); err != nil {
		unix.Flock(int(f.Fd()), unix.LOCK_UN)
		f.Close()
		return nil, fmt.Errorf("clearing %s: %w", path, err)
	}
	return &Lock{f: f, path: path}, nil
}

// Lock is a held catalog-store lock, and the place its holder publishes the
// address other processes can reach it at.
type Lock struct {
	f    *os.File
	path string
}

// Publish records this process's pid and the client URL of the catalog
// server it is running.
func (l *Lock) Publish(url string) error {
	if _, err := l.f.WriteAt([]byte(fmt.Sprintf("%d\n%s\n", os.Getpid(), url)), 0); err != nil {
		return err
	}
	return l.f.Sync()
}

// Release clears the published address and drops the lock.
//
// It TRUNCATES rather than unlinks. Removing the file would break the
// flock rendezvous itself: a third process would create a fresh inode,
// flock that, and believe it held a lock the survivor also holds. An
// emptied file says "nobody is publishing an address here", which is what
// a reader needs; a process killed outright leaves a stale address instead,
// which is why a reader dials before trusting it.
func (l *Lock) Release() {
	l.f.Truncate(0)
	unix.Flock(int(l.f.Fd()), unix.LOCK_UN)
	l.f.Close()
}

// LockPath is the lock file inside a catalog store directory.
func LockPath(dir string) string { return filepath.Join(dir, "wadjet.lock") }

// Holder is what a lock file says about the process holding it.
type Holder struct {
	PID int
	URL string
}

// ReadHolder reads the address a holder published, if it has. ok=false
// means the file is absent, empty or half-written — all of which say "no
// address to dial right now".
func ReadHolder(dir string) (Holder, bool) {
	data, err := os.ReadFile(LockPath(dir))
	if err != nil {
		return Holder{}, false
	}
	lines := strings.SplitN(strings.TrimSpace(string(data)), "\n", 2)
	if len(lines) != 2 || strings.TrimSpace(lines[1]) == "" {
		return Holder{}, false
	}
	pid, _ := strconv.Atoi(strings.TrimSpace(lines[0]))
	return Holder{PID: pid, URL: strings.TrimSpace(lines[1])}, true
}
