package catalog

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

const (
	lockTTL        = 30 * time.Second
	lockPollInterval = 500 * time.Millisecond
	lockBucket     = "wadjet_catalog_locks"
)

// LockManager provides distributed read-write locks via NATS KV.
type LockManager struct {
	kv jetstream.KeyValue
}

// NewLockManager creates a lock manager backed by a NATS KV bucket.
func NewLockManager(js jetstream.JetStream) (*LockManager, error) {
	kv, err := js.CreateOrUpdateKeyValue(context.Background(), jetstream.KeyValueConfig{
		Bucket: lockBucket,
		TTL:    lockTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("creating lock KV bucket: %w", err)
	}
	return &LockManager{kv: kv}, nil
}

// Lock represents a held distributed lock.
type Lock struct {
	mgr  *LockManager
	key  string
	rev  uint64
	done chan struct{}
}

// AcquireWriteLock acquires an exclusive write lock on a table.
// Blocks until the lock is acquired or the context is cancelled.
func (lm *LockManager) AcquireWriteLock(ctx context.Context, space, table string) (*Lock, error) {
	key := fmt.Sprintf("write.%s.%s", space, table)
	return lm.acquire(ctx, key)
}

// AcquireReadLock acquires a shared read lock on a table.
// Multiple readers can hold locks concurrently. Each reader gets a unique key.
func (lm *LockManager) AcquireReadLock(ctx context.Context, space, table, readerID string) (*Lock, error) {
	key := fmt.Sprintf("read.%s.%s.%s", space, table, readerID)
	return lm.acquire(ctx, key)
}

func (lm *LockManager) acquire(ctx context.Context, key string) (*Lock, error) {
	for {
		rev, err := lm.kv.Create(ctx, key, []byte("locked"))
		if err == nil {
			lock := &Lock{
				mgr:  lm,
				key:  key,
				rev:  rev,
				done: make(chan struct{}),
			}
			go lock.refreshLoop()
			return lock, nil
		}

		// Key exists — watch for deletion/expiry instead of polling.
		// Falls back to polling if the watcher can't be created.
		if !lm.waitForRelease(ctx, key) {
			return nil, ctx.Err()
		}
	}
}

// waitForRelease blocks until the lock key is deleted or expires, using a
// NATS KV watcher for event-driven notification. Falls back to polling if
// the watcher cannot be created. Returns false only if ctx is cancelled.
func (lm *LockManager) waitForRelease(ctx context.Context, key string) bool {
	watcher, err := lm.kv.Watch(ctx, key)
	if err != nil {
		// Watcher unavailable — fall back to poll
		select {
		case <-ctx.Done():
			return false
		case <-time.After(lockPollInterval):
			return true
		}
	}
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case entry := <-watcher.Updates():
			if entry == nil {
				// Initial values done — check if key is already gone
				_, err := lm.kv.Get(ctx, key)
				if err != nil {
					return true // key gone, try to acquire
				}
				continue
			}
			op := entry.Operation()
			if op == jetstream.KeyValueDelete || op == jetstream.KeyValuePurge {
				return true // lock released or expired
			}
		case <-time.After(lockTTL + time.Second):
			// Safety timeout: if watcher misses TTL expiry, retry anyway
			return true
		}
	}
}

// Release releases the lock.
func (l *Lock) Release() error {
	close(l.done)
	return l.mgr.kv.Delete(context.Background(), l.key)
}

// refreshLoop periodically updates the lock value to prevent TTL expiry.
func (l *Lock) refreshLoop() {
	ticker := time.NewTicker(lockTTL / 3)
	defer ticker.Stop()

	for {
		select {
		case <-l.done:
			return
		case <-ticker.C:
			rev, err := l.mgr.kv.Update(context.Background(), l.key, []byte("locked"), l.rev)
			if err != nil {
				return // lock lost
			}
			l.rev = rev
		}
	}
}

// HasWriteLock checks if a write lock is currently held on a table.
func (lm *LockManager) HasWriteLock(ctx context.Context, space, table string) (bool, error) {
	key := fmt.Sprintf("write.%s.%s", space, table)
	_, err := lm.kv.Get(ctx, key)
	if err != nil {
		return false, nil
	}
	return true, nil
}
