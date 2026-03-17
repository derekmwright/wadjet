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
			// Start TTL refresh loop
			go lock.refreshLoop()
			return lock, nil
		}

		// Key already exists — wait and retry
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lockPollInterval):
			// Check if lock expired (TTL handles this automatically in NATS KV)
			continue
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
