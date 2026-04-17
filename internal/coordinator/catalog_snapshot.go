package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
)

// SetCatalogSnapshotOptions configures the S3 target for catalog snapshots.
// Call once during coordinator setup. If Store is nil, snapshot/restore
// functionality is disabled.
func (c *Coordinator) SetCatalogSnapshotOptions(opts catalog.SnapshotOptions) {
	c.catalogSnapshotOpts = opts
}

// SetCatalogSnapshotInterval configures the periodic snapshot cadence.
// Zero disables periodic snapshots (explicit-only).
func (c *Coordinator) SetCatalogSnapshotInterval(d time.Duration) {
	c.catalogSnapshotInterval = d
}

// MaybeRestoreCatalog restores the catalog from S3 if:
//   - Snapshot options are configured (Store != nil)
//   - Either forceTS is set, or the KV has no <cluster>.meta key (empty)
//
// Otherwise it is a no-op. Errors during restore are propagated unchanged;
// the caller decides whether to fatal.
func (c *Coordinator) MaybeRestoreCatalog(ctx context.Context, forceTS string) error {
	if c.catalogSnapshotOpts.Store == nil {
		return nil
	}
	if forceTS == "" {
		empty, err := c.catalog.IsKVEmpty(ctx)
		if err != nil {
			return fmt.Errorf("checking KV empty: %w", err)
		}
		if !empty {
			// Non-empty: skip restore.
			return nil
		}
	}
	ts, err := c.catalog.Restore(ctx, catalog.RestoreOptions{
		SnapshotOptions: c.catalogSnapshotOpts,
		ForceTS:         forceTS,
	})
	if err != nil {
		return err
	}
	_ = ts // caller can log if desired
	return nil
}

// StartCatalogSnapshotLoop begins periodic catalog snapshots. Safe to call
// only while this coordinator holds leadership. No-op when snapshot options
// are not configured or the interval is zero.
func (c *Coordinator) StartCatalogSnapshotLoop(parent context.Context) {
	if c.catalogSnapshotOpts.Store == nil || c.catalogSnapshotInterval <= 0 {
		return
	}
	c.StopCatalogSnapshotLoop()
	ctx, cancel := context.WithCancel(parent)
	c.catalogSnapshotCancel = cancel
	go c.runCatalogSnapshotLoop(ctx)
}

// StopCatalogSnapshotLoop cancels the running loop and waits for it to exit.
// Safe to call when no loop is running.
func (c *Coordinator) StopCatalogSnapshotLoop() {
	if c.catalogSnapshotCancel != nil {
		c.catalogSnapshotCancel()
		c.catalogSnapshotCancel = nil
	}
}

func (c *Coordinator) runCatalogSnapshotLoop(ctx context.Context) {
	t := time.NewTicker(c.catalogSnapshotInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := c.catalog.Snapshot(ctx, c.catalogSnapshotOpts); err != nil {
				// Best-effort: log via stdlib. Don't die on transient S3 errors.
				fmt.Printf("catalog snapshot tick error: %v\n", err)
				continue
			}
			// GC retention: keep 10 newest + anything <24h.
			if err := c.catalog.GCSnapshots(ctx, c.catalogSnapshotOpts, 10, 24*time.Hour); err != nil {
				fmt.Printf("catalog GC error: %v\n", err)
			}
		}
	}
}
