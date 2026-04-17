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
