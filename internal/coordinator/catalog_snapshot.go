package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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
//   - Either forceTS is set, or the catalog is fresh: no KV at all, or a KV
//     that holds no tables
//
// Otherwise it is a no-op. Errors during restore are propagated unchanged;
// the caller decides whether to fatal.
//
// Freshness cannot be IsKVEmpty alone: every production caller runs
// Catalog.Init first, and Init writes the <cluster>.meta key, so the KV is
// never empty by the time this runs. Gating on it made the whole path dead
// code — `wadjet serve --catalog-snapshot-s3-prefix=...` against a bucket
// full of data silently restored nothing and came up with an empty table
// list. Restore overwrites meta with the snapshot's own copy, so the
// Init-written stub is fine to clobber. (cmd/tpch-bench reached the same
// conclusion and worked around it locally; this is that fix, upstream.)
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
			tables, err := c.catalog.ListTables(ctx)
			if err != nil {
				return fmt.Errorf("listing tables: %w", err)
			}
			if len(tables) > 0 {
				// This catalog already has contents of its own: leave them.
				return nil
			}
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
	c.catalogSnapshotWG.Add(1)
	go func() {
		defer c.catalogSnapshotWG.Done()
		c.runCatalogSnapshotLoop(ctx)
	}()
}

// StopCatalogSnapshotLoop cancels the running loop and waits for it to exit.
// Safe to call when no loop is running.
func (c *Coordinator) StopCatalogSnapshotLoop() {
	if c.catalogSnapshotCancel != nil {
		c.catalogSnapshotCancel()
		c.catalogSnapshotCancel = nil
	}
	c.catalogSnapshotWG.Wait()
}

func (c *Coordinator) runCatalogSnapshotLoop(ctx context.Context) {
	lg := c.logger
	if lg == nil {
		lg = slog.Default()
	}
	t := time.NewTicker(c.catalogSnapshotInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := c.catalog.Snapshot(ctx, c.catalogSnapshotOpts); err != nil {
				// Best-effort: don't die on transient S3 errors.
				lg.Warn("catalog snapshot tick error", "err", err)
				continue
			}
			// GC retention: keep 10 newest + anything <24h.
			if err := c.catalog.GCSnapshots(ctx, c.catalogSnapshotOpts, 10, 24*time.Hour); err != nil {
				lg.Warn("catalog snapshot GC error", "err", err)
			}
		}
	}
}

// handleCreateSnapshotSQL runs an explicit catalog snapshot and returns a
// 1-row 3-column SQLResult with {snapshot_id, prefix, key_count}.
func (c *Coordinator) handleCreateSnapshotSQL(ctx context.Context) (*SQLResult, error) {
	if c.catalogSnapshotOpts.Store == nil {
		return nil, fmt.Errorf("catalog snapshots are not configured; set --catalog-snapshot-s3-prefix on the coordinator")
	}
	ts, err := c.catalog.Snapshot(ctx, c.catalogSnapshotOpts)
	if err != nil {
		return nil, err
	}
	keyCount := int64(0)
	if mf, err := readSnapshotManifest(ctx, c.catalogSnapshotOpts, ts); err == nil {
		keyCount = int64(mf.KeyCount)
	}
	return newCreateSnapshotResult(ts, c.catalogSnapshotOpts.Prefix, keyCount), nil
}

// readSnapshotManifest fetches the manifest.json for a completed snapshot.
func readSnapshotManifest(ctx context.Context, opts catalog.SnapshotOptions, ts string) (*catalog.SnapshotManifest, error) {
	prefix := opts.Prefix
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	r, _, err := opts.Store.Get(ctx, opts.Bucket, prefix+"snapshots/"+ts+"/manifest.json")
	if err != nil {
		return nil, err
	}
	defer r.Close()
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	var mf catalog.SnapshotManifest
	if err := json.Unmarshal(body, &mf); err != nil {
		return nil, err
	}
	return &mf, nil
}

// snapshotResultSchema is the 3-column schema for CREATE SNAPSHOT results.
var snapshotResultSchema = []parquet.Column{
	{Name: "snapshot_id", Type: parquet.TypeString},
	{Name: "prefix", Type: parquet.TypeString},
	{Name: "key_count", Type: parquet.TypeInt64},
}

// newCreateSnapshotResult builds a 1-row SQLResult for a completed snapshot.
func newCreateSnapshotResult(snapshotID, prefix string, keyCount int64) *SQLResult {
	b := batch.FromRows(snapshotResultSchema, []map[string]any{
		{"snapshot_id": snapshotID, "prefix": prefix, "key_count": keyCount},
	})
	return &SQLResult{
		Columns:   []string{"snapshot_id", "prefix", "key_count"},
		Batches:   []*batch.RecordBatch{b},
		TotalRows: 1,
	}
}
