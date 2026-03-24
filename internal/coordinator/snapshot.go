package coordinator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

// SnapshotConfig controls catalog snapshot behavior.
type SnapshotConfig struct {
	// Enabled controls whether periodic snapshots run. Default: true.
	Enabled bool
	// Interval between snapshots. Zero uses DefaultSnapshotInterval.
	Interval time.Duration
	// Retention is the max number of snapshots to keep. Zero uses DefaultSnapshotRetention.
	Retention int
	// Prefix is the S3 key prefix for snapshot files. Zero uses DefaultSnapshotPrefix.
	Prefix string
	// Debounce is the minimum time between snapshots triggered by catalog mutations.
	// Zero uses DefaultSnapshotDebounce.
	Debounce time.Duration
	// LeaderOnly restricts snapshots to the elected leader in distributed mode.
	// Default: true. In standalone mode this has no effect.
	LeaderOnly bool
}

const (
	DefaultSnapshotInterval  = 5 * time.Minute
	DefaultSnapshotRetention = 48
	DefaultSnapshotPrefix    = "_catalog/snapshots"
	DefaultSnapshotDebounce  = 30 * time.Second
)

// CatalogSnapshot is a point-in-time dump of all catalog KV entries.
type CatalogSnapshot struct {
	Version   int                    `json:"version"`
	ClusterID string                 `json:"cluster_id"`
	Timestamp time.Time              `json:"timestamp"`
	Entries   []CatalogSnapshotEntry `json:"entries"`
}

// CatalogSnapshotEntry is a single KV pair in the snapshot.
type CatalogSnapshotEntry struct {
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

// CatalogSnapshotter manages periodic catalog metadata snapshots to object storage.
type CatalogSnapshotter struct {
	catalog   *catalog.Catalog
	store     objstore.Store
	bucket    string
	leader    *LeaderElection
	config    SnapshotConfig
	logger    *slog.Logger

	mu       sync.Mutex
	notifyCh chan struct{} // debounce trigger from catalog mutations
	lastSnap time.Time
}

// NewCatalogSnapshotter creates a snapshotter with the given configuration.
func NewCatalogSnapshotter(cat *catalog.Catalog, store objstore.Store, bucket string, cfg SnapshotConfig, logger *slog.Logger) *CatalogSnapshotter {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultSnapshotInterval
	}
	if cfg.Retention == 0 {
		cfg.Retention = DefaultSnapshotRetention
	}
	if cfg.Prefix == "" {
		cfg.Prefix = DefaultSnapshotPrefix
	}
	if cfg.Debounce == 0 {
		cfg.Debounce = DefaultSnapshotDebounce
	}
	return &CatalogSnapshotter{
		catalog:  cat,
		store:    store,
		bucket:   bucket,
		config:   cfg,
		logger:   logger,
		notifyCh: make(chan struct{}, 1),
	}
}

// SetLeaderElection attaches leader election so snapshots only run on the leader.
func (cs *CatalogSnapshotter) SetLeaderElection(le *LeaderElection) {
	cs.leader = le
}

// Notify signals that a catalog mutation occurred. The snapshotter will take a
// snapshot within the debounce window. Safe to call from any goroutine.
func (cs *CatalogSnapshotter) Notify() {
	select {
	case cs.notifyCh <- struct{}{}:
	default: // already pending
	}
}

// Start launches the background snapshot loop. It listens for both periodic
// ticks and debounced mutation notifications.
func (cs *CatalogSnapshotter) Start(ctx context.Context) {
	if !cs.config.Enabled {
		cs.logger.Info("catalog snapshots disabled")
		return
	}

	go cs.run(ctx)
	cs.logger.Info("catalog snapshotter started",
		"interval", cs.config.Interval,
		"retention", cs.config.Retention,
		"prefix", cs.config.Prefix,
		"debounce", cs.config.Debounce,
	)
}

func (cs *CatalogSnapshotter) run(ctx context.Context) {
	ticker := time.NewTicker(cs.config.Interval)
	defer ticker.Stop()

	// Take an initial snapshot on startup.
	cs.trySnapshot(ctx, "startup")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cs.trySnapshot(ctx, "periodic")
		case <-cs.notifyCh:
			cs.tryDebouncedSnapshot(ctx)
		}
	}
}

// trySnapshot takes a snapshot if this node should be snapshotting.
func (cs *CatalogSnapshotter) trySnapshot(ctx context.Context, trigger string) {
	if cs.config.LeaderOnly && cs.leader != nil && !cs.leader.IsLeader() {
		return
	}

	snap, err := cs.TakeSnapshot(ctx)
	if err != nil {
		cs.logger.Error("catalog snapshot failed", "trigger", trigger, "error", err)
		return
	}

	cs.mu.Lock()
	cs.lastSnap = snap.Timestamp
	cs.mu.Unlock()

	cs.logger.Info("catalog snapshot saved",
		"trigger", trigger,
		"entries", len(snap.Entries),
		"timestamp", snap.Timestamp.Format(time.RFC3339),
	)

	// Best-effort prune.
	if err := cs.Prune(ctx); err != nil {
		cs.logger.Warn("snapshot prune failed", "error", err)
	}
}

// tryDebouncedSnapshot takes a snapshot only if enough time has passed since the last one.
func (cs *CatalogSnapshotter) tryDebouncedSnapshot(ctx context.Context) {
	cs.mu.Lock()
	elapsed := time.Since(cs.lastSnap)
	cs.mu.Unlock()

	if elapsed < cs.config.Debounce {
		return
	}
	cs.trySnapshot(ctx, "mutation")
}

// TakeSnapshot reads all catalog KV entries and writes them to object storage.
func (cs *CatalogSnapshotter) TakeSnapshot(ctx context.Context) (*CatalogSnapshot, error) {
	kv := cs.catalog.KV()
	clusterID := cs.catalog.ClusterID()

	keys, err := kv.List(clusterID + ".")
	if err != nil {
		return nil, fmt.Errorf("listing catalog keys: %w", err)
	}

	entries := make([]CatalogSnapshotEntry, 0, len(keys))
	for _, key := range keys {
		val, _, err := kv.Get(key)
		if err != nil {
			cs.logger.Warn("skipping key in snapshot", "key", key, "error", err)
			continue
		}
		entries = append(entries, CatalogSnapshotEntry{
			Key:   key,
			Value: json.RawMessage(val),
		})
	}

	snap := &CatalogSnapshot{
		Version:   1,
		ClusterID: clusterID,
		Timestamp: time.Now().UTC(),
		Entries:   entries,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return nil, fmt.Errorf("marshaling snapshot: %w", err)
	}

	key := fmt.Sprintf("%s/%s.json", cs.config.Prefix, snap.Timestamp.Format("20060102T150405.000Z"))
	if _, err := cs.store.Put(ctx, cs.bucket, key, bytes.NewReader(data), int64(len(data)), "application/json"); err != nil {
		return nil, fmt.Errorf("writing snapshot to %s: %w", key, err)
	}

	return snap, nil
}

// ListSnapshots returns available snapshots ordered newest-first.
func (cs *CatalogSnapshotter) ListSnapshots(ctx context.Context) ([]objstore.ObjectInfo, error) {
	objects, err := cs.store.List(ctx, cs.bucket, objstore.ListOptions{
		Prefix: cs.config.Prefix + "/",
	})
	if err != nil {
		return nil, fmt.Errorf("listing snapshots: %w", err)
	}

	// Sort newest first by key (timestamps sort lexicographically).
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key > objects[j].Key
	})
	return objects, nil
}

// LoadSnapshot reads and parses a snapshot from object storage.
func (cs *CatalogSnapshotter) LoadSnapshot(ctx context.Context, key string) (*CatalogSnapshot, error) {
	rc, _, err := cs.store.Get(ctx, cs.bucket, key)
	if err != nil {
		return nil, fmt.Errorf("reading snapshot %s: %w", key, err)
	}
	defer rc.Close()

	var snap CatalogSnapshot
	if err := json.NewDecoder(rc).Decode(&snap); err != nil {
		return nil, fmt.Errorf("decoding snapshot: %w", err)
	}
	return &snap, nil
}

// Restore replays a snapshot into the catalog KV, replacing all existing entries.
func (cs *CatalogSnapshotter) Restore(ctx context.Context, snap *CatalogSnapshot) error {
	kv := cs.catalog.KV()
	clusterID := cs.catalog.ClusterID()

	// Delete existing keys for this cluster.
	existing, err := kv.List(clusterID + ".")
	if err != nil {
		return fmt.Errorf("listing existing keys: %w", err)
	}
	for _, key := range existing {
		_ = kv.Delete(key)
	}

	// Write snapshot entries.
	var errs []string
	for _, entry := range snap.Entries {
		if _, err := kv.Put(entry.Key, entry.Value); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", entry.Key, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("restore had %d errors: %s", len(errs), strings.Join(errs, "; "))
	}
	return nil
}

// RestoreLatest loads the most recent snapshot and restores it.
func (cs *CatalogSnapshotter) RestoreLatest(ctx context.Context) (*CatalogSnapshot, error) {
	snapshots, err := cs.ListSnapshots(ctx)
	if err != nil {
		return nil, err
	}
	if len(snapshots) == 0 {
		return nil, fmt.Errorf("no snapshots found under %s/", cs.config.Prefix)
	}

	snap, err := cs.LoadSnapshot(ctx, snapshots[0].Key)
	if err != nil {
		return nil, err
	}
	if err := cs.Restore(ctx, snap); err != nil {
		return nil, err
	}
	return snap, nil
}

// Prune removes old snapshots beyond the retention limit.
func (cs *CatalogSnapshotter) Prune(ctx context.Context) error {
	snapshots, err := cs.ListSnapshots(ctx)
	if err != nil {
		return err
	}

	if len(snapshots) <= cs.config.Retention {
		return nil
	}

	var errs []string
	for _, obj := range snapshots[cs.config.Retention:] {
		if err := cs.store.Delete(ctx, cs.bucket, obj.Key); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", obj.Key, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("prune errors: %s", strings.Join(errs, "; "))
	}
	return nil
}
