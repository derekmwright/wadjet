package physical

import (
	"context"
	"sync"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// ManifestSnapshot pins each table's manifest and aggregated column stats for
// one statement (#502), independent of scan-node count and Optimize passes.
// Every scan shares one ScanDeletes snapshot, so first-wins unions cannot retain
// stale markers across concurrent writes (#491). Pin the two Catalog operations
// separately: the floor is two manifest reads per table, pending #540.
// Attach before planning: NewPlanner creates a fresh snapshot; forSubquery shares
// it. Callers creating several Planners per statement MUST assign the same pointer.
// Asynchronous watch caching cannot provide the required freshness (#483).
// See docs/internals/statement-manifest-snapshot.md for the design.
type ManifestSnapshot struct {
	mu      sync.Mutex
	byTbl   map[string]*cachedManifestEntry
	colStat map[string]*cachedColStatsEntry
}

// cachedManifestEntry pins one table's read, success or failure. A failed
// read is cached too: a table that does not exist does not start existing a
// moment later within the same statement, and retrying it on every caller
// would defeat the pin exactly as much as retrying a successful one would.
type cachedManifestEntry struct {
	manifest *catalog.PartitionManifest
	rev      uint64
	err      error
}

// cachedColStatsEntry pins one table's AggregateColumnStats result the same
// way cachedManifestEntry pins its manifest. AggregateColumnStats reads the
// manifest internally (to key its own cache on the manifest's revision) via
// Catalog.manifestWithRevision directly, NOT through Planner.getManifest —
// it is a Catalog method, not a Planner one — so it needed its own pin
// alongside GetManifest's: pinning only GetManifest still left
// AnnotateScanColumns paying one extra unpinned manifest read per scan node
// per optimizer pass through this call, which is where most of the #483
// review's 9 reads for a single-table SELECT actually came from.
type cachedColStatsEntry struct {
	stats map[string]catalog.TableColumnStats
	err   error
}

// NewManifestSnapshot returns an empty snapshot ready to pin a statement's
// first read of each table it touches.
func NewManifestSnapshot() *ManifestSnapshot {
	return &ManifestSnapshot{
		byTbl:   make(map[string]*cachedManifestEntry),
		colStat: make(map[string]*cachedColStatsEntry),
	}
}

// Get returns table's manifest, reading it from cat on the first call for
// that table within this snapshot's lifetime and returning the SAME
// manifest object (and error) to every later call for that table,
// regardless of which caller or which Planner instance makes it.
//
// Concurrency-safe: a local-fastpath scan's Init can run from a parallel
// pipeline worker, so two goroutines racing to be the first reader of one
// table both complete their catalog reads, but only one result is kept —
// matching Catalog's own manifestWithRevision, where every caller of a
// race is handed the winner rather than blocked behind it.
func (m *ManifestSnapshot) Get(ctx context.Context, cat *catalog.Catalog, table string) (*catalog.PartitionManifest, error) {
	m.mu.Lock()
	if e, ok := m.byTbl[table]; ok {
		m.mu.Unlock()
		return e.manifest, e.err
	}
	m.mu.Unlock()

	manifest, rev, err := cat.GetManifestWithRevision(ctx, table)

	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.byTbl[table]; ok {
		// Lost the race: keep whichever read landed first, so every
		// caller in this statement agrees on one manifest object.
		return e.manifest, e.err
	}
	m.byTbl[table] = &cachedManifestEntry{manifest: manifest, rev: rev, err: err}
	return manifest, err
}

// pinnedManifest returns the snapshot's entry for table, reading it through
// Get first when nothing has yet. It is what lets AggregateColumnStats hand
// the SAME manifest object and revision to the catalog rather than letting
// it read the key again (#540).
func (m *ManifestSnapshot) pinnedManifest(ctx context.Context, cat *catalog.Catalog, table string) *cachedManifestEntry {
	m.Get(ctx, cat, table)
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.byTbl[table]
}

// AggregateColumnStats returns table's aggregated per-column stats, reading
// them from cat on the first call for that table within this snapshot's
// lifetime and returning the SAME result to every later call — the
// AggregateColumnStats counterpart to Get, for the same statement-pin
// reason (see cachedColStatsEntry).
func (m *ManifestSnapshot) AggregateColumnStats(ctx context.Context, cat *catalog.Catalog, table string) (map[string]catalog.TableColumnStats, error) {
	m.mu.Lock()
	if e, ok := m.colStat[table]; ok {
		m.mu.Unlock()
		return e.stats, e.err
	}
	m.mu.Unlock()

	// Read the STATS from the manifest this statement has already pinned,
	// not from a fresh one. Two reads of one key can straddle a writer, and
	// the statistics are the newer half (#540).
	var stats map[string]catalog.TableColumnStats
	var err error
	if e := m.pinnedManifest(ctx, cat, table); e != nil && e.err == nil {
		stats, err = cat.AggregateColumnStatsFrom(ctx, table, e.manifest, e.rev)
	} else {
		stats, err = cat.AggregateColumnStats(ctx, table)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.colStat[table]; ok {
		return e.stats, e.err
	}
	m.colStat[table] = &cachedColStatsEntry{stats: stats, err: err}
	return stats, err
}

// getManifest is catalog.GetManifest pinned to p's ManifestSnapshot when it
// has one, and the ordinary per-call catalog.GetManifest otherwise — the
// fallback a bare Planner literal (rather than one built by NewPlanner)
// gets, which today is only test code.
//
// EVERY in-package manifest read goes through here; a direct
// p.catalog.GetManifest call silently opts that site out of the statement's
// pin, which is not visible in any answer — only in the read count. The
// sites are AnnotateScanColumns, walkStages and estimateSubtreeBytes
// (plan.go), EstimatePlanScanBytes (scan_estimate.go — the FIRST catalog
// read the default route makes, in tryLocalFastPath, once per scan node),
// tryBuildMetadataCount (metadata_count.go) and tryBuildMetadataMinMax
// (metadata_minmax.go) — the two shapes that answer from the manifest
// instead of scanning, so an extra fetch there is a large fraction of the
// query — and estimateFKReferencedRowCount (dynamic_filter.go), whose reads
// are SPECULATIVE lookups of table names guessed from a column name and so
// are pure overhead when the guess misses.
func (p *Planner) getManifest(ctx context.Context, table string) (*catalog.PartitionManifest, error) {
	if p.ManifestSnapshot != nil {
		return p.ManifestSnapshot.Get(ctx, p.catalog, table)
	}
	return p.catalog.GetManifest(ctx, table)
}

// getAggregateColumnStats is catalog.AggregateColumnStats pinned to p's
// ManifestSnapshot when it has one, mirroring getManifest.
func (p *Planner) getAggregateColumnStats(ctx context.Context, table string) (map[string]catalog.TableColumnStats, error) {
	if p.ManifestSnapshot != nil {
		return p.ManifestSnapshot.AggregateColumnStats(ctx, p.catalog, table)
	}
	return p.catalog.AggregateColumnStats(ctx, table)
}

// getManifestWith is getManifest for a caller that holds a *catalog.Catalog
// and an optional *ManifestSnapshot but not a *Planner — scannerExecSource,
// the local-fastpath scan source's execution-time counterpart to
// catalogScanSource, is constructed by a free function (newScannerSource)
// rather than as a Planner method.
func getManifestWith(ctx context.Context, snap *ManifestSnapshot, cat *catalog.Catalog, table string) (*catalog.PartitionManifest, error) {
	if snap != nil {
		return snap.Get(ctx, cat, table)
	}
	return cat.GetManifest(ctx, table)
}

// manifestSnapshotCtxKey types the context key WithManifestSnapshot and
// ManifestSnapshotFromContext use, so it cannot collide with a key any
// other package defines.
type manifestSnapshotCtxKey struct{}

// WithManifestSnapshot attaches snap to ctx. A coordinator entry point that
// handles one statement end to end but builds several Planner instances for
// it — each construction is a separate physical.NewPlanner call, so a
// Planner-instance-scoped snapshot alone cannot span them — calls this ONCE
// near the top, before any planning begins, and passes the resulting
// context to everything downstream. NewPlannerForContext is the pairing
// half: every Planner built from that context onward shares snap.
func WithManifestSnapshot(ctx context.Context, snap *ManifestSnapshot) context.Context {
	return context.WithValue(ctx, manifestSnapshotCtxKey{}, snap)
}

// ManifestSnapshotFromContext returns the snapshot WithManifestSnapshot
// attached, or nil if ctx carries none.
func ManifestSnapshotFromContext(ctx context.Context) *ManifestSnapshot {
	snap, _ := ctx.Value(manifestSnapshotCtxKey{}).(*ManifestSnapshot)
	return snap
}

// NewPlannerForContext is NewPlanner plus: if ctx carries a ManifestSnapshot
// (WithManifestSnapshot), the new Planner shares it instead of getting the
// fresh, private one NewPlanner otherwise gives it. Prefer this over
// NewPlanner at any coordinator entry point that builds more than one
// Planner for the same statement (#502) — a bare NewPlanner call there
// silently opts that Planner instance out of the statement's pin.
func NewPlannerForContext(ctx context.Context, cat *catalog.Catalog) *Planner {
	p := NewPlanner(cat)
	if snap := ManifestSnapshotFromContext(ctx); snap != nil {
		p.ManifestSnapshot = snap
	}
	return p
}
