package compaction

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

const DefaultCompactionInterval = 5 * time.Minute

// DefaultGCMinAge is the minimum age before delete markers are eligible for GC.
// Set to 30 minutes to safely exceed the duration of long-running analytical
// queries, preventing GC from rewriting files that are being scanned.
const DefaultGCMinAge = 30 * time.Minute

// BackgroundConfig controls the periodic compaction loop.
type BackgroundConfig struct {
	// Enabled controls whether background compaction runs. Default: true.
	Enabled bool
	// Interval between compaction sweeps. Zero uses DefaultCompactionInterval.
	Interval time.Duration
	// Compaction controls compaction trigger thresholds.
	Compaction Config
	// GCMinAge is the minimum age before delete markers are garbage collected
	// and their files are force-rewritten. Zero uses DefaultGCMinAge.
	GCMinAge time.Duration
	// DropGrace is the minimum age before a dropped table's data files are
	// physically deleted (catalog.Catalog.FlushDroppedTableFiles). Zero
	// uses catalog.DefaultDropTableGrace. Only consulted when
	// ReclaimDroppedTables is true.
	DropGrace time.Duration
	// ReclaimDroppedTables controls whether the sweep calls
	// catalog.Catalog.FlushDroppedTableFiles at all. Default: false.
	//
	// Deliberately opt-in rather than tied to Enabled (#494 review): this
	// *Catalog is not the only one a DROP can go through. An embedded
	// wadjet.DB and a standalone pgwire DB each own a separate *Catalog
	// from the one a BackgroundCompactor sweeps (cmd/wadjet/main.go's
	// standalone mode is the concrete case — its pgwire server opens its
	// own wadjet.DB), so turning this on unconditionally would reclaim a
	// DROPped table's files for queries issued through the compactor's
	// catalog while silently never reclaiming ones issued through psql or
	// the embedded API against the others. An explicit, honest default of
	// "not reclaimed anywhere yet" beats an inconsistent "reclaimed here,
	// not there" that looks like a bug until someone reads the wiring.
	// Turning it on is safe wherever it runs: FlushDroppedTableFiles's
	// live-manifest guard and table-prefix scoping hold regardless of
	// which *Catalog instance calls it.
	ReclaimDroppedTables bool
}

// BackgroundCompactor runs periodic compaction sweeps across all tables.
type BackgroundCompactor struct {
	compactor *Compactor
	catalog   *catalog.Catalog
	config    BackgroundConfig
	logger    *slog.Logger
}

// NewBackgroundCompactor creates a background compactor.
func NewBackgroundCompactor(cat *catalog.Catalog, cfg BackgroundConfig, logger *slog.Logger) *BackgroundCompactor {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Interval == 0 {
		cfg.Interval = DefaultCompactionInterval
	}
	if cfg.GCMinAge == 0 {
		cfg.GCMinAge = DefaultGCMinAge
	}
	if cfg.DropGrace == 0 {
		cfg.DropGrace = catalog.DefaultDropTableGrace
	}
	if cfg.Enabled && cfg.ReclaimDroppedTables {
		// Declare the flusher before anything can DROP through this
		// catalog: DropTable records a pending entry only where something
		// will consume it, so a catalog nobody sweeps never grows a list.
		// Done at construction rather than in Start so the window between
		// "the process is up" and "the sweep loop is running" is not a
		// window in which drops go unrecorded.
		//
		// Gated on cfg.Enabled too: Start returns immediately when it is
		// false and the sweep loop that would ever call
		// FlushDroppedTableFiles never runs. Without this gate a
		// Enabled:false, ReclaimDroppedTables:true compactor still wired
		// the flusher, so every DROP through this *Catalog would record a
		// pending entry that nothing will ever consume — accumulating
		// toward maxPendingDropPaths and evicting (leaking) rather than
		// costing nothing, which is the whole point of declaring the
		// flusher lazily in the first place.
		cat.EnableDropReclaim()
	}
	return &BackgroundCompactor{
		compactor: New(cat, logger, cfg.Compaction),
		catalog:   cat,
		config:    cfg,
		logger:    logger,
	}
}

// Start launches the background compaction loop.
func (bc *BackgroundCompactor) Start(ctx context.Context) {
	if !bc.config.Enabled {
		bc.logger.Info("background compaction disabled")
		return
	}
	go bc.run(ctx)
	bc.logger.Info("background compaction started", "interval", bc.config.Interval)
}

func (bc *BackgroundCompactor) run(ctx context.Context) {
	ticker := time.NewTicker(bc.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			bc.sweep(ctx)
		}
	}
}

func (bc *BackgroundCompactor) sweep(ctx context.Context) {
	// Physically delete compacted-away files whose DeleteGrace has expired
	// (their manifest entries are long gone; the grace existed only for
	// queries dispatched against the pre-compaction manifest).
	bc.compactor.FlushDeferredDeletes(ctx)

	// Same idea for DROP TABLE (#494), gated behind ReclaimDroppedTables
	// (see its doc comment for why this is opt-in rather than unconditional
	// like FlushDeferredDeletes above): physically delete a dropped table's
	// data files once DropGrace has elapsed. Independent of the table loop
	// below by construction — the table is already gone from ListTables.
	if bc.config.ReclaimDroppedTables {
		if dropped := bc.catalog.FlushDroppedTableFiles(ctx, bc.config.DropGrace); dropped > 0 {
			bc.logger.Info("flushed dropped-table files", "files", dropped)
		}
	}

	tables, err := bc.catalog.ListTables(ctx)
	if err != nil {
		bc.logger.Error("compaction sweep: listing tables", "error", err)
		return
	}

	for _, table := range tables {
		if ctx.Err() != nil {
			return
		}

		result, err := bc.compactor.CompactTable(ctx, table)
		// A partition whose merge failed is reported and skipped, not fatal
		// to the sweep: CompactTable has already compacted whatever else the
		// table had, and the delete-marker GC below is independent of it. The
		// unconditional `continue` this replaced meant one drifted partition
		// stopped GC for the whole table indefinitely — delete markers that
		// never age out, and the deleted rows still physically present.
		var failed *CompactionFailed
		switch {
		case errors.As(err, &failed):
			bc.logger.Warn("compaction partially failed",
				"table", table,
				"partitions_failed", len(failed.Failures),
				"partitions_compacted", failed.Compacted,
				"error", err)
		case err != nil:
			// The manifest, the store, or the context: nothing about this
			// table's state is known, so leave it for the next sweep.
			bc.logger.Warn("compaction failed", "table", table, "error", err)
			continue
		}

		if result != nil && result.PartitionsCompacted > 0 {
			bc.logger.Info("compaction complete",
				"table", table,
				"partitions", result.PartitionsCompacted,
				"files_removed", result.FilesRemoved,
				"files_created", result.FilesCreated,
				"rows_merged", result.RowsMerged,
				"pass_limit_reached", result.PassLimitReached,
			)
		}

		// GC aged delete markers: rewrite files with pending deletes, drop orphans
		rewriteTargets, orphanPaths, err := bc.catalog.GCDeleteMarkers(ctx, table, bc.config.GCMinAge)
		if err != nil {
			bc.logger.Warn("delete marker GC failed", "table", table, "error", err)
			continue
		}
		if len(orphanPaths) > 0 {
			bc.logger.Info("delete marker GC: dropped orphan markers",
				"table", table, "count", len(orphanPaths))
		}
		for fp, indices := range rewriteTargets {
			if ctx.Err() != nil {
				return
			}
			gcSet := make(map[int64]bool, len(indices))
			for _, idx := range indices {
				gcSet[idx] = true
			}
			if err := bc.compactor.ForceCompactFile(ctx, table, fp, gcSet); err != nil {
				bc.logger.Warn("force compact failed",
					"table", table, "file", fp, "error", err)
			}
		}
	}
}
