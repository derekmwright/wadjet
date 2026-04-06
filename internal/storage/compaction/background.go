package compaction

import (
	"context"
	"log/slog"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
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
		if err != nil {
			bc.logger.Warn("compaction failed", "table", table, "error", err)
			continue
		}

		if result.PartitionsCompacted > 0 {
			bc.logger.Info("compaction complete",
				"table", table,
				"partitions", result.PartitionsCompacted,
				"files_removed", result.FilesRemoved,
				"files_created", result.FilesCreated,
				"rows_merged", result.RowsMerged,
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
