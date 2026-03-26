package compaction

import (
	"context"
	"log/slog"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
)

const DefaultCompactionInterval = 5 * time.Minute

// BackgroundConfig controls the periodic compaction loop.
type BackgroundConfig struct {
	// Enabled controls whether background compaction runs. Default: true.
	Enabled bool
	// Interval between compaction sweeps. Zero uses DefaultCompactionInterval.
	Interval time.Duration
	// Compaction controls compaction trigger thresholds.
	Compaction Config
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
	}
}
