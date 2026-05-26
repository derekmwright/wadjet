// Package ingest provides micro-batch accumulation and flushing to object storage.
package ingest

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/internal/storage/partition"
	"github.com/google/uuid"
)

// Config configures the micro-batch ingester.
type Config struct {
	MaxBufferSize int           // max bytes before flush (default 128 MB)
	MaxBufferRows int           // max rows before flush (default 1M)
	FlushInterval time.Duration // max time before flush (default 60s)
	RowGroupSize  int           // rows per row group in Parquet (default 128K)
	MinFlushRows  int           // min rows to flush on timer (default 100; 0 = no minimum)
}

// DefaultConfig returns default ingest configuration.
func DefaultConfig() Config {
	return Config{
		MaxBufferSize: 128 * 1024 * 1024,
		MaxBufferRows: 1_000_000,
		FlushInterval: 60 * time.Second,
		RowGroupSize:  128 * 1024,
		MinFlushRows:  100,
	}
}

// Ingester accumulates rows and periodically flushes them as Parquet files.
type Ingester struct {
	catalog   *catalog.Catalog
	tableName string
	schema    parquet.Schema
	strategy  *partition.Strategy
	config    Config
	logger    *slog.Logger

	mu      sync.Mutex
	buffers map[string]*partitionBuffer // partition path -> buffer
	done    chan struct{}
	wg      sync.WaitGroup
}

type partitionBuffer struct {
	values map[string]string
	path   string
	rows   []map[string]any
	size   int // estimated size in bytes
}

// New creates a new Ingester for the given table.
func New(cat *catalog.Catalog, tableName string, schema parquet.Schema, partKeys []string, cfg Config) *Ingester {
	if cfg.MaxBufferSize <= 0 {
		cfg.MaxBufferSize = DefaultConfig().MaxBufferSize
	}
	if cfg.MaxBufferRows <= 0 {
		cfg.MaxBufferRows = DefaultConfig().MaxBufferRows
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultConfig().FlushInterval
	}
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = DefaultConfig().RowGroupSize
	}

	return &Ingester{
		catalog:   cat,
		tableName: tableName,
		schema:    schema,
		strategy:  partition.NewStrategy(partKeys),
		config:    cfg,
		logger:    slog.Default(),
		buffers:   make(map[string]*partitionBuffer),
		done:      make(chan struct{}),
	}
}

// Start begins the background flush timer.
func (ing *Ingester) Start() {
	ing.wg.Add(1)
	go func() {
		defer ing.wg.Done()
		ticker := time.NewTicker(ing.config.FlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if err := ing.flushReady(context.Background()); err != nil {
					ing.logger.Error("periodic flush failed", "error", err)
				}
			case <-ing.done:
				return
			}
		}
	}()
}

// Stop stops the background flush timer and flushes remaining data.
func (ing *Ingester) Stop(ctx context.Context) error {
	close(ing.done)
	ing.wg.Wait()
	return ing.FlushAll(ctx)
}

// validateRow checks that a row conforms to the table schema.
// It verifies that all non-nullable columns are present and non-nil,
// and that values have compatible types.
func (ing *Ingester) validateRow(row map[string]any) error {
	for _, col := range ing.schema.Columns {
		v, ok := row[col.Name]
		if !ok {
			if !col.Nullable {
				return fmt.Errorf("missing required column %q (NOT NULL)", col.Name)
			}
			continue
		}
		if v == nil {
			if !col.Nullable {
				return fmt.Errorf("column %q cannot be null (NOT NULL constraint)", col.Name)
			}
			continue
		}
		if err := checkType(col, v); err != nil {
			return err
		}
	}
	return nil
}

// checkType validates that a value is compatible with the column type.
func checkType(col parquet.Column, v any) error {
	switch col.Type {
	case parquet.TypeBool:
		if _, ok := v.(bool); !ok {
			return fmt.Errorf("column %q: expected bool, got %T", col.Name, v)
		}
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		switch v.(type) {
		case int, int8, int16, int32, int64, uint8, uint16, uint32:
			// ok
		default:
			return fmt.Errorf("column %q: expected integer, got %T", col.Name, v)
		}
	case parquet.TypeInt64:
		switch v.(type) {
		case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
			// ok
		default:
			return fmt.Errorf("column %q: expected integer, got %T", col.Name, v)
		}
	case parquet.TypeFloat32:
		switch v.(type) {
		case float32, float64, int, int32, int64:
			// ok
		default:
			return fmt.Errorf("column %q: expected float, got %T", col.Name, v)
		}
	case parquet.TypeFloat64:
		switch v.(type) {
		case float32, float64, int, int32, int64:
			// ok
		default:
			return fmt.Errorf("column %q: expected float, got %T", col.Name, v)
		}
	case parquet.TypeString, parquet.TypeIPv4, parquet.TypeIPv6, parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID:
		if _, ok := v.(string); !ok {
			return fmt.Errorf("column %q: expected string, got %T", col.Name, v)
		}
	case parquet.TypeBytes:
		switch v.(type) {
		case []byte, string:
			// ok
		default:
			return fmt.Errorf("column %q: expected bytes or string, got %T", col.Name, v)
		}
	case parquet.TypeTimestamp:
		switch v.(type) {
		case time.Time, int64, string:
			// ok
		default:
			return fmt.Errorf("column %q: expected timestamp (time.Time, int64, or string), got %T", col.Name, v)
		}
	case parquet.TypeDate:
		switch v.(type) {
		case time.Time, int32, int64, string:
			// ok
		default:
			return fmt.Errorf("column %q: expected date (time.Time, int, or string), got %T", col.Name, v)
		}
	case parquet.TypeDuration:
		switch v.(type) {
		case time.Duration, int64, string:
			// ok
		default:
			return fmt.Errorf("column %q: expected duration (time.Duration, int64, or string), got %T", col.Name, v)
		}
	}
	return nil
}

// Ingest adds rows to the buffer. Rows are partitioned based on partition key values.
// Each row is validated against the table schema before buffering.
func (ing *Ingester) Ingest(ctx context.Context, rows []map[string]any) error {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	for _, row := range rows {
		// Validate row against schema
		if err := ing.validateRow(row); err != nil {
			return fmt.Errorf("schema validation: %w", err)
		}

		partValues := make(map[string]string, len(ing.strategy.Keys))
		for _, key := range ing.strategy.Keys {
			v, ok := row[key]
			if !ok {
				return fmt.Errorf("missing partition key %q in row", key)
			}
			partValues[key] = ing.formatPartitionValue(key, v)
		}

		partPath := ing.strategy.PartitionPath(partValues)
		buf, ok := ing.buffers[partPath]
		if !ok {
			buf = &partitionBuffer{
				values: partValues,
				path:   partPath,
			}
			ing.buffers[partPath] = buf
		}

		buf.rows = append(buf.rows, row)
		buf.size += estimateRowSize(row)
	}

	// Check if any buffer needs flushing
	for partPath, buf := range ing.buffers {
		if buf.size >= ing.config.MaxBufferSize || len(buf.rows) >= ing.config.MaxBufferRows {
			if err := ing.flushBuffer(ctx, partPath, buf); err != nil {
				return fmt.Errorf("flushing partition %s: %w", partPath, err)
			}
		}
	}

	return nil
}

// FlushAll flushes all buffered data to storage unconditionally.
func (ing *Ingester) FlushAll(ctx context.Context) error {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	for partPath, buf := range ing.buffers {
		if len(buf.rows) == 0 {
			continue
		}
		if err := ing.flushBuffer(ctx, partPath, buf); err != nil {
			return fmt.Errorf("flushing partition %s: %w", partPath, err)
		}
	}
	return nil
}

// flushReady flushes partitions that have accumulated enough rows.
// Partitions below MinFlushRows are skipped to avoid creating tiny files.
// Used by the background timer; explicit callers should use FlushAll.
func (ing *Ingester) flushReady(ctx context.Context) error {
	ing.mu.Lock()
	defer ing.mu.Unlock()

	for partPath, buf := range ing.buffers {
		if len(buf.rows) == 0 {
			continue
		}
		if ing.config.MinFlushRows > 0 && len(buf.rows) < ing.config.MinFlushRows {
			continue
		}
		if err := ing.flushBuffer(ctx, partPath, buf); err != nil {
			return fmt.Errorf("flushing partition %s: %w", partPath, err)
		}
	}
	return nil
}

func (ing *Ingester) flushBuffer(ctx context.Context, partPath string, buf *partitionBuffer) error {
	if len(buf.rows) == 0 {
		return nil
	}

	chunkID := fmt.Sprintf("chunk_%s", uuid.New().String()[:8])
	filePath := ing.strategy.FilePath(
		partition.TablePrefix(ing.tableName),
		buf.values,
		chunkID,
	)

	// Write parquet to buffer
	var parquetBuf bytes.Buffer
	pw, err := parquet.NewWriter(&parquetBuf, ing.schema, parquet.WriterConfig{
		RowGroupSize: ing.config.RowGroupSize,
	})
	if err != nil {
		return fmt.Errorf("creating parquet writer: %w", err)
	}

	if err := pw.WriteRows(buf.rows); err != nil {
		return fmt.Errorf("writing rows: %w", err)
	}
	if err := pw.Close(); err != nil {
		return fmt.Errorf("closing parquet writer: %w", err)
	}

	data := parquetBuf.Bytes()
	_, err = ing.catalog.Store().Put(ctx, ing.catalog.Bucket(), filePath,
		bytes.NewReader(data), int64(len(data)), "application/octet-stream")
	if err != nil {
		return fmt.Errorf("uploading parquet file: %w", err)
	}

	// Extract column statistics from the written Parquet file
	colStats := extractColumnStats(data)
	// Augment with per-column HLL sketches computed from the raw rows
	// before they were serialized. Parquet metadata doesn't carry NDV
	// info, so the only place to capture it cheaply is right here while
	// the rows are still in memory.
	hlls := computeColumnHLLs(buf.rows, ing.schema)
	if len(hlls) > 0 {
		if colStats == nil {
			colStats = make(map[string]catalog.FileColumnStats, len(hlls))
		}
		for col, h := range hlls {
			cs := colStats[col]
			cs.HLL = h.Bytes()
			colStats[col] = cs
		}
	}

	// Update catalog manifest
	fileEntry := catalog.FileEntry{
		Path:        filePath,
		SizeBytes:   int64(len(data)),
		NumRows:     int64(len(buf.rows)),
		CreatedAt:   time.Now().UTC(),
		ColumnStats: colStats,
	}

	if err := ing.catalog.AddFiles(ctx, ing.tableName, buf.values, partPath, []catalog.FileEntry{fileEntry}); err != nil {
		return fmt.Errorf("updating manifest: %w", err)
	}

	ing.logger.Info("flushed partition",
		"table", ing.tableName,
		"partition", partPath,
		"rows", len(buf.rows),
		"bytes", len(data),
		"file", filePath,
	)

	// Reset buffer
	buf.rows = buf.rows[:0]
	buf.size = 0

	return nil
}

func (ing *Ingester) formatPartitionValue(key string, v any) string {
	switch t := v.(type) {
	case time.Time:
		for _, col := range ing.schema.Columns {
			if col.Name == key {
				if col.Type == parquet.TypeDate {
					return t.Format("2006-01-02")
				}
				if col.Type == parquet.TypeTimestamp {
					return t.Format(time.RFC3339)
				}
				break
			}
		}
		return t.Format(time.RFC3339)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// extractColumnStats reads Parquet metadata from written data to extract
// per-column min/max/null statistics for the catalog.
func extractColumnStats(data []byte) map[string]catalog.FileColumnStats {
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	nrg := reader.NumRowGroups()
	if nrg == 0 {
		return nil
	}

	merged := make(map[string]catalog.FileColumnStats)
	for i := 0; i < nrg; i++ {
		rgs := reader.RowGroupStats(i)
		for col, cs := range rgs.Columns {
			if !cs.HasStats {
				continue
			}
			cur, ok := merged[col]
			if !ok {
				cur = catalog.FileColumnStats{
					MinValue:  cs.MinValue,
					MaxValue:  cs.MaxValue,
					NullCount: cs.NullCount,
				}
			} else {
				cur.NullCount += cs.NullCount
				if cs.MinValue != nil && (cur.MinValue == nil || parquet.CompareNative(cs.MinValue, cur.MinValue) < 0) {
					cur.MinValue = cs.MinValue
				}
				if cs.MaxValue != nil && (cur.MaxValue == nil || parquet.CompareNative(cs.MaxValue, cur.MaxValue) > 0) {
					cur.MaxValue = cs.MaxValue
				}
			}
			merged[col] = cur
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return merged
}

func estimateRowSize(row map[string]any) int {
	size := 64 // map overhead
	for k, v := range row {
		size += len(k) + 16 // key string + pointer
		switch val := v.(type) {
		case string:
			size += len(val)
		case []byte:
			size += len(val)
		default:
			size += 8 // numeric types
		}
	}
	return size
}
