// Package ingest provides micro-batch accumulation and flushing to object storage.
package ingest

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/storage/partition"
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

	// refusal, when non-nil, is returned by every Ingest and FlushAll call.
	// New() cannot report an error, so a caller that constructs an Ingester
	// over an inadmissible schema — a column in the planner's reserved slot
	// namespace, which this Ingester would CREATE the table with — is told at
	// the first call that does something. See wadjet.NewIngester.
	refusal error

	mu      sync.Mutex
	buffers map[string]*partitionBuffer // partition path -> buffer
	done    chan struct{}
	wg      sync.WaitGroup

	// deferCommit holds every flushed file OUT of the manifest so the caller
	// can commit them together with something else. See DeferManifestCommit.
	deferCommit bool
	pending     []catalog.PendingFile
}

// DeferManifestCommit stops this Ingester from registering its flushed files
// in the manifest, holding them in PendingFiles instead.
//
// It exists for one caller: a DML statement, which must commit the rows it
// WRITES and the delete markers that remove the rows they REPLACE in one CAS
// or neither (#691). The default — a manifest commit per flushed file — makes
// an UPDATE two independent commits, so a marker commit refused at the end
// left the replacement rows beside the originals, reporting an error over a
// table that now had both.
//
// Call it before the first Ingest, and never on an Ingester whose background
// flusher is Start()ed: the pending list is drained by the caller, not by a
// timer. The files are already durable in the object store when they land
// here; only their manifest entries are held.
func (ing *Ingester) DeferManifestCommit() { ing.deferCommit = true }

// PendingFiles returns the flushed files whose manifest entries are being
// held, and clears the list.
func (ing *Ingester) PendingFiles() []catalog.PendingFile {
	ing.mu.Lock()
	defer ing.mu.Unlock()
	out := ing.pending
	ing.pending = nil
	return out
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
				// PostgreSQL: 23502, not_null_violation. Without a class this
				// crossed the wire as the blanket 42000, which tells a client
				// its SQL was malformed rather than that its DATA was (#814).
				return sqlerr.New("23502",
					"null value in column %q violates not-null constraint", col.Name)
			}
			continue
		}
		if v == nil {
			if !col.Nullable {
				return sqlerr.New("23502",
					"null value in column %q violates not-null constraint", col.Name)
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
		switch tv := v.(type) {
		case time.Time, int32, int64:
			// ok
		case string:
			// Reject an unparseable or nonexistent calendar date at the
			// ingest boundary, before the writer turns it into the epoch
			// (day 0 = 1970-01-01) — silent data corruption, since the
			// original text is then gone (#560).
			if err := parquet.ValidateDateString(tv); err != nil {
				return fmt.Errorf("column %q: %w", col.Name, err)
			}
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
	case parquet.TypeDecimal:
		// Refuse a value with no DECIMAL at this column's (p, s) HERE, at the
		// ingest boundary, rather than at the flush that eventually writes it.
		// The writer refuses it too — DecimalValueFromBox is the same function
		// on both sides — but the flush happens per BUFFER, so a single bad
		// row failing there takes a batch of good ones with it and reports
		// against a partition rather than against the INSERT that carried it
		// (#647). Nothing is rewritten: the box the writer receives is the box
		// this row already holds.
		if _, err := parquet.DecimalValueFromBox(v, col.Precision, col.Scale); err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
	case parquet.TypeArray, parquet.TypeRow, parquet.TypeMap:
		// Reject a value no leaf of this container's declaration can hold at
		// the ingest boundary, before the writer's leaf turns it into the
		// epoch (DATE, #560) or refuses it at the flush (DECIMAL, #647) --
		// the flush is per BUFFER, so a bad row failing there takes the
		// already-accepted rows beside it with it. The top-level checks never
		// saw either one.
		if err := parquet.ValidateNestedLeaves(col, v); err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
	}
	return nil
}

// Ingest adds rows to the buffer. Rows are partitioned based on partition key values.
// Each row is validated against the table schema before buffering.
// RefuseWith arms this Ingester to reject every Ingest and FlushAll with err.
// Used by the constructor's caller for a schema the ingest door refuses.
func (ing *Ingester) RefuseWith(err error) { ing.refusal = err }

func (ing *Ingester) Ingest(ctx context.Context, rows []map[string]any) error {
	if ing.refusal != nil {
		return ing.refusal
	}
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
	if ing.refusal != nil {
		return ing.refusal
	}
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

	// Full UUIDv7, not a truncated prefix (#494): a v4 UUID's first 8 hex
	// chars carry only 32 bits, and mergeFileEntries keys the manifest by
	// Path and REPLACES on a collision — two chunks landing on the same
	// 8 chars silently drops one file's rows (p ~= n^2/2^33, ~0.6% at
	// 100k files in one table). The full ~122 random bits of a v7 UUID
	// make that astronomically unlikely, and v7's leading 48-bit
	// millisecond timestamp keeps chunk names roughly sortable by
	// creation order, same as a ULID, without a new dependency:
	// google/uuid (already imported here) has shipped NewV7 since v1.6.0.
	chunkID := fmt.Sprintf("chunk_%s", uuid.Must(uuid.NewV7()).String())
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
	// Augment with per-column HLL sketches AND reservoir samples
	// computed from the raw rows before they were serialized.
	// - HLL → NDV estimation in the planner
	// - Sample → cross-file histogram merge for selectivity estimation
	// Sketches are bundled into one object-store blob per parquet file
	// (sketches.go) so the catalog manifest stays under the NATS 1 MB
	// payload limit even at SF100 scale (60+ lineitem files × 16 cols).
	extra := computeColumnStats(buf.rows, ing.schema)
	var sketchEntries []catalog.FileSketchesEntry
	if len(extra) > 0 {
		if colStats == nil {
			colStats = make(map[string]catalog.FileColumnStats, len(extra))
		}
		for col, st := range extra {
			cs := colStats[col]
			var hllBytes, sampleBytes []byte
			if st.hll != nil {
				hllBytes = st.hll.Bytes()
			}
			if st.sampler != nil {
				vals, total, tc := st.sampler.Snapshot()
				if len(vals) > 0 {
					sampleBytes = catalog.SampleBytes(vals, total, tc)
				}
			}
			if len(hllBytes) > 0 || len(sampleBytes) > 0 {
				sketchEntries = append(sketchEntries, catalog.FileSketchesEntry{
					Column: col, HLL: hllBytes, Sample: sampleBytes,
				})
			}
			colStats[col] = cs
		}
	}
	var sketchesKey string
	if len(sketchEntries) > 0 {
		key, err := ing.catalog.UploadFileSketches(ctx, ing.tableName, filePath, sketchEntries)
		if err != nil {
			ing.logger.Warn("ingest: sketch upload failed; planner falls back to heuristic NDV",
				"table", ing.tableName, "file", filePath, "error", err)
		} else {
			sketchesKey = key
		}
	}

	// Update catalog manifest
	fileEntry := catalog.FileEntry{
		Path:        filePath,
		SizeBytes:   int64(len(data)),
		NumRows:     int64(len(buf.rows)),
		CreatedAt:   time.Now().UTC(),
		ColumnStats: colStats,
		SketchesKey: sketchesKey,
	}

	// AddNewFiles, not AddFiles: chunkID is a freshly minted UUIDv7 (#494),
	// never a path this table's manifest can already hold, so a collision
	// here should be refused loudly rather than silently replacing an
	// existing entry.
	//
	// Unless the caller deferred the manifest commit, in which case the file
	// is HELD and committed later in the same CAS as the delete markers that
	// supersede what it replaces (#691). See DeferManifestCommit.
	if ing.deferCommit {
		ing.pending = append(ing.pending, catalog.PendingFile{
			PartValues: buf.values,
			PartPath:   partPath,
			Entry:      fileEntry,
		})
	} else if err := ing.catalog.AddNewFiles(ctx, ing.tableName, buf.values, partPath, []catalog.FileEntry{fileEntry}); err != nil {
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
	// Unbox any CIDR bound before these stats leave for the CATALOG.
	// parquet.RowGroupStats hands back a confirmed CIDR min/max as a
	// parquet.CidrInetBound so the prune layer can compare it in inet order
	// (#523) — but that box's Key is a BINARY string, and
	// catalog.FileColumnStats is JSON-tagged and persisted in NATS KV, where
	// encoding/json rewrites every byte above 0x7F as U+FFFD with no way
	// back. The merge above needed the box (CompareNative orders CIDR by the
	// key); the catalog needs the winning row's address TEXT, which is what
	// the box's other half carries.
	for col, cs := range merged {
		if b, ok := cs.MinValue.(parquet.CidrInetBound); ok {
			cs.MinValue = b.Text
		}
		if b, ok := cs.MaxValue.(parquet.CidrInetBound); ok {
			cs.MaxValue = b.Text
		}
		merged[col] = cs
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
