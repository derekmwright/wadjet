// This file holds scanner source for the physical planner, governed by ADR-0026 and ADR-0027.
package physical

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// newScannerSource creates a scanner exec.Source from the catalog. snap may
// be nil (falls back to an ordinary catalog.GetManifest call in Init).
func newScannerSource(cat *catalog.Catalog, tableName string, partFilter map[string]string, requiredCols []string, scanPreds []logical.Predicate, snap *ManifestSnapshot) exec.Source {
	return &scannerExecSource{
		catalog:          cat,
		tableName:        tableName,
		partitionFilter:  partFilter,
		requiredCols:     requiredCols,
		scanPreds:        scanPreds,
		manifestSnapshot: snap,
	}
}

type scannerExecSource struct {
	catalog          *catalog.Catalog
	tableName        string
	partitionFilter  map[string]string
	requiredCols     []string
	scanPreds        []logical.Predicate
	allowedFiles     []string // probe-split: only scan these files (nil = all)
	scanner          *scanSourceInner
	bloomFilter      *exec.BloomScanFilter
	dynamicFilter    []exec.DynamicRange
	rowLimit         int64                // LIMIT pushdown: enables lazy file downloading (0 = eager)
	memTracker       *memory.Tracker      // per-query memory tracker; passed to scanSourceInner at Init
	spillMgr         *memory.SpillManager // for pre-emptive relief on file-load reservations; nil-safe
	emitRowLoc       bool                 // top-N late materialization: stamp __row_loc on scan batches
	rowPreds         []scan.RowPred       // scan-level filter conjuncts
	shapeOnlyCols    map[string]bool      // byte-array columns decoded as lengths only
	manifestSnapshot *ManifestSnapshot    // pins this table's manifest to one read per statement (#502); nil-safe
}

type scanSourceInner struct {
	cat            *catalog.Catalog
	tableName      string
	files          []catalog.FileEntry
	idx            int64 // atomic index for parallel file workers (fallback path)
	schema         []parquet.Column
	requiredCols   []string
	scanPreds      []scanPredicate // converted predicates for row-group pruning
	rowsScanned    int64
	deleteMarkers  map[string]map[int64]bool // file path -> set of row indices to skip
	hasNestedTypes bool                      // true if schema has ARRAY/ROW/MAP types
	rowLimit       int64                     // >0: lazy file downloading (LIMIT pushdown)

	// row-group-level parallel scan
	rgUnits       []rgUnit        // flat list of row group work units
	rgIdx         int64           // atomic index for parallel RG workers
	emitRowLoc    bool            // stamp __row_loc (rgUnit ordinal, row) on every scan batch; disables batch pooling
	eqProbes      []scan.EqProbe  // "=" conjuncts for dictionary-probe row-group pruning (dict_prune.go)
	rowPreds      []scan.RowPred  // scan-level filter conjuncts, evaluated per row group in readRG
	shapeOnlyCols map[string]bool // lowercased names of columns decoded as lengths only (lengths_decode.go)
	countOnlyScan bool            // requiredCols is exactly the row-count sentinel: batches carry Len/Sel only
	useNative     bool            // true if native page decoder can be used (no Decimal/Array/Map)
	loadGate      *loadGate       // byte-budgeted admission for in-flight file LOADs (data, not metadata)

	// batch pooling — reuse batch allocations across row groups
	pool *batch.BatchPool

	cachedReadSchema     []parquet.Column // projected schema, computed once
	cachedReadSchemaOnce sync.Once        // guards cachedReadSchema for concurrent rgWorker access

	// parallel scan
	batchCh chan *batch.RecordBatch
	errCh   chan error
	wg      sync.WaitGroup
	cancel  context.CancelFunc

	// Bloom filter pushdown from hash join build side.
	bloomFilter *exec.BloomScanFilter

	// Dynamic min/max range filter from hash join build side.
	dynamicFilter []exec.DynamicRange

	// failedFiles counts files that failed to read during buildRGUnits.
	// When > 0, Init returns an error to prevent silent data loss.
	failedFiles  int
	firstFileErr error // sample error from the first file failure

	// fatalScanErr is a failure the scan must NOT tolerate, however many
	// other files succeeded. failedFiles is deliberately forgiving — it only
	// fails the scan when EVERY file failed, because a since-deleted object
	// is a survivable degradation. A recovered panic is not in that class: a
	// footer decoder that panicked has no idea how many row groups it should
	// have produced, so tolerating it drops that file's rows and answers a
	// wrong number (#511).
	fatalScanErr error

	// pooledBufs tracks []byte buffers obtained from readBufPool during
	// buildRGUnits. These are returned to the pool when the scan source
	// is closed, enabling cross-query buffer reuse.
	pooledBufsMu sync.Mutex
	pooledBufs   [][]byte

	// memTracker accounts for pooled buffers (parquet file []byte loads).
	// nil-safe: when nil, tracking is a no-op. Wired by the planner at
	// scan-source construction when a per-query spill manager is available.
	memTracker *memory.Tracker

	// trackedBufBytes is the cumulative bytes currently reported to memTracker
	// from pooledBufs. Released atomically in releasePooledBufs to avoid
	// double-release on idempotent close.
	trackedBufBytes atomic.Int64

	// spillMgr lets file-load reservations request operator relief before
	// waiting on the budget (memory.ReserveOrForce). nil-safe.
	spillMgr *memory.SpillManager

	// residentSlabs counts the row-group buffers this scan source is holding
	// across every file (scan_rowgroup_load.go). It is the deadlock-freedom
	// floor: a loader with nothing resident admits its next row group without
	// waiting, because there is nothing decoding that could free room for it.
	residentSlabs atomic.Int64

	// batchCharges maps decoded batches currently held by the scan source
	// (decode in progress, prefetched, or queued in batchCh) to the bytes
	// charged against memTracker when they were decoded. Released when the
	// batch leaves through next(), is dropped on a filter path, or is
	// drained at Close. LoadAndDelete makes every release idempotent.
	batchCharges sync.Map
}

// trackScanBatch charges a freshly decoded batch's footprint to the memory
// tracker until the batch leaves the scan source. No-op without a tracker.
func (inner *scanSourceInner) trackScanBatch(b *batch.RecordBatch) {
	if inner.memTracker == nil || b == nil {
		return
	}
	n := b.MemBytes()
	if n <= 0 {
		return
	}
	inner.batchCharges.Store(b, n)
	inner.memTracker.ForceReserveFor(n, memory.ForceScanDecodedBatch)
}

// releaseScanBatch releases the charge recorded by trackScanBatch.
// Idempotent: a second release for the same batch is a no-op.
func (inner *scanSourceInner) releaseScanBatch(b *batch.RecordBatch) {
	if inner.memTracker == nil || b == nil {
		return
	}
	if n, ok := inner.batchCharges.LoadAndDelete(b); ok {
		inner.memTracker.ReleaseForced(n.(int64), memory.ForceScanDecodedBatch)
	}
}

// drainSlotCharges releases the lazy fileSlot state of every slot whose row
// groups were not fully consumed — buffers, load-gate bytes and shared-tracker
// charges abandoned by an early Close (LIMIT, cancel, error). Must run after
// wg.Wait (no rg worker may still be loading) and BEFORE releasePooledBufs,
// which nils rgUnits (the only reference to the slots).
func (inner *scanSourceInner) drainSlotCharges() {
	seen := make(map[*fileSlot]bool)
	for _, u := range inner.rgUnits {
		if u.slot == nil || seen[u.slot] {
			continue
		}
		seen[u.slot] = true
		if u.slot.rgRemaining.Load() > 0 {
			u.slot.drainAbandoned(inner)
		}
	}
}

// drainBatchCharges releases every outstanding decoded-batch charge —
// batches stranded in batchCh by cancellation or never sent by an exiting
// worker. Callers must ensure the rg/scan workers have exited first (the
// charges live on a shared worker-level tracker; a racing Store here would
// leak its bytes for the worker's lifetime).
func (inner *scanSourceInner) drainBatchCharges() {
	if inner.memTracker == nil {
		return
	}
	inner.batchCharges.Range(func(k, _ any) bool {
		if n, ok := inner.batchCharges.LoadAndDelete(k); ok {
			inner.memTracker.Release(n.(int64))
		}
		return true
	})
}

// trackPooledBuf records a buffer obtained from readBufPool so it can be
// returned when the scan source is closed. Thread-safe for parallel readers.
func (inner *scanSourceInner) trackPooledBuf(buf []byte) {
	inner.pooledBufsMu.Lock()
	inner.pooledBufs = append(inner.pooledBufs, buf)
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		n := int64(cap(buf))
		inner.memTracker.ForceReserveFor(n, memory.ForceScanPooledBuffer)
		inner.trackedBufBytes.Add(n)
	}
}

// releasePooledBufs returns all tracked buffers to readBufPool.
// Safe to call multiple times — subsequent calls are no-ops.
func (inner *scanSourceInner) releasePooledBufs() {
	inner.pooledBufsMu.Lock()
	bufs := inner.pooledBufs
	inner.pooledBufs = nil
	inner.pooledBufsMu.Unlock()

	if inner.memTracker != nil {
		released := inner.trackedBufBytes.Swap(0)
		if released > 0 {
			inner.memTracker.ReleaseForced(released, memory.ForceScanPooledBuffer)
		}
	}

	// Nil out rgUnits to break pqFile → bytes.Reader → []byte reference
	// chain before returning buffers, so GC doesn't pin old data.
	inner.rgUnits = nil
	for _, buf := range bufs {
		putReadBuf(buf)
	}
}

// scanPredicate is a simple predicate for row-group stats pruning.
type scanPredicate struct {
	Column string
	Op     string
	Value  any
}

func (s *scannerExecSource) Init(ctx context.Context) error {
	manifest, err := getManifestWith(ctx, s.manifestSnapshot, s.catalog, s.tableName)
	if err != nil {
		return err
	}
	tableMeta, err := s.catalog.GetTable(ctx, s.tableName)
	if err != nil {
		return err
	}

	var files []catalog.FileEntry
	for _, p := range manifest.Partitions {
		// Prune partitions that don't match the filter
		if len(s.partitionFilter) > 0 && len(p.Values) > 0 {
			if !matchesPartitionFilter(p.Values, s.partitionFilter) {
				continue
			}
		}
		files = append(files, p.Files...)
	}

	// Probe-split: restrict to only allowed files for this scan alias.
	if len(s.allowedFiles) > 0 {
		allowed := make(map[string]bool, len(s.allowedFiles))
		for _, f := range s.allowedFiles {
			allowed[f] = true
		}
		filtered := files[:0]
		for _, f := range files {
			if allowed[f.Path] {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}

	scanCtx, cancel := context.WithCancel(ctx)
	// Convert logical predicates to scan predicates for row-group pruning.
	//
	// The literal is in the ENGINE's domain and the row group's statistics
	// and dictionary are in the FILE's, and for several types those are not
	// the same thing: a DATE is a day number against a text literal, a
	// DECIMAL's bounds are the unscaled integer against a float, an IPV6's
	// are the raw sixteen bytes against an address in text. The prune layer
	// compares two `any` values by their Go kind and cannot tell — so the
	// conversion happens HERE, the one place that still holds the column's
	// type and scale, and a predicate with no conversion is WITHHELD rather
	// than pushed down raw (#442, #438). kernel.StatsDomainValue is the same
	// conversion the filter kernel applies to the literal, so the prune and
	// the filter cannot disagree about what the predicate means.
	var sp []scanPredicate
	var eqProbes []scan.EqProbe
	for _, pred := range s.scanPreds {
		if pred.Column == "" || pred.Op == "" || pred.Value == nil {
			continue
		}
		// The predicate's column arrives as a REFERENCE — an unquoted
		// identifier folds to lower case at the lexer (#731) — while the
		// schema keeps the spelling the parquet file gave it, and CamelCase
		// column names are ordinary there (ClickBench's `hits` has
		// `EventDate`, `UserAgent`, `ResolutionWidth`). A byte-exact lookup
		// missed every column of every such table, so neither the row-group
		// statistics prune nor the dictionary probe was ever built for it:
		// the answer stayed right and the whole table was read. Resolve the
		// way the engine resolves every other reference, and carry the
		// SCHEMA's spelling forward — that name keys the row group's
		// per-column statistics (`scan.CanPruneRowGroup`) and matches the
		// file's own leaves (`scan.CanDictPruneRowGroup`), neither of which
		// has ever seen the folded spelling.
		ci := batch.ResolveSchemaIndex(tableMeta.Schema.Columns, pred.Column)
		if ci < 0 {
			continue
		}
		col := tableMeta.Schema.Columns[ci]
		// A DECIMAL bound is converted from the literal's TEXT: the float64
		// box has already dropped the digits past a double, and a bound that
		// is off by a fraction of the last place prunes the row group the
		// answer is in (#452).
		lit := pred.Value
		if col.Type == parquet.TypeDecimal && pred.ValueText != "" {
			lit = pred.ValueText
		}
		val, ok := kernel.StatsDomainValue(col.Type, int(col.Scale), lit)
		if !ok {
			continue
		}
		sp = append(sp, scanPredicate{Column: col.Name, Op: pred.Op, Value: val})
		// Equality conjuncts also feed the dictionary probe — the
		// precise prune where zonemaps are blind (point filters on
		// high-cardinality columns). Dictionary entries are raw file
		// values too, so they take the same converted literal.
		if pred.Op == "=" && scan.DictPrune.On() {
			// A DECIMAL probe's carrier is at the CATALOG scale; the file's
			// dictionary is at the file's own scale. Carry the catalog
			// declaration so the probe layer can reconcile the two before
			// comparing (dictProbeDecimalAbsent), the dictionary twin of the
			// stats-path reconcile (#707/#916). Non-DECIMAL probes leave these
			// zero and the probe layer never reads them.
			ep := scan.EqProbe{ColName: col.Name, Value: val}
			if col.Type == parquet.TypeDecimal {
				ep.Scale, ep.Precision = int(col.Scale), int(col.Precision)
			}
			eqProbes = append(eqProbes, ep)
		}
	}

	// Load delete markers for merge-on-read deletes
	var delMarkers map[string]map[int64]bool
	if len(manifest.DeleteMarkers) > 0 {
		delMarkers = make(map[string]map[int64]bool, len(manifest.DeleteMarkers))
		for _, dm := range manifest.DeleteMarkers {
			idxSet := make(map[int64]bool, len(dm.RowIndices))
			for _, idx := range dm.RowIndices {
				idxSet[idx] = true
			}
			delMarkers[dm.FilePath] = idxSet
		}
	}

	// Use a smaller batch channel for LIMIT queries to bound in-flight downloads.
	batchChSize := scanParallelism()
	if s.rowLimit > 0 {
		batchChSize = 2
	}

	inner := &scanSourceInner{
		cat:           s.catalog,
		tableName:     s.tableName,
		files:         files,
		schema:        tableMeta.Schema.Columns,
		requiredCols:  s.requiredCols,
		scanPreds:     sp,
		deleteMarkers: delMarkers,
		bloomFilter:   s.bloomFilter,
		dynamicFilter: s.dynamicFilter,
		rowLimit:      s.rowLimit,
		batchCh:       make(chan *batch.RecordBatch, batchChSize),
		errCh:         make(chan error, 1),
		cancel:        cancel,
		memTracker:    s.memTracker,
		spillMgr:      s.spillMgr,
		emitRowLoc:    s.emitRowLoc,
		eqProbes:      eqProbes,
		rowPreds:      s.rowPreds,
		shapeOnlyCols: s.shapeOnlyCols,
		countOnlyScan: len(s.requiredCols) == 1 && s.requiredCols[0] == logical.RowCountOnlyColumn,
	}
	// Nested (ARRAY/MAP/ROW) schemas must take the file-level scan whose
	// readBatchDirect falls back to the row-based reader. This MUST be
	// decided HERE: the eager branch only learned about nested types
	// inside buildRGUnits, which runs after the branch was already taken —
	// the early return left zero rgUnits and every query against a nested
	// table returned 0 rows with no error (issue #144 suite finding).
	// Decided on the columns this scan READS, matching readBatchDirect's own
	// test: one ARRAY/ROW/MAP column in a table used to put every query on
	// that table onto the row reader, which mints unpooled batches and reads
	// every column of every row group (#393).
	innerSchema := parquet.Schema{Columns: buildReadSchema(inner.schema, inner.requiredCols)}
	inner.hasNestedTypes = innerSchema.HasNestedColumns()
	s.scanner = inner

	// Row-loc stamping needs the row-group-parallel path: rgUnit ordinals
	// are the row identity. The planner's rewrite only engages on shapes
	// that take the eager branch; this is the belt-and-braces check.
	if inner.emitRowLoc && (inner.rowLimit > 0 || inner.hasNestedTypes) {
		cancel()
		return fmt.Errorf("scan %s: row-loc emission requires the row-group-parallel scan path", s.tableName)
	}
	// Same for pushed scan filters: the lazy/nested path never evaluates
	// them, and a silently dropped filter is wrong results. The planner
	// gates both conditions; fail loudly if they ever meet anyway.
	if len(inner.rowPreds) > 0 && (inner.rowLimit > 0 || inner.hasNestedTypes) {
		cancel()
		return fmt.Errorf("scan %s: pushed scan filters require the row-group-parallel scan path", s.tableName)
	}

	if inner.rowLimit > 0 || inner.hasNestedTypes {
		// Lazy file-level scan: download files on-demand, one at a time per worker.
		// Used for LIMIT pushdown (avoids downloading all files upfront) and
		// nested types (which need row-level reading).
		// Workers stop when context is cancelled (pipeline cancels after LIMIT satisfied).
		workers := scanParallelism()
		if workers > len(files) {
			workers = len(files)
		}
		if workers < 1 {
			workers = 1
		}
		inner.wg.Add(workers)
		for i := 0; i < workers; i++ {
			go inner.scanWorker(scanCtx)
		}
	} else {
		// Eager row-group-level parallel scan: download all files, enumerate
		// row groups, apply predicate pruning, then process RGs in parallel.
		inner.buildRGUnits(scanCtx)

		// A panic on a footer reader is fatal to the scan regardless of how
		// many other files parsed, because the rows it would have
		// contributed are simply missing from the answer.
		if inner.fatalScanErr != nil {
			cancel()
			return fmt.Errorf("scan %s: %w", s.tableName, inner.fatalScanErr)
		}

		// Fail the scan if all files failed to read — prevents silent 0-row
		// results that are indistinguishable from correct empty results.
		if inner.failedFiles > 0 && len(inner.rgUnits) == 0 && len(inner.files) > 0 {
			cancel()
			sampleErr := ""
			if inner.firstFileErr != nil {
				sampleErr = fmt.Sprintf(": %v", inner.firstFileErr)
			}
			return fmt.Errorf("scan %s: all %d files failed to read (%d failures)%s", s.tableName, len(inner.files), inner.failedFiles, sampleErr)
		}

		// Initialize batch pool from the LARGEST row group: GetForSize
		// falls back to a fresh unpooled allocation for any request above
		// the pool's batch size, so sizing from rgUnits[0] meant every
		// row group bigger than the first bypassed the pool entirely —
		// full vector allocation + zeroing per row group (13% of the
		// 100-part floor probe's makeslice profile).
		if len(inner.rgUnits) > 0 {
			rgSize := 0
			for _, u := range inner.rgUnits {
				if int(u.numRows) > rgSize {
					rgSize = int(u.numRows)
				}
			}
			readSchema := inner.readSchema()
			// Row-loc stamping appends a column after decode, which would
			// poison the fixed-schema pool on release — skip pooling (the
			// narrow late-mat scan allocates little anyway).
			if rgSize > 0 && len(readSchema) > 0 && !inner.emitRowLoc {
				inner.pool = batch.NewBatchPool(readSchema, rgSize)
				inner.pool.PreWarm(runtime.NumCPU())
			}
			// Pre-compute whether native page decoding can be used.
			inner.useNative = !scan.HasUnsupportedColumnarTypes(readSchema)
		}

		// Byte-aware decode parallelism: CPU-count workers with a CPU-count
		// queue are blind to batch WIDTH. On a 105-column SELECT * scan each
		// decoded row-group batch is hundreds of MB; 16 decoders + 16 queued
		// batches held multiple GB of live wide batches (plus the GC target
		// doubling that live set), which OOM-killed the c6a on ClickBench
		// Q24 even with the sort side bounded. Clamp workers + queue so
		// estimated in-flight decoded bytes stay within a budget slice;
		// narrow scans (TPC-H) still get full CPU-count parallelism.
		workers := scanParallelism()
		if len(inner.rgUnits) > 0 {
			rgRows := int(inner.rgUnits[0].numRows)
			perBatch := estimateDecodedBatchBytes(inner.readSchema(), rgRows)
			inflightCap := int64(8 << 30)
			if inner.memTracker != nil {
				if b := inner.memTracker.Budget(); b > 0 && b/3 < inflightCap {
					inflightCap = b / 3
				}
			}
			if perBatch > 0 {
				maxInflight := int(inflightCap / perBatch)
				if maxInflight < 2 {
					maxInflight = 2
				}
				if workers > maxInflight-1 {
					workers = maxInflight - 1
				}
				queue := maxInflight - workers
				if queue < 1 {
					queue = 1
				}
				if queue < cap(inner.batchCh) {
					inner.batchCh = make(chan *batch.RecordBatch, queue)
				}
			}
		}
		if workers > len(inner.rgUnits) {
			workers = len(inner.rgUnits)
		}
		if workers < 1 {
			workers = 1
		}
		inner.wg.Add(workers)
		for i := 0; i < workers; i++ {
			go inner.rgWorker(scanCtx)
		}
	}

	// Close batchCh when all workers are done
	go func() {
		defer inner.recoverWorkerPanic(ctx, "scan batch-channel closer")
		inner.wg.Wait()
		close(inner.batchCh)
	}()

	return nil
}

// estimateDecodedBatchBytes estimates the decoded in-memory size of one
// row-group batch for the projected schema. Fixed types use their storage
// width; variable-length types assume 48 B/row — a deliberate overestimate
// for short strings (safe direction: it only reduces decode parallelism).
func estimateDecodedBatchBytes(schema []parquet.Column, rows int) int64 {
	if rows <= 0 {
		return 0
	}
	perRow := 0
	for _, c := range schema {
		switch c.Type {
		case parquet.TypeBool:
			perRow += 1
		case parquet.TypeInt32, parquet.TypeDate, parquet.TypePort, parquet.TypeProtocol, parquet.TypeFloat32:
			perRow += 4
		case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeFloat64, parquet.TypeIPv4,
			parquet.TypeMAC, parquet.TypeDuration:
			perRow += 8
		case parquet.TypeDecimal, parquet.TypeUUID, parquet.TypeIPv6:
			perRow += 16
		default: // strings/bytes/nested
			perRow += 48
		}
	}
	return int64(perRow) * int64(rows)
}

// matchesPartitionFilter returns true if all filter keys match the partition values.
func matchesPartitionFilter(partValues, filter map[string]string) bool {
	for k, v := range filter {
		pv, ok := partValues[k]
		if !ok {
			continue // partition doesn't have this key, skip
		}
		if pv != v {
			return false
		}
	}
	return true
}

func (s *scannerExecSource) Next(ctx context.Context) (*batch.RecordBatch, error) {
	return s.scanner.next(ctx)
}

func (s *scannerExecSource) Close() error {
	if s.scanner != nil {
		if s.scanner.cancel != nil {
			s.scanner.cancel()
		}
		// Wait for the scan workers to exit before draining: a worker racing
		// drainBatchCharges could charge a batch after the drain, leaking the
		// bytes on the shared worker-level tracker for the worker's lifetime.
		// Workers observe the cancel at the loop head and in every blocking
		// select, so this wait is bounded by one in-flight row-group decode.
		s.scanner.wg.Wait()
		s.scanner.drainBatchCharges()
		s.scanner.drainSlotCharges()
		s.scanner.releasePooledBufs()
	}
	return nil
}

func (s *scannerExecSource) RowsScanned() int64 {
	if s.scanner != nil {
		return atomic.LoadInt64(&s.scanner.rowsScanned)
	}
	return 0
}

// recoverWorkerPanic converts a panic raised on a scan goroutine into the
// scan's error instead of letting it take the process down.
//
// These goroutines are not the caller's: Pipeline.Run recovers on ITS
// goroutine, so a *batch.TypeMismatchError raised by Vector.SetValue — whose
// whole design (#361) is "a query error, never the server" — killed the
// process here, and with it every other client's query (#400, and #393 as the
// query that reaches it). Since #511 it converts ANY panic, not only the
// FatalEvalPanic class: a decoder bug on a scan worker is still one query's
// failure, not the server's.
//
// errCh is buffered, and next() selects on it, so a non-blocking send is
// enough; the cancel stops the sibling workers.
func (inner *scanSourceInner) recoverWorkerPanic(ctx context.Context, what string) {
	r := recover()
	if r == nil {
		return
	}
	err := exec.RecoverQueryPanic(ctx, what, r)
	select {
	case inner.errCh <- fmt.Errorf("%s: %w", what, err):
	default:
	}
	if inner.cancel != nil {
		inner.cancel()
	}
}

// scanWorker reads files in parallel, writing decoded batches to batchCh.
func (inner *scanSourceInner) scanWorker(ctx context.Context) {
	defer inner.wg.Done()
	defer inner.recoverWorkerPanic(ctx, "scan worker")

	for {
		idx := int(atomic.AddInt64(&inner.idx, 1) - 1)
		if idx >= len(inner.files) {
			return
		}
		if ctx.Err() != nil {
			return
		}

		file := inner.files[idx]
		var reader *parquet.Reader
		if ras, ok := inner.cat.Store().(objstore.ReaderAtStore); ok {
			rac, size, err := ras.GetReaderAt(ctx, inner.cat.Bucket(), file.Path)
			if err != nil {
				continue
			}
			reader, err = parquet.NewReader(rac, size)
			if err != nil {
				rac.Close()
				continue
			}
		} else {
			rc, _, err := inner.cat.Store().Get(ctx, inner.cat.Bucket(), file.Path)
			if err != nil {
				continue
			}
			data, err := readAllSized(rc, file.SizeBytes, true)
			rc.Close()
			if err != nil {
				continue
			}
			inner.trackPooledBuf(data)
			reader, err = parquet.NewReaderFromBytesCached(data,
				footerCacheIdentity(inner.cat, file, int64(len(data))))
			if err != nil {
				continue
			}
		}

		b, err := readBatchDirect(reader, inner.schema, inner.requiredCols, inner.scanPreds...)
		if err != nil {
			// Surface the first decode error so the scan FAILS instead of
			// dropping this file's rows: a swallowed error here is
			// indistinguishable from a file that legitimately contributed
			// nothing (the same silent-partial class readRG guards).
			select {
			case inner.errCh <- fmt.Errorf("reading %s: %w", file.Path, err):
			default:
			}
			return
		}
		if b == nil || b.Len == 0 {
			continue
		}
		inner.trackScanBatch(b)

		// Apply delete markers: skip rows marked for deletion
		if delSet := inner.deleteMarkers[file.Path]; len(delSet) > 0 {
			sel := make([]uint32, 0, b.Len)
			for i := 0; i < b.Len; i++ {
				if !delSet[int64(i)] {
					sel = append(sel, uint32(i))
				}
			}
			if len(sel) == 0 {
				inner.releaseScanBatch(b)
				continue
			}
			if len(sel) < b.Len {
				b.Sel = sel
			}
		}

		atomic.AddInt64(&inner.rowsScanned, int64(b.ActiveLen()))

		select {
		case inner.batchCh <- b:
		case <-ctx.Done():
			return
		}
	}
}

// readSchema returns the column-projected schema for this scan.
// Multiple rgWorker goroutines call this concurrently for the same source,
// so the cache is guarded by sync.Once to avoid a data race on the
// cachedReadSchema field.
func (inner *scanSourceInner) readSchema() []parquet.Column {
	inner.cachedReadSchemaOnce.Do(func() {
		inner.cachedReadSchema = buildReadSchema(inner.schema, inner.requiredCols)
	})
	return inner.cachedReadSchema
}

func (inner *scanSourceInner) next(ctx context.Context) (*batch.RecordBatch, error) {
	select {
	case b, ok := <-inner.batchCh:
		if !ok {
			// Channel closed, check for errors
			select {
			case err := <-inner.errCh:
				return nil, err
			default:
				return nil, nil
			}
		}
		// The batch leaves the scan source here — downstream operators that
		// retain it account for it themselves (TrackBatch et al).
		inner.releaseScanBatch(b)
		return b, nil
	case err := <-inner.errCh:
		return nil, err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
