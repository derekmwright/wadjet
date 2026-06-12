package exec

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// WindowFunc identifies a window function type.
type WindowFunc int

const (
	WinRowNumber WindowFunc = iota
	WinRank
	WinDenseRank
	WinSum
	WinCount
	WinAvg
	WinMin
	WinMax
	WinLag
	WinLead
	WinFirstValue
	WinLastValue
	WinNtile
	WinPercentRank
	WinCumeDist
	WinNthValue
)

// WindowFrameSpec describes a window frame specification for execution.
type WindowFrameSpec struct {
	Mode  string // "rows" or "range"
	Start WindowBound
	End   WindowBound
}

// WindowBound describes one end of a window frame.
type WindowBound struct {
	Type   string // "unbounded_preceding", "preceding", "current_row", "following", "unbounded_following"
	Offset int
}

// WindowColumn defines a window function computation.
type WindowColumn struct {
	Func           WindowFunc
	InputCol       string // for aggregate window functions (empty for ranking funcs)
	OutputCol      string
	OutputType     parquet.TypeID
	PartitionBy    []string
	OrderBy        []SortKey
	Frame          *WindowFrameSpec // optional frame specification
	LagLeadOffset  int              // offset for LAG/LEAD (default 1)
	LagLeadDefault any              // default value for LAG/LEAD (default NULL)
	NtileBuckets   int              // number of buckets for NTILE
	NthValueN      int              // N for NTH_VALUE (1-based)
}

// Window is a SinkSource that collects all rows, partitions and sorts them,
// computes window function values, and emits the original rows with computed
// window columns appended. Operates directly on column vectors to avoid
// map[string]any materialization overhead.
// When a SpillManager is set, Window will spill input batches to disk under
// memory pressure and read them back during Finalize.
type Window struct {
	Columns []WindowColumn
	Spill   *memory.SpillManager // optional: enables spill-to-disk

	mu         sync.Mutex
	batches    []*batch.RecordBatch
	totalRows  int
	trackedMem int64 // memory reserved from shared tracker by this operator
	schema     []parquet.Column
	spillFiles []string // legacy row-oriented spill (schemas the columnar run format can't carry)
	runFiles   []string // sorted columnar runs (external partition-at-a-time path)
	groups     []windowSpecGroup
	ext        *windowExtState // final-pass streaming state, drained by Next

	result  []*batch.RecordBatch
	pos     int
	emitted bool

	// AccountedOperator (Phase 2) state — see HashAggregate for the contract.
	// Window registers at Init (not first-Consume) because it self-spills
	// during Consume, so it must be a relief candidate from the moment input
	// can arrive. finalized gates SpillableBytes to 0 once finalize runs.
	accInstanceID       uint64
	accState            atomic.Int32
	unregisterAccounted func()
	finalized           bool
}

// NewWindow creates a new window operator.
func NewWindow(cols []WindowColumn) *Window {
	return &Window{Columns: cols}
}

func (w *Window) Init(_ context.Context) error {
	w.batches = nil
	w.totalRows = 0
	w.result = nil
	w.pos = 0
	w.emitted = false
	w.finalized = false
	w.groups = groupWindowSpecs(w.Columns)
	w.ext = nil
	// Register with the relief registry up front: Window self-spills during
	// Consume, so it must be visible to RequestRelief before any batch arrives.
	// A zero footprint at registration is fine (Inspect reports OwnedBytes==0).
	if w.Spill != nil && w.unregisterAccounted == nil {
		w.accInstanceID = memory.NextInstanceID()
		w.accState.Store(int32(memory.OpActive))
		w.unregisterAccounted = w.Spill.RegisterAccounted(w)
	}
	return nil
}

func (w *Window) Consume(_ context.Context, b *batch.RecordBatch) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.schema == nil {
		w.schema = b.Schema
	}
	b.Detach() // prevent pool recycle — pipeline calls Release() after Consume()
	w.batches = append(w.batches, b)
	w.totalRows += b.ActiveLen()

	// Track memory usage for spill pressure detection
	if w.Spill != nil {
		cost := b.MemBytes()
		w.Spill.TrackBatch(cost)
		w.trackedMem += cost
		if w.accInstanceID != 0 {
			w.Spill.Tracker().PublishOwned(w.accInstanceID, w.trackedMem)
		}
	}

	// Spill to disk if memory pressure is high. The columnar run path
	// accumulates at least minSortRunBytes before flushing (merge-friendly
	// runs); the legacy row path keeps the old flush-on-pressure behavior.
	// Peer relief (SpillSome) bypasses the floor.
	if w.Spill != nil && w.Spill.ShouldSpillFor(memory.SpillCheap) && len(w.batches) > 0 {
		if !w.useColumnarRuns() || w.trackedMem >= minSortRunBytes {
			if _, err := w.flushSpillLocked(); err != nil {
				return err
			}
		}
	}
	return nil
}

// useColumnarRuns reports whether spill goes through the external
// partition-at-a-time path. Every column type round-trips the columnar run
// format (nested ARRAY/MAP/ROW included); the only remaining requirement is
// a window spec to sort runs by — the degenerate no-spec case keeps the
// legacy row spill.
func (w *Window) useColumnarRuns() bool {
	return len(w.groups) > 0
}

// flushSpillLocked drains all buffered batches to disk and releases their
// tracking, returning the bytes freed. The external path writes a run sorted
// by the first spec group's (PARTITION BY, ORDER BY); the fallback writes the
// legacy row-oriented file. Caller holds w.mu.
func (w *Window) flushSpillLocked() (int64, error) {
	if len(w.batches) == 0 || w.trackedMem == 0 {
		return 0, nil
	}
	if w.useColumnarRuns() {
		path, err := sortBatchesToRun(w.Spill.SpillDir(), w.schema, w.batches, w.totalRows, w.groups[0].sortKeys, 0)
		if err != nil {
			return 0, err
		}
		if path != "" {
			w.runFiles = append(w.runFiles, path)
		}
	} else {
		var rows []map[string]any
		for _, sb := range w.batches {
			rows = append(rows, sb.ToRows()...)
		}
		path, err := w.Spill.SpillRows(rows)
		if err != nil {
			return 0, err
		}
		w.spillFiles = append(w.spillFiles, path)
	}
	w.batches = w.batches[:0]
	w.totalRows = 0
	freed := w.trackedMem
	w.Spill.ReleaseTracking(freed)
	w.trackedMem = 0
	if w.accInstanceID != 0 {
		w.Spill.Tracker().PublishOwned(w.accInstanceID, 0)
	}
	return freed, nil
}

func (w *Window) Finalize(_ context.Context) error {
	w.mu.Lock()
	w.finalized = true
	w.mu.Unlock()
	if w.unregisterAccounted != nil {
		w.accState.Store(int32(memory.OpClosed))
		w.unregisterAccounted()
		w.unregisterAccounted = nil
	}
	if len(w.spillFiles) > 0 {
		return w.finalizeWithSpill()
	}
	if len(w.runFiles) > 0 {
		return w.finalizeExternal()
	}
	return w.finalizeColumnar()
}

// finalizeExternal prepares the partition-at-a-time stream over sorted runs.
// Non-final spec groups run disk-to-disk passes here (each bounded by the
// largest partition plus the merge fan-in); the final group streams through
// Next(), so finalize never materializes the dataset.
func (w *Window) finalizeExternal() error {
	// The in-memory remainder joins the runs as one more sorted run; the
	// multi-pass flow then has a single uniform input.
	w.mu.Lock()
	_, ferr := w.flushSpillLocked()
	runs := w.runFiles
	w.runFiles = nil
	w.mu.Unlock()
	if ferr != nil {
		return ferr
	}
	dir := w.Spill.SpillDir()
	passSchema := w.schema

	// charge surfaces per-partition accumulation to the tracker so a
	// pathologically large partition is visible to the pressure breaker.
	charge := func(delta int64) {
		if delta >= 0 {
			w.Spill.TrackBatch(delta)
		} else {
			w.Spill.ReleaseTracking(-delta)
		}
	}

	var err error
	for gi := 0; gi < len(w.groups)-1; gi++ {
		if gi > 0 {
			runs, err = resortRunsByKeys(dir, passSchema, runs, w.groups[gi].sortKeys)
			if err != nil {
				return err
			}
		}
		runs, passSchema, err = windowDiskPass(dir, passSchema, runs, w.groups[gi], charge)
		if err != nil {
			removeRunFiles(runs)
			return err
		}
	}

	last := w.groups[len(w.groups)-1]
	if len(w.groups) > 1 {
		runs, err = resortRunsByKeys(dir, passSchema, runs, last.sortKeys)
		if err != nil {
			return err
		}
	}
	if len(last.partitionBy) == 0 {
		// Final group with no PARTITION BY: two-pass streaming instead of
		// accumulating the whole input as one partition (window_global.go).
		merger, runs, err := openRunMerger(dir, passSchema, last.sortKeys, runs)
		if err != nil {
			removeRunFiles(runs)
			return err
		}
		stats, err := collectGlobalWindowStats(merger, passSchema, last)
		merger.close()
		if err != nil {
			removeRunFiles(runs)
			return err
		}
		merger2, runs2, err := openRunMerger(dir, passSchema, last.sortKeys, runs)
		if err != nil {
			removeRunFiles(runs)
			return err
		}
		w.ext = &windowExtState{
			global:    newGlobalWindowStreamer(merger2, passSchema, last, stats, charge),
			merger:    merger2,
			schema:    passSchema,
			group:     last,
			reorder:   buildWindowReorder(len(w.schema), w.groups, len(w.Columns)),
			outSchema: w.buildOutputSchema(),
			runs:      runs2,
		}
		return nil
	}
	merger, runs, err := openRunMerger(dir, passSchema, last.sortKeys, runs)
	if err != nil {
		removeRunFiles(runs)
		return err
	}
	w.ext = &windowExtState{
		walker:    newPartitionWalker(merger, last.partitionBy, charge),
		merger:    merger,
		schema:    passSchema,
		group:     last,
		reorder:   buildWindowReorder(len(w.schema), w.groups, len(w.Columns)),
		outSchema: w.buildOutputSchema(),
		runs:      runs,
	}
	return nil
}

// Inspect implements memory.AccountedOperator.
func (w *Window) Inspect() memory.OperatorFootprint {
	w.mu.Lock()
	defer w.mu.Unlock()
	st := memory.OpState(w.accState.Load())
	if st == memory.OpClosed {
		return memory.OperatorFootprint{State: memory.OpClosed, InstanceID: w.accInstanceID, Name: "Window"}
	}
	return memory.OperatorFootprint{
		OwnedBytes:     w.trackedMem,
		RetainedBytes:  w.trackedMem,
		SpillableBytes: w.spillableBytesLocked(),
		State:          st,
		InstanceID:     w.accInstanceID,
		Name:           "Window",
	}
}

// spillableBytesLocked: Window's input batches are raw-row spillable throughout
// the collect/sort phase (its most memory-intensive phase). Once finalize runs
// the batches are consumed, so it reports 0. Caller holds w.mu.
func (w *Window) spillableBytesLocked() int64 {
	if w.Spill == nil || w.finalized || w.trackedMem == 0 || len(w.batches) == 0 {
		return 0
	}
	return w.trackedMem
}

// EstimateRelief implements memory.AccountedOperator (all-or-nothing).
func (w *Window) EstimateRelief(target int64) int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	if target <= 0 {
		return 0
	}
	return w.spillableBytesLocked()
}

// SpillSome implements memory.AccountedOperator: drains all buffered input
// batches to disk and releases the freed bytes — a sorted columnar run on
// the external path, a raw-row file on the nested-type fallback.
func (w *Window) SpillSome(_ int64) (int64, error) {
	w.accState.Store(int32(memory.OpSpilling))
	defer w.accState.CompareAndSwap(int32(memory.OpSpilling), int32(memory.OpActive))
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.Spill == nil || w.finalized {
		return 0, nil
	}
	return w.flushSpillLocked()
}

// finalizeColumnar is the fast path when no spill occurred — operates entirely
// on column vectors without row materialization.
func (w *Window) finalizeColumnar() error {
	allBatches := w.batches
	w.batches = nil

	combined := windowConcatBatches(allBatches, w.schema)
	if combined == nil || combined.Len == 0 {
		return nil
	}

	outSchema := w.buildOutputSchema()

	for _, wc := range w.Columns {
		vec := batch.NewVector(wc.OutputType, combined.Len)
		vec.Nulls = batch.NewBitmapAllNull(combined.Len)
		combined.Columns = append(combined.Columns, vec)
		combined.Schema = append(combined.Schema, parquet.Column{
			Name:     wc.OutputCol,
			Type:     wc.OutputType,
			Nullable: true,
		})
	}

	numOrigCols := len(w.schema)
	for i, wc := range w.Columns {
		computeWindowColumnar(combined, numOrigCols+i, wc)
	}

	for pos := 0; pos < combined.Len; {
		end := pos + batch.DefaultBatchSize
		if end > combined.Len {
			end = combined.Len
		}
		batchLen := end - pos
		out := batch.NewRecordBatch(outSchema, batchLen)
		for j := range outSchema {
			windowCopyVectorRange(out.Columns[j], combined.Columns[j], 0, pos, batchLen)
		}
		w.result = append(w.result, out)
		pos = end
	}
	return nil
}

// finalizeWithSpill handles the spill path — reads spilled rows and computes
// window functions in row-oriented mode to avoid materializing a giant combined
// columnar batch (which would defeat the purpose of spilling).
func (w *Window) finalizeWithSpill() error {
	// Collect all data as rows — in-memory batches + spill files
	var allRows []map[string]any
	for _, b := range w.batches {
		allRows = append(allRows, b.ToRows()...)
	}
	w.batches = nil

	for _, f := range w.spillFiles {
		spilled, err := memory.ReadSpilledRows(f)
		if err != nil {
			return err
		}
		allRows = append(allRows, spilled...)
	}
	w.spillFiles = nil

	if len(allRows) == 0 {
		return nil
	}

	// Initialize window output columns in each row
	for _, row := range allRows {
		for _, wc := range w.Columns {
			row[wc.OutputCol] = nil
		}
	}

	// Compute each window function on the row slice
	for _, wc := range w.Columns {
		computeWindowRowOriented(allRows, wc)
	}

	// Materialize into output batches
	outSchema := w.buildOutputSchema()
	for pos := 0; pos < len(allRows); {
		end := pos + batch.DefaultBatchSize
		if end > len(allRows) {
			end = len(allRows)
		}
		w.result = append(w.result, batch.FromRows(outSchema, allRows[pos:end]))
		pos = end
	}
	return nil
}

func (w *Window) buildOutputSchema() []parquet.Column {
	outSchema := make([]parquet.Column, len(w.schema))
	copy(outSchema, w.schema)
	for _, wc := range w.Columns {
		outSchema = append(outSchema, parquet.Column{
			Name:     wc.OutputCol,
			Type:     wc.OutputType,
			Nullable: true,
		})
	}
	return outSchema
}

// Close releases any tracker reservation Window still holds for buffered rows
// that never crossed the spill threshold, and unregisters from the relief
// registry. The previous no-op leaked a phantom reservation for the lifetime
// of the process when a Window accumulated but never spilled and Finalize was
// not reached (early cancel).
func (w *Window) Close() error {
	w.mu.Lock()
	if w.ext != nil {
		// Early cancel mid-stream: close cursors and delete run scratch.
		w.ext.cleanup()
		w.ext = nil
	}
	removeRunFiles(w.runFiles)
	w.runFiles = nil
	if w.Spill != nil && w.trackedMem > 0 {
		w.Spill.ReleaseTracking(w.trackedMem)
		w.trackedMem = 0
	}
	w.mu.Unlock()
	if w.unregisterAccounted != nil {
		w.accState.Store(int32(memory.OpClosed))
		w.unregisterAccounted()
		w.unregisterAccounted = nil
	}
	return nil
}

// Next returns windowed results in batches. On the external path it streams
// one partition at a time from the final pass's merge.
func (w *Window) Next(_ context.Context) (*batch.RecordBatch, error) {
	if w.ext != nil {
		return w.nextExternal()
	}
	if w.pos >= len(w.result) {
		return nil, nil
	}
	b := w.result[w.pos]
	w.pos++
	return b, nil
}

// nextExternal drains the chunk queue, refilling it one computed partition at
// a time; at stream EOF it tears down cursors and deletes run scratch.
func (w *Window) nextExternal() (*batch.RecordBatch, error) {
	e := w.ext
	for {
		if e.qpos < len(e.queue) {
			b := e.queue[e.qpos]
			e.qpos++
			return b, nil
		}
		if e.done {
			return nil, nil
		}
		if e.global != nil {
			b, err := e.global.Next()
			if err != nil {
				return nil, err
			}
			if b == nil {
				e.cleanup()
				return nil, nil
			}
			out := reorderBatchColumns(b, e.reorder, e.outSchema)
			e.queue = chunkBatch(out, batch.DefaultBatchSize)
			e.qpos = 0
			continue
		}
		parts, bytes, err := e.walker.nextPartition()
		if err != nil {
			return nil, err
		}
		if parts == nil {
			e.cleanup()
			return nil, nil
		}
		combined := computeWindowPartition(parts, e.schema, e.group)
		e.walker.releasePartition(bytes)
		if combined == nil {
			continue
		}
		out := reorderBatchColumns(combined, e.reorder, e.outSchema)
		e.queue = chunkBatch(out, batch.DefaultBatchSize)
		e.qpos = 0
	}
}

// --- Columnar helpers ---

// windowConcatBatches combines multiple RecordBatches into a single batch.
// Selection vectors are applied before concatenation.
func windowConcatBatches(batches []*batch.RecordBatch, schema []parquet.Column) *batch.RecordBatch {
	totalRows := 0
	for _, b := range batches {
		totalRows += b.ActiveLen()
	}
	if totalRows == 0 {
		return nil
	}
	combined := batch.NewRecordBatch(schema, totalRows)
	pos := 0
	for _, b := range batches {
		cb := b.Compact()
		n := cb.Len
		for j := range schema {
			windowCopyVectorRange(combined.Columns[j], cb.Columns[j], pos, 0, n)
		}
		pos += n
	}
	return combined
}

// windowCopyVectorRange copies count values from src[srcOff..] to dst[dstOff..].
// Uses native slice copy for fixed-width types.
func windowCopyVectorRange(dst, src *batch.Vector, dstOff, srcOff, count int) {
	// Copy null bitmap
	for i := 0; i < count; i++ {
		if src.Nulls.IsNullFast(srcOff + i) {
			dst.Nulls.SetNull(dstOff + i)
		}
	}
	switch dst.Type {
	case batch.TypeBool:
		copy(dst.BoolData[dstOff:dstOff+count], src.BoolData[srcOff:srcOff+count])
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		copy(dst.Int32Data[dstOff:dstOff+count], src.Int32Data[srcOff:srcOff+count])
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		copy(dst.Int64Data[dstOff:dstOff+count], src.Int64Data[srcOff:srcOff+count])
	case batch.TypeFloat32:
		copy(dst.Float32Data[dstOff:dstOff+count], src.Float32Data[srcOff:srcOff+count])
	case batch.TypeFloat64:
		copy(dst.Float64Data[dstOff:dstOff+count], src.Float64Data[srcOff:srcOff+count])
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		if !src.Nulls.HasNulls() {
			dst.BytesData.BulkCopy(dstOff, &src.BytesData, srcOff, count)
		} else {
			for i := 0; i < count; i++ {
				if src.Nulls.IsNullFast(srcOff + i) {
					// BytesColumn writes must be sequential: a null row still
					// needs its offset slot advanced or every later row in
					// this column reads back as concatenated garbage.
					dst.BytesData.Set(dstOff+i, nil)
				} else {
					dst.BytesData.SetFrom(dstOff+i, &src.BytesData, srcOff+i)
				}
			}
		}
	case batch.TypeDecimal:
		copy(dst.DecimalData.Data[dstOff:dstOff+count], src.DecimalData.Data[srcOff:srcOff+count])
	default:
		// Typed per-value copy; handles VECTOR and nested ARRAY/MAP/ROW
		// (sequential-dst contract holds at every call site — concat and
		// chunk assembly write ranges in order).
		for i := 0; i < count; i++ {
			copyVectorValue(dst, dstOff+i, src, srcOff+i)
		}
	}
}

// windowGatherBatch creates a new batch by gathering rows according to permutation.
func windowGatherBatch(src *batch.RecordBatch, perm []int) *batch.RecordBatch {
	n := len(perm)
	dst := batch.NewRecordBatch(src.Schema, n)
	for j := range src.Schema {
		windowGatherVector(dst.Columns[j], src.Columns[j], perm)
	}
	return dst
}

// windowGatherVector reorders a vector according to a permutation array.
func windowGatherVector(dst, src *batch.Vector, perm []int) {
	switch dst.Type {
	case batch.TypeBool:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.BoolData[i] = src.BoolData[p]
			}
		}
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Int32Data[i] = src.Int32Data[p]
			}
		}
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Int64Data[i] = src.Int64Data[p]
			}
		}
	case batch.TypeFloat32:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Float32Data[i] = src.Float32Data[p]
			}
		}
	case batch.TypeFloat64:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.Float64Data[i] = src.Float64Data[p]
			}
		}
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
				dst.BytesData.Set(i, nil)
			} else {
				dst.BytesData.SetFrom(i, &src.BytesData, p)
			}
		}
	case batch.TypeDecimal:
		for i, p := range perm {
			if src.Nulls.IsNullFast(p) {
				dst.Nulls.SetNull(i)
			} else {
				dst.DecimalData.Data[i] = src.DecimalData.Data[p]
			}
		}
	default:
		// Typed per-value copy; handles VECTOR and nested ARRAY/MAP/ROW
		// (perm gathers write dst sequentially).
		for i, p := range perm {
			copyVectorValue(dst, i, src, p)
		}
	}
}

// compareVectorValues compares two values in a vector without boxing.
// Returns -1, 0, or 1.
func compareVectorValues(col *batch.Vector, a, b int) int {
	switch col.Type {
	case batch.TypeInt64, batch.TypeTimestamp, batch.TypeIPv4, batch.TypeMAC, batch.TypeDuration:
		va, vb := col.Int64Data[a], col.Int64Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeFloat64:
		va, vb := col.Float64Data[a], col.Float64Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeInt32, batch.TypePort, batch.TypeProtocol, batch.TypeDate:
		va, vb := col.Int32Data[a], col.Int32Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeFloat32:
		va, vb := col.Float32Data[a], col.Float32Data[b]
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeBool:
		va, vb := col.BoolData[a], col.BoolData[b]
		if !va && vb {
			return -1
		}
		if va && !vb {
			return 1
		}
		return 0
	case batch.TypeString:
		va := col.BytesData.UnsafeStringValue(a)
		vb := col.BytesData.UnsafeStringValue(b)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	default:
		return compareAny(col.GetValue(a), col.GetValue(b))
	}
}

// vecFloat64 reads a float64 value from any numeric vector without boxing.
func vecFloat64(v *batch.Vector, i int) float64 {
	if v == nil || v.Nulls.IsNullFast(i) {
		return 0
	}
	switch v.Type {
	case batch.TypeFloat64:
		return v.Float64Data[i]
	case batch.TypeFloat32:
		return float64(v.Float32Data[i])
	case batch.TypeInt64, batch.TypeTimestamp:
		return float64(v.Int64Data[i])
	case batch.TypeInt32:
		return float64(v.Int32Data[i])
	default:
		return 0
	}
}

// sameColumnar returns true if two rows have equal values for the given column indices.
func sameColumnar(combined *batch.RecordBatch, a, b int, colIdxs []int) bool {
	for _, idx := range colIdxs {
		if idx < 0 {
			continue
		}
		col := combined.Columns[idx]
		aNil := col.Nulls.IsNullFast(a)
		bNil := col.Nulls.IsNullFast(b)
		if aNil != bNil {
			return false
		}
		if aNil {
			continue // both null, considered equal
		}
		if compareVectorValues(col, a, b) != 0 {
			return false
		}
	}
	return true
}

// --- Columnar window computation ---

// computeWindowColumnar computes a single window function over columnar data.
// It sorts the combined batch in-place by the window's partition/order keys,
// then walks partitions and computes values directly on column vectors.
func computeWindowColumnar(combined *batch.RecordBatch, winVecIdx int, wc WindowColumn) {
	n := combined.Len
	if n == 0 {
		return
	}

	// Build sort keys: partition columns first, then order columns
	var sortKeys []SortKey
	for _, pc := range wc.PartitionBy {
		sortKeys = append(sortKeys, SortKey{Column: pc, Order: Ascending})
	}
	sortKeys = append(sortKeys, wc.OrderBy...)

	if len(sortKeys) > 0 {
		// Resolve column indices for sort
		sortKeyIdxs := make([]int, len(sortKeys))
		for i, key := range sortKeys {
			sortKeyIdxs[i] = combined.ColumnIndex(key.Column)
		}

		// Build and sort permutation
		perm := make([]int, n)
		for i := range perm {
			perm[i] = i
		}
		sort.SliceStable(perm, func(a, b int) bool {
			for ki, key := range sortKeys {
				idx := sortKeyIdxs[ki]
				if idx < 0 {
					continue
				}
				col := combined.Columns[idx]
				aNil := col.Nulls.IsNullFast(perm[a])
				bNil := col.Nulls.IsNullFast(perm[b])
				if aNil && bNil {
					continue
				}
				if aNil || bNil {
					if key.NullsLast {
						return !aNil
					}
					return aNil
				}
				cmp := compareVectorValues(col, perm[a], perm[b])
				if cmp == 0 {
					continue
				}
				if key.Order == Descending {
					return cmp > 0
				}
				return cmp < 0
			}
			return false
		})

		// Gather combined batch by permutation (sort in-place)
		gathered := windowGatherBatch(combined, perm)
		for j := range combined.Columns {
			combined.Columns[j] = gathered.Columns[j]
		}
	}

	// Re-acquire winVec after potential gather
	winVec := combined.Columns[winVecIdx]

	// Resolve column indices
	inputIdx := -1
	if wc.InputCol != "" {
		inputIdx = combined.ColumnIndex(wc.InputCol)
	}
	partIdxs := make([]int, len(wc.PartitionBy))
	for i, col := range wc.PartitionBy {
		partIdxs[i] = combined.ColumnIndex(col)
	}
	orderIdxs := make([]int, len(wc.OrderBy))
	for i, key := range wc.OrderBy {
		orderIdxs[i] = combined.ColumnIndex(key.Column)
	}

	// Walk partitions on sorted data
	i := 0
	for i < n {
		partEnd := i + 1
		for partEnd < n && sameColumnar(combined, i, partEnd, partIdxs) {
			partEnd++
		}
		computePartitionColumnar(combined, winVec, i, partEnd, wc, inputIdx, orderIdxs)
		i = partEnd
	}
}

// computePartitionColumnar computes the window function for a single partition.
// Operates directly on column vectors rather than row maps.
func computePartitionColumnar(combined *batch.RecordBatch, winVec *batch.Vector, start, end int, wc WindowColumn, inputIdx int, orderIdxs []int) {
	n := end - start
	var inputVec *batch.Vector
	if inputIdx >= 0 {
		inputVec = combined.Columns[inputIdx]
	}

	switch wc.Func {
	case WinRowNumber:
		for i := 0; i < n; i++ {
			winVec.Int64Data[start+i] = int64(i + 1)
			winVec.Nulls.SetValid(start + i)
		}

	case WinRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameColumnar(combined, start+i-1, start+i, orderIdxs) {
				rank = int64(i + 1)
			}
			winVec.Int64Data[start+i] = rank
			winVec.Nulls.SetValid(start + i)
		}

	case WinDenseRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameColumnar(combined, start+i-1, start+i, orderIdxs) {
				rank++
			}
			winVec.Int64Data[start+i] = rank
			winVec.Nulls.SetValid(start + i)
		}

	case WinSum:
		if len(orderIdxs) > 0 {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
				winVec.Float64Data[start+i] = sum
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
			}
			for i := 0; i < n; i++ {
				winVec.Float64Data[start+i] = sum
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinCount:
		if len(orderIdxs) > 0 {
			var count int64
			for i := 0; i < n; i++ {
				count++
				winVec.Int64Data[start+i] = count
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			count := int64(n)
			for i := 0; i < n; i++ {
				winVec.Int64Data[start+i] = count
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinAvg:
		if len(orderIdxs) > 0 {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
				winVec.Float64Data[start+i] = sum / float64(i+1)
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			var sum float64
			for i := 0; i < n; i++ {
				sum += vecFloat64(inputVec, start+i)
			}
			avg := sum / float64(n)
			for i := 0; i < n; i++ {
				winVec.Float64Data[start+i] = avg
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinMin:
		if len(orderIdxs) > 0 {
			var minVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if minVal == nil || (v != nil && compareAny(v, minVal) < 0) {
					minVal = v
				}
				winVec.SetValue(start+i, minVal)
			}
		} else {
			var minVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if minVal == nil || (v != nil && compareAny(v, minVal) < 0) {
					minVal = v
				}
			}
			for i := 0; i < n; i++ {
				winVec.SetValue(start+i, minVal)
			}
		}

	case WinMax:
		if len(orderIdxs) > 0 {
			var maxVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if maxVal == nil || (v != nil && compareAny(v, maxVal) > 0) {
					maxVal = v
				}
				winVec.SetValue(start+i, maxVal)
			}
		} else {
			var maxVal any
			for i := 0; i < n; i++ {
				v := inputVec.GetValue(start + i)
				if maxVal == nil || (v != nil && compareAny(v, maxVal) > 0) {
					maxVal = v
				}
			}
			for i := 0; i < n; i++ {
				winVec.SetValue(start+i, maxVal)
			}
		}

	case WinLag:
		offset := wc.LagLeadOffset
		if offset <= 0 {
			offset = 1
		}
		for i := 0; i < n; i++ {
			if i-offset >= 0 {
				winVec.SetValue(start+i, inputVec.GetValue(start+i-offset))
			} else if wc.LagLeadDefault != nil {
				winVec.SetValue(start+i, wc.LagLeadDefault)
			} else {
				winVec.Nulls.SetNull(start + i)
			}
		}

	case WinLead:
		offset := wc.LagLeadOffset
		if offset <= 0 {
			offset = 1
		}
		for i := 0; i < n; i++ {
			if i+offset < n {
				winVec.SetValue(start+i, inputVec.GetValue(start+i+offset))
			} else if wc.LagLeadDefault != nil {
				winVec.SetValue(start+i, wc.LagLeadDefault)
			} else {
				winVec.Nulls.SetNull(start + i)
			}
		}

	case WinFirstValue:
		if n > 0 {
			first := inputVec.GetValue(start)
			for i := 0; i < n; i++ {
				if first != nil {
					winVec.SetValue(start+i, first)
				} else {
					winVec.Nulls.SetNull(start + i)
				}
			}
		}

	case WinLastValue:
		if len(orderIdxs) > 0 {
			// With ORDER BY: running last value (current row's value)
			for i := 0; i < n; i++ {
				winVec.SetValue(start+i, inputVec.GetValue(start+i))
			}
		} else {
			if n > 0 {
				last := inputVec.GetValue(start + n - 1)
				for i := 0; i < n; i++ {
					if last != nil {
						winVec.SetValue(start+i, last)
					} else {
						winVec.Nulls.SetNull(start + i)
					}
				}
			}
		}

	case WinNtile:
		buckets := wc.NtileBuckets
		if buckets <= 0 {
			buckets = 1
		}
		base := n / buckets
		remainder := n % buckets
		bucket := int64(1)
		count := 0
		limit := base
		if remainder > 0 {
			limit++
		}
		for i := 0; i < n; i++ {
			winVec.Int64Data[start+i] = bucket
			winVec.Nulls.SetValid(start + i)
			count++
			if count >= limit && int(bucket) < buckets {
				bucket++
				count = 0
				if int(bucket) <= remainder {
					limit = base + 1
				} else {
					limit = base
				}
			}
		}

	case WinPercentRank:
		if n <= 1 {
			for i := 0; i < n; i++ {
				winVec.Float64Data[start+i] = 0
				winVec.Nulls.SetValid(start + i)
			}
		} else {
			rank := int64(1)
			for i := 0; i < n; i++ {
				if i > 0 && !sameColumnar(combined, start+i-1, start+i, orderIdxs) {
					rank = int64(i + 1)
				}
				winVec.Float64Data[start+i] = float64(rank-1) / float64(n-1)
				winVec.Nulls.SetValid(start + i)
			}
		}

	case WinCumeDist:
		for i := 0; i < n; {
			j := i + 1
			for j < n && sameColumnar(combined, start+i, start+j, orderIdxs) {
				j++
			}
			cd := float64(j) / float64(n)
			for k := i; k < j; k++ {
				winVec.Float64Data[start+k] = cd
				winVec.Nulls.SetValid(start + k)
			}
			i = j
		}

	case WinNthValue:
		nth := wc.NthValueN
		if nth <= 0 {
			nth = 1
		}
		if nth <= n {
			val := inputVec.GetValue(start + nth - 1)
			for i := 0; i < n; i++ {
				if val != nil {
					winVec.SetValue(start+i, val)
				} else {
					winVec.Nulls.SetNull(start + i)
				}
			}
		} else {
			for i := 0; i < n; i++ {
				winVec.Nulls.SetNull(start + i)
			}
		}
	}
}

// --- Row-oriented window computation (spill path) ---

// computeWindowRowOriented computes a single window function over row data.
// Used when spill occurred to avoid materializing a giant columnar batch.
func computeWindowRowOriented(rows []map[string]any, wc WindowColumn) {
	if len(rows) == 0 {
		return
	}

	// Sort by partition+order keys
	sort.SliceStable(rows, func(a, b int) bool {
		for _, pk := range wc.PartitionBy {
			cmp := compareAny(rows[a][pk], rows[b][pk])
			if cmp != 0 {
				return cmp < 0
			}
		}
		for _, ok := range wc.OrderBy {
			cmp := compareAny(rows[a][ok.Column], rows[b][ok.Column])
			if cmp != 0 {
				if ok.Order == Descending {
					return cmp > 0
				}
				return cmp < 0
			}
		}
		return false
	})

	// Walk partitions
	i := 0
	for i < len(rows) {
		partEnd := i + 1
		for partEnd < len(rows) && samePartitionRows(rows[i], rows[partEnd], wc.PartitionBy) {
			partEnd++
		}
		computePartitionRowOriented(rows[i:partEnd], wc)
		i = partEnd
	}
}

func samePartitionRows(a, b map[string]any, partCols []string) bool {
	for _, col := range partCols {
		if compareAny(a[col], b[col]) != 0 {
			return false
		}
	}
	return true
}

func sameOrderRows(a, b map[string]any, orderCols []SortKey) bool {
	for _, ok := range orderCols {
		if compareAny(a[ok.Column], b[ok.Column]) != 0 {
			return false
		}
	}
	return true
}

func rowFloat64(row map[string]any, col string) float64 {
	v := row[col]
	if v == nil {
		return 0
	}
	switch val := v.(type) {
	case float64:
		return val
	case float32:
		return float64(val)
	case int64:
		return float64(val)
	case int32:
		return float64(val)
	case int:
		return float64(val)
	default:
		return 0
	}
}

// computePartitionRowOriented computes a window function for one partition.
func computePartitionRowOriented(part []map[string]any, wc WindowColumn) {
	n := len(part)
	outCol := wc.OutputCol

	switch wc.Func {
	case WinRowNumber:
		for i := 0; i < n; i++ {
			part[i][outCol] = int64(i + 1)
		}

	case WinRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameOrderRows(part[i-1], part[i], wc.OrderBy) {
				rank = int64(i + 1)
			}
			part[i][outCol] = rank
		}

	case WinDenseRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameOrderRows(part[i-1], part[i], wc.OrderBy) {
				rank++
			}
			part[i][outCol] = rank
		}

	case WinSum:
		if len(wc.OrderBy) > 0 {
			var sum float64
			for i := 0; i < n; i++ {
				sum += rowFloat64(part[i], wc.InputCol)
				part[i][outCol] = sum
			}
		} else {
			var sum float64
			for i := 0; i < n; i++ {
				sum += rowFloat64(part[i], wc.InputCol)
			}
			for i := 0; i < n; i++ {
				part[i][outCol] = sum
			}
		}

	case WinCount:
		if len(wc.OrderBy) > 0 {
			for i := 0; i < n; i++ {
				part[i][outCol] = int64(i + 1)
			}
		} else {
			count := int64(n)
			for i := 0; i < n; i++ {
				part[i][outCol] = count
			}
		}

	case WinAvg:
		if len(wc.OrderBy) > 0 {
			var sum float64
			for i := 0; i < n; i++ {
				sum += rowFloat64(part[i], wc.InputCol)
				part[i][outCol] = sum / float64(i+1)
			}
		} else {
			var sum float64
			for i := 0; i < n; i++ {
				sum += rowFloat64(part[i], wc.InputCol)
			}
			avg := sum / float64(n)
			for i := 0; i < n; i++ {
				part[i][outCol] = avg
			}
		}

	case WinMin:
		if len(wc.OrderBy) > 0 {
			var minVal any
			for i := 0; i < n; i++ {
				v := part[i][wc.InputCol]
				if minVal == nil || (v != nil && compareAny(v, minVal) < 0) {
					minVal = v
				}
				part[i][outCol] = minVal
			}
		} else {
			var minVal any
			for i := 0; i < n; i++ {
				v := part[i][wc.InputCol]
				if minVal == nil || (v != nil && compareAny(v, minVal) < 0) {
					minVal = v
				}
			}
			for i := 0; i < n; i++ {
				part[i][outCol] = minVal
			}
		}

	case WinMax:
		if len(wc.OrderBy) > 0 {
			var maxVal any
			for i := 0; i < n; i++ {
				v := part[i][wc.InputCol]
				if maxVal == nil || (v != nil && compareAny(v, maxVal) > 0) {
					maxVal = v
				}
				part[i][outCol] = maxVal
			}
		} else {
			var maxVal any
			for i := 0; i < n; i++ {
				v := part[i][wc.InputCol]
				if maxVal == nil || (v != nil && compareAny(v, maxVal) > 0) {
					maxVal = v
				}
			}
			for i := 0; i < n; i++ {
				part[i][outCol] = maxVal
			}
		}

	case WinLag:
		offset := wc.LagLeadOffset
		if offset <= 0 {
			offset = 1
		}
		for i := 0; i < n; i++ {
			if i-offset >= 0 {
				part[i][outCol] = part[i-offset][wc.InputCol]
			} else if wc.LagLeadDefault != nil {
				part[i][outCol] = wc.LagLeadDefault
			}
		}

	case WinLead:
		offset := wc.LagLeadOffset
		if offset <= 0 {
			offset = 1
		}
		for i := 0; i < n; i++ {
			if i+offset < n {
				part[i][outCol] = part[i+offset][wc.InputCol]
			} else if wc.LagLeadDefault != nil {
				part[i][outCol] = wc.LagLeadDefault
			}
		}

	case WinFirstValue:
		if n > 0 {
			first := part[0][wc.InputCol]
			for i := 0; i < n; i++ {
				part[i][outCol] = first
			}
		}

	case WinLastValue:
		if len(wc.OrderBy) > 0 {
			for i := 0; i < n; i++ {
				part[i][outCol] = part[i][wc.InputCol]
			}
		} else if n > 0 {
			last := part[n-1][wc.InputCol]
			for i := 0; i < n; i++ {
				part[i][outCol] = last
			}
		}

	case WinNtile:
		buckets := wc.NtileBuckets
		if buckets <= 0 {
			buckets = 1
		}
		base := n / buckets
		remainder := n % buckets
		bucket := int64(1)
		count := 0
		limit := base
		if remainder > 0 {
			limit++
		}
		for i := 0; i < n; i++ {
			part[i][outCol] = bucket
			count++
			if count >= limit && int(bucket) < buckets {
				bucket++
				count = 0
				if int(bucket) <= remainder {
					limit = base + 1
				} else {
					limit = base
				}
			}
		}

	case WinPercentRank:
		if n <= 1 {
			for i := 0; i < n; i++ {
				part[i][outCol] = float64(0)
			}
		} else {
			rank := int64(1)
			for i := 0; i < n; i++ {
				if i > 0 && !sameOrderRows(part[i-1], part[i], wc.OrderBy) {
					rank = int64(i + 1)
				}
				part[i][outCol] = float64(rank-1) / float64(n-1)
			}
		}

	case WinCumeDist:
		for i := 0; i < n; {
			j := i + 1
			for j < n && sameOrderRows(part[i], part[j], wc.OrderBy) {
				j++
			}
			cd := float64(j) / float64(n)
			for k := i; k < j; k++ {
				part[k][outCol] = cd
			}
			i = j
		}

	case WinNthValue:
		nth := wc.NthValueN
		if nth <= 0 {
			nth = 1
		}
		if nth <= n {
			val := part[nth-1][wc.InputCol]
			for i := 0; i < n; i++ {
				part[i][outCol] = val
			}
		}
	}
}
