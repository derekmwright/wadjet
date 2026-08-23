package exec

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/memory"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// ParseWindowFunc maps a SQL window function name (case-insensitive) onto its
// WindowFunc constant. ok is false for a name this operator does not
// implement, and the returned WindowFunc is then WinRowNumber — the zero
// value, which the single-process planner has always fallen back to. A caller
// that ships the spec somewhere else (the distributed fragment builder) should
// refuse on !ok instead: computing ROW_NUMBER for a function nobody
// recognized is a wrong answer with no error attached.
//
// It lives here rather than in the planner because both the planner and the
// worker turn a name into this package's constant, and two switch statements
// are two chances to disagree.
func ParseWindowFunc(s string) (WindowFunc, bool) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "row_number":
		return WinRowNumber, true
	case "rank":
		return WinRank, true
	case "dense_rank":
		return WinDenseRank, true
	case "sum":
		return WinSum, true
	case "count":
		return WinCount, true
	case "avg":
		return WinAvg, true
	case "min":
		return WinMin, true
	case "max":
		return WinMax, true
	case "lag":
		return WinLag, true
	case "lead":
		return WinLead, true
	case "first_value":
		return WinFirstValue, true
	case "last_value":
		return WinLastValue, true
	case "ntile":
		return WinNtile, true
	case "percent_rank":
		return WinPercentRank, true
	case "cume_dist":
		return WinCumeDist, true
	case "nth_value":
		return WinNthValue, true
	default:
		return WinRowNumber, false
	}
}

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

// windowValueFunc reports whether f returns a value lifted out of its input
// column rather than computing one. Those five are the functions whose output
// type IS the input column's type, and the only ones whose compute path goes
// through Vector.GetValue/SetValue — every other window function writes its
// typed backing array directly (Int64Data for the ranks, Float64Data for
// SUM/AVG), so re-typing one of those would panic instead of correcting it.
//
// WinMin/WinMax also copy an input value through SetValue, and were the last
// input-dependent functions left on the planner's float64 declaration —
// MIN(int32_col) OVER and MIN(a_string) OVER answered 0 on every row, #345's
// symptom reached through a different name (#361). They are not on THIS
// list because their answer is chosen by compareAny rather than lifted by
// position: retyping is only sound for the input types that comparison
// orders correctly on Vector.GetValue's representation, which is
// WindowMinMaxType's decision.
func windowValueFunc(f WindowFunc) bool {
	switch f {
	case WinLag, WinLead, WinFirstValue, WinLastValue, WinNthValue:
		return true
	}
	return false
}

// WindowMinMaxType is the output type MIN/MAX over a window declare for an
// input column of type in, and whether they may re-declare at all.
//
// The vetted set is exactly the types whose Vector.GetValue representation
// compareAny orders correctly — the ints and floats, strings (byte order),
// TIMESTAMP/DURATION (bare int64), DATE (its ISO string form is
// byte-ordered), PORT/PROTOCOL (int32). The output type is the input type:
// MIN/MAX return one of their input's values untouched.
//
// ok=false declines: BYTES compares as 0-everywhere under compareAny (no
// []byte arm), the network types and UUID surface display strings whose
// byte order is not their value order ("9.x" > "10.x"), and DECIMAL
// surfaces a formatted string with the same problem. Those keep the
// planner's float64 declaration, where Vector.SetValue's type guard now
// reports the write it cannot perform instead of dropping it — an error,
// not a silently wrong ordering.
//
// Exported because the physical planner declares from the catalog with this
// same function; two vetting lists would drift.
func WindowMinMaxType(in parquet.TypeID) (parquet.TypeID, bool) {
	switch in {
	case batch.TypeInt32, batch.TypeInt64, batch.TypeFloat32, batch.TypeFloat64,
		batch.TypeString, batch.TypeTimestamp, batch.TypeDate,
		batch.TypeDuration, batch.TypePort, batch.TypeProtocol:
		return in, true
	}
	return 0, false
}

// windowOutputColumn declares one window function's output column. When the
// output IS the input column's own type — which is the whole point of
// retypeValueColumns below — the input's PARAMETERISATION rides along too.
//
// A bare TypeID is not a type for five of the twenty-two. DECIMAL without its
// scale, VECTOR without its dimension, ARRAY/MAP without an element and ROW
// without fields are all unusable, and unusable in SILENCE: Vector.SetValue's
// ARRAY/MAP arm returns early on a nil Child, its ROW arm on nil Children and
// its VECTOR arm on a zero dimension, over a vector whose null mask was
// pre-set all-null — so `FIRST_VALUE(arr_col) OVER (...)` wrote nothing and
// read back NULL on every row (#406). DECIMAL was the quiet one: SetValue
// re-parses the formatted string GetValue produced against the OUTPUT
// vector's scale, so a scale-4 column came back through a scale-0 vector as
// 3 where the row holds 3.0003 — a wrong number, not a missing one.
//
// This is the aggregate's aggInputMeta rule (aggregate.go, #392) applied to
// the window: the metadata travels with the type because it is what makes the
// boxed value round-trip. The `col.Type != wc.OutputType` guard is the same
// one, and for the same reason — metadata is copied only when it describes
// the very type being declared.
func windowOutputColumn(wc WindowColumn, schema []parquet.Column) parquet.Column {
	out := parquet.Column{Name: wc.OutputCol, Type: wc.OutputType, Nullable: true}
	if wc.InputCol == "" {
		return out
	}
	for _, col := range schema {
		if col.Name != wc.InputCol || col.Type != wc.OutputType {
			continue
		}
		out.Precision, out.Scale = col.Precision, col.Scale
		out.Fields, out.ElementType, out.Dimension = col.Fields, col.ElementType, col.Dimension
		break
	}
	return out
}

// retypeValueColumns re-declares each value function's output type from the
// input vector it will actually read, the way exec.Project resolves a
// projection's type from its input batch instead of trusting the planner's
// declaration (project.go). It reports whether anything changed.
//
// Defence in depth for #345: the planner now resolves these types from the
// catalog, but a declaration that arrives wrong — a spec built by a caller
// with no schema to resolve against, an input type the planner had to decline
// — is otherwise final, because Window allocates batch.NewVector(OutputType)
// and every write of a value the vector cannot hold is dropped in silence.
//
// The lookup is RecordBatch.ColumnIndex's exact-name match, which is how
// computePartitionColumnar resolves InputCol, so the type declared here is
// always the type of the vector the compute reads. A name the input does not
// carry leaves the declaration alone — the compute finds no input column
// either and writes nothing but NULLs.
func (w *Window) retypeValueColumns() bool {
	changed := false
	for i := range w.Columns {
		wc := &w.Columns[i]
		minMax := wc.Func == WinMin || wc.Func == WinMax
		if (!windowValueFunc(wc.Func) && !minMax) || wc.InputCol == "" {
			continue
		}
		for _, col := range w.schema {
			if col.Name != wc.InputCol {
				continue
			}
			t := col.Type
			if minMax {
				// Only the compareAny-vetted types may re-declare (#361);
				// the rest keep the planner's declaration, whose wrong
				// writes SetValue now reports instead of dropping.
				vetted, ok := WindowMinMaxType(t)
				if !ok {
					break
				}
				t = vetted
			}
			if wc.OutputType != t {
				wc.OutputType = t
				changed = true
			}
			break
		}
	}
	return changed
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
// NewWindow copies the spec slice. retypeValueColumns REWRITES OutputType in
// place, so sharing the caller's backing array lets one Window's correction
// land in another's specs — and the other then sees retypeValueColumns report
// "nothing changed", skips the w.groups rebuild, and keeps the spec COPIES
// groupWindowSpecs took at Init under the OLD type. The external path reads
// its types from those copies, so it would allocate a FLOAT64 output vector
// for an ARRAY value and raise the #361 guard on the write.
func NewWindow(cols []WindowColumn) *Window {
	own := make([]WindowColumn, len(cols))
	copy(own, cols)
	return &Window{Columns: own}
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
		if w.retypeValueColumns() {
			// The grouping holds COPIES of the specs, so it has to be
			// rebuilt to see the corrected types. Group identity is
			// (PartitionBy, OrderBy) only, so the grouping itself — and any
			// run already spilled under w.groups[0].sortKeys — is unchanged.
			w.groups = groupWindowSpecs(w.Columns)
		}
	}
	FlattenForConsumer(b, nil) // retained past the batch cycle: views must not survive
	b.Detach()                 // prevent pool recycle — pipeline calls Release() after Consume()
	// Snapshot the selection vector — Filter operators reuse their sel
	// buffer across calls (see CollectSink.Consume); a stored batch would
	// otherwise see its Sel silently rewritten by the NEXT batch's filter
	// pass, yielding out-of-range physical indices at finalize (ClickBench
	// Q24: SELECT * + LIKE filter straight into Sort — panic in
	// sortCompareInt64NoNulls) or silently wrong sorted output.
	if b.Sel != nil {
		selCopy := make([]uint32, len(b.Sel))
		copy(selCopy, b.Sel)
		b.Sel = selCopy
	}
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
		// runFiles was already taken; Close's backstop sees nil. Delete
		// here or the runs outlive the query in the shared spill dir.
		removeRunFiles(runs)
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

	// Cleanup contract: resortRunsByKeys, windowDiskPass, and openRunMerger
	// each delete every run file they were given (inputs and partial
	// outputs) on their own error paths — callers below just propagate.
	// The previous caller-side removeRunFiles calls here were no-ops: the
	// multi-assignment had already overwritten runs with the helper's nil
	// error return before the cleanup ran.
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
	if len(last.partitionBy) == 0 && !groupNeedsMaterializedFrame(last) {
		// Final group with no PARTITION BY: two-pass streaming instead of
		// accumulating the whole input as one partition (window_global.go).
		merger, runs, err := openRunMerger(dir, passSchema, last.sortKeys, runs)
		if err != nil {
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
		outCol := windowOutputColumn(wc, w.schema)
		vec := batch.NewColumnVector(outCol, combined.Len)
		vec.Nulls = batch.NewBitmapAllNull(combined.Len)
		combined.Columns = append(combined.Columns, vec)
		combined.Schema = append(combined.Schema, outCol)
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
		// Consumed for good — unlink now rather than relying on
		// SpillManager.Cleanup, which the shared (worker-injected) manager
		// path never calls (#324). Files not yet reached when an error
		// aborts this loop stay in w.spillFiles for Close's backstop.
		w.Spill.RemoveSpilled(f)
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
		computeWindowRowOriented(allRows, wc, w.schema)
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
		outSchema = append(outSchema, windowOutputColumn(wc, w.schema))
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
	if w.Spill != nil {
		if w.trackedMem > 0 {
			w.Spill.ReleaseTracking(w.trackedMem)
			w.trackedMem = 0
		}
		// Row-oriented spill files never consumed by finalizeWithSpill
		// (error or early-cancel path). Nothing else removes them on the
		// shared-manager path (#324).
		for _, f := range w.spillFiles {
			w.Spill.RemoveSpilled(f)
		}
		w.spillFiles = nil
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
		// kernel's float order (NaN greatest, NaN == NaN), the same one the
		// sort kernels and the container comparators take — a window must
		// not draw its PARTITION BY boundaries and RANK peer groups on a
		// different order from the one that sorted the rows (#446).
		return kernel.CompareFloat64(col.Float64Data[a], col.Float64Data[b])
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
		return kernel.CompareFloat32(col.Float32Data[a], col.Float32Data[b])
	case batch.TypeBool:
		va, vb := col.BoolData[a], col.BoolData[b]
		if !va && vb {
			return -1
		}
		if va && !vb {
			return 1
		}
		return 0
	case batch.TypeString, batch.TypeBytes, batch.TypeIPv6, batch.TypeCIDR, batch.TypeUUID:
		// All five are BytesColumn-backed and the sort kernel groups them
		// together, so they order by BYTES here too. Only STRING was listed
		// before; the other four fell to compareAny over GetValue, which
		// compares the RENDERED text — and "2001:db8::10" sorts before
		// "2001:db8::9" as text while the addresses do not. Same
		// path-dependent sequence #394 found for DECIMAL, here deciding
		// PARTITION BY boundaries and RANK peer groups.
		va := col.BytesData.UnsafeStringValue(a)
		vb := col.BytesData.UnsafeStringValue(b)
		if va < vb {
			return -1
		}
		if va > vb {
			return 1
		}
		return 0
	case batch.TypeArray, batch.TypeMap, batch.TypeRow, batch.TypeVector:
		// Columnar container comparison, the same total order ORDER BY and
		// the sort-merge join take. compareAny's boxed form would answer
		// too, but it renders a nested DECIMAL as text and loses a ROW's
		// field ORDER, so the window would partition a container column
		// differently from the way the sort orders it (#415).
		return kernel.CompareValuesAt(col, a, col, b)
	case batch.TypeDecimal:
		// Not the default's business: Vector.GetValue boxes a DECIMAL as its
		// FORMATTED string, so compareAny would order "10.001" before
		// "2.0002" — the same lexicographic-vs-numeric split #394 found in
		// the sort kernel, here driving PARTITION BY boundaries and the
		// RANK/DENSE_RANK/PERCENT_RANK/CUME_DIST peer groups.
		return kernel.CompareDecimalAt(col, a, col, b)
	default:
		return compareAny(col.GetValue(a), col.GetValue(b))
	}
}

// vecFloat64 reads a float64 value from any numeric vector without boxing.
// The second result reports whether the COLUMN has a numeric reading at all;
// false means the caller must produce NULL, never a zero.
//
// Its four-case switch defaulted to 0 for DECIMAL, DURATION, PORT, PROTOCOL
// and DATE, so a window SUM or AVG over any of them computed ZERO and marked
// the row valid — a wrong number, not a missing one, and identical on every
// arm of every differential gate. The type list now lives in
// numeric_promote.go alongside the grouped aggregate's, so the windowed and
// grouped forms of one query cannot disagree.
//
// Returning the bool is the second half of that: the promotion table already
// said IPV4 and MAC have no numeric reading, and this function discarded the
// answer, so `SUM(ipv4_col) OVER ()` computed 0 and marked the row VALID
// while the grouped `SUM(ipv4_col)` answered NULL — the exact
// windowed-vs-grouped disagreement #412 was filed for.
//
// A NULL CELL is not that case: it reads (0, true), because a NULL
// contributes nothing to a sum and leaves the sum defined. The bool is about
// the type, so it is constant across a column's rows.
func vecFloat64(v *batch.Vector, i int) (float64, bool) {
	if v == nil || !numericPromotable(v.Type) {
		return 0, false
	}
	if v.Nulls.IsNullFast(i) {
		return 0, true
	}
	return numericFloat64(v, i)
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

// --- Window frames ---
//
// A frame narrows what one row SEES inside its partition. WindowColumn.Frame
// was parsed, carried through the logical plan, put on the exec spec and on
// the wire — and then read by nothing: every value and aggregate function
// decided on `len(orderIdxs) > 0` alone, so an explicit ROWS/RANGE clause was
// discarded on BOTH execution paths (#350). LAST_VALUE over an explicit
// whole-partition frame returned the current row's value, and SUM over one
// returned a running total instead of the partition total — a wrong number
// that looks exactly like a right one.
//
// The frame is resolved once per partition into per-row half-open [lo, hi)
// row ranges, and every frame-sensitive function reads its rows from there.
// The default frame is not a special case: it is a real frame that happens to
// be the one SQL supplies.

// defaultWindowFrame is what a window spec with no ROWS/RANGE clause gets:
// RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW.
//
// One spec covers both shapes the old code branched on. RANGE's CURRENT ROW
// means "through the end of this row's ORDER-BY peer group", so with an ORDER
// BY this is the running frame — which is why LAST_VALUE with a bare ORDER BY
// legitimately answers with the current row, and why that is NOT the bug in
// #350. With no ORDER BY every row is a peer of every other, so the same
// spec widens to the whole partition on its own.
var defaultWindowFrame = WindowFrameSpec{
	Mode:  "range",
	Start: WindowBound{Type: "unbounded_preceding"},
	End:   WindowBound{Type: "current_row"},
}

// frameSensitive reports whether the frame changes f's answer. The aggregates
// and the three positional value functions read the frame; the rank family,
// NTILE, PERCENT_RANK, CUME_DIST, LAG and LEAD are defined against the
// partition and its ORDER BY and ignore it, which is why an engine can carry
// a Frame field for years and pass its window tests.
func frameSensitive(f WindowFunc) bool {
	switch f {
	case WinSum, WinCount, WinAvg, WinMin, WinMax, WinFirstValue, WinLastValue, WinNthValue:
		return true
	}
	return false
}

// groupNeedsMaterializedFrame reports whether g asks for an EXPLICIT frame.
//
// The streaming empty-PARTITION-BY evaluator (window_global.go) resolves each
// row from O(1) carried state plus a one-pass scan, which answers the default
// frame and nothing else: a frame that reaches FORWARD (UNBOUNDED FOLLOWING,
// `k FOLLOWING`) or whose lower end MOVES needs rows the stream has not
// produced or has already dropped. Rather than answer such a query with the
// default frame — the shape of #350 — the group falls back to the
// partition-at-a-time walker, which materializes the partition and goes
// through computePartitionColumnar like every other path.
func groupNeedsMaterializedFrame(g windowSpecGroup) bool {
	for _, wc := range g.cols {
		if wc.Frame != nil && frameSensitive(wc.Func) {
			return true
		}
	}
	return false
}

// resolvedFrame answers "which rows does row i see" for one partition, as a
// half-open [lo, hi) range of PARTITION-RELATIVE indices. hi <= lo is an
// empty frame, whose answer is NULL for every function.
//
// peerLo/peerHi are the ORDER-BY peer-group bounds each row belongs to, built
// only in RANGE mode with an ORDER BY. Nil peer arrays mean "every row is a
// peer" — the no-ORDER-BY case — which reads as [0, n).
type resolvedFrame struct {
	n      int
	rows   bool // ROWS mode: bounds count rows. RANGE mode: bounds move by peer group.
	start  WindowBound
	end    WindowBound
	peerLo []int32
	peerHi []int32
}

// resolveFrame builds the frame for one partition. peerGroups is called only
// when it is needed — RANGE mode with an ORDER BY — and must return, per row,
// the half-open bounds of that row's peer group.
func resolveFrame(wc WindowColumn, n int, hasOrderBy bool, peerGroups func() ([]int32, []int32)) resolvedFrame {
	spec := defaultWindowFrame
	if wc.Frame != nil {
		spec = *wc.Frame
	}
	f := resolvedFrame{n: n, rows: spec.Mode == "rows", start: spec.Start, end: spec.End}
	if !f.rows && hasOrderBy {
		f.peerLo, f.peerHi = peerGroups()
	}
	return f
}

func (f resolvedFrame) peerLoAt(i int) int {
	if f.peerLo == nil {
		return 0
	}
	return int(f.peerLo[i])
}

func (f resolvedFrame) peerHiAt(i int) int {
	if f.peerHi == nil {
		return f.n
	}
	return int(f.peerHi[i])
}

// bounds returns the half-open row range row i sees, clamped to the
// partition. hi <= lo means the frame is empty.
//
// RANGE mode with a VALUE offset (`RANGE BETWEEN 5 PRECEDING …`) never
// reaches here: the parser rejects that spelling rather than answer it with
// row offsets, which is a different query. The peer-bound fallbacks below
// keep the switch total.
func (f resolvedFrame) bounds(i int) (int, int) {
	var lo int
	switch f.start.Type {
	case "unbounded_preceding":
		lo = 0
	case "preceding":
		if f.rows {
			lo = i - f.start.Offset
		} else {
			lo = f.peerLoAt(i)
		}
	case "current_row":
		if f.rows {
			lo = i
		} else {
			lo = f.peerLoAt(i)
		}
	case "following":
		if f.rows {
			lo = i + f.start.Offset
		} else {
			lo = f.peerHiAt(i)
		}
	case "unbounded_following":
		lo = f.n
	}

	var hi int
	switch f.end.Type {
	case "unbounded_preceding":
		hi = 0
	case "preceding":
		if f.rows {
			hi = i - f.end.Offset + 1
		} else {
			hi = f.peerLoAt(i)
		}
	case "current_row":
		if f.rows {
			hi = i + 1
		} else {
			hi = f.peerHiAt(i)
		}
	case "following":
		if f.rows {
			hi = i + f.end.Offset + 1
		} else {
			hi = f.peerHiAt(i)
		}
	case "unbounded_following":
		hi = f.n
	}

	// Clamp both ends into the partition. Clamping preserves the property
	// every caller relies on — each end is non-decreasing in i — which is
	// what lets the aggregates slide instead of rescanning.
	lo = min(max(lo, 0), f.n)
	hi = min(max(hi, 0), f.n)
	return lo, hi
}

// columnarPeerGroups labels every row of a partition with its ORDER-BY peer
// group. The partition is already sorted by the order keys, so peers are
// contiguous and one linear pass finds them — the same walk WinCumeDist does.
func columnarPeerGroups(combined *batch.RecordBatch, start, n int, orderIdxs []int) ([]int32, []int32) {
	lo := make([]int32, n)
	hi := make([]int32, n)
	for i := 0; i < n; {
		j := i + 1
		for j < n && sameColumnar(combined, start+i, start+j, orderIdxs) {
			j++
		}
		for k := i; k < j; k++ {
			lo[k], hi[k] = int32(i), int32(j)
		}
		i = j
	}
	return lo, hi
}

// frameMinMaxDeque tracks the min (or max) of a sliding frame in amortized
// O(1) per row. Frame bounds only ever move forward — every bound type is a
// non-decreasing function of the row index — so a monotonic deque of
// candidate indices is enough: values that can never win again are dropped on
// arrival, and the front is evicted once it falls out of the frame.
//
// The alternative, rescanning the frame per row, is O(n·width): fine for `1
// PRECEDING AND 1 FOLLOWING`, quadratic for a frame a user writes wide.
type frameMinMaxDeque struct {
	idx  []int
	want int // -1 = keep minimum, +1 = keep maximum
	get  func(i int) any
	// cmp orders two partition-relative rows, both known non-NULL. It is
	// resolved by the caller from what the caller HAS: the columnar path
	// hands it the column's own kernel comparator, the row-oriented path a
	// comparator built from the declared column (#444). Comparing the boxed
	// values here with compareAny was the site that ordered a ROW's fields by
	// NAME while every other path ordered them positionally.
	cmp  func(i, j int) int
	next int // next partition-relative row not yet pushed
}

func (d *frameMinMaxDeque) advance(lo, hi int) {
	for ; d.next < hi; d.next++ {
		if d.get(d.next) == nil {
			continue // SQL MIN/MAX skip NULLs
		}
		// Every index already in the deque passed the same NULL check, so
		// both sides of cmp are non-NULL.
		for len(d.idx) > 0 {
			if d.cmp(d.next, d.idx[len(d.idx)-1])*d.want < 0 {
				break
			}
			d.idx = d.idx[:len(d.idx)-1]
		}
		d.idx = append(d.idx, d.next)
	}
	for len(d.idx) > 0 && d.idx[0] < lo {
		d.idx = d.idx[1:]
	}
}

// value returns the extreme value over [lo, hi), or nil when the frame holds
// no non-NULL value.
func (d *frameMinMaxDeque) value(lo, hi int) any {
	d.advance(lo, hi)
	if len(d.idx) == 0 {
		return nil
	}
	return d.get(d.idx[0])
}

// computePartitionColumnar computes the window function for a single partition.
// Operates directly on column vectors rather than row maps.
func computePartitionColumnar(combined *batch.RecordBatch, winVec *batch.Vector, start, end int, wc WindowColumn, inputIdx int, orderIdxs []int) {
	n := end - start
	var inputVec *batch.Vector
	if inputIdx >= 0 {
		inputVec = combined.Columns[inputIdx]
	}
	if inputVec == nil {
		switch wc.Func {
		case WinSum, WinAvg, WinMin, WinMax, WinLag, WinLead, WinFirstValue, WinLastValue, WinNthValue:
			// The spec names a column this batch does not carry, so there
			// is nothing to read. Leave the output NULL — it is already
			// all-null — instead of dereferencing a nil vector: a plan that
			// loses a column may cost an answer, never the process (#310).
			// The streaming empty-PARTITION-BY path (window_global.go)
			// already skips on the same condition; this is the in-memory
			// and partition-at-a-time path catching up.
			return
		}
	}

	// The frame-sensitive functions all read their rows from fr; the rest
	// never build one.
	var fr resolvedFrame
	if frameSensitive(wc.Func) {
		fr = resolveFrame(wc, n, len(orderIdxs) > 0, func() ([]int32, []int32) {
			return columnarPeerGroups(combined, start, n, orderIdxs)
		})
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

	// SUM and AVG slide one accumulator across the partition: the frame's
	// ends only move forward, so each row is added once and removed once.
	// With the default frame the lower end never moves, so nothing is ever
	// subtracted and the arithmetic is the same running total, in the same
	// order, that this branch computed before frames existed.
	case WinSum:
		// No numeric reading for this column type (IPV4, MAC, a byte-backed
		// type, a container): the answer is NULL for every row, which is
		// what the grouped SUM answers and what winVec already holds.
		if !vecSummable(inputVec) {
			return
		}
		var sum float64
		curLo, curHi := 0, 0
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			curLo, curHi = slideFrameSum(inputVec, start, lo, hi, curLo, curHi, &sum)
			if hi <= lo {
				continue // empty frame: SUM is NULL, and winVec starts all-null
			}
			winVec.Float64Data[start+i] = sum
			winVec.Nulls.SetValid(start + i)
		}

	// COUNT is the one aggregate an empty frame does not make NULL: it
	// counts the rows it can see, and seeing none is 0.
	case WinCount:
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			if hi < lo {
				hi = lo
			}
			winVec.Int64Data[start+i] = int64(hi - lo)
			winVec.Nulls.SetValid(start + i)
		}

	case WinAvg:
		if !vecSummable(inputVec) {
			return // see WinSum
		}
		var sum float64
		curLo, curHi := 0, 0
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			curLo, curHi = slideFrameSum(inputVec, start, lo, hi, curLo, curHi, &sum)
			if hi <= lo {
				continue
			}
			winVec.Float64Data[start+i] = sum / float64(hi-lo)
			winVec.Nulls.SetValid(start + i)
		}

	case WinMin, WinMax:
		want := -1
		if wc.Func == WinMax {
			want = 1
		}
		d := &frameMinMaxDeque{
			want: want,
			get:  func(i int) any { return inputVec.GetValue(start + i) },
			// The column is right here, so the comparison is the columnar
			// one — no box, and no second opinion about a ROW's field order.
			cmp: func(i, j int) int {
				return kernel.CompareValuesAt(inputVec, start+i, inputVec, start+j)
			},
		}
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			if hi < lo {
				hi = lo
			}
			if v := d.value(lo, hi); v != nil {
				winVec.SetValue(start+i, v)
			} else {
				winVec.WriteNullAt(start + i)
			}
		}

	// WriteNullAt, not Nulls.SetNull, throughout the value functions: their
	// output vector is now the INPUT column's type, so it can be
	// variable-length, and a bytes column's null still owes its offset slot.
	// Skipping it leaves Offsets[i+1] at zero and every later row in the
	// column reads back from the start of the arena — LAG's own leading NULL
	// made the next row return the whole partition concatenated. The gather
	// and range-copy helpers above already advance on null for this reason.
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
				winVec.WriteNullAt(start + i)
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
				winVec.WriteNullAt(start + i)
			}
		}

	// The three value functions index into the FRAME, not the partition.
	// Under the default frame that reproduces what they answered before —
	// FIRST_VALUE the partition's first row, LAST_VALUE the current row —
	// because the default frame's first row IS the partition's first and its
	// last row IS the current row (or the last of its peer group). What
	// changes is that an explicit frame is now obeyed: LAST_VALUE over
	// UNBOUNDED FOLLOWING reaches the partition's end (#350).
	case WinFirstValue:
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			var v any
			if hi > lo {
				v = inputVec.GetValue(start + lo)
			}
			if v != nil {
				winVec.SetValue(start+i, v)
			} else {
				winVec.WriteNullAt(start + i)
			}
		}

	case WinLastValue:
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			var v any
			if hi > lo {
				v = inputVec.GetValue(start + hi - 1)
			}
			if v != nil {
				winVec.SetValue(start+i, v)
			} else {
				winVec.WriteNullAt(start + i)
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
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			var v any
			if pos := lo + nth - 1; pos < hi {
				v = inputVec.GetValue(start + pos)
			}
			if v != nil {
				winVec.SetValue(start+i, v)
			} else {
				winVec.WriteNullAt(start + i)
			}
		}
	}
}

// slideFrameSum advances a running sum from the frame [curLo, curHi) to the
// frame [lo, hi) and returns the new position. Rows are added before any are
// removed so the window never inverts on an empty frame (hi < lo), where the
// rows added to reach lo are immediately subtracted again and the sum
// correctly lands on zero.
func slideFrameSum(inputVec *batch.Vector, start, lo, hi, curLo, curHi int, sum *float64) (int, int) {
	if hi < lo {
		hi = lo
	}
	for curHi < hi {
		f, _ := vecFloat64(inputVec, start+curHi)
		*sum += f
		curHi++
	}
	for curLo < lo {
		f, _ := vecFloat64(inputVec, start+curLo)
		*sum -= f
		curLo++
	}
	return curLo, curHi
}

// vecSummable reports whether a window SUM or AVG over this column has a
// numeric reading. The answer is a property of the column TYPE, so it is
// asked once per partition rather than per row — a per-row check would let a
// frame that happens to add no rows write a zero for a column that has no
// number in it at all.
func vecSummable(v *batch.Vector) bool {
	_, ok := vecFloat64(v, 0)
	return ok
}

// --- Row-oriented window computation (spill path) ---

// windowRowCompares holds the boxed comparators one row-oriented window
// column needs — one per PARTITION BY column, one per ORDER BY key, and one
// for the value column MIN/MAX ranks — resolved ONCE from the declared
// schema.
//
// Resolving them is what makes this path agree with the columnar one. A row
// map carries values, not declarations: Vector.GetValue renders a ROW as an
// unordered map and a DECIMAL as text, so a comparator with nothing but the
// box has to guess a field order and a numeric order, and guessed the wrong
// one both times (#444). Resolving per column also keeps the type switch out
// of the comparison loop, which is the codebase's typed-kernel rule.
type windowRowCompares struct {
	partition []boxedCompare
	order     []boxedCompare
	input     boxedCompare
}

func newWindowRowCompares(schema []parquet.Column, wc WindowColumn) windowRowCompares {
	rc := windowRowCompares{
		partition: make([]boxedCompare, len(wc.PartitionBy)),
		order:     make([]boxedCompare, len(wc.OrderBy)),
		input:     newBoxedCompareFor(schema, wc.InputCol),
	}
	for i, pk := range wc.PartitionBy {
		rc.partition[i] = newBoxedCompareFor(schema, pk)
	}
	for i, ok := range wc.OrderBy {
		rc.order[i] = newBoxedCompareFor(schema, ok.Column)
	}
	return rc
}

// computeWindowRowOriented computes a single window function over row data.
// Used when spill occurred to avoid materializing a giant columnar batch.
func computeWindowRowOriented(rows []map[string]any, wc WindowColumn, schema []parquet.Column) {
	if len(rows) == 0 {
		return
	}
	rc := newWindowRowCompares(schema, wc)

	// Sort by partition+order keys
	sort.SliceStable(rows, func(a, b int) bool {
		for i, pk := range wc.PartitionBy {
			cmp := rc.partition[i](rows[a][pk], rows[b][pk])
			if cmp != 0 {
				return cmp < 0
			}
		}
		for i, ok := range wc.OrderBy {
			cmp := rc.order[i](rows[a][ok.Column], rows[b][ok.Column])
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
		for partEnd < len(rows) && samePartitionRows(rows[i], rows[partEnd], wc.PartitionBy, rc.partition) {
			partEnd++
		}
		computePartitionRowOriented(rows[i:partEnd], wc, rc)
		i = partEnd
	}
}

func samePartitionRows(a, b map[string]any, partCols []string, cmps []boxedCompare) bool {
	for i, col := range partCols {
		if cmps[i](a[col], b[col]) != 0 {
			return false
		}
	}
	return true
}

func sameOrderRows(a, b map[string]any, orderCols []SortKey, cmps []boxedCompare) bool {
	for i, ok := range orderCols {
		if cmps[i](a[ok.Column], b[ok.Column]) != 0 {
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

// rowPeerGroups labels every row of a partition with its ORDER-BY peer group,
// the row-oriented twin of columnarPeerGroups.
func rowPeerGroups(part []map[string]any, orderBy []SortKey, cmps []boxedCompare) ([]int32, []int32) {
	n := len(part)
	lo := make([]int32, n)
	hi := make([]int32, n)
	for i := 0; i < n; {
		j := i + 1
		for j < n && sameOrderRows(part[i], part[j], orderBy, cmps) {
			j++
		}
		for k := i; k < j; k++ {
			lo[k], hi[k] = int32(i), int32(j)
		}
		i = j
	}
	return lo, hi
}

// computePartitionRowOriented computes a window function for one partition.
func computePartitionRowOriented(part []map[string]any, wc WindowColumn, rc windowRowCompares) {
	n := len(part)
	outCol := wc.OutputCol

	// Same frame resolution as computePartitionColumnar — the two paths owe
	// the same answer, and the spill path silently disagreeing with the
	// in-memory one is the failure mode nobody notices (#350).
	var fr resolvedFrame
	if frameSensitive(wc.Func) {
		fr = resolveFrame(wc, n, len(wc.OrderBy) > 0, func() ([]int32, []int32) {
			return rowPeerGroups(part, wc.OrderBy, rc.order)
		})
	}

	switch wc.Func {
	case WinRowNumber:
		for i := 0; i < n; i++ {
			part[i][outCol] = int64(i + 1)
		}

	case WinRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameOrderRows(part[i-1], part[i], wc.OrderBy, rc.order) {
				rank = int64(i + 1)
			}
			part[i][outCol] = rank
		}

	case WinDenseRank:
		rank := int64(1)
		for i := 0; i < n; i++ {
			if i > 0 && !sameOrderRows(part[i-1], part[i], wc.OrderBy, rc.order) {
				rank++
			}
			part[i][outCol] = rank
		}

	case WinSum:
		var sum float64
		curLo, curHi := 0, 0
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			curLo, curHi = slideRowSum(part, wc.InputCol, lo, hi, curLo, curHi, &sum)
			if hi <= lo {
				part[i][outCol] = nil
				continue
			}
			part[i][outCol] = sum
		}

	case WinCount:
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			if hi < lo {
				hi = lo
			}
			part[i][outCol] = int64(hi - lo)
		}

	case WinAvg:
		var sum float64
		curLo, curHi := 0, 0
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			curLo, curHi = slideRowSum(part, wc.InputCol, lo, hi, curLo, curHi, &sum)
			if hi <= lo {
				part[i][outCol] = nil
				continue
			}
			part[i][outCol] = sum / float64(hi-lo)
		}

	case WinMin, WinMax:
		want := -1
		if wc.Func == WinMax {
			want = 1
		}
		d := &frameMinMaxDeque{
			want: want,
			get:  func(i int) any { return part[i][wc.InputCol] },
			cmp: func(i, j int) int {
				return rc.input(part[i][wc.InputCol], part[j][wc.InputCol])
			},
		}
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			if hi < lo {
				hi = lo
			}
			part[i][outCol] = d.value(lo, hi)
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
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			if hi <= lo {
				part[i][outCol] = nil
				continue
			}
			part[i][outCol] = part[lo][wc.InputCol]
		}

	case WinLastValue:
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			if hi <= lo {
				part[i][outCol] = nil
				continue
			}
			part[i][outCol] = part[hi-1][wc.InputCol]
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
				if i > 0 && !sameOrderRows(part[i-1], part[i], wc.OrderBy, rc.order) {
					rank = int64(i + 1)
				}
				part[i][outCol] = float64(rank-1) / float64(n-1)
			}
		}

	case WinCumeDist:
		for i := 0; i < n; {
			j := i + 1
			for j < n && sameOrderRows(part[i], part[j], wc.OrderBy, rc.order) {
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
		for i := 0; i < n; i++ {
			lo, hi := fr.bounds(i)
			if pos := lo + nth - 1; pos < hi {
				part[i][outCol] = part[pos][wc.InputCol]
			} else {
				part[i][outCol] = nil
			}
		}
	}
}

// slideRowSum is slideFrameSum over row maps.
func slideRowSum(part []map[string]any, inputCol string, lo, hi, curLo, curHi int, sum *float64) (int, int) {
	if hi < lo {
		hi = lo
	}
	for curHi < hi {
		*sum += rowFloat64(part[curHi], inputCol)
		curHi++
	}
	for curLo < lo {
		*sum -= rowFloat64(part[curLo], inputCol)
		curLo++
	}
	return curLo, curHi
}
