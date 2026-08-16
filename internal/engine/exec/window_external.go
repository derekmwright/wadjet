package exec

import (
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ---- External window: partition-at-a-time over externally sorted runs ----
//
// The legacy spill path read every spilled row back into []map[string]any and
// computed row-oriented — peak finalize memory was the whole input, boxed.
// The external path sorts spilled runs by (PARTITION BY, ORDER BY), streams a
// k-way merge, and materializes ONE partition at a time: peak memory is the
// largest partition plus one buffered batch per run. Window columns with an
// empty PARTITION BY form a single partition spanning the input — that case
// streams through the two-pass evaluator in window_global.go instead of
// materializing (lead/cume_dist hold back a bounded lookahead window).
//
// Window columns are grouped by identical (PartitionBy, OrderBy). Each group
// is one pass: re-sort runs by the group's keys (skipped for the first group,
// whose runs were written pre-sorted at spill time), merge, walk partitions,
// compute the group's columns, and either write augmented runs for the next
// pass or stream final batches out of Next().

// windowSpecGroup is a set of window columns sharing one sort order.
type windowSpecGroup struct {
	partitionBy []string
	orderBy     []SortKey
	sortKeys    []SortKey // partitionBy (ascending) then orderBy — the pass's sort order
	cols        []WindowColumn
	origIdx     []int // positions in Window.Columns, for output-schema ordering
}

func sameSortKeys(a, b []SortKey) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// groupWindowSpecs buckets window columns by identical (PartitionBy, OrderBy),
// preserving first-occurrence order.
func groupWindowSpecs(cols []WindowColumn) []windowSpecGroup {
	var groups []windowSpecGroup
	for k, wc := range cols {
		found := -1
		for gi := range groups {
			if sameStrings(groups[gi].partitionBy, wc.PartitionBy) && sameSortKeys(groups[gi].orderBy, wc.OrderBy) {
				found = gi
				break
			}
		}
		if found < 0 {
			sk := make([]SortKey, 0, len(wc.PartitionBy)+len(wc.OrderBy))
			for _, pc := range wc.PartitionBy {
				sk = append(sk, SortKey{Column: pc, Order: Ascending})
			}
			sk = append(sk, wc.OrderBy...)
			groups = append(groups, windowSpecGroup{
				partitionBy: wc.PartitionBy,
				orderBy:     wc.OrderBy,
				sortKeys:    sk,
			})
			found = len(groups) - 1
		}
		groups[found].cols = append(groups[found].cols, wc)
		groups[found].origIdx = append(groups[found].origIdx, k)
	}
	return groups
}

// ---- Partition walker ----

// partitionWalker segments a sorted merge stream at PARTITION BY boundaries.
// Each yielded partition is a list of selection-vector views over the
// merger's output batches. charge, when set, receives byte deltas as
// partition segments accumulate; the caller releases via releasePartition —
// this makes a pathologically large partition visible to the memory tracker.
type partitionWalker struct {
	m           *runMerger
	partitionBy []string
	partIdxs    []int
	compare     []kernel.SortCompareKernel
	charge      func(delta int64)

	cur      []*batch.RecordBatch // segments of the partition being accumulated
	curBytes int64
	lastB    *batch.RecordBatch // last row of the previous segment (boundary compare)
	lastRow  int

	pending    *batch.RecordBatch // batch with rows left to scan after a boundary
	pendingPos int
	resolved   bool
	eof        bool
}

func newPartitionWalker(m *runMerger, partitionBy []string, charge func(delta int64)) *partitionWalker {
	w := &partitionWalker{m: m, partitionBy: partitionBy, charge: charge}
	w.partIdxs = make([]int, len(partitionBy))
	for i := range w.partIdxs {
		w.partIdxs[i] = -1
	}
	w.compare = make([]kernel.SortCompareKernel, len(partitionBy))
	return w
}

// resolve binds partition column indices and comparison kernels against the
// first batch's schema. Null-aware kernels report null==null as 0, so
// all-null key runs group into one partition (sameColumnar semantics).
func (w *partitionWalker) resolve(b *batch.RecordBatch) {
	for i, name := range w.partitionBy {
		idx := b.ColumnIndex(name)
		if idx >= len(b.Columns) {
			continue
		}
		w.partIdxs[i] = idx
		w.compare[i] = kernel.ResolveSortCompare(b.Columns[idx].Type)
	}
	w.resolved = true
}

// sameRow reports whether two rows agree on every partition column.
func (w *partitionWalker) sameRow(ba *batch.RecordBatch, ra int, bb *batch.RecordBatch, rb int) bool {
	for i, idx := range w.partIdxs {
		if idx < 0 || w.compare[i] == nil {
			continue
		}
		if w.compare[i](ba.Columns[idx], ra, bb.Columns[idx], rb) != 0 {
			return false
		}
	}
	return true
}

// appendSegment adds rows [from, to) of b to the current partition as a
// selection-vector view over b's columns.
func (w *partitionWalker) appendSegment(b *batch.RecordBatch, from, to int) {
	if to <= from {
		return
	}
	sel := make([]uint32, to-from)
	for i := range sel {
		sel[i] = uint32(from + i)
	}
	seg := &batch.RecordBatch{Columns: b.Columns, Schema: b.Schema, Len: b.Len, Sel: sel}
	w.cur = append(w.cur, seg)
	w.lastB = b
	w.lastRow = to - 1
	if w.charge != nil {
		// Approximate: charge the backing batch's bytes per segment. Segments
		// of one batch share backing arrays, but the case needing visibility
		// — one partition spanning many batches — is charged accurately.
		cost := b.MemBytes()
		w.curBytes += cost
		w.charge(cost)
	}
}

func (w *partitionWalker) takeCurrent() ([]*batch.RecordBatch, int64) {
	parts, bytes := w.cur, w.curBytes
	w.cur = nil
	w.curBytes = 0
	w.lastB = nil
	return parts, bytes
}

// releaseCurrent drops the charge for a partition still being accumulated —
// the teardown path for an error mid-accumulation. Without it the charge
// (ForceReserve on the worker-lifetime shared tracker) was stranded
// permanently: up to largest-partition-sized phantom reservation per failed
// window query, poisoning admission and spill decisions for later tasks.
func (w *partitionWalker) releaseCurrent() {
	if w.curBytes > 0 && w.charge != nil {
		w.charge(-w.curBytes)
	}
	w.cur = nil
	w.curBytes = 0
	w.lastB = nil
	w.pending = nil
}

// releasePartition returns a finished partition's charge to the tracker.
func (w *partitionWalker) releasePartition(bytes int64) {
	if w.charge != nil && bytes > 0 {
		w.charge(-bytes)
	}
}

// nextPartition returns the segments of the next complete partition and their
// charged bytes, or (nil, 0, nil) when the stream is exhausted. With no
// partition columns the whole input is one partition.
func (w *partitionWalker) nextPartition() ([]*batch.RecordBatch, int64, error) {
	for {
		var b *batch.RecordBatch
		var start int
		switch {
		case w.pending != nil:
			b, start = w.pending, w.pendingPos
			w.pending = nil
		case !w.eof:
			nb, err := w.m.Next()
			if err != nil {
				return nil, 0, err
			}
			if nb == nil {
				w.eof = true
				continue
			}
			if nb.Len == 0 {
				continue
			}
			if !w.resolved {
				w.resolve(nb)
			}
			b, start = nb, 0
		default:
			// EOF: flush the final open partition.
			if len(w.cur) > 0 {
				parts, bytes := w.takeCurrent()
				return parts, bytes, nil
			}
			return nil, 0, nil
		}

		// Does this batch continue the open partition?
		if w.lastB != nil && len(w.partIdxs) > 0 && !w.sameRow(w.lastB, w.lastRow, b, start) {
			parts, bytes := w.takeCurrent()
			w.pending, w.pendingPos = b, start
			return parts, bytes, nil
		}

		if len(w.partIdxs) == 0 {
			// No PARTITION BY — single partition, no boundaries to find.
			w.appendSegment(b, start, b.Len)
			continue
		}

		boundary := -1
		for i := start + 1; i < b.Len; i++ {
			if !w.sameRow(b, i-1, b, i) {
				boundary = i
				break
			}
		}
		if boundary < 0 {
			w.appendSegment(b, start, b.Len)
			continue
		}
		w.appendSegment(b, start, boundary)
		parts, bytes := w.takeCurrent()
		w.pending, w.pendingPos = b, boundary
		return parts, bytes, nil
	}
}

// ---- Window passes over runs ----

// resortRunsByKeys rewrites runs sorted by a new key set: stream every run,
// buffer batches up to minSortRunBytes, sort each buffer with the typed
// kernels, and write it as a new run. Consumed runs are deleted. Memory is
// bounded by one buffer regardless of total spilled bytes.
func resortRunsByKeys(dir string, schema []parquet.Column, runs []string, keys []SortKey) ([]string, error) {
	var out []string
	var buf []*batch.RecordBatch
	var bufRows int
	var bufBytes int64
	flush := func() error {
		if len(buf) == 0 {
			return nil
		}
		p, err := sortBatchesToRun(dir, schema, buf, bufRows, keys, 0)
		if err != nil {
			return err
		}
		if p != "" {
			out = append(out, p)
		}
		buf, bufRows, bufBytes = nil, 0, 0
		return nil
	}
	// Error contract: delete the rewritten outputs AND the input runs —
	// the operation is unrecoverable, and inputs are otherwise deleted
	// only on the success path below.
	for _, rp := range runs {
		r, err := openSpillBatchReader(rp)
		if err != nil {
			removeRunFiles(out)
			removeRunFiles(runs)
			return nil, err
		}
		for {
			b, err := r.Next()
			if err != nil {
				r.Close()
				removeRunFiles(out)
				removeRunFiles(runs)
				return nil, err
			}
			if b == nil {
				break
			}
			buf = append(buf, b)
			bufRows += b.Len
			bufBytes += b.MemBytes()
			if bufBytes >= minSortRunBytes {
				if err := flush(); err != nil {
					r.Close()
					removeRunFiles(out)
					removeRunFiles(runs)
					return nil, err
				}
			}
		}
		r.Close()
	}
	if err := flush(); err != nil {
		removeRunFiles(out)
		removeRunFiles(runs)
		return nil, err
	}
	removeRunFiles(runs)
	return out, nil
}

// openRunMerger pre-merges runs down to the fan-in cap and returns a merger
// over file cursors, plus the (possibly rewritten) run list for cleanup.
// On error every run file has been deleted (preMergeRuns cleans its own
// failures; cursor failures clean the post-merge list here) — callers just
// propagate.
func openRunMerger(dir string, schema []parquet.Column, keys []SortKey, runs []string) (*runMerger, []string, error) {
	runs, err := preMergeRuns(dir, schema, keys, runs, maxMergeFanIn, 0)
	if err != nil {
		return nil, nil, err
	}
	cursors := make([]*runCursor, 0, len(runs))
	for ord, p := range runs {
		c, err := newFileRunCursor(p)
		if err != nil {
			for _, prev := range cursors {
				prev.close()
			}
			removeRunFiles(runs)
			return nil, nil, err
		}
		c.ord = ord
		cursors = append(cursors, c)
	}
	return newRunMerger(schema, keys, cursors), runs, nil
}

// computeWindowPartition concatenates one partition's segments, appends the
// group's window output columns, and computes them. The input is already
// sorted by (partitionBy, orderBy), so no per-column re-sort happens — the
// stream order IS the window order.
func computeWindowPartition(parts []*batch.RecordBatch, schema []parquet.Column, g windowSpecGroup) *batch.RecordBatch {
	combined := windowConcatBatches(parts, schema)
	if combined == nil || combined.Len == 0 {
		return nil
	}
	n := combined.Len
	base := len(schema)

	// Copy the schema before appending: combined.Schema aliases the pass
	// schema slice and appending in place could clobber its backing array.
	outSchema := make([]parquet.Column, base, base+len(g.cols))
	copy(outSchema, schema)
	for _, wc := range g.cols {
		vec := batch.NewVector(wc.OutputType, n)
		vec.Nulls = batch.NewBitmapAllNull(n)
		combined.Columns = append(combined.Columns, vec)
		outSchema = append(outSchema, parquet.Column{
			Name:     wc.OutputCol,
			Type:     wc.OutputType,
			Nullable: true,
		})
	}
	combined.Schema = outSchema

	for i, wc := range g.cols {
		inputIdx := -1
		if wc.InputCol != "" {
			inputIdx = combined.ColumnIndex(wc.InputCol)
		}
		orderIdxs := make([]int, len(wc.OrderBy))
		for j, key := range wc.OrderBy {
			orderIdxs[j] = combined.ColumnIndex(key.Column)
		}
		computePartitionColumnar(combined, combined.Columns[base+i], 0, n, wc, inputIdx, orderIdxs)
	}
	return combined
}

// chunkBatch splits a batch into DefaultBatchSize pieces so a huge partition
// doesn't travel the pipeline (or the spill format) as one giant batch.
func chunkBatch(b *batch.RecordBatch, size int) []*batch.RecordBatch {
	if b.Len <= size {
		return []*batch.RecordBatch{b}
	}
	out := make([]*batch.RecordBatch, 0, (b.Len+size-1)/size)
	for pos := 0; pos < b.Len; {
		end := pos + size
		if end > b.Len {
			end = b.Len
		}
		n := end - pos
		chunk := batch.NewRecordBatch(b.Schema, n)
		for j := range b.Schema {
			windowCopyVectorRange(chunk.Columns[j], b.Columns[j], 0, pos, n)
		}
		out = append(out, chunk)
		pos = end
	}
	return out
}

// windowDiskPass runs one non-final spec group: merge the (sorted) runs, walk
// partitions, compute the group's columns, and write the augmented stream as
// a single new run (the pass preserves the merge's sort order). Consumed runs
// are deleted. Returns the new run list and the augmented schema.
func windowDiskPass(dir string, schema []parquet.Column, runs []string, g windowSpecGroup, charge func(int64)) ([]string, []parquet.Column, error) {
	if len(g.partitionBy) == 0 {
		// Empty PARTITION BY: the walker would accumulate the whole input
		// as one partition. Stream it twice instead (window_global.go).
		return globalWindowDiskPass(dir, schema, runs, g, charge)
	}
	merger, runs, err := openRunMerger(dir, schema, g.sortKeys, runs)
	if err != nil {
		return nil, nil, err
	}
	defer merger.close()
	walker := newPartitionWalker(merger, g.partitionBy, charge)

	outSchema := make([]parquet.Column, len(schema), len(schema)+len(g.cols))
	copy(outSchema, schema)
	for _, wc := range g.cols {
		outSchema = append(outSchema, parquet.Column{Name: wc.OutputCol, Type: wc.OutputType, Nullable: true})
	}

	sw, err := newSpillBatchWriter(dir, "window-pass")
	if err != nil {
		removeRunFiles(runs)
		return nil, nil, err
	}
	for {
		parts, bytes, err := walker.nextPartition()
		if err != nil {
			walker.releaseCurrent()
			sw.abort()
			removeRunFiles(runs)
			return nil, nil, err
		}
		if parts == nil {
			break
		}
		combined := computeWindowPartition(parts, schema, g)
		walker.releasePartition(bytes)
		if combined == nil {
			continue
		}
		for _, chunk := range chunkBatch(combined, batch.DefaultBatchSize) {
			if err := sw.writeBatch(chunk); err != nil {
				sw.abort()
				removeRunFiles(runs)
				return nil, nil, err
			}
		}
	}
	path, err := sw.close()
	if err != nil {
		removeRunFiles(runs)
		return nil, nil, err
	}
	removeRunFiles(runs)
	if path == "" {
		return nil, outSchema, nil
	}
	return []string{path}, outSchema, nil
}

// buildWindowReorder maps the final pass's combined column order
// (original schema, then each group's columns in pass order) back to the
// declared output order (original schema, then Window.Columns order).
// reorder[outPos] = combined column index.
func buildWindowReorder(numOrigCols int, groups []windowSpecGroup, numWindowCols int) []int {
	reorder := make([]int, numOrigCols+numWindowCols)
	for i := 0; i < numOrigCols; i++ {
		reorder[i] = i
	}
	offset := numOrigCols
	for _, g := range groups {
		for pi, k := range g.origIdx {
			reorder[numOrigCols+k] = offset + pi
		}
		offset += len(g.cols)
	}
	return reorder
}

// reorderBatchColumns produces a batch whose columns follow outSchema's
// order, sharing the source vectors (no copy).
func reorderBatchColumns(b *batch.RecordBatch, reorder []int, outSchema []parquet.Column) *batch.RecordBatch {
	cols := make([]*batch.Vector, len(reorder))
	for outPos, srcIdx := range reorder {
		cols[outPos] = b.Columns[srcIdx]
	}
	return &batch.RecordBatch{Columns: cols, Schema: outSchema, Len: b.Len, Sel: b.Sel}
}

// windowExtState is the final group's streaming state, driven by Next().
type windowExtState struct {
	walker    *partitionWalker
	global    *globalWindowStreamer // non-nil when the final group has no PARTITION BY
	merger    *runMerger
	schema    []parquet.Column // input schema of the final pass
	group     windowSpecGroup
	reorder   []int
	outSchema []parquet.Column
	runs      []string // run files to delete once drained
	queue     []*batch.RecordBatch
	qpos      int
	done      bool
}

func (e *windowExtState) cleanup() {
	if e.global != nil {
		e.global.release()
		e.global = nil
	}
	if e.walker != nil {
		e.walker.releaseCurrent()
		e.walker = nil
	}
	if e.merger != nil {
		e.merger.close()
		e.merger = nil
	}
	removeRunFiles(e.runs)
	e.runs = nil
	e.queue = nil
	e.done = true
}
