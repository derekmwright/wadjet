package exec

import (
	"container/heap"
	"fmt"
	"os"
	"sort"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ---- External sort: sorted-run generation + streaming k-way merge ----
//
// Sort and Window used to spill raw unsorted rows and read EVERYTHING back at
// finalize for one in-memory sort over boxed []map[string]any — spill deferred
// peak memory instead of bounding it (the finalize working set was the whole
// input, in a representation several times larger than columnar). The external
// path bounds memory for real: each spill sorts the buffered batches with the
// typed kernels and writes a sorted columnar run; finalize streams a k-way
// merge over the runs, holding one batch per run.

// minSortRunBytes is the floor on buffered bytes before Sort/Window self-spill
// a sorted run under SpillCheap pressure. Without a floor, sustained pressure
// makes every Consume flush a single-batch run — thousands of tiny files and a
// huge merge fan-in. 64 MiB matches memory.minOperatorBytes (the boot
// invariant's per-task progress floor) and HashAggregate's
// spillFileTargetBytes. Peer-demanded relief (SpillSome) ignores the floor —
// EstimateRelief promises the full buffer, so SpillSome must deliver it.
// Package var so tests can force tiny runs.
var minSortRunBytes int64 = 64 << 20

// maxMergeFanIn caps how many runs a single k-way merge holds open at once
// (one buffered batch each). Beyond the cap, runs are pre-merged in groups
// into longer runs (classic multi-level external merge), bounding merge
// memory at maxMergeFanIn × batch size. Package var so tests can force the
// multi-level path with few runs.
var maxMergeFanIn = 64

// resolvedSortKey is a sort key resolved against a concrete batch set: column
// index plus the typed comparison kernel with null ordering baked in.
type resolvedSortKey struct {
	colIdx  int
	order   SortOrder
	compare kernel.SortCompareKernel
}

// resolveSortCompareForKey picks the comparison kernel for one sort key.
//
// SortKey.NullsLast states where NULLs land in the OUTPUT. The multi-key less
// functions below apply direction by NEGATING the kernel's result (`cmp > 0`
// for DESC), and that negation hits the kernel's null handling as well as its
// value comparison — so a NULLS-LAST kernel under DESC emits NULLs FIRST.
// Null placement is not a direction-relative property: `ORDER BY k DESC NULLS
// LAST` asks for NULLs at the end of the descending sequence. The kernel is
// therefore chosen for the placement that SURVIVES the negation, which is the
// opposite one under DESC (#343). Before this, both explicit spellings were
// inverted on a DESC key while the defaults came out right by cancelling
// against the same negation.
//
// A column with no NULLs in any batch takes the null-free kernel, where
// placement cannot matter.
func resolveSortCompareForKey(typ batch.TypeID, key SortKey, nullFree bool) kernel.SortCompareKernel {
	if nullFree {
		return kernel.ResolveSortCompareNoNulls(typ)
	}
	nullsLast := key.NullsLast
	if key.Order == Descending {
		nullsLast = !nullsLast
	}
	if nullsLast {
		return kernel.ResolveSortCompareNullsLast(typ)
	}
	return kernel.ResolveSortCompare(typ)
}

// resolveSortKeysForBatches resolves key columns and picks comparison kernels.
// Null ordering (NULLS FIRST vs NULLS LAST) is baked into kernel selection;
// the null-free fast kernel is used only when no batch has nulls in the column.
//
// A resolved column that gets no comparator is an ERROR, not a skip: every
// one of the 22 types resolves (kernel.ResolveSortCompareCoversEveryType),
// so a nil here means a key's column type is one this engine cannot order at
// all, and silently dropping it from the comparison — as the two callers
// below used to — merges every value of that key together, exactly as a
// missing PARTITION BY key merges every partition. Mirrors
// SortMergeJoin.resolveCompareKernels (sort_merge_join.go), which already
// fails loudly on the same condition for join keys.
func resolveSortKeysForBatches(keys []SortKey, batches []*batch.RecordBatch) ([]resolvedSortKey, error) {
	resolved := make([]resolvedSortKey, len(keys))
	if len(batches) == 0 {
		for i, key := range keys {
			resolved[i] = resolvedSortKey{colIdx: -1, order: key.Order}
		}
		return resolved, nil
	}
	firstBatch := batches[0]
	for i, key := range keys {
		// Qualified/bare fallback, the same resolution the rest of the
		// engine uses (columnIndexFallback). A join qualifies only the
		// BUILD side's colliding names, so a self-joined table's
		// probe-side alias arrives bare: the planner's key `n1.n_name`
		// found no exact match, this loop set idx = -1, and the key was
		// silently skipped — TPC-H Q07 came back with correct rows in
		// arbitrary order (#314). A key that matches nothing is still
		// skipped, as before — that is a missing COLUMN, a different
		// failure than an unsupported TYPE.
		// key.index, not columnIndexFallback: the planner may have supplied
		// the key's POSITION, which is the only address that survives two
		// output columns sharing a name (#557).
		idx := key.index(firstBatch)
		if idx >= len(firstBatch.Columns) {
			idx = -1 // schema/column count mismatch — skip this key
		}
		var cmp kernel.SortCompareKernel
		if idx >= 0 {
			colType := firstBatch.Columns[idx].Type
			allNullFree := true
			for _, b := range batches {
				if idx >= len(b.Columns) || b.Columns[idx].Nulls.HasNulls() {
					allNullFree = false
					break
				}
			}
			cmp = resolveSortCompareForKey(colType, key, allNullFree)
			if cmp == nil {
				return nil, fmt.Errorf("sort: unsupported key type %d for %q", colType, key.Column)
			}
		}
		resolved[i] = resolvedSortKey{colIdx: idx, order: key.Order, compare: cmp}
	}
	return resolved, nil
}

// buildSortEntries flattens the active rows of batches into sort entries.
func buildSortEntries(batches []*batch.RecordBatch, totalRows int) []sortEntry {
	entries := make([]sortEntry, 0, totalRows)
	for bi, b := range batches {
		if b.Sel != nil {
			for _, idx := range b.Sel {
				entries = append(entries, sortEntry{batchIdx: uint32(bi), rowIdx: idx})
			}
		} else {
			for i := 0; i < b.Len; i++ {
				entries = append(entries, sortEntry{batchIdx: uint32(bi), rowIdx: uint32(i)})
			}
		}
	}
	return entries
}

// sortEntriesLessFunc builds the multi-key comparison closure over a batch set.
func sortEntriesLessFunc(resolved []resolvedSortKey, batches []*batch.RecordBatch) func(ei, ej sortEntry) bool {
	return func(ei, ej sortEntry) bool {
		bi, bj := batches[ei.batchIdx], batches[ej.batchIdx]
		ri, rj := int(ei.rowIdx), int(ej.rowIdx)
		for _, key := range resolved {
			// colIdx < 0 is a column resolveSortKeysForBatches could not
			// find — a legitimate skip, unrelated to type support. A key
			// that resolved a column always carries a non-nil compare:
			// resolveSortKeysForBatches errors out before returning one
			// otherwise, so this loop never has to guess what an
			// unorderable type means for the sequence.
			if key.colIdx < 0 || key.colIdx >= len(bi.Columns) || key.colIdx >= len(bj.Columns) {
				continue
			}
			cmp := key.compare(bi.Columns[key.colIdx], ri, bj.Columns[key.colIdx], rj)
			if cmp == 0 {
				continue
			}
			if key.order == Descending {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	}
}

// selectSortedEntries sorts entries; when 0 < limit < len/2 it uses the
// bounded heap to find the top limit entries in O(n log k), and in all
// limited cases truncates the result to limit (rows past limit in a sorted
// run can never appear in the global top-K). limit < 0 (NoLimit) means
// unbounded; limit == 0 is a real, empty top-K — the heap path is already
// guarded by limit > 0, so without this case the function would fall through
// and return EVERY entry (the #481 bug), not panic.
func selectSortedEntries(entries []sortEntry, less func(a, b sortEntry) bool, limit int) []sortEntry {
	if limit == 0 {
		return entries[:0]
	}
	if limit > 0 && limit < len(entries)/2 {
		k := limit
		h := &topNHeap{
			entries: make([]sortEntry, k),
			less:    less,
		}
		copy(h.entries, entries[:k])
		heap.Init(h)
		for _, e := range entries[k:] {
			if less(e, h.entries[0]) {
				h.entries[0] = e
				heap.Fix(h, 0)
			}
		}
		entries = h.entries
		sort.SliceStable(entries, func(i, j int) bool {
			return less(entries[i], entries[j])
		})
		return entries
	}
	sort.SliceStable(entries, func(i, j int) bool {
		return less(entries[i], entries[j])
	})
	if limit > 0 && limit < len(entries) {
		entries = entries[:limit]
	}
	return entries
}

// gatherEntriesBatch materializes entries[from:to] into a fresh batch using
// the typed gather kernels. Columns missing from any source batch stay
// zero-valued, matching finalizeColumnar's tolerance for schema drift.
func gatherEntriesBatch(schema []parquet.Column, batches []*batch.RecordBatch, entries []sortEntry) *batch.RecordBatch {
	out := batch.NewRecordBatch(schema, len(entries))
	for j := range schema {
		allHaveCol := true
		for _, e := range entries {
			if j >= len(batches[e.batchIdx].Columns) {
				allHaveCol = false
				break
			}
		}
		if allHaveCol {
			gatherSortVector(out.Columns[j], j, entries, batches)
		}
	}
	return out
}

// writeSortedRun gathers entries (already sorted) into DefaultBatchSize
// chunks and writes them as one columnar run file. Returns "" when entries
// is empty.
func writeSortedRun(dir string, schema []parquet.Column, batches []*batch.RecordBatch, entries []sortEntry) (string, error) {
	if len(entries) == 0 {
		return "", nil
	}
	sw, err := newSpillBatchWriter(dir, "sort-run")
	if err != nil {
		return "", err
	}
	for pos := 0; pos < len(entries); {
		end := pos + batch.DefaultBatchSize
		if end > len(entries) {
			end = len(entries)
		}
		if err := sw.writeBatch(gatherEntriesBatch(schema, batches, entries[pos:end])); err != nil {
			sw.abort()
			return "", err
		}
		pos = end
	}
	return sw.close()
}

// sortBatchesToRun sorts the active rows of batches by keys and writes them
// as one sorted run file. limit >= 0 truncates the run to its top limit
// rows (limit == 0 writes an empty run); limit < 0 (NoLimit) writes every row.
func sortBatchesToRun(dir string, schema []parquet.Column, batches []*batch.RecordBatch, totalRows int, keys []SortKey, limit int) (string, error) {
	entries := buildSortEntries(batches, totalRows)
	resolved, err := resolveSortKeysForBatches(keys, batches)
	if err != nil {
		return "", err
	}
	entries = selectSortedEntries(entries, sortEntriesLessFunc(resolved, batches), limit)
	return writeSortedRun(dir, schema, batches, entries)
}

// ---- Run cursors ----

// runCursor walks one sorted run — either a spill file (streamed one batch at
// a time) or the operator's in-memory remainder (sorted entries gathered
// lazily in DefaultBatchSize chunks). cur==nil means exhausted.
type runCursor struct {
	// file-backed run
	reader *spillBatchReader
	path   string

	// in-memory run
	memSchema  []parquet.Column
	memBatches []*batch.RecordBatch
	memEntries []sortEntry
	memPos     int

	cur *batch.RecordBatch
	row int
	ord int // arrival order of the run; merge tie-break for determinism
}

func newFileRunCursor(path string) (*runCursor, error) {
	r, err := openSpillBatchReader(path)
	if err != nil {
		return nil, err
	}
	c := &runCursor{reader: r, path: path}
	if err := c.loadNext(); err != nil {
		c.close()
		return nil, err
	}
	return c, nil
}

func newMemRunCursor(schema []parquet.Column, batches []*batch.RecordBatch, entries []sortEntry) (*runCursor, error) {
	c := &runCursor{memSchema: schema, memBatches: batches, memEntries: entries}
	if err := c.loadNext(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *runCursor) loadNext() error {
	c.row = 0
	if c.reader != nil {
		b, err := c.reader.Next()
		if err != nil {
			return err
		}
		c.cur = b
		return nil
	}
	if c.memPos >= len(c.memEntries) {
		c.cur = nil
		return nil
	}
	end := c.memPos + batch.DefaultBatchSize
	if end > len(c.memEntries) {
		end = len(c.memEntries)
	}
	c.cur = gatherEntriesBatch(c.memSchema, c.memBatches, c.memEntries[c.memPos:end])
	c.memPos = end
	return nil
}

// advance moves to the next row, loading the next batch at a batch boundary.
func (c *runCursor) advance() error {
	c.row++
	if c.cur != nil && c.row < c.cur.Len {
		return nil
	}
	return c.loadNext()
}

func (c *runCursor) close() {
	if c.reader != nil {
		c.reader.Close()
		c.reader = nil
	}
	c.memBatches = nil
	c.memEntries = nil
	c.cur = nil
}

// ---- K-way merge ----

// mergeHeap orders cursors by their current row using the resolved keys;
// ties break on run arrival order so equal keys keep input order (matching
// the stable in-memory sort).
type mergeHeap struct {
	cursors  []*runCursor
	resolved []resolvedSortKey
}

func (h *mergeHeap) Len() int { return len(h.cursors) }

func (h *mergeHeap) Less(i, j int) bool {
	a, b := h.cursors[i], h.cursors[j]
	for _, key := range h.resolved {
		// colIdx < 0 is a missing column, a legitimate skip; a resolved
		// column always carries a non-nil compare (newRunMerger errors out
		// before building this heap otherwise).
		if key.colIdx < 0 || key.colIdx >= len(a.cur.Columns) || key.colIdx >= len(b.cur.Columns) {
			continue
		}
		cmp := key.compare(a.cur.Columns[key.colIdx], a.row, b.cur.Columns[key.colIdx], b.row)
		if cmp == 0 {
			continue
		}
		if key.order == Descending {
			return cmp > 0
		}
		return cmp < 0
	}
	return a.ord < b.ord
}

func (h *mergeHeap) Swap(i, j int) { h.cursors[i], h.cursors[j] = h.cursors[j], h.cursors[i] }
func (h *mergeHeap) Push(x any)    { h.cursors = append(h.cursors, x.(*runCursor)) }
func (h *mergeHeap) Pop() any {
	old := h.cursors
	n := len(old)
	x := old[n-1]
	h.cursors = old[:n-1]
	return x
}

// runMerger streams a k-way merge over sorted runs, one output batch at a
// time. Memory held: one buffered batch per live cursor.
type runMerger struct {
	h      *mergeHeap
	schema []parquet.Column
}

// newRunMerger builds a merger over the cursors. Comparison kernels are
// resolved from the first live cursor's batch (all runs share the schema);
// always null-aware because per-run null-freedom isn't tracked across files.
//
// A resolved column that gets no comparator is an error for the same reason
// resolveSortKeysForBatches treats it as one: every type this engine has
// resolves, so nil means the key's column type cannot be ordered at all, and
// merging its runs as if every value tied — the old behavior — silently
// scrambles the k-way merge order instead of refusing it.
func newRunMerger(schema []parquet.Column, keys []SortKey, cursors []*runCursor) (*runMerger, error) {
	live := make([]*runCursor, 0, len(cursors))
	for _, c := range cursors {
		if c.cur != nil {
			live = append(live, c)
		} else {
			c.close()
		}
	}
	resolved := make([]resolvedSortKey, len(keys))
	for i, key := range keys {
		resolved[i] = resolvedSortKey{colIdx: -1, order: key.Order}
		if len(live) == 0 {
			continue
		}
		first := live[0].cur
		idx := key.index(first)
		if idx >= len(first.Columns) {
			continue
		}
		cmp := resolveSortCompareForKey(first.Columns[idx].Type, key, false)
		if cmp == nil {
			for _, c := range live {
				c.close()
			}
			return nil, fmt.Errorf("sort merge: unsupported key type %d for %q", first.Columns[idx].Type, key.Column)
		}
		resolved[i] = resolvedSortKey{colIdx: idx, order: key.Order, compare: cmp}
	}
	h := &mergeHeap{cursors: live, resolved: resolved}
	heap.Init(h)
	return &runMerger{h: h, schema: schema}, nil
}

// mergeRef pins one merged output row: the source batch stays alive until the
// rows referencing it are materialized.
type mergeRef struct {
	b   *batch.RecordBatch
	row int
}

// Next returns the next merged batch (up to DefaultBatchSize rows), or
// (nil, nil) when all runs are exhausted.
func (m *runMerger) Next() (*batch.RecordBatch, error) {
	if m.h.Len() == 0 {
		return nil, nil
	}
	refs := make([]mergeRef, 0, batch.DefaultBatchSize)
	for len(refs) < batch.DefaultBatchSize && m.h.Len() > 0 {
		c := m.h.cursors[0]
		refs = append(refs, mergeRef{b: c.cur, row: c.row})
		if err := c.advance(); err != nil {
			return nil, err
		}
		if c.cur == nil {
			c.close()
			heap.Pop(m.h)
		} else {
			heap.Fix(m.h, 0)
		}
	}
	out := batch.NewRecordBatch(m.schema, len(refs))
	for j := range m.schema {
		dst := out.Columns[j]
		for di, r := range refs {
			if j >= len(r.b.Columns) {
				continue
			}
			copyVectorValue(dst, di, r.b.Columns[j], r.row)
		}
	}
	return out, nil
}

func (m *runMerger) close() {
	for _, c := range m.h.cursors {
		c.close()
	}
	m.h.cursors = nil
}

// preMergeRuns reduces the run count to at most maxRuns by merging groups of
// maxMergeFanIn runs into longer runs (multi-level external merge). Consumed
// run files are deleted. limit >= 0 truncates intermediate runs (sorted, so
// rows past limit cannot reach the global top-K); limit < 0 (NoLimit) keeps
// every row.
//
// Ownership contract: on error, every run file involved — accumulated
// intermediates and not-yet-consumed inputs alike — has been deleted. The
// query is failing; stranding scratch in the worker-lifetime spill dir
// (which has no per-task RemoveAll) would self-compound the ENOSPC
// conditions these merges die under.
func preMergeRuns(dir string, schema []parquet.Column, keys []SortKey, runs []string, maxRuns, limit int) ([]string, error) {
	for len(runs) > maxRuns {
		next := make([]string, 0, (len(runs)+maxMergeFanIn-1)/maxMergeFanIn)
		for i := 0; i < len(runs); i += maxMergeFanIn {
			end := i + maxMergeFanIn
			if end > len(runs) {
				end = len(runs)
			}
			group := runs[i:end]
			if len(group) == 1 {
				next = append(next, group[0])
				continue
			}
			path, err := mergeRunsToFile(dir, schema, keys, group, limit)
			if err != nil {
				// mergeRunsToFile removed its partial output; runs[:i]
				// were consumed and deleted by earlier groups.
				removeRunFiles(next)
				removeRunFiles(runs[i:])
				return nil, err
			}
			for _, p := range group {
				os.Remove(p)
			}
			if path != "" {
				next = append(next, path)
			}
		}
		runs = next
	}
	return runs, nil
}

// mergeRunsToFile merges the given runs into one new run file. limit == 0
// (a real top-K of zero) short-circuits to the same "" empty-output
// convention writeSortedRun uses for zero entries — the caller still owns
// removing the input run files, so no cursors need to be opened at all.
func mergeRunsToFile(dir string, schema []parquet.Column, keys []SortKey, runs []string, limit int) (string, error) {
	if limit == 0 {
		return "", nil
	}
	cursors := make([]*runCursor, 0, len(runs))
	closeAll := func() {
		for _, c := range cursors {
			c.close()
		}
	}
	for ord, p := range runs {
		c, err := newFileRunCursor(p)
		if err != nil {
			closeAll()
			return "", err
		}
		c.ord = ord
		cursors = append(cursors, c)
	}
	m, err := newRunMerger(schema, keys, cursors)
	if err != nil {
		closeAll()
		return "", err
	}
	defer m.close()

	sw, err := newSpillBatchWriter(dir, "sort-merge")
	if err != nil {
		return "", err
	}
	written := 0
	for {
		b, err := m.Next()
		if err != nil {
			sw.abort()
			return "", err
		}
		if b == nil {
			break
		}
		if limit > 0 && written+b.Len > limit {
			b.Len = limit - written // safe: columnar format writes b.Len rows
		}
		if err := sw.writeBatch(b); err != nil {
			sw.abort()
			return "", err
		}
		written += b.Len
		if limit > 0 && written >= limit {
			break
		}
	}
	return sw.close()
}

// removeRunFiles best-effort deletes run files (spill scratch).
func removeRunFiles(paths []string) {
	for _, p := range paths {
		os.Remove(p)
	}
}
