package exec

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec/kernel"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// SortMergeJoin joins two large inputs by sorting both sides on the join keys
// and streaming a two-cursor merge. Unlike HashJoin, neither side is held
// resident: each side buffers under the shared tracker and self-spills sorted
// columnar runs (Sort's external-merge machinery), so peak memory is
// O(run buffer + one batch per merge cursor) regardless of input size.
//
// The build side (right, as in HashJoin) arrives via Build; the probe side
// (left) arrives via Consume. Both are pipeline breakers here — inherent to
// sort-based joins over unsorted input. After Finalize, Next streams joined
// batches: probe columns first, then build columns, with the same
// duplicate-name qualification and OutputFilter semantics as HashJoinProbe.
//
// v1 scope (docs/design/sort-merge-join.md): INNER equi-joins only, no
// JoinFilter. Rows with a NULL in any join key are excluded at buffer time
// (SQL equi-join semantics: NULL matches nothing — mirrors the hash paths,
// where null keys produce no index entry). Not Cloneable: the breaker path
// runs it single-consumer.
type SortMergeJoin struct {
	JoinType  JoinType // v1: InnerJoin only; validated in Init/Finalize
	LeftKeys  []string // join key columns from left (probe) side
	RightKeys []string // join key columns from right (build) side

	// BuildTableAlias / BuildColOrigins / QualifyAllBuildCols / OutputFilter
	// carry HashJoinProbe's output-schema semantics — see
	// joinOutputSchemaWithMapping.
	BuildTableAlias     string
	BuildColOrigins     map[string]string
	QualifyAllBuildCols bool
	OutputFilter        map[string]bool

	// Spill enables tracker accounting and spill-to-disk. When nil the
	// operator buffers unbounded in memory (embedded/test paths), like Sort.
	Spill *memory.SpillManager

	mu        sync.Mutex
	build     smjSide // right side
	probe     smjSide // left side
	buildDone bool
	// finalized is set once Finalize begins; past that point the buffered
	// batches are owned by the merge and there is nothing left to spill.
	finalized bool

	// AccountedOperator state — one registration owns both side states;
	// see Sort for the contract.
	accInstanceID       uint64
	accState            atomic.Int32
	unregisterAccounted func()

	// Merge state, set up by Finalize and drained by Next.
	outSchema  []parquet.Column
	outMapping []outColSource
	lstream    *smjStream
	rstream    *smjStream
	compare    []kernel.SortCompareKernel // per join key, resolved once

	// Duplicate-group state: the build side's current key group, held as
	// refs into merge-output batches (no copying). A batch the group still
	// references after the cursor moves past it is pinned: its bytes are
	// tracker-Reserved, so a pathologically hot key that drags a growing
	// tail of batches fails loudly via Reserve instead of OOMing (the
	// documented v1 bound). Groups contained in the live cursor batch —
	// the overwhelmingly common case — reserve nothing.
	group struct {
		refs     []mergeRef
		reserved int64
	}
	groupActive bool
	groupPos    int // next group row to pair with the current probe row
	// pendingRelease accumulates retired groups' reservations; released
	// after the flush that materializes the pairs still referencing them.
	pendingRelease int64

	pending   []smjPair
	exhausted bool
	done      bool
}

// smjSide is one side's buffering/spill core: Sort's accumulate → self-spill
// sorted runs pattern, parameterized by the side's join-key sort keys.
type smjSide struct {
	name string // "probe" / "build", for error context
	keys []SortKey
	// counterpart holds the OTHER side's key names, positionally paired.
	// SQL may put the build-side column on the left of "=" ("JOIN t ON
	// t.id = probe.id"), so a side's assigned key can belong to the other
	// side; when the own name doesn't resolve against the first batch, the
	// counterpart name is tried — symmetric adoption on both sides is
	// exactly the pair swap HashJoin.FixKeyAssignment performs post-build.
	counterpart []string
	schema      []parquet.Column
	batches     []*batch.RecordBatch
	totalRows   int
	trackedMem  int64
	runFiles    []string
}

// smjPair pins one joined output row. The referenced batches are merge output
// (fresh, unpooled), kept alive by the reference until the pending flush.
type smjPair struct {
	lb   *batch.RecordBatch
	lrow uint32
	rb   *batch.RecordBatch
	rrow uint32
}

// smjStream is a row cursor over one side's fully merged sorted stream: the
// side's spilled runs plus its in-memory remainder, collapsed by a runMerger.
// cur==nil means exhausted.
type smjStream struct {
	merger *runMerger
	runs   []string // run files to delete at teardown
	keyIdx []int    // join-key column indices in the merged schema
	cur    *batch.RecordBatch
	row    int
}

func (s *smjStream) advance() error {
	s.row++
	if s.cur != nil && s.row < s.cur.Len {
		return nil
	}
	b, err := s.merger.Next()
	if err != nil {
		return err
	}
	s.cur = b
	s.row = 0
	return nil
}

func (s *smjStream) close() {
	if s.merger != nil {
		s.merger.close()
		s.merger = nil
	}
	removeRunFiles(s.runs)
	s.runs = nil
	s.cur = nil
}

// NewSortMergeJoin creates an inner sort-merge join. leftKeys are the probe
// (left) side's join columns, rightKeys the build (right) side's, positionally
// paired.
func NewSortMergeJoin(leftKeys, rightKeys []string) *SortMergeJoin {
	j := &SortMergeJoin{
		JoinType:  InnerJoin,
		LeftKeys:  leftKeys,
		RightKeys: rightKeys,
	}
	j.probe = smjSide{name: "probe", keys: ascendingKeys(leftKeys), counterpart: rightKeys}
	j.build = smjSide{name: "build", keys: ascendingKeys(rightKeys), counterpart: leftKeys}
	return j
}

func ascendingKeys(cols []string) []SortKey {
	keys := make([]SortKey, len(cols))
	for i, c := range cols {
		keys[i] = SortKey{Column: c, Order: Ascending}
	}
	return keys
}

// Init prepares the probe/merge state. It deliberately does NOT reset the
// build side: Build runs before the probe pipeline starts (HashJoin's
// contract), and pipeline Init would otherwise wipe it.
func (j *SortMergeJoin) Init(_ context.Context) error {
	if j.JoinType != InnerJoin {
		return fmt.Errorf("sort-merge join: join type %d not supported (v1 is inner-only)", j.JoinType)
	}
	if len(j.LeftKeys) == 0 || len(j.LeftKeys) != len(j.RightKeys) {
		return fmt.Errorf("sort-merge join: key count mismatch (%d left, %d right)", len(j.LeftKeys), len(j.RightKeys))
	}
	return nil
}

// Build drains the build-side (right) source, mirroring HashJoin.Build's
// contract: inits and closes the source, reports row progress.
func (j *SortMergeJoin) Build(ctx context.Context, source Source) error {
	if err := source.Init(ctx); err != nil {
		return fmt.Errorf("build source init: %w", err)
	}
	defer source.Close()

	progress := ProgressReporterFromContext(ctx)
	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("sort-merge join build cancelled: %w", err)
		}
		b, err := source.Next(ctx)
		if err != nil {
			return fmt.Errorf("build source next: %w", err)
		}
		if b == nil {
			break
		}
		if progress != nil {
			progress.AddRows(int64(b.ActiveLen()))
		}
		if err := j.consume(&j.build, b); err != nil {
			return err
		}
		b.Release()
	}
	j.mu.Lock()
	j.buildDone = true
	j.mu.Unlock()
	return nil
}

// Consume buffers a probe-side (left) batch.
func (j *SortMergeJoin) Consume(_ context.Context, b *batch.RecordBatch) error {
	return j.consume(&j.probe, b)
}

// consume buffers one batch into a side, excluding null-key rows via the
// selection vector, tracking bytes, and self-spilling a sorted run under
// SpillCheap pressure past the run floor — Sort.Consume's checklist.
func (j *SortMergeJoin) consume(side *smjSide, b *batch.RecordBatch) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	FlattenForConsumer(b, nil) // retained past the batch cycle: views must not survive
	if side.schema == nil {
		// Resolve the side's key names against the actual schema before
		// anything sorts by them: the planner emits SQL-qualified names
		// ("l.id") while batch columns may be bare, and join outputs carry
		// qualified duplicates the other way around. Rewriting the sort keys
		// to the schema's exact names here means every downstream exact-match
		// resolution (run sorting, merge cursors, kernels) just works —
		// without this, resolveSortKeysForBatches silently SKIPS an
		// unresolvable key and the merge would run over unsorted runs.
		for i := range side.keys {
			idx := columnIndexFallback(b, side.keys[i].Column)
			if idx < 0 && i < len(side.counterpart) {
				// Swapped pair — see smjSide.counterpart.
				idx = columnIndexFallback(b, side.counterpart[i])
			}
			if idx < 0 {
				return fmt.Errorf("sort-merge join: %s-side key column %q not found", side.name, side.keys[i].Column)
			}
			side.keys[i].Column = b.Schema[idx].Name
		}
		side.schema = b.Schema
		// Register on the first buffered batch from either side (footprint
		// exists from here on). One registration covers both sides.
		if j.Spill != nil && j.unregisterAccounted == nil {
			j.accInstanceID = memory.NextInstanceID()
			j.accState.Store(int32(memory.OpActive))
			j.unregisterAccounted = j.Spill.RegisterAccounted(j)
		}
	}

	sel, err := nonNullKeySel(b, side)
	if err != nil {
		return err
	}
	if sel != nil && len(sel) == 0 {
		return nil // every active row has a NULL join key — contributes nothing
	}

	b.Detach() // prevent pool recycle — pipeline calls Release() after Consume()
	if sel != nil {
		b.Sel = sel
	} else if b.Sel != nil {
		// Snapshot the selection vector — filter operators reuse their outSel
		// scratch across calls; sinks that hold batches across calls would
		// otherwise see clobbered Sel data (see BatchSink.Consume).
		selCopy := make([]uint32, len(b.Sel))
		copy(selCopy, b.Sel)
		b.Sel = selCopy
	}
	side.batches = append(side.batches, b)
	side.totalRows += b.ActiveLen()

	if j.Spill != nil {
		cost := b.MemBytes()
		j.Spill.TrackBatch(cost)
		side.trackedMem += cost
		j.publishOwnedLocked()

		if j.Spill.ShouldSpillFor(memory.SpillCheap) && side.trackedMem >= minSortRunBytes {
			if _, err := j.flushSideLocked(side); err != nil {
				return err
			}
		}
	}
	return nil
}

// nonNullKeySel returns a fresh selection vector of active rows whose join
// keys are all non-null, or nil when no filtering is needed (no nulls in any
// key column). Errors if a key column is missing from the batch.
func nonNullKeySel(b *batch.RecordBatch, side *smjSide) ([]uint32, error) {
	keyIdx := make([]int, len(side.keys))
	anyNulls := false
	for i, k := range side.keys {
		idx := b.ColumnIndex(k.Column)
		if idx < 0 {
			return nil, fmt.Errorf("sort-merge join: %s-side key column %q not found", side.name, k.Column)
		}
		keyIdx[i] = idx
		if b.Columns[idx].Nulls.HasNulls() {
			anyNulls = true
		}
	}
	if !anyNulls {
		return nil, nil
	}
	rowOK := func(row int) bool {
		for _, idx := range keyIdx {
			if b.Columns[idx].Nulls.IsNull(row) {
				return false
			}
		}
		return true
	}
	sel := make([]uint32, 0, b.ActiveLen())
	if b.Sel != nil {
		for _, idx := range b.Sel {
			if rowOK(int(idx)) {
				sel = append(sel, idx)
			}
		}
	} else {
		for i := 0; i < b.Len; i++ {
			if rowOK(i) {
				sel = append(sel, uint32(i))
			}
		}
	}
	return sel, nil
}

// flushSideLocked drains a side's buffered batches into one sorted run file
// and releases their tracking, returning the bytes freed. Caller holds j.mu.
func (j *SortMergeJoin) flushSideLocked(side *smjSide) (int64, error) {
	if len(side.batches) == 0 || side.trackedMem == 0 {
		return 0, nil
	}
	path, err := sortBatchesToRun(j.Spill.SpillDir(), side.schema, side.batches, side.totalRows, side.keys, 0)
	if err != nil {
		return 0, err
	}
	if path != "" {
		side.runFiles = append(side.runFiles, path)
	}
	side.batches = side.batches[:0]
	side.totalRows = 0
	freed := side.trackedMem
	j.Spill.ReleaseTracking(freed)
	side.trackedMem = 0
	j.publishOwnedLocked()
	return freed, nil
}

// publishOwnedLocked reports the combined side footprint. Caller holds j.mu.
func (j *SortMergeJoin) publishOwnedLocked() {
	if j.accInstanceID != 0 {
		j.Spill.Tracker().PublishOwned(j.accInstanceID, j.probe.trackedMem+j.build.trackedMem)
	}
}

// Finalize collapses each side into one sorted stream (spilled runs + the
// in-memory remainder under a bounded-fan-in merger) and resolves the
// cross-side comparison kernels. Nothing is materialized — Next streams the
// merge one output batch at a time.
func (j *SortMergeJoin) Finalize(_ context.Context) error {
	j.mu.Lock()
	if !j.buildDone {
		j.mu.Unlock()
		return fmt.Errorf("sort-merge join: build phase not complete")
	}
	j.finalized = true
	j.mu.Unlock()
	// Past this point the buffered batches belong to the merge; deregister so
	// the relief registry stops considering us (Sort's pattern).
	if j.unregisterAccounted != nil {
		j.accState.Store(int32(memory.OpClosed))
		j.unregisterAccounted()
		j.unregisterAccounted = nil
	}
	if j.JoinType != InnerJoin {
		return fmt.Errorf("sort-merge join: join type %d not supported (v1 is inner-only)", j.JoinType)
	}

	// An empty side means an empty inner join — tear down eagerly so the
	// other side's runs and reservations don't wait for Close.
	if j.probe.schema == nil || j.build.schema == nil {
		j.mu.Lock()
		j.teardownLocked()
		j.mu.Unlock()
		return nil
	}

	j.outSchema, j.outMapping = joinOutputSchemaWithMapping(j.JoinType, j.probe.schema, j.build.schema,
		j.BuildTableAlias, j.BuildColOrigins, j.QualifyAllBuildCols, j.OutputFilter)

	if err := j.resolveCompareKernels(); err != nil {
		j.mu.Lock()
		j.teardownLocked()
		j.mu.Unlock()
		return err
	}

	ls, err := j.openStream(&j.probe)
	if err != nil {
		j.mu.Lock()
		j.teardownLocked()
		j.mu.Unlock()
		return fmt.Errorf("sort-merge join: probe stream: %w", err)
	}
	j.lstream = ls
	rs, err := j.openStream(&j.build)
	if err != nil {
		j.mu.Lock()
		j.teardownLocked()
		j.mu.Unlock()
		return fmt.Errorf("sort-merge join: build stream: %w", err)
	}
	j.rstream = rs
	return nil
}

// resolveCompareKernels resolves one typed kernel per key pair, using the
// consume-time-normalized side key names. Requires identical key column
// types on both sides — the equi-join planner gate guarantees this;
// anything else fails loudly here.
func (j *SortMergeJoin) resolveCompareKernels() error {
	j.compare = make([]kernel.SortCompareKernel, len(j.probe.keys))
	for i := range j.probe.keys {
		lCol, rCol := j.probe.keys[i].Column, j.build.keys[i].Column
		lIdx := schemaColumnIndex(j.probe.schema, lCol)
		rIdx := schemaColumnIndex(j.build.schema, rCol)
		if lIdx < 0 || rIdx < 0 {
			return fmt.Errorf("sort-merge join: key pair %q/%q not found in side schemas", lCol, rCol)
		}
		lType, rType := j.probe.schema[lIdx].Type, j.build.schema[rIdx].Type
		if lType != rType {
			return fmt.Errorf("sort-merge join: key type mismatch for %q/%q (%d vs %d)", lCol, rCol, lType, rType)
		}
		// Null-aware kernel is defensive only — null-key rows never enter
		// the runs.
		cmp := kernel.ResolveSortCompare(lType)
		if cmp == nil {
			return fmt.Errorf("sort-merge join: unsupported key type %d for %q", lType, lCol)
		}
		j.compare[i] = cmp
	}
	return nil
}

func schemaColumnIndex(schema []parquet.Column, name string) int {
	for i, col := range schema {
		if col.Name == name {
			return i
		}
	}
	return -1
}

// openStream builds the side's merged sorted stream: pre-merged file runs
// plus the sorted in-memory remainder as the last cursor (Sort's
// finalizeExternalMerge shape, including its run-file error contract).
func (j *SortMergeJoin) openStream(side *smjSide) (*smjStream, error) {
	j.mu.Lock()
	runs := side.runFiles
	side.runFiles = nil
	batches := side.batches
	totalRows := side.totalRows
	j.mu.Unlock()

	var spillDir string
	if j.Spill != nil {
		spillDir = j.Spill.SpillDir()
	}
	runs, err := preMergeRuns(spillDir, side.schema, side.keys, runs, maxMergeFanIn-1, 0)
	if err != nil {
		return nil, err
	}

	cursors := make([]*runCursor, 0, len(runs)+1)
	for ord, p := range runs {
		c, err := newFileRunCursor(p)
		if err != nil {
			for _, prev := range cursors {
				prev.close()
			}
			removeRunFiles(runs)
			return nil, err
		}
		c.ord = ord
		cursors = append(cursors, c)
	}
	if len(batches) > 0 {
		entries := buildSortEntries(batches, totalRows)
		resolved := resolveSortKeysForBatches(side.keys, batches)
		entries = selectSortedEntries(entries, sortEntriesLessFunc(resolved, batches), 0)
		c, err := newMemRunCursor(side.schema, batches, entries)
		if err != nil {
			for _, prev := range cursors {
				prev.close()
			}
			removeRunFiles(runs)
			return nil, err
		}
		c.ord = len(runs)
		cursors = append(cursors, c)
	}

	s := &smjStream{
		merger: newRunMerger(side.schema, side.keys, cursors),
		runs:   runs,
		keyIdx: make([]int, len(side.keys)),
	}
	for i, k := range side.keys {
		s.keyIdx[i] = schemaColumnIndex(side.schema, k.Column)
	}
	// Load the first batch.
	b, err := s.merger.Next()
	if err != nil {
		s.close()
		return nil, err
	}
	s.cur = b
	return s, nil
}

// Next streams joined output batches (up to DefaultBatchSize rows each) from
// the two-cursor merge, or (nil, nil) once both sides are exhausted. Merge
// memory: one batch per live run cursor per side, the pinned duplicate-group
// batches, and the pending flush window.
func (j *SortMergeJoin) Next(_ context.Context) (*batch.RecordBatch, error) {
	if j.done {
		return nil, nil
	}
	if j.lstream == nil || j.rstream == nil {
		// Empty-side finalize already tore down.
		j.finishMerge()
		return nil, nil
	}

	for !j.exhausted && len(j.pending) < batch.DefaultBatchSize {
		if j.groupActive {
			if err := j.stepGroup(); err != nil {
				return nil, err
			}
			continue
		}
		l, r := j.lstream, j.rstream
		if l.cur == nil || r.cur == nil {
			j.exhausted = true
			break
		}
		cmp := j.compareLR()
		switch {
		case cmp < 0:
			if err := l.advance(); err != nil {
				return nil, err
			}
		case cmp > 0:
			if err := r.advance(); err != nil {
				return nil, err
			}
		default:
			if err := j.collectGroup(); err != nil {
				return nil, err
			}
		}
	}

	if len(j.pending) > 0 {
		return j.flushPending(), nil
	}
	j.finishMerge()
	return nil, nil
}

// compareLR compares the probe cursor's current row against the build
// cursor's, key by key.
func (j *SortMergeJoin) compareLR() int {
	l, r := j.lstream, j.rstream
	for i, cmp := range j.compare {
		if c := cmp(l.cur.Columns[l.keyIdx[i]], l.row, r.cur.Columns[r.keyIdx[i]], r.row); c != 0 {
			return c
		}
	}
	return 0
}

// keysEqual compares row (b1,r1) against (b2,r2) using keyIdx1/keyIdx2 for
// the respective batches.
func (j *SortMergeJoin) keysEqual(b1 *batch.RecordBatch, r1 int, keyIdx1 []int, b2 *batch.RecordBatch, r2 int, keyIdx2 []int) bool {
	for i, cmp := range j.compare {
		if cmp(b1.Columns[keyIdx1[i]], r1, b2.Columns[keyIdx2[i]], r2) != 0 {
			return false
		}
	}
	return true
}

// collectGroup gathers every consecutive build-side row equal to the current
// key into the group ref list. Each time the cursor moves INTO a new batch
// while the group still references the previous one, the previous batch is
// pinned (tracker-Reserved) — its lifetime is now the group's, not the
// cursor's. Stream exhaustion pins nothing: the final batch was live cursor
// memory a moment ago and stays bounded at one batch, the same untracked
// envelope as any live cursor. The build cursor ends positioned on the first
// row past the group.
func (j *SortMergeJoin) collectGroup() error {
	r := j.rstream
	keyBatch, keyRow := r.cur, r.row
	for {
		j.group.refs = append(j.group.refs, mergeRef{b: r.cur, row: r.row})
		prev := r.cur
		if err := r.advance(); err != nil {
			return err
		}
		if r.cur != nil && r.cur != prev {
			if err := j.pinGroupBatch(prev); err != nil {
				return err
			}
		}
		if r.cur == nil || !j.keysEqual(r.cur, r.row, r.keyIdx, keyBatch, keyRow, r.keyIdx) {
			break
		}
	}
	j.groupActive = true
	j.groupPos = 0
	return nil
}

// pinGroupBatch reserves a group-referenced batch's bytes once the merge
// cursor has moved past it. This is the loud v1 bound on per-key duplication:
// a hot key whose group drags an ever-growing batch tail fails the query with
// a clear error instead of OOMing the worker. The cursor transitions off a
// batch exactly once, so each spanned batch is reserved exactly once.
func (j *SortMergeJoin) pinGroupBatch(b *batch.RecordBatch) error {
	if j.Spill == nil {
		return nil
	}
	cost := b.MemBytes()
	if err := j.Spill.Tracker().Reserve(cost); err != nil {
		return fmt.Errorf("sort-merge join: build-side duplicate group for one key exceeds memory budget (%d rows buffered): %w", len(j.group.refs), err)
	}
	j.group.reserved += cost
	return nil
}

// stepGroup emits pairs of the current probe row against the group, advancing
// the probe cursor through every row that matches the group key, then retires
// the group.
func (j *SortMergeJoin) stepGroup() error {
	l := j.lstream
	key := j.group.refs[0]
	if l.cur == nil || !j.keysEqual(l.cur, l.row, l.keyIdx, key.b, key.row, j.rstream.keyIdx) {
		j.retireGroup()
		return nil
	}
	for j.groupPos < len(j.group.refs) && len(j.pending) < batch.DefaultBatchSize {
		ref := j.group.refs[j.groupPos]
		j.pending = append(j.pending, smjPair{lb: l.cur, lrow: uint32(l.row), rb: ref.b, rrow: uint32(ref.row)})
		j.groupPos++
	}
	if j.groupPos == len(j.group.refs) {
		j.groupPos = 0
		return l.advance()
	}
	return nil // pending is full; resume mid-group on the next call
}

// retireGroup drops the current group. Its reservation is released only after
// the next flush — pending pairs may still reference the pinned batches.
func (j *SortMergeJoin) retireGroup() {
	j.pendingRelease += j.group.reserved
	j.group.refs = j.group.refs[:0]
	j.group.reserved = 0
	j.groupActive = false
	j.groupPos = 0
}

// flushPending materializes the pending pairs into one output batch using the
// typed gather kernels (per-column hoisted type switch), then releases
// retired-group reservations that no longer have pending references.
func (j *SortMergeJoin) flushPending() *batch.RecordBatch {
	n := len(j.pending)
	// Per-flush batch lists + entries: consecutive pairs overwhelmingly share
	// batches, so dedup by comparing against the last appended pointer.
	var lBatches, rBatches []*batch.RecordBatch
	lEntries := make([]sortEntry, n)
	rEntries := make([]sortEntry, n)
	for i, p := range j.pending {
		if len(lBatches) == 0 || lBatches[len(lBatches)-1] != p.lb {
			lBatches = append(lBatches, p.lb)
		}
		lEntries[i] = sortEntry{batchIdx: uint32(len(lBatches) - 1), rowIdx: p.lrow}
		if len(rBatches) == 0 || rBatches[len(rBatches)-1] != p.rb {
			rBatches = append(rBatches, p.rb)
		}
		rEntries[i] = sortEntry{batchIdx: uint32(len(rBatches) - 1), rowIdx: p.rrow}
	}

	out := batch.NewRecordBatch(j.outSchema, n)
	for c, m := range j.outMapping {
		if m.fromProbe {
			gatherSortVector(out.Columns[c], m.srcIdx, lEntries, lBatches)
		} else {
			gatherSortVector(out.Columns[c], m.srcIdx, rEntries, rBatches)
		}
	}
	j.pending = j.pending[:0]

	if j.pendingRelease > 0 && j.Spill != nil {
		j.Spill.Tracker().Release(j.pendingRelease)
		j.pendingRelease = 0
	}
	return out
}

// finishMerge tears down the merge: closes cursors, deletes run scratch,
// releases every reservation still held (in-memory remainders, group pins).
func (j *SortMergeJoin) finishMerge() {
	j.mu.Lock()
	j.teardownLocked()
	j.mu.Unlock()
}

// teardownLocked releases all merge and buffer state. Idempotent; caller
// holds j.mu.
func (j *SortMergeJoin) teardownLocked() {
	if j.lstream != nil {
		j.lstream.close()
		j.lstream = nil
	}
	if j.rstream != nil {
		j.rstream.close()
		j.rstream = nil
	}
	for _, side := range []*smjSide{&j.probe, &j.build} {
		removeRunFiles(side.runFiles)
		side.runFiles = nil
		side.batches = nil
		side.totalRows = 0
		if j.Spill != nil && side.trackedMem > 0 {
			j.Spill.ReleaseTracking(side.trackedMem)
			side.trackedMem = 0
		}
	}
	if j.Spill != nil {
		if j.group.reserved > 0 {
			j.Spill.Tracker().Release(j.group.reserved)
		}
		if j.pendingRelease > 0 {
			j.Spill.Tracker().Release(j.pendingRelease)
		}
	}
	j.group.refs = nil
	j.group.reserved = 0
	j.pendingRelease = 0
	j.pending = nil
	j.groupActive = false
	j.done = true
}

// Close releases buffered state, merge state, run scratch, and any tracker
// reservation still held — see Sort.Close for why this matters (phantom
// reservations outlive the query otherwise).
func (j *SortMergeJoin) Close() error {
	if j.unregisterAccounted != nil {
		j.accState.Store(int32(memory.OpClosed))
		j.unregisterAccounted()
		j.unregisterAccounted = nil
	}
	j.mu.Lock()
	j.teardownLocked()
	j.mu.Unlock()
	return nil
}

// Inspect implements memory.AccountedOperator.
func (j *SortMergeJoin) Inspect() memory.OperatorFootprint {
	j.mu.Lock()
	defer j.mu.Unlock()
	st := memory.OpState(j.accState.Load())
	if st == memory.OpClosed {
		return memory.OperatorFootprint{State: memory.OpClosed, InstanceID: j.accInstanceID, Name: "SortMergeJoin"}
	}
	owned := j.probe.trackedMem + j.build.trackedMem
	return memory.OperatorFootprint{
		OwnedBytes:     owned,
		RetainedBytes:  owned,
		SpillableBytes: j.spillableBytesLocked(),
		State:          st,
		InstanceID:     j.accInstanceID,
		Name:           "SortMergeJoin",
	}
}

// spillableBytesLocked: both sides are spillable pre-finalize (each drains
// all-or-nothing to a sorted run); once finalize begins the buffers belong to
// the merge cursors. Caller holds j.mu.
func (j *SortMergeJoin) spillableBytesLocked() int64 {
	if j.Spill == nil || j.finalized {
		return 0
	}
	var total int64
	if len(j.probe.batches) > 0 {
		total += j.probe.trackedMem
	}
	if len(j.build.batches) > 0 {
		total += j.build.trackedMem
	}
	return total
}

// EstimateRelief implements memory.AccountedOperator: what SpillSome(target)
// would free — the larger buffered side, plus the smaller one when the larger
// alone doesn't cover target.
func (j *SortMergeJoin) EstimateRelief(target int64) int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	if target <= 0 || j.Spill == nil || j.finalized {
		return 0
	}
	larger, smaller := j.sidesBySizeLocked()
	lb, sb := sideSpillable(larger), sideSpillable(smaller)
	if lb >= target {
		return lb
	}
	return lb + sb
}

// SpillSome drains buffered batches to sorted runs, larger side first,
// stopping once target is met. Implements memory.Spillable and
// memory.AccountedOperator.
func (j *SortMergeJoin) SpillSome(target int64) (int64, error) {
	j.accState.Store(int32(memory.OpSpilling))
	defer j.accState.CompareAndSwap(int32(memory.OpSpilling), int32(memory.OpActive))
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.Spill == nil || j.finalized {
		return 0, nil
	}
	larger, smaller := j.sidesBySizeLocked()
	freed, err := j.flushSideLocked(larger)
	if err != nil {
		return freed, err
	}
	if freed < target {
		more, err := j.flushSideLocked(smaller)
		freed += more
		if err != nil {
			return freed, err
		}
	}
	return freed, nil
}

// sidesBySizeLocked orders the sides by buffered footprint, larger first.
// Caller holds j.mu.
func (j *SortMergeJoin) sidesBySizeLocked() (larger, smaller *smjSide) {
	if j.build.trackedMem >= j.probe.trackedMem {
		return &j.build, &j.probe
	}
	return &j.probe, &j.build
}

func sideSpillable(side *smjSide) int64 {
	if len(side.batches) == 0 {
		return 0
	}
	return side.trackedMem
}
