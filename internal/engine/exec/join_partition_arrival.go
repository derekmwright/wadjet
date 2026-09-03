package exec

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// accumFlushRows is the row count at which an open per-partition build
// accumulator is frozen (indexed into the hash table and handed to
// buildBatches) and a fresh one is started. Bounding accumulators at
// batch.DefaultBatchSize keeps per-batch index growth (EnsureCapacity +
// CheckGrow) on its tuned path and caps any single buildBatches entry to one
// arrival batch's worth. Accumulators grow on demand toward this bound rather
// than pre-allocating it, so a sparsely-filled partition stays small.
const accumFlushRows = batch.DefaultBatchSize

// buildPartitioned is the partition-on-arrival build path. Instead of
// accumulating every batch flat and reactively switching to partitioned-spill
// on first pressure event, this path allocates spillState upfront, scatters
// every arriving batch into its 64 hash partitions, and indexes per-partition
// rows incrementally into the global hash table. When pool pressure rises,
// spillOneInMemoryPartition picks the largest in-memory partition, writes its
// batches to disk, and frees them — an O(partition_size) eviction instead of
// the legacy path's O(total_size) "freeze, repartition everything, reset
// hash-table, rebuild from in-memory partitions" sequence.
//
// This matches the Grace Hash Join shape that Spark's UnsafeShuffleSorter and
// Trino's HashBuilderOperator implement: build is partitioned-by-default, so
// spill is just "evict one partition," not a global state reset.
//
// Probe-side correctness: HashJoinProbe.Execute already routes spilled-partition
// rows to disk before any hash lookup when spillState != nil, and within a
// hash-bucket all chain entries share the same key (intHashTable.Get returns the
// chain head for an exact key match) — so the chain for a probed key always
// resolves to a single partition. If that partition is in-memory the whole
// chain points to live batches; if it's spilled the partition routing has
// already diverted the probe row to disk. The freed h.buildBatches[i] = nil
// slots are therefore unreachable on the in-memory probe path.
//
// Caller invariants:
//   - h.MemTracker and h.Spill must both be set; otherwise the legacy path runs.
//   - SemiAntiKeyOnly takes its own no-storage build path before this fires.
//   - The serial build path is the production caller; parallel-build (which
//     merges per-worker locals) is currently key-only and not affected.
func (h *HashJoin) buildPartitioned(ctx context.Context, source Source) error {
	progress := ProgressReporterFromContext(ctx)

	// Spillable registration lives in Build() — both the partition-on-arrival
	// and the legacy-flat-then-reactively-converted paths benefit from being
	// reachable by the cooperative-spill advisor.

	for {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("join build cancelled: %w", err)
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

		h.mu.Lock()

		// Narrow filtered semi/anti builds to keys + filter columns before any
		// storage decision: the partition scatter, per-partition accumulators,
		// spill files, and the tracker Reserve below all see only the retained
		// columns. The view shares the arrival batch's vectors; every path
		// below copies rows out (appendRows / compactBatchForRows), so the
		// arrival batch is never retained and needs no Detach here.
		b = h.projectForStore(b)

		if h.buildSchema == nil {
			h.buildSchema = b.Schema
			h.buildKeyIdx = make([]int, len(h.RightKeys))
			for i, col := range h.RightKeys {
				h.buildKeyIdx[i] = columnIndexFallback(b, col)
			}
			h.tryEnableIntKey(b)
			if h.buildKeyErr != nil {
				return h.buildKeyErr
			}

			// Pre-allocate arena and string index using BuildRowHint when set,
			// bounded by the budget's room — this is unspillable state and
			// pre-sizing it charged 191,072 bytes on a 20-row batch (#823).
			// Hash tables for the int paths were sized by tryEnableIntKey.
			if hint := h.preSizeRowHint(b); hint > 0 {
				h.arena = make([]buildRef, 0, hint)
				h.arenaNext = make([]int32, 0, hint)
				if !h.useIntKey && !h.useDualIntKey {
					h.strIndex = newStrHashTable(hint)
				}
			}

			h.spillState = newSpillState(h.Spill.SpillDir(), b.Schema)
		}

		// Track this batch's bytes against the shared pool. Like the legacy
		// path, we Reserve and fall back to spilling on over-budget. Unlike
		// the legacy path, the spill is incremental — pick one partition and
		// evict it instead of repartitioning the whole flat state.
		cost := hashBuildBytes(b)
		if err := h.MemTracker.Reserve(cost); err != nil {
			if spillErr := h.spillUntilCanReserve(cost); spillErr != nil {
				h.mu.Unlock()
				return fmt.Errorf("hash join build: %w (build_rows=%d, batches=%d)",
					spillErr, h.buildRows, len(h.buildBatches))
			}
			if err2 := h.MemTracker.Reserve(cost); err2 != nil {
				h.mu.Unlock()
				return fmt.Errorf("hash join build: %w (build_rows=%d, batches=%d)",
					err2, h.buildRows, len(h.buildBatches))
			}
		}
		h.trackedMem += cost

		// Update key min/max even before partitioning — bloom + dynamic-range
		// pushdowns use these for ALL keys, including those that later spill.
		h.updateKeyMinMax(b)

		if err := h.partitionAndIndexBatch(b); err != nil {
			h.mu.Unlock()
			return fmt.Errorf("partition+index: %w", err)
		}

		// Reactive spill if pool pressure is over the spill-cheap threshold
		// (60% of budget) — release one partition's bytes before the next
		// batch arrives. spillUntilCanReserve uses the same logic but is
		// driven by Reserve failure; here we proactively keep headroom.
		if h.Spill.ShouldSpillFor(memory.SpillCheap) {
			if _, err := h.spillOneInMemoryPartition(); err != nil {
				h.mu.Unlock()
				return fmt.Errorf("spilling under pressure: %w", err)
			}
		}

		h.reconcileHashMemory()

		h.mu.Unlock()
	}

	// Freeze any remaining open per-partition accumulators so their buffered
	// rows are indexed and joined into the build side before probe begins. This
	// extends the arena, so it must run before arenaMatched is sized below.
	h.mu.Lock()
	if err := h.freezeAllOpenAccums(); err != nil {
		h.mu.Unlock()
		return fmt.Errorf("freezing accumulators at build end: %w", err)
	}
	h.mu.Unlock()

	// Allocate matched bitmap for right/full outer join and right semi/anti tracking
	if (h.JoinType == RightJoin || h.JoinType == FullOuterJoin ||
		h.JoinType == RightSemiJoin || h.JoinType == RightAntiJoin) && len(h.arena) > 0 {
		h.arenaMatched = make([]bool, len(h.arena))
	}

	// consolidateBuild() is a no-op when spillState != nil (check at the top
	// of that function), so the per-partition mini-batches stay separate. The
	// probe pair-sort optimization is sacrificed in this mode in exchange for
	// O(partition) spill instead of O(total) repartition. Per-partition
	// consolidation is a future optimisation.
	h.buildBloom()
	h.reconcileHashMemory()

	h.warmBuildNullBitmaps()
	h.applyBuildSchemaHint()
	h.buildDone = true
	return nil
}

// partitionAndIndexBatch scatters b's rows into hash partitions, writing rows
// for already-spilled partitions directly to disk and appending rows for
// in-memory partitions to h.buildBatches with a fresh global batch index plus
// a parallel ss.batchPartID entry. For in-memory rows it also indexes the
// resulting per-partition mini-batch into the global hash table via the same
// indexBuildBatch helper used by the legacy partitioned-reload path.
//
// Caller must hold h.mu.
func (h *HashJoin) partitionAndIndexBatch(b *batch.RecordBatch) error {
	ss := h.spillState

	if !ss.accumChecked {
		ss.accumEligible = accumEligibleSchema(b)
		ss.accumChecked = true
	}

	// Reset the reusable per-partition scatter scratch (capacity retained) and
	// scatter this arrival batch's rows into it — no per-batch map allocation.
	for p := range ss.partRowScratch {
		ss.partRowScratch[p] = ss.partRowScratch[p][:0]
	}
	computeBuildPartitionRows(h, b, &ss.partRowScratch)

	for partID := 0; partID < numSpillPartitions; partID++ {
		rows := ss.partRowScratch[partID]
		if len(rows) == 0 {
			continue
		}

		// Already spilling to disk: write a transient compacted batch straight
		// out. Spilled partitions are never indexed or accumulated, so there is
		// no live ref and no accumulator involved.
		if ss.spilledParts[partID] {
			partBatch := compactBatchForRows(b, rows)
			partBytes := hashBuildBytes(partBatch)
			if err := ss.writeBuildBatch(partID, partBatch); err != nil {
				return fmt.Errorf("writing build batch for spilled partition %d: %w", partID, err)
			}
			// Release the cost portion that these rows contributed to the
			// arrival batch's Reserve(cost). They just went to disk, so the
			// corresponding bytes are no longer charged against the in-memory
			// budget, and there's no in-memory partition to spill later that
			// would otherwise release them. Without this, every arrival batch
			// processed after partitions start spilling drifts the tracker
			// upward by the to-disk fraction — the failure mode
			// TestPartitionOnArrival_BasicSpill exposed before this release.
			if h.MemTracker != nil && partBytes > 0 {
				h.MemTracker.Release(partBytes)
				h.trackedMem -= partBytes
				if h.trackedMem < 0 {
					h.trackedMem = 0
				}
			}
			continue
		}

		// In-memory partition. Eligible (scalar/bytes/decimal) schemas append
		// into a reusable growable per-partition accumulator; nested-column
		// schemas index one tightly-sized batch per arrival (append-grow can't
		// extend nested element storage).
		if ss.accumEligible {
			if err := h.appendToAccum(b, partID, rows); err != nil {
				return err
			}
		} else {
			h.freezeTightForPartition(partID, b, rows)
		}
	}
	return nil
}

// accumEligibleSchema reports whether every column of b can be grown in place by
// Vector.EnsureLen (scalar, bytes, or decimal). Nested ARRAY/MAP/ROW/VECTOR
// columns cannot, so such schemas use the tight per-arrival path.
func accumEligibleSchema(b *batch.RecordBatch) bool {
	for _, col := range b.Columns {
		switch col.Type {
		case batch.TypeArray, batch.TypeMap, batch.TypeRow, batch.TypeVector:
			return false
		}
	}
	return true
}

// freezeTightForPartition indexes one tightly-sized batch of an in-memory
// partition's rows directly into the build side (no accumulator reuse). Used
// for nested-column schemas and oversized arrival contributions. partMemory is
// charged the batch's true MemBytes (its capacity equals its row count).
// Caller must hold h.mu.
func (h *HashJoin) freezeTightForPartition(partID int, b *batch.RecordBatch, rows []int) {
	ss := h.spillState
	tight := compactBatchForRows(b, rows)
	tight.Detach()
	ss.partMemory[partID] += tight.MemBytes() + int64(len(rows))*40
	h.registerFrozen(partID, tight)
}

// appendToAccum appends b's selected rows for an in-memory partition into that
// partition's open accumulator, freezing it first if the rows would overflow
// accumFlushRows. partMemory is charged with the TIGHT byte footprint of the
// appended rows (independent of the accumulator's physical capacity), keeping
// the spill accounting balanced against the arrival batch's Reserve(cost) and
// making an open accumulator's rows visible to spill selection before freeze.
//
// Caller must hold h.mu.
func (h *HashJoin) appendToAccum(b *batch.RecordBatch, partID int, rows []int) error {
	ss := h.spillState

	// Defensive: an arrival batch never exceeds DefaultBatchSize, so a fresh
	// accumulator always fits one partition's slice within accumFlushRows. If a
	// future caller delivers an oversized batch, freeze any partial accumulator
	// (to preserve arrival order within the partition) and index the oversized
	// slice as one tight batch rather than overflowing the freeze bound.
	if len(rows) > accumFlushRows {
		if err := h.freezeAccum(partID); err != nil {
			return err
		}
		h.freezeTightForPartition(partID, b, rows)
		return nil
	}

	acc := ss.openAccum[partID]
	if acc != nil && acc.Len+len(rows) > accumFlushRows {
		if err := h.freezeAccum(partID); err != nil {
			return err
		}
		acc = nil
	}
	if acc == nil {
		// Mint empty; the backing arrays grow on demand via EnsureCapacity so a
		// partition that receives few rows never pre-commits an accumFlushRows-
		// sized buffer (which would over-allocate 64 partitions on a tight pool
		// and starve concurrent builds — the regime this path exists to serve).
		acc = batch.NewRecordBatch(b.Schema, 0)
		acc.Detach() // build retains references; batch must not be recycled
		ss.openAccum[partID] = acc
	}
	dstStart := acc.Len
	acc.EnsureCapacity(dstStart + len(rows)) // grows backing arrays; sets acc.Len
	dataBytes := appendRows(acc, b, rows, dstStart)
	ss.partMemory[partID] += dataBytes + int64(len(rows))*40
	ss.openAccumData[partID] += dataBytes
	return nil
}

// freezeAccum finalizes partID's open accumulator: it sets the batch's logical
// length, indexes it into the global hash table, and appends it to buildBatches
// (after which it backs live buildRefs and must never be mutated or Reset).
// openAccum[partID] is cleared. No-op when there is no open accumulator.
//
// Caller must hold h.mu.
func (h *HashJoin) freezeAccum(partID int) error {
	ss := h.spillState
	acc := ss.openAccum[partID]
	if acc == nil {
		return nil
	}
	ss.openAccum[partID] = nil
	if acc.Len == 0 {
		ss.openAccumData[partID] = 0
		return nil
	}
	// Ensure each column's logical length matches the filled row count so the
	// frozen batch is structurally a normal compacted batch (EnsureCapacity
	// already set these during the last append; this is defensive).
	for _, col := range acc.Columns {
		col.Len = acc.Len
	}
	// Reconcile the accumulator's fixed-capacity overhead. Its arrays were
	// allocated for accumFlushRows rows regardless of fill, so MemBytes() (which
	// counts slice length + Data capacity) exceeds the tight per-row data
	// charged at append. Charge the difference to partMemory AND the shared
	// tracker so the resident-but-unfilled capacity is accounted: undercounting
	// it would let the shared pool over-commit to concurrent tasks — the OOM
	// risk this path exists to remove. On spill, partMemory[partID] (now
	// inclusive) is released, balancing this charge; Close releases the rest.
	if excess := acc.MemBytes() - ss.openAccumData[partID]; excess > 0 {
		ss.partMemory[partID] += excess
		if h.MemTracker != nil {
			h.MemTracker.ForceReserve(excess)
			h.trackedMem += excess
		}
	}
	ss.openAccumData[partID] = 0
	h.registerFrozen(partID, acc)
	return nil
}

// registerFrozen indexes a finalized in-memory partition batch into the hash
// table and records it in buildBatches plus the per-partition bookkeeping. The
// batch must be Detached, dense (Sel==nil), and have its logical Len set.
// partMemory is charged by the caller (tightly, at append time), so this does
// not touch it. Caller must hold h.mu.
func (h *HashJoin) registerFrozen(partID int, frozen *batch.RecordBatch) {
	ss := h.spillState
	batchIdx := int32(len(h.buildBatches))
	h.buildBatches = append(h.buildBatches, frozen)
	ss.batchPartID = append(ss.batchPartID, partID)
	ss.partBuildBatches[partID] = append(ss.partBuildBatches[partID], frozen)

	// Pre-grow the hash table for this batch's rows so PutNoGrow won't overflow
	// during indexing. Mirrors the legacy flat path's EnsureCapacity+CheckGrow.
	batchRows := frozen.ActiveLen()
	if h.useIntKey || h.useDualIntKey {
		if h.intIndex == nil {
			h.intIndex = newIntHashTable(batchRows)
		}
		h.intIndex.EnsureCapacity(batchRows)
	} else if h.strIndex != nil {
		h.strIndex.EnsureCapacity(batchRows)
	} else {
		h.strIndex = newStrHashTable(batchRows)
	}

	h.indexBuildBatch(frozen, batchIdx)

	if h.useIntKey || h.useDualIntKey {
		h.intIndex.CheckGrow()
	} else if h.strIndex != nil {
		h.strIndex.CheckGrow()
	}
}

// freezeAllOpenAccums freezes every open per-partition accumulator. Called once
// at build end so the final partial accumulators are indexed before probe.
// Caller must hold h.mu.
func (h *HashJoin) freezeAllOpenAccums() error {
	ss := h.spillState
	if ss == nil {
		return nil
	}
	for partID := 0; partID < numSpillPartitions; partID++ {
		if ss.openAccum[partID] != nil {
			if err := h.freezeAccum(partID); err != nil {
				return err
			}
		}
	}
	return nil
}

// computeBuildPartitionRows assigns each row of b to a hash partition based
// on the join's key encoding (int / dual-int / string), appending the row
// index into out[partition]. The caller owns out and must truncate each
// out[p] to [:0] before the call. Rows whose key is null are routed to
// partition 0 and will produce no hash-table match — but keeping them grouped
// means the same partition holds all keys that hash to it, including a
// probe-side null lookup that hits this same partition.
//
// It is also the one place that sees EVERY build row's key before grace
// partitioning splits them, so it is where a null-aware anti join learns its
// build held a NULL at all — a fact that poisons the whole answer and must
// therefore not be per-partition (#507).
func computeBuildPartitionRows(h *HashJoin, b *batch.RecordBatch, out *[numSpillPartitions][]int) {
	switch {
	case h.useIntKey:
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				key, ok := intKeyFromVector(col, row)
				if !ok {
					h.buildHasNullKey = true
					out[0] = append(out[0], row)
					continue
				}
				p := spillPartition(key)
				out[p] = append(out[p], row)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				key, ok := intKeyFromVector(col, i)
				if !ok {
					h.buildHasNullKey = true
					out[0] = append(out[0], i)
					continue
				}
				p := spillPartition(key)
				out[p] = append(out[p], i)
			}
		}
	case h.useDualIntKey:
		col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				a, bb, ok := dualIntKeyFromVectors(col0, col1, row)
				if !ok {
					h.buildHasNullKey = true
					out[0] = append(out[0], row)
					continue
				}
				p := spillPartition(dualIntHash(a, bb))
				out[p] = append(out[p], row)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, i)
				if !ok {
					h.buildHasNullKey = true
					out[0] = append(out[0], i)
					continue
				}
				p := spillPartition(dualIntHash(a, bb))
				out[p] = append(out[p], i)
			}
		}
	default:
		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				h.buildKeyFromBatch(b, row)
				p := spillPartitionBytes(h.keyBuf)
				out[p] = append(out[p], row)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				h.buildKeyFromBatch(b, i)
				p := spillPartitionBytes(h.keyBuf)
				out[p] = append(out[p], i)
			}
		}
	}
}

// spillOneInMemoryPartition picks the largest in-memory partition, writes its
// batches to disk via the spillState writer, frees the corresponding entries
// in h.buildBatches (sets them nil so column data is GC-eligible) and releases
// that partition's bytes from h.MemTracker. Returns the number of bytes freed
// from the tracker, or 0 if no in-memory partition exists.
//
// Caller must hold h.mu. The hash-table arena entries that point to the freed
// batch slots are NOT removed: they remain in the chain but are unreachable on
// the in-memory probe path because HashJoinProbe.Execute routes spilled-
// partition rows to disk before any hash lookup. Their ~12 bytes/row of arena
// + arenaNext overhead is a tracked-but-not-freed residual; the dominant
// memory cost (column data) is freed cleanly.
// JoinPartitionsEvicted counts grace-partition evictions
// (spillOneInMemoryPartition) across the process.
//
// It exists so a gate can PROVE it reached the eviction path instead of
// skipping when it did not. #550's pin had to `t.Skip` when no partition was
// evicted, which on a machine where the fixture stayed resident made the pin
// silently vacuous; a counter turns "the shape stopped spilling" into a
// failure of the test that depends on it.
var JoinPartitionsEvicted atomic.Int64

func (h *HashJoin) spillOneInMemoryPartition() (int64, error) {
	ss := h.spillState
	if ss == nil {
		return 0, nil
	}
	partID := ss.largestInMemoryPartition()
	if partID < 0 {
		return 0, nil
	}

	// Freeze this partition's open accumulator (if any) so its buffered rows
	// become a writable, indexed batch in buildBatches before we evict. Those
	// rows are already counted in partMemory[partID] (charged at append), so
	// largestInMemoryPartition could have selected this partition on their
	// account and the freed total below stays correct.
	if err := h.freezeAccum(partID); err != nil {
		return 0, err
	}

	// Mark spilled BEFORE writing, so any concurrent (re-entry-safe) callers
	// see the partition as already in the spilled set.
	ss.spilledParts[partID] = true

	// Write each in-memory batch to the partition's long-lived spill writer
	// then nil out the slot in h.buildBatches so the column data becomes
	// GC-eligible. ss.partBuildBatches is the source of truth here; the
	// parallel ss.batchPartID tells us which global slot holds each batch.
	for i, pid := range ss.batchPartID {
		if pid != partID {
			continue
		}
		bb := h.buildBatches[i]
		if bb == nil {
			continue // already evicted
		}
		if err := ss.writeBuildBatch(partID, bb); err != nil {
			return 0, fmt.Errorf("spilling partition %d batch %d: %w", partID, i, err)
		}
		h.buildBatches[i] = nil
	}

	freed := ss.partMemory[partID]
	delete(ss.partBuildBatches, partID)
	delete(ss.partMemory, partID)
	JoinPartitionsEvicted.Add(1)

	if h.MemTracker != nil && freed > 0 {
		h.MemTracker.Release(freed)
		h.trackedMem -= freed
		if h.trackedMem < 0 {
			h.trackedMem = 0
		}
	}
	return freed, nil
}

// spillUntilCanReserve evicts in-memory partitions one at a time until either
// h.MemTracker.Used() + needed fits within budget or this operator runs out
// of evictable partitions. When self-spill is exhausted but pressure remains,
// it asks the worker's cooperative-spill advisor to evict from any other
// concurrent operator on the same shared pool — this addresses the cross-
// operator pool starvation case where one task holds the bulk of the pool
// and another can't make Reserve progress on its own.
//
// Caller must hold h.mu.
func (h *HashJoin) spillUntilCanReserve(needed int64) error {
	if h.MemTracker == nil || h.MemTracker.Budget() <= 0 {
		return nil
	}
	for h.MemTracker.Used()+needed > h.MemTracker.Budget() {
		freed, err := h.spillOneInMemoryPartition()
		if err != nil {
			return err
		}
		if freed == 0 {
			break // nothing left in memory to evict from THIS operator
		}
	}
	if h.MemTracker.Used()+needed <= h.MemTracker.Budget() {
		return nil
	}
	// Cooperative spill: ask the worker's advisor to evict from any other
	// concurrent operator. Skip if no SpillManager (test paths, single-operator
	// path). We LOOP rather than make a single request: the advisor evicts one
	// victim partition per call and may free less than the gap in one round,
	// and a concurrent build on the same pool can add between rounds. A single
	// round that comes up a few bytes short would otherwise bubble a spurious
	// "memory budget exceeded" up to the caller even though the combined
	// unspillable footprint fits the budget. Keep requesting relief (and
	// re-checking our own self-spill, in case an open accumulator became
	// spillable meanwhile) until we fit or a full round frees nothing anywhere.
	if h.Spill == nil {
		return nil
	}
	for h.MemTracker.Used()+needed > h.MemTracker.Budget() {
		gap := h.MemTracker.Used() + needed - h.MemTracker.Budget()
		// h.mu is held — release before calling cross-operator advisory. A
		// concurrent operator's SpillSome takes ITS mu, never ours, so this is
		// deadlock-free; we just shouldn't pin our partitioning loop while
		// another operator does its own potentially-large spill.
		h.mu.Unlock()
		relieved, err := h.Spill.RequestRelief(gap)
		h.mu.Lock()
		if err != nil {
			return err
		}
		selfFreed, err := h.spillOneInMemoryPartition()
		if err != nil {
			return err
		}
		if relieved == 0 && selfFreed == 0 {
			// Neither we nor any peer has anything left to evict; the build
			// genuinely cannot fit. Return nil and let the caller's Reserve
			// surface the real over-budget error with full context.
			return nil
		}
	}
	return nil
}

// Inspect implements memory.AccountedOperator. OwnedBytes includes the hash
// arena/index overhead (trackedMem) plus keyBuf scratch; SpillableBytes is the
// reclaimable in-memory partition bytes only (the arena can't be freed without
// rebuilding). RetainedBytes is the build-side column data.
func (h *HashJoin) Inspect() memory.OperatorFootprint {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := memory.OpState(h.accState.Load())
	if st == memory.OpClosed {
		return memory.OperatorFootprint{State: memory.OpClosed, InstanceID: h.accInstanceID, Name: h.accName()}
	}
	return memory.OperatorFootprint{
		OwnedBytes:     h.trackedMem + int64(cap(h.keyBuf)),
		RetainedBytes:  h.trackedMem,
		SpillableBytes: h.spillableBytesLocked(),
		State:          st,
		InstanceID:     h.accInstanceID,
		Name:           h.accName(),
	}
}

// accName is the stable identifier for this join instance.
func (h *HashJoin) accName() string {
	if h.BuildTableAlias != "" {
		return "HashJoin/build=" + h.BuildTableAlias
	}
	return "HashJoin"
}

// spillableBytesLocked reports the reclaimable in-memory partition bytes — the
// same sum as the legacy SpillFootprint. A flat (non-partitioned) build has
// spillState==nil and reports 0: it can't evict a partition without a rebuild,
// matching existing semantics. Caller holds h.mu.
func (h *HashJoin) spillableBytesLocked() int64 {
	if h.spillState == nil {
		return 0
	}
	var total int64
	for _, mem := range h.spillState.partMemory {
		total += mem
	}
	return total
}

// EstimateRelief implements memory.AccountedOperator: the reclaimable partition
// bytes, capped at target.
func (h *HashJoin) EstimateRelief(target int64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.spillableBytesLocked()
	if target > 0 && target < s {
		return target
	}
	return s
}

// SpillSome attempts to free at least target bytes from this join's in-memory
// partitions. Picks the largest partition repeatedly until target is met or
// no partitions remain. Implements memory.Spillable.
func (h *HashJoin) SpillSome(target int64) (int64, error) {
	if target <= 0 {
		return 0, nil
	}
	h.accState.Store(int32(memory.OpSpilling))
	defer h.accState.CompareAndSwap(int32(memory.OpSpilling), int32(memory.OpActive))
	h.mu.Lock()
	defer h.mu.Unlock()
	var freed int64
	for freed < target {
		n, err := h.spillOneInMemoryPartition()
		if err != nil {
			return freed, err
		}
		if n == 0 {
			break
		}
		freed += n
	}
	if h.accInstanceID != 0 {
		h.MemTracker.PublishOwned(h.accInstanceID, h.trackedMem+int64(cap(h.keyBuf)))
	}
	return freed, nil
}
