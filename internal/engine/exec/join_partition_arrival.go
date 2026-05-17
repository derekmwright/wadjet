package exec

import (
	"context"
	"fmt"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
)

// PartitionOnArrivalThresholdPercent is the shared-pool used percentage at
// or above which the worker should turn on partition-on-arrival for new
// HashJoin builds. Below this threshold, the legacy flat path is cheaper:
// it skips the per-batch compactBatchForRows × 64 allocations that pure
// partition-on-arrival paid upfront. Above the threshold, eager
// partitioning is worth it because spill is likely and the partition-on-
// arrival path makes spill O(partition) instead of O(total).
//
// 30% is heuristic: chosen so concurrent builds on a shared pool start
// partitioning before the pool fills, but don't pay the allocation tax
// when the pool is mostly empty. Tune via SF10/SF100 deploys.
const PartitionOnArrivalThresholdPercent = 30

// SharedPoolUnderPressure reports whether the shared tracker is at or above
// PartitionOnArrivalThresholdPercent of its budget. Returns false when no
// budget is configured (test paths, embedded callers without a memory pool).
//
// Worker callers use this to decide whether to enable HashJoin.PartitionOnArrival
// at task entry — the Build path itself doesn't read pressure dynamically;
// it trusts the caller's flag and runs the partitioned path unconditionally
// when set.
func SharedPoolUnderPressure(t *memory.Tracker) bool {
	if t == nil {
		return false
	}
	budget := t.Budget()
	if budget <= 0 {
		return false
	}
	return t.Used()*100 >= budget*PartitionOnArrivalThresholdPercent
}

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

		if h.buildSchema == nil {
			h.buildSchema = b.Schema
			h.buildKeyIdx = make([]int, len(h.RightKeys))
			for i, col := range h.RightKeys {
				h.buildKeyIdx[i] = columnIndexFallback(b, col)
			}
			h.tryEnableIntKey(b)

			// Pre-allocate arena and string index using BuildRowHint when set.
			// Hash tables for the int paths were sized by tryEnableIntKey.
			if h.BuildRowHint > 0 {
				hint := int(h.BuildRowHint)
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
		cost := EstimateBatchBytes(b)
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
		if h.trackedMem > h.peakTrackedMem {
			h.peakTrackedMem = h.trackedMem
		}

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
	partRows := computeBuildPartitionRows(h, b)

	for partID, rows := range partRows {
		if len(rows) == 0 {
			continue
		}
		partBatch := compactBatchForRows(b, rows)
		partBytes := EstimateBatchBytes(partBatch)

		if ss.spilledParts[partID] {
			if err := ss.writeBuildBatch(partID, partBatch); err != nil {
				return fmt.Errorf("writing build batch for spilled partition %d: %w", partID, err)
			}
			// Release the cost portion that this batch contributed to the
			// arrival batch's Reserve(cost). The rows just went to disk —
			// the corresponding bytes are no longer charged against the
			// in-memory budget, and there's no in-memory partition to spill
			// later that would otherwise release them. Without this, every
			// arrival batch processed after partitions start spilling
			// drifts the tracker upward by the to-disk fraction, eventually
			// outrunning the recoverable partMemory pool — which is the
			// failure mode TestPartitionOnArrival_BasicSpill exposed before
			// this release.
			if h.MemTracker != nil && partBytes > 0 {
				h.MemTracker.Release(partBytes)
				h.trackedMem -= partBytes
				if h.trackedMem < 0 {
					h.trackedMem = 0
				}
			}
			continue
		}

		partBatch.Detach() // build retains references; batch must not be recycled
		batchIdx := int32(len(h.buildBatches))
		h.buildBatches = append(h.buildBatches, partBatch)
		ss.batchPartID = append(ss.batchPartID, partID)
		ss.partBuildBatches[partID] = append(ss.partBuildBatches[partID], partBatch)
		ss.partMemory[partID] += partBytes

		// Pre-grow hash table for this partition's rows so PutNoGrow won't
		// overflow during indexing. Mirrors the legacy flat path's per-batch
		// EnsureCapacity + CheckGrow pattern.
		batchRows := partBatch.ActiveLen()
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

		h.indexBuildBatch(partBatch, batchIdx)

		if h.useIntKey || h.useDualIntKey {
			h.intIndex.CheckGrow()
		} else if h.strIndex != nil {
			h.strIndex.CheckGrow()
		}
	}
	return nil
}

// computeBuildPartitionRows assigns each row of b to a hash partition based
// on the join's key encoding (int / dual-int / string). Rows whose key is
// null are routed to partition 0 and will produce no hash-table match — but
// keeping them grouped means the same partition holds all keys that hash to
// it, including a probe-side null lookup that hits this same partition.
func computeBuildPartitionRows(h *HashJoin, b *batch.RecordBatch) map[int][]int {
	partRows := make(map[int][]int)
	switch {
	case h.useIntKey:
		col := b.Columns[h.buildKeyIdx[0]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				key, ok := intKeyFromVector(col, row)
				if !ok {
					partRows[0] = append(partRows[0], row)
					continue
				}
				p := spillPartition(key)
				partRows[p] = append(partRows[p], row)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				key, ok := intKeyFromVector(col, i)
				if !ok {
					partRows[0] = append(partRows[0], i)
					continue
				}
				p := spillPartition(key)
				partRows[p] = append(partRows[p], i)
			}
		}
	case h.useDualIntKey:
		col0, col1 := b.Columns[h.buildKeyIdx[0]], b.Columns[h.buildKeyIdx[1]]
		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				a, bb, ok := dualIntKeyFromVectors(col0, col1, row)
				if !ok {
					partRows[0] = append(partRows[0], row)
					continue
				}
				p := spillPartition(dualIntHash(a, bb))
				partRows[p] = append(partRows[p], row)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				a, bb, ok := dualIntKeyFromVectors(col0, col1, i)
				if !ok {
					partRows[0] = append(partRows[0], i)
					continue
				}
				p := spillPartition(dualIntHash(a, bb))
				partRows[p] = append(partRows[p], i)
			}
		}
	default:
		if b.Sel != nil {
			for _, si := range b.Sel {
				row := int(si)
				h.buildKeyFromBatch(b, row)
				p := spillPartitionBytes(h.keyBuf)
				partRows[p] = append(partRows[p], row)
			}
		} else {
			for i := 0; i < b.Len; i++ {
				h.buildKeyFromBatch(b, i)
				p := spillPartitionBytes(h.keyBuf)
				partRows[p] = append(partRows[p], i)
			}
		}
	}
	return partRows
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
func (h *HashJoin) spillOneInMemoryPartition() (int64, error) {
	ss := h.spillState
	if ss == nil {
		return 0, nil
	}
	partID := ss.largestInMemoryPartition()
	if partID < 0 {
		return 0, nil
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
	// concurrent operator. Skip if no SpillManager (test paths), no other
	// operators (single-operator path), or the budget hasn't been crossed.
	if h.Spill == nil {
		return nil
	}
	gap := h.MemTracker.Used() + needed - h.MemTracker.Budget()
	// h.mu is held — release before calling cross-operator advisory. A
	// concurrent operator's SpillSome takes ITS mu, never ours, but we
	// shouldn't pin our partitioning loop while another operator does its
	// own potentially-large spill.
	h.mu.Unlock()
	_, err := h.Spill.RequestRelief(gap)
	h.mu.Lock()
	return err
}

// SpillFootprint reports how many bytes of in-memory build state this join
// currently holds. Implements memory.Spillable for the worker's cooperative
// spill advisor. Footprint is the sum across all in-memory partitions; the
// hash-table arena/index overhead is excluded because it isn't reclaimable
// without rebuilding the hash table.
func (h *HashJoin) SpillFootprint() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.spillState == nil {
		return 0
	}
	var total int64
	for _, mem := range h.spillState.partMemory {
		total += mem
	}
	return total
}

// SpillableName implements memory.Inspectable: returns a stable identifier
// incorporating the build alias when available. Used for per-operator peak
// attribution at task end.
func (h *HashJoin) SpillableName() string {
	if h.BuildTableAlias != "" {
		return fmt.Sprintf("HashJoin/build=%s", h.BuildTableAlias)
	}
	return "HashJoin"
}

// PeakFootprint implements memory.Inspectable: returns the high-water mark
// of this join's tracker reservations. Includes column data + arena/index
// overhead, matching trackedMem.
func (h *HashJoin) PeakFootprint() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.peakTrackedMem
}

// SpillSome attempts to free at least target bytes from this join's in-memory
// partitions. Picks the largest partition repeatedly until target is met or
// no partitions remain. Implements memory.Spillable.
func (h *HashJoin) SpillSome(target int64) (int64, error) {
	if target <= 0 {
		return 0, nil
	}
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
	return freed, nil
}
