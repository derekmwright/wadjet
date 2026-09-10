// This file holds aggregate finalization, spill relief, and spill-aware lifecycle methods.
// ADR-0010, ADR-0023, and ADR-0027 govern partial-state transport, key identity, and spill ownership.
package exec

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// flushSpillBuffer writes h.spillBuffer to a new spill file and releases the
// tracker bytes for those rows. Caller must hold h.mu.
func (h *HashAggregate) flushSpillBuffer() error {
	if len(h.spillBuffer) == 0 {
		return nil
	}
	// A container group key (or a container agg input) boxes as []any /
	// map[string]any / []float32, which SpillRows can only render to display
	// text — a string batch.FromRows then refuses to write back into a
	// container vector (#611). Encode those boxes to the lossless ADR-0023
	// container codec first; Finalize's read loop decodes them symmetrically.
	// The buffer is discarded right after this flush, so the in-place rewrite
	// is safe.
	encodeContainerColsForSpill(h.spillBuffer, h.inputSchema)
	path, err := h.Spill.SpillRows(h.spillBuffer)
	if err != nil {
		return err
	}
	h.spillFiles = append(h.spillFiles, path)
	RawRowSpillFiles.Add(1)
	// The rows are now on disk — release the tracker bytes charged for them
	// so ShouldSpill can flip back to false and subsequent batches can be
	// consumed directly into the hash table again.
	h.Spill.ReleaseTracking(h.spillBufferBytes)
	h.spillBuffer = nil
	h.spillBufferBytes = 0
	return nil
}

func (h *HashAggregate) Finalize(_ context.Context) error {
	// Once Finalize starts, the aggregate is no longer a viable cooperative
	// spill target — it's about to drain its in-memory state into the merger.
	// Deregister here so peer operators don't waste a RequestRelief call on
	// us mid-finalize. Close still calls unregister as a backstop.
	if h.unregisterAccounted != nil {
		h.accState.Store(int32(memory.OpClosed))
		h.unregisterAccounted()
		h.unregisterAccounted = nil
	}

	// Legacy raw-row spill re-aggregation. This MUST run before the
	// partial-merge dispatch below: an aggregate that changes group-key
	// paths mid-stream AFTER partial-state runs already exist (post-spill
	// MergeSink migration to the generic map, null-group-key demotion)
	// loses canUseExternalMerge, so later pressure consumes buffer raw rows
	// into spillFiles/spillBuffer while partialSpillFiles still holds the
	// earlier runs. Re-consuming the raw rows into the in-memory table here
	// folds them into the remainder that finalizeViaPartialMerge merges.
	// The previous ordering returned early on partialSpillFiles and
	// silently orphaned the legacy files (rows dropped from the output).
	if len(h.spillFiles) > 0 || len(h.spillBuffer) > 0 {
		// Re-process spilled input rows through the same aggregate logic.
		// This is correct for all aggregate functions because we're
		// processing raw input, not merging partial results.
		for _, f := range h.spillFiles {
			rows, err := memory.ReadSpilledRows(f)
			if err != nil {
				return err
			}
			// A consumed file is done for good — unlink it here rather than
			// leaving it for SpillManager.Cleanup, which the shared
			// (worker-injected) manager path never calls (#324). Files not
			// yet reached when an error aborts this loop stay in
			// h.spillFiles for Close's backstop removal.
			h.Spill.RemoveSpilled(f)
			if len(rows) == 0 {
				continue
			}
			// Reverse flushSpillBuffer's container encoding: rebuild the box
			// GetValue produced before batch.FromRows writes it (#611).
			if err := decodeContainerColsFromSpill(rows, h.inputSchema); err != nil {
				return err
			}
			b := batch.FromRows(h.inputSchema, rows)
			// Resolve indices only if not already resolved. Do NOT force
			// re-resolution (h.resolved = false) — after a parallel merge
			// (MergeSink), the aggregate has been migrated from compact/int
			// key mode to generic string mode. Re-resolving would switch
			// back to compact mode with a fresh intGroupStates, losing all
			// merged groups and causing index-out-of-bounds in Next().
			// The spilled batch uses h.inputSchema (same column order as
			// the original), so re-resolution is unnecessary.
			if !h.resolved {
				if err := h.resolveIndices(b); err != nil {
					return err
				}
			}
			h.consumeBatch(b)
		}
		h.spillFiles = nil

		// Drain any rows that were buffered but never crossed the flush
		// threshold — they're still in memory and can be consumed directly
		// without a round-trip through disk.
		if len(h.spillBuffer) > 0 {
			b := batch.FromRows(h.inputSchema, h.spillBuffer)
			if !h.resolved {
				if err := h.resolveIndices(b); err != nil {
					return err
				}
			}
			h.consumeBatch(b)
			if h.Spill != nil {
				h.Spill.ReleaseTracking(h.spillBufferBytes)
			}
			h.spillBuffer = nil
			h.spillBufferBytes = 0
		}
	}

	// External-merge path: when partial-state files exist, k-way merge them
	// (plus any in-memory remainder, which now includes any re-aggregated
	// legacy spill from above) instead of re-aggregating raw rows. This
	// is the SF100+ unblock: the legacy raw-row Finalize re-reads ALL spilled
	// input back into the hash table and re-runs Consume on it, blowing the
	// budget when input >> output. Partial-state merge processes each group
	// once and is bounded by the merged result size.
	if len(h.partialSpillFiles) > 0 {
		if err := h.finalizeViaPartialMerge(); err != nil {
			return err
		}
	}
	// Partitioned-disjoint adoption: each adopted partition finalizes its
	// own state (including any drained runs it produced).
	for _, ap := range h.adoptedPartitions {
		if err := ap.Finalize(context.Background()); err != nil {
			return fmt.Errorf("finalizing adopted partition: %w", err)
		}
	}
	return nil
}

// Close releases any tracker reservation HashAggregate still holds for
// group-state memory and buffered-but-unspilled rows. Without this, a
// non-spilling HashAggregate accumulates a phantom reservation in the
// shared tracker for the lifetime of the process; see HashJoin.Close for
// the full background.
func (h *HashAggregate) Close() error {
	// Stop the parallel drain FIRST and wait for it: its goroutines own the
	// adopted partitions (each closes its own unit, and with it that unit's
	// off-heap registry) and read this aggregate's own SoA arrays. Nothing
	// below may run while a drain goroutine is still live.
	if h.emit != nil {
		h.emit.shutdown()
		h.emit = nil
	}
	for _, ap := range h.adoptedPartitions {
		ap.Close()
	}
	h.adoptedPartitions = nil
	if h.unregisterAccounted != nil {
		h.accState.Store(int32(memory.OpClosed))
		h.unregisterAccounted()
		h.unregisterAccounted = nil
	}
	if h.Spill != nil {
		if h.trackedGroupMem > 0 {
			h.Spill.ReleaseTracking(h.trackedGroupMem)
			h.trackedGroupMem = 0
		}
		if h.spillBufferBytes > 0 {
			h.Spill.ReleaseTracking(h.spillBufferBytes)
			h.spillBufferBytes = 0
		}
		// Legacy raw-row spill files never consumed by Finalize (error or
		// early-cancel path). On the shared-manager path nothing else
		// removes them (#324); on the owned path this merely beats
		// SpillManager.Cleanup to the unlink.
		for _, f := range h.spillFiles {
			h.Spill.RemoveSpilled(f)
		}
		h.spillFiles = nil
	}
	// Close the streaming merger (if any) and remove its spill files. This
	// is the backstop for early-termination paths (cancellation, error
	// before exhaustion); the normal drain in Next() also calls
	// closePartialMerger when the merger returns nil.
	h.closePartialMerger()
	// Clone-partial runs never handed to a primary (error path where the
	// barrier merge did not run) would otherwise leak on the spill volume.
	for _, path := range h.drainedRuns {
		os.Remove(path)
	}
	h.drainedRuns = nil
	h.spillBuffer = nil
	// Unmap off-heap state LAST: every slice referencing the reservations
	// (flat accs, key SoAs — including any adopted from merged clones) is
	// dropped above or dead with this instance. Drop the slice headers
	// explicitly so a use-after-Close is a nil-index panic rather than a
	// fault into unmapped memory.
	if h.offheap != nil {
		h.intFlatAccs = nil
		h.packedKeys = nil
		h.intKeys = nil
		h.offheap.Close()
		h.offheap = nil
	}
	h.closeRetiredOffheap()
	return nil
}

// Inspect implements memory.AccountedOperator. Wait-free w.r.t. the registry
// but takes h.mu to read group-state fields consistently.
func (h *HashAggregate) Inspect() memory.OperatorFootprint {
	h.mu.Lock()
	defer h.mu.Unlock()
	st := memory.OpState(h.accState.Load())
	if st == memory.OpClosed || h.Spill == nil || !h.canUseExternalMerge() {
		return memory.OperatorFootprint{
			State: memory.OpClosed, InstanceID: h.accInstanceID, Name: h.accName(),
		}
	}
	owned := h.trackedGroupMem
	return memory.OperatorFootprint{
		OwnedBytes:     owned,
		RetainedBytes:  owned, // all group state is detained
		SpillableBytes: h.spillableBytesLocked(),
		SpillReadBytes: h.spillReadBytesLocked(),
		State:          st,
		InstanceID:     h.accInstanceID,
		Name:           h.accName(),
	}
}

// StateBytes reports the current in-memory group-state size. Exposed for
// callers that bound memory without a SpillManager (the shuffle sender's
// capped partial aggregate flushes an epoch when this crosses its cap);
// Inspect() reports zero footprint when no Spill is attached, so it can't
// serve that role.
func (h *HashAggregate) StateBytes() int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.groupMemoryUsage()
}

// accName is the stable identifier for this aggregate instance.
func (h *HashAggregate) accName() string {
	if len(h.GroupByCols) == 0 {
		return "HashAggregate"
	}
	return "HashAggregate/group_by=" + strings.Join(h.GroupByCols, ",")
}

// spillableBytesLocked reports the bytes RequestRelief may target without
// forming a drain-rebuild loop or re-reading about-to-be-merged state (TODO-1).
//
// Derivation: before Finalize installs partialMerger the merge has not begun
// and every in-memory byte is reclaimable; the only cap is that a single
// cooperative drain must leave one survivor partition (pickPartitionsToDrain
// caps at K-1), so the int-keyed path reports perPartition*(K-1). Once
// partialMerger != nil the merger owns the remaining bytes and we report 0 so
// the SpillManager never targets state that is being merged back in. Caller
// holds h.mu.
func (h *HashAggregate) spillableBytesLocked() int64 {
	if h.Spill == nil || !h.canUseExternalMerge() {
		return 0
	}
	if h.partialMerger != nil {
		return 0 // finalize started: merger owns the bytes
	}
	if !h.useIntGroupKey {
		return h.trackedGroupMem // non-int paths whole-drain; all reclaimable
	}
	n := h.intIndexLen()
	if n == 0 {
		return 0
	}
	K := h.drainK
	if K == 0 {
		K = computeAdaptiveK(n)
	}
	if K <= 1 {
		return h.trackedGroupMem
	}
	perPartition := h.trackedGroupMem / int64(K)
	return perPartition * int64(K-1) // never offer the last survivor partition
}

// spillReadBytesLocked reports bytes currently being read back by the merger
// during finalize (about to re-enter the heap; never reclaimable). Caller
// holds h.mu.
func (h *HashAggregate) spillReadBytesLocked() int64 {
	if h.partialMerger != nil {
		return h.trackedGroupMem
	}
	return 0
}

// EstimateRelief implements memory.AccountedOperator: a pure read of the
// rebuild-safe spillable bytes, capped at target.
func (h *HashAggregate) EstimateRelief(target int64) int64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := h.spillableBytesLocked()
	if target > 0 && target < s {
		return target
	}
	return s
}

// selfSpillReliefTarget computes the bytes to release in response to our
// own Consume-time pressure detection. Targets bringing tracker.Used()
// below 55% of budget — 5% hysteresis below the 60% SpillCheap trigger so
// the next batch doesn't immediately re-trip the threshold.
//
// Returns 0 when the budget is unknown (tests without a tracker, ad-hoc
// environments) so the dispatcher falls through to spillFullState — the
// pre-partial-drain behavior.
//
// Caller is the self-spill Consume path; cooperative SpillSome callers
// supply their own target via the Spillable interface.
func (h *HashAggregate) selfSpillReliefTarget() int64 {
	if h.Spill == nil {
		return 0
	}
	t := h.Spill.Tracker()
	if t == nil || h.Spill.SpillBudget() <= 0 {
		return 0
	}
	// SpillBudget: honors a #318 degraded-retry view's reduced cap.
	threshold := h.Spill.SpillBudget() * 55 / 100
	relief := t.Used() - threshold
	if relief <= 0 {
		return 0
	}
	return relief
}

// SpillSome drains a portion of the SoA hash state to a partial-state spill
// file and releases the freed bytes back to the tracker, returning the
// number of bytes released. Called by SpillManager.RequestRelief on behalf
// of a peer operator under memory pressure.
//
// On the int-keyed path, spillPartialState drains a hash-partition slice
// sized roughly to `target` bytes, leaving surviving groups in place. This
// breaks the drain-rebuild loop that whole-table draining created at SF100
// scale (PR #88 → drain-rebuild loop → heartbeat starvation): future
// Consume rows whose keys hash to a surviving partition continue to hit
// existing in-memory groups, paying no rebuild cost.
//
// On other paths (packed, compact, string, generic) and when target
// covers the full footprint, falls through to the whole-drain path —
// semantically identical to the pre-partial-drain behavior.
//
// Implements memory.Spillable and memory.AccountedOperator. The OpSpilling
// state is published for the duration so a concurrent RequestRelief snapshot
// skips this instance rather than double-dispatching.
func (h *HashAggregate) SpillSome(target int64) (int64, error) {
	h.accState.Store(int32(memory.OpSpilling))
	defer h.accState.CompareAndSwap(int32(memory.OpSpilling), int32(memory.OpActive))
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.Spill == nil || !h.canUseExternalMerge() {
		return 0, nil
	}
	before := h.trackedGroupMem
	if before == 0 {
		return 0, nil
	}
	start := time.Now()
	if err := h.spillPartialState(target); err != nil {
		return 0, err
	}
	// Rebase the #325 drain gate on the post-drain footprint: a cooperative
	// drain reclaims just as a self-triggered one does, and leaving the
	// baseline stale would make the next self-drain wait for regrowth past a
	// footprint we no longer hold.
	h.noteDrain(before, start)
	freed := before - h.trackedGroupMem
	if h.accInstanceID != 0 {
		h.Spill.Tracker().PublishOwned(h.accInstanceID, h.trackedGroupMem)
	}
	return freed, nil
}

// Next returns the aggregated results in batches of DefaultBatchSize rows.
//
// With adopted disjoint partitions (partitioned parallel aggregation) the
// emission is fanned across one goroutine per partition when eligible — see
// aggregate_parallel_emit.go. The serial fallback below streams this
// aggregate's own state and then each adopted partition in turn.
func (h *HashAggregate) Next(ctx context.Context) (*batch.RecordBatch, error) {
	if h.emit != nil {
		return h.emit.next()
	}
	if h.parallelEmitEligible() {
		h.startParallelEmit(ctx)
		return h.emit.next()
	}
	b, err := h.nextOwn(ctx)
	if err != nil || b != nil {
		return b, err
	}
	// Own state exhausted — stream adopted disjoint partitions in order.
	for len(h.adoptedPartitions) > 0 {
		b, err := h.adoptedPartitions[0].Next(ctx)
		if err != nil || b != nil {
			return b, err
		}
		h.adoptedPartitions[0].Close()
		h.adoptedPartitions = h.adoptedPartitions[1:]
	}
	return nil, nil
}
