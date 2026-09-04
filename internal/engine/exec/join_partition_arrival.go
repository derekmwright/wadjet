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

// probeRoutesByPartition reports whether every probe row consults ONLY the
// build rows of its own grace partition. It is the precondition
// spillOneInMemoryPartition's contract rests on, and the build dispatch
// (Build, join.go) now CHECKS it rather than assuming it, because a build that
// partitions and evicts is only readable by a probe that routes.
//
// A CROSS join is the join for which it is false, and it is false completely:
// its probe has no key, so `computeBuildPartitionRows` sends every build row
// to ONE partition (the empty key's), `HashJoinProbe.Execute` returns to
// `nextCrossChunk` before the spilled-partition routing runs at all, and
// nextCrossChunk then walks EVERY entry of h.buildBatches for every probe row.
// One eviction therefore nils the slot it is about to read — a nil
// dereference at `buildBatch.Len`, and, if the nil were skipped instead, a
// silently missing row, which is worse.
//
// This is the shape #832 arrives as. A join whose ON clause is an equality of
// EXPRESSIONS rather than of bare columns has no equi-key for the planner to
// give the operator, so it is planned as a cross join with the ON as a filter
// above — `ON CONCAT('x', a.g) = CONCAT('x', b.g)`, `ON (a.c_str || 'x') =
// (b.c_str || 'x')`, `ON UPPER(a.c_str) = UPPER(b.c_str)`, `ON a.id + 1 =
// b.id + 1`, a CAST key, a key computed on one side only. All of them answer
// on the single-process, DAG and DAG-shuffled arms and panic on the spilled
// one, and the panic is not a race: a cross join whose build evicts ALWAYS
// reads a nil slot.
//
// A cross join therefore takes the flat build, which reserves per batch and
// REFUSES when the budget cannot hold the build (ADR-0006: degrade or fail
// loudly, never die and never answer differently). What it does not get is a
// spill, because wadjet has no blockwise nested-loop join to spill INTO; that
// is a distinct piece of work and it is recorded on #832 rather than faked by
// a partitioner whose premise the operator does not meet.
func (h *HashJoin) probeRoutesByPartition() bool {
	return h.JoinType != CrossJoin
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
// That argument is about a KEY-ROUTED probe, and it was written as though
// every probe were one. A CROSS join's is not — it reads every build batch for
// every probe row and never reaches the routing at all — so it read the nil
// slots, which is #832. probeRoutesByPartition above is the same sentence
// turned into a precondition this path's caller checks.
//
// Caller invariants:
//   - h.MemTracker and h.Spill must both be set; otherwise the legacy path runs.
//   - h.probeRoutesByPartition() must hold; otherwise the flat path runs.
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

			// This build can evict, so its index is PER PARTITION: evicting a
			// partition then frees its table, arena and chain with its
			// columns (#823, join_index_parts.go).
			h.initIndexParts(numSpillPartitions)

			// The pre-size hint is bounded by the budget's room — this is
			// unspillable-until-eviction state and pre-sizing it charged
			// 191,072 bytes on a 20-row batch (#823). Each partition takes
			// its share (perPartHint); nothing is allocated until a key
			// lands, so partitions that stay empty cost only their header.
			h.indexHint = h.preSizeRowHint(b)

			h.spillState = newSpillState(h.Spill.SpillDir(), b.Schema)
		}

		if err := h.absorbArrivalBatch(b); err != nil {
			h.mu.Unlock()
			return err
		}

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
		h.JoinType == RightSemiJoin || h.JoinType == RightAntiJoin) && h.arenaRows() > 0 {
		h.allocMatched()
		h.matchedAlloc = true
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

// minArrivalChunkRows is the floor below which splitting an arrival batch
// stops being a way to fit the build and starts being a way to spend the
// query's time. A build that cannot reserve 32 rows' worth of columns has a
// budget below this operator's floor, and the honest answer there is the loud
// refusal ADR-0006 asks for, not 5,000 single-row reservations.
const minArrivalChunkRows = 32

// absorbArrivalBatch charges one arrival batch to the shared pool, scatters it
// into its grace partitions and indexes it. Caller holds h.mu.
//
// Like the legacy path it Reserves and falls back to spilling on over-budget;
// unlike the legacy path the spill is incremental - pick one partition and
// evict it instead of repartitioning the whole flat state.
//
// #598 is the third fallback, after Reserve and after eviction: a batch whose
// own columns do not fit the pool is SPLIT and absorbed in pieces. Without it
// the build's FIRST batch had nowhere to go - largestInMemoryPartition returns
// -1 when nothing has been stored yet, so spillUntilCanReserve frees 0 and the
// retry fails for exactly the reason the first attempt did, and the query died
// with `used=0, requested=7813532` while the same rows delivered in smaller
// batches built fine. The trigger is exactly hashBuildBytes(b) > what the pool
// can give, which the parquet ROW GROUP decides: the scan hands the build one
// batch per row group, so a fat row group is a fat arrival batch.
//
// Splitting and not overcommitting is deliberate. The filing's other direction
// - reserve past the budget for the first batch - would be another unceilinged
// ForceReserve producer on a query tracker (ADR-0006's 2026-09-03 census
// enumerates the ones that exist, two of them in this file's own operator), and
// the overcommitted bytes would join the floor every DOWNSTREAM operator's
// Reserve is measured against. Splitting adds no new overcommit.
//
// # The reservation is RECONCILED to what the build kept
//
// One arrival batch is charged ONCE, as hashBuildBytes(b). What happens to its
// rows afterwards is one of two things: they are appended to an in-memory
// partition (retained, and released when that partition is evicted) or written
// straight to an already-spilled partition (retained by nobody). So the release
// owed at the end of this call is `cost - retained`, and `retained` is the sum
// of what partitionAndIndexBatch actually put into partMemory.
//
// It used to be neither of those. The spilled branch released
// hashBuildBytes(compactBatchForRows(b, rows)) PER PARTITION — a figure
// computed from a freshly minted batch, which pays the per-column fixed
// overhead (null-bitmap words, a bytes column's len+1 offsets, capacity
// rounding) once per partition against an arrival batch that paid it once.
// Measured on this arc's fixture: an arrival batch of 256 rows charged 24,932
// bytes released 30,372 across its 63 partitions — 1.22x, over-releasing 5,440
// bytes EVERY BATCH. The in-memory branch was wrong in the other direction: it
// charges partMemory the tight per-row data bytes, which is less than the
// arrival share, so a build that never spilled leaked ~1,000 bytes per batch
// upward. Which way a build drifted therefore followed how many partitions had
// spilled by the time each batch arrived, i.e. pressure and timing — the
// moving floor of #789 — and at 100,000 build rows `used` reached MINUS 867,561
// against a 1 MiB budget, a ledger that under-reports by 1.67 MB and admits the
// next operator against room that does not exist.
func (h *HashJoin) absorbArrivalBatch(b *batch.RecordBatch) error {
	cost := hashBuildBytes(b)
	if joinFloorArmed.Load() {
		// One relaxed load per arrival batch, disarmed in production. It is
		// the instant #789 is a question about (join_floor_probe.go).
		noteJoinFloor(h.MemTracker)
	}
	if err := h.MemTracker.Reserve(cost); err != nil {
		if spillErr := h.spillUntilCanReserve(cost); spillErr != nil {
			return fmt.Errorf("hash join build: %w (build_rows=%d, batches=%d)",
				spillErr, h.buildRows, len(h.buildBatches))
		}
		if err2 := h.MemTracker.Reserve(cost); err2 != nil {
			if chunk := h.arrivalChunkRows(b, cost); chunk > 0 {
				return h.absorbInChunks(b, chunk)
			}
			return fmt.Errorf("hash join build: %w (build_rows=%d, batches=%d)",
				err2, h.buildRows, len(h.buildBatches))
		}
	}
	h.trackedMem += cost

	// Update key min/max even before partitioning - bloom + dynamic-range
	// pushdowns use these for ALL keys, including those that later spill.
	h.updateKeyMinMax(b)

	retained, err := h.partitionAndIndexBatch(b)
	if err != nil {
		return fmt.Errorf("partition+index: %w", err)
	}
	h.reconcileArrivalCharge(cost, retained)

	// Reactive spill if pool pressure is over the spill-cheap threshold
	// (60% of budget) - release one partition's bytes before the next
	// batch arrives. spillUntilCanReserve uses the same logic but is
	// driven by Reserve failure; here we proactively keep headroom.
	if h.Spill.ShouldSpillFor(memory.SpillCheap) {
		if _, err := h.spillOneInMemoryPartition(); err != nil {
			return fmt.Errorf("spilling under pressure: %w", err)
		}
	}

	h.reconcileHashMemory()
	return nil
}

// arrivalChunkRows returns how many rows of b to absorb at a time when b as a
// whole cannot be reserved, or 0 when splitting cannot help and the build must
// refuse loudly.
//
// It is asked only after eviction has already run, so Used() is at this
// query's floor and (budget - used) is the largest reservation that can still
// be satisfied. Half of that is the target, leaving the other half for the
// index growth the partition step charges through reconcileHashMemory
// immediately afterwards. The result is capped at accumFlushRows so a split
// batch stays on the per-partition accumulator's tuned path, and floored at
// minArrivalChunkRows so a budget below this operator's floor refuses instead
// of grinding.
func (h *HashJoin) arrivalChunkRows(b *batch.RecordBatch, cost int64) int {
	rows := b.ActiveLen()
	if rows < 2 || cost <= 0 {
		return 0
	}
	budget := h.MemTracker.Budget()
	if budget <= 0 {
		return 0 // no budget to fit into: the Reserve failed for another reason
	}
	avail := (budget - h.MemTracker.Used()) / 2
	if avail <= 0 {
		return 0
	}
	perRow := cost / int64(rows)
	if perRow <= 0 {
		perRow = 1
	}
	chunk := int(avail / perRow)
	if chunk > accumFlushRows {
		chunk = accumFlushRows
	}
	if chunk < minArrivalChunkRows || chunk >= rows {
		return 0 // splitting buys nothing, or buys too little to be worth it
	}
	return chunk
}

// absorbInChunks re-offers b to absorbArrivalBatch in dense slices of chunk
// rows. Each slice is a COPY (compactBatchForRows), so what the build stores
// and charges is the slice's own footprint rather than a view onto a batch
// whose MemBytes describes all of it; b itself is released by its owner as
// usual. A chunk that still cannot be reserved refuses through the same path
// as any other batch. The recursion is bounded, not one level deep: a chunk
// that still cannot reserve re-enters arrivalChunkRows, which either returns 0
// (chunk >= rows, or below minArrivalChunkRows) and refuses, or returns a
// strictly smaller chunk. Since every step divides by at least two and stops
// at minArrivalChunkRows, the depth is at most log2(rows/32) - and no shape
// found in this arc's probes recursed twice.
func (h *HashJoin) absorbInChunks(b *batch.RecordBatch, chunk int) error {
	rows := activeRowIndexes(b)
	for start := 0; start < len(rows); start += chunk {
		end := min(start+chunk, len(rows))
		part := compactBatchForRows(b, rows[start:end])
		if err := h.absorbArrivalBatch(part); err != nil {
			return err
		}
	}
	return nil
}

// activeRowIndexes returns b's live row positions: its selection vector when
// it carries one, and 0..Len otherwise.
func activeRowIndexes(b *batch.RecordBatch) []int {
	if b.Sel != nil {
		out := make([]int, len(b.Sel))
		for i, s := range b.Sel {
			out[i] = int(s)
		}
		return out
	}
	out := make([]int, b.Len)
	for i := range out {
		out[i] = i
	}
	return out
}

// partitionAndIndexBatch scatters b's rows into hash partitions, writing rows
// for already-spilled partitions directly to disk and appending rows for
// in-memory partitions to h.buildBatches with a fresh global batch index plus
// a parallel ss.batchPartID entry. For in-memory rows it also indexes the
// resulting per-partition mini-batch into the global hash table via the same
// indexBuildBatch helper used by the legacy partitioned-reload path.
//
// Caller must hold h.mu.
// It returns the bytes this batch left RESIDENT — the sum of what it added to
// partMemory, which is what an eviction will release. Its caller reconciles the
// arrival reservation against that figure.
func (h *HashJoin) partitionAndIndexBatch(b *batch.RecordBatch) (int64, error) {
	var retained int64
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
			if err := ss.writeBuildBatch(partID, partBatch); err != nil {
				return retained, fmt.Errorf("writing build batch for spilled partition %d: %w", partID, err)
			}
			// These rows went to disk, so they are retained by nobody and add
			// nothing to `retained`. The caller's reconcile is what gives their
			// share of the arrival reservation back — releasing a figure
			// computed HERE, from a batch minted here, is what over-released
			// (see absorbArrivalBatch).
			continue
		}

		// In-memory partition. Eligible (scalar/bytes/decimal) schemas append
		// into a reusable growable per-partition accumulator; nested-column
		// schemas index one tightly-sized batch per arrival (append-grow can't
		// extend nested element storage).
		if ss.accumEligible {
			n, err := h.appendToAccum(b, partID, rows)
			if err != nil {
				return retained, err
			}
			retained += n
		} else {
			retained += h.freezeTightForPartition(partID, b, rows)
		}
	}
	return retained, nil
}

// reconcileArrivalCharge brings the arrival batch's single reservation to what
// the build actually kept. `retained` is what went into partMemory and will be
// released, byte for byte, when those partitions are evicted; the rest of the
// reservation covered a copy that is over.
//
// The retained figure can EXCEED the reservation — 64 tightly-minted
// per-partition batches carry the per-column fixed overhead 64 times over — and
// then the difference is force-reserved, because that memory is resident
// whether the ledger admits it or not. It is producer 6's class and it carries
// producer 6's purpose.
func (h *HashJoin) reconcileArrivalCharge(cost, retained int64) {
	if h.MemTracker == nil {
		return
	}
	switch {
	case retained < cost:
		give := cost - retained
		h.MemTracker.Release(give)
		h.trackedMem -= give
	case retained > cost:
		take := retained - cost
		h.MemTracker.ForceReserveFor(take, memory.ForceJoinPartitionStore)
		h.forcedStoreBytes += take
		h.trackedMem += take
	}
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
func (h *HashJoin) freezeTightForPartition(partID int, b *batch.RecordBatch, rows []int) int64 {
	ss := h.spillState
	tight := compactBatchForRows(b, rows)
	tight.Detach()
	kept := tight.MemBytes() + int64(len(rows))*40
	ss.partMemory[partID] += kept
	h.registerFrozen(partID, tight)
	return kept
}

// appendToAccum appends b's selected rows for an in-memory partition into that
// partition's open accumulator, freezing it first if the rows would overflow
// accumFlushRows. partMemory is charged with the TIGHT byte footprint of the
// appended rows (independent of the accumulator's physical capacity), keeping
// the spill accounting balanced against the arrival batch's Reserve(cost) and
// making an open accumulator's rows visible to spill selection before freeze.
//
// Caller must hold h.mu.
func (h *HashJoin) appendToAccum(b *batch.RecordBatch, partID int, rows []int) (int64, error) {
	ss := h.spillState

	// Defensive: an arrival batch never exceeds DefaultBatchSize, so a fresh
	// accumulator always fits one partition's slice within accumFlushRows. If a
	// future caller delivers an oversized batch, freeze any partial accumulator
	// (to preserve arrival order within the partition) and index the oversized
	// slice as one tight batch rather than overflowing the freeze bound.
	if len(rows) > accumFlushRows {
		if err := h.freezeAccum(partID); err != nil {
			return 0, err
		}
		return h.freezeTightForPartition(partID, b, rows), nil
	}

	acc := ss.openAccum[partID]
	if acc != nil && acc.Len+len(rows) > accumFlushRows {
		if err := h.freezeAccum(partID); err != nil {
			return 0, err
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
	kept := dataBytes + int64(len(rows))*40
	ss.partMemory[partID] += kept
	ss.openAccumData[partID] += dataBytes
	return kept, nil
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
			h.MemTracker.ForceReserveFor(excess, memory.ForceJoinPartitionStore)
			h.forcedStoreBytes += excess
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

	// Pre-grow THIS partition's index for the batch's rows so PutNoGrow won't
	// overflow during indexing. Every row of a frozen batch belongs to partID
	// by construction (computeBuildPartitionRows scattered them), so the whole
	// batch's growth lands on one part.
	pt := &h.parts[partID]
	h.growPartFor(pt, frozen.ActiveLen())
	h.indexBuildBatch(frozen, batchIdx)
	h.checkGrowPart(pt)
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
	if !h.probeRoutesByPartition() {
		// Unreachable through Build, which no longer gives such a join a
		// spillState — but the precondition belongs where the nil-ing
		// happens, not only where the path is chosen. Freeing nothing makes
		// spillUntilCanReserve stop and the build refuse loudly, which is the
		// disposition, rather than nil-ing a slot the probe will read (#832).
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

	// The partition's index goes with its columns — that is the whole of #823.
	// freeIndexPart drops the table, arena, chain and matched bitmap; the
	// reconcile below turns the smaller indexBytes() into a tracker release
	// under the purpose the growth was charged with.
	h.freeIndexPart(partID)

	h.releaseStoreBytes(freed)
	h.reconcileHashMemory()
	return freed, nil
}

// releaseStoreBytes gives back n bytes of build-side storage, taking the FORCED
// portion off the forced census first.
//
// The split is aggregate, not per partition: a build whose per-partition
// batches together weigh more than the arrival batch they were cut from forces
// the difference once per arrival batch, and that excess is not attributable to
// one partition. What the census reports is the OUTSTANDING forced total, and
// that total is exact — every forced byte is released as forced, here or at
// Close. Which eviction returns it is not something the census claims.
func (h *HashJoin) releaseStoreBytes(n int64) {
	if h.MemTracker == nil || n <= 0 {
		return
	}
	forced := min(n, h.forcedStoreBytes)
	if forced > 0 {
		h.MemTracker.ReleaseForced(forced, memory.ForceJoinPartitionStore)
		h.forcedStoreBytes -= forced
	}
	if rest := n - forced; rest > 0 {
		h.MemTracker.Release(rest)
	}
	// NOT clamped at zero. The arrival reservation is reconciled to what the
	// build kept, so what an eviction releases is exactly what that partition
	// was charged and this cannot go negative — and if it ever does, the
	// ledger-conservation gate's `used == trackedMem` assert is the place to
	// find out, which a clamp would take away. The same argument the tracker's
	// own Release makes, in the arc's own code.
	h.trackedMem -= n
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
