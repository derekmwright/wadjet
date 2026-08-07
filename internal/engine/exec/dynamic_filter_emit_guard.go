package exec

import (
	"context"

	"github.com/citc-tech/wadjet/internal/engine/batch"
)

// Guarded re-emit support (docs/design/attach-on-arrival-dynamic-filters.md
// §Guarded re-emit): a cascade mid-scan consumes an upstream bloom attach-
// style AND emits its own downstream filter. Rows scanned before the
// consumed bloom installs would poison the emitted bloom with keys the
// upstream filter drops — so while any guard is unresolved, the emit op
// buffers (emit-key, guard-value) pairs instead of inserting, and retro-
// filters the buffer once every guard settles (mid-scan or at finalize).
// The emitted bloom ends exactly as tight as under the start barrier.

// GuardProbe is the emit op's view of one pending upstream bloom. The
// worker adapts its attach-on-arrival poll slot to this interface.
type GuardProbe interface {
	// TryResolve returns the bloom if it is available now.
	TryResolve() (bloom []uint64, mask uint64, ok bool)
	// Done reports whether the poll has terminated (bloom available or
	// permanently given up).
	Done() bool
	// Wait blocks until the poll terminates or ctx ends.
	Wait(ctx context.Context) (bloom []uint64, mask uint64, ok bool)
}

// emitGuardBufferCap bounds the pair buffer. The cascade marking passes cap
// eligible mid-scan stages at ~2M rows, so a compliant stage never hits
// this; a mis-estimate degrades to unguarded inserts (wider bloom — still
// drop-only correct), never to unbounded memory.
const emitGuardBufferCap = 2 << 20

// emitGuard tracks one upstream bloom guarding this emit.
type emitGuard struct {
	filterID string
	column   string
	probe    GuardProbe

	colIdx   int
	resolved bool // colIdx resolution attempted

	settled bool // probe terminated
	have    bool // bloom available (settled && ok)
	bloom   []uint64
	mask    uint64

	vals  []int64
	nulls []bool
}

// settle refreshes the guard's probe state. Returns true when terminated.
func (g *emitGuard) settle() bool {
	if g.settled {
		return true
	}
	if bloom, mask, ok := g.probe.TryResolve(); ok {
		g.bloom, g.mask, g.have = bloom, mask, true
		g.settled = true
		return true
	}
	if g.probe.Done() {
		// Terminated without a bloom (withheld/expired): guard passes all
		// buffered rows — identical to the consume side's "no filter"
		// degradation, wider bloom, drop-only correct.
		g.settled = true
		return true
	}
	return false
}

// passes reports whether a buffered row survives this guard.
func (g *emitGuard) passes(i int) bool {
	if !g.have {
		return true // no bloom ⇒ nothing to check
	}
	if g.colIdx < 0 {
		// Guard column unresolvable in the batch schema: the guard cannot
		// be applied, and wrongly dropping a key falsely rejects rows
		// downstream. Pass (wider bloom, drop-only correct).
		return true
	}
	if g.nulls[i] {
		// BloomFilterOp drops null-key rows; the retro-filter must match.
		return false
	}
	return bloomContains(g.bloom, g.mask, bloomHashInt(g.vals[i]))
}

// AddGuard attaches an upstream pending bloom to this emit. Must be called
// before the first Execute.
func (op *DynamicFilterEmitOp) AddGuard(filterID, column string, probe GuardProbe) {
	op.guards = append(op.guards, &emitGuard{
		filterID: filterID, column: column, probe: probe, colIdx: -1,
	})
}

// guardsSettled refreshes and reports whether every guard has terminated.
func (op *DynamicFilterEmitOp) guardsSettled() bool {
	all := true
	for _, g := range op.guards {
		if !g.settle() {
			all = false
		}
	}
	return all
}

// bufferBatch appends the batch's active rows to the guard buffer instead
// of inserting them. Emit-key nulls are skipped (the direct path never
// inserts them either). Returns false — consuming NOTHING — when the batch
// would overflow the cap: the caller degrades and routes the whole batch
// through the direct path, so no key is ever lost (a lost key falsely
// rejects rows downstream).
func (op *DynamicFilterEmitOp) bufferBatch(in *batch.RecordBatch) bool {
	if len(op.bufKeys)+in.ActiveLen() > emitGuardBufferCap {
		return false
	}
	keyCol := in.Columns[op.keyIdx]
	// Resolve guard columns against this batch's schema (same lazy pattern
	// and suffix fallback as the emit key column).
	for _, g := range op.guards {
		if !g.resolved {
			g.colIdx = in.ColumnIndex(g.column)
			if g.colIdx < 0 {
				g.colIdx = uniqueSuffixColumnIndex(in, g.column)
			}
			g.resolved = true
		}
	}
	appendRow := func(idx int) bool {
		if keyCol.Nulls.IsNullFast(idx) {
			return true
		}
		key, ok := intKeyFromVector(keyCol, idx)
		if !ok {
			return true
		}
		op.bufKeys = append(op.bufKeys, key)
		for _, g := range op.guards {
			if g.colIdx < 0 {
				// Unresolvable guard column: placeholder entry; passes()
				// short-circuits to pass for the whole guard. Tracked for
				// the runner's observability line.
				op.guardUnresolvable = true
				g.vals = append(g.vals, 0)
				g.nulls = append(g.nulls, false)
				continue
			}
			gcol := in.Columns[g.colIdx]
			if gcol.Nulls.IsNullFast(idx) {
				g.vals = append(g.vals, 0)
				g.nulls = append(g.nulls, true)
				continue
			}
			v, vok := intKeyFromVector(gcol, idx)
			if !vok {
				g.vals = append(g.vals, 0)
				g.nulls = append(g.nulls, true)
				continue
			}
			g.vals = append(g.vals, v)
			g.nulls = append(g.nulls, false)
		}
		return true
	}
	if in.Sel != nil {
		for _, idx := range in.Sel {
			if !appendRow(int(idx)) {
				return false
			}
		}
	} else {
		for i := 0; i < in.Len; i++ {
			if !appendRow(i) {
				return false
			}
		}
	}
	return true
}

// flushGuardBuffer runs every buffered pair through the settled guards and
// inserts survivors into the bloom/range. Clears the buffer.
func (op *DynamicFilterEmitOp) flushGuardBuffer() {
	for i, key := range op.bufKeys {
		pass := true
		for _, g := range op.guards {
			if !g.passes(i) {
				pass = false
				break
			}
		}
		if pass {
			op.insertKey(key)
		} else {
			op.guardDropped++
		}
	}
	op.guardBuffered += int64(len(op.bufKeys))
	op.bufKeys = nil
	for _, g := range op.guards {
		g.vals, g.nulls = nil, nil
	}
}

// degradeGuardsToUnguarded flushes the buffer with NO guard filtering and
// marks the op overflowed: subsequent rows insert directly. Wider bloom,
// drop-only correct.
func (op *DynamicFilterEmitOp) degradeGuardsToUnguarded() {
	for _, key := range op.bufKeys {
		op.insertKey(key)
	}
	op.guardBuffered += int64(len(op.bufKeys))
	op.bufKeys = nil
	for _, g := range op.guards {
		g.vals, g.nulls = nil, nil
	}
	op.guardOverflowed = true
}

// FinalizeGuards settles outstanding guards (waiting up to the ctx
// deadline) and flushes the buffer. Must be called before Snapshot on
// guarded ops; a nil-guard op returns immediately. The caller bounds ctx —
// waiting preserves emitted-bloom quality when the scan outran the
// upstream bloom, at the cost of delaying this stage's partial upload.
func (op *DynamicFilterEmitOp) FinalizeGuards(ctx context.Context) {
	if len(op.guards) == 0 {
		return
	}
	for _, g := range op.guards {
		if g.settle() {
			continue
		}
		if bloom, mask, ok := g.probe.Wait(ctx); ok {
			g.bloom, g.mask, g.have = bloom, mask, true
		}
		g.settled = true
	}
	if len(op.bufKeys) > 0 {
		op.flushGuardBuffer()
	}
}

// GuardStats reports the guarded-emit telemetry for the runner's log line.
func (op *DynamicFilterEmitOp) GuardStats() (guards int, buffered, dropped int64, overflowed, unresolvable bool) {
	return len(op.guards), op.guardBuffered, op.guardDropped, op.guardOverflowed, op.guardUnresolvable
}
