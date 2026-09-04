package exec

import (
	"sync/atomic"

	"github.com/derekmwright/wadjet/internal/engine/memory"
)

// #789 is a question about ONE INSTANT: what the query tracker holds when a
// hash join build makes its first reservation. That is the floor every later
// admission in the query is measured against, and the filing's complaint is
// that it differed run to run for one query on one fixture — 465738, 447345,
// 449423, 482816, 492138, 660967.
//
// A number at an instant cannot be read out of a row set, and it cannot be read
// out of a log either: the WARNs only fire when something crosses the budget.
// So the instant gets a probe. It is DISARMED by default and the production
// cost is one relaxed atomic load per ARRIVAL BATCH — not per row — on the
// grace build's charge path.
//
// It records the tracker's total AND the forced census split, because "the
// floor moved" is only half an answer: the other half is whose bytes moved, and
// the census names them.

// JoinFloorSnapshot is the query tracker's state at the first reservation of a
// grace build. Seen is false when no such build ran, which is a gate's cue that
// it measured nothing rather than measured zero.
type JoinFloorSnapshot struct {
	Seen             bool
	Used             int64
	ScanFileLoad     int64
	ScanDecodedBatch int64
	ScanPooledBuffer int64
	JoinIndex        int64
}

var (
	joinFloorArmed atomic.Bool
	joinFloorTaken atomic.Bool
	joinFloorSnap  atomic.Pointer[JoinFloorSnapshot]
)

// ArmJoinFloorProbe starts recording and returns a function that stops it and
// hands back what was recorded. TEST-ONLY: it is process-wide and single-shot,
// so a caller owns the probe for the length of one query.
func ArmJoinFloorProbe() func() JoinFloorSnapshot {
	joinFloorSnap.Store(nil)
	joinFloorTaken.Store(false)
	joinFloorArmed.Store(true)
	return func() JoinFloorSnapshot {
		joinFloorArmed.Store(false)
		if s := joinFloorSnap.Load(); s != nil {
			return *s
		}
		return JoinFloorSnapshot{}
	}
}

// noteJoinFloor records the first armed reservation and nothing after it.
func noteJoinFloor(t *memory.Tracker) {
	if t == nil || !joinFloorTaken.CompareAndSwap(false, true) {
		return
	}
	joinFloorSnap.Store(&JoinFloorSnapshot{
		Seen:             true,
		Used:             t.Used(),
		ScanFileLoad:     t.ForcedFor(memory.ForceScanFileLoad),
		ScanDecodedBatch: t.ForcedFor(memory.ForceScanDecodedBatch),
		ScanPooledBuffer: t.ForcedFor(memory.ForceScanPooledBuffer),
		JoinIndex:        t.ForcedFor(memory.ForceJoinIndex),
	})
}
