package memory

import "sync/atomic"

// OpState is the lifecycle phase of an AccountedOperator instance. The
// SpillManager uses it to skip operators that cannot release bytes (Closed) or
// that are mid-spill (Spilling) when ranking relief candidates.
type OpState int32

const (
	OpRegistered OpState = iota // registered, not yet holding reclaimable bytes
	OpActive                    // holding bytes; SpillableBytes may be > 0
	OpSpilling                  // a SpillSome is in flight on this instance
	OpClosed                    // finalized/closed; Inspect must report zeros
)

func (s OpState) String() string {
	switch s {
	case OpRegistered:
		return "registered"
	case OpActive:
		return "active"
	case OpSpilling:
		return "spilling"
	case OpClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// ViewRef is a zero-byte handle a shared-build consumer (e.g. a broadcast-join
// probe) reports so attribution does not double-count the shared build. A
// ViewRef NEVER owns bytes and NEVER frees bytes: WouldFreeBytes is always 0.
type ViewRef struct {
	OwnerInstanceID uint64 // InstanceID of the AccountedOperator that owns the bytes
	WouldFreeBytes  int64  // invariant: always 0 (closing a view frees nothing)
}

// OperatorFootprint is the single accounting snapshot an AccountedOperator
// exposes. It subsumes the old SpillableSnapshot (Name/Current/Peak) and adds
// the byte-class split the floating-budget SpillManager ranks on.
//
// Byte-class invariants (asserted by the contract test):
//
//	OwnedBytes     >= RetainedBytes
//	OwnedBytes     >= SpillableBytes
//	SpillableBytes >= 0
//	when State == OpClosed: all byte fields == 0
type OperatorFootprint struct {
	// OwnedBytes is every byte this instance is accountable for: column data
	// plus operator overhead (hash arena/index, scratch). It is what the
	// drift-backstop ranks on and what PublishOwned reports. It corresponds to
	// the old PeakFootprint's unit (incl. overhead) but as a live, not peak,
	// value.
	OwnedBytes int64

	// RetainedBytes is the subset of OwnedBytes held in batches detained past
	// their producer's lifetime (the build/sort/window retain sites).
	// Observability only.
	RetainedBytes int64

	// SpillableBytes is the bytes RequestRelief may target on this instance
	// right now WITHOUT triggering a rebuild loop or re-reading
	// about-to-be-merged state. It is the floating relief signal, not a raw
	// footprint. For Sort post-finalize and HashAggregate post-merger it is 0.
	SpillableBytes int64

	// SpillReadBytes is bytes currently being read back from disk during a
	// merge/finalize (about to re-enter the heap). Reported so the advisor
	// never counts them as reclaimable; never summed into any tier.
	SpillReadBytes int64

	State      OpState
	InstanceID uint64
	Name       string

	// Departed is only set on entries returned by SpillManager.Inspect for
	// closed operators: the number of same-Name instances coalesced into this
	// entry (the footprint itself is the max-peak instance's). Zero on live
	// entries and on snapshots returned by AccountedOperator.Inspect.
	Departed int

	// SharedViews lists zero-byte references this instance holds onto a build
	// owned by another instance (broadcast probes). Empty for owners.
	SharedViews []ViewRef
}

// AccountedOperator is the single interface that replaces Spillable +
// Inspectable. Every pipeline-breaker that detains reclaimable bytes implements
// it and registers with the SpillManager.
type AccountedOperator interface {
	// Inspect returns a coherent point-in-time footprint snapshot. It must be
	// safe to call concurrently with the operator's own pipeline goroutine and
	// with SpillSome (the contract test exercises this under -race), and must
	// return all-zero byte fields with State==OpClosed after Close/Finalize.
	Inspect() OperatorFootprint

	// EstimateRelief reports, without side effects, how many bytes a
	// SpillSome(target) call would free right now. RequestRelief uses it to
	// rank candidates and to detect a delivered<claimed shortfall afterward. It
	// must be <= Inspect().SpillableBytes and must be a pure read.
	EstimateRelief(target int64) int64

	// SpillSome attempts to free at least target bytes by writing reclaimable
	// state to disk, returning bytes actually released back to the shared
	// tracker. RequestRelief serializes calls per instance via OpSpilling, so
	// re-entrancy is not required; concurrency with Inspect IS required. May
	// return < target.
	SpillSome(target int64) (int64, error)
}

// instanceIDCounter hands out process-unique, monotonically increasing
// AccountedOperator instance IDs. It starts at 1 so the first NextInstanceID()
// returns 2 — operator IDs are always >= 2, never colliding with
// batch.ReservoirOwner (1) or the unstamped sentinel (0). That lets a batch's
// ownerID be set directly to an operator InstanceID in a later phase without
// ambiguity.
var instanceIDCounter atomic.Uint64

func init() { instanceIDCounter.Store(1) }

// NextInstanceID returns a fresh process-unique operator instance ID (>= 2).
func NextInstanceID() uint64 { return instanceIDCounter.Add(1) }
