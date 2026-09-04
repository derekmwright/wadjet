package memory

// ForcePurpose names what a charge taken PAST the budget was taken for.
//
// ADR-0006's 2026-09-03 census enumerates seven producers that ForceReserve a
// QUERY tracker. Every one of them is memory that exists and that the budget
// did not admit, so every downstream Reserve is measured against it — and,
// before this enumeration, a refusal could report `used=465738` with nothing to
// say about whose bytes those were. That was the missing diagnostic in #789's
// investigation, which spent three rounds asking what the floor was made of.
//
// A purpose is decremented by ReleaseForced, so what the census reports is the
// OUTSTANDING forced bytes, not a lifetime total. Every producer that can force
// pairs its charge with a release under the same purpose; a producer that does
// not know whether its reservation was forced keeps the flag ReserveOrForce
// hands back.
type ForcePurpose uint8

const (
	// ForceUnattributed is what a bare ForceReserve records. It is not a
	// category, it is a producer nobody has named yet.
	ForceUnattributed ForcePurpose = iota
	// ForceScanFileLoad is a parquet file's (or row group's) bytes, charged
	// before the read so admission sees the load coming (producers 1 and 2).
	ForceScanFileLoad
	// ForceScanDecodedBatch is a decoded row-group batch, charged after the
	// decode and released when the consumer takes it (producer 3).
	ForceScanDecodedBatch
	// ForceScanPooledBuffer is the eager scan path's whole-file buffers,
	// released at scan close (producer 4).
	ForceScanPooledBuffer
	// ForceJoinIndex is a hash join's index: tables, arenas, chains, bloom
	// (producer 5).
	ForceJoinIndex
	// ForceJoinPartitionStore is what a grace build retains for its
	// in-memory partitions above what the arrival batch reserved — the
	// per-partition accumulator's fixed-capacity excess and the tight
	// per-partition batches (producer 6).
	ForceJoinPartitionStore
	// ForceSpillTracking is an operator batch charged past the budget so
	// ShouldSpill can see it (producer 7).
	ForceSpillTracking

	numForcePurposes
)

// String is the name a WARN line and a refusal message use.
func (p ForcePurpose) String() string {
	switch p {
	case ForceScanFileLoad:
		return "scan file load"
	case ForceScanDecodedBatch:
		return "scan decoded batch"
	case ForceScanPooledBuffer:
		return "scan pooled buffer"
	case ForceJoinIndex:
		return "hash join index"
	case ForceJoinPartitionStore:
		return "hash join partition store"
	case ForceSpillTracking:
		return "spill tracking"
	default:
		return "unattributed"
	}
}
