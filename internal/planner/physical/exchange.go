// Package physical defines stage-type string constants used by
// Stage.Type. Exchange stages are inserted by EnsureDistribution
// to bridge distribution mismatches between child output and parent
// required input.
package physical

const (
	StageScan          = "scan"
	StageAggregate     = "aggregate"
	StageSort          = "sort"
	StageHashJoin      = "hash_join"
	StageBroadcastJoin = "broadcast_join"
	// StageSortMergeJoin is a hash-shuffled join executed as a sort-merge
	// join (docs/design/sort-merge-join.md): identical exchange children and
	// distribution properties to StageHashJoin — only the join operator
	// differs. Emitted when the SortMergeJoinBytes gate passes.
	StageSortMergeJoin = "sort_merge_join"
	StageWindow        = "window"
	StagePipeline      = "pipeline"

	// Exchange stages — inserted by EnsureDistribution.
	// Repartition is the rename of the legacy "shuffle" type; the string
	// value changes so that the old name does not silently leak through.
	StageExchangeRepartition = "exchange-repartition"
	StageExchangeReplicate   = "exchange-replicate"
	StageExchangeGather      = "exchange-gather"
)

// ExchangeStage carries the per-variant payload attached to an Exchange
// Stage. Stored on Stage.Exchange (pointer) so non-Exchange stages pay
// no memory cost.
//
// Keys, Count are Repartition-only. Ordering is Gather-only.
// BuildAlias, ProbeAlias, BuildBytes are populated by EnsureDistribution
// on Repartition and (BuildAlias/ProbeAlias only) Replicate stages, so
// the coordinator lowering pass can synthesize ShuffleCandidate without
// calling PickShuffleCandidate.
type ExchangeStage struct {
	Keys       []string      // Repartition only
	Count      int           // Repartition only
	Ordering   []SortKeySpec // Gather only (optional sort-merge gather)
	BuildAlias string        // Repartition, Replicate
	ProbeAlias string        // Repartition, Replicate
	BuildBytes int64         // Repartition (for logging / threshold checks)
}
