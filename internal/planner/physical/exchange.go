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
	// ComputedCols are expression columns APPENDED to the shuffle payload
	// (after the projected scan columns). Set by dedupeSubsumedScanExchanges
	// so one raw exchange can serve a dropped filtered sibling: the
	// sibling's scan filter ships as a cheap computed flag (1 byte/row)
	// instead of a second full scan+shuffle of the table. Workers evaluate
	// Expr per batch and append the result under Name.
	ComputedCols []ComputedCol
	// ExtraReadCols are columns the shuffle's source scan must READ so the
	// ComputedCols expressions can evaluate, but which are NOT part of the
	// shipped payload — the worker drops them after computing the flags.
	ExtraReadCols []string
	// PartialAggGroupBy/PartialAggSpecs mark this Repartition for
	// sender-side partial aggregation (markExchangePartialAgg): the
	// shuffle task pre-combines rows on PartialAggGroupBy, shipping
	// name-preserving SUM/MIN/MAX partials (OutputCol == InputCol)
	// instead of raw rows. Set only when every consumer is proven
	// merge-compatible; empty means ship raw.
	PartialAggGroupBy []string
	PartialAggSpecs   []AggSpec
}

// ComputedCol is one appended expression column on a shuffle payload.
type ComputedCol struct {
	Name string
	Expr string
}
