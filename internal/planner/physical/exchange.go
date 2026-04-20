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
// Stage. Stored on the Stage itself (not embedded) to keep Stage a flat
// value type.
type ExchangeStage struct {
	Keys     []string     // Repartition only
	Count    int          // Repartition only
	Ordering []SortKeySpec // Gather only (optional sort-merge gather)
}
