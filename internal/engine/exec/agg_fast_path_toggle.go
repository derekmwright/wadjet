package exec

import "github.com/derekmwright/wadjet/internal/optswitch"

// aggFastPaths gates the typed GROUP BY fast-path family as one unit: the
// single-int / dual-int / compact / single-string / generic-SoA hash paths,
// their flat SoA accumulators, and the deferred group-state and key-boxing
// allocation that rides on them. Off forces the generic AoS accumulator
// path (the same code non-simple aggregates always take), which is the
// pre-optimization behavior. Kill switch: WADJET_AGG_FAST_PATHS=0.
var aggFastPaths = optswitch.Register("agg-fast-paths", "WADJET_AGG_FAST_PATHS",
	"typed GROUP BY fast paths: SoA accumulators, deferred group states and key boxing")
