package physical

import (
	"os"
	"strings"
	"sync/atomic"
)

// ElidedCoPartitionedExchanges counts identity exchanges removed at plan
// time (observability; mirrors coordinator.SkewSplitsPlanned).
var ElidedCoPartitionedExchanges atomic.Int64

// elideCoPartitionedExchanges removes an identity exchange-repartition only
// with one dependency in the same ClusterID, HashPartitioned distribution,
// identical partition Count and per-position case-insensitive key NAME match.
// No cross-name equi-join equivalence until Satisfies and consistency checks
// share it. Rewire Dependencies/LeftDepStage/RightDepStage to the dependency;
// distribution labels stay valid. Join task p consumes and emits partition p.
// WADJET_EXCHANGE_ELIDE=0 disables elision.
// See docs/internals/copartitioned-exchange-elision.md for the design.
var exchangeElide = os.Getenv("WADJET_EXCHANGE_ELIDE") != "0"

func elideCoPartitionedExchanges(stages []Stage) []Stage {
	if !exchangeElide {
		return stages
	}
	byID := make(map[string]*Stage, len(stages))
	for i := range stages {
		byID[stages[i].ID] = &stages[i]
	}
	elided := make(map[string]string) // exchange ID → its dep's ID
	for i := range stages {
		e := &stages[i]
		if e.Type != StageExchangeRepartition || e.Exchange == nil {
			continue
		}
		if len(e.Dependencies) != 1 || len(e.Exchange.Keys) == 0 || e.Exchange.Count <= 0 {
			continue
		}
		d, ok := byID[e.Dependencies[0]]
		if !ok || d.ClusterID != e.ClusterID {
			continue
		}
		if _, chained := elided[d.ID]; chained {
			// D itself is being elided this pass; one layer per pass
			// keeps the rewiring trivially correct (a second pass would
			// catch the rest — the shape does not occur in practice).
			continue
		}
		if d.Distribution.Kind != DistHashPartitioned ||
			d.Distribution.Count != e.Exchange.Count ||
			len(d.Distribution.Keys) != len(e.Exchange.Keys) {
			continue
		}
		match := true
		for k := range e.Exchange.Keys {
			if !strings.EqualFold(d.Distribution.Keys[k], e.Exchange.Keys[k]) {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		// …and at the same TYPE. A key pair that needed WIDENING is hashed at
		// the pair's resolved common type, not the column's (#615), so two
		// exchanges over the same column NAME are only interchangeable when
		// they also agree about the width they hashed at — otherwise eliding
		// one leaves its consumer reading rows partitioned for a different
		// join, which matches nothing and says nothing.
		if !keyTypesEqual(d.Distribution.KeyTypes, e.Exchange.KeyTypes, len(e.Exchange.Keys)) {
			continue
		}
		elided[e.ID] = d.ID
		ElidedCoPartitionedExchanges.Add(1)
	}
	if len(elided) == 0 {
		return stages
	}
	out := make([]Stage, 0, len(stages)-len(elided))
	for i := range stages {
		s := stages[i]
		if _, drop := elided[s.ID]; drop {
			continue
		}
		for di, dep := range s.Dependencies {
			if repl, ok := elided[dep]; ok {
				s.Dependencies[di] = repl
			}
		}
		if repl, ok := elided[s.LeftDepStage]; ok {
			s.LeftDepStage = repl
		}
		if repl, ok := elided[s.RightDepStage]; ok {
			s.RightDepStage = repl
		}
		// A CHAINED or FUSED join names its build side in a THIRD place, and
		// that one is not a Dependencies entry: `ChainedJoinSpec.BuildDepStage`
		// is looked up in the dispatcher's `inputs` map by ID. Rewiring the
		// two fields above and not these left a chained join pointing at a
		// stage this pass had just deleted — `stage join-4 chained join 0:
		// build dep "exchange-repartition-7" output not found`, at DISPATCH,
		// on a query PostgreSQL and the broadcast lowering both answer
		// (#755). The elision is sound for them for the same reason it is
		// sound for a dependency: E's output was byte-identical to D's.
		for k := range s.FusedJoins {
			if repl, ok := elided[s.FusedJoins[k].BuildDepStage]; ok {
				s.FusedJoins[k].BuildDepStage = repl
			}
		}
		for k := range s.ChainedJoins {
			if repl, ok := elided[s.ChainedJoins[k].BuildDepStage]; ok {
				s.ChainedJoins[k].BuildDepStage = repl
			}
		}
		out = append(out, s)
	}
	return out
}
