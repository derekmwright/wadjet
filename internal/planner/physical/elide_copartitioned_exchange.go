package physical

import (
	"os"
	"strings"
	"sync/atomic"
)

// ElidedCoPartitionedExchanges counts identity exchanges removed at plan
// time (observability; mirrors coordinator.SkewSplitsPlanned).
var ElidedCoPartitionedExchanges atomic.Int64

// elideCoPartitionedExchanges removes exchange-repartition stages whose
// input is ALREADY hash-partitioned exactly as the exchange would
// partition it — the identity re-shuffle (docs/design/exchange-reuse.md
// §2 B). walkStages emits the two input exchanges of every hash join
// unconditionally, and EnsureDistribution only ever ADDS exchanges, so a
// join whose probe side is itself a co-partitioned join output gets its
// rows re-hashed, re-written, and re-read for nothing: Q18's
// repartition-15 moved 40.13 GB / 600M rows at SF100 to reproduce the
// exact partition layout join-8 had already produced.
//
// Elision rule, conservative by construction:
//   - E is exchange-repartition, single dependency D, same ClusterID;
//   - D.Distribution is HashPartitioned with E's exact partition Count;
//   - per position, D's distribution key equals E's key BY NAME
//     (case-insensitive). Cross-name equivalence through equi-join pairs
//     (o_orderkey ≡ l_orderkey after join-8) is deliberately out of this
//     slice: AssertExchangeConsistency and Distribution.Satisfies compare
//     names, and both TPC-H identity shuffles (Q18 r-15, Q21 r-10/r-14)
//     are exact-name matches. Equivalence classes are lever B2 and land
//     together with the Satisfies() extension or not at all.
//
// Consumers of E are rewired to D (Dependencies, LeftDepStage,
// RightDepStage). Distribution labels stay valid: E's output distribution
// was by definition identical to D's. Partition alignment holds because a
// hash-join task p consumes input partition p and emits only rows whose
// keys hash to p — its output IS partition p.
//
// Kill switch: WADJET_EXCHANGE_ELIDE=0 (mirrors WADJET_SCAN_COL_SANITIZE).
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
		for k := range s.UnionArms {
			if repl, ok := elided[s.UnionArms[k].DepStage]; ok {
				s.UnionArms[k].DepStage = repl
			}
		}
		out = append(out, s)
	}
	return out
}
