// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"

	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// stageTypeCTEAlias marks a phantom stage that walkStages emits in place of
// a re-computed CTE subtree when ctePlannedTerminal already has a cached
// terminal stage ID. flattenCTEAliases removes these stages from the final
// plan and rewrites every dependency edge that targets an alias to target
// the alias's underlying CTE terminal instead. The alias never reaches
// dispatch — it exists purely to give parent walkStages cases something to
// pick up via leafStages without changing every parent's child-resolution
// logic.
const stageTypeCTEAlias = "cte-alias"

// flattenCTEAliases collapses cte-alias stages: replaces every Dependencies
// reference to an alias with its target, recursing through chains of aliases,
// then drops alias stages from the slice. Idempotent on slices that contain
// no aliases.
func flattenCTEAliases(stages []Stage) []Stage {
	// Build alias → target map. Aliases have exactly one Dependencies entry
	// pointing at the cached CTE terminal (or another alias, in pathological
	// chain cases — recurse to flatten).
	aliasTarget := map[string]string{}
	for _, s := range stages {
		if s.Type == stageTypeCTEAlias && len(s.Dependencies) == 1 {
			aliasTarget[s.ID] = s.Dependencies[0]
		}
	}
	if len(aliasTarget) == 0 {
		return stages
	}
	// Resolve transitively: follow alias→alias chains until we hit a real
	// stage. Caps at len(aliasTarget) hops to defend against any cycle.
	resolve := func(id string) string {
		for i := 0; i <= len(aliasTarget); i++ {
			next, ok := aliasTarget[id]
			if !ok {
				return id
			}
			id = next
		}
		return id
	}
	// Rewrite every Dependencies / LeftDepStage / RightDepStage / FusedJoin
	// build dep that points at an alias.
	for i := range stages {
		s := &stages[i]
		for j, dep := range s.Dependencies {
			s.Dependencies[j] = resolve(dep)
		}
		if t, ok := aliasTarget[s.LeftDepStage]; ok {
			s.LeftDepStage = resolve(t)
		}
		if t, ok := aliasTarget[s.RightDepStage]; ok {
			s.RightDepStage = resolve(t)
		}
		for j, fj := range s.FusedJoins {
			if t, ok := aliasTarget[fj.BuildDepStage]; ok {
				s.FusedJoins[j].BuildDepStage = resolve(t)
			}
		}
		for ph, prod := range s.ScalarDependencies {
			if t, ok := aliasTarget[prod]; ok {
				s.ScalarDependencies[ph] = resolve(t)
			}
		}
		// A set operation's per-arm producer needs no rewrite of its own: it
		// IS Dependencies[i] (see UnionArm). It used to be a stored copy this
		// loop rewrote separately, and one that rewrote the copy without the
		// list refused a UNION ALL over a twice-referenced CTE outright —
		// `arm 1 names producer "cte-alias-1" but Dependencies[1] is
		// "scan-0"` (#660, #715).
	}
	// Drop alias stages.
	out := make([]Stage, 0, len(stages))
	for _, s := range stages {
		if s.Type == stageTypeCTEAlias {
			continue
		}
		out = append(out, s)
	}
	return out
}

// cteSubtreeHash returns a hex SHA-256 over a structural projection of the
// logical subtree rooted at n. Two CTE clones with the same hash are safe
// to dedupe in walkStages: identical scan tables, identical pushed-down
// predicates, identical projections/aggregates, identical column-pruning
// outputs, identical child shapes. A clone where the optimizer pushed
// different filters or columns has a different hash and is NOT deduped.
//
// The hash is intentionally over the *post-optimization* logical shape —
// we want bit-identical execution paths, not source-text equality.
func cteSubtreeHash(n *logical.Node) string {
	h := sha256.New()
	hashLogicalNode(h, n)
	return hex.EncodeToString(h.Sum(nil))
}

func hashLogicalNode(h io.Writer, n *logical.Node) {
	if n == nil {
		_, _ = io.WriteString(h, "<nil>|")
		return
	}
	// Order matters and so does separator — Sprint with a delimiter so a
	// field containing the same characters as another can't collide. The
	// fields chosen are the ones the physical planner actually reads from
	// when emitting stages; if a future planner change reads a new field
	// during walkStages, add it here.
	//
	// IMPORTANT: RequiredColumns is INTENTIONALLY excluded. The optimizer's
	// column-pruning analysis pushes columns referenced anywhere in the
	// outer query into the CTE's inner scan rc list — including columns
	// that don't even belong to the CTE's tables (e.g., supplier_no
	// projected by the Project ABOVE the CTE body, or s_suppkey from the
	// JOIN's other side). Two clones of the same CTE will therefore
	// disagree on RequiredColumns even though they compute byte-identical
	// data; downstream scan code already over-approximates and prunes to
	// real schema columns at execution time. Hashing RC would defeat the
	// dedup whenever a CTE is consumed by two consumers with different
	// outer column needs (i.e., always).
	fmt.Fprintf(h, "T:%v|TBL:%s|PF:%v|SP:%v|", n.Type, n.TableName, n.PartitionFilter, n.ScanPredicates)
	fmt.Fprintf(h, "Pred:%v|Proj:%v|", n.Predicates, n.Projections)
	fmt.Fprintf(h, "GB:%v|GBE:%v|Agg:%v|", n.GroupBy, n.GroupByExprs, n.AggExprs)
	fmt.Fprintf(h, "OB:%v|Lim:%d|Off:%d|", n.OrderBy, n.LimitVal, n.OffsetVal)
	fmt.Fprintf(h, "JT:%s|JC:%s|JF:%s|LK:%v|RK:%v|", n.JoinType, n.JoinCond, n.JoinFilter, n.LeftKeys, n.RightKeys)
	fmt.Fprintf(h, "Win:%v|UA:%v|", n.WindowExprs, n.UnionAll)
	// Don't fold n.CTEName into the hash — two clones of the same CTE
	// SHARE that name, that's the whole point. The cache key in walkStages
	// already uses CTEName as a separate dimension.
	_, _ = io.WriteString(h, "C:[")
	for i, c := range n.Children {
		if i > 0 {
			_, _ = io.WriteString(h, ",")
		}
		hashLogicalNode(h, c)
	}
	_, _ = io.WriteString(h, "]|")
}
