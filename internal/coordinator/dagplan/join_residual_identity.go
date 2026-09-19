// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// residualWithStageSpellings re-spells an outer join's ON residual into the
// names the STAGE publishes, so the residual crosses the stage boundary with
// its identity (docs/design/window-key-ownership.md).
//
// A residual is evaluated AT the join and is the only part of an ON clause
// that travels to the worker as TEXT. The join's equi-KEYS already make that
// trip re-spelled (`resolveShuffleKey`); the residual's leaves did not, and a
// Project emits no stage of its own, so a residual over two RENAMING derived
// arms reached a fragment whose sides publish the source names. Nothing
// resolved, the evaluator's unbound slot is SQL NULL, the residual was UNKNOWN
// for every candidate pair, and a LEFT join answered its whole probe side
// NULL-padded — in silence, on a shape the base refused loudly.
//
// THE SIDE IS PART OF THE IDENTITY, not just the name: two arms of one join
// routinely re-spell to the same source column, and a bare name in the
// residual binds PROBE-first. A build-side reference is therefore re-spelled
// QUALIFIED BY THE STAGE'S OWN BUILD ALIAS (`spec.BuildAlias`, the side the
// evaluator forces); a probe-side one is left bare. A reference neither arm
// re-spells is left exactly as written, so a residual over two base tables is
// byte-identical to what it was.
func (p *StagePlanner) residualWithStageSpellings(node *logical.Node, buildAlias, filter string) string {
	if filter == "" || node == nil || len(node.Children) != 2 {
		return filter
	}
	expr := p.PlanContext.ParseJoinCondExpr(filter)
	if expr == nil {
		return filter
	}
	refs, err := plansql.ColumnRefs(expr)
	if err != nil {
		// A subquery, a window function or a node this walk does not know.
		// The compile check beside this one refuses those by name; re-spelling
		// what cannot be enumerated is not this function's business.
		return filter
	}
	changed := false
	for _, r := range refs {
		spelled := r.Column
		if r.Table != "" {
			spelled = r.Table + "." + r.Column
		}
		respelled, fromBuild, ok := p.respellResidualRef(spelled, node)
		if !ok {
			continue
		}
		qual, bare, isIdent := plansql.SplitIdentRef(respelled)
		if !isIdent {
			continue
		}
		if qual == "" && fromBuild && buildAlias != "" {
			qual = buildAlias
		}
		r.Table, r.Column = qual, bare
		changed = true
	}
	if !changed {
		return filter
	}
	return expr.String()
}

// respellResidualRef answers the name ONE reference carries on the stage, and
// which side of the join it belongs to.
//
// It asks each arm the question `resolveShuffleKey` answers for a key: walk
// down the arm and report the spelling the producing stream carries. An arm
// that does not re-spell the reference answers it unchanged, which is how the
// side is decided — the arm that changes it is the arm that owns it. When
// neither arm changes it there is nothing to do, and `ok` is false rather than
// a guess: a reference both arms could answer to is one this rewrite must not
// move, because moving it would be the positional binding that reads the
// other side's column.
func (p *StagePlanner) respellResidualRef(ref string, node *logical.Node) (string, bool, bool) {
	probe := resolveShuffleKey(ref, node.Children[0], p.publishedBlocks)
	build := resolveShuffleKey(ref, node.Children[1], p.publishedBlocks)
	probeMoved := !strings.EqualFold(probe, ref)
	buildMoved := !strings.EqualFold(build, ref)
	switch {
	case probeMoved && !buildMoved:
		return probe, false, true
	case buildMoved && !probeMoved:
		return build, true, true
	}
	return ref, false, false
}
