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
// A residual is evaluated AT the join, over the combined probe/build row, and
// it is the only part of an ON clause that travels to the worker as TEXT. The
// join's equi-KEYS already make this trip re-spelled — `resolveShuffleKey`
// walks each key down the arm it belongs to and answers the name the producing
// stream really carries — but the residual's leaves did not, and a Project
// emits no stage of its own, so
//
//	FROM (SELECT id AS a, k AS kk, s AS ss FROM jr_l) x
//	LEFT JOIN (SELECT id AS b, k AS kk2, s AS ss2 FROM jr_r) y
//	  ON x.kk = y.kk2 AND LOWER(y.ss2) = x.ss
//
// reached a fragment whose two sides publish `[id k s]` with a residual
// spelling `y.ss2` and `x.ss`. Neither resolved, the evaluator's unbound slot
// is SQL NULL, the residual was UNKNOWN for every candidate pair, and the LEFT
// join answered its whole probe side NULL-padded — six rows where PostgreSQL
// answers seven, in silence, on a shape the base refused loudly.
//
// THE SIDE IS PART OF THE IDENTITY, not just the name. Both references above
// re-spell to `s`, because both arms select the same source column under
// different aliases, and a bare `s` in the residual binds PROBE-first — so
// re-spelling by NAME alone would bind the build's reference to the probe's
// column, which is the wrong value one operator over. A build-side reference is
// therefore re-spelled QUALIFIED BY THE STAGE'S OWN BUILD ALIAS, the one the
// evaluator forces to the build side (`buildAlias`, and the same string the
// fragment carries as `spec.BuildAlias`); a probe-side one is left bare.
//
// A reference neither arm re-spells is left exactly as written: the walk
// answers the input unchanged when it finds nothing to translate, and a
// residual over two base tables — every cell of the arc's own corpus — is
// therefore byte-identical to what it was.
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
