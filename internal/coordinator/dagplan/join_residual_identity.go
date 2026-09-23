// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"
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
//
// A RESPELLING THAT MERGES TWO LEAVES IS REFUSED, NOT EMITTED. The side of a
// leaf is decided by which arm MOVES its name, so two leaves that were
// different columns in the logical residual can come out as ONE stage column:
// a decorrelated `total < total` (outer `o.total`, inner `b.total` over a body
// that renames `amt AS total`, through any number of pass-through layers)
// became `b.amt < b.amt`, and an enclosing `total AS amt` turned `amt < amt`
// into `total < total` on the probe — false for every pair, so EXISTS answered
// no rows and NOT EXISTS every row on the DAG arms (arc DC rounds 4–5, the
// Codex reviews' B2/B1). The respelled text cannot say which leaf was which, so
// the plan is refused with ErrResidualSidesMergedDistributed and the
// coordinator runs it on the single-process pipeline, which binds the two
// leaves by their own arms and answers PostgreSQL's rows. Keeping each leaf's
// side through the respell is the real repair (filed, `distributed`).
func (p *StagePlanner) residualWithStageSpellings(node *logical.Node, buildAlias, filter string) (string, error) {
	if filter == "" || node == nil || len(node.Children) != 2 {
		return filter, nil
	}
	expr := p.PlanContext.ParseJoinCondExpr(filter)
	if expr == nil {
		return filter, nil
	}
	refs, err := plansql.ColumnRefs(expr)
	if err != nil {
		// A subquery, a window function or a node this walk does not know.
		// The compile check beside this one refuses those by name; re-spelling
		// what cannot be enumerated is not this function's business.
		return filter, nil
	}
	type leaf struct {
		orig, final string
		bareOrig    bool
		side        int // 0 probe, 1 build, -1 not decided (neither arm moved it)
		moved       bool
	}
	leaves := make([]leaf, 0, len(refs))
	changed := false
	for _, r := range refs {
		spelled := r.Column
		if r.Table != "" {
			spelled = r.Table + "." + r.Column
		}
		lf := leaf{orig: strings.ToLower(spelled), bareOrig: r.Table == "", side: -1}
		respelled, fromBuild, ok := p.respellResidualRef(spelled, node)
		if ok {
			if qual, bare, isIdent := plansql.SplitIdentRef(respelled); isIdent {
				if qual == "" && fromBuild && buildAlias != "" {
					qual = buildAlias
				}
				r.Table, r.Column = qual, bare
				changed = true
				lf.moved = true
				lf.side = 0
				if fromBuild {
					lf.side = 1
				}
			}
		}
		final := r.Column
		if r.Table != "" {
			final = r.Table + "." + r.Column
		}
		lf.final = strings.ToLower(final)
		leaves = append(leaves, lf)
	}
	for i := range leaves {
		for j := i + 1; j < len(leaves); j++ {
			a, b := leaves[i], leaves[j]
			if a.final != b.final {
				continue
			}
			merged := false
			switch {
			case a.orig == b.orig && a.bareOrig && (a.moved || b.moved):
				// One bare text, moved to one arm: a residual never compares
				// a column with itself on purpose, and the bare text is the
				// only record that the two leaves were two sides.
				merged = true
			case a.orig != b.orig && (a.moved || b.moved):
				// Two different references landed on one stage column: the
				// enclosing `total AS amt` beside a body `amt`, or a leaf the
				// respelling could not place beside one it moved.
				merged = true
			}
			if merged {
				return filter, fmt.Errorf("%w: the join residual %q re-spells %q and %q to the "+
					"one stage column %q, so the two sides it compares would be read from the "+
					"same arm", ErrResidualSidesMergedDistributed, filter, a.orig, b.orig, a.final)
			}
		}
	}
	if !changed {
		return filter, nil
	}
	return expr.String(), nil
}

// ErrResidualSidesMergedDistributed hands a plan whose join residual the stage
// re-spelling would collapse (two leaves, one stage column) to the coordinator's
// single-process pipeline. See residualWithStageSpellings.
var ErrResidualSidesMergedDistributed = errors.New(
	"a join residual's two sides re-spell to one stage column")

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
