// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"errors"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// residualWithStageSpellings translates ON residual leaves to stage columns,
// qualifying a moved build-side leaf with the stage build alias and leaving
// unmoved references intact. If translation merges distinct leaves, it
// returns ErrResidualSidesMergedDistributed so the coordinator runs the
// statement on the single-process pipeline. Side identity must survive
// translation. Build-side leaves must be qualified because a bare name
// binds probe-first; see ADR-0021 §1r and docs/design/window-key-ownership.md.
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
