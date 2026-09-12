// This file holds per-block recursive CTE materialization, governed by ADR-0021.
package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// buildNestedRecursiveCTE serves a RECURSIVE CTE reference whose materialized
// result the cache does not hold, by materializing it where THIS block is
// planned. handled is false for a CTE-tagged node that is not such a reference
// — an INLINED non-recursive CTE subtree carries the same tag and is built by
// the caller as the subtree it is.
//
// The definition comes off the reference (logical.Node.RecursiveCTE), which is
// what makes this per-BLOCK: `materializeCTEs` fills the cache from the
// statement root's CTE list, and a recursive CTE declared inside a derived
// table, inside another CTE's body or inside a LATERAL is in no such list —
// the lookup missed, and the tagged scan fell through to a scan of a relation
// that does not exist, which answers ZERO ROWS instead of failing (#1047).
//
// A reference this cannot serve is REFUSED. The name belongs to the CTE and to
// nothing else in scope, so reading it as a table is reading a relation that
// does not exist; the old fall-through is the same "a missing cache entry is
// an empty CTE" door #1041 names, one level up, and an empty answer is the one
// thing a caller cannot tell from a right one.
func (p *Planner) buildNestedRecursiveCTE(ctx context.Context, node *logical.Node) (
	exec.Source, []exec.UnaryOperator, exec.Sink, error, bool) {

	if node.Type != logical.NodeScan || node.RecursiveCTE == nil {
		return nil, nil, nil, nil, false
	}
	def := node.RecursiveCTE
	if mat, ok := p.nestedCTECache[def]; ok {
		return nestedCTESource(mat), nil, &exec.CollectSink{}, nil, true
	}
	if p.cteCache == nil {
		p.cteCache = make(map[string]*cteMaterialized)
	}
	// A RECURSIVE CTE's own name IS in scope inside its body — that is what
	// makes it recursive — and the fixed-point iteration re-plans the
	// recursive term as a whole statement (materializeRecursiveCTE →
	// executeSubquery), which resolves names against p.ctes. At the root that
	// list already holds the definition; inside a nested block it holds the
	// ENCLOSING scope's, so the self-reference resolved as a TABLE, read
	// nothing, and the iteration stopped after the anchor — one row where
	// PostgreSQL answers three.
	//
	// APPENDED and not substituted: the body may name an enclosing CTE too,
	// and PostgreSQL has both in scope there.
	// A materialization this statement already attempted and that FAILED is
	// answered with its own error, not attempted again: the root pass runs
	// before any reference is built, and retrying would hide why.
	name := strings.ToLower(strings.TrimSpace(node.CTEName))
	if err, failed := p.cteMaterializeErr[name]; failed {
		return nil, nil, nil, err, true
	}
	savedCTEs := p.ctes
	p.ctes = append(append([]plansql.CTEDef(nil), p.ctes...), *def)
	// The NAME binding is what the iteration seeds and reads for the
	// self-reference, so the materialization has to own it — and hand it back
	// afterwards, because the name may belong to a different relation in the
	// enclosing scope.
	savedEntry, hadEntry := p.cteCache[node.CTEName]
	materr := p.materializeRecursiveCTE(ctx, *def)
	mat, ok := p.cteCache[node.CTEName]
	if hadEntry {
		p.cteCache[node.CTEName] = savedEntry
	} else {
		delete(p.cteCache, node.CTEName)
	}
	p.ctes = savedCTEs
	if materr != nil {
		if p.cteMaterializeErr == nil {
			p.cteMaterializeErr = map[string]error{}
		}
		p.cteMaterializeErr[name] = materr
		return nil, nil, nil, materr, true
	}
	if !ok {
		// The INVARIANT's guard, not a path any SQL reaches: every failure
		// materializeRecursiveCTE knows about now returns an error, which the
		// branch above surfaces, and the six refusal cells of
		// TestC1DARecursiveCTEFormIsDecidedBeforeTheBodyIsPlanned take exactly
		// that route. If the contract is ever broken, the answer is still a
		// refusal rather than a scan of a relation that does not exist.
		return nil, nil, nil, sqlerr.New("0A000",
			"the recursive CTE %q could not be materialized, and there is no relation "+
				"of that name to read instead", node.CTEName), true
	}
	if p.nestedCTECache == nil {
		p.nestedCTECache = make(map[*plansql.CTEDef]*cteMaterialized)
	}
	p.nestedCTECache[def] = mat
	return nestedCTESource(mat), nil, &exec.CollectSink{}, nil, true
}

// nestedCTESource replays a materialized CTE, from the spill-backed collector
// where there is one and from the boxed rows otherwise — the same two forms
// buildPipeline serves a root CTE from.
func nestedCTESource(mat *cteMaterialized) exec.Source {
	if mat.coll != nil {
		return mat.coll.NewReplaySource()
	}
	return exec.NewSliceSource(mat.schema, mat.rows)
}
