// SPDX-License-Identifier: MIT

// This file holds the once-per-statement evaluation of a volatile CTE, governed by ADR-0021.
package physical

import (
	"context"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// A VOLATILE CTE IS EVALUATED ONCE PER STATEMENT, AND EVERY REFERENCE READS
// THAT ONE RESULT (#1531, ADR-0021 §2d).
//
// PostgreSQL never inlines a CTE whose body contains a volatile function, and
// it materializes any CTE referenced more than once, so in
// `WITH s AS (SELECT sum(random()) AS r FROM t) … (SELECT r FROM s) <> (SELECT r FROM s)`
// both references read the same r and the comparison is false. This engine
// inlines a CTE's body at every reference, and `materializeCTEs` caches a ROOT
// CTE only when the logical tree tags it — so a CTE read only from an
// expression subquery's TEXT, a CTE declared on a nested block, and a CTE read
// by the arms of a set operation at the statement root (which carries no WITH
// list) were each evaluated once PER REFERENCE: two draws of random(), two
// samples of a TABLESAMPLE, two uuid() columns.
//
// The rule is one decision at one place: a reference whose definition is
// volatile (logical.Node.OnceCTE, set by the builder from
// plansql.CTEDef.EvaluatedOnce) is served from a cache keyed by the
// definition's IDENTITY — shared by every copy of the WITH item, whichever
// scope carried it there — and the first reference to arrive evaluates the
// body into a spill-backed collector that every reference replays. A
// deterministic body is untouched: it is inlined (and pushed into) exactly as
// before.
//
// The cache exists only on a statement planned by Plan — the single-process
// pipeline, its subquery planners (forSubquery shares the pointer), the
// coordinator's local fast path and its refused-plan route. The stage DAG's
// planner never calls Plan, so the distributed path is unchanged: there a
// volatile CTE read more than once is still evaluated per consumer
// (docs/adr/0012-divergences/other.md).
type onceCTECache struct {
	mu      sync.Mutex
	entries map[*plansql.CTEIdentity]*onceCTEEntry
}

type onceCTEEntry struct {
	done chan struct{} // closed when mat/err are final
	mat  *cteMaterialized
	err  error
}

// serveOnceCTE answers a reference to a volatile CTE from its one
// evaluation, evaluating it if this is the first reference to arrive.
// handled is false when the reference is not one this rule serves.
func (p *Planner) serveOnceCTE(ctx context.Context, node *logical.Node) (exec.Source, bool, error) {
	cache := p.onceCTEs
	def := node.OnceCTE
	if cache == nil || def == nil || def.Identity() == nil {
		return nil, false, nil
	}
	id := def.Identity()
	cache.mu.Lock()
	if cache.entries == nil {
		cache.entries = map[*plansql.CTEIdentity]*onceCTEEntry{}
	}
	e, ok := cache.entries[id]
	if !ok {
		e = &onceCTEEntry{done: make(chan struct{})}
		cache.entries[id] = e
	}
	cache.mu.Unlock()
	if !ok {
		e.mat, e.err = p.evaluateOnceCTE(ctx, def, node.OnceCTEScope)
		close(e.done)
	} else {
		// Another reference — possibly on another pipeline goroutine, through
		// a subquery planner — is evaluating it: wait for that one result.
		select {
		case <-e.done:
		case <-ctx.Done():
			return nil, true, ctx.Err()
		}
	}
	if e.err != nil {
		return nil, true, e.err
	}
	return nestedCTESource(e.mat), true, nil
}

// evaluateOnceCTE runs a volatile CTE's body into a spill-backed collector:
// the same columnar materialization `materializeCTEs` gives a root CTE,
// planned with the WITH items in scope inside the body (the ones before it).
func (p *Planner) evaluateOnceCTE(ctx context.Context, def *plansql.CTEDef, scope []plansql.CTEDef) (*cteMaterialized, error) {
	// The body is a CTE's, not a scalar subquery's answer, whichever planner
	// reached it (ExecuteSubquerySchema marks its own body only).
	saved := p.scalarBody
	p.scalarBody = false
	defer func() { p.scalarBody = saved }()
	body, _ := def.BodySelect()
	coll, schema, err := p.materializeCTEColumnarRuns(ctx, def.SQL, body, scope, true)
	if err != nil {
		return nil, err
	}
	if schema == nil {
		coll.Release()
		return nil, sqlerr.New("0A000",
			"the WITH query %q produced no column list, so there is nothing to read it as", def.Name)
	}
	return &cteMaterialized{schema: schema, coll: coll}, nil
}

// releaseOnceCTEs frees every collector the statement's volatile CTEs were
// evaluated into (tracker charge + spill scratch). Idempotent.
func (c *onceCTECache) release() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, e := range c.entries {
		select {
		case <-e.done:
			if e.mat != nil && e.mat.coll != nil {
				e.mat.coll.Release()
			}
		default:
			// Still evaluating: Plan's cleanup runs after the pipeline, so a
			// pending entry belongs to a goroutine that failed out; its
			// collector is released by its own error path.
		}
	}
	c.entries = nil
}

// onceCTENames lists, in plan order, the volatile WITH items declared by the
// blocks of a plan tree (each block root's WITH list) and named by its
// references — what EXPLAIN VERBOSE reports as evaluated once. A WITH inside
// an expression subquery's text is planned when that subquery runs and is not
// listed.
func onceCTENames(root *logical.Node) []string {
	var out []string
	seen := map[*plansql.CTEIdentity]bool{}
	add := func(def *plansql.CTEDef) {
		if def == nil || !def.EvaluatedOnce() || seen[def.Identity()] {
			return
		}
		seen[def.Identity()] = true
		out = append(out, def.Name)
	}
	var walk func(*logical.Node)
	walk = func(n *logical.Node) {
		if n == nil {
			return
		}
		for i := range n.CTEs {
			add(&n.CTEs[i])
		}
		add(n.OnceCTE)
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(root)
	return out
}
