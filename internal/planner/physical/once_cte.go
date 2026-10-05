// SPDX-License-Identifier: MIT

// This file holds the once-per-statement evaluation of a volatile CTE, governed by ADR-0021.
package physical

import (
	"context"
	"sync"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// A VOLATILE CTE READ MORE THAN ONCE IS EVALUATED ONCE PER STATEMENT, AND
// EVERY REFERENCE READS THAT ONE EVALUATION, ON DEMAND (#1531, ADR-0021 §2d).
//
// PostgreSQL materializes a CTE referenced more than once into ONE tuplestore
// that its CTE Scans fill on demand, so in
// `WITH s AS (SELECT sum(random()) AS r FROM t) … (SELECT r FROM s) <> (SELECT r FROM s)`
// both references read the same r and the comparison is false. This engine
// inlines a CTE's body at every reference, and `materializeCTEs` caches a ROOT
// CTE only when the logical tree tags it — so a CTE read from an expression
// subquery's TEXT, a CTE declared on a nested block, and a CTE read by the
// arms of a set operation at the statement root were each evaluated once PER
// REFERENCE: two draws of random(), two samples of a TABLESAMPLE.
//
// The rule is one decision at one place: a reference whose definition is
// volatile AND read more than once in the statement (logical.Node.OnceCTE,
// set by the builder from plansql.CTEDef.EvaluatedOnce) reads a SHARED SPOOL
// keyed by the definition's IDENTITY — shared by every copy of the WITH item,
// whichever scope carried it there. The first reference to be BUILT builds
// the body's pipeline; the body RUNS only when some reader first asks for a
// batch, and advances one batch at a time only when a reader asks for one the
// spool does not hold yet (exec.SharedSpool). A reader that stops early
// (LIMIT, EXISTS) therefore never forces rows nobody reads, nor an error on
// one. A CTE read ONCE, and a deterministic body, are untouched: they are
// planned exactly as before.
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
	built chan struct{} // closed when spool/err are final
	spool *exec.SharedSpool
	err   error
}

// serveOnceCTE answers a reference to a volatile CTE read more than once with
// a reader of the statement's one spool, building the spool's body if this is
// the first reference built. handled is false when the reference is not one
// this rule serves.
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
		e = &onceCTEEntry{built: make(chan struct{})}
		cache.entries[id] = e
	}
	cache.mu.Unlock()
	if !ok {
		// Built outside the lock: the body may read another such CTE, whose
		// reference comes back here.
		source, ops, err := p.buildOnceCTEBody(ctx, def, node.OnceCTEScope)
		if err == nil {
			spool := &exec.SharedSpool{Spill: p.getSpillManager(), Source: source, Ops: ops}
			// A result that is only ever replayed in order has no merge to
			// keep cheap: drain in runs of a quarter of the budget (the
			// recursive closure's reason, recursive_cte_iteration.go).
			if sm := spool.Spill; sm != nil {
				spool.RunBytes = sm.SpillBudget() / 4
			}
			e.spool = spool
		}
		e.err = err
		close(e.built)
	} else {
		// Another reference — possibly on another goroutine, through a
		// subquery planner — is building it: wait for that one body.
		select {
		case <-e.built:
		case <-ctx.Done():
			return nil, true, ctx.Err()
		}
	}
	if e.err != nil {
		return nil, true, e.err
	}
	return e.spool.NewReader(), true, nil
}

// buildOnceCTEBody plans a volatile CTE's body — planned, not run — with the
// WITH items in scope inside the body (the ones before it).
func (p *Planner) buildOnceCTEBody(ctx context.Context, def *plansql.CTEDef, scope []plansql.CTEDef) (exec.Source, []exec.UnaryOperator, error) {
	// The body is a CTE's, not a scalar subquery's answer, whichever planner
	// reached it (ExecuteSubquerySchema marks its own body only).
	saved := p.scalarBody
	p.scalarBody = false
	defer func() { p.scalarBody = saved }()
	var source exec.Source
	var ops []exec.UnaryOperator
	var err error
	if body, _ := def.BodySelect(); body != nil {
		source, ops, _, err = p.buildSubqueryPipelineScopedFor(ctx, body, scope)
	} else {
		source, ops, _, err = p.buildSubqueryPipelineScoped(ctx, def.SQL, scope)
	}
	if err != nil {
		return nil, nil, err
	}
	return source, ops, nil
}

// release ends every spool of the statement: a body still running is
// cancelled, and the runs and the tracker charge are freed. Idempotent.
func (c *onceCTECache) release() {
	if c == nil {
		return
	}
	c.mu.Lock()
	entries := c.entries
	c.entries = nil
	c.mu.Unlock()
	for _, e := range entries {
		select {
		case <-e.built:
			if e.spool != nil {
				e.spool.Close()
			}
		default:
			// Still being built: Plan's cleanup runs after the pipeline, so a
			// pending entry belongs to a goroutine that failed out mid-build.
		}
	}
}

// spools lists the statement's spools (gates).
func (c *onceCTECache) spools() []*exec.SharedSpool {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*exec.SharedSpool
	for _, e := range c.entries {
		if e.spool != nil {
			out = append(out, e.spool)
		}
	}
	return out
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
