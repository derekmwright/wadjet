// This file holds planner entry for the physical planner, governed by ADR-0034.
package physical

import (
	"context"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
)

// mergeDuplicateScans detects tables scanned multiple times in a query
// (e.g., from decorrelated subqueries) and merges their required columns.
// When duplicates are found, a scanCache entry is created so the first scan
// caches its results and subsequent scans replay from memory.
func (p *Planner) mergeDuplicateScans(node *logical.Node) {
	// Collect all scan nodes grouped by table name.
	scansByTable := map[string][]*logical.Node{}
	var walkScans func(n *logical.Node)
	walkScans = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.Type == logical.NodeScan && n.TableName != "" {
			scansByTable[n.TableName] = append(scansByTable[n.TableName], n)
		}
		for _, c := range n.Children {
			walkScans(c)
		}
	}
	walkScans(node)

	// For tables scanned more than once, merge RequiredColumns across all scans.
	for table, scans := range scansByTable {
		if len(scans) < 2 {
			continue
		}
		// Skip if any scan has predicates or partition filters
		// (different filters = different result sets, not cacheable)
		incompatible := false
		for _, s := range scans {
			if len(s.ScanPredicates) > 0 || len(s.PartitionFilter) > 0 {
				incompatible = true
				break
			}
		}
		if incompatible {
			continue
		}
		// Compute the union of required columns in first-seen order. The
		// union lives on the CACHE entry only — each scan node keeps its
		// own RequiredColumns and catalogScanSource projects the cached
		// (union-wide) batches back down per consumer. Rewriting the scan
		// nodes to the union, as this used to do, silently widened every
		// consumer: hash-join build sides stored the union's columns and
		// spilled them on eviction (Q21's semi/anti lineitem builds).
		// A scan with no RequiredColumns needs every column: the union
		// degrades to nil (full schema).
		var merged []string
		colSet := map[string]bool{}
		needAll := false
		for _, s := range scans {
			if len(s.RequiredColumns) == 0 {
				needAll = true
				break
			}
			for _, col := range s.RequiredColumns {
				if !colSet[col] {
					colSet[col] = true
					merged = append(merged, col)
				}
			}
		}
		if needAll {
			merged = nil
		}
		// Initialize the scan cache entry.
		if p.scanCache == nil {
			p.scanCache = make(map[string]*scanCached)
		}
		p.scanCache[table] = &scanCached{unionCols: merged}
	}
}

// Plan converts a logical plan to a physical plan for local execution.
func (p *Planner) Plan(ctx context.Context, node *logical.Node) (*PhysicalPlan, error) {
	p.planCtx = ctx           // store for subquery runner context propagation
	p.releaseScanCache()      // reset per-query scan cache (drops tracker reservation)
	p.res = &queryResources{} // reset per-query spill manager + memory tracker
	p.releaseCTECache()       // reset per-query CTE cache (frees stale spill scratch)
	// Propagate CTE definitions from the logical plan so scalar subqueries
	// (e.g., in WHERE/HAVING) can resolve CTE table references.
	if len(node.CTEs) > 0 {
		p.ctes = node.CTEs
	}

	// A star that could not be expanded, refused with the planner's own
	// sentence BEFORE the ordinal one — the order PlanDistributed uses, so
	// both engines say the same thing about `SELECT s.* … ORDER BY 1`: the
	// star is the reason and the un-countable ordinal is its consequence.
	if err := refuseUnexpandedStarAnywhere(node); err != nil {
		return nil, err
	}
	// A `SELECT * ... ORDER BY <n>` whose star never expanded (#810). Refused
	// here rather than in buildSort so this path and PlanDistributed say the
	// same thing about the same query.
	if err := logical.RefuseUnresolvedOrdinalSortKeys(node); err != nil {
		return nil, err
	}
	// …and a COLUMN-ALIAS LIST longer than the `SELECT *` body it renames, for
	// the same reason and at the same place: the width is a pass later than
	// the builder, so PostgreSQL's 42P10 is raised a pass later too (#958,
	// column_alias_defer.go).
	if err := logical.RefuseUnappliedColumnAliasLists(node); err != nil {
		return nil, err
	}

	// The projection whose names the CLIENT reads, resolved once (#732).
	p.outputProjection = findOutputProjectionNode(node)

	// Materialize CTEs referenced multiple times. Each CTE is computed once
	// and cached so that all references (main query + subqueries) see the
	// exact same data. This prevents float64 accumulation-order divergence
	// that would break exact equality comparisons (e.g., TPC-H Q15).
	p.materializeCTEs(ctx, node)

	// Detect tables scanned multiple times and merge their column needs.
	// The first scan caches decoded batches; subsequent scans replay from cache.
	p.mergeDuplicateScans(node)

	// The same plan-time refusal PlanDistributed makes, so the single-process
	// engine and the small-query fast path raise it too — and raise it for a
	// predicate no row ever reaches, which the operator-level check cannot
	// (#631 follow-up).
	if err := refuseUnrepresentableRealInList(node); err != nil {
		p.resources().releaseSubqueryCharges()
		p.releaseCTECache()
		p.releaseScanCache()
		return nil, err
	}

	source, ops, sink, err := p.buildPipeline(ctx, node)
	if err != nil {
		p.resources().releaseSubqueryCharges()
		p.releaseCTECache()  // free CTE spill scratch on the no-Cleanup path
		p.releaseScanCache() // drop the scan cache's tracker reservation
		return nil, err
	}

	// Drop the columns the logical builder materialized for its own use so the
	// client sees exactly the columns it selected (#320).
	if trim := hiddenSortTrimOp(node); trim != nil {
		ops = append(ops, trim)
	}

	// Front-load bloom filters whose key columns exist in the source scan.
	// In multi-way joins with selective semi/anti-join bloom filters (e.g.,
	// Q18's HAVING filter), this eliminates most rows at the source before
	// expensive join probes run.
	ops = frontLoadBlooms(source, ops)

	// Enable parallel pipeline execution when the source supports
	// concurrent Next() calls (channel-based scan sources).
	pipelineWorkers := 0
	switch source.(type) {
	case *catalogScanSource, *scannerExecSource, *deferredJoinBridge:
		pipelineWorkers = scanParallelism()
	}

	plan := &PhysicalPlan{
		Pipeline: &exec.Pipeline{
			Source:  source,
			Ops:     ops,
			Sink:    sink,
			Workers: pipelineWorkers,
		},
		OutputSchema: declaredOutputSchema(node, p.subqueryOutputColumn),
	}
	// Hand the sink the plan's answer for the case where no batch will ever
	// tell it: a zero-row result. It is consulted only then (#416).
	if cs, ok := sink.(*exec.CollectSink); ok {
		cs.SchemaHint = plan.OutputSchema
		// The names the CLIENT is owed, positionally: PostgreSQL's
		// FigureColname for every unaliased item (#732). Applied at the sink
		// rather than inside the plan, because inside the plan a name is also
		// a HANDLE — a sort key, a HAVING reference, an aggregate's OutputCol
		// — and the two are not the same string.
		cs.OutputNames = publishedOutputNames(p.outputProjection)
		// Unlike SchemaHint, this is consulted on EVERY result, zero-row or
		// not: which DECIMAL columns are aggregate output is a property of
		// the PLAN, not of whether a batch arrived (FIX 2, #457/#458 fold-in).
		//
		// Both maps are keyed by the name the CLIENT reads, which is the
		// PUBLISHED one (#732): filed under the resolution spelling they miss,
		// and an unaliased `s_acctbal + 1` goes out with a DECIMAL typmod
		// PostgreSQL sends -1 for.
		rawWire, rawLens := declaredWireUnconstrainedDecimal(node), DeclaredStringLengths(node)
		// POSITIONAL first, and it is the authority: a name is not an address
		// when two output columns publish one (#732, round-1 review B2).
		cs.SchemaHintWireUnconstrainedPos, cs.SchemaHintStringLengthPos =
			publishedOutputDecls(p.outputProjection, rawWire, rawLens)
		cs.SchemaHintWireUnconstrainedDecimal = republishDeclaredNames(p.outputProjection, rawWire)
		// And the string family's modifier, which is a LENGTH rather than a
		// (p,s) — same lifecycle, same reason (#838).
		cs.SchemaHintStringLength = republishDeclaredNames(p.outputProjection, rawLens)
	}

	// Attach spill file cleanup. CTE collectors and the scan cache
	// release first (tracker charge + their scratch files) so the
	// SpillManager sweep that follows never races their removal. The
	// scan cache release matters most on the shared-tracker path: its
	// reservation would otherwise outlive the query as a permanent
	// phantom on the worker-lifetime tracker.
	res := p.resources()
	if sm := p.spillManagerIfSet(); sm != nil {
		plan.Cleanup = func() {
			p.closeBuiltJoins()
			res.releaseSubqueryCharges()
			p.releaseCTECache()
			p.releaseScanCache()
			sm.Cleanup()
		}
	} else if p.cteCacheHasCollectors() || p.scanCache != nil || len(p.builtJoins) > 0 || res.hasSubqueryCharges() {
		// Shared (worker-injected) spill manager: its dir outlives this
		// query, so the collectors' scratch must be released explicitly.
		plan.Cleanup = func() {
			p.closeBuiltJoins()
			res.releaseSubqueryCharges()
			p.releaseCTECache()
			p.releaseScanCache()
		}
	}

	// Generate distributed stages for coordinator dispatch
	plan.Stages = p.generateStages(node)

	if err := p.enforceQueryLimits(ctx, plan.Stages, node); err != nil {
		p.resources().releaseSubqueryCharges()
		p.releaseCTECache()
		p.releaseScanCache()
		return nil, err
	}

	return plan, nil
}
