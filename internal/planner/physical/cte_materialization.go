// SPDX-License-Identifier: MIT

// This file holds cte materialization for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// materializeCTEs pre-computes any CTE that is referenced more than once in
// the plan tree (including inside scalar subqueries). Both the main pipeline
// and any subquery pipelines will read from the cached result, ensuring they
// see bit-identical data.
func (p *Planner) materializeCTEs(ctx context.Context, root *logical.Node) {
	if len(root.CTEs) == 0 {
		return
	}
	// Count how many times each CTE name appears as a CTEName tag.
	refCounts := map[string]int{}
	var countRefs func(n *logical.Node)
	countRefs = func(n *logical.Node) {
		if n == nil {
			return
		}
		if n.CTEName != "" {
			refCounts[n.CTEName]++
		}
		for _, c := range n.Children {
			countRefs(c)
		}
	}
	countRefs(root)

	// Also count CTE references inside scalar subquery expressions.
	// These are in predicate ASTExpr nodes that reference CTE table names.
	// A simple heuristic: if a CTE name appears in the CTE list AND has
	// at least 1 reference in the plan tree, check if any scalar subquery
	// in the plan text also references it. We conservatively materialize
	// any CTE that has >=1 ref in the plan tree AND appears in the CTE
	// list (since scalar subqueries may reference it too).
	for _, cte := range root.CTEs {
		if refCounts[cte.Name] > 0 {
			// Conservatively mark as multi-ref since scalar subqueries
			// (not visible in the logical tree) may also reference it.
			refCounts[cte.Name] = 2
		}
	}

	p.cteCache = make(map[string]*cteMaterialized)

	for i := range root.CTEs {
		cte := &root.CTEs[i]
		if cte.Recursive {
			// The error is RECORDED, not dropped: this pass runs long before
			// the reference is built, and a reference with no cache entry must
			// refuse rather than read a relation that does not exist.
			if err := p.materializeRecursiveCTE(ctx, *cte); err != nil {
				if p.cteMaterializeErr == nil {
					p.cteMaterializeErr = map[string]error{}
				}
				p.cteMaterializeErr[strings.ToLower(strings.TrimSpace(cte.Name))] = err
			}
			continue
		}
		if refCounts[cte.Name] < 2 {
			continue
		}
		// EARLIER CTEs only — see buildSubqueryPipelineScoped (#771).
		//
		// Materialize columnar into a tracker-charged, spill-backed
		// collector. The previous shape boxed the whole result via
		// CollectSink.ToRows (one map[string]any per row, entirely outside
		// the budget/spill machinery) — `WITH x AS (SELECT * FROM lineitem)`
		// held the full table in coordinator-process heap.
		// The MEMOIZED body, so this materialization plans the tree the
		// binder validated rather than a private re-parse of the same text
		// (#851). BodySelect answers nil on a parse error, and the sql
		// argument keeps the old path's message for that case.
		body, _ := cte.BodySelect()
		coll, schema, err := p.materializeCTEColumnar(ctx, cte.SQL, body, root.CTEs[:i])
		if err != nil {
			continue // fall back to inline expansion
		}
		if schema == nil {
			coll.Release()
			continue
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
	}
}

// materializeCTEColumnar runs a CTE body into a spill-backed collector.
// Returns the collector and the schema the body's own pipeline produced (see
// cteMaterializingSink); the caller owns the collector and must Release it —
// normally via PhysicalPlan.Cleanup through releaseCTECache.
func (p *Planner) materializeCTEColumnar(ctx context.Context, sql string,
	body *plansql.SelectInfo, scope []plansql.CTEDef) (*exec.SpillableBatchCollector, []parquet.Column, error) {
	var source exec.Source
	var ops []exec.UnaryOperator
	var err error
	if body != nil {
		source, ops, _, err = p.buildSubqueryPipelineScopedFor(ctx, body, scope)
	} else {
		source, ops, _, err = p.buildSubqueryPipelineScoped(ctx, sql, scope)
	}
	if err != nil {
		return nil, nil, err
	}
	coll := &exec.SpillableBatchCollector{Spill: p.getSpillManager()}
	sink := &cteMaterializingSink{coll: coll}
	pipeline := &exec.Pipeline{Source: source, Ops: ops, Sink: sink}
	if err := pipeline.Run(ctx); err != nil {
		coll.Release()
		return nil, nil, err
	}
	schema := sink.schema
	if schema == nil {
		// Empty result: no batch ever arrived, so derive column names from
		// the SQL like the boxed path did — downstream projection still
		// needs the names to resolve.
		schema = p.inferCTESchema(sql)
	}
	return coll, schema, nil
}

// closeBuiltJoins closes every HashJoin this plan built: it returns the
// build side's tracker reservation and removes the grace build's partition
// files, which the flush loop only removes when the query runs to
// completion. Idempotent — HashJoin.Close zeroes what it releases.
func (p *Planner) closeBuiltJoins() {
	for _, hj := range p.builtJoins {
		hj.Close()
	}
	p.builtJoins = nil
}

// releaseScanCache drops every duplicate-scan cache entry so cached
// batches don't outlive their query. Idempotent; wired into
// PhysicalPlan.Cleanup, Plan's error paths, and Plan's per-query reset.
func (p *Planner) releaseScanCache() {
	for _, c := range p.scanCache {
		c.mu.Lock()
		c.batches = nil
		c.mu.Unlock()
	}
	p.scanCache = nil
}

// releaseCTECache frees every columnar CTE collector (tracker charge +
// spill scratch). Idempotent; wired into PhysicalPlan.Cleanup and Plan's
// error paths.
func (p *Planner) releaseCTECache() {
	for _, mat := range p.cteCache {
		if mat.coll != nil {
			mat.coll.Release()
		}
	}
	p.cteCache = nil
	// The per-block recursive materializations are a SECOND set of collectors
	// with the same lifetime: a nested entry is moved OUT of cteCache when it
	// is built (nested_recursive_cte.go), so nothing here is released twice.
	for _, mat := range p.nestedCTECache {
		if mat.coll != nil {
			mat.coll.Release()
		}
	}
	p.nestedCTECache = nil
	// Both are per-STATEMENT facts and must not outlive it: a name left marked
	// in progress would refuse the next statement's legal recursion, and a
	// recorded failure would refuse a query that has not been tried.
	p.cteInProgress = nil
	p.cteMaterializeErr = nil
}

// cteCacheHasCollectors reports whether any cached CTE holds spill-backed
// state that requires an explicit release at query end.
func (p *Planner) cteCacheHasCollectors() bool {
	for _, mat := range p.cteCache {
		if mat.coll != nil {
			return true
		}
	}
	for _, mat := range p.nestedCTECache {
		if mat.coll != nil {
			return true
		}
	}
	return false
}

// cteMaterializingSink records the CTE body's own pipeline schema as the CTE
// schema. Never infer a column's type by parsing its values: numeric-looking
// TEXT must remain TEXT, independent of row order or filtering (#727;
// ADR-0026 §2c). Bare columns carry catalog declarations; literals receive
// their types at the projection (SELECT 1 is INT64, #369).
type cteMaterializingSink struct {
	coll   *exec.SpillableBatchCollector
	schema []parquet.Column // the first non-empty batch's schema; nil until then
}

func (s *cteMaterializingSink) Init(ctx context.Context) error { return s.coll.Init(ctx) }

func (s *cteMaterializingSink) Consume(ctx context.Context, b *batch.RecordBatch) error {
	if s.schema == nil && b.ActiveLen() > 0 {
		s.schema = append([]parquet.Column(nil), b.Schema...)
	}
	return s.coll.Consume(ctx, b)
}

func (s *cteMaterializingSink) Finalize(ctx context.Context) error { return s.coll.Finalize(ctx) }

func (s *cteMaterializingSink) Close() error { return s.coll.Close() }

// inferCTESchema derives a CTE's column NAMES from its SQL, for a
// non-recursive materialization whose body produced no batch. The types are
// not known here and are declared text; a recursive CTE never comes this way —
// its schema is its seed's (recursive_cte_iteration.go).
func (p *Planner) inferCTESchema(sql string) []parquet.Column {
	pq, err := plansql.Parse(sql)
	if err != nil {
		return nil
	}
	info, err := plansql.ExtractSelect(pq)
	if err != nil {
		return nil
	}
	// A SET OPERATION publishes its LEFT arm's names, which is PostgreSQL's
	// rule and the one `plansql.BlockOutputColumns` already states. The union
	// node itself carries no SELECT list, so a multi-arm body gave a schema of
	// ZERO columns and the reference answered "the result has no columns"
	// (round-2 review, B3).
	for info.Union != nil && info.Union.Left != nil {
		info = info.Union.Left
	}
	schema := make([]parquet.Column, len(info.Columns))
	for i, col := range info.Columns {
		name := col.Alias
		if name == "" {
			name = col.Expr
		}
		schema[i] = parquet.Column{Name: name, Type: parquet.TypeString, Nullable: true}
	}
	return schema
}

// materializeRecursiveCTE executes a recursive CTE using fixed-point iteration.
// The CTE body must contain UNION ALL separating the anchor query from the
// recursive query. The recursive query references the CTE name itself.
//
// IT MARKS THE NAME IN PROGRESS FOR THE WHOLE MATERIALIZATION, and that marker
// is the termination guarantee. Everything below PLANS the body, and the body's
// self-reference is a tagged scan whose cache lookup misses until the first
// iteration seeds it — so without a marker a spelling this cannot split
// re-materialized the SAME definition from inside its own materialization,
// without bound: `WITH RECURSIVE r AS (SELECT 1 AS v UNION SELECT v+1 FROM r
// WHERE v<3)` never returned and took 25 GB of RSS in 45 seconds, reachable by
// any client at the statement ROOT (the round-2 review's B3). The builder's own
// comment says why the body is not expanded there; this is the same door one
// layer down.
//
// The error is RETURNED rather than swallowed: a reference that cannot be
// served must refuse, and the caller is the only place that knows which
// reference asked.
func (p *Planner) materializeRecursiveCTE(ctx context.Context, cte plansql.CTEDef) error {
	name := strings.ToLower(strings.TrimSpace(cte.Name))
	if p.cteInProgress[name] {
		// A self-reference reached here from INSIDE this CTE's own
		// materialization, in a position the iteration has not seeded the work
		// table for — PostgreSQL's "recursive reference to query %q must not
		// appear within its non-recursive term". Refusing is the whole of the
		// termination guarantee.
		return sqlerr.New("42P19",
			"recursive reference to query %q must not appear within its non-recursive term",
			cte.Name)
	}
	if p.cteInProgress == nil {
		p.cteInProgress = map[string]bool{}
	}
	p.cteInProgress[name] = true
	defer delete(p.cteInProgress, name)

	// THE FORM IS DECIDED FROM THE PARSED SET-OPERATION TREE, which is
	// PostgreSQL's own rule and not a property of the body's text.
	form, anchorSQL, recursiveSQL, err := classifyRecursiveBody(cte)
	if err != nil {
		return err
	}
	if form == recursiveFormNotRecursive {
		// A RECURSIVE CTE's name IS in scope inside its own body, which is
		// what makes it recursive, so this one keeps the whole list.
		coll, schema, err := p.materializeCTEColumnar(ctx, cte.SQL, nil, p.Ctes)
		if err != nil {
			return err
		}
		if schema == nil {
			coll.Release()
			return errNoCTESchema(cte.Name)
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
		return nil
	}

	return p.iterateRecursiveCTE(ctx, cte, anchorSQL, recursiveSQL)
}

// errNoCTESchema is what a materialization that produced NO column list says.
//
// It exists so that `materializeRecursiveCTE` keeps ONE contract — either a
// cache entry or an error — which is what lets the reference refuse instead of
// falling through to a scan of a relation that does not exist (#1041's door).
// Every path that used to leave silently now names itself.
func errNoCTESchema(name string) error {
	return sqlerr.New("0A000",
		"the recursive CTE %q produced no column list, so there is nothing to read it as",
		name)
}

// recursiveForm is what a RECURSIVE CTE's body IS, decided from the parsed
// set-operation tree.
type recursiveForm int

const (
	// recursiveFormNotRecursive: the body does not name itself. The RECURSIVE
	// keyword decorates an ordinary query, which PostgreSQL answers as one.
	recursiveFormNotRecursive recursiveForm = iota
	// recursiveFormUnionAll: `non-recursive-term UNION ALL recursive-term`,
	// the one form this engine iterates.
	recursiveFormUnionAll
)

// classifyRecursiveBody decides a recursive CTE's FORM from the PARSED
// set-operation tree, and returns the anchor and recursive-term TEXT the
// fixed-point iteration re-plans.
//
// PostgreSQL parses `A UNION ALL B UNION ALL C` LEFT-ASSOCIATIVELY, so the
// non-recursive term is `A UNION ALL B` and the recursive term is `C`; this
// parser does the same. Deciding the form from the body's TEXT instead — a
// split at the FIRST top-level UNION ALL — put an arm that names the CTE and
// an arm that does not into one "recursive term", and the iteration re-ran the
// constant arm every round: 1002 rows (one, then 1001 NULLs) where PostgreSQL
// answers five (round-2 review, B3).
//
// Three answers, and each is PostgreSQL's own:
//
//   - a self-reference ANYWHERE but the last arm is 42P19, "recursive
//     reference to query %q must not appear within its non-recursive term" —
//     measured for a two-, three- and four-arm body with the reference in each
//     position, UNION and UNION ALL alike;
//   - the last arm names the CTE and the TOP operator is UNION without ALL:
//     PostgreSQL iterates and removes duplicates at every step, which this
//     engine has no fixed-point form for, so 0A000 (a feature gap, not a
//     syntax class — ADR-0012);
//   - no arm names the CTE: not recursive, and answered as the ordinary set
//     operation it is.
//
// The TEXT split is verified against the parse rather than trusted: the two
// halves are re-parsed and must name the CTE exactly as the tree said, or the
// body is refused. A split that disagrees with the form is what produced the
// 1002 rows.
func classifyRecursiveBody(cte plansql.CTEDef) (recursiveForm, string, string, error) {
	name := strings.ToLower(strings.TrimSpace(cte.Name))
	body, err := cte.BodySelect()
	if err != nil || body == nil {
		// A body that does not parse is reported by whoever plans it; this
		// pass says nothing about a tree it cannot read.
		return recursiveFormNotRecursive, "", "", nil
	}
	notTheForm := sqlerr.New("42P19",
		"recursive query %q does not have the form "+
			"non-recursive-term UNION [ALL] recursive-term", cte.Name)
	inNonRecursiveTerm := sqlerr.New("42P19",
		"recursive reference to query %q must not appear within its non-recursive term",
		cte.Name)

	if body.Union == nil {
		if plansql.SelectNamesRelation(body, name) {
			return recursiveFormNotRecursive, "", "", notTheForm
		}
		return recursiveFormNotRecursive, "", "", nil
	}
	// THE TOP NODE IS THE LAST OPERATOR, because the parse is left-associative:
	// its Left is every earlier arm together and its Right is the last one.
	top := body.Union
	if plansql.SelectNamesRelation(top.Left, name) {
		return recursiveFormNotRecursive, "", "", inNonRecursiveTerm
	}
	if !plansql.SelectNamesRelation(top.Right, name) {
		return recursiveFormNotRecursive, "", "", nil
	}
	if top.Op != plansql.SetOpUnion {
		return recursiveFormNotRecursive, "", "", notTheForm
	}
	if !top.All {
		return recursiveFormNotRecursive, "", "", sqlerr.New("0A000",
			"a recursive CTE written with UNION rather than UNION ALL is not supported: "+
				"PostgreSQL answers %q by removing duplicates at every step, and this "+
				"engine has no fixed-point form for that. Write UNION ALL, or remove the "+
				"duplicates in the query that reads it", cte.Name)
	}
	anchorSQL, recursiveSQL, ok := plansql.SplitLastTopLevelUnionAll(cte.SQL)
	if !ok ||
		selectTextNamesRelation(anchorSQL, name) ||
		!selectTextNamesRelation(recursiveSQL, name) {
		// The text split and the parse disagree about which arm is which.
		// Iterating on a split nobody verified is what answered 1002 rows.
		return recursiveFormNotRecursive, "", "", notTheForm
	}
	return recursiveFormUnionAll, anchorSQL, recursiveSQL, nil
}

// selectTextNamesRelation is selectNamesRelation over one arm's TEXT, for the
// caller that holds the split halves rather than the parsed body.
func selectTextNamesRelation(sql, want string) bool {
	parsed, err := plansql.Parse(sql)
	if err != nil {
		// Unparseable: assume it names the CTE, so the iteration keeps the
		// behaviour it had rather than silently taking the other path.
		return true
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil || info == nil {
		return true
	}
	return plansql.SelectNamesRelation(info, want)
}
