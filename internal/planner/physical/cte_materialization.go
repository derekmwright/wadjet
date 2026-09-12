// This file holds cte materialization for the physical planner, governed by ADR-0026 and ADR-0034.
package physical

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
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
		schema = p.inferCTESchema(sql, nil)
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
func (s *cteMaterializingSink) Close() error                       { return s.coll.Close() }

// inferCTESchema derives column types from a CTE's SQL and data rows.
func (p *Planner) inferCTESchema(sql string, rows []map[string]any) []parquet.Column {
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
	// node itself carries no SELECT list, so a multi-arm anchor —
	// `SELECT 1 AS v UNION ALL SELECT 2`, which the left-associative form test
	// makes the non-recursive term of a three-arm body — gave a schema of
	// ZERO columns and the reference answered "the result has no columns"
	// (round-2 review, B3).
	for info.Union != nil && info.Union.Left != nil {
		info = info.Union.Left
	}
	schema := make([]parquet.Column, len(info.Columns))
	names := make([]string, len(info.Columns))
	for i, col := range info.Columns {
		names[i] = col.Alias
		if names[i] == "" {
			names[i] = col.Expr
		}
	}
	// The key each POSITION appears under in the rows executeSubquery
	// returns. Two output columns may share a name — `SELECT 1 AS x, 10 AS x`
	// is legal SQL — and a row is a Go map, so the second would overwrite the
	// first; subqueryRowsPerColumn suffixes every later duplicate with its
	// position for exactly that reason. Reading by the bare NAME here made
	// both positions answer with the FIRST column's value, which is #957.
	keys := subqueryRowKeys(names)
	for i := range info.Columns {
		name := names[i]
		key := keys[i]
		typ := parquet.TypeString
		if len(rows) > 0 {
			if v, ok := rows[0][key]; ok {
				switch v.(type) {
				case int64:
					typ = parquet.TypeInt64
				case int32:
					typ = parquet.TypeInt32
				case float64:
					typ = parquet.TypeFloat64
				case bool:
					typ = parquet.TypeBool
				case string:
					// Check if the string value is actually a numeric literal
					// (SELECT 1 returns "1" as a string from the expression evaluator)
					s := v.(string)
					if _, err := strconv.ParseInt(s, 10, 64); err == nil {
						typ = parquet.TypeInt64
						// Convert all rows' values from string to int64
						for _, row := range rows {
							if sv, ok := row[key].(string); ok {
								if iv, err := strconv.ParseInt(sv, 10, 64); err == nil {
									row[key] = iv
								}
							}
						}
					} else if _, err := strconv.ParseFloat(s, 64); err == nil {
						typ = parquet.TypeFloat64
						for _, row := range rows {
							if sv, ok := row[key].(string); ok {
								if fv, err := strconv.ParseFloat(sv, 64); err == nil {
									row[key] = fv
								}
							}
						}
					}
				}
			}
		}
		schema[i] = parquet.Column{Name: name, Type: typ, Nullable: true}
	}
	return schema
}

const maxRecursiveIterations = 1000

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
		coll, schema, err := p.materializeCTEColumnar(ctx, cte.SQL, nil, p.ctes)
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

	// Step 1: Execute anchor query
	anchorRows, err2 := p.executeSubquery(ctx, anchorSQL)
	if err2 != nil {
		return err2
	}
	if len(anchorRows) == 0 {
		schema := p.inferCTESchema(anchorSQL, nil)
		if schema == nil {
			return errNoCTESchema(cte.Name)
		}
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: nil}
		return nil
	}

	// Infer schema from anchor results
	schema := p.inferCTESchema(anchorSQL, anchorRows)
	if schema == nil {
		return errNoCTESchema(cte.Name)
	}

	// Apply column aliases if specified: WITH t(a, b) AS (...)
	//
	// POSITIONALLY, through the key each column really appears under. A
	// column list is what makes a duplicate-name body legal and useful —
	// `WITH RECURSIVE t(a, b) AS (SELECT 1 AS x, 10 AS x …)` — and reading
	// both positions by the bare name `x` gave both aliases the FIRST
	// column's value, so the working row collapsed and every later iteration
	// read it: `1,10 | 2,100 | 3,10000` in PostgreSQL 17 came back
	// `1,1 | 2,1 | 3,1` (#957).
	if len(cte.Columns) > 0 && len(cte.Columns) <= len(schema) {
		anchorRows = renameRowColumnsPositional(anchorRows, schema, cte.Columns)
		for i, name := range cte.Columns {
			schema[i].Name = name
		}
	}

	// Accumulate iteration results columnar into a tracker-charged,
	// spill-backed collector instead of an unbounded boxed slice — the
	// iteration count was bounded (1000) but the row count was not, and
	// every accumulated row lived as a map[string]any until Plan returned.
	// The per-iteration work table stays boxed: it is one iteration's
	// delta (inherent to the fixed-point algorithm) and is re-seeded into
	// the cache each step for the recursive query's self-reference.
	coll := &exec.SpillableBatchCollector{Spill: p.getSpillManager()}
	appendRowsColumnar := func(rs []map[string]any) error {
		for off := 0; off < len(rs); off += batch.DefaultBatchSize {
			end := off + batch.DefaultBatchSize
			if end > len(rs) {
				end = len(rs)
			}
			if err := coll.Consume(ctx, batch.FromRows(schema, rs[off:end])); err != nil {
				return err
			}
		}
		return nil
	}
	if err := appendRowsColumnar(anchorRows); err != nil {
		coll.Release()
		return err
	}

	// Derive the expected column names from the schema (aliases already applied).
	schemaNames := make([]string, len(schema))
	for i, col := range schema {
		schemaNames[i] = col.Name
	}

	// Parse the recursive SQL to get its output column names so we can
	// positionally rename them to match the CTE schema. Strip table alias
	// prefixes (e.g., "e.id" → "id") because the Project operator outputs
	// unqualified column names.
	var recursiveColNames []string
	if rpq, err := plansql.Parse(recursiveSQL); err == nil {
		if ri, err := plansql.ExtractSelect(rpq); err == nil {
			for _, col := range ri.Columns {
				name := col.Alias
				if name == "" {
					name = cleanExpr(col.Expr)
				}
				recursiveColNames = append(recursiveColNames, name)
			}
		}
	}
	// …and the keys those names really occupy, for the same reason the anchor
	// needs them: two recursive-term outputs may share a name.
	recursiveRowKeys := subqueryRowKeys(recursiveColNames)

	// Step 2: Fixed-point iteration
	workTable := anchorRows
	for iter := 0; iter < maxRecursiveIterations; iter++ {
		// Seed the CTE cache with the current work table so the recursive
		// query's reference to the CTE name resolves to these rows.
		p.cteCache[cte.Name] = &cteMaterialized{schema: schema, rows: workTable}

		newRows, err := p.executeSubquery(ctx, recursiveSQL)
		if err != nil {
			break
		}
		if len(newRows) == 0 {
			break
		}

		// Rename output columns to match CTE schema. The recursive SQL may
		// produce different column names (e.g., "n + 1" vs "n").
		newRows = renameRowColumnsFromTo(newRows, recursiveRowKeys, schemaNames)

		if err := appendRowsColumnar(newRows); err != nil {
			// Spill scratch failure mid-iteration: abandon materialization.
			// Without a cache entry the recursive reference cannot resolve
			// and the query errors — same failure mode as an anchor error.
			coll.Release()
			delete(p.cteCache, cte.Name)
			return err
		}
		workTable = newRows
	}

	// Store final accumulated results (columnar; replayed per reference).
	p.cteCache[cte.Name] = &cteMaterialized{schema: schema, coll: coll}
	return nil
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
		if selectNamesRelation(body, name) {
			return recursiveFormNotRecursive, "", "", notTheForm
		}
		return recursiveFormNotRecursive, "", "", nil
	}
	// THE TOP NODE IS THE LAST OPERATOR, because the parse is left-associative:
	// its Left is every earlier arm together and its Right is the last one.
	top := body.Union
	if selectNamesRelation(top.Left, name) {
		return recursiveFormNotRecursive, "", "", inNonRecursiveTerm
	}
	if !selectNamesRelation(top.Right, name) {
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
	anchorSQL, recursiveSQL, ok := splitLastTopLevelUnion(cte.SQL)
	if !ok ||
		selectTextNamesRelation(anchorSQL, name) ||
		!selectTextNamesRelation(recursiveSQL, name) {
		// The text split and the parse disagree about which arm is which.
		// Iterating on a split nobody verified is what answered 1002 rows.
		return recursiveFormNotRecursive, "", "", notTheForm
	}
	return recursiveFormUnionAll, anchorSQL, recursiveSQL, nil
}

// splitLastTopLevelUnion splits a body's TEXT at the LAST top-level `UNION ALL`
// — the operator the left-associative parse puts at the root — into the
// non-recursive term and the recursive term.
//
// Splitting at the FIRST one is what made a three-arm body's "recursive term"
// two arms (round-2 review, B3). The caller VERIFIES the halves against the
// parse; this function only finds the position.
func splitLastTopLevelUnion(sql string) (anchor, recursive string, ok bool) {
	upper := strings.ToUpper(sql)
	depth := 0
	inStr := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if inStr {
			if ch == '\'' {
				if i+1 < len(sql) && sql[i+1] == '\'' {
					i++ // escaped quote
				} else {
					inStr = false
				}
			}
			continue
		}
		switch ch {
		case '\'':
			inStr = true
			continue
		case '(':
			depth++
		case ')':
			depth--
		}
		if depth != 0 || i+5 > len(upper) || upper[i:i+5] != "UNION" {
			continue
		}
		if i > 0 && !isSQLBreak(sql[i-1]) {
			continue // inside a longer identifier
		}
		rest := strings.TrimLeft(upper[i+5:], " \t\n\r")
		if !strings.HasPrefix(rest, "ALL") {
			continue
		}
		end := i + 5
		for end < len(sql) && isSQLSpace(sql[end]) {
			end++
		}
		end += 3 // skip ALL
		// Keep scanning: the LAST top-level one is the root of the parse.
		anchor, recursive, ok = strings.TrimSpace(sql[:i]), strings.TrimSpace(sql[end:]), true
	}
	return anchor, recursive, ok
}

func isSQLSpace(c byte) bool { return c == ' ' || c == '\t' || c == '\n' || c == '\r' }

// isSQLBreak reports whether c cannot be part of an identifier, so a keyword
// that starts after it really is a keyword.
func isSQLBreak(c byte) bool {
	switch {
	case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
		return false
	}
	return true
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
	return selectNamesRelation(info, want)
}

// selectNamesRelation reports whether info's FROM — at any nesting, through a
// derived table and through both arms of a set operation — names `want`.
func selectNamesRelation(info *plansql.SelectInfo, want string) bool {
	if info == nil {
		return false
	}
	if info.Union != nil {
		if selectNamesRelation(info.Union.Left, want) || selectNamesRelation(info.Union.Right, want) {
			return true
		}
	}
	refNames := func(t plansql.TableRef) bool {
		if strings.HasPrefix(t.Name, "(") {
			sub, err := t.SubSelect()
			if err != nil {
				return false
			}
			return selectNamesRelation(sub, want)
		}
		return strings.EqualFold(strings.TrimSpace(t.Name), want)
	}
	for i := range info.Tables {
		if refNames(info.Tables[i]) {
			return true
		}
	}
	for i := range info.Joins {
		ref := plansql.TableRef{Name: info.Joins[i].RightTable}
		if info.Joins[i].RightTableRef != nil {
			ref = *info.Joins[i].RightTableRef
		}
		if refNames(ref) {
			return true
		}
	}
	// A nested block's OWN `WITH` may shadow the name; this walk deliberately
	// does not, because a shadowing item is answered from the enclosing scope
	// here anyway (docs/internals/nested-with-scope-precedence.md) and a
	// false positive costs a refusal where a wrong answer would otherwise
	// stand.
	for i := range info.CTEs {
		if b, err := info.CTEs[i].BodySelect(); err == nil && selectNamesRelation(b, want) {
			return true
		}
	}
	return false
}

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

// renameRowColumnsFromTo remaps row keys from srcNames[i] to dstNames[i].
func renameRowColumnsFromTo(rows []map[string]any, srcNames, dstNames []string) []map[string]any {
	if len(rows) == 0 || len(srcNames) == 0 || len(dstNames) == 0 {
		return rows
	}
	needsRename := false
	for i := range srcNames {
		if i < len(dstNames) && srcNames[i] != dstNames[i] {
			needsRename = true
			break
		}
	}
	if !needsRename {
		return rows
	}
	result := make([]map[string]any, len(rows))
	for ri, row := range rows {
		newRow := make(map[string]any, len(row))
		for k, v := range row {
			newRow[k] = v
		}
		for i, src := range srcNames {
			if i < len(dstNames) && src != dstNames[i] {
				newRow[dstNames[i]] = row[src]
				delete(newRow, src)
			}
		}
		result[ri] = newRow
	}
	return result
}

// subqueryRowKeys is the map key each POSITION of a result occupies in the
// rows executeSubquery returns.
//
// It states subqueryRowsPerColumn's rule once so the readers cannot drift from
// the writer: the first occurrence of a name keeps the name, and every later
// column of that name carries `:<position>`. A colon cannot appear in an
// identifier the binder resolves, so a disambiguated key collides with
// nothing.
func subqueryRowKeys(names []string) []string {
	keys := make([]string, len(names))
	seen := make(map[string]bool, len(names))
	for i, n := range names {
		k := n
		if seen[k] {
			k = fmt.Sprintf("%s:%d", n, i)
		}
		seen[k] = true
		keys[i] = k
	}
	return keys
}

// renameRowColumnsPositional rebuilds each row under the target aliases,
// reading column i by the key POSITION i occupies rather than by the schema's
// name. A shorter alias list renames the LEADING columns and the rest keep
// their own names, which is PostgreSQL's rule for a column list.
func renameRowColumnsPositional(rows []map[string]any, schema []parquet.Column, aliases []string) []map[string]any {
	keys := make([]string, len(schema))
	names := make([]string, len(schema))
	for i, c := range schema {
		names[i] = c.Name
	}
	copy(keys, subqueryRowKeys(names))
	out := make([]map[string]any, len(rows))
	for ri, row := range rows {
		nr := make(map[string]any, len(schema))
		for i := range schema {
			name := schema[i].Name
			if i < len(aliases) {
				name = aliases[i]
			}
			nr[name] = row[keys[i]]
		}
		out[ri] = nr
	}
	return out
}

// renameRowColumns remaps row keys from schema column names to the target aliases.
func renameRowColumns(rows []map[string]any, schema []parquet.Column, aliases []string) []map[string]any {
	// Check if rename is needed
	needsRename := false
	for i, alias := range aliases {
		if i < len(schema) && schema[i].Name != alias {
			needsRename = true
			break
		}
	}
	if !needsRename {
		return rows
	}
	result := make([]map[string]any, len(rows))
	for ri, row := range rows {
		newRow := make(map[string]any, len(row))
		for i, col := range schema {
			if i < len(aliases) {
				newRow[aliases[i]] = row[col.Name]
			} else {
				newRow[col.Name] = row[col.Name]
			}
		}
		result[ri] = newRow
	}
	return result
}
