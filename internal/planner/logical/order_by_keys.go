package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// ORDER BY over an expression, and the family it closes.
//
// A Sort reads columns by NAME. Everything upstream of it — the SELECT-list
// Project, the aggregate, the scan — decides which names exist, and a sort
// key that names none of them used to match nothing and return the input
// untouched: right rows, arbitrary sequence, no error. #313 and #316 were two
// spellings of that failure (an alias no stage emitted); this is the third and
// widest one. `ORDER BY year(d)` and `ORDER BY -id` name no column at all
// because nothing ever computed them, and `ORDER BY b` over `SELECT a` names a
// column the Project already dropped. All three came back unsorted, and adding
// the term to the SELECT list "fixed" each one — the tell that the sort was
// keying on the projection's output names all along.
//
// Two rules close it:
//
//  1. A term the Sort's input does not carry is MATERIALIZED as a hidden
//     column on the SELECT-list projection — evaluated where the expression's
//     inputs still exist, sorted on, then dropped before the rows reach the
//     client (Projection.Hidden).
//
//  2. A term that can be neither resolved nor materialized is an ERROR. Sorting
//     is not advisory: an engine that cannot honour an ORDER BY must say so
//     rather than hand back an arbitrary order that looks like an answer. The
//     shapes that cannot be materialized are named explicitly in
//     hiddenSortProjection — none of them fails quietly.

// hiddenSortColPrefix names a materialized ORDER BY term. The "__" marks it
// derived, the same convention as __having_N and __gb_expr_N, and makes the
// column recognizable to every pass that has to leave it out of the user's
// result schema.
const hiddenSortColPrefix = string(plansql.SlotSortKey)

// IsHiddenSortColumn reports whether name is a materialized ORDER BY term.
func IsHiddenSortColumn(name string) bool {
	return strings.HasPrefix(name, hiddenSortColPrefix)
}

// HasHiddenProjection reports whether any of projs was materialized for a sort
// key rather than selected by the user.
func HasHiddenProjection(projs []Projection) bool {
	for _, p := range projs {
		if p.Hidden {
			return true
		}
	}
	return false
}

// VisibleProjections returns projs without the columns the planner
// materialized for its own use. Returns projs itself when there are none, so
// the common plan costs no allocation.
func VisibleProjections(projs []Projection) []Projection {
	if !HasHiddenProjection(projs) {
		return projs
	}
	out := make([]Projection, 0, len(projs))
	for _, p := range projs {
		if !p.Hidden {
			out = append(out, p)
		}
	}
	return out
}

// resolveOrderBy turns a parsed ORDER BY list into the Sort node's keys.
//
// child is the node the Sort will read (the SELECT-list Project, a Distinct
// over it, or — for `SELECT *` — whatever the query's FROM produced), and
// project is the SELECT-list Project when the query has one. Terms the child
// does not carry are materialized onto project as hidden columns; the returned
// node is the child the caller should hang the Sort on, which differs from the
// one passed in only when a `SELECT *` query needed a projection created for
// it.
func resolveOrderBy(child, project *Node, info *plansql.SelectInfo) (*Node, []OrderExpr, error) {
	outputs, items := selectOutputNames(info)
	starOnly := isStarOnly(info.Columns)

	keys := make([]OrderExpr, 0, len(info.OrderBy))
	var hidden []Projection

	// One allocator for this query's materialized ORDER BY terms, seeded with
	// the SELECT list's output names so a hidden key can never collide with a
	// column the user selected, and never with another hidden key
	// (plansql.SlotAllocator).
	sortAlloc := plansql.NewSlotAllocator(outputs...)

	for _, ob := range info.OrderBy {
		key := resolveOrderByColumn(cleanExpr(ob.Column), info.Columns)
		if sortKeyCarried(key, items, outputs, starOnly, ob.Expr) {
			keys = append(keys, orderExprFor(key, ob))
			continue
		}

		// An aggregate term over an UNGROUPED aggregate query (#811).
		//
		// Dropped, not materialized, and the reason is a proof rather than a
		// convenience: an aggregate with no GROUP BY and no GROUPING SETS
		// emits exactly ONE row, and one row is sorted. Nothing about the
		// result can depend on this term, so there is no "arbitrary order"
		// for the drop to hide — which is the ONLY condition under which this
		// pass drops a term rather than failing.
		//
		// The aggregate itself is still computed: the builder hoisted it into
		// the aggregate node above, so a SUM that would overflow still
		// overflows. With a GROUP BY the drop would be a silent wrong order
		// over many rows, and that shape keeps its refusal below (#597).
		if ungroupedAggregateSort(ob, project, info) {
			continue
		}

		// A select-list POSITION the parser could not count, because the list
		// carries a `*`. It is not materialized — the value it names is a
		// column of the expanded list, and ResolveOrdinalSortKeys names it
		// once the star has expanded (#810).
		if pos, ok := deferredOrdinal(ob, info.Columns); ok {
			keys = append(keys, OrderExpr{
				Column: cleanExpr(ob.Column), Position: pos,
				Desc: ob.Desc, NullsFirst: ob.NullsFirst,
			})
			continue
		}

		name, ok := sortAlloc.Next(plansql.SlotSortKey)
		if !ok {
			continue // the family is exhausted; leave the term as written
		}
		proj, err := hiddenSortProjection(ob, child, project, name, starOnly)
		if err != nil {
			return nil, nil, err
		}
		hidden = append(hidden, proj)
		keys = append(keys, orderExprFor(name, ob))
	}

	if len(hidden) == 0 {
		return child, keys, nil
	}
	if project == nil {
		// `SELECT * ... ORDER BY <expr>`: there is no projection to hang the
		// materialized column on, so give the query one that keeps the star.
		// ExpandStarProjections rewrites the star into the scan's columns
		// before the physical planner sees it; hiddenSortProjection has
		// already refused the shapes where that expansion cannot happen.
		project = NewProject(child, []Projection{{
			Expr:    "*",
			Column:  "*",
			ASTExpr: &plansql.StarNode{},
		}})
		child = project
	}
	// Hidden columns go LAST and stay last: the DAG's gather aligns its
	// SELECT-list renames against this slice by index, and only a trailing
	// tail leaves that alignment intact.
	project.Projections = append(project.Projections, hidden...)
	return child, keys, nil
}

// hiddenSortProjection builds the projection that materializes one ORDER BY
// term, or explains why this shape cannot have one. Every return path here is
// deliberate and named: an ORDER BY the engine cannot honour must fail loudly,
// never come back as an arbitrary order (#320).
func hiddenSortProjection(ob plansql.OrderByItem, child, project *Node, name string, starOnly bool) (Projection, error) {
	if ob.Expr == nil {
		return Projection{}, orderByError(ob, "42601",
			"the term did not parse into an expression the planner can evaluate")
	}
	if lit, ok := unwrapParens(ob.Expr).(*plansql.Lit); ok && lit.Kind == plansql.LitNumber {
		// A numeric constant in ORDER BY is a select-list POSITION.
		// resolvePositionalRefs rewrites it to the item it names and rejects
		// one that is out of range, so reaching here means it named nothing —
		// a `SELECT *` list, whose positions are not countable until the star
		// expands, later. Materializing the constant would sort by a column
		// that is the same in every row: a silent no-op, which is the whole
		// failure mode this pass exists to end.
		return Projection{}, orderByError(ob, "42P10",
			"a numeric constant is a select-list position, and this one names no select item")
	}
	if child != nil && child.Type == NodeDistinct {
		// Materializing here would widen the DISTINCT's dedup key and change
		// which rows survive. SQL rejects the shape for the same reason.
		// PostgreSQL's own class and wording: "for SELECT DISTINCT, ORDER BY
		// expressions must appear in select list", 42P10.
		return Projection{}, orderByError(ob, "42P10",
			"for SELECT DISTINCT, ORDER BY expressions must appear in select list")
	}
	if starOnly {
		// The star has to expand into real columns before the projection can
		// carry anything alongside it, and ExpandStarProjections only resolves
		// a star that reads one base table.
		if loneScan(child) == nil {
			// 0A000, not 42703: PostgreSQL ANSWERS this shape. The refusal is
			// wadjet's own bound — a star over a join is left unexpanded
			// because guessing its column set would change which columns the
			// query returns — and a client is owed the class that says
			// "this engine does not implement it" rather than one that says
			// "your SQL is wrong".
			return Projection{}, orderByError(ob, "0A000",
				"`SELECT *` over more than one relation cannot carry a computed sort key — name the columns, or select the sort expression")
		}
	}
	if agg := aggregateBelow(project); agg != nil && !sortTermResolvesOverAggregate(ob.Expr, agg) {
		// #597: `GROUP BY g ORDER BY MAX(id)` — an aggregate the SELECT list
		// does not carry. PostgreSQL answers it, DuckDB answers it, and this
		// engine refused it.
		//
		// The value already exists by the time this runs: BuildFromSelect
		// hoists every aggregate named only in ORDER BY onto the Aggregate
		// node under a slot of its own (#811), for the reason PostgreSQL has
		// to compute it too — one that raises must raise. So the term needs no
		// evaluation at all, only a NAME: the hidden projection is a plain
		// reference to that output column, which the single-process Project
		// copies and which `physical.resolveHiddenSortKeys` maps straight onto
		// the column the aggregate stage emits (annotateHiddenSortSource's
		// simple-ColRef arm sets SourceColumn, and nothing has to be
		// materialized on either engine).
		//
		// Only a BARE call qualifies. `ORDER BY COUNT(*) * 2` is a value
		// COMPUTED from the aggregate's output, which is the case the refusal
		// below is really about — the DAG's sort runs between the aggregate
		// and the gather with nothing in between to evaluate it — and it stays
		// refused, with a fixture that attempts it.
		if call, ok := bareAggregateCall(ob.Expr); ok {
			if out := aggregateOutputForCall(call, agg); out != "" {
				return Projection{
					Expr:    out,
					Column:  out,
					Alias:   name,
					ASTExpr: &plansql.ColRef{Column: out},
					Hidden:  true,
				}, nil
			}
		}
		// The DELIBERATE exclusion. Over a GROUP BY, a materialized key is
		// honoured only where BOTH engines can find it: the single-process
		// Project computes it from the aggregate's output, and the DAG's
		// walkStages maps it back through that Project to a column the
		// aggregate stage emits (resolveSortKeyColumn). That mapping lands
		// only on a group key or an aggregate output — a value COMPUTED from
		// them, `LENGTH(o_orderpriority)` or `COUNT(*) * 2`, exists nowhere in
		// the DAG, whose sort runs between the aggregate and the gather with
		// nothing in between to evaluate it. Rather than honour the ORDER BY
		// on small inputs and lose it on large ones — the routing-dependent
		// answer this whole suite exists to prevent — the query fails on both.
		if containsAggregateCall(ob.Expr) {
			// 0A000: PostgreSQL ANSWERS this (`GROUP BY g ORDER BY MAX(id)`
			// returns the groups ordered by their maxima), so the class has
			// to say "not implemented here", not "your SQL is wrong". #597.
			return Projection{}, orderByError(ob, "0A000",
				"an aggregate expression that is not itself a select item cannot be sorted on — select it, then ORDER BY its alias")
		}
		// 0A000, measured rather than assumed. The shape PostgreSQL refuses
		// here — `GROUP BY g ORDER BY LENGTH(c_str)`, an UNGROUPED column in
		// the sort term — never reaches this line: physical.checkUngrouped
		// settles it earlier with PostgreSQL's own 42803 and its own wording.
		// What reaches this line is a term computed from GROUPED columns
		// (`ORDER BY g * 2`), which PostgreSQL ANSWERS, so the class owed is
		// "not implemented here". The one case that could arrive here and be
		// a genuine 42803 is a block whose sources checkUngrouped could not
		// enumerate, where it declines to judge at all; a class divergence
		// there is the cost of that stance, not of this one.
		return Projection{}, orderByError(ob, "0A000",
			"over a GROUP BY, only a grouped column, a grouping expression, or a select-list alias can be sorted on — select this expression, then ORDER BY its alias")
	}
	proj := Projection{
		Expr:    cleanExpr(ob.Column),
		Alias:   name,
		ASTExpr: ob.Expr,
		Hidden:  true,
	}
	if col, ok := ob.Expr.(*plansql.ColRef); ok {
		proj.Column = qualifiedColumn(col)
	}
	return proj, nil
}

// orderByError reports a sort key the engine cannot honour. Named separately
// so every instance of this family reads the same way at the client.
//
// Every one of them carries a SQLSTATE, and which one is a measured question
// rather than a stylistic one (family C, #811): 42P10 and 42803 where
// PostgreSQL refuses the same shape with that class, 0A000 where PostgreSQL
// ANSWERS it and the refusal is wadjet's own bound. A refusal with no class
// reaches a client as the blanket 42000, which cannot be told from a syntax
// error — and every refusal in this file used to send exactly that.
func orderByError(ob plansql.OrderByItem, state, reason string) error {
	return sqlerr.New(state, "ORDER BY %s: %s", cleanExpr(ob.Column), reason)
}

func orderExprFor(column string, ob plansql.OrderByItem) OrderExpr {
	// SlotPos rides along with every key built from an ORDINAL. A name
	// addresses one column only while it is unique, and this is where the
	// position would otherwise be lost (#557).
	return OrderExpr{Column: column, Desc: ob.Desc, NullsFirst: ob.NullsFirst, SlotPos: ob.Ordinal}
}

// sortKeyCarried reports whether the Sort's input already emits key.
//
// With a SELECT list, the Project below the Sort narrows the schema to exactly
// its outputs, so the select-list names are the whole of what a sort key can
// resolve against. With `SELECT *` the Sort reads the relation itself, whose
// column set is not known here — a bare column reference is taken at its word
// (that is the pre-existing contract, and the catalog rejects a name that does
// not exist), and anything computed is materialized.
//
// items are the SELECT-list columns outputs was derived from, index for index,
// and they are needed for one rule: **a QUALIFIED ORDER BY term names an INPUT
// column, never a SELECT-list alias** (#488). PostgreSQL only consults output
// names for a bare identifier — that is what makes `SELECT s_acctbal AS
// s_suppkey … ORDER BY s_suppkey` order by ACCTBAL — and `x.col` is resolved in
// the FROM scope like any other expression. The two rules meet here because
// namesSameColumn deliberately tolerates one side carrying a qualifier the
// other omits, so `s.s_suppkey` matched the output named `s_suppkey` and the
// sort read the alias: verified live on postgres:17-alpine, which orders that
// query by the real key while this engine ordered it by the shadowing alias, on
// both arms and silently.
//
// The match therefore has to prove the output IS that input column: the select
// item is a bare column reference of the same name, qualified by the same
// relation or by none. Anything else falls through and the term is
// materialized as a hidden key over the input, where it belongs.
func sortKeyCarried(key string, items []plansql.SelectColumn, outputs []string, starOnly bool, ast plansql.Node) bool {
	if starOnly {
		_, isCol := ast.(*plansql.ColRef)
		return isCol || ast == nil
	}
	ref := qualifiedInputRef(key, ast)
	for i, out := range outputs {
		if !namesSameColumn(out, key) {
			continue
		}
		if ref == nil || selectItemIsColumn(items, i, ref) {
			return true
		}
	}
	return false
}

// qualifiedInputRef returns the column reference an ORDER BY term names when
// the term is a QUALIFIED one still being resolved against the input.
//
// resolveOrderByColumn runs first and may already have mapped the term onto a
// select item by its EXPRESSION text (`SELECT s.a AS k … ORDER BY s.a` becomes
// the key `k`). That mapping names the same column the qualified reference
// does, so it is not the shadowing case and gets no extra scrutiny — which is
// what the key-vs-spelling comparison here detects.
func qualifiedInputRef(key string, ast plansql.Node) *plansql.ColRef {
	col, ok := unwrapParens(ast).(*plansql.ColRef)
	if !ok || col.Table == "" {
		return nil
	}
	if !strings.EqualFold(key, qualifiedColumn(col)) {
		return nil
	}
	return col
}

// selectItemIsColumn reports whether the select item at index i IS the input
// column ref names — a bare column reference of the same name, qualified by
// the same relation or by none.
//
// An unqualified item is accepted because a bare column in a SELECT list is
// only legal when one relation in scope carries that name, so it cannot be the
// other side of a self-join; a DIFFERENTLY qualified one is rejected for
// exactly that reason (`SELECT n2.nm … ORDER BY n1.nm` orders by n1's column in
// PostgreSQL, and the bare fallback would have matched n2's).
func selectItemIsColumn(items []plansql.SelectColumn, i int, ref *plansql.ColRef) bool {
	if i >= len(items) {
		return false
	}
	sc := items[i]
	if sc.Alias != "" || sc.ColumnRef == "" || sc.IsAgg || sc.IsWindow {
		return false
	}
	if !strings.EqualFold(sc.ColumnRef, ref.Column) {
		return false
	}
	return sc.TableRef == "" || strings.EqualFold(sc.TableRef, ref.Table)
}

// namesSameColumn compares two column spellings the way the engine's runtime
// lookup does: case-insensitively, and tolerating one side carrying a table
// qualifier the other omits (columnIndexFallback).
func namesSameColumn(a, b string) bool {
	aq, bq := isQuotedIdent(a), isQuotedIdent(b)
	a, b = unquoteIdent(a), unquoteIdent(b)
	if strings.EqualFold(a, b) {
		return true
	}
	// A quoted identifier may legitimately CONTAIN a dot — Zeek's `id.orig_h`
	// is one column, not a qualified reference (#304) — so the qualifier
	// fallback only applies to spellings that were not quoted. Without this,
	// ORDER BY "id.orig_h" over GROUP BY "id.orig_h" compared orig_h against
	// id.orig_h, matched nothing, and the new unresolvable-key guard rejected
	// a query that had always worked.
	if aq || bq {
		return false
	}
	return strings.EqualFold(bareColumn(a), bareColumn(b))
}

// unquoteIdent strips one layer of double quotes from an identifier, leaving
// unquoted text untouched.
func unquoteIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return s[1 : len(s)-1]
	}
	return s
}

// isQuotedIdent reports whether a spelling arrived double-quoted, which is
// what makes a dot part of the name rather than a qualifier separator.
func isQuotedIdent(s string) bool {
	s = strings.TrimSpace(s)
	return len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"'
}

func bareColumn(s string) string {
	if dot := strings.LastIndexByte(s, '.'); dot >= 0 {
		return s[dot+1:]
	}
	return s
}

func qualifiedColumn(c *plansql.ColRef) string {
	if c.Table != "" {
		return c.Table + "." + c.Column
	}
	return c.Column
}

// selectOutputNames lists the column names the SELECT-list Project emits, in
// order — the same choice buildProject makes: the alias, else the column
// reference, else the expression text — together with the select item each
// name came from, index for index. sortKeyCarried needs the items to tell an
// output that IS an input column from one that merely shares its name.
func selectOutputNames(info *plansql.SelectInfo) ([]string, []plansql.SelectColumn) {
	out := make([]string, 0, len(info.Columns))
	items := make([]plansql.SelectColumn, 0, len(info.Columns))
	for _, col := range info.Columns {
		if col.Star {
			continue
		}
		switch {
		case col.IsWindow:
			// The same choice buildProject makes for a window column, and it
			// has to be the SAME choice: an unaliased window used to be read
			// off WindowSpec.Alias, which is "" exactly when the column has
			// no alias, so the sort compared its key against "" (#694).
			out = append(out, windowOutputName(col))
		case col.Alias != "":
			out = append(out, col.Alias)
		case col.ColumnRef != "":
			out = append(out, col.ColumnRef)
		default:
			out = append(out, cleanExpr(col.Expr))
		}
		items = append(items, col)
	}
	return out, items
}

// aggregateBelow finds the Aggregate the SELECT-list projection reads from,
// descending only through nodes that pass its output along unchanged. Returns
// nil when the projection reads rows rather than groups.
func aggregateBelow(project *Node) *Node {
	if project == nil {
		return nil
	}
	for _, n := range project.Children {
		for n != nil {
			switch n.Type {
			case NodeAggregate:
				return n
			case NodeFilter, NodeProject, NodeWindow, NodeSort, NodeLimit:
				if len(n.Children) != 1 {
					return nil
				}
				n = n.Children[0]
			default:
				return nil
			}
		}
	}
	return nil
}

// sortTermResolvesOverAggregate reports whether a materialized sort term reads
// straight off the aggregate below rather than computing something new from it.
//
// Two spellings qualify. A plain reference to a group key or an aggregate
// output is a passthrough: the Project copies the column, and the DAG maps the
// hidden name back to it. The GROUP BY's own expression is the same thing said
// differently — buildProject rewrites it to the synthetic __gb_expr_N column
// the aggregate already emits, and the DAG's key resolution matches it against
// the group-key text. Anything else has to be evaluated after the grouping,
// which only the single-process pipeline can do.
func sortTermResolvesOverAggregate(ast plansql.Node, agg *Node) bool {
	if col, ok := unwrapParens(ast).(*plansql.ColRef); ok {
		name := qualifiedColumn(col)
		for _, gb := range agg.GroupBy {
			if namesSameColumn(gb, name) {
				return true
			}
		}
		for _, a := range agg.AggExprs {
			if namesSameColumn(a.OutputCol, name) {
				return true
			}
		}
		return false
	}
	// By identity, not by rendered text: `ORDER BY (g + 1)` over
	// `GROUP BY g + 1` is the grouping expression said differently, and
	// comparing the renderings refused it with "only a grouped column, a
	// grouping expression, or a select-list alias can be sorted on" for a
	// query PostgreSQL answers (#723).
	id := plansql.ExprIdentity(ast)
	for _, gb := range agg.GroupBy {
		if strings.EqualFold(gb, id) {
			return true
		}
		if parsed, err := plansql.ParseExpression(gb); err == nil &&
			plansql.ExprIdentity(parsed) == id {
			return true
		}
	}
	for _, gbe := range agg.GroupByExprs {
		if gbe != nil && plansql.ExprIdentity(gbe) == id {
			return true
		}
	}
	return false
}

func unwrapParens(n plansql.Node) plansql.Node {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			return n
		}
		n = p.Inner
	}
}

// containsAggregateCall reports whether the expression calls an aggregate.
func containsAggregateCall(ast plansql.Node) bool {
	return len(plansql.FindAllAggregates(ast)) > 0
}

// orderByAggregates is every aggregate CALL named in an ORDER BY term.
//
// PostgreSQL's parseCheckAggregates reads the sort clause along with the
// select list and the HAVING, so an aggregate here makes the query aggregated
// (#811). Exported inside the package only; the physical binder asks the same
// question of the same SelectInfo through its own copy, because it validates
// before a logical plan exists.
func orderByAggregates(info *plansql.SelectInfo) []*plansql.FuncCallNode {
	var out []*plansql.FuncCallNode
	for _, ob := range info.OrderBy {
		if ob.Expr == nil {
			continue
		}
		out = append(out, plansql.FindAllAggregates(ob.Expr)...)
	}
	return out
}

// ungroupedAggregateSort reports whether this ORDER BY term may be dropped
// because the query it sorts returns exactly one row.
//
// Three conditions, all necessary: the term names an aggregate, an Aggregate
// node exists below the projection, and that aggregate has NO grouping — no
// GROUP BY and no GROUPING SETS. Only then is the row count provably one.
func ungroupedAggregateSort(ob plansql.OrderByItem, project *Node, info *plansql.SelectInfo) bool {
	if ob.Expr == nil || !containsAggregateCall(ob.Expr) {
		return false
	}
	if len(info.GroupBy) > 0 || len(info.GroupingSets) > 0 {
		return false
	}
	return aggregateBelow(project) != nil
}

// bareAggregateCall reports the aggregate call an ORDER BY term IS, as opposed
// to one it merely contains. `MAX(id)` and `(MAX(id))` qualify; `MAX(id) + 1`
// and `LENGTH(MIN(c_str))` do not — those are values computed FROM an
// aggregate's output, which is a different question with a different answer.
func bareAggregateCall(ast plansql.Node) (*plansql.FuncCallNode, bool) {
	call, ok := unwrapParens(ast).(*plansql.FuncCallNode)
	if !ok {
		return nil, false
	}
	aggs := plansql.FindAllAggregates(call)
	if len(aggs) != 1 || aggs[0] != call {
		return nil, false
	}
	return call, true
}

// aggregateOutputForCall names the column an Aggregate node publishes this
// call's value under, or "" when it computes no such aggregate.
//
// The match is reuseOrAddAggregate's, field for field, because that is the
// function that decided which output the call was hoisted into: comparing
// rendered TEXT instead would miss `COUNT(*)`, whose AggExpr carries an empty
// InputCol where the call says `*` (the same normalisation HAVING's reuse test
// had to make, and got wrong once).
func aggregateOutputForCall(call *plansql.FuncCallNode, agg *Node) string {
	if call == nil || agg == nil || len(call.Args) > 1 {
		return ""
	}
	inputCol := ""
	if len(call.Args) > 0 {
		inputCol = cleanExpr(call.Args[0].String())
	}
	funcName := strings.ToLower(call.Name)
	if funcName == "count" && (inputCol == "*" || inputCol == "") {
		inputCol = ""
	}
	for _, a := range agg.AggExprs {
		if a.InputCol2 != "" || a.Separator != "" || a.Percentile != 0 {
			continue
		}
		if strings.EqualFold(a.Func, funcName) &&
			strings.EqualFold(a.InputCol, inputCol) &&
			a.Distinct == call.Distinct {
			return a.OutputCol
		}
	}
	return ""
}
