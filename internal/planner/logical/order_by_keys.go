package logical

import (
	"fmt"
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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
const hiddenSortColPrefix = "__sortkey_"

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
	outputs := selectOutputNames(info)
	starOnly := isStarOnly(info.Columns)

	keys := make([]OrderExpr, 0, len(info.OrderBy))
	var hidden []Projection

	for _, ob := range info.OrderBy {
		key := resolveOrderByColumn(cleanExpr(ob.Column), info.Columns)
		if sortKeyCarried(key, outputs, starOnly, ob.Expr) {
			keys = append(keys, orderExprFor(key, ob))
			continue
		}

		name := fmt.Sprintf("%s%d", hiddenSortColPrefix, len(hidden))
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
		return Projection{}, orderByError(ob, "the term did not parse into an expression the planner can evaluate")
	}
	if lit, ok := unwrapParens(ob.Expr).(*plansql.Lit); ok && lit.Kind == plansql.LitNumber {
		// A numeric constant in ORDER BY is a select-list POSITION.
		// resolvePositionalRefs rewrites it to the item it names and rejects
		// one that is out of range, so reaching here means it named nothing —
		// a `SELECT *` list, whose positions are not countable until the star
		// expands, later. Materializing the constant would sort by a column
		// that is the same in every row: a silent no-op, which is the whole
		// failure mode this pass exists to end.
		return Projection{}, orderByError(ob, "a numeric constant is a select-list position, and this one names no select item — `SELECT *` is expanded too late for the planner to count its positions here")
	}
	if child != nil && child.Type == NodeDistinct {
		// Materializing here would widen the DISTINCT's dedup key and change
		// which rows survive. SQL rejects the shape for the same reason.
		return Projection{}, orderByError(ob, "SELECT DISTINCT requires every ORDER BY term to appear in the select list")
	}
	if starOnly {
		// The star has to expand into real columns before the projection can
		// carry anything alongside it, and ExpandStarProjections only resolves
		// a star that reads one base table.
		if loneScan(child) == nil {
			return Projection{}, orderByError(ob, "`SELECT *` over more than one relation cannot carry a computed sort key — name the columns, or select the sort expression")
		}
	}
	if agg := aggregateBelow(project); agg != nil && !sortTermResolvesOverAggregate(ob.Expr, agg) {
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
			return Projection{}, orderByError(ob, "an aggregate expression that is not itself a select item cannot be sorted on — select it, then ORDER BY its alias")
		}
		return Projection{}, orderByError(ob, "over a GROUP BY, only a grouped column, a grouping expression, or a select-list alias can be sorted on — select this expression, then ORDER BY its alias")
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
func orderByError(ob plansql.OrderByItem, reason string) error {
	return fmt.Errorf("ORDER BY %s: %s", cleanExpr(ob.Column), reason)
}

func orderExprFor(column string, ob plansql.OrderByItem) OrderExpr {
	return OrderExpr{Column: column, Desc: ob.Desc, NullsFirst: ob.NullsFirst}
}

// sortKeyCarried reports whether the Sort's input already emits key.
//
// With a SELECT list, the Project below the Sort narrows the schema to exactly
// its outputs, so the select-list names are the whole of what a sort key can
// resolve against. With `SELECT *` the Sort reads the relation itself, whose
// column set is not known here — a bare column reference is taken at its word
// (that is the pre-existing contract, and the catalog rejects a name that does
// not exist), and anything computed is materialized.
func sortKeyCarried(key string, outputs []string, starOnly bool, ast plansql.Node) bool {
	if starOnly {
		_, isCol := ast.(*plansql.ColRef)
		return isCol || ast == nil
	}
	for _, out := range outputs {
		if namesSameColumn(out, key) {
			return true
		}
	}
	return false
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
// reference, else the expression text.
func selectOutputNames(info *plansql.SelectInfo) []string {
	out := make([]string, 0, len(info.Columns))
	for _, col := range info.Columns {
		if col.Star {
			continue
		}
		switch {
		case col.IsWindow && col.WindowSpec != nil && col.Alias == "":
			out = append(out, col.WindowSpec.Alias)
		case col.Alias != "":
			out = append(out, col.Alias)
		case col.ColumnRef != "":
			out = append(out, col.ColumnRef)
		default:
			out = append(out, cleanExpr(col.Expr))
		}
	}
	return out
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
	text := ast.String()
	for _, gb := range agg.GroupBy {
		if strings.EqualFold(gb, text) {
			return true
		}
	}
	for _, gbe := range agg.GroupByExprs {
		if gbe != nil && strings.EqualFold(gbe.String(), text) {
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
