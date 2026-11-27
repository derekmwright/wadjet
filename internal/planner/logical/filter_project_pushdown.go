// SPDX-License-Identifier: MIT

package logical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #384: pushdownPredicates' Filter-Project swap used to be unconditional. A
// predicate referencing a column the Project COMPUTES (`NULLIF(x, 2) AS rk2`
// ... `WHERE rk2 > 1`) was pushed below the Project without substitution, so
// the filter ran against a schema that does not carry the alias: the
// single-process pipeline errored ("filter column does not exist"), and the
// stage DAG's scan-stage filter matched nothing and silently returned 0 rows.
//
// The classic fix is predicate substitution: rewrite each reference to a
// computed (or renamed) output with the alias's defining expression, then
// push — the substituted predicate names only source columns, so it also
// rides scan pushdown. Where substitution is unsound — aggregate outputs,
// volatile functions, subquery-bearing definitions, or expressions the
// rewriter cannot see through — the predicate DECLINES the push and stays
// above the Project, which is its original, trivially correct position.
// Three-valued logic holds through substitution: the predicate evaluates the
// exact defining expression the Project would have produced, NULLs included.

// projOutput describes one Project output as seen by a predicate above the
// Project: what to substitute for a reference to it when the predicate moves
// below (def), and whether such a move must be declined (unsafe).
type projOutput struct {
	// def is the expression substituted for a ColRef naming this output.
	// nil means the output passes its source column through under the same
	// name, so a reference needs no rewrite.
	def plansql.Node
	// unsafe means a predicate referencing this output must not be pushed
	// below the Project at all.
	unsafe bool
}

// projectSubstitutions builds the output-name → substitution map for a
// Project's projections. blockSwap is splitFilterForProjectPush's own
// context (see projRefs.blockSwap); it is false for every other caller,
// which keeps their resolution bit-for-bit as it was.
func projectSubstitutions(projs []Projection, blockSwap bool) map[string]projOutput {
	outs := make(map[string]projOutput, len(projs))
	for _, p := range projs {
		name := strings.ToLower(p.Alias)
		if name == "" {
			name = bareColumnName(p.Column)
		}
		if name == "" {
			// An expression without an alias has no name a predicate's
			// ColRef can carry; nothing to map.
			continue
		}
		out := classifyProjection(p, name, blockSwap)
		if prev, dup := outs[name]; dup {
			// Two outputs share a name: a reference is ambiguous unless
			// both are the same passthrough.
			if prev.def != nil || prev.unsafe || out.def != nil || out.unsafe {
				outs[name] = projOutput{unsafe: true}
			}
			continue
		}
		outs[name] = out
	}
	return outs
}

// classifyProjection decides what a reference to this projection's output
// becomes below the Project. blockSwap is projectSubstitutions' own context
// argument (see projRefs.blockSwap).
func classifyProjection(p Projection, name string, blockSwap bool) projOutput {
	if p.IsAgg {
		// An aggregate output has no row-wise defining expression to
		// substitute below the Project.
		return projOutput{unsafe: true}
	}
	if p.Column != "" {
		if bareColumnName(p.Column) == name {
			// p.Column carries the parser's BARE ColumnRef (select_parser.go
			// strips the qualifier into a separate field the Projection does
			// not keep); the qualifier, when the source had one, survives
			// only in p.ASTExpr. Within the block-swap context, a
			// passthrough is "the same name below" only when the SOURCE was
			// itself unqualified: a QUALIFIED source (`j.v` published under
			// its own bare name `v`) is not the same name below once this
			// Project draws from more than one relation — a sibling arm can
			// carry an UNSELECTED column of that same bare name, and a bare
			// reference pushed below would bind IT instead of the published
			// source (arc JP #1299 P1: `d.v` over `(SELECT i.id, i.oid, j.v
			// FROM jp_i i JOIN jp_j j ON …) d` bound `i.v`, though only `j.v`
			// was ever published as `v`). Such a source falls through to the
			// ordinary rename logic below. Every OTHER caller (the stage
			// DAG's filter-through-project respelling) keeps its pre-#1299
			// reading unchanged: only the swap's own predicate rewrite knows
			// an unresolved qualifier can only be the block's own alias
			// (projRefs.resolve's blockSwap branch); left unconditional here
			// it would ALSO substitute a qualified passthrough there, for
			// a qualifier that is not necessarily self-referential.
			if !blockSwap {
				return projOutput{} // passthrough: same name below
			}
			if ref := simpleColRef(p.ASTExpr); ref == nil || ref.Table == "" {
				return projOutput{} // passthrough: same UNQUALIFIED name below
			}
		}
		// Rename: substitute a reference to the source column. Prefer the
		// projection's own AST (it may carry a table qualifier); fall back
		// to the Column string.
		if ref := simpleColRef(p.ASTExpr); ref != nil {
			return projOutput{def: ref}
		}
		if q, col, ok := plansql.SplitIdentRef(p.Column); ok {
			return projOutput{def: &plansql.ColRef{Table: q, Column: col}}
		}
		return projOutput{def: &plansql.ColRef{Column: p.Column}}
	}
	// Computed output. Substitution needs the defining AST, and it must be
	// deterministic and self-contained.
	if p.ASTExpr == nil || substitutionUnsafe(p.ASTExpr) {
		return projOutput{unsafe: true}
	}
	if ref := simpleColRef(p.ASTExpr); ref != nil {
		return projOutput{def: ref} // rename spelled without Column
	}
	// Parenthesize so the regenerated Raw string keeps the expression's
	// precedence when a worker re-parses it.
	return projOutput{def: &plansql.ParenNode{Inner: p.ASTExpr}}
}

// bareColumnName returns the lower-cased unqualified name of a column
// reference string ("t.x" → "x").
func bareColumnName(col string) string {
	if col == "" {
		return ""
	}
	if _, name, ok := plansql.SplitIdentRef(col); ok {
		return strings.ToLower(name)
	}
	return strings.ToLower(col)
}

// simpleColRef unwraps parens and returns the node if it is a plain column
// reference, else nil.
func simpleColRef(n plansql.Node) *plansql.ColRef {
	for {
		switch e := n.(type) {
		case *plansql.ColRef:
			return e
		case *plansql.ParenNode:
			n = e.Inner
		default:
			return nil
		}
	}
}

// substitutionUnsafe reports whether substituting this defining expression
// into a predicate could change the query's meaning: volatile functions
// (duplicated evaluation), subqueries and window functions (not row-wise
// self-contained), or any node the walker does not recognize.
func substitutionUnsafe(n plansql.Node) bool {
	if n == nil {
		return false
	}
	switch e := n.(type) {
	case *plansql.ColRef, *plansql.Lit, *plansql.IntervalLit:
		return false
	case *plansql.CmpExpr:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.AndNode:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.OrNode:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.BinaryOp:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Right)
	case *plansql.UnaryOp:
		return substitutionUnsafe(e.Inner)
	case *plansql.NotNode:
		return substitutionUnsafe(e.Inner)
	case *plansql.ParenNode:
		return substitutionUnsafe(e.Inner)
	case *plansql.CastNode:
		return substitutionUnsafe(e.Inner)
	case *plansql.FuncCallNode:
		if expr.IsVolatileFunction(e.Name) {
			return true
		}
		// A set-returning item is a SET, not a row-wise value: substituting
		// its definition below the Project that expands it filters the
		// ARRAY (a field of it reads NULL, a comparison fails to plan)
		// instead of each element (arc PC).
		if isSetReturningName(e.Name) {
			return true
		}
		for _, arg := range e.Args {
			if substitutionUnsafe(arg) {
				return true
			}
		}
		return false
	case *plansql.InExpr:
		if substitutionUnsafe(e.Left) {
			return true
		}
		for _, v := range e.Values {
			if substitutionUnsafe(v) {
				return true
			}
		}
		return false
	case *plansql.BetweenExpr:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Low) || substitutionUnsafe(e.High)
	case *plansql.LikeExpr:
		return substitutionUnsafe(e.Left) || substitutionUnsafe(e.Pattern)
	case *plansql.IsExpr:
		return substitutionUnsafe(e.Left)
	case *plansql.CaseNode:
		if substitutionUnsafe(e.Subject) || substitutionUnsafe(e.Else) {
			return true
		}
		for _, w := range e.Whens {
			if substitutionUnsafe(w.Cond) || substitutionUnsafe(w.Result) {
				return true
			}
		}
		return false
	case *plansql.ArrayLitNode:
		for _, el := range e.Elements {
			if substitutionUnsafe(el) {
				return true
			}
		}
		return false
	case *plansql.TupleNode:
		for _, el := range e.Elements {
			if substitutionUnsafe(el) {
				return true
			}
		}
		return false
	default:
		// SubqueryNode, ExistsNode, AnyAllExpr, WindowFuncNode, StarNode,
		// and anything added later: not row-wise self-contained (or not
		// understood) — do not substitute it.
		return true
	}
}

// volatileFuncs lists the builtin functions whose result differs across
// evaluations, so duplicating them via substitution would change the query's
// meaning. The set is the function registry's mark (expr.VolatileBuiltins),
// the one the CTE rule reads too (plansql.CTEDef.EvaluatedOnce).
var volatileFuncs = func() map[string]bool {
	m := map[string]bool{}
	for _, name := range expr.VolatileBuiltins() {
		m[name] = true
	}
	return m
}()

// projRefs resolves references against a Project's output AND its scope.
// A bare-name map suffices directly above a Project, but not across join arms.
// names includes the CTE name, derived aliases and scan aliases/table names;
// a qualifier naming that scope reads its OUTPUT, never a sibling's definition.
// A qualifier naming an output is a candidate ROW field path (ADR-0022):
// substitute the ROW column's qualifier while preserving the field (rw.b → c_row.b).
// Leave qualifiers claimed by neither scope nor output exactly as written.
// A bare name also emitted by a sibling must not be assigned by guessing.
// See docs/internals/project-reference-scope.md for the design.
type projRefs struct {
	outs  map[string]projOutput
	names map[string]bool
	// subst, when non-nil, collects the lowercased OUTPUT names this Project
	// substituted away. ResolveFilterThroughProjects hands them to the stage
	// DAG, which cannot otherwise tell which names the query's own spelling
	// used — and choosing between the two spellings later needs to know what
	// to look for on the producing fragment (#656).
	subst *[]string
	// overAgg marks a Project whose INPUT is an aggregate's OUTPUT, and
	// groupBys are that aggregate's GROUP BY keys. Below such a Project the
	// rows are one per GROUP, so an output's defining expression is
	// evaluable there only when the aggregate itself emits a column of that
	// name: `g + 1 AS gk` over `GROUP BY g + 1` substitutes to `(g + 1) > 3`,
	// an expression over `g`, which the aggregate's output does not carry.
	// Both paths then answered ZERO rows for a query PostgreSQL answers
	// (#656 shape f, through a derived table).
	overAgg  bool
	groupBys map[string]bool
	// published are the names that aggregate publishes a group key under in
	// place of its source text (Node.GroupByPublish — a LATERAL's minted
	// key slot). The output of that name IS the stream's column, so it is a
	// passthrough: substituting its source (`s.__key_0` → `i.k`) named a
	// column the aggregate never emits, and a lifted equality over a grouped
	// LATERAL body read the OUTER `k` through the qualifier strip, or
	// nothing — every row or no row on the stage DAG (arc JP).
	published map[string]bool
	// rowFields maps a lower-cased column name BELOW this Project to the
	// fields it declares, when it is a ROW. It is what tells a candidate ROW
	// FIELD PATH from an ordinary qualified reference whose qualifier happens
	// to name one of this Project's outputs (ADR-0022, #769).
	rowFields map[string][]parquet.Column
	// blockSwap marks a projRefs built for pushdownPredicates' Filter-Project
	// swap (splitFilterForProjectPush) — the ONLY context where a qualifier
	// this Project cannot otherwise place is guaranteed to be the alias the
	// ENCLOSING query gives this whole block (a derived table's or a
	// LATERAL arm's own name, never visible to a reference written inside
	// the block, and therefore never one this Project's own inScope/output
	// checks can match). resolve's final fallback and classifyProjection's
	// passthrough test read it; every OTHER caller (ResolveFilterThroughProjects'
	// stage-DAG walk) leaves it false and keeps its pre-#1299 reading, since an
	// unresolved qualifier reaching THAT walk is not guaranteed to be self
	// (arc JP #1299 P1; N3 tracks the DAG's own, separate defect on this shape).
	blockSwap bool
}

func newProjRefs(n *Node, blockSwap bool) projRefs {
	return projRefs{
		outs:      projectSubstitutions(n.Projections, blockSwap),
		names:     nodeScopeNames(n),
		overAgg:   readsAnAggregate(n),
		groupBys:  aggregateGroupKeys(n),
		published: aggregatePublishedKeys(n),
		rowFields: subtreeRowFields(n),
		blockSwap: blockSwap,
	}
}

// subtreeRowFields collects the declared ROW fields of every scan below n
// (Node.ScanColFields, populated by physical.AnnotateScanColumns), keyed by
// the column's lower-cased name. Empty when nothing below is annotated, which
// is the conservative answer: no reference is then read as a field path here
// and the resolution below keeps the pre-#769 order.
func subtreeRowFields(n *Node) map[string][]parquet.Column {
	var out map[string][]parquet.Column
	var walk func(*Node)
	walk = func(cur *Node) {
		if cur == nil {
			return
		}
		for name, fields := range cur.ScanColFields {
			if len(fields) == 0 {
				continue
			}
			if out == nil {
				out = make(map[string][]parquet.Column, len(cur.ScanColFields))
			}
			out[strings.ToLower(name)] = fields
		}
		for _, c := range cur.Children {
			walk(c)
		}
	}
	walk(n)
	return out
}

// declaresField reports whether the container output's DEFINITION names a ROW
// column below this Project that declares `field`. It is the guard that keeps
// the field-path arm off an ordinary qualified reference: `rw.b` over
// `SELECT id AS rw, id AS b` names no container, so the bare-column arm
// answers for it exactly as before.
func (p projRefs) declaresField(o projOutput, container, field string) bool {
	name := strings.ToLower(container)
	if o.def != nil {
		ref := simpleColRef(o.def)
		if ref == nil || ref.Table != "" {
			return false
		}
		name = strings.ToLower(ref.Column)
	}
	for _, f := range p.rowFields[name] {
		if strings.EqualFold(f.Name, field) {
			return true
		}
	}
	return false
}

// readsAnAggregate reports whether this Project's input is an Aggregate's
// OUTPUT — one row per group, columns named by the group keys and the
// aggregates.
//
// AggregateOverGroupRows, not AggregateBelowProject: this asks about the ROWS,
// and every wrapper AggScopePreservingWrapper names leaves them one per group.
// AggregateBelowProject asks which aggregate STAGE a Project sits on, which is
// a narrower question — see AggregateOverGroupRows' comment and #774.
func readsAnAggregate(n *Node) bool {
	return AggregateOverGroupRows(n) != nil
}

// AggregateBelowProject returns the Aggregate whose OUTPUT rows this Project
// reads AND whose stage it sits directly on, or nil when it reads something
// else.
//
// A HAVING is a Filter between the two, and it changes nothing about the
// rows: it drops whole groups, it does not restore the aggregate's input
// columns. Stopping at it is what made a CTE with both a HAVING and an outer
// WHERE on a computed group key answer zero rows (#656 shape f with a
// HAVING), so the walk descends through it.
//
// It descends through a HAVING and NOTHING ELSE, which is deliberate and is
// why it is not AggScopePreservingWrapper's fourth reader. Both callers —
// dagplan.aggregateProjectionTarget and physical.aggregateGroupKeyName — go
// on to map this Project's SELECT list onto the aggregate's own STAGE, and a
// Sort, a LIMIT or a WINDOW between the two emits a stage of ITS own that the
// projection would then be carried past. The question "are these rows one per
// group", which has no such constraint, is AggregateOverGroupRows'.
func AggregateBelowProject(n *Node) *Node {
	if n == nil || n.Type != NodeProject {
		return nil
	}
	for c := n; c != nil && len(c.Children) == 1; c = c.Children[0] {
		switch child := c.Children[0]; {
		case child == nil:
			return nil
		case child.Type == NodeAggregate:
			return child
		case child.Type == NodeFilter:
			// HAVING: keep descending.
		default:
			return nil
		}
	}
	return nil
}

// aggregateGroupKeys is the GROUP BY key list of the Aggregate this Project
// reads, lowercased and trimmed; nil when it reads something else.
func aggregateGroupKeys(n *Node) map[string]bool {
	agg := AggregateOverGroupRows(n)
	if agg == nil {
		return nil
	}
	out := make(map[string]bool, len(agg.GroupBy))
	for _, k := range agg.GroupBy {
		out[strings.ToLower(strings.TrimSpace(k))] = true
	}
	return out
}

// aggregatePublishedKeys is the set of names the aggregate under n publishes a
// group key under instead of the key's own text (Node.GroupByPublish).
func aggregatePublishedKeys(n *Node) map[string]bool {
	agg := AggregateOverGroupRows(n)
	if agg == nil {
		return nil
	}
	var out map[string]bool
	for _, pub := range agg.GroupByPublish {
		if pub == "" {
			continue
		}
		if out == nil {
			out = map[string]bool{}
		}
		out[strings.ToLower(pub)] = true
	}
	return out
}

// withSubstitutionLog returns a copy of p that records every output name it
// substitutes away into names.
func (p projRefs) withSubstitutionLog(names *[]string) projRefs {
	p.subst = names
	return p
}

// record notes one substituted output name, de-duplicated and lowercased.
func (p projRefs) record(name string) {
	if p.subst == nil || name == "" {
		return
	}
	low := strings.ToLower(name)
	for _, s := range *p.subst {
		if s == low {
			return
		}
	}
	*p.subst = append(*p.subst, low)
}

// inScope reports whether a qualifier names the relation scope this Project
// projects out of.
func (p projRefs) inScope(qualifier string) bool {
	return p.names[strings.ToLower(qualifier)]
}

// touches reports whether a BARE name refers to an output this Project
// renames, computes, or cannot express — the test the opaque-node arm of
// substituteColRefs applies, where it cannot rewrite in place.
func (p projRefs) touches(bare string) bool {
	o, ok := p.outs[bare]
	return ok && (o.def != nil || o.unsafe)
}

// resolve returns a replacement, nil to leave the reference alone, or ok=false
// to decline the WHOLE rewrite. Follow runtime expr.ResolveColumnRef's order:
// exact spelling, then a ROW column DECLARING the field, then bare qualifier stripping
// (ADR-0022 §1). Changing this order can resolve a different column.
// declaresField rejects non-ROW outputs and undeclared fields, letting relation references
// fall through to stripping. Unparenthesized ROW-field spelling is the deliberate
// PostgreSQL superset recorded in ADR-0012 (#769).
// See docs/internals/project-column-resolution-order.md for the design.
func (p projRefs) resolve(ref *plansql.ColRef) (plansql.Node, bool) {
	if ref.Table == "" {
		o, ok := p.outs[strings.ToLower(ref.Column)]
		if !ok {
			return nil, true
		}
		return p.apply(o, ref.Column, "")
	}
	// A projection that aliases the QUALIFIED spelling itself owns the name
	// outright (`n1.n_name AS "n1.n_name"`).
	if o, ok := p.outs[strings.ToLower(ref.String())]; ok {
		return p.apply(o, ref.String(), "")
	}
	// Step 2 applies when the qualifier is one this Project can answer for:
	// its own relation scope, or one of its outputs (the ROW-container
	// candidate). A qualifier that is neither names a sibling join arm or an
	// outer scope, and nothing here may touch it.
	container, isContainer := p.outs[strings.ToLower(ref.Table)]
	if !p.inScope(ref.Table) && !isContainer {
		// The qualifier names neither a relation this Project draws from nor
		// one of its own outputs used as a ROW container. Within the
		// block-swap context (p.blockSwap) the one thing it CAN still mean
		// is the alias the ENCLOSING query gives this whole block (a
		// derived table's or a LATERAL arm's own name): that name is never
		// visible to a reference written INSIDE the block, so it can never
		// match inScope or isContainer here. Resolve the bare column
		// against this Project's own published list — the same lookup an
		// unqualified reference takes — instead of leaving the
		// now-meaningless qualifier in place for a later, scope-wide
		// qualifier strip to bind the WRONG relation (arc JP #1299 P1).
		// Every OTHER caller keeps the original "leave it" reading: an
		// unresolved qualifier reaching the stage DAG's own filter-through-
		// project walk is not guaranteed to be self (N3's separate,
		// pre-existing DAG defect on this same shape).
		if p.blockSwap {
			if o, ok := p.outs[strings.ToLower(ref.Column)]; ok {
				return p.apply(o, ref.Column, "")
			}
		}
		return nil, true
	}
	if isContainer && p.declaresField(container, ref.Table, ref.Column) {
		return p.apply(container, ref.Table, ref.Column)
	}
	if o, ok := p.outs[strings.ToLower(ref.Column)]; ok {
		return p.apply(o, ref.Column, "")
	}
	if isContainer {
		return p.apply(container, ref.Table, ref.Column)
	}
	return nil, true
}

// apply turns one matched output into the replacement text. field is the ROW
// field to keep, or "" for a plain column reference.
func (p projRefs) apply(o projOutput, name, field string) (plansql.Node, bool) {
	if o.unsafe {
		return nil, false
	}
	if o.def == nil {
		return nil, true // passthrough: the input carries the same name
	}
	if p.overAgg && field == "" && p.published[strings.ToLower(name)] {
		return nil, true // the aggregate emits this very name (see published)
	}
	// Below an aggregate's output the rows are one per GROUP. A definition
	// that is not a bare column of that output — a computed group key, an
	// expression over one — evaluates against columns the group rows do not
	// have, so it must not be substituted in either direction: the swap keeps
	// the Filter above the Project, and the DAG keeps the alias and evaluates
	// it above the projection that materializes it.
	if p.overAgg && !p.evaluableOverGroups(o.def) {
		return nil, false
	}
	if field == "" {
		p.record(name)
		return o.def, true
	}
	// The qualifier is substituted and the field kept, so the definition has
	// to BE a column reference — a computed ROW has no spelling a field path
	// can hang off, and a qualified one would need three parts.
	ref := simpleColRef(o.def)
	if ref == nil || ref.Table != "" {
		return nil, false
	}
	p.record(name)
	return &plansql.ColRef{Table: ref.Column, Column: field}, true
}

// evaluableOverGroups reports whether a projection's definition can be
// evaluated against an aggregate's OUTPUT rows: a bare column reference the
// aggregate emits (a group key spelled as a column, or an aggregate's own
// output), and nothing else.
func (p projRefs) evaluableOverGroups(def plansql.Node) bool {
	ref := simpleColRef(def)
	if ref == nil {
		return false
	}
	return p.groupBys[strings.ToLower(strings.TrimSpace(ref.String()))] ||
		p.groupBys[strings.ToLower(ref.Column)]
}

// nodeScopeNames is the set of relation names a reference into this subtree
// may be QUALIFIED by: each scan's alias or table name, the derived-table
// scopes stamped on it, and any CTE the subtree is the body of. It is the
// logical-side twin of physical.subtreeNamesRelation, which asks the same
// question one name at a time for the DAG's other alias resolvers.
func nodeScopeNames(n *Node) map[string]bool {
	out := map[string]bool{}
	var walk func(*Node)
	walk = func(n *Node) {
		if n == nil {
			return
		}
		if n.CTEName != "" {
			out[strings.ToLower(n.CTEName)] = true
		}
		if n.CTERefAlias != "" {
			out[strings.ToLower(n.CTERefAlias)] = true
		}
		if n.Type == NodeScan {
			if n.TableAlias != "" {
				out[strings.ToLower(n.TableAlias)] = true
			}
			if n.TableName != "" {
				out[strings.ToLower(n.TableName)] = true
			}
			for _, d := range n.DerivedAliases {
				out[strings.ToLower(d)] = true
			}
		}
		for _, c := range n.Children {
			walk(c)
		}
	}
	walk(n)
	return out
}

// splitFilterForProjectPush partitions a Filter's predicates for the
// Filter-Project swap: `pushed` may cross below the Project (rewritten where
// they referenced renamed or computed outputs), `kept` must stay above it.
//
// A Project that expands a SET (ProjectsASet) keeps every predicate: below
// it a set-returning output is still the ARRAY, so a predicate over it —
// however it is spelled, `u`, `r.u` or `(r.k).x` — would filter arrays, not
// elements (arc PC: `(r.k).x = 3` over `_pg_expandarray` answered
// zero rows, `r.u = 3` failed to plan).
func splitFilterForProjectPush(preds []Predicate, project *Node) (pushed, kept []Predicate) {
	if ProjectsASet(project) {
		return nil, preds
	}
	p := newProjRefs(project, true) // blockSwap: an unresolved qualifier here is the block's own alias
	for _, pred := range preds {
		newAST, ok := rewritePredThroughProject(pred, p)
		if !ok {
			kept = append(kept, pred)
			continue
		}
		if newAST != nil {
			pred.ASTExpr = newAST
			pred.Raw = newAST.String()
			// The simple-form fields, if set, named the alias; the AST is
			// now the authority.
			pred.Column, pred.Op, pred.Value = "", "", nil
		}
		pushed = append(pushed, pred)
	}
	return pushed, kept
}

// rewritePredThroughProject decides one predicate's fate for the swap.
// Returns (nil, true) to push unchanged, (ast, true) to push with the
// rewritten expression, (nil, false) to decline the push.
func rewritePredThroughProject(pred Predicate, p projRefs) (plansql.Node, bool) {
	// A simple-form predicate (Column/Op/Value) naming a renamed or
	// computed output has no AST to rewrite consistently: decline.
	if col := bareColumnName(pred.Column); col != "" && p.touches(col) {
		return nil, false
	}
	if pred.ASTExpr == nil {
		// Nothing to analyze — the pre-#384 behavior (push unchanged) only
		// arises for predicates that never named a Project alias.
		return nil, true
	}
	return rewriteASTThroughProject(pred.ASTExpr, p)
}

// rewriteASTThroughProject is rewritePredThroughProject's expression half:
// (nil, true) means the expression names no output this Project renames or
// computes, (ast, true) is the rewritten form, (nil, false) declines.
//
// "Names nothing of ours" is read off the rewrite itself: substituteColRefs
// is copy-on-write, so an expression it did not touch comes back as the same
// node.
func rewriteASTThroughProject(ast plansql.Node, p projRefs) (plansql.Node, bool) {
	newAST, ok := substituteColRefs(ast, p)
	if !ok {
		return nil, false
	}
	if newAST == ast {
		return nil, true
	}
	return newAST, true
}

// ResolveFilterThroughProjects changes spelling, never predicate placement (#653).
// Substitute exact defining expressions, NULLs included, even across a CTE Project;
// CTE materialization forbids the single-process swap, and a JOIN child is not swapped.
// Walk joins one arm at a time with that arm's scope; leave other-arm references alone.
// Stop at unexpressible Project outputs (aggregates, volatile functions), Sort or LIMIT:
// their stages carry published names, not the source spelling below the Project.
// physical.validate owns ambiguous bare-name errors (42702) on both paths.
// Return nil AST and ok=false if unchanged; aliases are substituted OUTPUT names,
// lowercased, for dagplan.resolveFilterAliasSpelling's alias/source choice (#656).
// See docs/internals/dag-filter-project-respelling.md for the design.
//
// qualifyBuild, when non-nil, names how EVERY join the walk crosses qualifies
// its build side: qualifyBuild(join) answers that join's qualifier, or nil
// for a join that qualifies nothing. Each column reference a definition
// substituted from a join's BUILD arm brings in is re-qualified by
// q(table, column), q being that join's qualifier and table the qualifier the
// definition wrote ("" when bare); an answer of "" or of table itself leaves
// the reference as it stands. The definition's names were bound in the arm's
// scope: on the join's stream a bare name binds the PROBE first, and a
// qualifier from inside the arm (the table's own name, `upper(t.s)`) is one
// the stream may not carry, so left as written they read the other arm's
// column of that name (#1678 line j). The rule is the same at every depth: a
// derived table that is a LOWER join's build, read by a filter an upper join
// carries, is qualified by the name the lower join's stream gave it, which
// every stream above it carries on (#1678 line j, round 3).
func ResolveFilterThroughProjects(pred Predicate, child *Node, qualifyBuild func(join *Node) func(table, column string) string) (ast plansql.Node, aliases []string, ok bool) {
	if pred.ASTExpr == nil {
		return nil, nil, false
	}
	var subst []string
	var bq *buildQualifying
	if qualifyBuild != nil {
		bq = &buildQualifying{qualifyFor: qualifyBuild, settled: map[*plansql.ColRef]bool{}}
	}
	ast, changed := resolveFilterInSubtree(pred.ASTExpr, child, false, &subst, bq)
	if !changed {
		return nil, nil, false
	}
	return ast, subst, true
}

// buildQualifying is the state of ResolveFilterThroughProjects' build-arm
// qualification. held is one set per join whose BUILD arm the walk is inside,
// outermost first: the references that join found present before its build
// walk, and so are NOT its build arm's to qualify. A join deeper in that arm
// re-makes every reference as a fresh node before its own build walk; carry
// keeps each enclosing set's membership on the copy, so the enclosing join
// still tells the query's own references (and its probe arm's) from the ones
// its build arm brought in. Without it, `upper(a.s)` written by the query
// above `a JOIN (x JOIN (SELECT upper(s) AS k …) y) b` was taken for one of
// b's and re-qualified into b's stream: `upper(y.s) = (upper(y.s))`.
//
// settled holds the references a join has already qualified for its build
// arm. The join that qualified one is the one whose stream first named the
// column, and every stream above it carries that name on: a join further up
// whose build arm holds that join leaves the reference as it stands. Its own
// qualifier would only know its build's scans by bare name, and answered a
// column the arm's probe also carries by the probe's scan:
// `(SELECT x.id, y.k FROM t x JOIN (SELECT upper(s) AS k …) y …) b` arrived
// as `upper(x.s)`, x's value for y's.
type buildQualifying struct {
	qualifyFor func(join *Node) func(table, column string) string
	held       []map[*plansql.ColRef]bool
	settled    map[*plansql.ColRef]bool
}

// carry records copy as a member of every enclosing set orig is a member of,
// and as settled when orig is.
func (bq *buildQualifying) carry(orig, copy *plansql.ColRef) {
	for _, set := range bq.held {
		if set[orig] {
			set[copy] = true
		}
	}
	if bq.settled[orig] {
		bq.settled[copy] = true
	}
}

// resolveFilterInSubtree walks the producing subtree applying each rename it
// meets, and returns the expression as the stage carrying the filter will see
// it. A subtree it cannot model STOPS the walk with whatever has been settled
// so far, which is the spelling the predicate had on entry to that subtree.
// bq, when non-nil, qualifies each join's build-arm definitions, at every
// depth (see ResolveFilterThroughProjects).
func resolveFilterInSubtree(ast plansql.Node, n *Node, changed bool, subst *[]string, bq *buildQualifying) (plansql.Node, bool) {
	for ; n != nil; n = n.Children[0] {
		switch n.Type {
		case NodeJoin:
			// A join emits BOTH arms' columns, so a predicate above it can
			// name either. Each arm is resolved with its OWN scope names, so
			// a qualified reference only rewrites against the arm it names.
			if len(n.Children) != 2 {
				return ast, changed
			}
			// The probe arm is walked under the same rule: a join inside it
			// qualifies what ITS build arm brings in, by the name its own
			// stream gives those columns, and the streams above carry that
			// name on.
			ast, changed = resolveFilterInSubtree(ast, n.Children[0], changed, subst, bq)
			var q func(table, column string) string
			if bq != nil {
				q = bq.qualifyFor(n)
			}
			if q == nil {
				return resolveFilterInSubtree(ast, n.Children[1], changed, subst, bq)
			}
			// Every reference present now is the query's own or the probe
			// arm's. They are re-made as fresh nodes first: a definition the
			// build arm's walk splices in is the logical plan's own node, and
			// the probe arm's walk may already have spliced in the very same
			// node (a CTE read on both sides shares its definitions), so only
			// nodes the build walk cannot produce tell the arms apart. A
			// reference the build walk leaves that is not one of these fresh
			// nodes came from a build-arm definition.
			ast = rewriteColRefs(ast, func(r *plansql.ColRef) *plansql.ColRef {
				c := *r
				bq.carry(r, &c)
				return &c
			})
			before := map[*plansql.ColRef]bool{}
			plansql.WalkColRefs(ast, func(r *plansql.ColRef) { before[r] = true })
			// A join inside the build arm qualifies first; this join's
			// qualifier then has the last word on every reference the arm
			// brought in, because this join's stream is where it is read.
			bq.held = append(bq.held, before)
			built, bchanged := resolveFilterInSubtree(ast, n.Children[1], changed, subst, bq)
			bq.held = bq.held[:len(bq.held)-1]
			if built != ast {
				built = qualifyIntroducedRefs(built, before, bq.settled, q)
			}
			return built, bchanged
		case NodeDistinct, NodeSort, NodeLimit, NodeWindow:
			if WindowShadowedInput(n) != nil {
				// The window's input carries the derived table's declared
				// columns under their own names (WindowShadowedInput).
				return ast, changed
			}
			// None of the four renames anything, so the walk continues
			// through them. A WINDOW forwards every input column and appends
			// its own outputs, so a predicate above one still names the
			// Projects below it — stopping here left `WHERE gk = 1` above a
			// window over `SELECT g AS gk` naming a column the window's
			// input does not carry, UNKNOWN on every row, zero rows in
			// silence (the #653 class, one operator over).
			//
			// Sort and LIMIT DO emit stages, and a predicate
			// re-spelled past one names the SOURCE column their stream
			// carries rather than the alias the query wrote — which is right
			// exactly when no pass materialized the alias onto the producing
			// fragment. That is not knowable here, so both spellings travel
			// to the stage and dagplan.resolveFilterAliasSpelling picks
			// once attachScanSelectProjections has decided (#656).
		case NodeProject:
			if WindowShadowedBlock(n) {
				// A relation of its own: its items keep the block's names,
				// which its stage publishes (WindowShadowedBlock).
				return ast, changed
			}
			newAST, ok := rewriteASTThroughProject(ast, newProjRefs(n, false).withSubstitutionLog(subst))
			if !ok {
				return ast, changed
			}
			if newAST != nil {
				ast, changed = newAST, true
			}
		default:
			// Everything else emits a stage under names the predicate can
			// already see.
			return ast, changed
		}
		if len(n.Children) != 1 {
			return ast, changed
		}
	}
	return ast, changed
}

// qualifyIntroducedRefs returns ast with every column reference that is not in
// keep re-qualified by qualify(table, column), copy-on-write: the definitions
// a substitution spliced in are the logical plan's own nodes, shared with the
// single-process path, and are never written to. A reference qualify answers
// "" or its own qualifier for is left as it stands.
//
// A reference in settled was already qualified by a join DEEPER in the arm
// (buildQualifying.settled) and is left as it stands; every reference this
// call decides is added to settled for the joins above it.
func qualifyIntroducedRefs(ast plansql.Node, keep, settled map[*plansql.ColRef]bool, qualify func(table, column string) string) plansql.Node {
	return rewriteColRefs(ast, func(r *plansql.ColRef) *plansql.ColRef {
		if keep[r] || settled[r] {
			return r
		}
		q := qualify(r.Table, r.Column)
		if q == "" || strings.EqualFold(q, r.Table) {
			settled[r] = true
			return r
		}
		c := *r
		c.Table = q
		settled[&c] = true
		return &c
	})
}

// rewriteColRefs returns ast with every column reference of its own block
// replaced by f's answer for it, copy-on-write as plansql.RewriteExpr is,
// except that a call keeps every field RewriteExpr would drop.
func rewriteColRefs(ast plansql.Node, f func(*plansql.ColRef) *plansql.ColRef) plansql.Node {
	var fn func(plansql.Node) (plansql.Node, bool)
	fn = func(n plansql.Node) (plansql.Node, bool) {
		switch e := n.(type) {
		case *plansql.ColRef:
			return f(e), true
		case *plansql.FuncCallNode:
			// RewriteExpr rebuilds a call from its name and arguments only;
			// keep every other field (a ROW field access renders through
			// OutputLabel).
			c := *e
			c.Args = make([]plansql.Node, len(e.Args))
			for i, a := range e.Args {
				c.Args[i] = plansql.RewriteExpr(a, fn)
			}
			return &c, true
		}
		return nil, false
	}
	return plansql.RewriteExpr(ast, fn)
}

// substituteColRefs returns expr with every column reference to a renamed or
// computed Project output replaced by its definition (copy-on-write: shared,
// unchanged subtrees are reused, matching ReplaceAllAggregates). ok=false
// means the rewrite could not be done soundly and the caller must decline
// the push.
func substituteColRefs(n plansql.Node, p projRefs) (plansql.Node, bool) {
	if n == nil {
		return nil, true
	}
	switch e := n.(type) {
	case *plansql.ColRef:
		rep, ok := p.resolve(e)
		if !ok {
			return nil, false
		}
		if rep == nil {
			return n, true
		}
		return rep, true
	case *plansql.Lit, *plansql.IntervalLit:
		return n, true
	case *plansql.CmpExpr:
		l, lok := substituteColRefs(e.Left, p)
		r, rok := substituteColRefs(e.Right, p)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.CmpExpr{Left: l, Op: e.Op, Right: r}, true
	case *plansql.AndNode:
		l, lok := substituteColRefs(e.Left, p)
		r, rok := substituteColRefs(e.Right, p)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.AndNode{Left: l, Right: r}, true
	case *plansql.OrNode:
		l, lok := substituteColRefs(e.Left, p)
		r, rok := substituteColRefs(e.Right, p)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.OrNode{Left: l, Right: r}, true
	case *plansql.BinaryOp:
		l, lok := substituteColRefs(e.Left, p)
		r, rok := substituteColRefs(e.Right, p)
		if !lok || !rok {
			return nil, false
		}
		if l == e.Left && r == e.Right {
			return n, true
		}
		return &plansql.BinaryOp{Left: l, Op: e.Op, Right: r}, true
	case *plansql.UnaryOp:
		in, ok := substituteColRefs(e.Inner, p)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.UnaryOp{Op: e.Op, Inner: in}, true
	case *plansql.NotNode:
		in, ok := substituteColRefs(e.Inner, p)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.NotNode{Inner: in}, true
	case *plansql.ParenNode:
		in, ok := substituteColRefs(e.Inner, p)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.ParenNode{Inner: in}, true
	case *plansql.CastNode:
		in, ok := substituteColRefs(e.Inner, p)
		if !ok {
			return nil, false
		}
		if in == e.Inner {
			return n, true
		}
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName, Column: e.Column, Answer: e.Answer}, true
	case *plansql.FuncCallNode:
		newArgs := make([]plansql.Node, len(e.Args))
		changed := false
		for i, arg := range e.Args {
			na, ok := substituteColRefs(arg, p)
			if !ok {
				return nil, false
			}
			newArgs[i] = na
			if na != arg {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.FuncCallNode{Name: e.Name, Args: newArgs, Distinct: e.Distinct, Star: e.Star}, true
	case *plansql.InExpr:
		l, lok := substituteColRefs(e.Left, p)
		if !lok {
			return nil, false
		}
		newVals := make([]plansql.Node, len(e.Values))
		changed := l != e.Left
		for i, v := range e.Values {
			nv, ok := substituteColRefs(v, p)
			if !ok {
				return nil, false
			}
			newVals[i] = nv
			if nv != v {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.InExpr{Left: l, Not: e.Not, Values: newVals}, true
	case *plansql.BetweenExpr:
		l, lok := substituteColRefs(e.Left, p)
		lo, look := substituteColRefs(e.Low, p)
		hi, hok := substituteColRefs(e.High, p)
		if !lok || !look || !hok {
			return nil, false
		}
		if l == e.Left && lo == e.Low && hi == e.High {
			return n, true
		}
		return &plansql.BetweenExpr{Left: l, Not: e.Not, Low: lo, High: hi}, true
	case *plansql.LikeExpr:
		l, lok := substituteColRefs(e.Left, p)
		p, pok := substituteColRefs(e.Pattern, p)
		if !lok || !pok {
			return nil, false
		}
		if l == e.Left && p == e.Pattern {
			return n, true
		}
		return &plansql.LikeExpr{Left: l, Not: e.Not, Pattern: p}, true
	case *plansql.IsExpr:
		l, ok := substituteColRefs(e.Left, p)
		if !ok {
			return nil, false
		}
		if l == e.Left {
			return n, true
		}
		return &plansql.IsExpr{Left: l, Not: e.Not, Check: e.Check}, true
	case *plansql.CaseNode:
		subj, sok := substituteColRefs(e.Subject, p)
		els, eok := substituteColRefs(e.Else, p)
		if !sok || !eok {
			return nil, false
		}
		changed := subj != e.Subject || els != e.Else
		newWhens := make([]plansql.WhenClause, len(e.Whens))
		for i, w := range e.Whens {
			cond, cok := substituteColRefs(w.Cond, p)
			res, rok := substituteColRefs(w.Result, p)
			if !cok || !rok {
				return nil, false
			}
			newWhens[i] = plansql.WhenClause{Cond: cond, Result: res}
			if cond != w.Cond || res != w.Result {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.CaseNode{Subject: subj, Whens: newWhens, Else: els}, true
	case *plansql.ArrayLitNode:
		newEls := make([]plansql.Node, len(e.Elements))
		changed := false
		for i, el := range e.Elements {
			ne, ok := substituteColRefs(el, p)
			if !ok {
				return nil, false
			}
			newEls[i] = ne
			if ne != el {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.ArrayLitNode{Elements: newEls}, true
	case *plansql.TupleNode:
		newEls := make([]plansql.Node, len(e.Elements))
		changed := false
		for i, el := range e.Elements {
			ne, ok := substituteColRefs(el, p)
			if !ok {
				return nil, false
			}
			newEls[i] = ne
			if ne != el {
				changed = true
			}
		}
		if !changed {
			return n, true
		}
		return &plansql.TupleNode{Elements: newEls}, true
	default:
		// Opaque to the rewriter (subqueries, EXISTS, window functions,
		// future node types): sound to leave in place only if it names no
		// renamed, computed, or unsafe output — collectASTColumnRefs sees
		// into subquery outer references, so correlation on an alias is
		// caught here.
		refs := make(map[string]bool)
		collectASTColumnRefs(n, refs)
		for r := range refs {
			if strings.Contains(r, ".") {
				continue
			}
			if p.touches(r) {
				return nil, false
			}
		}
		return n, true
	}
}
