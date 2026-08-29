package logical

import (
	"strings"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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
// Project's projections.
func projectSubstitutions(projs []Projection) map[string]projOutput {
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
		out := classifyProjection(p, name)
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
// becomes below the Project.
func classifyProjection(p Projection, name string) projOutput {
	if p.IsAgg {
		// An aggregate output has no row-wise defining expression to
		// substitute below the Project.
		return projOutput{unsafe: true}
	}
	if p.Column != "" {
		if bareColumnName(p.Column) == name {
			return projOutput{} // passthrough: same name below
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
		if volatileFuncs[strings.ToLower(e.Name)] {
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

// volatileFuncs lists functions whose result differs across evaluations, so
// duplicating them via substitution would change the query's meaning.
var volatileFuncs = map[string]bool{
	"rand":            true,
	"random":          true,
	"uuid":            true,
	"gen_random_uuid": true,
}

// projRefs is one Project's answer to "what does this column reference
// mean?", and it is deliberately more than the output map.
//
// The map alone matches on the BARE column name and ignores the qualifier,
// which is right where a Filter sits directly on a Project (every reference
// it can carry names that Project's output) and WRONG under a join, where the
// walk applies each arm's map to the whole predicate in turn. A reference
// qualified to the OTHER arm was rewritten with this arm's definition:
// `… c JOIN typemx_dim d ON c.gg = d.k WHERE d.k > 3 OR c.gg > 100` over
// `SELECT id AS k, g AS gg` became `id > 3 or g > 100` — 4612 rows where
// PostgreSQL answers 1978, a silent wrong answer replacing the obviously
// wrong 0 that came before. So the qualifier decides:
//
//   - names is the set of relation names this Project's scope answers to
//     (its CTE name, the derived alias stamped on the scans below it, each
//     scan's own alias or table name). A reference qualified by one of them
//     names this Project's OUTPUT column.
//   - a qualifier this scope does not answer to belongs to a sibling arm or
//     an outer scope, and is left exactly as written.
//   - a qualifier that names one of this Project's OUTPUTS is a ROW FIELD
//     PATH, not a table reference (ADR-0022): `rw.b` over `c_row AS rw` is
//     field `b` of the renamed ROW column, so the QUALIFIER is substituted
//     and the field kept — `c_row.b`. Looking `b` up as a column, which the
//     bare-name map does, finds nothing and leaves a name no stage emits.
//   - ambiguous, when set, reports a bare name the SIBLING join arm can also
//     emit. Nothing in the predicate's text says which arm is meant, so the
//     rewrite refuses rather than picking one.
//
// projRefs is one Project's answer to "what does this column reference
// mean?", and it is deliberately more than the output map.
//
// The map alone matches on the BARE column name and ignores the qualifier,
// which is right where a Filter sits directly on a Project (every reference
// it can carry names that Project's output) and WRONG under a join, where the
// walk applies each arm's map to the whole predicate in turn. A reference
// qualified to the OTHER arm was rewritten with this arm's definition:
// `… c JOIN typemx_dim d ON c.gg = d.k WHERE d.k > 3 OR c.gg > 100` over
// `SELECT id AS k, g AS gg` became `id > 3 or g > 100` — 4612 rows where
// PostgreSQL answers 1978, a silent wrong answer replacing the obviously
// wrong 0 that came before. So the qualifier decides:
//
//   - names is the set of relation names this Project's scope answers to
//     (its CTE name, the derived alias stamped on the scans below it, each
//     scan's own alias or table name). A reference qualified by one of them
//     names this Project's OUTPUT column.
//   - a qualifier that names one of this Project's OUTPUTS is a candidate ROW
//     FIELD PATH — `rw.b` over `c_row AS rw`.
//   - a qualifier this scope does not answer to and no output claims belongs
//     to a sibling arm or an outer scope, and is left exactly as written.
type projRefs struct {
	outs  map[string]projOutput
	names map[string]bool
}

func newProjRefs(n *Node) projRefs {
	return projRefs{outs: projectSubstitutions(n.Projections), names: nodeScopeNames(n)}
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

// resolve returns the replacement for one column reference, or nil to leave
// it alone. ok=false declines the whole rewrite.
//
// The order is ADR-0022 §1's, which is expr.ResolveColumnRef's, which is what
// actually resolves the name at RUN time: the spelling as written, then the
// BARE column after dropping the qualifier, and only then the qualifier read
// as a ROW column with the name as its field. Resolving in a different order
// describes a different column — `rw.b` over
// `SELECT c_row AS rw, id AS b` is `id`, because `b` is a column of the
// projection and the run-time lookup finds it before it ever considers a
// field. Skipping step 2 for a qualified reference made the DAG answer the
// FIELD where the single-process engine answered the COLUMN, and moved the
// derived-table spelling on BOTH paths, so one query answered two ways by
// spelling. (PostgreSQL 17 rejects the unparenthesised form outright —
// `missing FROM-clause entry for table "rw"`, 42P01 — so it has no answer to
// follow here; `(rw).b` is its field spelling. Answering at all is the
// documented superset, and the ADR fixes which answer.)
func (p projRefs) resolve(ref *plansql.ColRef) (plansql.Node, bool) {
	if ref.Table == "" {
		o, ok := p.outs[strings.ToLower(ref.Column)]
		if !ok {
			return nil, true
		}
		return p.apply(o, "")
	}
	// A projection that aliases the QUALIFIED spelling itself owns the name
	// outright (`n1.n_name AS "n1.n_name"`).
	if o, ok := p.outs[strings.ToLower(ref.String())]; ok {
		return p.apply(o, "")
	}
	// Step 2 applies when the qualifier is one this Project can answer for:
	// its own relation scope, or one of its outputs (the ROW-container
	// candidate). A qualifier that is neither names a sibling join arm or an
	// outer scope, and nothing here may touch it.
	container, isContainer := p.outs[strings.ToLower(ref.Table)]
	if !p.inScope(ref.Table) && !isContainer {
		return nil, true
	}
	if o, ok := p.outs[strings.ToLower(ref.Column)]; ok {
		return p.apply(o, "")
	}
	if isContainer {
		return p.apply(container, ref.Column)
	}
	return nil, true
}

// apply turns one matched output into the replacement text. field is the ROW
// field to keep, or "" for a plain column reference.
func (p projRefs) apply(o projOutput, field string) (plansql.Node, bool) {
	if o.unsafe {
		return nil, false
	}
	if o.def == nil {
		return nil, true // passthrough: the input carries the same name
	}
	if field == "" {
		return o.def, true
	}
	// The qualifier is substituted and the field kept, so the definition has
	// to BE a column reference — a computed ROW has no spelling a field path
	// can hang off, and a qualified one would need three parts.
	ref := simpleColRef(o.def)
	if ref == nil || ref.Table != "" {
		return nil, false
	}
	return &plansql.ColRef{Table: ref.Column, Column: field}, true
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
func splitFilterForProjectPush(preds []Predicate, project *Node) (pushed, kept []Predicate) {
	p := newProjRefs(project)
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

// ResolveFilterThroughProjects re-spells a predicate that sits ABOVE one or
// more Projects into the names their INPUT carries. It is the stage DAG's
// half of the question the Filter-Project swap answers for the single-process
// pipeline, and it exists because the two paths lower a Project differently.
//
// pushdownPredicates SWAPS a Filter below a Project and substitutes each
// reference to a renamed or computed output with its defining expression. It
// DECLINES that swap for a Project tagged with a CTEName — a materialization
// fence, because the single-process planner replays ONE cached result for
// every reference of a CTE and a predicate pushed inside it would apply to
// all of them — and it never applies at all when the Filter's child is a
// JOIN, whichever kind of subquery the rename came from. Declining is right
// in both cases: the predicate does not move.
//
// What is wrong on the DAG is the SPELLING. An ordinary Project emits NO
// STAGE there (docs/internals/native-dag-execution.md §Derived-table
// aliases), so the predicate walkStages attaches to the producing stage is
// evaluated against a schema carrying SOURCE column names. A reference to the
// alias resolves to nothing, `expr.ColRef.Eval` answers nil, the predicate is
// UNKNOWN on every row, and a WHERE that admits only TRUE drops all of them —
// silently, for every type (#653). Every other consumer of a derived name on
// the DAG has a resolver for exactly this reason; the filter had none.
//
// So the predicate stays where it is and only its spelling changes, which is
// sound whatever the Project is tagged with: substitution evaluates the exact
// defining expression the Project would have produced, NULLs included. The
// walk descends a join ONE ARM AT A TIME with that arm's scope names, so a
// reference qualified to the other arm is left alone (projRefs), and it stops
// at the first Project whose output the substitution cannot express — an
// aggregate output, a volatile function — because a stage that emits such a
// column emits it under the alias, which is the name the predicate already
// carries.
//
// It also stops at a Sort or a LIMIT. Those DO emit stages, carrying the
// names above them, so a predicate re-spelled past one would name a column
// the stage below the Project has and the stage the filter lands on does not.
//
// AMBIGUITY is not this pass's to report. A bare name two relations in scope
// both carry is rejected by physical.validate before any of this runs
// ("column reference %q is ambiguous", 42702, on both paths), so a decline
// here can only be a shape the resolver leaves alone — never a name the query
// failed to disambiguate.
//
// Returns (nil, false) when nothing changed; the caller then ships the
// predicate exactly as it did before.
func ResolveFilterThroughProjects(pred Predicate, child *Node) (plansql.Node, bool) {
	if pred.ASTExpr == nil {
		return nil, false
	}
	ast, changed := resolveFilterInSubtree(pred.ASTExpr, child, false)
	if !changed {
		return nil, false
	}
	return ast, true
}

// resolveFilterInSubtree walks the producing subtree applying each rename it
// meets, and returns the expression as the stage carrying the filter will see
// it. A subtree it cannot model STOPS the walk with whatever has been settled
// so far, which is the spelling the predicate had on entry to that subtree.
func resolveFilterInSubtree(ast plansql.Node, n *Node, changed bool) (plansql.Node, bool) {
	for ; n != nil; n = n.Children[0] {
		switch n.Type {
		case NodeJoin:
			// A join emits BOTH arms' columns, so a predicate above it can
			// name either. Each arm is resolved with its OWN scope names, so
			// a qualified reference only rewrites against the arm it names.
			if len(n.Children) != 2 {
				return ast, changed
			}
			ast, changed = resolveFilterInSubtree(ast, n.Children[0], changed)
			ast, changed = resolveFilterInSubtree(ast, n.Children[1], changed)
			return ast, changed
		case NodeDistinct:
			// Emits no stage and renames nothing.
		case NodeProject:
			newAST, ok := rewriteASTThroughProject(ast, newProjRefs(n))
			if !ok {
				return ast, changed
			}
			if newAST != nil {
				ast, changed = newAST, true
			}
		default:
			// Sort and LIMIT emit stages of their own carrying the names
			// above them; everything else emits a stage under names the
			// predicate can already see.
			return ast, changed
		}
		if len(n.Children) != 1 {
			return ast, changed
		}
	}
	return ast, changed
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
		return &plansql.CastNode{Inner: in, TypeName: e.TypeName}, true
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
