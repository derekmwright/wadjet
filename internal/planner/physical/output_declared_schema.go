package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// declaredOutputSchema derives, at PLAN time, the columns a query will
// produce — names from the SELECT list, types from the catalog annotation
// AnnotateScanColumns leaves on the scans beneath it.
//
// It exists for the one case the runtime cannot answer. Wadjet derives a
// result's schema from DATA FLOW: exec.CollectSink captures it from the first
// batch it CONSUMES and exec.Project resolves its output types from the first
// batch it SEES. A query that returns zero rows produces no batch, so it has
// no schema — and `SELECT a, b FROM t WHERE false` handed the client OID 25
// (text) for every column through pgwire's coordinator path, and no columns
// AT ALL through the coordinator's correlated-local route: not an empty table
// with headers, no table (#416).
//
// The result is ADVISORY. exec.CollectSink.SchemaHint is consulted only when
// the sink consumed nothing, so an approximation for a shape this walk cannot
// type exactly costs nothing on any non-empty result. Where a column's type
// cannot be resolved it is declared STRING, which is what both entry points
// already fall back to today — so an unresolved column is no worse than
// before while a resolved one is right.
//
// Naming follows the SELECT list exactly as the projection builder does
// (alias, else the unqualified column, else the cleaned expression text), so
// an empty result names its columns the way a non-empty one would.
func declaredOutputSchema(root *logical.Node) []parquet.Column {
	if cols, ok := setOpDeclaredOutputSchema(root); ok {
		return cols
	}
	projs, childTypes, strictInt, ok := declaredProjectionInputs(root)
	if !ok {
		return nil
	}
	out := make([]parquet.Column, 0, len(projs))
	for _, proj := range projs {
		name := declaredProjectionName(proj)
		if name == "" {
			// A column the walk cannot even name makes the whole answer
			// nil: a partially-named schema is worse than none, because
			// the client would receive a result whose column COUNT does
			// not match the SELECT list.
			return nil
		}
		d := declaredProjectionDecl(proj, childTypes, strictInt)
		col := parquet.Column{
			Name:     name,
			Type:     d.ID,
			Nullable: true,
		}
		if d.ID == parquet.TypeDecimal && d.DecKnown {
			// precision 0 (pgTypeMod's "unconstrained") when it cannot be
			// resolved — the honest fallback, not a fabricated (p,s) (#458).
			col.Precision, col.Scale = d.Precision, d.Scale
		}
		out = append(out, col)
	}
	return out
}

// setOpDeclaredOutputSchema is declaredOutputSchema for a query whose OUTPUT
// is a set operation. findOutputProjectionNode stops at one — there is no
// single Project to read a SELECT list from — so the walk answered nil and a
// ZERO-ROW `SELECT a FROM t WHERE false UNION ALL SELECT b FROM t` reached
// the client with no RowDescription fields AT ALL: not an empty table with
// headers, no table. That is #416's symptom, over the shape #416 did not
// reach.
//
// The names come from the first arm, exactly as the executed schema does. The
// TYPE is the arms reconciled through setOpWiden — the ladder pinned live
// against postgres:17 — with a DECIMAL's (p,s) from batch.DecimalCommon, the
// same rule reconcileSetOpArmTypes coerces the arms with, so the declared
// answer and the executed one describe one type.
//
// ok=false means "not a set operation, or one this walk cannot type", and the
// ordinary projection walk answers.
func setOpDeclaredOutputSchema(root *logical.Node) ([]parquet.Column, bool) {
	n := setOpRoot(root)
	if n == nil {
		return nil, false
	}
	arms := setOpArmSchemas(n)
	if len(arms) < 2 {
		return nil, false
	}
	out := make([]parquet.Column, len(arms[0]))
	copy(out, arms[0])
	for i := range out {
		metas := []batch.DecimalType{}
		for _, arm := range arms {
			if i >= len(arm) {
				return nil, false
			}
			t, ok := setOpWiden(out[i].Type, arm[i].Type)
			if !ok {
				// Two types the ladder does not reconcile (two strings, two
				// dates, a mismatch): the first arm's declaration stands,
				// which is what the executed schema does too.
				metas = nil
				break
			}
			out[i].Type = t
			if m, ok := batch.DecimalTypeOf(arm[i].Type,
				batch.DecimalType{Precision: arm[i].Precision, Scale: arm[i].Scale}); ok && metas != nil {
				metas = append(metas, m)
			} else {
				metas = nil
			}
		}
		if out[i].Type != parquet.TypeDecimal {
			out[i].Precision, out[i].Scale = 0, 0
			continue
		}
		m, ok := batch.DecimalCommon(metas)
		if !ok {
			// No (p,s) the arms agree on: precision 0 is the honest
			// "unconstrained" answer, never a fabricated one (#458).
			out[i].Precision, out[i].Scale = 0, 0
			continue
		}
		out[i].Precision, out[i].Scale = m.Precision, m.Scale
	}
	return out, true
}

// setOpRoot returns the set operation a query's output IS, descending through
// the nodes that pass a result along unchanged, or nil for anything else.
func setOpRoot(n *logical.Node) *logical.Node {
	for n != nil {
		switch n.Type {
		case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
			return n
		case logical.NodeSort, logical.NodeLimit, logical.NodeDistinct:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// setOpArmSchemas is the declared output schema of each arm of a set
// operation, flattening a nested one so a three-arm union compares all three.
// A nil entry anywhere makes the whole answer empty: a partially-typed set
// operation is worse than none, for the reason declaredOutputSchema returns
// nil on a column it cannot name.
func setOpArmSchemas(n *logical.Node) [][]parquet.Column {
	out, _ := setOpArmSchemasAndTypmods(n)
	return out
}

// setOpArmSchemasAndTypmods is setOpArmSchemas with each arm's own
// unconstrained-DECIMAL set alongside, so the reconciliation can tell an arm
// that carries a real typmod from one that carries none.
func setOpArmSchemasAndTypmods(n *logical.Node) ([][]parquet.Column, []map[string]bool) {
	var out [][]parquet.Column
	var mods []map[string]bool
	for _, c := range n.Children {
		if inner := setOpRoot(c); inner != nil {
			nested, nestedMods := setOpArmSchemasAndTypmods(inner)
			if len(nested) == 0 {
				return nil, nil
			}
			out = append(out, nested...)
			mods = append(mods, nestedMods...)
			continue
		}
		schema := declaredOutputSchema(c)
		if len(schema) == 0 {
			return nil, nil
		}
		out = append(out, schema)
		mods = append(mods, declaredWireUnconstrainedDecimal(c))
	}
	return out, mods
}

// declaredWireUnconstrainedDecimal names the DECIMAL output columns whose
// PostgreSQL wire typmod must declare "unconstrained" (-1) even though this
// engine's own declared/exec schema keeps a real (p,s) for them.
//
// Verified live against postgres:17-alpine's \gdesc: an aggregate function
// call NEVER carries its argument's typmod through — MIN(n)/MAX(n)/
// MIN_BY(x,n)/MAX_BY(x,n)/SUM(n)/AVG(n) over a numeric(p,s) column all
// report an unconstrained numeric, and only a BARE column reference in the
// SELECT list keeps (p,s). declaredOutputSchema's own Precision/Scale answer
// stays real for these columns — internal/engine/exec/aggregate.go's DECIMAL
// vector allocation and internal/storage/parquet's file writer both key
// physical decisions off Precision/Scale (18-digit INT64 vs 38-digit
// FixedLenByteArray encoding), so zeroing it there would risk silently
// mis-encoding a materialized MIN/MAX-of-DECIMAL(38,s) result — this is
// wire-metadata ONLY, consulted solely by pgTypeMod (fold-in to #457/#458,
// FIX 2).
//
// The gate is PostgreSQL's select_common_typmod: a numeric result KEEPS a
// typmod when every input it is resolved from carries the SAME one, and is
// unconstrained otherwise — verified live against 17.11's \gdesc, where
// GREATEST(a, a), COALESCE(a, a), CASE … THEN a ELSE a and NULLIF(a, b) over
// numeric(9,2) a and numeric(18,4) b all describe as numeric(9,2), NULLIF(b,
// a) and LEAST(b, b) as numeric(18,4), and GREATEST(a, b) as plain numeric.
//
// It is emphatically NOT "computed ⇒ unconstrained": that reading is wrong in
// both directions, dropping the typmod PostgreSQL keeps for a choice over one
// column and keeping the one it drops for a set operation over a computed
// arm. What carries a typmod is a BARE COLUMN REFERENCE and the choice
// constructs folded over bare references; an aggregate, a window function,
// arithmetic, a CAST and every other function call carry -1, and one -1
// anywhere in the fold makes the result -1 (#587, #542, ADR-0024 item 5).
func declaredWireUnconstrainedDecimal(root *logical.Node) map[string]bool {
	if out := setOpWireUnconstrainedDecimal(root); out != nil {
		return out
	}
	projs, childTypes, _, ok := declaredProjectionInputs(root)
	if !ok {
		return nil
	}
	var computed map[string]bool
	if pn := findOutputProjectionNode(root); pn != nil && len(pn.Children) == 1 {
		computed = emittedComputedCols(pn.Children[0])
	}
	var out map[string]bool
	for _, proj := range projs {
		name := declaredProjectionName(proj)
		if name == "" {
			continue
		}
		// No "is it DECIMAL" precondition. The plan cannot always type an
		// aggregate — aggSpecOutputDecimal declines a non-bare-ColRef input,
		// so `MAX(COALESCE(a, a))` resolved to the STRING fallback here
		// while the RUNTIME schema declared numeric(9,2), and skipping it
		// left the wire carrying a modifier PostgreSQL drops for every
		// aggregate call. An entry for a column that turns out not to be
		// DECIMAL is vacuous: pgTypeMod consults this only on the DECIMAL
		// arm, and every other type's modifier is -1 already.
		if projectionKeepsTypmod(proj, childTypes, computed) {
			continue
		}
		if out == nil {
			out = make(map[string]bool)
		}
		out[name] = true
	}
	return out
}

// projectionKeepsTypmod reports whether one output projection carries a real
// PostgreSQL type modifier — select_common_typmod over what it is resolved
// from.
func projectionKeepsTypmod(proj logical.Projection, decls colDecls, computed map[string]bool) bool {
	if proj.IsAgg {
		// An aggregate call. PostgreSQL never carries its argument's typmod
		// through one.
		return false
	}
	if proj.ASTExpr == nil {
		// Nothing to walk: the value is a copy of the column this projection
		// names, so it keeps that column's typmod unless something below
		// computed it.
		return !computed[strings.ToLower(sourceRefName(proj))]
	}
	_, _, ok := declaredTypmod(proj.ASTExpr, decls, computed)
	return ok
}

// declaredTypmod is PostgreSQL's select_common_typmod over an expression
// tree: the (precision, scale) the result carries on the wire, and whether it
// carries one at all.
//
// A BARE COLUMN REFERENCE carries its column's own typmod — unless some node
// below the projection COMPUTED that column, which is how a window function
// reaches the SELECT list looking exactly like a column (#587). The choice
// constructs — CASE, COALESCE, NULLIF, IFNULL, IF, GREATEST, LEAST — fold
// their branches, over the same candidate positions the TYPE resolution folds
// (expr.Ret.SameAsArgs), and keep the typmod only when every branch carries
// the same one. A NULL branch carries nothing and is skipped, the way it is
// skipped when the common TYPE is chosen. Everything else — an aggregate, an
// operator, a CAST, any other function call — carries -1, and one of those
// anywhere in the fold makes the whole result -1.
func declaredTypmod(node plansql.Node, decls colDecls, computed map[string]bool) (int, int, bool) {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return declaredTypmod(n.Inner, decls, computed)
	case *plansql.ColRef:
		if computed[strings.ToLower(cleanExpr(n.String()))] || computed[strings.ToLower(n.Column)] {
			return 0, 0, false
		}
		c, ok := decls.colDecl(n)
		if !ok {
			return 0, 0, false
		}
		if c.Type != parquet.TypeDecimal {
			// A non-DECIMAL column carries no numeric modifier, and that is
			// an ANSWER rather than an absence: (0,0) agrees with itself, so
			// a bare reference to one keeps whatever it has and a fold that
			// mixes one with a numeric(p,s) disagrees and drops to -1.
			return 0, 0, true
		}
		if c.Precision <= 0 {
			return 0, 0, false
		}
		return c.Precision, c.Scale, true
	case *plansql.CaseNode:
		arms := make([]plansql.Node, 0, len(n.Whens)+1)
		for _, w := range n.Whens {
			arms = append(arms, w.Result)
		}
		// n.Else is appended even when nil: a CASE with no ELSE has an
		// implicit NULL branch, and foldTypmod reads a nil arm as one.
		arms = append(arms, n.Else)
		return foldTypmod(arms, decls, computed)
	case *plansql.FuncCallNode:
		idx, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args))
		if !poly {
			// A fixed declaration is a function's OWN type, and PostgreSQL
			// gives a function result no typmod.
			return 0, 0, false
		}
		arms := make([]plansql.Node, 0, len(idx))
		for _, i := range idx {
			if i >= 0 && i < len(n.Args) {
				arms = append(arms, n.Args[i])
			}
		}
		return foldTypmod(arms, decls, computed)
	}
	return 0, 0, false
}

// foldTypmod is select_common_typmod over a set of alternatives: they all
// carry the same modifier, or the result carries none.
//
// A NULL branch is NOT skipped, which is where the TYPMOD fold parts company
// with the TYPE fold beside it. An untyped NULL coerced into the common type
// carries typmod -1, so it drops the modifier the same way an aggregate
// argument does — verified live: COALESCE(numeric(9,2), NULL) describes as
// plain numeric, while its TYPE is still numeric(9,2) (which is why
// expr.CommonDeclType skips it and this does not).
func foldTypmod(arms []plansql.Node, decls colDecls, computed map[string]bool) (int, int, bool) {
	p, s, have := 0, 0, false
	for _, a := range arms {
		if a == nil {
			// A missing ELSE is an implicit NULL branch and carries -1 like
			// an explicit one.
			return 0, 0, false
		}
		ap, as, ok := declaredTypmod(a, decls, computed)
		if !ok {
			return 0, 0, false
		}
		if !have {
			p, s, have = ap, as, true
			continue
		}
		if ap != p || as != s {
			return 0, 0, false
		}
	}
	return p, s, have
}

// projectionIsComputed reports whether a projection's value comes from an
// expression rather than from copying one input column — the distinction
// exec.Project draws with ProjectColumn.Computed, and the one PostgreSQL
// draws when it decides whether a numeric result keeps its typmod.
func projectionIsComputed(proj logical.Projection) bool {
	return proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr)
}

// sourceRefName is the input column a bare (or renamed) projection copies.
func sourceRefName(proj logical.Projection) string {
	if proj.Column != "" {
		return proj.Column
	}
	return cleanExpr(proj.Expr)
}

// emittedComputedCols names the columns a subtree emits that are NOT a bare
// copy of a stored column: a Window's own outputs, an Aggregate's, and a
// Project's computed items. A projection that merely REFERENCES one of these
// is not a bare column reference in PostgreSQL's sense, however bare it looks
// in the SELECT list — which is the whole of #587.
func emittedComputedCols(n *logical.Node) map[string]bool {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeWindow:
		if len(n.Children) != 1 {
			return nil
		}
		out := emittedComputedCols(n.Children[0])
		for _, we := range n.WindowExprs {
			if we.OutputCol == "" {
				continue
			}
			if out == nil {
				out = make(map[string]bool, len(n.WindowExprs))
			}
			out[strings.ToLower(we.OutputCol)] = true
		}
		return out
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		var out map[string]bool
		for _, agg := range n.AggExprs {
			if agg.OutputCol == "" {
				continue
			}
			if out == nil {
				out = make(map[string]bool, len(n.AggExprs))
			}
			out[strings.ToLower(agg.OutputCol)] = true
		}
		return out
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		below := emittedComputedCols(n.Children[0])
		var out map[string]bool
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			if !proj.IsAgg && !projectionIsComputed(proj) && !below[strings.ToLower(sourceRefName(proj))] {
				continue
			}
			if out == nil {
				out = make(map[string]bool, len(n.Projections))
			}
			out[strings.ToLower(name)] = true
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedComputedCols(n.Children[0])
	case logical.NodeJoin:
		// The UNION of the two sides. This arm is not optional beside the two
		// above it: without them the whole type map was nil and EVERY column
		// under a join fell back to "no typmod", which happened to keep the
		// right answer for the computed ones. With them resolving, a bare
		// rename below a join keeps its typmod (#697) and a window output, an
		// aggregate output, arithmetic or a CAST below the same join must
		// still lose it (#587, #542) — which is what this carries across.
		//
		// A semi/anti join emits only its probe side, so merging the build
		// side's names in is over-broad. It is inert — no SELECT list can
		// name an inner-subquery column — and it is what inputColTypes
		// already does for those joins, so the two walks agree rather than
		// differing by a case this cannot exercise.
		if len(n.Children) != 2 {
			return nil
		}
		left, right := emittedComputedCols(n.Children[0]), emittedComputedCols(n.Children[1])
		if left == nil {
			return right
		}
		out := make(map[string]bool, len(left)+len(right))
		for k := range left {
			out[k] = true
		}
		for k := range right {
			out[k] = true
		}
		return out
	}
	return nil
}

// setOpWireUnconstrainedDecimal answers declaredWireUnconstrainedDecimal for
// a query whose OUTPUT is a set operation, which findOutputProjectionNode
// stops at (there is no single Project to read a SELECT list from).
//
// PostgreSQL keeps a numeric's typmod across a set operation only when EVERY
// arm carries the same one, and declares the result unconstrained otherwise
// — verified live against postgres:17-alpine's \gdesc. Wadjet declared a real
// (p,s) either way, which is #542; the corpus pins both directions, so an
// agreeing pair still has to keep its typmod.
//
// nil means "not a set operation" and the ordinary projection walk answers.
func setOpWireUnconstrainedDecimal(root *logical.Node) map[string]bool {
	n := root
	for n != nil {
		switch n.Type {
		case logical.NodeUnion, logical.NodeIntersect, logical.NodeExcept:
			return setOpArmDecimalDisagreements(n)
		case logical.NodeSort, logical.NodeLimit, logical.NodeDistinct:
			if len(n.Children) != 1 {
				return nil
			}
			n = n.Children[0]
		default:
			return nil
		}
	}
	return nil
}

// setOpArmDecimalDisagreements names the DECIMAL result columns of a set
// operation whose arms do not all declare the same (p,s). The result's column
// NAMES come from the first arm, exactly as the executed schema does.
func setOpArmDecimalDisagreements(n *logical.Node) map[string]bool {
	arms, armUnconstrained := setOpArmSchemasAndTypmods(n)
	if len(arms) < 2 {
		// An arm this walk cannot type says nothing about agreement.
		// Declaring every DECIMAL unconstrained is the safe answer: it is
		// what PostgreSQL sends whenever the arms are not provably
		// identical, and it never claims a (p,s) nothing verified.
		return setOpAllDecimalUnconstrained(arms)
	}
	var out map[string]bool
	for i, col := range arms[0] {
		if col.Type != parquet.TypeDecimal {
			continue
		}
		agree := true
		for j, other := range arms {
			// An arm whose OWN column carries no typmod — an aggregate, a
			// window function, arithmetic, a CAST — makes the result
			// unconstrained however well the arms' (p,s) line up:
			// `SELECT MIN(a) FROM t UNION ALL SELECT a FROM t` is plain
			// numeric on PostgreSQL, and wadjet declared numeric(9,2)
			// because it compared only the widths (ADR-0024 item 5).
			if j < len(armUnconstrained) && i < len(other) && armUnconstrained[j][other[i].Name] {
				agree = false
				break
			}
			if j == 0 {
				continue
			}
			if i >= len(other) || other[i].Type != parquet.TypeDecimal ||
				other[i].Precision != col.Precision || other[i].Scale != col.Scale {
				agree = false
				break
			}
		}
		if agree {
			continue
		}
		if out == nil {
			out = make(map[string]bool)
		}
		out[col.Name] = true
	}
	return out
}

// setOpAllDecimalUnconstrained marks every DECIMAL column of the arms
// resolved so far, for the case an arm could not be typed at all.
func setOpAllDecimalUnconstrained(arms [][]parquet.Column) map[string]bool {
	var out map[string]bool
	for _, arm := range arms {
		for _, col := range arm {
			if col.Type != parquet.TypeDecimal {
				continue
			}
			if out == nil {
				out = make(map[string]bool)
			}
			out[col.Name] = true
		}
	}
	return out
}

// declaredProjectionInputs is declaredOutputSchema's and
// declaredWireUnconstrainedDecimal's shared setup: the visible projection
// list plus the child's declarations each projection is resolved against. ok is false when there is nothing to declare (no output
// projection node, or an empty SELECT list) — callers return their own
// empty answer in that case rather than proceeding with nil maps.
func declaredProjectionInputs(root *logical.Node) (projs []logical.Projection, childTypes colDecls, strictInt map[string]bool, ok bool) {
	pn := findOutputProjectionNode(root)
	if pn == nil {
		return nil, colDecls{}, nil, false
	}
	projs = logical.VisibleProjections(pn.Projections)
	if len(projs) == 0 {
		return nil, colDecls{}, nil, false
	}
	if len(pn.Children) == 1 {
		// The ROW fields come from inputColFields rather than an emitted-
		// column walk of its own: the nodes emittedColTypes adds — an
		// Aggregate and a Project — rebind names, and a field path over
		// either resolves against nothing anyway. Everything else passes
		// its input through, which is exactly inputColFields' walk (#568).
		childTypes = colDecls{
			types:  emittedColTypes(pn.Children[0]),
			fields: inputColFields(pn.Children[0]),
			// The (p,s) beside the TypeIDs, so a DECIMAL projection is
			// resolved by ONE walk instead of two hand-mirrored ones
			// (declaredProjectionDecl, ADR-0024 item 2).
			dec: emittedColDecimal(pn.Children[0]),
		}
		// The same integer-preserving-arithmetic hint the projection builder
		// passes: without it `id + 1` declares FLOAT64 here where the
		// operator emits INT64 (#297's rule), so an empty result would
		// disagree with a full one about the type of its own column.
		strictInt = strictIntArithCols(pn.Children[0])
	}
	return projs, childTypes, strictInt, true
}

// declaredProjectionName mirrors the naming rule in the projection builder
// (buildProjectOp): the alias, else the unqualified column name, else the
// cleaned expression text.
func declaredProjectionName(proj logical.Projection) string {
	if proj.Alias != "" {
		return proj.Alias
	}
	if proj.Column != "" {
		return proj.Column
	}
	return cleanExpr(proj.Expr)
}

// declaredProjectionType answers what exec.Project will emit for one
// projection, which is NOT the type the planner puts on ProjectColumn: that
// declaration is a placeholder for a bare column reference and the operator
// overwrites it from the input batch. So a bare reference is typed from the
// input's columns here, the way the operator would, and only a COMPUTED
// expression takes inferProjectionTypeCols' answer.
func declaredProjectionType(proj logical.Projection, decls colDecls, strictInt map[string]bool) parquet.TypeID {
	return declaredProjectionDecl(proj, decls, strictInt).ID
}

// declaredProjectionDecl is declaredProjectionType with the parameterized
// part of the answer kept — a DECIMAL's (precision, scale).
//
// It replaces the pair declaredProjectionType/declaredProjectionDecimal,
// which resolved the SAME name twice through two hand-mirrored copies of one
// rule and could therefore describe two different columns; ADR-0024's whole
// premise is that the declared type of a DECIMAL is (TypeID, p, s) and not a
// TypeID with a lookaside map.
//
// The (p,s) half is undecided — reported as precision 0, pgTypeMod's
// "unconstrained" — wherever it cannot be resolved rather than fabricated
// (#458).
func declaredProjectionDecl(proj logical.Projection, decls colDecls, strictInt map[string]bool) expr.DeclType {
	if proj.IsAgg {
		// The aggregate below emitted a column under this alias; its type
		// is in the child's emitted map.
		name := declaredProjectionName(proj)
		t, ok := lookupColType(decls.types, name)
		if !ok {
			return expr.Decl(parquet.TypeString)
		}
		if t == parquet.TypeDecimal {
			if m, ok := lookupColDecimal(decls.dec, name); ok && m.Precision > 0 {
				return expr.DeclDecimal(m.Precision, m.Scale)
			}
			return expr.Decl(parquet.TypeDecimal)
		}
		return expr.Decl(t)
	}
	// A ROW FIELD PATH is not the bare reference it looks like: the name
	// resolution below strips the qualifier and then finds no column, so
	// every field path was declared STRING and pgwire reported OID 25 for a
	// zero-row result whatever the field's type (#568).
	//
	// The field's declaration answers DIRECTLY, not through
	// colRefDeclaredType — which declines every parameterized type because
	// it can return only a TypeID. That decline is right for the OUTPUT
	// VECTOR a projection allocates and wrong here: this schema is advisory
	// metadata, its DECIMAL (p,s) comes from declaredProjectionDecimal
	// alongside, and a bare column of the same type is already declared this
	// way one branch down (lookupColType makes no such distinction). Leaving
	// a DECIMAL field at STRING would make the empty result disagree with
	// the full one about its own column.
	if fc, ok := declaredFieldPath(proj, decls); ok {
		if fc.Type == parquet.TypeDecimal && fc.Precision > 0 {
			return expr.DeclDecimal(fc.Precision, fc.Scale)
		}
		return expr.Decl(fc.Type)
	}
	if proj.ASTExpr != nil && !isSimpleColRefForRename(proj.ASTExpr) {
		return inferProjectionDeclType(proj.ASTExpr, parquet.TypeString, strictInt, decls)
	}
	// A PARENTHESIZED bare reference — `SELECT (a)` — is a bare reference,
	// which isSimpleColRefForRename already says and the name resolution
	// below could not: proj.Column is empty for it and cleanExpr answers the
	// parenthesized TEXT, which names no column, so every such projection
	// was declared STRING where PostgreSQL declares the column's own type.
	// Resolve it from the AST, where the reference still is one.
	if cr, ok := bareColRefOf(proj.ASTExpr); ok {
		if c, ok := decls.colDecl(cr); ok {
			if c.Type == parquet.TypeDecimal && c.Precision > 0 {
				return expr.DeclDecimal(c.Precision, c.Scale)
			}
			return expr.Decl(c.Type)
		}
	}
	ref := proj.Column
	if ref == "" {
		ref = cleanExpr(proj.Expr)
	}
	t, ok := lookupColType(decls.types, ref)
	if !ok {
		return expr.Decl(parquet.TypeString)
	}
	if t == parquet.TypeDecimal {
		if m, ok := lookupColDecimal(decls.dec, ref); ok && m.Precision > 0 {
			return expr.DeclDecimal(m.Precision, m.Scale)
		}
		return expr.Decl(parquet.TypeDecimal)
	}
	return expr.Decl(t)
}

// declaredProjectionDecimal is the DECIMAL half of declaredProjectionDecl,
// kept as a name for the callers that ask only that question. It resolves
// through the SAME function as the type, so the two can never describe
// different columns — before ADR-0024 they were two hand-mirrored walks
// (#458).
func declaredProjectionDecimal(proj logical.Projection, decls colDecls, decMeta map[string]logical.DecimalMeta) (logical.DecimalMeta, bool) {
	if decls.dec == nil {
		decls.dec = decMeta
	}
	d := declaredProjectionDecl(proj, decls, nil)
	if d.ID != parquet.TypeDecimal || !d.DecKnown {
		return logical.DecimalMeta{}, false
	}
	return logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}, true
}

// bareColRefOf unwraps a projection expression that IS a column reference,
// parentheses and all — the shape isSimpleColRefForRename accepts.
func bareColRefOf(e plansql.Node) (*plansql.ColRef, bool) {
	for {
		switch n := e.(type) {
		case *plansql.ColRef:
			return n, true
		case *plansql.ParenNode:
			e = n.Inner
		default:
			return nil, false
		}
	}
}

// declaredFieldPath resolves a non-aggregate projection that is a ROW field
// path to the FIELD's declaration. Both declaredProjectionType and
// declaredProjectionDecimal go through it so the two never describe different
// columns.
func declaredFieldPath(proj logical.Projection, decls colDecls) (parquet.Column, bool) {
	if proj.IsAgg || proj.ASTExpr == nil {
		return parquet.Column{}, false
	}
	cr, ok := proj.ASTExpr.(*plansql.ColRef)
	if !ok {
		if p, isParen := proj.ASTExpr.(*plansql.ParenNode); isParen {
			cr, ok = p.Inner.(*plansql.ColRef)
		}
		if !ok {
			return parquet.Column{}, false
		}
	}
	if !decls.isFieldPath(cr) {
		return parquet.Column{}, false
	}
	return decls.field(cr)
}

// lookupColDecimal is lookupColType's companion for a DECIMAL-meta map.
func lookupColDecimal(decMeta map[string]logical.DecimalMeta, name string) (logical.DecimalMeta, bool) {
	if decMeta == nil || name == "" {
		return logical.DecimalMeta{}, false
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if m, ok := decMeta[lc]; ok {
		return m, true
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		if m, ok := decMeta[lc[dot+1:]]; ok {
			return m, true
		}
	}
	return logical.DecimalMeta{}, false
}

// lookupColType resolves a possibly-qualified name against a column-type map,
// falling back to the bare suffix the way every other name resolution in the
// planner does.
func lookupColType(colTypes map[string]parquet.TypeID, name string) (parquet.TypeID, bool) {
	if colTypes == nil || name == "" {
		return 0, false
	}
	lc := strings.ToLower(strings.TrimSpace(name))
	if t, ok := colTypes[lc]; ok {
		return t, true
	}
	if dot := strings.LastIndexByte(lc, '.'); dot >= 0 {
		if t, ok := colTypes[lc[dot+1:]]; ok {
			return t, true
		}
	}
	return 0, false
}

// emittedColTypes describes the columns a node EMITS, by name.
//
// inputColTypes answers the same question for the nodes that pass their
// input through unchanged, and deliberately STOPS at anything that rebinds a
// name — Project, Aggregate, Window, the set operators — because descending
// past one of those would answer with the wrong value's type. This adds the
// two rebinding nodes whose output IS derivable: an Aggregate emits its group
// columns at their input types plus one column per aggregate at the type
// aggSpecOutputType declares, and a Project emits its own projections.
// Everything else still answers nil, and a nil map means every column falls
// back to STRING rather than to a guess.
func emittedColTypes(n *logical.Node) map[string]parquet.TypeID {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColTypes(n.Children[0])
		out := make(map[string]parquet.TypeID, len(n.GroupBy)+len(n.AggExprs))
		for _, g := range n.GroupBy {
			if t, ok := lookupColType(in, g); ok {
				out[strings.ToLower(g)] = t
			}
		}
		for _, agg := range n.AggExprs {
			name := strings.ToLower(agg.OutputCol)
			if name == "" {
				continue
			}
			if t, known := aggSpecOutputType(n, agg); known {
				out[name] = t
			}
		}
		return out
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColTypes(n.Children[0])
		strictInt := strictIntArithCols(n.Children[0])
		decls := colDecls{types: in, dec: emittedColDecimal(n.Children[0])}
		out := make(map[string]parquet.TypeID, len(n.Projections))
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			out[strings.ToLower(name)] = declaredProjectionType(proj, decls, strictInt)
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColTypes(n.Children[0])
	case logical.NodeWindow:
		// Window appends its output columns to the input batch in place — it
		// drops nothing — so every input column passes through at its input
		// type, and only the window expressions themselves need typing. Using
		// windowSpecOutputType (walkStages/buildWindow's own resolver) keeps
		// this answer identical to what the operator actually emits: an INT64
		// passthrough column no longer declares STRING on a zero-row result,
		// ROW_NUMBER/RANK/COUNT declare INT64, SUM/AVG declare DECIMAL over a
		// DECIMAL argument and FLOAT64 otherwise (#586), and a value function
		// (LAG/LEAD/FIRST_VALUE/...) or MIN/MAX declares its argument
		// column's type when that is known.
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColTypes(n.Children[0])
		out := make(map[string]parquet.TypeID, len(in)+len(n.WindowExprs))
		for k, t := range in {
			out[k] = t
		}
		for _, we := range n.WindowExprs {
			name := strings.ToLower(we.OutputCol)
			if name == "" {
				continue
			}
			out[name] = windowSpecOutputType(n, we).ID
		}
		return out
	case logical.NodeJoin:
		// A JOIN emits both sides' columns, and this walk must cross it
		// ITSELF rather than fall through to inputColTypes: that one's own
		// join arm recurses with inputColTypes, which has no Project,
		// Aggregate or Window arm, so a side that is any of those answered
		// nil and its `left == nil || right == nil` rule then nilled the
		// WHOLE map. Every column of the query lost its type, and with it its
		// typmod — which is #697: a decorrelated correlated subquery is a
		// Join whose right side is an Aggregate, so `SELECT s_acctbal … WHERE
		// ps_supplycost = (SELECT MIN(…) …)` described a bare numeric(15,2)
		// column as unconstrained while the same projection without the
		// subquery kept it. The same nil also declared every column of a
		// ZERO-ROW result STRING — #416's failure mode, over the shape #416
		// did not reach.
		//
		// A nil SIDE is tolerated rather than fatal, the way inputColFields'
		// join arm already tolerates one: the names this walk resolved are
		// still that side's own names, and a name it could not resolve is
		// absent, which is exactly the "fall back to STRING" answer. The
		// disagreement rule is inputColTypes' verbatim — a name the two sides
		// declare at different types is DROPPED rather than picked, because a
		// self-join is not the only way to reach one.
		if len(n.Children) != 2 {
			return nil
		}
		return mergeJoinSides(emittedColTypes(n.Children[0]), emittedColTypes(n.Children[1]))
	}
	return inputColTypes(n)
}

// mergeJoinSides merges the two sides of a join's emitted columns: a name only
// one side carries is kept, and a name both carry at DIFFERENT values is
// dropped rather than resolved to a side. It is inputColTypes' join rule with
// a nil side tolerated instead of fatal — see emittedColTypes' NodeJoin arm.
func mergeJoinSides[V comparable](left, right map[string]V) map[string]V {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}
	merged := make(map[string]V, len(left)+len(right))
	for c, v := range left {
		merged[c] = v
	}
	for c, v := range right {
		if prev, dup := merged[c]; dup && prev != v {
			delete(merged, c)
			continue
		}
		merged[c] = v
	}
	return merged
}

// emittedColDecimal is emittedColTypes' companion for DECIMAL precision/
// scale (#458): the same walk over the same node kinds, but holding only
// entries whose column IS declared DECIMAL by emittedColTypes — a name
// present here and absent (or a different type) there would be a
// contradiction between the two answers describing the same column.
//
// A Window's own expressions DO introduce DECIMAL entries since ADR-0024:
// MIN/MAX and the value functions carry their argument's own (p,s), and
// SUM/AVG carry the accumulator's (38,s) and (38,min(s+4,38)) — see the
// NodeWindow arm below.
func emittedColDecimal(n *logical.Node) map[string]logical.DecimalMeta {
	if n == nil {
		return nil
	}
	switch n.Type {
	case logical.NodeAggregate:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColDecimal(n.Children[0])
		out := make(map[string]logical.DecimalMeta, len(n.GroupBy)+len(n.AggExprs))
		for _, g := range n.GroupBy {
			if m, ok := lookupColDecimal(in, g); ok {
				out[strings.ToLower(g)] = m
			}
		}
		for _, agg := range n.AggExprs {
			name := strings.ToLower(agg.OutputCol)
			if name == "" {
				continue
			}
			if m, known := aggSpecOutputDecimal(n, agg); known {
				out[name] = m
			}
		}
		return out
	case logical.NodeProject:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColDecimal(n.Children[0])
		fieldDecls := colDecls{types: emittedColTypes(n.Children[0]),
			fields: inputColFields(n.Children[0]), dec: in}
		out := make(map[string]logical.DecimalMeta, len(n.Projections))
		for _, proj := range n.Projections {
			name := declaredProjectionName(proj)
			if name == "" {
				continue
			}
			if m, ok := declaredProjectionDecimal(proj, fieldDecls, in); ok {
				out[strings.ToLower(name)] = m
			}
		}
		return out
	case logical.NodeFilter, logical.NodeLimit, logical.NodeSort, logical.NodeDistinct:
		if len(n.Children) != 1 {
			return nil
		}
		return emittedColDecimal(n.Children[0])
	case logical.NodeWindow:
		if len(n.Children) != 1 {
			return nil
		}
		in := emittedColDecimal(n.Children[0])
		// A window's OWN outputs can be DECIMAL now: MIN/MAX and the value
		// functions over a DECIMAL column answer that column's type, (p,s)
		// and all, and SUM/AVG answer the accumulator's DECIMAL(38,s) /
		// DECIMAL(38,min(s+4,38)) — which is what exec.windowOutputColumn
		// already emits (#586).
		// Before ADR-0024 windowSpecOutputType could not resolve a
		// parameterized argument at all, so a ZERO-ROW window result — which
		// is described from the plan alone — went out as float8 where the
		// same query over rows went out as numeric (#587).
		var out map[string]logical.DecimalMeta
		for _, we := range n.WindowExprs {
			name := strings.ToLower(we.OutputCol)
			if name == "" {
				continue
			}
			d := windowSpecOutputType(n, we)
			if d.ID != parquet.TypeDecimal || !d.DecKnown {
				continue
			}
			if out == nil {
				out = make(map[string]logical.DecimalMeta, len(in)+len(n.WindowExprs))
				for k, v := range in {
					out[k] = v
				}
			}
			out[name] = logical.DecimalMeta{Precision: d.Precision, Scale: d.Scale}
		}
		if out != nil {
			return out
		}
		return in
	case logical.NodeJoin:
		// The (p,s) half of emittedColTypes' join arm, and it must cross the
		// join for the same reason and by the same rule — the two answers
		// describe one column, so a name kept there and dropped here would be
		// a DECIMAL with no precision, which #458's sentinel then reads as
		// unconstrained.
		if len(n.Children) != 2 {
			return nil
		}
		return mergeJoinSides(emittedColDecimal(n.Children[0]), emittedColDecimal(n.Children[1]))
	}
	return inputColDecimal(n)
}
