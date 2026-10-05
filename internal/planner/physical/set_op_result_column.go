// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// SetOpArmFacts is what one arm's SELECT list says, per result position,
// beyond the column it declares: whether the item has a type of its own, and
// what it contributes to the result column's mark (ADR-0024 §10). Both are
// read off the item's SPELLING, so the stage planner and the single-process
// path, which see the same logical arm, read the same facts.
type SetOpArmFacts []setOpArmFact

type setOpArmFact struct {
	// untyped: PostgreSQL gives the item no type of its own and resolves it
	// to the other arms' — a quoted literal, a bare NULL, and a NULL cast to
	// plain NUMERIC (which is numeric there; this engine's expression typing
	// declares that cast double precision, so the declaration is not used).
	untyped bool
	// weak: an untyped item that still DECLARES a type (the NULL cast to
	// plain NUMERIC). When no other arm is typed, its declaration is the
	// result's, as it was before, rather than leaving the column untyped.
	weak bool
	role setOpMarkRole
}

// setOpMarkRole is what one arm contributes to the result column's mark.
//
// PostgreSQL types a set-operation column over numeric arms of different
// type modifiers as plain numeric, and prints every value at its own scale.
// One stored scale per column here: a MARKED column prints its values with
// trailing fraction zeros trimmed, an unmarked one at the stored scale. The
// result is marked when at least one arm is a marked column and no arm holds
// fraction digits of its own that trimming would destroy — a constrained
// NUMERIC(p,s) column or CAST, or a literal spelled with trailing zeros, whose
// values PostgreSQL prints at that scale. Every other arm — an integer, a NULL,
// a quoted or numeric literal without trailing zeros, an expression PostgreSQL
// types plain numeric — is neutral: trimming prints its values as PostgreSQL
// does. The rule is a fold (any veto wins, then any mark), so the arms' order
// and nesting give one answer. The measured table is ADR-0024 §10.
type setOpMarkRole uint8

const (
	// setOpMarkByDecl: the arm's declared column decides (a bare column, a
	// CAST to NUMERIC(p,s), a nested set operation).
	setOpMarkByDecl setOpMarkRole = iota
	setOpMarkNeutral
	setOpMarkVeto
	// setOpMarkMarked is only ever a resolved role, never a fact.
	setOpMarkMarked
)

func (f SetOpArmFacts) at(col int) setOpArmFact {
	if col >= 0 && col < len(f) {
		return f[col]
	}
	return setOpArmFact{}
}

// setOpArmFactsOf reads one arm's facts from its select list. nil (every
// position "decided by its declaration") when the arm has no projection of
// its own — a nested set operation, whose own result type carries what its
// fold found (SetOpColType.fold) — or when the list is not one item per
// result column.
func setOpArmFactsOf(arm *logical.Node, cols int) SetOpArmFacts {
	proj := findOutputProjectionNode(arm)
	if proj == nil || len(proj.Projections) != cols {
		return nil
	}
	out := make(SetOpArmFacts, cols)
	for i, pr := range proj.Projections {
		if pr.ASTExpr != nil {
			out[i] = setOpItemFact(pr.ASTExpr)
		}
	}
	return out
}

// setOpItemFact classifies one select item (the table in ADR-0024 §10).
func setOpItemFact(e plansql.Node) setOpArmFact {
	switch n := plansql.Unparen(e).(type) {
	case *plansql.Lit:
		switch n.Kind {
		case plansql.LitNull:
			return setOpArmFact{untyped: true, role: setOpMarkNeutral}
		case plansql.LitString:
			return setOpArmFact{untyped: true, role: setOpLiteralTextRole(n.Value)}
		}
	case *plansql.ColRef:
		return setOpArmFact{role: setOpMarkByDecl}
	case *plansql.SubqueryNode:
		// PostgreSQL gives a scalar subquery its column's type modifier.
		return setOpArmFact{role: setOpMarkByDecl}
	case *plansql.CastNode:
		if n.Column {
			// A correlated re-run's outer value, typed as that column.
			return setOpArmFact{role: setOpMarkByDecl}
		}
		_, _, hasParams, isDec := expr.DecimalCastDest(n.TypeName)
		if lit, ok := plansql.Unparen(n.Inner).(*plansql.Lit); ok && lit.Kind == plansql.LitNull {
			// A typed NULL holds no digits. Cast to plain NUMERIC it has no
			// (p,s) either, and is resolved like an untyped NULL.
			return setOpArmFact{untyped: isDec && !hasParams, weak: isDec && !hasParams, role: setOpMarkNeutral}
		}
		if isDec && hasParams {
			return setOpArmFact{role: setOpMarkByDecl}
		}
	}
	if d, ok := setOpLitArm(e); ok {
		return setOpArmFact{role: setOpLiteralTextRole(d.text)}
	}
	// An integer literal, arithmetic, a function, an aggregate, CASE: a value
	// PostgreSQL types plain numeric (or an integer), printed at its own scale.
	return setOpArmFact{role: setOpMarkNeutral}
}

// setOpLiteralTextRole: a literal spelled with trailing fraction zeros
// (`2.50`, `'1.50'`) is printed with them by PostgreSQL, so trimming would
// change its text; any other spelling is neutral.
func setOpLiteralTextRole(text string) setOpMarkRole {
	t := strings.TrimSpace(text)
	if strings.ContainsAny(t, "eE") || !strings.Contains(t, ".") {
		return setOpMarkNeutral
	}
	if strings.HasSuffix(t, "0") {
		return setOpMarkVeto
	}
	return setOpMarkNeutral
}

// setOpArmMarkRole resolves one arm's role against its declared column.
func setOpArmMarkRole(ct SetOpColType, f setOpArmFact) setOpMarkRole {
	if f.role != setOpMarkByDecl {
		return f.role
	}
	switch {
	case ct.fold != setOpMarkByDecl:
		return ct.fold
	case !ct.Known || ct.Typ != parquet.TypeDecimal:
		return setOpMarkNeutral
	case ct.DecKnown && ct.Dec.Unconstrained:
		return setOpMarkMarked
	case !ct.DecKnown || ct.Dec.Scale > 0:
		return setOpMarkVeto
	}
	return setOpMarkNeutral
}

// setOpResultColumn is THE rule for one result column of a set operation:
// its type, its DECIMAL (precision, scale) and its mark, from every arm's
// declared column and that arm's facts. The stage planner (setOpTargetType),
// the single-process path (unifySetOpSchemas) and the declared output
// (setOpDeclaredOutputSchema) all call it and make no decision of their own,
// so one query has one result column on every path.
//
// allKnown is false when a typed arm carries no type at all. An error is a
// pair the numeric ladder cannot meet (the plan-time refusal,
// setOpArmTypeConflict, normally raises it first). A DECIMAL whose (p,s) the
// arms do not resolve comes back with DecKnown false and, when every typed
// arm is a DECIMAL, Dec set to the widest precision and scale (#532: no arm's
// digits dropped) for the path that executes over runtime columns.
func setOpResultColumn(arms []SetOpColType, facts []setOpArmFact, name, op string) (SetOpColType, bool, error) {
	fact := func(i int) setOpArmFact {
		if i < len(facts) {
			return facts[i]
		}
		return setOpArmFact{}
	}
	// An untyped item takes the other arms' type. A weak one (a NULL cast to
	// plain NUMERIC) is typed by its declaration only when nothing else is.
	untyped := func(i int) bool { return fact(i).untyped }
	strong := false
	for i := range arms {
		strong = strong || !fact(i).untyped
	}
	if !strong {
		untyped = func(i int) bool { return fact(i).untyped && !fact(i).weak }
	}
	var want SetOpColType
	allKnown := true
	for i, ct := range arms {
		if untyped(i) {
			continue
		}
		if !ct.Known {
			allKnown = false
			continue
		}
		if !want.Known {
			want = ct
			continue
		}
		widened, ok := setOpWiden(want.Typ, ct.Typ)
		if !ok {
			if setOpNoCommonType(want.Typ, ct.Typ) {
				// PostgreSQL's own 42804. SetOpArmTypeConflict raises the same
				// refusal before any stage is emitted and the single-process
				// path calls it too, so one query takes one answer; this is
				// the backstop for a shape that reaches here without it.
				return SetOpColType{}, false, setOpTypeMismatch(op, name, want.Typ, ct.Typ)
			}
			// A pair PostgreSQL DOES match, on a ladder that does not reach it
			// (PORT beside an integer, two members of the inet family, DATE
			// beside a TIMESTAMP): the same carrier refusal
			// SetOpArmTypeConflict raises ahead of this walk.
			return SetOpColType{}, false, setOpCarrierGap(name, want.Typ, ct.Typ)
		}
		elem, err := setOpElementTarget(want, ct, name, op)
		if err != nil {
			return SetOpColType{}, false, err
		}
		want = SetOpColType{Typ: widened, Known: true, Fields: want.Fields, ElementType: elem}
	}
	want.fold = setOpMarkByDecl
	if !want.Known || want.Typ != parquet.TypeDecimal || !allKnown {
		want.Dec.Unconstrained = false
		return want, allKnown, nil
	}
	dec := make([]SetOpColType, 0, len(arms))
	allDecimal := true
	marked, veto := false, false
	for i, ct := range arms {
		switch setOpArmMarkRole(ct, fact(i)) {
		case setOpMarkMarked:
			marked = true
		case setOpMarkVeto:
			veto = true
		}
		if untyped(i) {
			// No type of its own, so no (p,s) either: counting its STRING
			// made the target unresolvable and refused a union PostgreSQL
			// answers as numeric.
			continue
		}
		dec = append(dec, ct)
		allDecimal = allDecimal && ct.Typ == parquet.TypeDecimal
	}
	want.Dec, want.DecKnown = setOpDecimalTarget(dec)
	if !want.DecKnown {
		want.Dec = logical.DecimalMeta{}
		if allDecimal {
			for _, ct := range dec {
				want.Dec.Precision = max(want.Dec.Precision, ct.Dec.Precision)
				want.Dec.Scale = max(want.Dec.Scale, ct.Dec.Scale)
			}
		}
	}
	// Part of the node's TYPE, so a set operation nested as an arm, or read
	// through a derived table or CTE, reports it to the operation above
	// (setOpNodeResultTypes, setOpNodeDecls), and an arm whose mark the
	// result does not keep is coerced to it.
	want.Dec.Unconstrained = want.DecKnown && marked && !veto
	switch {
	case veto:
		want.fold = setOpMarkVeto
	case marked:
		want.fold = setOpMarkMarked
	default:
		want.fold = setOpMarkNeutral
	}
	return want, true, nil
}

// setOpColTypeOfColumn is a runtime or declared column as an arm of
// setOpResultColumn. A DECIMAL without a precision (#458's "unconstrained"
// sentinel) is reported unresolved rather than taken at face value: a set
// operation would otherwise widen every arm to scale 0 and truncate them.
func setOpColTypeOfColumn(c parquet.Column) SetOpColType {
	ct := SetOpColType{Typ: c.Type, Known: true, Fields: c.Fields, ElementType: c.ElementType}
	if c.Type == parquet.TypeDecimal {
		ct.Dec = logical.DecimalMeta{Precision: c.Precision, Scale: c.Scale, Unconstrained: c.Unconstrained}
		ct.DecKnown = c.Precision > 0
	}
	return ct
}

// setOpColumnFromResult writes a result column computed by setOpResultColumn
// onto the column the result is named after (the first arm's). ok=false
// means "leave base exactly as it is": no type was resolved, or nothing about
// the column changes.
func setOpColumnFromResult(base parquet.Column, want SetOpColType, err error) (parquet.Column, bool) {
	if err != nil || !want.Known {
		return parquet.Column{}, false
	}
	col := base
	if want.Typ == parquet.TypeDecimal {
		if !want.DecKnown && want.Dec.Precision <= 0 {
			// No scale to move the arms to.
			return parquet.Column{}, false
		}
		col.Type = parquet.TypeDecimal
		col.Precision, col.Scale = want.Dec.Precision, want.Dec.Scale
	} else if want.Typ != base.Type {
		col.Type = want.Typ
		col.Precision, col.Scale = 0, 0
	}
	if want.ElementType != nil && base.ElementType != nil && (want.ElementType.Type != base.ElementType.Type ||
		want.ElementType.Precision != base.ElementType.Precision || want.ElementType.Scale != base.ElementType.Scale) {
		col.ElementType = want.ElementType
	}
	col.Unconstrained = want.Typ == parquet.TypeDecimal && want.Dec.Unconstrained
	moved := col.Type != base.Type || col.Precision != base.Precision || col.Scale != base.Scale ||
		col.ElementType != base.ElementType
	if moved {
		// A set operation's output column takes a NULL from either arm, and
		// the widened column is rebuilt, so it is declared nullable.
		col.Nullable = true
		return col, true
	}
	return col, col.Unconstrained != base.Unconstrained
}
