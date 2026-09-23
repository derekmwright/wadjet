// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// refuseSetOpOrderBy holds a set operation's OWN ORDER BY to PostgreSQL's
// rule: a term is a RESULT COLUMN NAME or an ORDINAL and nothing else (#1236).
//
// The result of `a UNION b` has no FROM clause. PostgreSQL resolves each term
// first as an output name or position (findTargetlistEntrySQL92) and otherwise
// transforms it against a scope holding only the result columns, so, measured
// on 17.11, term by term in the order written:
//
//	ORDER BY zz.id / lat_ord.id / b.id   42P01 missing FROM-clause entry for table "zz"
//	ORDER BY nosuch, ORDER BY "ID"       42703 column "nosuch" does not exist
//	ORDER BY id + 1, -id, SUM(id)        0A000 invalid UNION/INTERSECT/EXCEPT ORDER BY clause
//	ORDER BY id = true / id = 'x'        42883 / 22P02 — the transform's error first
//	ORDER BY id, 1, "V" for AS "V"       answered
//
// One qualified spelling is KEPT beyond PostgreSQL: a qualifier naming the
// FIRST arm's selected `q.col` (setOpResult.qualified). Every other qualifier
// is refused — an arm's FROM is out of scope above the operation. Before this,
// validateBlock returned from its set-operation branch before any clause
// check ran, the qualifier was dropped, and `… UNION ALL … ORDER BY zz.id`
// answered every row sorted by `id`; the expression and unknown-name terms
// failed in the executor with an internal "sort: key column … does not exist".
//
// The output names are the FIRST arm's, which is PostgreSQL's naming rule for
// a set operation. When they cannot be enumerated (a star over a source this
// binder cannot read) only the qualifier rule is applied — it asks nothing of
// the names — and everything else is left to the pipeline, as before.
func (b *binder) refuseSetOpOrderBy(ctx context.Context, info *plansql.SelectInfo) error {
	if info == nil || info.Union == nil || len(info.OrderBy) == 0 {
		return nil
	}
	first := info.Union.Left
	for first != nil && first.Union != nil {
		first = first.Union.Left
	}
	res := b.setOpResultNames(ctx, first)
	for _, ob := range info.OrderBy {
		if ob.Ordinal > 0 {
			continue
		}
		term := plansql.Unparen(ob.Expr)
		if term == nil {
			continue
		}
		if _, ok := term.(*plansql.Lit); ok {
			// A POSITION is resolved (and range-checked) by the parser; any
			// other constant is PostgreSQL's 42601, not this rule's.
			continue
		}
		var refs []*plansql.ColRef
		walkExpr(term, &refs, nil, nil)
		for _, r := range refs {
			if r.Table != "" {
				if !res.qualified(r) {
					return sqlerr.New("42P01", "missing FROM-clause entry for table %q", r.Table)
				}
				continue
			}
			if res.known && !res.name(r) {
				return sqlerr.New("42703", "column %q does not exist", r.Column)
			}
		}
		if _, ok := term.(*plansql.ColRef); ok {
			continue
		}
		if !res.known && len(refs) > 0 {
			continue
		}
		if err := b.transformSetOpOrderTerm(term, res); err != nil {
			return err
		}
		return sqlerr.New("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause")
	}
	return nil
}

// setOpResult is a set operation's result columns as its ORDER BY sees them:
// the FIRST arm's names — exactly as written where the arm lists them (an
// alias `AS "V"` keeps its case), folded where a star publishes them — their
// structural types, and the one qualified spelling kept as a superset.
type setOpResult struct {
	known bool
	exact bool
	names []string
	types []parquet.TypeID
	// quals[i] is the relation qualifier of result column i when the first
	// arm SELECTS it as `q.col` under its own name, else "".
	quals []string
}

func (b *binder) setOpResultNames(ctx context.Context, first *plansql.SelectInfo) setOpResult {
	var r setOpResult
	folded, known := b.blockColumns(ctx, first)
	r.known = known
	r.names = folded
	if st := b.structural[first]; len(st) == len(folded) {
		r.types = st
	}
	if _, star := blockOutputs(first); star || len(first.Columns) != len(folded) {
		return r
	}
	r.exact = true
	r.names = make([]string, len(folded))
	r.quals = make([]string, len(folded))
	for i, col := range first.Columns {
		r.names[i] = folded[i]
		ref, isRef := plansql.Unparen(col.ASTExpr).(*plansql.ColRef)
		switch {
		case col.Alias != "":
			r.names[i] = col.Alias
		case isRef:
			r.names[i] = ref.Column
		}
		if isRef && ref.Table != "" && r.names[i] == ref.Column {
			r.quals[i] = ref.Table
		}
	}
	return r
}

// name reports whether a bare reference names a result column. Where the
// first arm's names are known as written the match is EXACT — PostgreSQL's:
// `ORDER BY "ID"` over a result `id` is 42703, and so is `ORDER BY v` over
// `AS "V"`. A star's published names arrive folded and are matched without
// case.
func (r setOpResult) name(ref *plansql.ColRef) bool {
	for _, n := range r.names {
		if (r.exact && n == ref.Column) || (!r.exact && strings.EqualFold(n, ref.Column)) {
			return true
		}
	}
	return false
}

// qualified reports whether `q.col` names a result column the FIRST arm
// selected as `q.col` under its own name — the one qualified spelling kept as
// a superset (ADR-0012 §5): PostgreSQL raises 42P01 for every qualifier here,
// and the base engine answered this one correctly, identically on all five
// arms (`SELECT a.id … UNION ALL … ORDER BY a.id DESC`, arc BR round 2). A
// qualifier naming anything else — nothing, an alias-hidden table, a right
// arm's relation — stays refused.
func (r setOpResult) qualified(ref *plansql.ColRef) bool {
	for i, q := range r.quals {
		if q != "" && strings.EqualFold(q, ref.Table) && r.names[i] == ref.Column {
			return true
		}
	}
	return false
}

// transformSetOpOrderTerm does what PostgreSQL's transform does to an ORDER BY
// EXPRESSION before it refuses it with 0A000, so the error a client sees is
// the transform's where the transform fails (measured on 17.11): an unknown
// function or a text-only function over another type (42883), an operator
// between incompatible operands (42883), an unknown literal the operand type
// cannot read (22P02), an impossible cast (42846), a constant flag name or
// semver range that names nothing (22023). The scope is the result columns.
func (b *binder) transformSetOpOrderTerm(term plansql.Node, res setOpResult) error {
	var calls []*plansql.FuncCallNode
	walkExpr(term, nil, nil, &calls)
	for _, fc := range calls {
		if !plansql.IsAggregate(fc.Name) {
			if err := expr.ResolveFuncName(fc.Name); err != nil {
				return err
			}
		}
	}
	scope := newColScope()
	for i, n := range res.names {
		t := typeAmbiguous
		if i < len(res.types) {
			t = res.types[i]
		}
		q := ""
		if i < len(res.quals) {
			q = res.quals[i]
		}
		if t == typeAmbiguous {
			scope.addQualified(q, n)
		} else {
			scope.addQualifiedTyped(q, n, t)
		}
	}
	if err := b.checkExpr(term, scope); err != nil {
		return err
	}
	typeOf := structuralTypeOf(rowFieldScopeDecls(scope))
	for _, fc := range calls {
		sig, ok := expr.SignatureOf(strings.ToLower(fc.Name))
		if !ok {
			continue
		}
		for i, a := range fc.Args {
			t, ok := typeOf(a)
			if ok && sig.DomainAt(i) == expr.ArgTextOrBytes && t != parquet.TypeString && t != parquet.TypeBytes ||
				ok && sig.DomainAt(i) == expr.ArgText && t != parquet.TypeString {
				args := make([]string, len(fc.Args))
				for j, x := range fc.Args {
					args[j] = aggArgTypeName(x, typeOf)
				}
				return sqlerr.New("42883", "function %s(%s) does not exist", strings.ToLower(fc.Name), strings.Join(args, ", "))
			}
		}
	}
	var casts []*plansql.CastNode
	collectCasts(term, &casts)
	for _, c := range casts {
		from, ok := typeOf(c.Inner)
		if !ok {
			continue
		}
		to, ok := structuralCastType(c.TypeName)
		if ok && pgCastMissing(from, to) {
			return sqlerr.New("42846", "cannot cast type %s to %s", pgTypeName(from), pgTypeName(to))
		}
	}
	return nil
}

func collectCasts(n plansql.Node, out *[]*plansql.CastNode) {
	if c, ok := n.(*plansql.CastNode); ok {
		*out = append(*out, c)
	}
	for _, child := range exprOperands(n) {
		collectCasts(child, out)
	}
}

// pgCastMissing is the narrow set of casts PostgreSQL 17.11 has no function
// for that this layer can name with certainty: bigint/real/double/numeric
// to and from boolean (int4 has one), and a number to or from a date or
// timestamp. Anything else is left to the 0A000 that follows.
func pgCastMissing(from, to parquet.TypeID) bool {
	num := func(t parquet.TypeID) bool {
		switch t {
		case parquet.TypeInt64, parquet.TypeFloat32, parquet.TypeFloat64, parquet.TypeDecimal:
			return true
		}
		return false
	}
	temporal := func(t parquet.TypeID) bool { return t == parquet.TypeDate || t == parquet.TypeTimestamp }
	switch {
	case num(from) && to == parquet.TypeBool, from == parquet.TypeBool && num(to):
		return true
	case (num(from) || from == parquet.TypeInt32) && temporal(to), temporal(from) && (num(to) || to == parquet.TypeInt32):
		return true
	}
	return false
}
