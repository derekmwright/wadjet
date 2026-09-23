// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// refuseSetOpOrderBy holds a set operation's OWN ORDER BY to PostgreSQL's
// rule: a term is a RESULT COLUMN NAME or an ORDINAL and nothing else (#1236).
//
// The result of `a UNION b` has no FROM clause. PostgreSQL resolves each term
// first as an output name or position (findTargetlistEntrySQL92) and otherwise
// transforms it against a scope holding only the result columns, so, measured
// on 17.11, term by term in the order written:
//
//	ORDER BY zz.id / a.id / lat_ord.id   42P01 missing FROM-clause entry for table "zz"
//	ORDER BY nosuch, ORDER BY -nosuch    42703 column "nosuch" does not exist
//	ORDER BY id + 1, -id, SUM(id)        0A000 invalid UNION/INTERSECT/EXCEPT ORDER BY clause
//	ORDER BY id, 1, "V" for AS "V"       answered
//
// A qualified term is refused even when the qualifier names a relation one of
// the ARMS reads: an arm's FROM is out of scope above the operation. Before
// this, validateBlock returned from its set-operation branch before any clause
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
	names, known := b.blockColumns(ctx, first)
	isName := func(ref *plansql.ColRef) bool {
		for _, n := range names {
			// Case-insensitively, and that is a deliberate leniency: the
			// published names arrive FOLDED, so a delimited alias (`AS "V"`)
			// cannot be told from an unquoted one here, and refusing `ORDER BY
			// "V"` — which PostgreSQL answers — would be a false positive. The
			// cost is that `ORDER BY "ID"` over an output `id` still answers.
			if strings.EqualFold(n, ref.Column) {
				return true
			}
		}
		return false
	}
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
				return sqlerr.New("42P01", "missing FROM-clause entry for table %q", r.Table)
			}
			if known && !isName(r) {
				return sqlerr.New("42703", "column %q does not exist", r.Column)
			}
		}
		if ref, ok := term.(*plansql.ColRef); ok && ref.Table == "" {
			continue
		}
		if !known && len(refs) > 0 {
			continue
		}
		// The term is TRANSFORMED before it is judged, so what parse analysis
		// refuses inside it comes first: a function no one implements (42883)
		// and a constant flag name or semver range that names nothing (22023)
		// — the refusals every other ORDER BY term gets from checkExpr.
		var calls []*plansql.FuncCallNode
		walkExpr(term, nil, nil, &calls)
		for _, fc := range calls {
			if !plansql.IsAggregate(fc.Name) {
				if err := expr.ResolveFuncName(fc.Name); err != nil {
					return err
				}
			}
		}
		if err := refuseUnknownFlagNames(term); err != nil {
			return err
		}
		if err := refuseInvalidSemverRanges(term); err != nil {
			return err
		}
		return sqlerr.New("0A000", "invalid UNION/INTERSECT/EXCEPT ORDER BY clause")
	}
	return nil
}
