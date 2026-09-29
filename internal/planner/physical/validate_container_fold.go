// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// refuseContainerFold checks a common result type for container folds.
// CASE reads ELSE first; the first incompatible arm names the error pair.
// Different container kinds raise 42804 and differing ROW shapes 42846.
// Malformed ROW/ARRAY text raises 22P02; valid container text raises 0A000
// until this fold can convert it. Unknown arm types defer (docs/adr/0012-divergences/comparison-membership.md).
func refuseContainerFold(node plansql.Node, typeOf func(plansql.Node) (parquet.Column, bool)) error {
	switch n := node.(type) {
	case nil, *plansql.SubqueryNode, *plansql.ExistsNode:
		return nil
	case *plansql.WindowFuncNode:
		if n.Func != nil {
			return refuseContainerFold(n.Func, typeOf)
		}
		return nil
	}
	for _, child := range exprOperands(node) {
		if err := refuseContainerFold(child, typeOf); err != nil {
			return err
		}
	}
	switch n := node.(type) {
	case *plansql.CaseNode:
		arms := make([]plansql.Node, 0, len(n.Whens)+1)
		if n.Else != nil {
			arms = append(arms, n.Else)
		}
		for _, w := range n.Whens {
			arms = append(arms, w.Result)
		}
		return refuseFoldArms("CASE", arms, typeOf)
	case *plansql.FuncCallNode:
		switch name := strings.ToLower(n.Name); name {
		case "coalesce", "ifnull", "greatest", "least":
			kind := strings.ToUpper(name)
			if name == "ifnull" {
				kind = "COALESCE"
			}
			return refuseFoldArms(kind, n.Args, typeOf)
		}
	}
	return nil
}

func isContainerType(t parquet.TypeID) bool {
	switch t {
	case parquet.TypeRow, parquet.TypeArray, parquet.TypeMap, parquet.TypeVector:
		return true
	}
	return false
}

func refuseFoldArms(kind string, arms []plansql.Node, typeOf func(plansql.Node) (parquet.Column, bool)) error {
	var (
		common parquet.Column
		have   bool
	)
	var lits []*plansql.Lit
	for _, a := range arms {
		if lit, ok := plansql.Unparen(a).(*plansql.Lit); ok {
			if lit.Kind == plansql.LitString {
				lits = append(lits, lit)
			}
			continue
		}
		col, ok := typeOf(a)
		if !ok {
			continue
		}
		if !have {
			common, have = col, true
			continue
		}
		if _, ok := batch.TemporalCommonType(common.Type, col.Type); ok {
			return temporalFoldGap(kind, common, col)
		}
		if !isContainerType(common.Type) && !isContainerType(col.Type) {
			continue
		}
		if foldCompatible(common, col) {
			continue
		}
		if common.Type == parquet.TypeRow && col.Type == parquet.TypeRow {
			// Two ROW SHAPES: PostgreSQL's two composite types, and its
			// 42846 — measured, `COALESCE could not convert type rownt to
			// rowt` (the later arm to the type resolved so far).
			k := kind
			if k == "CASE" {
				k = "CASE/WHEN"
			}
			return sqlerr.New("42846", "%s could not convert type %s to %s", k, foldTypeName(col), foldTypeName(common))
		}
		return sqlerr.New("42804", "%s types %s and %s cannot be matched", kind, foldTypeName(common), foldTypeName(col))
	}
	if !have {
		return nil
	}
	for _, lit := range lits {
		v := strings.TrimSpace(lit.Value)
		switch common.Type {
		case parquet.TypeRow:
			if !strings.HasPrefix(v, "(") {
				return sqlerr.New("22P02", "malformed record literal: %q", lit.Value)
			}
			return sqlerr.New("0A000", "a quoted ROW literal in %s is not supported", kind)
		case parquet.TypeArray:
			if !strings.HasPrefix(v, "{") {
				return sqlerr.New("22P02", "malformed array literal: %q", lit.Value)
			}
			return sqlerr.New("0A000", "a quoted ARRAY literal in %s is not supported", kind)
		}
	}
	return nil
}

// temporalFoldGap refuses a CASE / COALESCE / GREATEST / LEAST whose arms
// mix DATE and TIMESTAMP. PostgreSQL resolves the pair to timestamp
// (batch.TemporalCommonType) and answers; here the result was declared by the
// first arm (expr.CommonDeclType names no temporal rung) and the winning
// arm's box was handed on as it stood (choiceBox has no temporal mode), so a
// DATE arm's day count landed in a TIMESTAMP vector (`19786` for 2024-03-04)
// or a TIMESTAMP's milliseconds overflowed a DATE one (22003) — never
// PostgreSQL's instant (#1378). Loud until the choice fold carries the rung.
func temporalFoldGap(kind string, a, b parquet.Column) error {
	return sqlerr.New("0A000", "%s types %s and %s are not supported together: PostgreSQL "+
		"resolves them to timestamp without time zone, and this engine does not yet carry a "+
		"DATE arm into a TIMESTAMP result; CAST the DATE arm to TIMESTAMP",
		kind, pgTypeName(a.Type), pgTypeName(b.Type))
}

// foldCompatible reports whether two arms fold to one container type: the same
// kind, and for a ROW the same field count with the same field types in order.
// A ROW whose fields this layer does not know decides nothing.
func foldCompatible(a, b parquet.Column) bool {
	if a.Type != b.Type {
		return false
	}
	if a.Type != parquet.TypeRow || len(a.Fields) == 0 || len(b.Fields) == 0 {
		return true
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		if a.Fields[i].Type != b.Fields[i].Type {
			return false
		}
	}
	return true
}

// foldTypeName is the type's PostgreSQL name, with a ROW's shape spelled out —
// this engine's ROW has no type name of its own, and two ROWs that cannot be
// matched would otherwise read `record and record`.
func foldTypeName(c parquet.Column) string {
	if c.Type == parquet.TypeRow && len(c.Fields) > 0 {
		parts := make([]string, len(c.Fields))
		for i, f := range c.Fields {
			parts[i] = fmt.Sprintf("%s %s", f.Name, pgTypeName(f.Type))
		}
		return "record(" + strings.Join(parts, ", ") + ")"
	}
	if c.Type == parquet.TypeArray && c.ElementType != nil {
		return pgTypeName(c.ElementType.Type) + "[]"
	}
	return pgTypeName(c.Type)
}

// foldArmTypeOf is the choice-fold refusal's arm typing: an arm's DECLARED
// type, whatever the arm's shape. foldTypeOf's walk (nodeDeclaredType, the
// one the projection declares its column by) is handed the binder's answer
// for a SCALAR SUBQUERY — the body validated against this scope, correlated
// or not, and its one declared output column — and a WINDOW call is typed as
// the function it windows, `max(ts) OVER ()` the timestamp `max(ts)` is and
// `lag(ts) OVER (…)` the timestamp its argument is. An
// arm left untyped was skipped by refuseFoldArms, so `COALESCE(d, (SELECT
// max(ts) …))` and `COALESCE(d, max(ts) OVER ())` kept the first arm's DATE
// declaration and answered day counts on every arm where PostgreSQL answers
// timestamps (#1378 round 2).
//
// The widened typing answers only a DATE or a TIMESTAMP: the #1060 container
// rules share this walk and keep the untyped subquery / window arm they had,
// so `COALESCE(first_value(arr) OVER (…), '{9}')` is not refused as a quoted
// ARRAY literal beside an ARRAY arm (#1378 round 3).
func (b *binder) foldArmTypeOf(scope *colScope) func(plansql.Node) (parquet.Column, bool) {
	decls := rowFieldScopeDecls(scope)
	decls.subqueryDecl = func(sql string) (parquet.Column, bool) {
		// The body's own output declaration (binder.outputDecls: the
		// aggregate-aware walk the body's SELECT list is declared by, so
		// `max(ts)` is the timestamp it is).
		sub := b.validatedBody(sql, scope)
		if sub == nil {
			return parquet.Column{}, false
		}
		ds := b.outputDecls[sub]
		if len(ds) != 1 || ds[0].Untyped || !foldTemporal(ds[0].ID) {
			return parquet.Column{}, false
		}
		return parquet.Column{Type: ds[0].ID}, true
	}
	shape := foldTypeOf(decls)
	var typeOf func(plansql.Node) (parquet.Column, bool)
	typeOf = func(n plansql.Node) (parquet.Column, bool) {
		if w, ok := plansql.Unparen(n).(*plansql.WindowFuncNode); ok {
			if w.Func == nil {
				return parquet.Column{}, false
			}
			// A value function (lag, first_value, …) lifts its first
			// argument's value, and is declared by it (windowValueFunc).
			var c parquet.Column
			var ok bool
			if fc := w.Func; windowValueFunc(strings.ToLower(fc.Name)) && len(fc.Args) > 0 {
				c, ok = typeOf(fc.Args[0])
			} else {
				c, ok = typeOf(w.Func)
			}
			// Only the DATE / TIMESTAMP rule reads a window arm's type; the
			// #1060 container rules keep the untyped window arm they had.
			if !ok || !foldTemporal(c.Type) {
				return parquet.Column{}, false
			}
			return c, true
		}
		return shape(n)
	}
	return typeOf
}

// declareTemporalArmColumns completes a block's output declarations
// (binder.outputDecls) for the columns whose value is a scalar subquery or a
// window call — or an expression over one — that is a DATE or a TIMESTAMP.
// The declaration walk that fills outputDecls types neither, so a derived
// table's, a CTE's, a set operation's or a LATERAL output's column carrying
// one reached the scope untyped, and `COALESCE(s.d, s.mt)` over `(SELECT
// a.d, (SELECT max(ts) …) AS mt …) s` skipped the refusal and answered a
// TIMESTAMP's milliseconds read as days (`3338-12-14`), or a DATE's day
// count as milliseconds, on every arm (#1378 round 3). The column is typed
// by the same arm walk the refusal types a subquery or window arm with, and
// only a DATE or a TIMESTAMP is recorded — the one pair that walk is for.
func (b *binder) declareTemporalArmColumns(info *plansql.SelectInfo, scope *colScope) {
	ds := b.outputDecls[info]
	if len(ds) != len(info.Columns) {
		return
	}
	var typeOf func(plansql.Node) (parquet.Column, bool)
	for i, col := range info.Columns {
		if !ds[i].Untyped || col.Star || col.ASTExpr == nil {
			continue
		}
		if typeOf == nil {
			typeOf = b.foldArmTypeOf(scope)
		}
		if c, ok := typeOf(col.ASTExpr); ok && foldTemporal(c.Type) {
			ds[i] = expr.DeclType{ID: c.Type}
		}
	}
}

// recursiveTemporalDecls is a recursive CTE's published declarations as the
// choice refusal reads them: the NON-RECURSIVE term's (the left-most arm of
// the body's set operation — PostgreSQL types a recursive CTE's column by it,
// and refuses 42804 when the recursive term disagrees), DATE and TIMESTAMP
// only. A recursive CTE published no declaration at all, so `COALESCE(s.d,
// s.t)` over one skipped the refusal whatever its columns were (#1378 round
// 3); the other types stay undeclared as they were, and the recursive term's
// own references are validated before the declarations are published.
func recursiveTemporalDecls(outputDecls map[*plansql.SelectInfo][]expr.DeclType, body *plansql.SelectInfo) []expr.DeclType {
	anchor := body
	for anchor != nil && anchor.Union != nil {
		anchor = anchor.Union.Left
	}
	src := outputDecls[anchor]
	out := make([]expr.DeclType, len(src))
	for i, d := range src {
		out[i] = expr.DeclType{Untyped: true}
		if !d.Untyped && foldTemporal(d.ID) {
			out[i] = expr.DeclType{ID: d.ID}
		}
	}
	return out
}

// foldTemporal reports whether a widened arm type is one the DATE / TIMESTAMP
// refusal reads.
func foldTemporal(t parquet.TypeID) bool {
	return t == parquet.TypeDate || t == parquet.TypeTimestamp
}

// foldTypeOf types one fold arm with its SHAPE where the scope carries it: a
// column's full declaration (a ROW's fields), else the arm's declared type.
func foldTypeOf(decls ColDecls) func(plansql.Node) (parquet.Column, bool) {
	return func(n plansql.Node) (parquet.Column, bool) {
		if ref, ok := plansql.Unparen(n).(*plansql.ColRef); ok {
			return decls.colDecl(ref)
		}
		d, c := fieldContainerDeclaredType(n, decls)
		if c != expr.Decided || d.Untyped {
			return parquet.Column{}, false
		}
		if d.Schema != nil {
			col := *d.Schema
			col.Type = d.ID
			return col, true
		}
		return parquet.Column{Type: d.ID, Fields: d.RowFields()}, true
	}
}
