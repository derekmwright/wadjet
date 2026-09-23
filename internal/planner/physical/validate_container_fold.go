// SPDX-License-Identifier: MIT

package physical

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// A CONTAINER FOLDED WITH SOMETHING IT CANNOT BE IS REFUSED, NOT RENDERED
// (#1060).
//
// CASE, COALESCE, GREATEST and LEAST resolve ONE result type over their arms
// (PostgreSQL's select_common_type). Where the arms put a ROW, an ARRAY, a MAP
// or a VECTOR beside a value of another kind — or two ROWs of different
// shapes — this engine took its STRING fallback and answered a column holding
// both `alice`-style strings and Go-rendered maps (`map[build: major:1 …]`)
// under OID 25; `COALESCE(c_arr, id)` and `CASE … THEN c_row ELSE <text>` failed
// mid-execution with the #361 silent-write guard instead. PostgreSQL 17.11
// refuses all of them at parse analysis (measured, arc BR's notes):
//
//	COALESCE(c_row, c_arr)                     42804 COALESCE types rowt and text[] cannot be matched
//	COALESCE(c_row, id)                        42804 COALESCE types rowt and bigint cannot be matched
//	CASE WHEN … THEN c_row ELSE CAST(id AS TEXT) END
//	                                           42804 CASE types text and rowt cannot be matched
//	CASE WHEN … THEN c_row ELSE c_rownest END  42846 CASE/WHEN could not convert type rowt to rownt
//	COALESCE(c_row, 'x')                       22P02 malformed record literal: "x"
//	GREATEST(c_arr, 'x')                       22P02 malformed array literal: "x"
//
// The pair a message names is PostgreSQL's: the type resolved so far against
// the first arm that cannot join it, with a CASE's ELSE read FIRST (the
// server puts the default result at the head of the list). Two ROWs of
// different shapes are 42804 here — this engine's ROW is a structural type
// with no name for the server's 42846 sentence to carry — and the message
// spells each shape. A QUOTED literal beside a ROW or an ARRAY is that
// container's own input: text not written in its grammar (`(…)`, `{…}`) is
// PostgreSQL's 22P02, and text that is is 0A000 here — PostgreSQL reads it and
// this fold answered the literal's TEXT as the value (`GREATEST(c_arr, '{x}')`
// answered the string `{x}`). An arm this layer cannot type decides nothing.
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
		if !isContainerType(common.Type) && !isContainerType(col.Type) {
			continue
		}
		if foldCompatible(common, col) {
			continue
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
