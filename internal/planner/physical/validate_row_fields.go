// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Resolve the container from the binder scope, including ROW child declarations.
func rowFieldScopeDecls(scope *colScope) ColDecls {
	decls := ColDecls{}
	if scope != nil {
		decls.Types = map[string]parquet.TypeID{}
		for name, t := range scope.colTypes {
			if t != typeAmbiguous {
				decls.Types[name] = t
			}
		}
		for q, cols := range scope.qualColTypes {
			for name, t := range cols {
				decls.Types[q+"."+name] = t
			}
		}
		decls.Fields = map[string][]parquet.Column{}
		decls.Dec = map[string]logical.DecimalMeta{}
		for k, v := range scope.rowFields {
			decls.Fields[k] = v
		}
		for k, d := range scope.fieldDecls {
			if !strings.Contains(k, ".") && scope.srcCount[k] > 1 {
				continue
			}
			decls.Types[k] = d.ID
			decls.Fields[k] = d.RowFields()
			if d.ID == parquet.TypeDecimal {
				col := declTypeParts(d)
				if d.Schema != nil {
					col = *d.Schema
				}
				if col.Precision > 0 {
					decls.Dec[k] = logical.DecimalMeta{Precision: col.Precision, Scale: col.Scale}
				}
			}
		}
	}
	return decls
}

func (s *colScope) addFieldDecl(qual, name string, d expr.DeclType) {
	if s.fieldDecls == nil {
		s.fieldDecls = map[string]expr.DeclType{}
	}
	s.fieldDecls[strings.ToLower(name)] = d
	s.fieldDecls[strings.ToLower(qual+"."+name)] = d
}

func refuseInvalidRowFields(node plansql.Node, scope *colScope) error {
	decls := rowFieldScopeDecls(scope)
	var calls []*plansql.FuncCallNode
	walkExpr(node, nil, nil, &calls)
	for _, fc := range calls {
		declOf := func(n plansql.Node) (expr.DeclType, expr.Confidence) {
			return fieldContainerDeclaredType(n, decls)
		}
		if err := expr.RefuseInvalidFixedRowField(fc, declOf); err != nil {
			return err
		}
		// THE BINDER'S HALF of the signature check, and the half that decides
		// whether the query is refused at all.
		//
		// PostgreSQL resolves a function during parse analysis, so
		// `SELECT upper(a, b) FROM t WHERE false` is 42883 there and does not
		// wait for a row. Folding that into expr.compileFuncCallNamed alone
		// gives it on the single-process path, where Plan compiles the whole
		// expression tree while it builds the physical plan — and NOT on the
		// stage DAG, where a stage's fragment compiles its own expressions
		// WHEN A TASK RUNS, so a position whose stage receives no rows is
		// never compiled and the wrong-arity call answers NULL. That is
		// refuseUnknownFlagNames's argument verbatim (#1018 round 6, B1), for
		// the same walk and the same four positions.
		//
		// It rides THIS walk rather than a new one because this is the walk
		// that already has the declarations: an argument DOMAIN is a question
		// about a column's type, which refuseUnknownFlagNames deliberately
		// does not carry.
		if err := expr.RefuseUnresolvableCall(fc, declOf); err != nil {
			return err
		}
	}
	return nil
}

// Aggregates are declared by the aggregate layer, not the scalar registry.
// Ask that layer for their input-dependent result before checking a postfix.
func fieldContainerDeclaredType(node plansql.Node, decls ColDecls) (expr.DeclType, expr.Confidence) {
	if call, ok := node.(*plansql.FuncCallNode); ok && plansql.IsAggregate(call.Name) {
		switch strings.ToLower(call.Name) {
		case "sum", "avg", "min", "max", "min_by", "max_by":
			if len(call.Args) == 0 {
				return expr.DeclType{}, expr.Undecided
			}
			in, c := nodeDeclaredType(call.Args[0], decls)
			if c != expr.Decided {
				return expr.DeclType{}, expr.Undecided
			}
			typ, p, s, known := aggOutputFromInputDecl(call.Name, call.Distinct, in.ID, in.Precision, in.Scale, aggInputIsWideInteger(call.Args[0], decls))
			if !known {
				return expr.DeclType{}, expr.Undecided
			}
			if typ == parquet.TypeDecimal {
				return expr.DeclDecimal(p, s), expr.Decided
			}
			if typ == parquet.TypeRow {
				return in, expr.Decided
			}
			return expr.Decl(typ), expr.Decided
		default:
			return expr.Decl(aggOutputType(call.Name, call.Distinct)), expr.Decided
		}
	}
	return nodeDeclaredType(node, decls)
}
