// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
)

// refuseByteaLikes is the binder's half of the bytea LIKE rule (#1593;
// expr.RefuseByteaLike has the rule and the reasoning): `vb LIKE pt` is 42883
// and `vb LIKE '\%'` is 22P02 while PostgreSQL analyses the statement, so they
// are refused over an empty table and under WHERE false — and on a stage DAG,
// whose fragments compile only when a task runs. It walks the expression the
// way walkExpr does and asks the one rule about each LIKE and each like_escape
// call, with the declarations the scope has.
func refuseByteaLikes(node plansql.Node, decls ColDecls) error {
	declOf := func(n plansql.Node) (expr.DeclType, expr.Confidence) {
		return fieldContainerDeclaredType(n, decls)
	}
	var walk func(n plansql.Node) error
	walk = func(n plansql.Node) error {
		switch v := n.(type) {
		case nil:
			return nil
		case *plansql.LikeExpr:
			if err := expr.RefuseByteaLike(v, declOf); err != nil {
				return err
			}
			if err := walk(v.Left); err != nil {
				return err
			}
			return walk(v.Pattern)
		case *plansql.FuncCallNode:
			if strings.EqualFold(strings.TrimSpace(v.Name), "like_escape") {
				if err := expr.RefuseByteaLike(v, declOf); err != nil {
					return err
				}
			}
			return walkAll(walk, v.Args...)
		case *plansql.BinaryOp:
			return walkAll(walk, v.Left, v.Right)
		case *plansql.UnaryOp:
			return walk(v.Inner)
		case *plansql.CmpExpr:
			return walkAll(walk, v.Left, v.Right)
		case *plansql.AndNode:
			return walkAll(walk, v.Left, v.Right)
		case *plansql.OrNode:
			return walkAll(walk, v.Left, v.Right)
		case *plansql.NotNode:
			return walk(v.Inner)
		case *plansql.ParenNode:
			return walk(v.Inner)
		case *plansql.CastNode:
			return walk(v.Inner)
		case *plansql.InExpr:
			if err := walk(v.Left); err != nil {
				return err
			}
			return walkAll(walk, v.Values...)
		case *plansql.BetweenExpr:
			return walkAll(walk, v.Left, v.Low, v.High)
		case *plansql.IsExpr:
			return walk(v.Left)
		case *plansql.CaseNode:
			if err := walk(v.Subject); err != nil {
				return err
			}
			for _, w := range v.Whens {
				if err := walkAll(walk, w.Cond, w.Result); err != nil {
					return err
				}
			}
			return walk(v.Else)
		case *plansql.AnyAllExpr:
			if err := walk(v.Left); err != nil {
				return err
			}
			return walkAll(walk, v.Values...)
		}
		return nil
	}
	return walk(node)
}

func walkAll(walk func(plansql.Node) error, nodes ...plansql.Node) error {
	for _, n := range nodes {
		if err := walk(n); err != nil {
			return err
		}
	}
	return nil
}
