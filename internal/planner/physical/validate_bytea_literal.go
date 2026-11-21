// SPDX-License-Identifier: MIT

package physical

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// refuseByteaLiterals is the plan-time half of #1590: an unknown-typed
// (quoted) literal that a construct resolves to bytea is read by byteain
// while PostgreSQL analyses the statement, so text byteain refuses — an odd
// or invalid hex digit (22023), a stray backslash (22P02) — is refused before
// any row, whether or not a row ever reads it: `SELECT COALESCE(b, '\x68zz')
// FROM t WHERE false`, a CASE arm no row takes and a LAG default no row
// fills are all refused on 17.11 (measured). The plan's CAST
// (typeByteaLiterals) refuses only where a value is read.
//
// The constructs are typeByteaLiterals' own — COALESCE / IFNULL / GREATEST /
// LEAST / NULLIF, position, a CASE's results and a simple CASE's subject and
// WHENs, `||`, the comparisons, IN and BETWEEN, and LAG / LEAD's value and
// default — and the rule is the same: every operand that is not an unknown
// literal is one this scope can prove is bytea. An operand it cannot type
// takes the refusal away (the binder's standing contract: a false positive
// breaks a working query). The text test is kernel.ByteaIn, the one reading
// the CAST applies, so the two cannot disagree.
func refuseByteaLiterals(e plansql.Node, scope *colScope) error {
	var err error
	var walk func(plansql.Node)
	walk = func(n plansql.Node) {
		if n == nil || err != nil {
			return
		}
		var kids []plansql.Node
		if w, ok := n.(*plansql.WindowFuncNode); ok {
			if w.Func != nil {
				kids = w.Func.Args
				switch strings.ToLower(w.Func.Name) {
				case "lag", "lead":
					if len(w.Func.Args) >= 3 {
						err = refuseByteaAmong(scope, []plansql.Node{w.Func.Args[0], w.Func.Args[2]})
					}
				}
			}
		} else {
			kids = exprOperands(n)
		}
		for _, c := range kids {
			walk(c)
		}
		if err != nil {
			return
		}
		switch v := n.(type) {
		case *plansql.FuncCallNode:
			switch strings.ToLower(v.Name) {
			case "coalesce", "ifnull", "greatest", "least", "nullif", "position", "strpos":
				err = refuseByteaAmong(scope, v.Args)
			}
		case *plansql.CaseNode:
			results := make([]plansql.Node, 0, len(v.Whens)+1)
			for _, w := range v.Whens {
				results = append(results, w.Result)
			}
			if v.Else != nil {
				results = append(results, v.Else)
			}
			if err = refuseByteaAmong(scope, results); err == nil && v.Subject != nil {
				subj := []plansql.Node{v.Subject}
				for _, w := range v.Whens {
					subj = append(subj, w.Cond)
				}
				err = refuseByteaAmong(scope, subj)
			}
		case *plansql.BinaryOp:
			if v.Op == "||" {
				err = refuseByteaAmong(scope, []plansql.Node{v.Left, v.Right})
			}
		case *plansql.CmpExpr:
			err = refuseByteaAmong(scope, []plansql.Node{v.Left, v.Right})
		case *plansql.InExpr:
			err = refuseByteaAmong(scope, append([]plansql.Node{v.Left}, v.Values...))
		case *plansql.BetweenExpr:
			err = refuseByteaAmong(scope, []plansql.Node{v.Left, v.Low, v.High})
		}
	}
	walk(e)
	return err
}

// refuseByteaAmong raises byteain's refusal of the first quoted literal among
// ops when every other operand is provably bytea (at least one).
func refuseByteaAmong(scope *colScope, ops []plansql.Node) error {
	// No quoted literal among them: nothing to refuse, and no operand is
	// typed to find that out.
	anyLit := false
	for _, o := range ops {
		if l, ok := unwrapParens(o).(*plansql.Lit); ok && l.Kind == plansql.LitString {
			anyLit = true
			break
		}
	}
	if !anyLit {
		return nil
	}
	bytea := false
	var lits []*plansql.Lit
	for _, o := range ops {
		if o == nil {
			return nil
		}
		if l, ok := unwrapParens(o).(*plansql.Lit); ok {
			switch l.Kind {
			case plansql.LitString:
				lits = append(lits, l)
				continue
			case plansql.LitNull:
				continue
			}
			return nil
		}
		if !byteaOperand(scope, o) {
			return nil
		}
		bytea = true
	}
	if !bytea {
		return nil
	}
	for _, l := range lits {
		if _, err := kernel.ByteaIn(l.Value); err != nil {
			return err
		}
	}
	return nil
}

// byteaOperand says this scope can prove the operand is bytea: a column (or
// a COALESCE-family / CASE fold argDeclaredType types), a CAST to BYTES, or a
// `||` with a bytea operand and no other kind beside it.
func byteaOperand(scope *colScope, n plansql.Node) bool {
	switch v := unwrapParens(n).(type) {
	case *plansql.CastNode:
		return isByteaTypeName(v.TypeName)
	case *plansql.BinaryOp:
		if v.Op != "||" {
			return false
		}
		have := false
		for _, o := range []plansql.Node{v.Left, v.Right} {
			if l, ok := unwrapParens(o).(*plansql.Lit); ok && (l.Kind == plansql.LitString || l.Kind == plansql.LitNull) {
				continue
			}
			if !byteaOperand(scope, o) {
				return false
			}
			have = true
		}
		return have
	}
	t, k := argDeclaredType(scope, n)
	return k == argTyped && t == parquet.TypeBytes
}

// refuseByteaSetOpLiterals is refuseByteaLiterals for a set operation: a
// quoted literal select item of one arm, in a column the other arm declares
// bytea, is read by byteain while the statement is analysed — `SELECT b FROM
// t UNION ALL SELECT '\x68zz' FROM t WHERE false` is 22023 on 17.11.
func refuseByteaSetOpLiterals(l, r *plansql.SelectInfo, ld, rd []expr.DeclType) error {
	check := func(arm *plansql.SelectInfo, other []expr.DeclType) error {
		if arm == nil || arm.Union != nil {
			return nil
		}
		for i, c := range arm.Columns {
			if i >= len(other) || other[i].Untyped || other[i].Quoted || other[i].ID != parquet.TypeBytes {
				continue
			}
			lit, ok := unwrapParens(c.ASTExpr).(*plansql.Lit)
			if !ok || lit.Kind != plansql.LitString {
				continue
			}
			if _, err := kernel.ByteaIn(lit.Value); err != nil {
				return err
			}
		}
		return nil
	}
	if err := check(r, ld); err != nil {
		return err
	}
	return check(l, rd)
}

func isByteaTypeName(s string) bool {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "BYTES", "BYTEA":
		return true
	}
	return false
}
