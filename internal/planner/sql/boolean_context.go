package sql

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// CoerceBooleanLiterals resolves an UNKNOWN-typed literal used as a truth
// value, which is the one shape a boolean context accepts that is not already
// a boolean (#599).
//
// PostgreSQL types a bare quoted literal from the context it meets, so a
// boolean context runs it through the boolean input function: measured live on
// postgres:17-alpine, `SELECT 1 WHERE 'true'`, `'yes'`, `'t'` and `' 1 '` all
// SUCCEED, `WHERE NULL` succeeds and returns nothing, and `WHERE 'abc'` is
// 22P02 `invalid input syntax for type boolean: "abc"` — NOT the 42804 a
// typed non-boolean gets.
//
// Wadjet answered 0 rows for all four, because nothing typed the literal at
// all: `expr.FilterPredicate`'s generic arm takes a failed `v.(bool)`
// assertion for FALSE. So this runs at parse time, where the literal can
// simply BECOME the boolean it names.
//
// The grammar is PostgreSQL's `parse_bool_with_len`, the same one
// `CAST(<string> AS BOOLEAN)` already follows (ADR-0012): case-insensitive, C
// whitespace trimmed, any non-empty PREFIX of "true"/"false"/"yes"/"no", plus
// "on"/"off" and the single characters "1" and "0". `'tr'` and `'fals'` ARE
// values; `'o'` alone is not, because it cannot choose between "on" and "off".
func CoerceBooleanLiterals(info *SelectInfo) error {
	if info == nil {
		return nil
	}
	if info.Union != nil {
		if err := CoerceBooleanLiterals(info.Union.Left); err != nil {
			return err
		}
		if err := CoerceBooleanLiterals(info.Union.Right); err != nil {
			return err
		}
	}
	if n, changed, err := coerceBoolNode(info.WhereExpr); err != nil {
		return err
	} else if changed {
		info.WhereExpr, info.Where = n, n.String()
	}
	if n, changed, err := coerceBoolNode(info.HavingExpr); err != nil {
		return err
	} else if changed {
		info.HavingExpr, info.Having = n, n.String()
	}
	if n, changed, err := coerceBoolNode(info.QualifyExpr); err != nil {
		return err
	} else if changed {
		info.QualifyExpr, info.Qualify = n, n.String()
	}
	for i := range info.Joins {
		n, changed, err := coerceBoolNode(info.Joins[i].CondExpr)
		if err != nil {
			return err
		}
		if changed {
			info.Joins[i].CondExpr, info.Joins[i].Condition = n, n.String()
		}
	}
	// A SEARCHED CASE's WHEN is a boolean context wherever the CASE sits, so
	// the SELECT list is walked too. `CASE x WHEN 'y'` is NOT one — there the
	// WHEN is a value compared against x — which is why Subject gates it.
	for i := range info.Columns {
		if err := coerceBoolInCases(info.Columns[i].ASTExpr); err != nil {
			return err
		}
	}
	return coerceBoolInCases(info.WhereExpr)
}

// coerceBoolNode rewrites a node that IS a boolean context, returning the
// replacement and whether anything changed.
func coerceBoolNode(n Node) (Node, bool, error) {
	switch e := n.(type) {
	case nil:
		return nil, false, nil
	case *Lit:
		if e.Kind != LitString {
			return n, false, nil
		}
		v, ok := ParseBoolText(e.Value)
		if !ok {
			return nil, false, sqlerr.New("22P02",
				"invalid input syntax for type boolean: \"%s\"", e.Value)
		}
		lit := &Lit{Value: "false", Kind: LitBool}
		if v {
			lit.Value = "true"
		}
		return lit, true, nil
	case *ParenNode:
		inner, changed, err := coerceBoolNode(e.Inner)
		if err != nil || !changed {
			return n, false, err
		}
		return &ParenNode{Inner: inner}, true, nil
	case *NotNode:
		inner, changed, err := coerceBoolNode(e.Inner)
		if err != nil || !changed {
			return n, false, err
		}
		return &NotNode{Inner: inner}, true, nil
	case *AndNode:
		l, lc, err := coerceBoolNode(e.Left)
		if err != nil {
			return nil, false, err
		}
		r, rc, err := coerceBoolNode(e.Right)
		if err != nil {
			return nil, false, err
		}
		if !lc && !rc {
			return n, false, nil
		}
		return &AndNode{Left: l, Right: r}, true, nil
	case *OrNode:
		l, lc, err := coerceBoolNode(e.Left)
		if err != nil {
			return nil, false, err
		}
		r, rc, err := coerceBoolNode(e.Right)
		if err != nil {
			return nil, false, err
		}
		if !lc && !rc {
			return n, false, nil
		}
		return &OrNode{Left: l, Right: r}, true, nil
	}
	return n, false, nil
}

// coerceBoolInCases rewrites a searched CASE's WHEN conditions IN PLACE,
// wherever the CASE appears in an expression.
func coerceBoolInCases(n Node) error {
	switch e := n.(type) {
	case nil:
		return nil
	case *CaseNode:
		if e.Subject == nil {
			for i := range e.Whens {
				c, changed, err := coerceBoolNode(e.Whens[i].Cond)
				if err != nil {
					return err
				}
				if changed {
					e.Whens[i].Cond = c
				}
				if err := coerceBoolInCases(e.Whens[i].Cond); err != nil {
					return err
				}
			}
		} else if err := coerceBoolInCases(e.Subject); err != nil {
			return err
		}
		for i := range e.Whens {
			if err := coerceBoolInCases(e.Whens[i].Result); err != nil {
				return err
			}
		}
		return coerceBoolInCases(e.Else)
	case *ParenNode:
		return coerceBoolInCases(e.Inner)
	case *NotNode:
		return coerceBoolInCases(e.Inner)
	case *AndNode:
		if err := coerceBoolInCases(e.Left); err != nil {
			return err
		}
		return coerceBoolInCases(e.Right)
	case *OrNode:
		if err := coerceBoolInCases(e.Left); err != nil {
			return err
		}
		return coerceBoolInCases(e.Right)
	case *BinaryOp:
		if err := coerceBoolInCases(e.Left); err != nil {
			return err
		}
		return coerceBoolInCases(e.Right)
	case *UnaryOp:
		return coerceBoolInCases(e.Inner)
	case *CmpExpr:
		if err := coerceBoolInCases(e.Left); err != nil {
			return err
		}
		return coerceBoolInCases(e.Right)
	case *CastNode:
		return coerceBoolInCases(e.Inner)
	case *FuncCallNode:
		for _, a := range e.Args {
			if err := coerceBoolInCases(a); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

// ParseBoolText is PostgreSQL's boolean input function, `parse_bool_with_len`.
// Exported because two doors need the SAME grammar and a second copy is how
// they drift: a boolean CONTEXT here, and CAST(<string> AS BOOLEAN) in the
// expression compiler.
func ParseBoolText(s string) (bool, bool) {
	s = strings.Trim(s, " \t\n\r\f\v")
	if s == "" {
		return false, false
	}
	lower := strings.ToLower(s)
	switch lower {
	case "1":
		return true, true
	case "0":
		return false, true
	}
	// Any non-empty PREFIX of these words. "o" alone matches neither "on" nor
	// "off" uniquely, and PostgreSQL rejects it for exactly that reason.
	for _, w := range []struct {
		word string
		val  bool
	}{{"true", true}, {"false", false}, {"yes", true}, {"no", false}} {
		if strings.HasPrefix(w.word, lower) {
			return w.val, true
		}
	}
	if lower != "o" {
		if strings.HasPrefix("on", lower) {
			return true, true
		}
		if strings.HasPrefix("off", lower) {
			return false, true
		}
	}
	return false, false
}
