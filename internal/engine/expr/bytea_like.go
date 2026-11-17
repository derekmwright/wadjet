// SPDX-License-Identifier: MIT

package expr

import (
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// bytea LIKE (#1593). PostgreSQL resolves `x LIKE p` to `bytea ~~ bytea` when
// an operand is a bytea, and then the pattern takes the BACKSLASH as its
// escape. The reading is chosen by the operands' DECLARED types, never by the
// Go type a value happens to box as (a BYTES value, a text and a quoted
// literal can all arrive as a string):
//
//	operand classes   B  declared BYTES
//	                  U  SQL's `unknown`: a quoted literal or NULL
//	                  T  declared TEXT
//	                  O  any other declared type
//	                  X  not decidable here
//
//	x LIKE p          a B beside B or U  -> bytea reading (U is read by byteain)
//	                  a B beside T or O  -> 42883, no operator bytea ~~ text
//	                  no B               -> the text reading, unchanged
//	x LIKE p ESCAPE e is `x ~~ like_escape(p, e)`: with a B among (p, e) the
//	                  other must be B or U (else 42883) and the pattern is a
//	                  bytea, which x must be (B or U) to match; with no B among
//	                  them the pattern is text, and a B value x is 42883
//
// ILIKE is not here: the grammar rewrites it to lower(x) LIKE lower(p), and
// lower over a bytea is 42883, as PostgreSQL's `bytea ~~* bytea` is.
//
// Measured on PostgreSQL 17.11 (docs/postgres-differences.md, bytea LIKE).
// The text reading is the engine's own matcher, which this file never calls.

// likeClass is one LIKE operand's declared class.
type likeClass uint8

const (
	likeClassX likeClass = iota // not decidable
	likeClassB
	likeClassU
	likeClassT
	likeClassO
)

// likeLiteralKind says what an operand's AST is syntactically.
type likeLiteralKind uint8

const (
	likeNotLiteral likeLiteralKind = iota
	likeStringLiteral
	likeNullLiteral
)

func likeLiteralOf(n plansql.Node) likeLiteralKind {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	if lit, ok := n.(*plansql.Lit); ok {
		switch lit.Kind {
		case plansql.LitString:
			return likeStringLiteral
		case plansql.LitNull:
			return likeNullLiteral
		}
	}
	return likeNotLiteral
}

// likeOperand is one operand's declaration source.
type likeOperand struct {
	decl *operandDecl
	lit  likeLiteralKind
}

func newLikeOperand(n plansql.Node, ctx *compileContext) likeOperand {
	return likeOperand{decl: newOperandDecl(n, ctx), lit: likeLiteralOf(n)}
}

// class is the operand's declared class against b.
func (o likeOperand) class(b *batch.RecordBatch, row int, operand Expr) likeClass {
	if o.lit != likeNotLiteral {
		return likeClassU
	}
	if o.decl == nil {
		return likeClassX
	}
	col := o.decl.shape(b, row, operand)
	if col == nil {
		return likeClassX
	}
	switch col.Type {
	case batch.TypeBytes:
		return likeClassB
	case batch.TypeString:
		return likeClassT
	}
	return likeClassO
}

// byteaOf is a B operand's bytes. A declared BYTES value boxes as []byte; a
// string is taken as its raw bytes, never read by byteain.
func byteaOf(v any) []byte {
	switch tv := v.(type) {
	case []byte:
		return tv
	case string:
		return []byte(tv)
	}
	return []byte(toString(v))
}

// byteaLiteral reads a quoted literal operand of a bytea LIKE by byteain, as
// PostgreSQL coerces an `unknown` beside a bytea; a bad literal is its 22P02.
func byteaLiteral(v any) []byte {
	raw, err := kernel.ByteaIn(toString(v))
	if err != nil {
		panic(fatalEval{err})
	}
	return raw
}

func likeClassName(c likeClass) string {
	switch c {
	case likeClassB:
		return "bytea"
	case likeClassT:
		return "text"
	}
	return "unknown"
}

// byteaLikeOperands reads (value, pattern) as bytea: a literal by byteain, a B
// as its bytes. isNull is true when either operand is NULL.
func byteaLikeOperands(cv, cp likeClass, v, p any) (vb, pb []byte, isNull bool) {
	// Both literals are read before either NULL is honoured: PostgreSQL
	// coerces them while it analyses the statement, so a bad literal is
	// 22P02 even beside a NULL.
	if cv == likeClassU && v != nil {
		vb = byteaLiteral(v)
	} else if v != nil {
		vb = byteaOf(v)
	}
	if cp == likeClassU && p != nil {
		pb = byteaLiteral(p)
	} else if p != nil {
		pb = byteaOf(p)
	}
	return vb, pb, v == nil || p == nil
}

func byteaLikeResult(vb, pb []byte, not bool) (bool, bool) {
	m, err := kernel.ByteaLike(vb, pb)
	if err != nil {
		panic(fatalEval{err})
	}
	if not {
		return !m, false
	}
	return m, false
}

// likeReading is what a LIKE's declared operand classes call for.
type likeReading uint8

const (
	likeReadText   likeReading = iota // the engine's text matcher, unchanged
	likeReadBytea                     // the bytea reading
	likeReadRefuse                    // PostgreSQL's 42883
)

// likeReadingOf is the ONE rule for `x LIKE p` (see the table above), used by
// the evaluator and by the binder's refusal alike.
func likeReadingOf(cv, cp likeClass) likeReading {
	if cv != likeClassB && cp != likeClassB {
		return likeReadText
	}
	okV := cv == likeClassB || cv == likeClassU
	okP := cp == likeClassB || cp == likeClassU
	if okV && okP {
		return likeReadBytea
	}
	if cv == likeClassX || cp == likeClassX {
		// One side is not decidable: keep the answer this engine has always
		// given for it rather than guess a refusal.
		return likeReadText
	}
	return likeReadRefuse
}

func likeOperatorError(cv, cp likeClass) error {
	return sqlerr.New("42883", "operator does not exist: %s ~~ %s", likeClassName(cv), likeClassName(cp))
}

// likeEscapeReadingOf is the rule for `x LIKE p ESCAPE e`, which PostgreSQL
// reads as `x ~~ like_escape(p, e)`: the bytea like_escape when p or e is a
// bytea (the other then a bytea or an unknown literal, else 42883), then the
// pattern it answers is a bytea for x to match, or text.
func likeEscapeReadingOf(cv, cp, ce likeClass) (likeReading, error) {
	if cv != likeClassB && cp != likeClassB && ce != likeClassB {
		return likeReadText, nil
	}
	if cp == likeClassX || ce == likeClassX || cv == likeClassX {
		return likeReadText, nil
	}
	okPE := func(c likeClass) bool { return c == likeClassB || c == likeClassU }
	if cp == likeClassB || ce == likeClassB {
		if !okPE(cp) || !okPE(ce) {
			return likeReadRefuse, sqlerr.New("42883", "function like_escape(%s, %s) does not exist",
				likeClassName(cp), likeClassName(ce))
		}
		if cv != likeClassB && cv != likeClassU {
			return likeReadRefuse, likeOperatorError(cv, likeClassB)
		}
		return likeReadBytea, nil
	}
	// The pattern is TEXT, and cv is the B: no operator.
	return likeReadRefuse, likeOperatorError(cv, likeClassT)
}

// likeEscapeNode is `x LIKE p ESCAPE e`, the call the grammar rewrites to
// (#1169). It keeps fnLikeEscape for the text reading and answers the bytea
// reading when the declared operands call for it.
type likeEscapeNode struct {
	args [3]Expr
	ops  [3]likeOperand
	// fallback is the generic call node, evaluated for the text reading.
	fallback Expr
}

func (e *likeEscapeNode) Eval(b *batch.RecordBatch, row int) any {
	cv := e.ops[0].class(b, row, e.args[0])
	cp := e.ops[1].class(b, row, e.args[1])
	ce := e.ops[2].class(b, row, e.args[2])
	reading, err := likeEscapeReadingOf(cv, cp, ce)
	if reading == likeReadText {
		return e.fallback.Eval(b, row)
	}
	var vals [3]any
	for i, a := range e.args {
		vals[i] = a.Eval(b, row)
	}
	// The literals are read before the refusal and before any NULL is
	// honoured: PostgreSQL coerces them while it analyses the statement, so a
	// bad pattern or escape literal is 22P02 even where the call is also a
	// 42883 or the value is NULL.
	read := func(c likeClass, v any) []byte {
		switch {
		case v == nil:
			return nil
		case c == likeClassU:
			return byteaLiteral(v)
		case c == likeClassB:
			return byteaOf(v)
		}
		return nil
	}
	var pb, eb []byte
	if cp == likeClassB || ce == likeClassB {
		// Only the bytea like_escape coerces its literals to bytea; beside a
		// text pattern nothing is read by byteain and the refusal is the
		// operator's.
		pb, eb = read(cp, vals[1]), read(ce, vals[2])
	}
	if reading == likeReadRefuse {
		panic(fatalEval{err})
	}
	vb := read(cv, vals[0])
	if vals[0] == nil || vals[1] == nil || vals[2] == nil {
		return nil
	}
	pat, perr := kernel.ByteaLikeEscape(pb, eb)
	if perr != nil {
		panic(fatalEval{perr})
	}
	m, _ := byteaLikeResult(vb, pat, false)
	return m
}

// declClass is a node's class by the planner's declared type; decl answers
// Undecided where it cannot say.
func declClass(n plansql.Node, decl func(plansql.Node) (DeclType, Confidence)) likeClass {
	if likeLiteralOf(n) != likeNotLiteral {
		return likeClassU
	}
	t, c := decl(n)
	if c != Decided {
		return likeClassX
	}
	switch t.ID {
	case batch.TypeBytes:
		return likeClassB
	case batch.TypeString:
		return likeClassT
	}
	return likeClassO
}

// bareStringLiteral is the text of a quoted-literal operand, for the binder's
// byteain check.
func bareStringLiteral(n plansql.Node) (string, bool) {
	for {
		p, ok := n.(*plansql.ParenNode)
		if !ok {
			break
		}
		n = p.Inner
	}
	if lit, ok := n.(*plansql.Lit); ok && lit.Kind == plansql.LitString {
		return lit.Value, true
	}
	return "", false
}

// RefuseByteaLike is the BINDER's half of the bytea LIKE rule (#1593):
// PostgreSQL resolves the operator while it analyses the statement, so `vb LIKE
// pt` is 42883 and `vb LIKE '\%'` is 22P02 over an empty table and under WHERE
// false, where the evaluator never runs. It reads LikeExpr and the like_escape
// call, with the same rule the evaluator uses; an operand decl cannot say is
// never refused (the evaluator then decides on the schema it executes).
func RefuseByteaLike(n plansql.Node, decl func(plansql.Node) (DeclType, Confidence)) error {
	var ops []plansql.Node
	var reading likeReading
	var rerr error
	switch v := n.(type) {
	case *plansql.LikeExpr:
		cv, cp := declClass(v.Left, decl), declClass(v.Pattern, decl)
		reading = likeReadingOf(cv, cp)
		if reading == likeReadRefuse {
			rerr = likeOperatorError(cv, cp)
		}
		ops = []plansql.Node{v.Left, v.Pattern}
	case *plansql.FuncCallNode:
		if !strings.EqualFold(strings.TrimSpace(v.Name), "like_escape") || len(v.Args) != 3 {
			return nil
		}
		cv, cp, ce := declClass(v.Args[0], decl), declClass(v.Args[1], decl), declClass(v.Args[2], decl)
		reading, rerr = likeEscapeReadingOf(cv, cp, ce)
		if cp == likeClassB || ce == likeClassB {
			// Only the bytea like_escape reads its literals by byteain, p and
			// e before x (the call is analysed before the operator).
			ops = []plansql.Node{v.Args[1], v.Args[2], v.Args[0]}
		}
	default:
		return nil
	}
	if reading == likeReadText {
		return nil
	}
	for _, o := range ops {
		if s, ok := bareStringLiteral(o); ok {
			if _, err := kernel.ByteaIn(s); err != nil {
				return err
			}
		}
	}
	return rerr
}
