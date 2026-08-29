package physical

import (
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// rlrDecls is the declaration set the refusal reads: one REAL column and one
// DOUBLE PRECISION column holding the same numbers, which is what makes the
// "the OPERAND's type decides" rule visible.
func rlrDecls() colDecls {
	return colDecls{types: map[string]parquet.TypeID{
		"r_val": parquet.TypeFloat32,
		"d_val": parquet.TypeFloat64,
		"r_key": parquet.TypeInt64,
	}}
}

func rlrCol(name string) *plansql.ColRef { return &plansql.ColRef{Column: name} }

func rlrNum(text string) *plansql.Lit {
	return &plansql.Lit{Value: text, Kind: plansql.LitNumber}
}

// TestRealTypedNodeFollowsPostgresOperandType pins the rule that decides
// whether an IN list is cast to real[] at all: the probed operand's OWN type,
// not whether it is a bare column.
//
// PostgreSQL resolves the array's element type over the members AND the probed
// expression, so any real-typed left operand pulls the list to real[] — read
// off EXPLAIN VERBOSE on postgres:17:
//
//	-r_val IN (-3.1, -7.1)          -> ((- r_val) = ANY ('{-3.1,-7.1}'::real[]))
//	CAST(d_val AS REAL) IN (3.1,…)  -> ((d_val)::real = ANY ('{3.1,7.1}'::real[]))
//	(r_val + 0) IN (3.1, 7.1)       -> (… = ANY ('{3.1,7.1}'::double precision[]))
//
// The last is the control and the reason this cannot simply walk down to a
// column: an integer literal added to a real gives DOUBLE PRECISION there
// (pg_typeof(r_val + 0) is `double precision`), so that shape stays widened.
func TestRealTypedNodeFollowsPostgresOperandType(t *testing.T) {
	cast := func(inner plansql.Node, typ string) *plansql.CastNode {
		return &plansql.CastNode{Inner: inner, TypeName: typ}
	}
	unary := func(op string, inner plansql.Node) *plansql.UnaryOp {
		return &plansql.UnaryOp{Op: op, Inner: inner}
	}

	cases := []struct {
		name string
		node plansql.Node
		want bool
	}{
		{"RealColumn", rlrCol("r_val"), true},
		{"DoubleColumn", rlrCol("d_val"), false},
		{"IntegerColumn", rlrCol("r_key"), false},
		// A name no declaration covers says nothing, so the refusal must not
		// fire on it — the runtime backstops own that shape.
		{"UndeclaredColumn", rlrCol("nosuch"), false},
		{"CastToReal", cast(rlrCol("d_val"), "REAL"), true},
		{"CastToFloat4", cast(rlrCol("d_val"), "float4"), true},
		{"CastSpacedSpelling", cast(rlrCol("d_val"), " Real "), true},
		// PostgreSQL's bare FLOAT is double precision, and so is float8.
		{"CastToDouble", cast(rlrCol("r_val"), "DOUBLE PRECISION"), false},
		{"CastToFloat", cast(rlrCol("r_val"), "FLOAT"), false},
		{"CastToInteger", cast(rlrCol("r_val"), "INTEGER"), false},
		// Unary +/- is the one operator that preserves real.
		{"NegatedReal", unary("-", rlrCol("r_val")), true},
		{"PlusReal", unary("+", rlrCol("r_val")), true},
		{"DoubleNegatedReal", unary("-", unary("-", rlrCol("r_val"))), true},
		{"NegatedCastToReal", unary("-", cast(rlrCol("d_val"), "real")), true},
		{"NegatedDouble", unary("-", rlrCol("d_val")), false},
		// Arithmetic does not: `r_val + 0` is double precision.
		{"RealPlusZero", &plansql.BinaryOp{Left: rlrCol("r_val"), Op: "+", Right: rlrNum("0")}, false},
		{"RealTimesOne", &plansql.BinaryOp{Left: rlrCol("r_val"), Op: "*", Right: rlrNum("1")}, false},
		{"ParenthesizedReal", &plansql.ParenNode{Inner: rlrCol("r_val")}, true},
		{"ParenthesizedRealPlusZero", &plansql.ParenNode{
			Inner: &plansql.BinaryOp{Left: rlrCol("r_val"), Op: "+", Right: rlrNum("0")}}, false},
		{"Literal", rlrNum("3.1"), false},
	}
	for _, c := range cases {
		if got := realTypedNode(c.node, rlrDecls()); got != c.want {
			t.Errorf("%s: realTypedNode(%s) = %v, want %v (PostgreSQL 17)", c.name, c.node.String(), got, c.want)
		}
	}
}

// TestRealListLiteralTextCarriesTheSign is the member half. The sign travels
// with the TEXT, because the 22003 message has to print it — PostgreSQL names
// "-10000000000000000000000000000000000000000" for `IN (-1e40, 3.1)` — and
// because reading a negated member as "not a literal" disarmed the check for
// the whole list.
//
// The second result separates "not a constant" (the array cast does not happen
// at all, so nothing is refused) from "a constant with no number in it" (NULL,
// a quoted string), which contributes nothing but leaves the rest of the list
// under the rule.
func TestRealListLiteralTextCarriesTheSign(t *testing.T) {
	cases := []struct {
		name string
		node plansql.Node
		text string
		ok   bool
	}{
		{"Number", rlrNum("3.1"), "3.1", true},
		{"Negated", &plansql.UnaryOp{Op: "-", Inner: rlrNum("1e40")}, "-1e40", true},
		{"NegatedAlreadySigned", &plansql.UnaryOp{Op: "-", Inner: rlrNum("-1e40")}, "1e40", true},
		{"Plus", &plansql.UnaryOp{Op: "+", Inner: rlrNum("1e40")}, "1e40", true},
		{"DoubleNegated", &plansql.UnaryOp{Op: "-", Inner: &plansql.UnaryOp{Op: "-", Inner: rlrNum("1e40")}}, "1e40", true},
		{"Paren", &plansql.ParenNode{Inner: rlrNum("3.1")}, "3.1", true},
		{"CastToReal", &plansql.CastNode{Inner: rlrNum("3.1"), TypeName: "REAL"}, "3.1", true},
		{"NegatedCastToReal", &plansql.UnaryOp{Op: "-",
			Inner: &plansql.CastNode{Inner: rlrNum("1e40"), TypeName: "real"}}, "-1e40", true},
		// A constant with no number in it: still a constant, still leaves the
		// list under the array-cast rule.
		{"Null", &plansql.Lit{Kind: plansql.LitNull}, "", true},
		{"String", &plansql.Lit{Value: "3.1", Kind: plansql.LitString}, "", true},
		// Not constants: PostgreSQL plans an OR of widened scalar comparisons
		// and no cast to real[] happens for any member.
		{"Column", rlrCol("r_val"), "", false},
		{"Arithmetic", &plansql.BinaryOp{Left: rlrNum("1"), Op: "+", Right: rlrNum("2")}, "", false},
		{"CastToDouble", &plansql.CastNode{Inner: rlrNum("3.1"), TypeName: "DOUBLE PRECISION"}, "", false},
		{"NegatedColumn", &plansql.UnaryOp{Op: "-", Inner: rlrCol("r_val")}, "", false},
	}
	for _, c := range cases {
		text, ok := realListLiteralText(c.node)
		if text != c.text || ok != c.ok {
			t.Errorf("%s: realListLiteralText(%s) = (%q, %v), want (%q, %v)",
				c.name, c.node.String(), text, ok, c.text, c.ok)
		}
	}
}

// TestNegateNumericTextFlipsTheSpelling — the sign is flipped in the TEXT, not
// in a parsed value: a member may be wider than a float64 (1e400), and the
// message prints the digits.
func TestNegateNumericTextFlipsTheSpelling(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"1e40", "-1e40"},
		{"-1e40", "1e40"},
		{"+1e40", "-1e40"},
		{"3.1", "-3.1"},
		{" 3.1 ", "-3.1"},
		{"0", "-0"},
	} {
		if got := negateNumericText(c.in); got != c.want {
			t.Errorf("negateNumericText(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

// TestRefuseRealInListMatchesPostgres runs the whole rule over one IN list.
// Every want is postgres:17's over a table declared
// `(r_key bigint, r_val real, d_val double precision)`.
func TestRefuseRealInListMatchesPostgres(t *testing.T) {
	in := func(left plansql.Node, values ...plansql.Node) *plansql.InExpr {
		return &plansql.InExpr{Left: left, Values: values}
	}
	neg := func(n plansql.Node) plansql.Node { return &plansql.UnaryOp{Op: "-", Inner: n} }

	cases := []struct {
		name string
		expr *plansql.InExpr
		want string // "" = no refusal
	}{
		{"Overflow", in(rlrCol("r_val"), rlrNum("1e40"), rlrNum("3.1")),
			`"10000000000000000000000000000000000000000" is out of range for type real`},
		{"NegativeOverflow", in(rlrCol("r_val"), neg(rlrNum("1e40")), rlrNum("3.1")),
			`"-10000000000000000000000000000000000000000" is out of range for type real`},
		// The member that used to disarm the whole list: a negative member
		// beside the offending one made the list "not all constants".
		{"NegativeMemberBesideOverflow", in(rlrCol("r_val"), neg(rlrNum("1.0")), rlrNum("1e40")),
			`"10000000000000000000000000000000000000000" is out of range for type real`},
		{"Underflow", in(rlrCol("r_val"), rlrNum("1e-46"), rlrNum("3.1")),
			`"0.0000000000000000000000000000000000000000000001" is out of range for type real`},
		// real's smallest denormal is about 1.4e-45: 1e-45 is representable
		// and PostgreSQL answers rows for it.
		{"DenormalBoundary", in(rlrCol("r_val"), rlrNum("1e-45"), rlrNum("3.1")), ""},
		{"Representable", in(rlrCol("r_val"), rlrNum("3.1"), rlrNum("7.1")), ""},
		// Arity one WIDENS in PostgreSQL — `real IN (1e40)` is a float8
		// comparison against a float8 literal and answers no rows.
		{"AritySingleWidens", in(rlrCol("r_val"), rlrNum("1e40")), ""},
		// A DOUBLE operand keeps the array at double precision, where 1e40 is
		// an ordinary value.
		{"DoubleOperand", in(rlrCol("d_val"), rlrNum("1e40"), rlrNum("3.1")), ""},
		// A real-typed EXPRESSION is still real: both of these refuse.
		{"NegatedRealOperand", in(neg(rlrCol("r_val")), rlrNum("1e40"), rlrNum("3.1")),
			`"10000000000000000000000000000000000000000" is out of range for type real`},
		{"CastToRealOperand", in(&plansql.CastNode{Inner: rlrCol("d_val"), TypeName: "REAL"},
			rlrNum("1e40"), rlrNum("3.1")),
			`"10000000000000000000000000000000000000000" is out of range for type real`},
		// `r_val + 0` is double precision: no array cast, nothing to refuse.
		{"PlusZeroOperand", in(&plansql.BinaryOp{Left: rlrCol("r_val"), Op: "+", Right: rlrNum("0")},
			rlrNum("1e40"), rlrNum("3.1")), ""},
		// A non-constant member takes the array away entirely.
		{"NonConstantMember", in(rlrCol("r_val"), rlrCol("d_val"), rlrNum("1e40")), ""},
		// A NULL member is a constant and does not disarm the rest.
		{"NullMemberBesideOverflow", in(rlrCol("r_val"), &plansql.Lit{Kind: plansql.LitNull}, rlrNum("1e40")),
			`"10000000000000000000000000000000000000000" is out of range for type real`},
	}
	for _, c := range cases {
		err := refuseRealInList(c.expr, rlrDecls())
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: %s raised %v; PostgreSQL answers rows", c.name, c.expr.String(), err)
		case c.want != "" && err == nil:
			t.Errorf("%s: %s was accepted; PostgreSQL raises 22003 %s", c.name, c.expr.String(), c.want)
		case c.want != "" && err != nil:
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("%s: %s raised %v, want the refusal naming %s", c.name, c.expr.String(), err, c.want)
			}
			if got := sqlerr.StateOf(err); got != "22003" {
				t.Errorf("%s: SQLSTATE %q, want 22003", c.name, got)
			}
		}
	}
}
