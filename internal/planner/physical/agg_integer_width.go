package physical

import (
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The WIDTH of a COMPUTED integer aggregate argument (#841's second half).
//
// PostgreSQL's SUM rule is by INPUT WIDTH: `sum(int2|int4)` is bigint, because
// there is a wider integer to grow into, and `sum(int8)` is NUMERIC, because
// there is not. A BARE column already gets that rule here
// (aggIntegerOutputType). A COMPUTED argument did not: every integer
// expression declares INT64 in this engine (ADR-0024's recorded widening), so
// aggOutputFromInputDecl could not tell `SUM(CASE WHEN … THEN 1 ELSE 0 END)`
// — TPC-H Q12's shape, int4 in PostgreSQL and bigint under SUM — from
// `SUM(bigint_col + 0)`, which is numeric there. It read them all as int4,
// keeping Q12's OID and leaving the int8 case as a residual: the total sums
// into an int64 carrier and a query PostgreSQL answers becomes 22003.
//
// That residual was invisible while `rewriteConstArithAggs` was lifting the
// constant out — `SUM(b + 0)` ran as `SUM(b) + 0*COUNT(b)` over a BARE column,
// which takes the exact path — and it surfaced the moment the lift stopped
// moving refusals. It is the same question #841 asks: one expression, one
// disposition, whichever position it is written in.
//
// The width is recoverable from the AST plus the column declarations, which is
// what this walk does. It answers "wide" ONLY for an expression that provably
// carries an int8-domain operand, and everything else keeps the int4 reading
// it had — so the change is confined to shapes that can be pointed at, and no
// declaration moves on a shape this walk cannot see through.
//
//	SUM(CASE WHEN … THEN 1 ELSE 0 END)   not wide → bigint   (PostgreSQL: bigint)
//	SUM(int32_col * 2)                   not wide → bigint   (PostgreSQL: bigint)
//	SUM(int64_col + 0)                   WIDE     → numeric  (PostgreSQL: numeric)
//	SUM(row_number_slot * 2)             WIDE     → numeric  (PostgreSQL: numeric)
//	SUM(9223372036854775807 * x)         WIDE     → numeric  (the literal is int8)
//	SUM(int64_col::bigint)               WIDE     → numeric  (the CAST's target)
//	SUM(int64_col::int4)                 not wide → bigint   (the cast narrows)
//
// It is asked by BOTH spellings — `aggComputedInputDecl` for `GROUP BY` and
// `windowComputedArgDecl` for `OVER (…)` — so an arm added here moves the two
// together by construction. That is why the CAST arm closes one divergence in
// two places at once, and why a missing arm is a divergence in two places at
// once: `SUM(bigint_col::bigint)` read as int4 in both.
//
// NOT covered, deliberately, and recorded rather than guessed at: PORT and
// PROTOCOL under ARITHMETIC. Both are int4-domain and a BARE one takes int4's
// result types (exec.IntegerAccOutputType, #953), but `c_port * 1` is
// evaluated on the FLOAT path — `expr.operandIsInt` keeps the network types
// there on purpose, and `intArithAllInt` mirrors it so the declaration cannot
// promise an integer the kernel will not produce. Answering "int4-domain" here
// alone would be that promise. See ADR-0012's #953 entry for the mechanism and
// the pinned cells.
func aggInputIsWideInteger(node plansql.Node, decls colDecls) bool {
	switch n := node.(type) {
	case *plansql.ParenNode:
		return aggInputIsWideInteger(n.Inner, decls)
	case *plansql.UnaryOp:
		return aggInputIsWideInteger(n.Inner, decls)
	case *plansql.BinaryOp:
		return aggInputIsWideInteger(n.Left, decls) || aggInputIsWideInteger(n.Right, decls)
	case *plansql.CaseNode:
		// A choice is as wide as its widest arm, which is what PostgreSQL's
		// own common-type resolution says for a CASE over int4 and int8.
		for _, w := range n.Whens {
			if aggInputIsWideInteger(w.Result, decls) {
				return true
			}
		}
		return n.Else != nil && aggInputIsWideInteger(n.Else, decls)
	case *plansql.FuncCallNode:
		// ABS and MOD answer in their ARGUMENT's own numeric domain — that is
		// expr.NumericDomainScalarFn's set, measured against PostgreSQL for
		// every width by #768, and it is the same predicate that makes a
		// materialized `ABS(int8_col)` column declare INT64 through
		// scalarFnDeclaredNumericDomain. So they are as wide as their
		// arguments, exactly as a polymorphic choice is.
		//
		// This is the half the WINDOW spelling already had and the grouped one
		// did not (#987 review, P2). `SUM(ABS(w_i64)) OVER ()` reads the
		// materialized argument's INT64 declaration and answers numeric, which
		// is PostgreSQL's type; the GROUPED spelling declined here, read the
		// expression as int4 and declared bigint. Same digits, two boxes — the
		// exact class #813 was, with the spellings' roles swapped. Both ask
		// this one predicate now.
		if _, domain := expr.NumericDomainScalarFn(n.Name); domain {
			for _, a := range n.Args {
				if aggInputIsWideInteger(a, decls) {
					return true
				}
			}
			return false
		}
		// COALESCE / GREATEST / LEAST / NULLIF / IF choose between their
		// arguments and are as wide as the widest. Every other function
		// declines: an unrecognized return width keeps today's reading, which
		// is the conservative side — it leaves the declaration where it is.
		if _, poly := expr.DefaultRegistry.ReturnType(n.Name).SameAsArgs(len(n.Args)); !poly {
			return false
		}
		for _, a := range n.Args {
			if aggInputIsWideInteger(a, decls) {
				return true
			}
		}
		return false
	case *plansql.CastNode:
		// A CAST answers in its TARGET type's domain, whatever the operand's
		// was — that is the whole point of writing one. PostgreSQL:
		//
		//	sum(bigint_col::bigint)   numeric   the cast keeps int8
		//	sum(bigint_col::int4)     bigint    the cast NARROWS to int4
		//	sum(int_col::bigint)      numeric   the cast WIDENS to int8
		//	sum(x::numeric)           numeric   not an integer at all
		//	sum(x::float8)            double    likewise
		//
		// So the target decides, and nodeDeclaredType is what reads it
		// (inferCastType, and castDeclaredDecimal for a DECIMAL destination).
		// Only INT64 is "wide"; INT32 is the int4 case this walk already
		// answers false for, and a non-integer target leaves the integer
		// table entirely — its declaration is what
		// exec.IntegerAccOutputType declines, and the float or DECIMAL
		// reading stands.
		//
		// Without this arm the walk fell off its end and answered "not wide"
		// for every int8 operand written under a cast, so
		// `SUM(bigint_col::bigint)` declared bigint in BOTH spellings where
		// PostgreSQL declares numeric — and past int64 a total PostgreSQL
		// ANSWERS became 22003 on four arms, while the identical query one
		// cast away answered it exactly. "PostgreSQL answers and we refuse"
		// is the direction ADR-0012 does not allow; the permitted superset
		// runs the other way (#987 review round 3, B1; #841's grouped half).
		//
		// The TARGET NAME is read, not nodeDeclaredType's answer for the
		// node: every integer cast spelling lands on INT64 there, because
		// the engine has no int16 and reads an int4 column as int64
		// everywhere else (inferCastType, ADR-0012 item 12's recorded OID
		// divergence). That reading cannot tell `::int4` from `::bigint`,
		// which is the only thing this walk is asking about.
		return castTargetIsWideInteger(n.TypeName)
	case *plansql.ColRef:
		if decls.isFieldPath(n) {
			f, ok := decls.field(n)
			return ok && f.Type == parquet.TypeInt64
		}
		c, ok := decls.colDecl(n)
		return ok && c.Type == parquet.TypeInt64
	case *plansql.Lit:
		if n.Kind != plansql.LitNumber {
			return false
		}
		// An integer literal outside int4's range is int8 in PostgreSQL, so
		// the arithmetic around it is int8 arithmetic and its SUM is numeric.
		v, err := strconv.ParseInt(strings.TrimSpace(n.Value), 10, 64)
		return err == nil && (v > 2147483647 || v < -2147483648)
	}
	return false
}

// castTargetIsWideInteger reports whether a CAST's target names the int8
// domain, for aggInputIsWideInteger's CastNode arm.
//
// The spellings are inferCastType's own integer list, split by WIDTH — which
// that function deliberately does not do, because the engine carries every
// integer as an int64 and the declaration only has to be wide enough. The
// aggregate's RESULT TYPE is the one question where the width is the whole
// answer: `sum(int4)` is bigint and `sum(int8)` is numeric.
//
// SIGNED has no PostgreSQL meaning (it is a MySQL spelling this engine
// accepts) and is read as int8: numeric is the reading that cannot lose
// digits, and an unknown target answers false for the same reason it is safe
// to — a non-integer target's declaration leaves the integer table anyway.
func castTargetIsWideInteger(typeName string) bool {
	switch strings.ToUpper(strings.TrimSpace(typeName)) {
	case "BIGINT", "INT8", "INT64", "SIGNED":
		return true
	}
	return false
}
