package coordinator

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The FLOAT32 (`real`) comparison-WIDTH fixture (#631, #633).
//
// PostgreSQL does not compare a `real` column at real width just because the
// column is a real. It resolves the comparison from the LITERAL, and the two
// answers it can give are different predicates over the same rows:
//
//	real <op> <numeric literal>   ->  float8 <op> float8   -- WIDEN the column
//	real IN (lit, lit, ...)       ->  real = ANY(real[])   -- NARROW the list
//	real IN (lit)                 ->  float8 = float8      -- WIDEN (arity 1)
//
// all three read off EXPLAIN VERBOSE on postgres:17-alpine. #549 fixed the
// NARROWING half in the vectorized kernel; #631 is the WIDENING half, which
// the kernel got wrong for all six operators; #633 is that the DISTRIBUTED
// path never evaluated either through that kernel at all.
//
// That last one is the reason this gate stands up a cluster. The stage DAG
// compiles a scan-pushed filter to the ROW evaluator (worker
// compileFilterExprs -> expr.FilterPredicate), a third IN mechanism beside the
// kernel and the #524 subquery-set path, and its `expr.In` compares BOXED
// values — a FLOAT32 column boxes as float64 (ColRef.Eval), so every list
// member was compared at DOUBLE width. `real IN (0.1, 3.1)` therefore matched
// NOTHING on the DAG while the single-process kernel matched both rows, and
// the pg-oracle corpus could not see it: it runs at SF0.01, where the
// coordinator takes the in-process fast path.
//
// Every expectation below is PostgreSQL 17's, taken live over this exact
// fixture — see rwpWant's per-case citation. The two arms are held to
// PostgreSQL, not to each other, so an engine that agrees with the other arm
// and with nothing else still fails.
const rwpTable = "realwidth"

func rwpSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "r_key", Type: parquet.TypeInt64},
		{Name: "r_val", Type: parquet.TypeFloat32, Nullable: true},
		// A second REAL column holding the same values, so a column-to-column
		// comparison and a join key — neither of which has a literal to
		// resolve a width from — can be asserted to be UNMOVED by the change.
		{Name: "r_other", Type: parquet.TypeFloat32, Nullable: true},
		// The same numbers as DOUBLE. PostgreSQL compares `real = double` by
		// widening the real (Filter: r_val = d_val, no cast on either side),
		// so the rows that match are exactly the ones whose real value
		// survives the round trip.
		{Name: "d_val", Type: parquet.TypeFloat64, Nullable: true},
	}}
}

// rwpData is the fixture #549 and #631 both turn on, plus the two rows that
// make the INTEGER-literal question non-vacuous.
//
// Rows 0..15 hold real(i)+0.1, which is NOT exactly representable in float32:
// float64(float32(3.1)) != 3.1, so the width of the comparison decides whether
// row 3 answers `= 3.1`, `< 3.1` or `>= 3.1`. Rows 16..17 hold 0.5 and 1.5,
// exactly representable, the values that mask the defect. Row 18 is NULL and
// row 19 is 0.0 — the value every "read the literal as the type's zero"
// defect in this family lands on.
//
// Row 20 holds 16777216 (2^24), the first integer float32 cannot follow: the
// literal 16777217 is a plain INTEGER, exactly representable in double and
// not in real, so `r_val = 16777217` is empty under PostgreSQL's widening and
// would match row 20 under narrowing. It is the one value that proves the
// integer-literal rule without depending on a decimal point.
//
// Row 21 holds NaN, which PostgreSQL orders ABOVE every other float and equal
// to itself (ADR-0012 item 8). It is here because the ROW path lost that order
// for a comparison against an INTEGER literal — `r_val > 1` dropped it while
// `r_val > 1.0` kept it — so the integer-literal entries below are also NaN
// entries. A NaN INGESTS (the parquet writer keeps it out of min/max, so the
// manifest JSON stats never see it); an infinity does NOT, which is why this
// fixture has no infinite row and the neighbouring float gates manufacture
// theirs with CAST.
//
// Rows 22 and 23 are NEGATIVE (-3.5 and -0.25, both exact in float32 so they
// add no width question of their own). They are here for the SIGN, and they
// are what makes the `'-Infinity'` entries below able to fail at all: a float
// compared against one of the specials used to fall through to a LEXICOGRAPHIC
// text comparison, and every finite value renders starting with a digit or
// '-', so only a NEGATIVE rendering ("-3.5") sorts below "-Infinity" as text.
// Over rows 0..21 — every one at or above zero — the correct float answer and
// the wrong text answer are the same row set (#534 review). They also give the
// ordinary width entries beside them a negative arm, which none had.
func rwpData() []map[string]any {
	rows := make([]map[string]any, 0, 24)
	add := func(k int64, v any) {
		rows = append(rows, map[string]any{"r_key": k, "r_val": v, "r_other": v, "d_val": nil})
	}
	for i := 0; i < 16; i++ {
		add(int64(i), float32(i)+0.1)
		rows[i]["d_val"] = float64(i) + 0.1
	}
	add(16, float32(0.5))
	rows[16]["d_val"] = 0.5
	add(17, float32(1.5))
	rows[17]["d_val"] = 1.5
	add(18, nil)
	add(19, float32(0))
	rows[19]["d_val"] = float64(0)
	add(20, float32(16777216))
	rows[20]["d_val"] = float64(16777216)
	add(21, float32(math.NaN()))
	rows[21]["d_val"] = math.NaN()
	add(22, float32(-3.5))
	rows[22]["d_val"] = float64(-3.5)
	add(23, float32(-0.25))
	rows[23]["d_val"] = float64(-0.25)
	return rows
}

// rwpCase is one predicate and the r_key set PostgreSQL answers it with.
type rwpCase struct {
	name  string
	where string
	want  []int64
}

// rwpWant is the corpus. Every `want` was produced by running the identical
// predicate against postgres:17-alpine over a table built from rwpData:
//
//	CREATE TABLE rp (r_key bigint, r_val real, r_other real, d_val double precision);
//	INSERT INTO rp SELECT i, i+0.1, i+0.1, i+0.1 FROM generate_series(0,15) i;
//	INSERT INTO rp VALUES (16,0.5,0.5,0.5),(17,1.5,1.5,1.5),(18,NULL,NULL,NULL),
//	                      (19,0,0,0),(20,16777216,16777216,16777216),
//	                      (21,'NaN','NaN','NaN'),(22,-3.5,-3.5,-3.5),(23,-0.25,-0.25,-0.25);
func rwpWant() []rwpCase {
	seq := func(lo, hi int64) []int64 {
		out := make([]int64, 0, hi-lo+1)
		for i := lo; i <= hi; i++ {
			out = append(out, i)
		}
		return out
	}
	join := func(parts ...[]int64) []int64 {
		out := []int64{}
		for _, p := range parts {
			out = append(out, p...)
		}
		sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
		return out
	}

	return []rwpCase{
		// --- #631: the six operators against a non-representable literal ---
		//
		// PostgreSQL WIDENS, so row 3 (float32(3)+0.1, which widens to
		// 3.0999999046325684) is BELOW 3.1, not equal to it. Under the
		// narrowing this replaces, row 3 answered `=`, `<=` and `>=` and was
		// absent from `<` — four of the six operators moved a row.
		{"EqNonRepresentable", "r_val = 3.1", nil},
		{"NeNonRepresentable", "r_val <> 3.1", join(seq(0, 17), []int64{19, 20, 21, 22, 23})},
		{"LtNonRepresentable", "r_val < 3.1", []int64{0, 1, 2, 3, 16, 17, 19, 22, 23}},
		{"LeNonRepresentable", "r_val <= 3.1", []int64{0, 1, 2, 3, 16, 17, 19, 22, 23}},
		{"GtNonRepresentable", "r_val > 3.1", join(seq(4, 15), []int64{20, 21})},
		{"GeNonRepresentable", "r_val >= 3.1", join(seq(4, 15), []int64{20, 21})},
		// The same number spelled with a trailing zero is the same literal:
		// PostgreSQL plans both as '3.1'::double precision.
		{"EqTrailingZero", "r_val = 3.10", nil},
		// Exactly-representable literals answer the same at either width —
		// the case that hid the defect.
		{"EqRepresentable", "r_val = 1.5", []int64{17}},
		{"EqZero", "r_val = 0", []int64{19}},
		{"BetweenNonRepresentable", "r_val BETWEEN 3.1 AND 4.1", []int64{4}},

		// --- #631: an INTEGER literal is widened too ---
		//
		// `r_val = 16777217` plans as '16777217'::double precision, so row 20
		// (2^24) does NOT match; narrowing the literal to real would round it
		// onto row 20's value and match. The companion `= 16777216` proves
		// the row is reachable at all.
		{"EqIntegerLiteralPastMantissa", "r_val = 16777217", nil},
		{"EqIntegerLiteralExact", "r_val = 16777216", []int64{20}},

		// --- #549 kept: a MULTI-element IN still NARROWS ---
		//
		// PostgreSQL casts the whole array literal to real[], so 3.1 becomes
		// the same real row 3 holds and matches. This is the opposite width
		// from `=` on the identical literal, which is why the two must not be
		// lowered to one another.
		{"InMultiNonRepresentable", "r_val IN (3.1, 7.1)", []int64{3, 7}},
		// The arity is SYNTACTIC: a NULL member is stripped for three-valued
		// logic but PostgreSQL still casts a two-element `{3.1,NULL}` to
		// real[], so this narrows and matches.
		{"InMultiWithNull", "r_val IN (3.1, NULL)", []int64{3}},
		{"NotInMulti", "r_val NOT IN (3.1, 7.1)",
			join([]int64{0, 1, 2}, []int64{4, 5, 6}, seq(8, 17), []int64{19, 20, 21, 22, 23})},
		// The narrowing and the widening on ONE literal, in one fixture: the
		// integer 16777217 misses through `=` (widened) and HITS through a
		// multi-element IN (narrowed onto 2^24). Anything that lowers IN to a
		// chain of `=`, on either path, fails exactly here.
		{"InMultiIntegerPastMantissa", "r_val IN (16777217, 99)", []int64{20}},

		// --- #549 kept: a SINGLE-element IN WIDENS (arity 1) ---
		{"InSingleNonRepresentable", "r_val IN (3.1)", nil},
		{"InSingleRepresentable", "r_val IN (1.5)", []int64{17}},

		// --- #654: a COMPUTED real operand narrows exactly like a bare one ---
		//
		// PostgreSQL resolves the array's element type from the operand's own
		// RESOLVED TYPE, not from whether it is a column. Every operand below
		// is `real` on the live server (pg_typeof, measured), so the list is
		// real[] and the non-representable 3.1 finds row 3 — and every one of
		// them answered ZERO ROWS here, because the rule was a syntactic case
		// list of {Paren, ColRef, CAST(real), UnaryOp(±)}.
		{"InAbs", "ABS(r_val) IN (3.1, 7.1)", []int64{3, 7}},
		{"InGreatest", "GREATEST(r_val, r_other) IN (3.1, 7.1)", []int64{3, 7}},
		{"InLeast", "LEAST(r_val, r_other) IN (3.1, 7.1)", []int64{3, 7}},
		{"InCoalesce", "COALESCE(r_val, r_other) IN (3.1, 7.1)", []int64{3, 7}},
		{"InCase", "(CASE WHEN r_key >= 0 THEN r_val ELSE r_other END) IN (3.1, 7.1)",
			[]int64{3, 7}},
		{"InNullif", "NULLIF(r_val, 0) IN (3.1, 7.1)", []int64{3, 7}},
		// real OP real is real: the multiply by a REAL one narrows and the
		// multiply by an INTEGER one does not. They are the pair that says
		// this tests BOTH operands rather than following one down to a column
		// — `pg_typeof(r * 1)` is double precision on the server.
		{"InRealTimesRealCast", "r_val * CAST(1 AS REAL) IN (3.1, 7.1)", []int64{3, 7}},
		{"InRealPlusRealCast", "r_val + CAST(0 AS REAL) IN (3.1, 7.1)", []int64{3, 7}},
		{"InRealTimesIntegerLiteralWidens", "r_val * 1 IN (3.1, 7.1)", nil},
		// A function with NO float4 overload widens, and CEIL is the one to
		// pick because its result is still integral: `ceil(real)` is double
		// precision there, and the two members are exact, so the rows match
		// for a reason that has nothing to do with the width.
		{"InCeilWidens", "CEIL(r_val) IN (4, 8)", []int64{3, 7}},
		// NOT IN over the same computed operand, and the DOUBLE control: the
		// narrowing must follow the operand's type and not the function.
		{"NotInAbs", "ABS(r_val) NOT IN (3.1, 7.1)",
			join([]int64{0, 1, 2}, []int64{4, 5, 6}, seq(8, 17), []int64{19, 20, 21, 22, 23})},
		{"InAbsOfDouble", "ABS(d_val) IN (3.1, 7.1)", []int64{3, 7}},
		// Arity 1 still WIDENS over a computed operand, exactly as over a
		// bare column: PostgreSQL builds no array for a single member.
		{"InAbsSingleWidens", "ABS(r_val) IN (3.1)", nil},

		// --- Column-to-column: no literal, so no width to resolve ---
		//
		// Unchanged by #631 and asserted so: PostgreSQL compares real to real
		// directly (every non-NULL row matches) and real to double by
		// widening the real (only the rows whose value survives the round
		// trip). A change that widened the COLUMN pair, or narrowed the
		// double one, moves one of these.
		{"EqRealColumn", "r_val = r_other", join(seq(0, 17), []int64{19, 20, 21, 22, 23})},
		{"EqDoubleColumn", "r_val = d_val", []int64{16, 17, 19, 20, 21, 22, 23}},

		// A finite literal past real's range is an ordinary double for `=`:
		// no row equals it, and PostgreSQL raises NO error (the 22003 belongs
		// to the multi-element IN, which casts to real[] — #549).
		{"EqOverRange", "r_val = 1e40", nil},

		// --- An explicit CAST TO REAL narrows, and is not a no-op ---
		//
		// PostgreSQL types the cast float4 and rounds the value into it, so
		// the comparison happens at REAL width and the non-representable
		// literal finds its row — the opposite answer from the same literal
		// written bare:
		//
		//	r_val = 3.1                -> Filter: (r_val = '3.1'::double precision) -> {}
		//	r_val = CAST(3.1 AS REAL)  -> Filter: (r_val = '3.1'::real)             -> {3}
		//	r_val IN (CAST(3.1 AS REAL), 7.1)
		//	                           -> Filter: (r_val = ANY ('{3.1,7.1}'::real[]))
		//	d_val = CAST(3.1 AS REAL)  -> Filter: (d_val = '3.1'::real)             -> {}
		//
		// The last one is the proof the cast really narrowed: the DOUBLE
		// column holds 3.1 exactly, and it stops matching once the literal has
		// been through float4. While CAST(x AS REAL) was a no-op all three
		// answered as if the cast were not written.
		{"EqCastReal", "r_val = CAST(3.1 AS REAL)", []int64{3}},
		{"InCastReal", "r_val IN (CAST(3.1 AS REAL), 7.1)", []int64{3, 7}},
		{"DoubleEqCastReal", "d_val = CAST(3.1 AS REAL)", nil},

		// --- An INTEGER literal keeps PostgreSQL's float order ---
		//
		// NaN is the greatest float value and equal to itself, so row 21
		// answers `>`, `>=` and `<>` against any finite constant and answers
		// `<` and `<=` against none. The ROW path — which is what the stage
		// DAG compiles every scan-pushed filter to — used Go's IEEE operators
		// for a mixed int/float pair, so it dropped the NaN row for `> 1`
		// while keeping it for `> 1.0`: the same predicate, two answers,
		// decided by whether the literal was spelled with a decimal point.
		// #459 closed this order everywhere else; this pair of spellings is
		// what kept the gap invisible.
		{"GtIntegerLiteral", "r_val > 1", join(seq(1, 15), []int64{17, 20, 21})},
		{"GtFloatLiteral", "r_val > 1.0", join(seq(1, 15), []int64{17, 20, 21})},
		{"GeIntegerLiteral", "r_val >= 1", join(seq(1, 15), []int64{17, 20, 21})},
		{"LtIntegerLiteral", "r_val < 1", []int64{0, 16, 19, 22, 23}},
		{"NeIntegerLiteral", "r_val <> 1", join(seq(0, 17), []int64{19, 20, 21, 22, 23})},
		// The FLOAT64 column has no width question at all, and lost the same
		// order for the same reason.
		{"DoubleGtIntegerLiteral", "d_val > 1", join(seq(1, 15), []int64{17, 20, 21})},
		{"DoubleNeIntegerLiteral", "d_val <> 1", join(seq(0, 17), []int64{19, 20, 21, 22, 23})},

		// --- A NEGATIVE member is still a constant ---
		//
		// The sign is a unary operator over the literal in the AST, and
		// reading that as "not a constant" took the narrowing — and the
		// refusal below — away from the whole list. PostgreSQL narrows exactly
		// as it does for a positive member.
		{"InMultiNegativeMember", "r_val IN (-1.0, 3.1)", []int64{3}},

		// --- A REAL-TYPED OPERAND, not just a real column ---
		//
		// PostgreSQL resolves the array's element type over the members AND
		// the probed expression, so any real-typed left operand pulls the list
		// to real[] (EXPLAIN VERBOSE):
		//
		//	-r_val IN (-3.1, -7.1)         -> ((- r_val) = ANY ('{-3.1,-7.1}'::real[]))
		//	CAST(d_val AS REAL) IN (3.1,…) -> ((d_val)::real = ANY ('{3.1,7.1}'::real[]))
		//	(r_val + 0) IN (3.1, 7.1)      -> (… = ANY ('{3.1,7.1}'::double precision[]))
		//
		// The third is the control: an INTEGER literal added to a real gives
		// DOUBLE PRECISION in PostgreSQL (pg_typeof), so that one must STAY
		// widened — which is why the rule is the operand's resolved TYPE and
		// not "walk down to a column". The two `=` companions are the scalar
		// halves, which widen whatever the operand is.
		{"NegatedOperandIn", "-r_val IN (-3.1, -7.1)", []int64{3, 7}},
		{"NegatedOperandEq", "-r_val = -3.1", nil},
		{"CastToRealOperandIn", "CAST(d_val AS REAL) IN (3.1, 7.1)", []int64{3, 7}},
		{"CastToRealOperandEq", "CAST(d_val AS REAL) = 3.1", nil},
		{"PlusZeroOperandStaysWidened", "(r_val + 0) IN (3.1, 7.1)", nil},

		// A literal at real's smallest DENORMAL is representable and must NOT
		// be refused — the boundary the underflow refusal has to respect.
		{"InMultiDenormalBoundary", "r_val IN (1e-45, 3.1)", []int64{3}},

		// --- #534: a FLOAT column against NaN and the infinities ----------
		//
		// float4 and float8 HOLD all three, so this is PostgreSQL's ordinary
		// float order (ADR-0012 item 8) — NaN above every value and equal to
		// itself, -Infinity below every one — and NOT the bound a DECIMAL
		// column gets, which exists only because that carrier has no such
		// value (ADR-0024 item 6). The row path used to fall through to a
		// LEXICOGRAPHIC text comparison here, which agrees with PostgreSQL on
		// every non-negative column; rows 22 and 23 are why these can fail.
		//
		// The CASE forces the row-at-a-time evaluator, which is what the DAG
		// compiles every scan-pushed filter to — so this is the arm #534's fix
		// had to reach on the distributed path. The BARE spelling used to be
		// left out here, because the vectorized kernel read a quoted constant
		// as 0.0; #646 closed that, so the bare forms are gated in the #646
		// block below (GtQuotedNegInfinity and its neighbours) and the
		// pg-oracle pins are deleted.
		{"GtNegInfinityInCase", "CASE WHEN r_val > '-Infinity' THEN 1 ELSE 0 END = 1",
			join(seq(0, 17), []int64{19, 20, 21, 22, 23})},
		{"LtNegInfinityInCase", "CASE WHEN r_val < '-Infinity' THEN 1 ELSE 0 END = 1", nil},
		// The NaN ROW is what these turn on: row 21 is NOT below NaN (NaN
		// equals itself) and IS above Infinity, so a reading that treats the
		// literal as anything else moves it.
		{"LtNaNInCase", "CASE WHEN r_val < 'NaN' THEN 1 ELSE 0 END = 1",
			join(seq(0, 17), []int64{19, 20, 22, 23})},
		{"EqNaNInCase", "CASE WHEN r_val = 'NaN' THEN 1 ELSE 0 END = 1", []int64{21}},
		{"LeInfinityInCase", "CASE WHEN r_val <= 'Infinity' THEN 1 ELSE 0 END = 1",
			join(seq(0, 17), []int64{19, 20, 22, 23})},
		{"GtInfinityInCase", "CASE WHEN r_val > 'Infinity' THEN 1 ELSE 0 END = 1", []int64{21}},
		// A SIGNED NaN is where the FLOAT grammar and the NUMERIC one part:
		// float reads '+NaN' and '-NaN' as NaN, numeric refuses both with
		// 22P02. So this ANSWERS here and stays refused against a DECIMAL.
		{"LtNegatedNaNInCase", "CASE WHEN r_val < '-NaN' THEN 1 ELSE 0 END = 1",
			join(seq(0, 17), []int64{19, 20, 22, 23})},
		// float8, so the rule is not pinned to the narrow width.
		{"DoubleGtNegInfinityInCase", "CASE WHEN d_val > '-Infinity' THEN 1 ELSE 0 END = 1",
			join(seq(0, 17), []int64{19, 20, 21, 22, 23})},
		{"DoubleLtNaNInCase", "CASE WHEN d_val < 'NaN' THEN 1 ELSE 0 END = 1",
			join(seq(0, 17), []int64{19, 20, 22, 23})},

		// --- #646: a QUOTED literal is coerced to the COLUMN's type -------
		//
		// This is the OPPOSITE direction from the widening above, and both are
		// PostgreSQL's. An unquoted decimal constant is `numeric` and drags
		// the comparison up to float8; a QUOTED one is unknown-typed and is
		// coerced straight to real (EXPLAIN VERBOSE, postgres:17):
		//
		//	r_val = 3.1    ->  (r_val = '3.1'::double precision)   -> {}
		//	r_val = '3.1'  ->  (r_val = '3.1'::real)               -> {3}
		//
		// The two spellings of one number are two predicates, and the pair
		// below is the whole gate: an engine that widens both, or narrows
		// both, fails one of them. Wadjet did NEITHER — kernel.toFloat64 has
		// no string arm, so every quoted constant read as 0.0 and `r_val =
		// '3.1'` selected row 19, the zero row.
		{"EqQuotedNonRepresentable", "r_val = '3.1'", []int64{3}},
		{"NeQuotedNonRepresentable", "r_val <> '3.1'",
			join(seq(0, 2), seq(4, 17), []int64{19, 20, 21, 22, 23})},
		{"LtQuoted", "r_val < '3.1'", []int64{0, 1, 2, 16, 17, 19, 22, 23}},
		{"LeQuoted", "r_val <= '3.1'", []int64{0, 1, 2, 3, 16, 17, 19, 22, 23}},
		{"GtQuoted", "r_val > '3.1'", join(seq(4, 15), []int64{20, 21})},
		{"GeQuoted", "r_val >= '3.1'", join(seq(3, 15), []int64{20, 21})},
		{"BetweenQuoted", "r_val BETWEEN '3.1' AND '4.1'", []int64{3, 4}},
		// The ZERO row is the one every "read the constant as the type's
		// zero" defect lands on, so it is asserted from both sides: '0'
		// selects it and '3.1' does not.
		{"EqQuotedZero", "r_val = '0'", []int64{19}},
		// IN narrows at BOTH arities for a quoted member — `r IN ('3.1')`
		// plans as `r = '3.1'::real` where `r IN (3.1)` plans as `r =
		// '3.1'::double precision` — so the single-element form answers the
		// row the unquoted one misses. Mixed lists narrow too: the array is
		// real[] whatever the members were spelled as.
		{"InSingleQuoted", "r_val IN ('3.1')", []int64{3}},
		{"InMultiQuoted", "r_val IN ('3.1', '7.1')", []int64{3, 7}},
		{"NotInMultiQuoted", "r_val NOT IN ('3.1', '7.1')",
			join(seq(0, 2), []int64{4, 5, 6}, seq(8, 17), []int64{19, 20, 21, 22, 23})},
		{"InMixedQuoted", "r_val IN ('3.1', 7.1)", []int64{3, 7}},
		// 16777217 is exact in double and not in real, so the QUOTED spelling
		// rounds onto row 20 (2^24) and matches where the unquoted spelling
		// (EqIntegerLiteralPastMantissa, above) answers nothing. One value,
		// two spellings, two answers — the narrowing stated as a row.
		{"EqQuotedIntegerPastMantissa", "r_val = '16777217'", []int64{20}},
		// PostgreSQL's float input is strtod, which reads C99 HEX floats —
		// '0x1p3' is 8. Refusing it would be a PG-superset regression.
		{"LtQuotedHexFloat", "r_val < '0x1p3'", []int64{0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 19, 22, 23}},
		{"EqQuotedHexFloat", "r_val = '0x1p3'", nil},
		// C whitespace is trimmed, so this is the LtQuoted row set.
		{"LtQuotedSpaced", "r_val < ' 3.1 '", []int64{0, 1, 2, 16, 17, 19, 22, 23}},
		// A literal at real's smallest DENORMAL is a value, not an underflow.
		{"EqQuotedDenormal", "r_val = '1e-45'", nil},
		// The specials, BARE — the shape the vectorized kernel answered as
		// `> 0.0` and `< 0.0`. The CASE-wrapped twins above gate the row path;
		// these gate the kernel, and rows 22/23 are what makes them able to
		// fail (a non-negative column answers the same either way).
		{"GtQuotedNegInfinity", "r_val > '-Infinity'",
			join(seq(0, 17), []int64{19, 20, 21, 22, 23})},
		{"LtQuotedNaN", "r_val < 'NaN'", join(seq(0, 17), []int64{19, 20, 22, 23})},
		{"EqQuotedNaN", "r_val = 'NaN'", []int64{21}},
		{"LeQuotedInfinity", "r_val <= 'Infinity'", join(seq(0, 17), []int64{19, 20, 22, 23})},
		{"GtQuotedInfinity", "r_val > 'Infinity'", []int64{21}},
		// float8, where no width question arises at all: the defect there was
		// the silent zero on its own.
		{"DoubleEqQuoted", "d_val = '3.1'", []int64{3}},
		{"DoubleLtQuoted", "d_val < '3.1'", []int64{0, 1, 2, 16, 17, 19, 22, 23}},
		{"DoubleInMultiQuoted", "d_val IN ('3.1', '7.1')", []int64{3, 7}},
		{"DoubleGtQuotedNegInfinity", "d_val > '-Infinity'",
			join(seq(0, 17), []int64{19, 20, 21, 22, 23})},
		{"DoubleLtQuotedNaN", "d_val < 'NaN'", join(seq(0, 17), []int64{19, 20, 22, 23})},
		// A literal past REAL's range but inside DOUBLE's is a value here and
		// a 22003 one column over — the range check is the column's, not the
		// literal's.
		{"DoubleEqOverReal", "d_val = '1e39'", nil},

		// --- #646 at the BOXED sites, over a real column ------------------
		//
		// A simple CASE's WHEN, IS DISTINCT FROM, GREATEST/LEAST and NULLIF
		// all compare through expr.boxedPair, where the column arrives as a
		// float32 box and the literal as its text. They read the DECIMAL
		// grammar before this and answered ok=false for anything it could not
		// read, falling through to compare().
		{"CaseWhenQuoted", "CASE WHEN r_val = '3.1' THEN 1 ELSE 0 END = 1", []int64{3}},
		{"CaseLtQuoted", "CASE WHEN r_val < '3.1' THEN 1 ELSE 0 END = 1",
			[]int64{0, 1, 2, 16, 17, 19, 22, 23}},
		{"SimpleCaseQuoted", "CASE r_val WHEN '3.1' THEN 1 ELSE 0 END = 1", []int64{3}},
		// IS DISTINCT FROM is total over NULL, so row 18 is in the answer.
		{"IsDistinctQuoted", "r_val IS DISTINCT FROM '3.1'",
			join(seq(0, 2), seq(4, 23))},
		// GREATEST/LEAST ignore a NULL argument in PostgreSQL, so row 18
		// answers `GREATEST(NULL,'3.1') = '3.1'`.
		{"GreatestQuoted", "GREATEST(r_val, '3.1') = '3.1'",
			[]int64{0, 1, 2, 3, 16, 17, 18, 19, 22, 23}},
		{"LeastQuoted", "LEAST(r_val, '3.1') = '3.1'",
			join(seq(3, 15), []int64{18, 20, 21})},
		{"NullifQuoted", "NULLIF(r_val, '3.1') IS NULL", []int64{3, 18}},
		{"DoubleCaseLtQuoted", "CASE WHEN d_val < '3.1' THEN 1 ELSE 0 END = 1",
			[]int64{0, 1, 2, 16, 17, 19, 22, 23}},

		// --- #646 over the INTEGER column, including the boxed sites ------
		//
		// r_key is a BIGINT holding 0..23, so the integer grammar's own
		// answers are visible here. The radix and underscore forms are #634's
		// PG-superset gap, closed with the same parser: PostgreSQL reads
		// '0x0A' and '1_0' as ten.
		{"KeyEqQuoted", "r_key = '3'", []int64{3}},
		{"KeyLtQuoted", "r_key < '3'", []int64{0, 1, 2}},
		{"KeyInQuoted", "r_key IN ('3', '7')", []int64{3, 7}},
		{"KeyCaseLtQuoted", "CASE WHEN r_key < '3' THEN 1 ELSE 0 END = 1", []int64{0, 1, 2}},
		{"KeyGreatestQuoted", "GREATEST(r_key, '3') = '3'", []int64{0, 1, 2, 3}},
		{"KeyNullifQuoted", "NULLIF(r_key, '3') IS NULL", []int64{3}},
		{"KeyEqUnderscore", "r_key = '1_0'", []int64{10}},
		{"KeyEqHex", "r_key = '0x0A'", []int64{10}},

		// --- The COMPOSITE's type is the call's, not the pair's (#646 review)
		//
		// GREATEST/LEAST, CASE and COALESCE resolve ONE type over every
		// argument (select_common_type) and coerce the unknown-typed literal
		// to THAT. Reading each argument's OWN type instead — which is what a
		// pairwise comparison does, and what a kind folded down to "some
		// number" leaves the row's BOX to decide — gives three different
		// wrong answers, and all three are here:
		//
		//   - a PG-SUPERSET REGRESSION, where the pair's narrower type
		//     refuses a literal the call's type reads. `GREATEST(r_key,
		//     '3.1', d_val)` folds to double precision and answers; asking
		//     bigint's input function for '3.1' is 22P02.
		//   - a SILENT one, where the literal is read at the wrong WIDTH.
		//   - a DATA-DEPENDENT one, where the box differs per row: the NULL
		//     row of `COALESCE(r_val, 0)` boxes an int64 and every other row
		//     a float64.
		//
		// Row 18 is all-NULL and row 21 is NaN, and both are in these answers
		// on purpose: PostgreSQL's GREATEST/LEAST SKIP a NULL argument (so
		// `GREATEST(NULL,'3.1',NULL)` is 3.1, which is > 0) and NaN is the
		// greatest float (so it wins every GREATEST and fails every `<=`).
		{"GreatestIntQuotedFracDouble", "GREATEST(r_key, '3.1', d_val) > 0", seq(0, 23)},
		{"LeastIntQuotedFracDouble", "LEAST(r_key, '3.1', d_val) > 0", join(seq(1, 18), []int64{20, 21})},
		// The literal is out of REAL's range and inside DOUBLE's, and the
		// call folds to double — so it is a value, not the 22003 the real
		// arm would raise.
		{"GreatestRealQuotedOverReal", "GREATEST(r_val, '1e39', d_val) > 0", seq(0, 23)},
		{"LeastRealQuotedOverReal", "LEAST(r_val, '1e39', d_val) > 0", join(seq(0, 18), []int64{20, 21})},
		// No literal inside the call at all: the composite's own folded type
		// is what the OUTER quoted literal is coerced to. real ∪ bigint is
		// real, so '3.1' narrows and finds row 3.
		{"GreatestIntRealEqQuoted", "GREATEST(r_key, r_val) = '3.1'", []int64{3}},
		{"CaseIntRealEqQuoted", "CASE WHEN r_key > 0 THEN r_key ELSE r_val END = '3.1'", nil},
		{"CoalesceRealIntEqQuoted", "COALESCE(r_val, 0) = '3.1'", []int64{3}},
		// The same COALESCE against a literal only the REAL rounding can
		// match: 16777217 narrows onto row 20's 2^24.
		{"CoalesceRealIntEqQuotedBig", "COALESCE(r_val, 0) = '16777217'", []int64{20}},
		{"CoalesceIntRealEqQuoted", "COALESCE(r_key, 0) = '3'", []int64{3}},
		// WIDTH, in both directions over one literal. The three-argument form
		// folds to DOUBLE, where 16777217 is exact and beats the bound; the
		// two-argument form folds to REAL, where it rounds to 2^24 and does
		// not. Same literal, same bound, opposite answers — which is the
		// whole of "the call's type decides".
		{"GreatestRealQuotedIntDouble", "GREATEST(r_val, '16777217', d_val) <= 16777216.5", nil},
		{"GreatestDoubleQuotedIntReal", "GREATEST(d_val, '16777217', r_val) <= 16777216.5", nil},
		{"GreatestRealOnlyQuotedInt", "GREATEST(r_val, '16777217') <= 16777216.5",
			join(seq(0, 20), []int64{22, 23})},
		// Argument ORDER changes nothing, which is why the permutation is
		// here: the fold is over the SET of arguments.
		{"GreatestDoubleQuotedFracInt", "GREATEST(d_val, '3.1', r_key) > 0", seq(0, 23)},
		{"GreatestIntQuotedIntDouble", "GREATEST(r_key, '3', d_val) > 0", seq(0, 23)},
		// A composite inside another boxed site.
		{"NullifGreatestIntRealQuoted", "NULLIF(GREATEST(r_key, r_val), '3.1') IS NULL", []int64{3}},

		// --- a NUMERIC-typed CONSTANT arm (#646 review, B4) ---------------
		//
		// PostgreSQL types an unsuffixed constant `numeric` as soon as it
		// carries a decimal point or an exponent, so `COALESCE(real, 0.0)` is
		// real ∪ numeric — which resolves to REAL — and `COALESCE(bigint,
		// 0.0)` is numeric. DECIMAL had no rung on this layer's ladder, so
		// both folds failed, the pair declined, and compare() ordered the
		// rendered number against the literal BYTEWISE: `> '9'` kept the two
		// rows whose text sorts above "9" instead of the nine whose VALUE
		// does.
		//
		// The three spellings of the constant are all here because they are
		// all `numeric` to PostgreSQL and it is the SPELLING that decides —
		// `0` would be an integer constant and a different fold.
		{"CoalesceRealNumericConstGt", "COALESCE(r_val, 0.0) > '9'",
			join(seq(9, 15), []int64{20, 21})},
		{"CoalesceRealNumericConstHalf", "COALESCE(r_val, 0.5) > '9'",
			join(seq(9, 15), []int64{20, 21})},
		{"CoalesceRealNumericConstExp", "COALESCE(r_val, 1e0) > '9'",
			join(seq(9, 15), []int64{20, 21})},
		{"GreatestRealNumericConst", "GREATEST(r_val, 0.0) > '9'",
			join(seq(9, 15), []int64{20, 21})},
		{"CaseRealNumericConst", "CASE WHEN r_key > 0 THEN r_val ELSE 0.0 END > '9'",
			join(seq(9, 15), []int64{20, 21})},
		// The INTEGER column's fold lands on numeric rather than real, and
		// the NULL row is in the answer because COALESCE supplies 0.0 there.
		{"CoalesceIntNumericConst", "COALESCE(r_key, 0.0) > '9'", seq(10, 23)},
		// The literal that only a REAL fold can match, through a composite:
		// real ∪ numeric is real, so '3.1' narrows and finds row 3.
		{"CoalesceRealNumericConstEq", "COALESCE(r_val, 0.0) = '3.1'", []int64{3}},
	}
}

// TestRealComparisonWidthTwoPath holds the single-process engine and the
// stage DAG to PostgreSQL's comparison width for a `real` column, predicate by
// predicate (#631 the scalar operators, #633 the distributed IN list).
func TestRealComparisonWidthTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	for _, c := range rwpWant() {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT r_key FROM %s WHERE %s ORDER BY r_key", rwpTable, c.where)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				got := rwpKeys(t, dtpRun(t, ctx, single, coord, sql, arm.dag))
				if !rwpEqual(got, c.want) {
					t.Errorf("%s: %s\n  got  %v\n  want %v (PostgreSQL 17)",
						arm.name, sql, got, c.want)
				}
			}
		})
	}

	// The VALUE half rides this cluster rather than standing its own: the
	// package convention, and what keeps internal/coordinator inside CI's
	// default 10-minute package timeout.
	t.Run("ExtremumWinnerMaterialization", func(t *testing.T) {
		runExtremumWinnerMaterialization(t, ctx, single, coord)
	})
}

// pgRealOverflowText is the DIGITS PostgreSQL names in the 22003 message for
// the over-range literal below. It prints the same forty-one digits whatever
// spelling the query used — 1e40, 1e+40 or the number written out — because
// the cast that fails is numeric->real and a numeric's text is its digits:
//
//	ERROR:  "10000000000000000000000000000000000000000" is out of range for type real
//
// Both wadjet paths used to print something else, and something DIFFERENT from
// each other ("1e+40" through the kernel, "1e40" through the row evaluator), so
// asserting the text is what keeps the two spellings from drifting apart again.
const pgRealOverflowText = `"10000000000000000000000000000000000000000" is out of range for type real`

// The other three refusals PostgreSQL gives for a real[] cast, each with its
// own digits. Two are the same rule in the other direction or with a sign; the
// UNDERFLOW one is a separate failure mode: 1e-46 narrows to 0.0, which would
// MATCH every row holding zero rather than merely miss, so the list answered
// {3, 19} where PostgreSQL refuses it.
const (
	pgRealUnderflowText = `"0.0000000000000000000000000000000000000000000001" is out of range for type real`
	pgRealNegOverflow   = `"-10000000000000000000000000000000000000000" is out of range for type real`
)

// TestRealInOverRangeLiteralRaisesOnBothPaths is the error half of the arity
// rule, which only shows up once the row path narrows too (#633).
//
// Narrowing a finite literal past real's range yields ±Inf, which would MATCH
// a genuine infinite row. PostgreSQL raises numeric_value_out_of_range for the
// whole predicate when it casts the array to real[], and answers a
// SINGLE-element list — which widens instead — with no rows and no error:
//
//	WHERE r_val IN (1e40, 3.1)  ->  ERROR 22003 ... out of range for type real
//	WHERE r_val IN (1e40)       ->  0 rows, no error
//	WHERE r_val = 1e40          ->  0 rows, no error
//
// Before this, the DAG answered the multi-element form with an empty result:
// the widened comparison simply missed, so an error PostgreSQL raises became a
// value, on the distributed path only.
func TestRealInOverRangeLiteralRaisesOnBothPaths(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	// Each shape with the exact digits PostgreSQL names for it. The list is
	// the four ways a member can fail to be a real: too large, too large and
	// negative, too small to be anything but zero, and too large behind a
	// real-typed EXPRESSION rather than a column.
	for _, c := range []struct{ where, want string }{
		{"r_val IN (1e40, 3.1)", pgRealOverflowText},
		{"r_val IN (-1e40, 3.1)", pgRealNegOverflow},
		// UNDERFLOW. 1e-46 narrows to 0.0, so the list did not merely miss —
		// it MATCHED the row holding zero, answering {3, 19} where PostgreSQL
		// refuses the query. Its representable neighbour 1e-45 is asserted as
		// a VALUE case (InMultiDenormalBoundary) so the boundary is pinned
		// from both sides.
		{"r_val IN (1e-46, 3.1)", pgRealUnderflowText},
		{"-r_val IN (1e40, 3.1)", pgRealOverflowText},
	} {
		sql := fmt.Sprintf("SELECT r_key FROM %s WHERE %s ORDER BY r_key", rwpTable, c.where)
		for _, arm := range []struct {
			name string
			run  func() error
		}{
			{"single", func() error { _, err := tmdRunSingle(ctx, single, sql); return err }},
			{"dag", func() error { _, err := tmdRunDAG(ctx, coord, sql); return err }},
		} {
			err := arm.run()
			if err == nil {
				t.Errorf("%s: %s returned rows; PostgreSQL raises 22003", arm.name, sql)
				continue
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("%s: %s raised %v, want the 22003 refusal naming %q",
					arm.name, sql, err, c.want)
			}
		}
	}

	// The refusal is a PLAN-time one in PostgreSQL: the array is cast to
	// real[] during parse analysis, so the error does not depend on a row
	// being examined — or on the predicate being REACHABLE at all. Both
	// shapes below returned rows on at least one path while the raise lived
	// in the row loop: the kernel resolves on the first BATCH, so an empty
	// scan never raised, and the row evaluator raises on the first non-NULL
	// value, so a predicate that only ever meets NULLs never raised either.
	for _, c := range []struct{ where, want string }{
		// Reachable, but only ever on rows whose r_val IS NULL — the boxed
		// path returns on the nil operand before it can raise.
		{"r_val IS NULL AND r_val IN (1e40, 3.1)", pgRealOverflowText},
		// Not reachable at all: no row survives the first conjunct, so
		// neither the kernel nor the row evaluator ever sees the list.
		{"r_key < 0 AND r_val IN (1e40, 3.1)", pgRealOverflowText},
		// The negated form: PostgreSQL raises for `NOT IN` too — the cast
		// happens whatever the operator does with the result.
		{"r_key < 0 AND r_val NOT IN (1e40, 3.1)", pgRealOverflowText},
		// A NEGATIVE member beside the offending one. The sign is a unary
		// operator over the literal in the AST, and reading that as "not a
		// constant" disarmed the whole check: this returned 0 rows on BOTH
		// paths, since no row reaches the predicate to trip the backstop.
		{"r_key < 0 AND r_val IN (-1.0, 1e40)", pgRealOverflowText},
		// The underflow, unreachable, so only a plan-time refusal can see it.
		{"r_key < 0 AND r_val IN (1e-46, 3.1)", pgRealUnderflowText},
	} {
		sql := fmt.Sprintf("SELECT r_key FROM %s WHERE %s", rwpTable, c.where)
		for _, arm := range []struct {
			name string
			run  func() error
		}{
			{"single", func() error { _, err := tmdRunSingle(ctx, single, sql); return err }},
			{"dag", func() error { _, err := tmdRunDAG(ctx, coord, sql); return err }},
		} {
			err := arm.run()
			if err == nil {
				t.Errorf("%s: %s returned rows; PostgreSQL raises 22003 at plan time",
					arm.name, sql)
				continue
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("%s: %s raised %v, want the 22003 refusal naming %q",
					arm.name, sql, err, c.want)
			}
		}
	}

	// The single-element form widens and is not an error at all.
	for _, sql := range []string{
		fmt.Sprintf("SELECT r_key FROM %s WHERE r_val IN (1e40) ORDER BY r_key", rwpTable),
		fmt.Sprintf("SELECT r_key FROM %s WHERE r_val = 1e40 ORDER BY r_key", rwpTable),
	} {
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			if rows := dtpRun(t, ctx, single, coord, sql, arm.dag); len(rows) != 0 {
				t.Errorf("%s: %s returned %d rows; PostgreSQL answers none, with no error",
					arm.name, sql, len(rows))
			}
		}
	}

	// The QUOTED-literal refusals ride this cluster for the same reason.
	t.Run("QuotedNumericLiteralRefusal", func(t *testing.T) {
		runQuotedNumericLiteralRefusals(t, ctx, single, coord)
	})
}

// TestQuotedNumericLiteralRefusalIsOnBothPaths is the ERROR half of #646: a
// quoted literal a numeric column's own input function cannot read is a query
// error, at every comparison site and on both execution paths.
//
// PostgreSQL coerces an unknown-typed literal with the COLUMN's input
// function, so the refusal is per type and both SQLSTATEs are live: 22P02
// (invalid_text_representation) for text that names no value, 22003
// (numeric_value_out_of_range) for a number the type cannot carry. The
// wording is PostgreSQL's, taken live from postgres:17-alpine over this exact
// fixture — it names the literal's TEXT VERBATIM here, unlike the numeric->real
// cast above, which names the numeric's DIGITS.
//
// Every one of these ANSWERED before: the float arms read a quoted constant as
// 0.0 (`r_val = 'abc'` selected the zero row), and the boxed arms fell through
// to compare(), which finds no reading of a number against "NaN" and reports
// FALSE — a value answer to a question that has none.
//
// The last two shapes are the reason the refusal cannot live in the row loop:
// a predicate no row reaches, and one that only ever meets NULLs, still error
// in PostgreSQL because the coercion happens at parse analysis.
func runQuotedNumericLiteralRefusals(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator) {

	for _, c := range []struct{ name, where, want string }{
		// --- real: 22P02 -------------------------------------------------
		{"RealEqGarbage", "r_val = 'abc'", `invalid input syntax for type real: "abc"`},
		{"RealNeGarbage", "r_val <> 'abc'", `invalid input syntax for type real: "abc"`},
		{"RealEqEmpty", "r_val = ''", `invalid input syntax for type real: ""`},
		// PostgreSQL's FLOAT input does NOT take the underscore separators its
		// integer and numeric inputs take since 16 (verified live).
		{"RealEqUnderscore", "r_val = '1_000'", `invalid input syntax for type real: "1_000"`},
		{"RealInGarbage", "r_val IN ('abc', '3.1')", `invalid input syntax for type real: "abc"`},
		{"RealBetweenGarbage", "r_val BETWEEN 'abc' AND '4.1'",
			`invalid input syntax for type real: "abc"`},
		{"RealCaseGarbage", "CASE WHEN r_val < 'abc' THEN 1 ELSE 0 END = 1",
			`invalid input syntax for type real: "abc"`},
		{"RealIsDistinctGarbage", "r_val IS DISTINCT FROM 'abc'",
			`invalid input syntax for type real: "abc"`},
		{"RealGreatestGarbage", "GREATEST(r_val, 'abc') IS NOT NULL",
			`invalid input syntax for type real: "abc"`},
		{"RealNullifGarbage", "NULLIF(r_val, 'abc') IS NOT NULL",
			`invalid input syntax for type real: "abc"`},
		// --- real: 22003, the literal's own text ---------------------------
		{"RealEqPastDouble", "r_val = '1e400'", `"1e400" is out of range for type real`},
		{"RealEqPastReal", "r_val = '1e39'", `"1e39" is out of range for type real`},
		// UNDERFLOW is a range failure too, and the boundary is real's
		// smallest DENORMAL: '1e-45' is a value (EqQuotedDenormal above) and
		// '7e-46' rounds to zero, which would MATCH the zero row.
		{"RealEqUnderflow", "r_val = '7e-46'", `"7e-46" is out of range for type real`},
		// --- double precision ---------------------------------------------
		{"DoubleEqGarbage", "d_val = 'abc'",
			`invalid input syntax for type double precision: "abc"`},
		{"DoubleEqPastDouble", "d_val = '1e400'",
			`"1e400" is out of range for type double precision`},
		// --- bigint: the #664 boxed sites ---------------------------------
		//
		// These four are the shapes the umbrella filed: every one of them
		// returned every row, because refuseArm and extremumRefusal tested
		// batch.TypeDecimal alone.
		{"KeyEqGarbage", "r_key = 'abc'", `invalid input syntax for type bigint: "abc"`},
		{"KeyEqFraction", "r_key = '3.1'", `invalid input syntax for type bigint: "3.1"`},
		{"KeyCaseNaN", "CASE WHEN r_key < 'NaN' THEN 1 ELSE 0 END = 1",
			`invalid input syntax for type bigint: "NaN"`},
		{"KeyIsDistinctNaN", "r_key IS DISTINCT FROM 'NaN'",
			`invalid input syntax for type bigint: "NaN"`},
		{"KeyGreatestNaN", "GREATEST(r_key, 'NaN') IS NOT NULL",
			`invalid input syntax for type bigint: "NaN"`},
		{"KeyNullifGarbage", "NULLIF(r_key, 'abc') IS NOT NULL",
			`invalid input syntax for type bigint: "abc"`},
		{"KeySimpleCaseGarbage", "CASE r_key WHEN 'abc' THEN 1 ELSE 0 END = 1",
			`invalid input syntax for type bigint: "abc"`},
		{"KeyEqPastBigint", "r_key = '99999999999999999999'",
			`value "99999999999999999999" is out of range for type bigint`},
		// --- the refusal is not a property of a row ------------------------
		{"UnreachablePredicate", "r_key < 0 AND r_val = 'abc'",
			`invalid input syntax for type real: "abc"`},
		{"NullOnlyPredicate", "r_val IS NULL AND r_val = 'abc'",
			`invalid input syntax for type real: "abc"`},
		// --- the COMPOSITE's folded type is what the message names --------
		//
		// A literal no numeric type can read is still refused inside a
		// composite, and the type in the message is the CALL's, not the first
		// column's: PostgreSQL folds `GREATEST(bigint, 'abc', double)` to
		// double precision and says so.
		{"GreatestGarbageFoldsToDouble", "GREATEST(r_key, 'abc', d_val) > 0",
			`invalid input syntax for type double precision: "abc"`},
		{"LeastGarbageFoldsToDouble", "LEAST(r_key, 'abc', d_val) > 0",
			`invalid input syntax for type double precision: "abc"`},
		// A range failure follows the fold too: real ∪ int4 is real, so
		// '1e39' is out of range HERE where the double fold above reads it.
		{"CoalesceRealIntOverReal", "COALESCE(r_val, 0) = '1e39'",
			`"1e39" is out of range for type real`},
		{"GreatestRealOnlyOverReal", "GREATEST(r_val, '1e39') > 0",
			`"1e39" is out of range for type real`},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT r_key FROM %s WHERE %s", rwpTable, c.where)
			for _, arm := range []struct {
				name string
				run  func() error
			}{
				{"single", func() error { _, err := tmdRunSingle(ctx, single, sql); return err }},
				{"dag", func() error { _, err := tmdRunDAG(ctx, coord, sql); return err }},
			} {
				err := arm.run()
				if err == nil {
					t.Errorf("%s: %s returned rows; PostgreSQL refuses it", arm.name, sql)
					continue
				}
				if !strings.Contains(err.Error(), c.want) {
					t.Errorf("%s: %s raised %v, want a refusal naming %q", arm.name, sql, err, c.want)
				}
			}
		})
	}
}

// TestExtremumWinnerIsMaterializedAtTheCallsType is the VALUE half of the
// composite rule (#646 review): when the QUOTED literal is the argument that
// wins, GREATEST/LEAST must answer the NUMBER at the call's folded type — not
// the Go string the literal arrived as.
//
// Returning the string was a crash on one argument order and a wrong value on
// the other: `GREATEST(d_val, '16777217', r_val)` projected four characters
// into a FLOAT64 vector ("cannot store string into FLOAT64 vector") while
// `GREATEST(r_val, '16777217', d_val)` answered the string through a path that
// happened to accept it. PostgreSQL answers the double 16777217 for both.
//
// Every case picks a row on which the LITERAL wins, so the assertion is on the
// literal's own materialization rather than on which argument was chosen: row
// 19 holds zero in r_val and d_val. The fold is DOUBLE (real ∪ double), where
// 16777217 is exact — the two-argument REAL fold, where it rounds to 2^24, is
// asserted as a row set by GreatestRealOnlyQuotedInt above. Both wants are
// PostgreSQL 17.11's over this fixture.
//
// The DECLARED type of the call is a SEPARATE layer, and until #724 it was
// the first decided argument's rather than the fold's — a QUOTED literal was
// typed a DECIDED string, so expr.CommonDeclType could not fold a list holding
// one and answered decided[0]. These two cases put the DOUBLE column first,
// where the declaration agreed with the fold whatever that layer did; the
// permutations where it did not were pinned below at wadjet's wrong answers
// and are asserted at PostgreSQL's now.
func runExtremumWinnerMaterialization(t *testing.T, ctx context.Context, single *wadjet.DB, coord *Coordinator) {

	for _, c := range []struct {
		name string
		expr string
		key  int
		want float64
	}{
		// This one is the ex-CRASH: returning the literal's Go string here
		// raised "cannot store string into FLOAT64 vector".
		{"GreatestDoubleQuotedReal", "GREATEST(d_val, '16777217', r_val)", 19, 16777217},
		{"LeastDoubleQuotedReal", "LEAST(d_val, '-16777217', r_val)", 19, -16777217},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT %s AS v FROM %s WHERE r_key = %d", c.expr, rwpTable, c.key)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %s returned %d rows, want 1", arm.name, sql, len(rows))
				}
				got, ok := rows[0]["v"].(float64)
				if !ok {
					t.Fatalf("%s: %s answered %#v (%T), want a float64 — PostgreSQL answers %v",
						arm.name, sql, rows[0]["v"], rows[0]["v"], c.want)
				}
				if got != c.want {
					t.Errorf("%s: %s = %v, want %v (PostgreSQL 17)", arm.name, sql, got, c.want)
				}
			}
		})
	}

	// The same value through a call whose FIRST decided argument is NARROWER
	// than the fold. These were pinned at wadjet's wrong answers with
	// PostgreSQL's beside each until #724: physical.nodeDeclaredType typed a
	// QUOTED string literal `Decl(TypeString), Decided`, so a call holding one
	// had a non-numeric decider, expr.CommonDeclType could not fold, and the
	// PROJECTION was declared from the first argument while the COMPARISON and
	// the MATERIALIZATION were already the fold's.
	//
	// The last four are the part that was not merely a narrowing. A double
	// 1e39 stored into an INT64 vector is int64's MINIMUM, and a NaN is too —
	// #462's failure mode, which ADR-0012 item 6 forbids and ADR-0024 item 4
	// makes a 22003 — and it reached the GROUP BY key the last entry builds
	// from the same projection.
	//
	// It could not be fixed from the comparison layer: the value materialized
	// there feeds the COMPARISON as often as it feeds a store
	// (`GREATEST(r_val,'1e39',d_val) > 0` projects nothing), so narrowing or
	// refusing at materialization would answer a different predicate than
	// PostgreSQL's. The declaration is what had to move.
	//
	// Every want is PostgreSQL 17.11's own text for the row.
	for _, c := range []struct {
		name string
		expr string
		key  int
		want string
	}{
		{"RealFirstFoldsToDouble", "GREATEST(r_val, '16777217', d_val)", 19, "16777217"},
		{"BigintFirstFoldsToDouble", "GREATEST(r_key, '3.5', d_val)", 0, "3.5"},
		{"BigintFirstPastInt64Range", "GREATEST(r_key, r_val, d_val, '1e39')", 0, "1e+39"},
		{"BigintFirstNaN", "GREATEST(r_key, r_val, d_val, 'NaN')", 0, "NaN"},
		{"RealFirstPastRealRange", "GREATEST(r_val, d_val, r_key, '1e39')", 0, "1e+39"},
		{"GroupKeyCarriesTheFold", "GREATEST(r_key, r_val, d_val, '1e39')", 19, "1e+39"},
	} {
		t.Run(c.name, func(t *testing.T) {
			sql := fmt.Sprintf("SELECT %s AS v FROM %s WHERE r_key = %d", c.expr, rwpTable, c.key)
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %s returned %d rows, want 1", arm.name, sql, len(rows))
				}
				if got := nfCell(rows[0]["v"]); got != c.want {
					t.Errorf("%s: %s = %s (%T); PostgreSQL 17.11 answers %s",
						arm.name, sql, got, rows[0]["v"], c.want)
				}
			}
		})
	}
}

// TestRealKeyedOperationsAreUnmovedByWidth is the other half of the blast
// radius: ORDER BY, GROUP BY, DISTINCT and a hash-join key over a `real`
// column compare COLUMN to COLUMN, so no literal decides their width and
// #631 must leave them exactly where they were. Asserting it is cheaper than
// arguing it — a widening that leaked into the key encoders would split one
// group in two or drop a join pair here.
//
// It carries the CROSS-width keys too (#615). A key pair is resolved to
// PostgreSQL's common type now, and `real` is the type that makes the two
// resolution ladders disagree: a JOIN widens `real = double` to float8, while
// `real ∪ double` NARROWS nothing and resolves to double precision, and
// `real ∪ integer` resolves to real. Holding all three over one fixture is
// what stops a fix to either ladder from being applied to the other.
//
// Every expectation is PostgreSQL 17.11's, taken live over a table loaded
// with rwpData's exact float bits.
func TestRealKeyedOperationsAreUnmovedByWidth(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	cases := []struct {
		name string
		sql  string
		want string // the single scalar cell, rendered
	}{
		// 24 rows, 23 distinct non-NULL values plus the NULL: PostgreSQL
		// GROUP BY collects NULL into its own group, and the NaN into one of
		// its own, so 24.
		{"GroupByReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT r_val FROM %s GROUP BY r_val) s", rwpTable), "24"},
		{"DistinctReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT DISTINCT r_val FROM %s) s", rwpTable), "24"},
		// Every non-NULL row joins exactly itself: 23 pairs, the NaN row
		// included — NaN equals itself in PostgreSQL's float order, so it is
		// a join key like any other. r_other holds the same values as r_val,
		// so this is a real COLUMN-to-column key (float4 = float4, compared
		// at real width) rather than the self-join on ONE column that used to
		// stand here — that one could not fail: widening both sides of
		// `a.r_val = b.r_val` to float8 keeps every pair it had, so it passed
		// whichever width the key was built at (#615).
		{"JoinRealAgainstReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.r_val = b.r_other", rwpTable, rwpTable), "23"},
		// The CROSS-width shapes, which is what the entry above cannot see.
		// PostgreSQL compares `real = double precision` as float8 (float48eq,
		// no cast printed on either side), so the pairs that survive are
		// exactly the ones whose real value round-trips: 7 of the 24 rows,
		// both argument orders. Before #615 the key was built at each side's
		// own width — four bytes against eight — and both answered 0.
		{"JoinRealAgainstDouble", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.r_val = b.d_val", rwpTable, rwpTable), "7"},
		{"JoinDoubleAgainstReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s a JOIN %s b ON a.d_val = b.r_val", rwpTable, rwpTable), "7"},
		// The same predicate as a semi join, which reaches a different
		// operator (the key-only build and the semi/anti probe).
		{"RealInDouble", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s a WHERE a.r_val IN (SELECT b.d_val FROM %s b)",
			rwpTable, rwpTable), "7"},
		{"DoubleInReal", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM %s a WHERE a.d_val IN (SELECT b.r_val FROM %s b)",
			rwpTable, rwpTable), "7"},
		// And as a SET operation, which resolves by a DIFFERENT ladder:
		// `real ∪ double precision` is double precision (select_common_type),
		// so both arms are read at float8 and NULL is a value equal to
		// itself. 24 distinct widened reals meet 24 distinct doubles in 8.
		{"IntersectRealDouble", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT r_val AS v FROM %s INTERSECT SELECT d_val FROM %s) s",
			rwpTable, rwpTable), "8"},
		{"ExceptRealDouble", fmt.Sprintf(
			"SELECT COUNT(*) AS n FROM (SELECT r_val AS v FROM %s EXCEPT SELECT d_val FROM %s) s",
			rwpTable, rwpTable), "16"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, c.sql, arm.dag)
				if len(rows) != 1 {
					t.Fatalf("%s: %d rows, want 1", arm.name, len(rows))
				}
				if got := fmt.Sprint(rows[0]["n"]); got != c.want {
					t.Errorf("%s: %s = %s, want %s (PostgreSQL 17)", arm.name, c.sql, got, c.want)
				}
			}
		})
	}

	// ORDER BY over the real column, ascending with NULLs last (PostgreSQL's
	// default for ASC), the two NEGATIVE rows first and the NaN row
	// second-to-last: NaN is the greatest VALUE and NULL is not a value at
	// all. The key order is the float32 one at every position; the
	// literal-width rule never enters it.
	t.Run("OrderByReal", func(t *testing.T) {
		sql := fmt.Sprintf("SELECT r_key FROM %s ORDER BY r_val, r_key", rwpTable)
		want := "22,23,19,0,16,1,17,2,3,4,5,6,7,8,9,10,11,12,13,14,15,20,21,18"
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			keys := rwpKeys(t, dtpRun(t, ctx, single, coord, sql, arm.dag))
			parts := make([]string, len(keys))
			for i, k := range keys {
				parts[i] = fmt.Sprint(k)
			}
			if got := strings.Join(parts, ","); got != want {
				t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17)", arm.name, sql, got, want)
			}
		}
	})
}

// TestRealCastMeetsSetOperationLadder is the interaction gate between the two
// changes that landed together: `CAST(x AS REAL)` now produces a FLOAT32, and
// a set operation now has a float4 rung in its type ladder (#541).
//
// A set operation reconciles its arms' types, so a real arm meeting a wider or
// narrower one is exactly where a newly-narrowing cast could go wrong — either
// by widening straight back (the no-op returning) or by dragging the other arm
// down. PostgreSQL's answers, live on postgres:17 (`pg_typeof` over the union's
// output plus the values):
//
//	real UNION ALL double precision  ->  double precision
//	real UNION ALL integer           ->  real
//	real UNION ALL real              ->  real
//
// so the real arm's values WIDEN in the first (3.1 as a real prints
// 3.0999999046325684 once it is a double) and the integer arm NARROWS in the
// second. The last case puts a filter above a real-typed union, which widens
// its literal exactly as it would over a real column.
func TestRealCastMeetsSetOperationLadder(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)

	cases := []struct {
		name string
		sql  string
		want []string // the v column, ordered, rendered
	}{
		// real ∪ double → double: the real arm's two values widen and print
		// their double expansions, the double arm's prints 3.1.
		{"RealUnionDouble", fmt.Sprintf(
			`SELECT v FROM (SELECT CAST(r_val AS REAL) AS v FROM %s WHERE r_key IN (3,7)
			  UNION ALL SELECT d_val FROM %s WHERE r_key = 3) s ORDER BY v`, rwpTable, rwpTable),
			[]string{"3.0999999046325684", "3.1", "7.099999904632568"}},
		// real ∪ integer → real: the integer arm's 5 becomes a real, and the
		// real arm keeps float32's shortest round-trip rendering.
		{"RealUnionInteger", fmt.Sprintf(
			`SELECT v FROM (SELECT CAST(d_val AS REAL) AS v FROM %s WHERE r_key IN (3,7)
			  UNION ALL SELECT r_key FROM %s WHERE r_key = 5) s ORDER BY v`, rwpTable, rwpTable),
			[]string{"3.1", "5", "7.1"}},
		{"RealUnionReal", fmt.Sprintf(
			`SELECT v FROM (SELECT CAST(3.1 AS REAL) AS v FROM %s WHERE r_key = 0
			  UNION ALL SELECT CAST(7.1 AS REAL) FROM %s WHERE r_key = 0) s ORDER BY v`, rwpTable, rwpTable),
			[]string{"3.1", "7.1"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			for _, arm := range []struct {
				name string
				dag  bool
			}{{"single", false}, {"dag", true}} {
				rows := dtpRun(t, ctx, single, coord, c.sql, arm.dag)
				got := make([]string, 0, len(rows))
				for _, r := range rows {
					got = append(got, fmt.Sprint(r["v"]))
				}
				if strings.Join(got, ",") != strings.Join(c.want, ",") {
					t.Errorf("%s: %s\n  got  %v\n  want %v (PostgreSQL 17)",
						arm.name, c.sql, got, c.want)
				}
			}
		})
	}

	// A filter ABOVE a real-typed union: the literal widens against the
	// union's real output exactly as it does against a real column, so the
	// non-representable 3.1 matches nothing.
	t.Run("FilterAboveRealUnion", func(t *testing.T) {
		sql := fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM (SELECT CAST(d_val AS REAL) AS v FROM %s
			  UNION ALL SELECT CAST(d_val AS REAL) FROM %s) s WHERE s.v = 3.1`, rwpTable, rwpTable)
		for _, arm := range []struct {
			name string
			dag  bool
		}{{"single", false}, {"dag", true}} {
			rows := dtpRun(t, ctx, single, coord, sql, arm.dag)
			if len(rows) != 1 || fmt.Sprint(rows[0]["n"]) != "0" {
				t.Errorf("%s: %s = %v, want 0 (PostgreSQL 17)", arm.name, sql, rows)
			}
		}
	})
}

// rwpKeys unboxes the r_key column, keeping the row order the query asked for.
func rwpKeys(t *testing.T, rows []map[string]any) []int64 {
	t.Helper()
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		k, ok := r["r_key"].(int64)
		if !ok {
			t.Fatalf("r_key = %#v (%T), want int64", r["r_key"], r["r_key"])
		}
		out = append(out, k)
	}
	return out
}

func rwpEqual(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}
