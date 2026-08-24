package coordinator

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// The predicate-semantics gate: what a WHERE clause MEANS, asserted by VALUE
// on both engines.
//
// The two-path gate next door (TestTypeMatrixTwoPath) asks whether the stage
// DAG and the single-process engine agree with EACH OTHER — and #461/#450
// slipped past it not because the two arms share a lowering, but because its
// corpus had no negated predicate and no NULL literal to drive either arm
// into the code that was wrong. The DAG arm was never at risk here: the
// worker compiles scan-pushed filters straight to the row evaluator
// (compileFilterExprs in internal/worker/filter_compile.go calls
// exec.NewFilter(expr.FilterPredicate(compiled)), never extractFilterOps) and
// that evaluator got NOT and NULL-literal comparisons right throughout. The
// single-process arm's physical planner additionally tries to vectorize a
// filter (buildFilterOp → tryVectorizeFilter/tryPartialVectorize in
// internal/planner/physical/plan.go), and it was extractFilterOps there that
// returned a NOT's OPERAND un-negated (#461) and let a NULL literal read as
// the column type's zero (#450) — a bug in one arm, not a shared one.
//
// So this gate carries its own expectation instead of comparing the arms to
// each other. Each case names the row set SQL requires, computed in Go from
// the same fixture the engines read, and both arms are held to it. An engine
// that agrees with the other but not with SQL fails here — which is exactly
// the failure mode the corpus gap above let through.
//
// Three-valued logic is the point of most of the corpus. A WHERE admits only
// TRUE: a NULL row satisfies neither a predicate nor its negation, and
// `NOT (a AND b)` is `NOT a OR NOT b` in Kleene logic too.

// predCase is one predicate, and the rows it must return.
type predCase struct {
	name string
	// where is spliced into `SELECT id FROM typemx WHERE <where> ORDER BY id`.
	where string
	// want reports whether fixture row r qualifies. It is written from SQL's
	// rules, not from the engine's behaviour.
	want func(r tmxRow) bool
}

// tmxRow is one fixture row's columns, already unboxed. Nullable columns
// carry a pointer so "absent" and "zero" stay distinct — the distinction the
// filter kernels lost.
type tmxRow struct {
	id  int64
	i64 *int64
	i32 *int32
	str *string
	dec *float64
	g   *int32
}

func tmxRows() []tmxRow {
	raw := typematrix.Data(typematrix.Rows)
	out := make([]tmxRow, len(raw))
	for i, r := range raw {
		row := tmxRow{id: r["id"].(int64)}
		if v, ok := r["c_i64"].(int64); ok {
			row.i64 = &v
		}
		if v, ok := r["c_i32"].(int32); ok {
			row.i32 = &v
		}
		if v, ok := r["c_str"].(string); ok {
			row.str = &v
		}
		if v, ok := r["c_dec"].(float64); ok {
			row.dec = &v
		}
		if v, ok := r["g"].(int32); ok {
			row.g = &v
		}
		out[i] = row
	}
	return out
}

// c_i64 holds i*1_000_003, NULL every 31st row; c_str holds "s-%06d", NULL
// every 43rd. These are the two literals the corpus compares against.
const (
	tmxI64At131 = 131 * 1_000_003
	tmxI64At100 = 100 * 1_000_003
)

func notPredicateCases() []predCase {
	return []predCase{
		// #461's four reported shapes, on a NOT NULL column: the answer is
		// the complement of the positive predicate.
		{"NotEqNonNullable", "NOT (id = 131)", func(r tmxRow) bool { return r.id != 131 }},
		{"NotLtNonNullable", "NOT (id < 10)", func(r tmxRow) bool { return r.id >= 10 }},
		{"NotInNonNullable", "NOT (id IN (1, 2, 3))", func(r tmxRow) bool {
			return r.id != 1 && r.id != 2 && r.id != 3
		}},
		{"NotBetweenNonNullable", "NOT (id BETWEEN 10 AND 19)", func(r tmxRow) bool {
			return r.id < 10 || r.id > 19
		}},
		// The other four comparisons.
		{"NotNeNonNullable", "NOT (id <> 131)", func(r tmxRow) bool { return r.id == 131 }},
		{"NotLeNonNullable", "NOT (id <= 10)", func(r tmxRow) bool { return r.id > 10 }},
		{"NotGtNonNullable", "NOT (id > 10)", func(r tmxRow) bool { return r.id <= 10 }},
		{"NotGeNonNullable", "NOT (id >= 10)", func(r tmxRow) bool { return r.id < 10 }},
		// A literal on the left keeps its flip through the negation.
		{"NotFlippedLiteral", "NOT (10 < id)", func(r tmxRow) bool { return r.id <= 10 }},

		// The same shapes on a NULLABLE column. A NULL row is UNKNOWN under
		// the predicate and UNKNOWN under its negation, so it appears in
		// neither answer — the complement of the positive answer is NOT the
		// negation's answer here, which is what makes these the load-bearing
		// cases.
		{"NotEqNullable", fmt.Sprintf("NOT (c_i64 = %d)", tmxI64At131), func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 != tmxI64At131
		}},
		{"NotNeNullable", fmt.Sprintf("NOT (c_i64 <> %d)", tmxI64At131), func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 == tmxI64At131
		}},
		{"NotLtNullable", fmt.Sprintf("NOT (c_i64 < %d)", tmxI64At100), func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 >= tmxI64At100
		}},
		{"NotGeNullable", fmt.Sprintf("NOT (c_i64 >= %d)", tmxI64At100), func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 < tmxI64At100
		}},
		{"NotInNullable", "NOT (c_i64 IN (1000003, 2000006, 3000009))", func(r tmxRow) bool {
			if r.i64 == nil {
				return false
			}
			return *r.i64 != 1000003 && *r.i64 != 2000006 && *r.i64 != 3000009
		}},
		{"NotNotInNullable", "NOT (c_i64 NOT IN (1000003, 2000006, 3000009))", func(r tmxRow) bool {
			if r.i64 == nil {
				return false
			}
			return *r.i64 == 1000003 || *r.i64 == 2000006 || *r.i64 == 3000009
		}},
		{"NotBetweenNullable", "NOT (c_i64 BETWEEN 10000030 AND 19000057)", func(r tmxRow) bool {
			return r.i64 != nil && (*r.i64 < 10000030 || *r.i64 > 19000057)
		}},
		{"NotNotBetweenNullable", "NOT (c_i64 NOT BETWEEN 10000030 AND 19000057)", func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 >= 10000030 && *r.i64 <= 19000057
		}},
		{"NotEqNullableInt32", "NOT (c_i32 = 393)", func(r tmxRow) bool {
			return r.i32 != nil && *r.i32 != 393
		}},

		// IS NULL is the one test a NULL row answers TRUE, and its negation
		// is the only way to ask for the rest.
		{"NotIsNull", "NOT (c_i64 IS NULL)", func(r tmxRow) bool { return r.i64 != nil }},
		{"NotIsNotNull", "NOT (c_i64 IS NOT NULL)", func(r tmxRow) bool { return r.i64 == nil }},

		// Strings: equality, LIKE and NOT LIKE under a negation.
		{"NotEqString", "NOT (c_str = 's-000131')", func(r tmxRow) bool {
			return r.str != nil && *r.str != "s-000131"
		}},
		{"NotLikeString", "NOT (c_str LIKE 's-00013%')", func(r tmxRow) bool {
			return r.str != nil && !strings.HasPrefix(*r.str, "s-00013")
		}},
		{"NotNotLikeString", "NOT (c_str NOT LIKE 's-00013%')", func(r tmxRow) bool {
			return r.str != nil && strings.HasPrefix(*r.str, "s-00013")
		}},

		// De Morgan, both directions, with a nullable operand so the Kleene
		// truth table is exercised: NOT (A AND B) is TRUE when EITHER side is
		// FALSE, even if the other is UNKNOWN.
		{"NotOfAnd", "NOT (id < 100 AND c_i64 > 0)", func(r tmxRow) bool {
			aFalse := r.id >= 100
			bFalse := r.i64 != nil && *r.i64 <= 0
			return aFalse || bFalse
		}},
		{"NotOfOr", "NOT (id < 100 OR c_i64 > 0)", func(r tmxRow) bool {
			aFalse := r.id >= 100
			bFalse := r.i64 != nil && *r.i64 <= 0
			return aFalse && bFalse
		}},
		// Two negations cancel — right before the fix too, because two
		// dropped negations cancel as well. It is here so the fix cannot
		// break what accident had right.
		{"DoubleNegation", "NOT (NOT (id = 131))", func(r tmxRow) bool { return r.id == 131 }},

		// A negation beside a conjunct the lowering cannot vectorize: the
		// vectorized half and the row-at-a-time half must agree on the same
		// negation (tryPartialVectorize).
		{"NotBesideRowPredicate", "NOT (id = 131) AND ABS(id) < 200", func(r tmxRow) bool {
			return r.id != 131 && r.id < 200
		}},
		// A negation the lowering must DECLINE: ABS(id) is not a bare column,
		// so the whole predicate falls to the row evaluator, which keeps the
		// negation.
		{"NotOverFunctionCall", "NOT (ABS(id) < 10)", func(r tmxRow) bool { return r.id >= 10 }},

		// Column against column, negated.
		{"NotColCol", "NOT (id = c_i64)", func(r tmxRow) bool {
			return r.i64 != nil && r.id != *r.i64
		}},

		// A DECIMAL column takes its own kernel (kernel.compareFilterDecimal,
		// the newest one), so the negation has to reach that arm too.
		{"NotLtDecimal", "NOT (c_dec < 100)", func(r tmxRow) bool {
			return r.dec != nil && *r.dec >= 100
		}},
	}
}

// nullLiteralPredicateCases pins what a comparison against a NULL LITERAL
// means. `col <op> NULL` is UNKNOWN on every row and a WHERE admits only
// TRUE, so almost every entry here is the empty answer — and almost every one
// of them returned rows before the fix, because the nil constant reached the
// typed kernel and was read as the column type's ZERO: `c_i64 = NULL` matched
// the rows holding 0, `c_str = NULL` the rows holding "", `id <> NULL` matched
// every row whose id is not 0 (#450).
//
// The entries that are NOT empty are the ones that make this a semantics test
// rather than a "returns nothing" test: a NULL inside an IN list drops out
// rather than poisoning it, a NULL BETWEEN bound leaves the other bound
// standing under NOT BETWEEN, and an OR keeps its other arm.
func nullLiteralPredicateCases() []predCase {
	none := func(tmxRow) bool { return false }
	return []predCase{
		// Every comparison, on a nullable column.
		{"NullLitEq", "c_i64 = NULL", none},
		{"NullLitNe", "c_i64 <> NULL", none},
		{"NullLitBangEq", "c_i64 != NULL", none},
		{"NullLitLt", "c_i64 < NULL", none},
		{"NullLitLe", "c_i64 <= NULL", none},
		{"NullLitGt", "c_i64 > NULL", none},
		{"NullLitGe", "c_i64 >= NULL", none},
		{"NullLitFlipped", "NULL = c_i64", none},
		{"NullLitFlippedGt", "NULL > c_i64", none},
		// And on a NOT NULL column, where the old answer was the whole table:
		// id <> 0 is true for 4999 of the 5000 rows.
		{"NullLitEqNonNullable", "id = NULL", none},
		{"NullLitNeNonNullable", "id <> NULL", none},
		{"NullLitGeNonNullable", "id >= NULL", none},
		// One per storage class, because the coercion was per type.
		{"NullLitString", "c_str = NULL", none},
		{"NullLitStringGt", "c_str > NULL", none},
		{"NullLitBool", "c_bool = NULL", none},
		{"NullLitBoolNe", "c_bool <> NULL", none},
		{"NullLitInt32", "c_i32 = NULL", none},
		{"NullLitFloat", "c_f64 = NULL", none},
		{"NullLitDecimal", "c_dec = NULL", none},
		{"NullLitDate", "c_date = NULL", none},
		// NOT UNKNOWN is UNKNOWN.
		{"NullLitNegated", "NOT (c_i64 = NULL)", none},
		{"NullLitNegatedNe", "NOT (id <> NULL)", none},
		// Set membership. A NULL member cannot make IN true, so it drops out
		// and the rest of the list still answers; in a NOT IN it makes every
		// row FALSE or UNKNOWN.
		{"NullLitInAlone", "c_i64 IN (NULL)", none},
		{"NullLitInWithValue", "c_i64 IN (1000003, NULL)", func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 == 1000003
		}},
		{"NullLitNotInAlone", "c_i64 NOT IN (NULL)", none},
		{"NullLitNotInWithValue", "c_i64 NOT IN (1000003, NULL)", none},
		// A NULL bound. BETWEEN admits nothing; NOT BETWEEN reduces to the
		// comparison that survives, because a FALSE conjunct makes the
		// conjunction FALSE whatever the UNKNOWN one says.
		{"NullLitBetweenLow", "c_i64 BETWEEN NULL AND 19000057", none},
		{"NullLitBetweenHigh", "c_i64 BETWEEN 10000030 AND NULL", none},
		{"NullLitNotBetweenLow", "c_i64 NOT BETWEEN NULL AND 19000057", func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 > 19000057
		}},
		{"NullLitNotBetweenHigh", "c_i64 NOT BETWEEN 10000030 AND NULL", func(r tmxRow) bool {
			return r.i64 != nil && *r.i64 < 10000030
		}},
		// No pattern to match against.
		{"NullLitLike", "c_str LIKE NULL", none},
		{"NullLitNotLike", "c_str NOT LIKE NULL", none},
		// Composition: an AND with an UNKNOWN conjunct is never TRUE; an OR
		// keeps its other arm.
		{"NullLitAnd", "c_i64 = NULL AND id < 10", none},
		{"NullLitOr", "c_i64 = NULL OR id < 10", func(r tmxRow) bool { return r.id < 10 }},
		// The controls. IS NULL is the only null test and was always right —
		// it takes a separate operator that reads the null bitmap. These fail
		// if a fix for the above ever reaches them.
		{"IsNullControl", "c_i64 IS NULL", func(r tmxRow) bool { return r.i64 == nil }},
		{"IsNotNullControl", "c_i64 IS NOT NULL", func(r tmxRow) bool { return r.i64 != nil }},
		{"IsNullOnNonNullable", "id IS NULL", none},
	}
}

// decimalColColPredicateCases pin what a COLUMN-against-COLUMN comparison
// involving a DECIMAL means on both arms (#476, #477).
//
// A DECIMAL column boxes as its RENDERED TEXT, so a mixed-type pair reached
// the row evaluator as (int64, string) and compared LEXICOGRAPHICALLY — "9"
// above "10" — with `=` and `<>` right and only the ORDERING operators wrong.
// A same-type pair never got that far on the single-process arm: two DECIMALs
// share a TypeID, so the mixed-type fallback was skipped and no kernel existed
// to take its place.
//
// c_dec is `i + 0.0001*(i%9973)` at DECIMAL(18,4) and c_i64 is `i*1_000_003`,
// both NULL on their own strides, so the two orderings below are decided by
// the FRACTION the lexicographic reading could not see: over 5000 rows the
// fraction is non-zero on every row that has a value at all.
func decimalColColPredicateCases() []predCase {
	// decUnscaled is c_dec at the column's own DECIMAL(18,4) scale, as an
	// exact integer, so no expectation here depends on float64 arithmetic.
	decUnscaled := func(r tmxRow) (int64, bool) {
		if r.dec == nil {
			return 0, false
		}
		return int64(math.Round(*r.dec * 10000)), true
	}
	// ord compares an integer column against c_dec at that scale, answering
	// the ordering SQL requires, or false for "this row has a NULL and
	// qualifies for nothing".
	ord := func(get func(tmxRow) (int64, bool), want func(int) bool) func(tmxRow) bool {
		return func(r tmxRow) bool {
			u, ok := decUnscaled(r)
			if !ok {
				return false
			}
			v, ok := get(r)
			if !ok {
				return false
			}
			switch n := v * 10000; {
			case n < u:
				return want(-1)
			case n > u:
				return want(1)
			default:
				return want(0)
			}
		}
	}
	i64 := func(r tmxRow) (int64, bool) {
		if r.i64 == nil {
			return 0, false
		}
		return *r.i64, true
	}
	i32 := func(r tmxRow) (int64, bool) {
		if r.i32 == nil {
			return 0, false
		}
		return int64(*r.i32), true
	}
	rowID := func(r tmxRow) (int64, bool) { return r.id, true }
	none := func(tmxRow) bool { return false }
	present := func(r tmxRow) bool { return r.dec != nil }
	lt := func(c int) bool { return c < 0 }
	le := func(c int) bool { return c <= 0 }
	gt := func(c int) bool { return c > 0 }
	ge := func(c int) bool { return c >= 0 }
	eq := func(c int) bool { return c == 0 }
	ne := func(c int) bool { return c != 0 }
	return []predCase{
		// INT64 against DECIMAL, every operator. c_i64 is i*1_000_003 and
		// c_dec is about i, so the integer is far larger on every row but the
		// first — and the lexicographic reading got that wrong for every row
		// whose digit counts differ, which is most of them.
		{"DecimalColColI64Gt", "c_i64 > c_dec", ord(i64, gt)},
		{"DecimalColColI64Ge", "c_i64 >= c_dec", ord(i64, ge)},
		{"DecimalColColI64Lt", "c_i64 < c_dec", ord(i64, lt)},
		{"DecimalColColI64Le", "c_i64 <= c_dec", ord(i64, le)},
		{"DecimalColColI64Eq", "c_i64 = c_dec", ord(i64, eq)},
		{"DecimalColColI64Ne", "c_i64 <> c_dec", ord(i64, ne)},
		{"DecimalColColFlipped", "c_dec < c_i64", ord(i64, gt)},
		// id is NOT NULL and equals the row index, so it sits just BELOW
		// c_dec's fraction on every row that has one: the operators split
		// where a lexicographic reading cannot.
		{"DecimalColColIdLt", "id < c_dec", ord(rowID, lt)},
		{"DecimalColColIdGe", "id >= c_dec", ord(rowID, ge)},
		{"DecimalColColIdEq", "id = c_dec", ord(rowID, eq)},
		// INT32 against DECIMAL: the widening is per storage class.
		{"DecimalColColI32Lt", "c_i32 < c_dec", ord(i32, lt)},
		{"DecimalColColI32Ge", "c_i32 >= c_dec", ord(i32, ge)},
		// The negated forms, where a NULL row appears in neither answer.
		{"DecimalColColNotGe", "NOT (c_i64 >= c_dec)", ord(i64, lt)},
		{"DecimalColColNotLt", "NOT (c_i64 < c_dec)", ord(i64, ge)},
		// DECIMAL against DECIMAL (#477): the same TypeID on both sides, which
		// skipped the mixed-type fallback and found no kernel to use instead,
		// so these did not answer wrongly — they FAILED the query on the
		// single-process arm while the DAG arm, which always compiles to the
		// row evaluator, answered from two rendered texts.
		{"DecimalColColSameEq", "c_dec = c_dec", present},
		{"DecimalColColSameGe", "c_dec >= c_dec", present},
		{"DecimalColColSameLe", "c_dec <= c_dec", present},
		{"DecimalColColSameNe", "c_dec <> c_dec", none},
		{"DecimalColColSameLt", "c_dec < c_dec", none},
		{"DecimalColColSameGt", "c_dec > c_dec", none},
		{"DecimalColColSameNotLt", "NOT (c_dec < c_dec)", present},
	}
}

func TestTypeMatrixPredicateSemanticsTwoPath(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: the predicate-semantics gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)

	coord := tmdCluster(t, ctx)
	single := tmdStandalone(t, ctx)
	rows := tmxRows()

	cases := append(notPredicateCases(), nullLiteralPredicateCases()...)
	cases = append(cases, decimalColColPredicateCases()...)
	t.Logf("predicate-semantics gate: %d predicates × 2 arms (A single-process, B stage DAG), "+
		"each held to the row set SQL requires", len(cases))

	check := func(t *testing.T, sql string, want func(tmxRow) bool) {
		t.Helper()
		var wantIDs []int64
		for _, r := range rows {
			if want(r) {
				wantIDs = append(wantIDs, r.id)
			}
		}
		for _, arm := range []struct {
			name string
			run  func() (*oracle.Result, error)
		}{
			{"single", func() (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
			{"dag", func() (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
		} {
			res, err := arm.run()
			if err != nil {
				t.Errorf("%s arm failed: %v\n  SQL: %s", arm.name, err, sql)
				continue
			}
			if diff := diffIDs(wantIDs, predIDs(t, res)); diff != "" {
				t.Errorf("%s arm answered the wrong rows\n  SQL: %s\n  %s", arm.name, sql, diff)
			}
		}
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			check(t, "SELECT id FROM "+typematrix.Table+" WHERE "+c.where+" ORDER BY id", c.want)
		})
	}

	// A negation over a JOIN: the predicate travels through join planning and,
	// on the DAG, through a separate stage's re-parse of the filter text. g is
	// the join key and is nullable, so a NULL-key row is dropped by the join
	// before the negation is ever asked.
	t.Run("NotOverJoin", func(t *testing.T) {
		check(t, "SELECT t.id AS id FROM "+typematrix.Table+" t JOIN "+typematrix.Dim+
			" d ON t.g = d.k WHERE NOT (t.id < 100) ORDER BY id",
			func(r tmxRow) bool { return r.g != nil && r.id >= 100 })
	})
}

// predIDs pulls the id column out of a result, in the order it arrived.
func predIDs(t *testing.T, res *oracle.Result) []int64 {
	t.Helper()
	out := make([]int64, 0, len(res.Rows))
	for _, r := range res.Rows {
		switch v := r["id"].(type) {
		case int64:
			out = append(out, v)
		case int32:
			out = append(out, int64(v))
		case float64:
			out = append(out, int64(v))
		default:
			t.Fatalf("id came back as %T (%v), which no arm should produce", r["id"], r["id"])
		}
	}
	return out
}

// diffIDs reports the first difference between two id sequences, plus the
// counts, which is usually enough to name the defect on sight: a dropped
// negation returns the complement.
func diffIDs(want, got []int64) string {
	if len(want) == len(got) {
		same := true
		for i := range want {
			if want[i] != got[i] {
				same = false
				break
			}
		}
		if same {
			return ""
		}
	}
	n := len(want)
	if len(got) < n {
		n = len(got)
	}
	for i := 0; i < n; i++ {
		if want[i] != got[i] {
			return fmt.Sprintf("want %d rows, got %d; first difference at index %d: want id=%d, got id=%d",
				len(want), len(got), i, want[i], got[i])
		}
	}
	return fmt.Sprintf("want %d rows, got %d; the shorter is a prefix of the longer (first %d ids agree)",
		len(want), len(got), n)
}
