package expr

import (
	"math"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// realColBatch is one FLOAT32 column r (plus a second one r2, so a
// column-member IN list has something non-constant to hold) with the given
// values; a nil value is a SQL NULL.
func realColBatch(values ...any) *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "r2", Type: parquet.TypeFloat32, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, len(values))
	for i, v := range values {
		if v == nil {
			b.Columns[0].Nulls.SetNull(i)
			b.Columns[1].Nulls.SetNull(i)
			continue
		}
		b.Columns[0].SetValue(i, v)
		b.Columns[1].SetValue(i, v)
	}
	b.Len = len(values)
	return b
}

// realWidthRows is the fixture every case below reads: rows 0..3 hold
// real(i)+0.1 (not representable in float32), row 4 holds 1.5 (exact), row 5
// is NULL, row 6 holds 2^24 — the first integer real cannot follow.
func realWidthRows() *batch.RecordBatch {
	return realColBatch(
		float32(0)+0.1, float32(1)+0.1, float32(2)+0.1, float32(3)+0.1,
		float32(1.5), nil, float32(16777216),
	)
}

// TestRealMultiElementInNarrowsOnTheRowPath is the regression for #633: the
// row-at-a-time IN — the evaluator the DISTRIBUTED stage DAG compiles every
// scan-pushed filter to (worker.compileFilterExprs → expr.FilterPredicate) —
// compared a FLOAT32 column's BOXED float64 against the float64 literals, so a
// multi-element `real IN (...)` matched nothing where PostgreSQL's real[] cast
// matches the row. #549 fixed only the vectorized kernel.
//
// Every want is PostgreSQL 17's over the same values:
//
//	CREATE TABLE rw (k int, r real);
//	INSERT INTO rw VALUES (0,0.1),(1,1.1),(2,2.1),(3,3.1),(4,1.5),(5,NULL),(6,16777216);
func TestRealMultiElementInNarrowsOnTheRowPath(t *testing.T) {
	b := realWidthRows()

	cases := []struct {
		name string
		sql  string
		// want[row] is the two-valued collapse a WHERE applies: TRUE only.
		want []bool
	}{
		// NARROW (arity > 1): 0.1 and 3.1 become the reals rows 0 and 3 hold.
		{"multi non-representable", "r IN (0.1, 3.1)",
			[]bool{true, false, false, true, false, false, false}},
		// The syntactic arity counts the NULL member, so this still narrows —
		// and a MISS is UNKNOWN, not FALSE, which a WHERE drops either way.
		{"multi with null", "r IN (3.1, NULL)",
			[]bool{false, false, false, true, false, false, false}},
		{"not in multi", "r NOT IN (0.1, 3.1)",
			[]bool{false, true, true, false, true, false, true}},
		// An INTEGER literal narrows in a multi-element list: 16777217 rounds
		// onto the real row 6 holds. The scalar `=` on the same literal does
		// NOT (it widens) — see TestRealScalarComparisonStaysWidened.
		{"multi integer past mantissa", "r IN (16777217, 99)",
			[]bool{false, false, false, false, false, false, true}},
		// WIDEN (arity 1): PostgreSQL folds a single-element list to
		// `= 'x'::double precision`, so a non-representable literal matches
		// nothing and an exact one still matches.
		{"single non-representable", "r IN (3.1)",
			[]bool{false, false, false, false, false, false, false}},
		{"single representable", "r IN (1.5)",
			[]bool{false, false, false, false, true, false, false}},
		// A NON-CONSTANT member takes the array away: PostgreSQL plans
		// `r IN (3.1, r2)` as `(r = '3.1'::double precision) OR (r = r2)`, the
		// WIDENED scalar rule twice — so row 3 does NOT match on the literal,
		// and every non-NULL row matches on the column.
		{"member is a column", "r IN (3.1, r2)",
			[]bool{true, true, true, true, true, false, true}},
		// A member explicitly typed DOUBLE PRECISION widens the whole list in
		// PostgreSQL — the array's element type is resolved over the members
		// and float8 is the preferred type of the numeric category, so the
		// list becomes double precision[] and the column is the side that
		// moves:
		//
		//	r IN (CAST('NaN' AS double precision), 3.1)
		//	  -> Filter: (r_val = ANY ('{NaN,3.1}'::double precision[]))
		//	r IN ('NaN'::real, 3.1)
		//	  -> Filter: (r_val = ANY ('{NaN,3.1}'::real[]))
		//
		// The binding declines a member that is not a literal box, which is
		// PostgreSQL's answer for the first spelling: row 3 does NOT match.
		{"member is an explicit double", "r IN (CAST('Infinity' AS DOUBLE PRECISION), 3.1)",
			[]bool{false, false, false, false, false, false, false}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pred := FilterPredicate(compileExprSQL(t, c.sql))
			// Three passes: the binding resolves the column's declared type
			// on the first batch and caches it, so a defect in that caching
			// shows up only on a later pass.
			for pass := 0; pass < 3; pass++ {
				for row := 0; row < b.Len; row++ {
					if got := pred(b, row); got != c.want[row] {
						t.Fatalf("pass %d row %d: %s = %v, want %v (PostgreSQL 17)",
							pass, row, c.sql, got, c.want[row])
					}
				}
			}
		})
	}
}

// TestRealScalarComparisonStaysWidened is #631 seen from the row path, which
// already widened (a FLOAT32 column boxes as float64) and had to KEEP doing so
// while the IN list next door started narrowing. The two rules living in one
// node is the whole subtlety of this type.
func TestRealScalarComparisonStaysWidened(t *testing.T) {
	b := realWidthRows()

	cases := []struct {
		name string
		sql  string
		want []bool
	}{
		{"eq non-representable", "r = 3.1",
			[]bool{false, false, false, false, false, false, false}},
		{"lt non-representable", "r < 3.1",
			[]bool{true, true, true, true, true, false, false}},
		{"ge non-representable", "r >= 3.1",
			[]bool{false, false, false, false, false, false, true}},
		{"eq integer past mantissa", "r = 16777217",
			[]bool{false, false, false, false, false, false, false}},
		{"eq representable", "r = 1.5",
			[]bool{false, false, false, false, true, false, false}},
		{"between", "r BETWEEN 3.1 AND 4.1",
			[]bool{false, false, false, false, false, false, false}},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pred := FilterPredicate(compileExprSQL(t, c.sql))
			for row := 0; row < b.Len; row++ {
				if got := pred(b, row); got != c.want[row] {
					t.Errorf("row %d: %s = %v, want %v (PostgreSQL 17)", row, c.sql, got, c.want[row])
				}
			}
		})
	}
}

// TestRealMultiElementInOverflowRaises22003 mirrors the kernel's refusal
// (exec.floatConstError, #549) on the row path. Narrowing a finite literal
// past real's range yields +Inf, which would MATCH a genuine infinite row;
// PostgreSQL raises numeric_value_out_of_range for the whole predicate when it
// casts the array to real[]:
//
//	SELECT * FROM rw WHERE r IN (1e40, 3.1);
//	ERROR:  "10000000000000000000000000000000000000000" is out of range for type real
//
// A SINGLE-element `r IN (1e40)` widens instead and answers 0 rows with no
// error at all, which is the other half of the arity rule.
func TestRealMultiElementInOverflowRaises22003(t *testing.T) {
	b := realWidthRows()

	err := catchFatalEval(t, func() {
		pred := FilterPredicate(compileExprSQL(t, "r IN (1e40, 3.1)"))
		for row := 0; row < b.Len; row++ {
			pred(b, row)
		}
	})
	if err == nil {
		t.Fatal("multi-element `r IN (1e40, 3.1)` did not raise; PostgreSQL raises 22003")
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Errorf("SQLSTATE %q, want 22003 (%v)", got, err)
	}

	// Single element: no error, no rows.
	if err := catchFatalEval(t, func() {
		pred := FilterPredicate(compileExprSQL(t, "r IN (1e40)"))
		for row := 0; row < b.Len; row++ {
			if pred(b, row) {
				t.Errorf("row %d matched `r IN (1e40)`; PostgreSQL answers no rows", row)
			}
		}
	}); err != nil {
		t.Errorf("single-element `r IN (1e40)` raised %v; PostgreSQL raises nothing", err)
	}

	// A literal that IS infinite is a legal real value, not an overflow.
	if err := catchFatalEval(t, func() {
		pred := FilterPredicate(compileExprSQL(t, "r IN (CAST('Infinity' AS DOUBLE PRECISION), 3.1)"))
		for row := 0; row < b.Len; row++ {
			pred(b, row)
		}
	}); err != nil {
		t.Errorf("an ±Inf member raised %v; it is a representable real", err)
	}
}

// TestRealInSetFoldsZerosAndCarriesNaNSeparately keeps the narrowed set's two
// float-order obligations honest (ADR-0012 item 8): -0.0 and +0.0 are ONE
// value, and NaN equals itself.
//
// The zero half is driven from SQL. The NaN half builds the node directly,
// because no SQL spelling produces a NaN LITERAL today — `CAST('NaN' AS
// DOUBLE PRECISION)` is a Cast node, which the binding declines (and which
// PostgreSQL widens the whole list for anyway; see the corpus above). The
// binding still has to carry the flag: a Go map keyed by float32 can never
// return a NaN by lookup, so inserting one and probing with one both miss —
// the same reason kernel.float32InSet carries it, and the contract this pins.
func TestRealInSetFoldsZerosAndCarriesNaNSeparately(t *testing.T) {
	negZero := float32(0)
	negZero = -negZero
	b := realColBatch(float32(math.NaN()), float32(0), negZero, float32(3)+0.1)

	zero := FilterPredicate(compileExprSQL(t, "r IN (0.0, 99.0)"))
	for _, row := range []int{1, 2} {
		if !zero(b, row) {
			t.Errorf("row %d: `r IN (0.0, 99.0)` missed a zero; -0.0 and +0.0 are one value", row)
		}
	}
	if zero(b, 0) {
		t.Error("row 0 (NaN) matched `r IN (0.0, 99.0)`")
	}

	nanIn := NewIn(&ColRef{Name: "r"},
		[]Expr{&Lit{Val: math.NaN()}, &Lit{Val: 3.1, Text: "3.1"}}, false)
	if nanIn.f32 == nil {
		t.Fatal("a two-element numeric-literal list over a real column did not bind")
	}
	pred := FilterPredicate(nanIn)
	want := []bool{true, false, false, true}
	for row := 0; row < b.Len; row++ {
		if got := pred(b, row); got != want[row] {
			t.Errorf("row %d: NaN-member IN = %v, want %v", row, got, want[row])
		}
	}
}

// TestRealInBindingDeclinesNonRealColumns proves the binding is keyed on the
// column's DECLARED type and not on the shape of the list: the same predicate
// over a FLOAT64 column must keep comparing at double width, where narrowing
// would be a new defect rather than a fix.
func TestRealInBindingDeclinesNonRealColumns(t *testing.T) {
	schema := []parquet.Column{{Name: "r", Type: parquet.TypeFloat64, Nullable: true}}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, 0.1)
	b.Columns[0].SetValue(1, float64(float32(0.1)))
	b.Len = 2

	// PostgreSQL: `double precision IN (0.1, 3.1)` compares at double width,
	// so the exact 0.1 row matches and the float32-rounded one does not.
	pred := FilterPredicate(compileExprSQL(t, "r IN (0.1, 3.1)"))
	if !pred(b, 0) {
		t.Error("double column: the exact 0.1 row did not match")
	}
	if pred(b, 1) {
		t.Error("double column: the float32-rounded row matched; the list must not narrow here")
	}
}

// catchFatalEval runs fn and returns the query error it raised through the
// expression layer's panic channel (fatalEval), or nil when it raised none.
func catchFatalEval(t *testing.T, fn func()) (err error) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		fe, ok := r.(fatalEval)
		if !ok {
			panic(r)
		}
		err = fe.FatalEvalError()
	}()
	fn()
	return nil
}

// realTypedRows is the fixture the OPERAND-TYPE cases read: the same numbers
// as a `real` and as a `double precision`, plus an integer key, so the rule
// "the operand's own type decides" can be told apart from "walk down to a
// column".
func realTypedRows() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "d", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "k", Type: parquet.TypeInt64},
	}
	vals := []any{float32(0) + 0.1, float32(1) + 0.1, float32(2) + 0.1, float32(3) + 0.1,
		float32(1.5), nil, float32(16777216)}
	b := batch.NewRecordBatch(schema, len(vals))
	for i, v := range vals {
		b.Columns[2].SetValue(i, int64(i))
		if v == nil {
			b.Columns[0].Nulls.SetNull(i)
			b.Columns[1].Nulls.SetNull(i)
			continue
		}
		b.Columns[0].SetValue(i, v)
		b.Columns[1].SetValue(i, float64(v.(float32)))
	}
	b.Len = len(vals)
	return b
}

// TestRealTypedOperandFollowsPostgresOperandType is physical.realTypedNode's
// runtime twin held to the same answers — the two decide the same thing on the
// two paths (the planner refuses a list this narrows), so a disagreement would
// mean one of them acting on a predicate the other calls something else.
//
// PostgreSQL resolves the array's element type over the members AND the probed
// expression, so a real-typed operand pulls the list to real[] even when it is
// not a column (EXPLAIN VERBOSE, postgres:17):
//
//	-r IN (-0.1, -3.1)       -> ((- r) = ANY ('{-0.1,-3.1}'::real[]))
//	(r + 0) IN (0.1, 3.1)    -> ((r + '0'::float8) = ANY ('{0.1,3.1}'::float8[]))
//
// The second is the control: an integer literal added to a real gives DOUBLE
// PRECISION there, so arithmetic does NOT preserve real and this cannot simply
// follow the operand down to a column.
func TestRealTypedOperandFollowsPostgresOperandType(t *testing.T) {
	b := realTypedRows()

	cases := []struct {
		sql           string
		real, settled bool
	}{
		{"r", true, true},
		{"d", false, true},
		{"k", false, true},
		{"CAST(d AS REAL)", true, true},
		{"CAST(d AS float4)", true, true},
		{"CAST(r AS DOUBLE PRECISION)", false, true},
		// PostgreSQL's bare FLOAT is double precision.
		{"CAST(r AS FLOAT)", false, true},
		{"CAST(r AS INTEGER)", false, true},
		{"-r", true, true},
		{"+r", true, true},
		{"-(-r)", true, true},
		{"-CAST(d AS REAL)", true, true},
		{"-d", false, true},
		{"r + 0", false, true},
		{"r * 1", false, true},
		{"(r)", true, true},
	}
	for _, c := range cases {
		gotReal, gotSettled := realTypedOperand(compileExprSQL(t, c.sql), b)
		if gotReal != c.real || gotSettled != c.settled {
			t.Errorf("realTypedOperand(%s) = (%v, %v), want (%v, %v) (PostgreSQL 17)",
				c.sql, gotReal, gotSettled, c.real, c.settled)
		}
	}

	// A name this batch cannot resolve is UNSETTLED: the next batch may carry
	// it, so the answer must not be cached as "not real". (A WHERE over a
	// column no batch resolves is #653, a different defect entirely.)
	if real, settled := realTypedOperand(compileExprSQL(t, "nosuchcol"), b); real || settled {
		t.Errorf("realTypedOperand(nosuchcol) = (%v, %v), want (false, false) — an unresolved name settles nothing",
			real, settled)
	}
}

// TestRealInNarrowsForARealTypedOperand is the row path's half of the same
// rule — the evaluator the stage DAG compiles every scan-pushed filter to.
// Wants are postgres:17's over the identical rows:
//
//	CREATE TABLE rw2 (k int, r real, d double precision);
//	INSERT INTO rw2 VALUES (0,0.1,0.1),(1,1.1,1.1),(2,2.1,2.1),(3,3.1,3.1),
//	                       (4,1.5,1.5),(5,NULL,NULL),(6,16777216,16777216);
func TestRealInNarrowsForARealTypedOperand(t *testing.T) {
	b := realTypedRows()

	cases := []struct {
		sql  string
		want []bool
	}{
		// NARROW: the operand is real-typed, so the array is real[] and the
		// members become the reals rows 0 and 3 hold.
		{"-r IN (-0.1, -3.1)", []bool{true, false, false, true, false, false, false}},
		{"+r IN (0.1, 3.1)", []bool{true, false, false, true, false, false, false}},
		{"CAST(d AS REAL) IN (0.1, 3.1)", []bool{true, false, false, true, false, false, false}},
		// WIDEN: `r + 0` is double precision, so the members stay doubles and
		// 0.1 is not the double the real row widens to.
		{"(r + 0) IN (0.1, 3.1)", []bool{false, false, false, false, false, false, false}},
		// The scalar halves widen whatever the operand is.
		{"-r = -3.1", []bool{false, false, false, false, false, false, false}},
		{"CAST(d AS REAL) = 3.1", []bool{false, false, false, false, false, false, false}},
	}
	for _, c := range cases {
		pred := FilterPredicate(compileExprSQL(t, c.sql))
		for row := 0; row < b.Len; row++ {
			if got := pred(b, row); got != c.want[row] {
				t.Errorf("row %d: %s = %v, want %v (PostgreSQL 17)", row, c.sql, got, c.want[row])
			}
		}
	}
}

// TestRealBoxNarrowsEitherBox pins what the narrowed probe accepts. Two boxes
// reach it and both are exact: a FLOAT32 column boxes as float64(float32)
// (ColRef.Eval), which round-trips bit for bit, and a CAST to REAL boxes a
// float32 outright. Anything else is a box the operand's type did not predict
// and must decline rather than be coerced.
func TestRealBoxNarrowsEitherBox(t *testing.T) {
	if got, ok := realBox(float32(3.1)); !ok || got != float32(3.1) {
		t.Errorf("realBox(float32 3.1) = (%v, %v), want (3.1, true)", got, ok)
	}
	if got, ok := realBox(float64(float32(3.1))); !ok || got != float32(3.1) {
		t.Errorf("realBox(widened float32) = (%v, %v), want (3.1, true)", got, ok)
	}
	for _, v := range []any{int64(3), int32(3), "3.1", nil, true} {
		if got, ok := realBox(v); ok {
			t.Errorf("realBox(%#v) = (%v, true), want declined", v, got)
		}
	}
}
