package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestSetOpWidenLadder pins the common type of a set operation's arms against
// what live postgres:17-alpine resolves for the same pair. The two DECIMAL
// rungs are the ones #533's fix added; the rest is the pre-existing ladder,
// asserted so widening a rung cannot quietly reorder it.
func TestSetOpWidenLadder(t *testing.T) {
	d, i32, i64 := parquet.TypeDecimal, parquet.TypeInt32, parquet.TypeInt64
	f32, f64 := parquet.TypeFloat32, parquet.TypeFloat64
	for _, tc := range []struct {
		a, b parquet.TypeID
		want parquet.TypeID
		ok   bool
	}{
		{i32, i32, i32, true},
		{i32, i64, i64, true},
		{i32, f64, f64, true},
		{f32, f64, f64, true},
		{f32, f32, f32, true},
		{d, d, d, true},
		// `numeric UNION ALL bigint` is numeric in PostgreSQL: an integer
		// converts to numeric implicitly, and not the other way.
		{d, i32, d, true},
		{d, i64, d, true},
		{i64, d, d, true},
		// `numeric UNION ALL double precision` is double precision: float8
		// is the PREFERRED type of PostgreSQL's numeric category.
		{d, f64, f64, true},
		{f64, d, f64, true},
		// float4 is PREFERRED too, so it beats every EXACT type it meets and
		// only float8 beats it. Verified live on postgres:17-alpine with
		// pg_typeof over the union itself, both arm orders:
		//
		//	real ∪ integer          → real
		//	real ∪ bigint           → real
		//	real ∪ numeric(9,2)     → real
		//	real ∪ double precision → double precision
		//
		// This rung is a VALUE question, not only an OID one: a real column
		// holding 0.1 renders 0.1, and the same value under a double
		// precision result renders 0.10000000149011612 — the float32 spelled
		// to float64 precision, which is a number neither engine holds.
		// `CREATE TABLE t (x FLOAT)` declares a FLOAT32 column here, so a
		// plain DDL table reaches it.
		{d, f32, f32, true},
		{f32, d, f32, true},
		{f32, i32, f32, true},
		{i32, f32, f32, true},
		{f32, i64, f32, true},
		{i64, f32, f32, true},
		{f64, f32, f64, true},
		// Nothing outside the numeric family widens: making the files line
		// up by rendering a number as text would answer a different query.
		{d, parquet.TypeString, 0, false},
		{d, parquet.TypeTimestamp, 0, false},
		{parquet.TypeString, parquet.TypeString, parquet.TypeString, true},
	} {
		got, ok := setOpWiden(tc.a, tc.b)
		if ok != tc.ok || (ok && got != tc.want) {
			t.Errorf("setOpWiden(%s, %s) = (%s, %v), want (%s, %v)", tc.a, tc.b, got, ok, tc.want, tc.ok)
		}
	}
}

// TestSetOpDecimalTarget pins the (p,s) every arm is coerced to.
//
// The scale is the maximum, which is the only choice that moves no value.
// The precision is rebuilt from the widest INTEGER part rather than taken as
// max(precision), because max(precision) is not a bound on the widened
// values — the (18,2) with (9,4) case below is where the two rules part, and
// where max(precision) would declare a type too small for its own values.
func TestSetOpDecimalTarget(t *testing.T) {
	dec := func(p, s int) setOpColType {
		return setOpColType{typ: parquet.TypeDecimal, known: true,
			dec: logical.DecimalMeta{Precision: p, Scale: s}, decKnown: true}
	}
	intArm := func(t parquet.TypeID) setOpColType {
		return setOpColType{typ: t, known: true}
	}
	for _, tc := range []struct {
		name   string
		arms   []setOpColType
		wantP  int
		wantS  int
		wantOk bool
	}{
		{"same_type", []setOpColType{dec(9, 2), dec(9, 2)}, 9, 2, true},
		// The issue's own pair: 7 integer digits and 14, so 14 + scale 4.
		{"the_issue", []setOpColType{dec(9, 2), dec(18, 4)}, 18, 4, true},
		{"reversed", []setOpColType{dec(18, 4), dec(9, 2)}, 18, 4, true},
		{"three_arms", []setOpColType{dec(9, 2), dec(18, 4), dec(38, 10)}, 38, 10, true},
		// Where max(precision) is WRONG: 16 integer digits at scale 4 needs
		// 20, and max(precision) would say 18.
		{"integer_part_beats_max_precision", []setOpColType{dec(18, 2), dec(9, 4)}, 20, 4, true},
		// Capped at the Int128 carrier's width; a value that does not fit is
		// then an error at coercion, never a wrapped number.
		{"capped_at_38", []setOpColType{dec(38, 2), dec(38, 10)}, 38, 10, true},
		// An integer arm contributes its whole range's digits.
		{"with_int32", []setOpColType{dec(9, 2), intArm(parquet.TypeInt32)}, 12, 2, true},
		{"with_int64", []setOpColType{dec(9, 2), intArm(parquet.TypeInt64)}, 21, 2, true},
		{"only_ints", []setOpColType{intArm(parquet.TypeInt32), intArm(parquet.TypeInt64)}, 19, 0, true},
		// An arm whose (p,s) nothing resolved: no target, which
		// reconcileSetOpArmTypes turns into a plan-time refusal (#551).
		{"unresolved_arm", []setOpColType{dec(9, 2),
			{typ: parquet.TypeDecimal, known: true}}, 0, 0, false},
		{"non_numeric_arm", []setOpColType{dec(9, 2),
			{typ: parquet.TypeString, known: true}}, 0, 0, false},
		{"no_arms", nil, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := setOpDecimalTarget(tc.arms)
			if ok != tc.wantOk {
				t.Fatalf("setOpDecimalTarget ok = %v, want %v", ok, tc.wantOk)
			}
			if ok && (got.Precision != tc.wantP || got.Scale != tc.wantS) {
				t.Errorf("setOpDecimalTarget = DECIMAL(%d,%d), want DECIMAL(%d,%d)",
					got.Precision, got.Scale, tc.wantP, tc.wantS)
			}
		})
	}
}

// TestSetOpDecimalTargetNeverNarrowsAnArm is the property the coercion
// operator relies on to refuse a downward move: whatever the arms are, the
// target's scale is at least every arm's own.
func TestSetOpDecimalTargetNeverNarrowsAnArm(t *testing.T) {
	for _, arms := range [][]setOpColType{
		{{typ: parquet.TypeDecimal, known: true, decKnown: true, dec: logical.DecimalMeta{Precision: 38, Scale: 10}},
			{typ: parquet.TypeDecimal, known: true, decKnown: true, dec: logical.DecimalMeta{Precision: 9, Scale: 0}}},
		{{typ: parquet.TypeDecimal, known: true, decKnown: true, dec: logical.DecimalMeta{Precision: 5, Scale: 5}},
			{typ: parquet.TypeInt64, known: true}},
		{{typ: parquet.TypeInt32, known: true},
			{typ: parquet.TypeDecimal, known: true, decKnown: true, dec: logical.DecimalMeta{Precision: 38, Scale: 38}}},
	} {
		want, ok := setOpDecimalTarget(arms)
		if !ok {
			t.Fatalf("setOpDecimalTarget declined for %v", arms)
		}
		for _, a := range arms {
			s := 0
			if a.typ == parquet.TypeDecimal {
				s = a.dec.Scale
			}
			if want.Scale < s {
				t.Errorf("target scale %d is below arm scale %d — a set operation must never scale an arm DOWN",
					want.Scale, s)
			}
		}
	}
}

// TestReconcileEmitsNoCoercionWhenTheArmsAlreadyAgree.
//
// Every coercion costs a fresh vector and a pass over the rows, and — more to
// the point — an OpDecimalCoerce in a fragment is a declaration that the arm's
// values MOVE. An arm already carrying the output type must carry neither. The
// same-(p,s) union is the overwhelmingly common case, and a reconciliation
// that emitted a no-op coercion for it would be paying for #533 on every union
// in the corpus.
func TestReconcileEmitsNoCoercionWhenTheArmsAlreadyAgree(t *testing.T) {
	dec := func(p, s int) setOpColType {
		return setOpColType{typ: parquet.TypeDecimal, known: true,
			dec: logical.DecimalMeta{Precision: p, Scale: s}, decKnown: true}
	}
	arm := func(cts ...setOpColType) setOpArmPlan {
		specs := make([]ProjectExprSpec, len(cts))
		for i := range cts {
			specs[i] = ProjectExprSpec{Expr: "c", Name: "v"}
		}
		return setOpArmPlan{specs: specs, types: cts}
	}

	t.Run("identical", func(t *testing.T) {
		plans := []setOpArmPlan{arm(dec(9, 2)), arm(dec(9, 2))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.coerce) != 0 {
				t.Errorf("arm %d got %d coercions for a column it already carries: %+v", i, len(p.coerce), p.coerce)
			}
		}
	})

	t.Run("three_identical_arms", func(t *testing.T) {
		plans := []setOpArmPlan{arm(dec(18, 4)), arm(dec(18, 4)), arm(dec(18, 4))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.coerce) != 0 {
				t.Errorf("arm %d got %d coercions: %+v", i, len(p.coerce), p.coerce)
			}
		}
	})

	t.Run("non_decimal_arms_are_untouched", func(t *testing.T) {
		i64 := setOpColType{typ: parquet.TypeInt64, known: true}
		plans := []setOpArmPlan{arm(i64), arm(i64)}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.coerce) != 0 {
				t.Errorf("arm %d got %d coercions for an INT64 column: %+v", i, len(p.coerce), p.coerce)
			}
		}
	})

	// The control, and the precise rule: ONLY the arm that differs is moved.
	// (9,2) with (18,4) resolves to (18,4), so the second arm already carries
	// the output type and must be left alone even though the operation as a
	// whole needed reconciling — the coercion is per ARM, not per operation.
	t.Run("only_the_differing_arm_is_moved", func(t *testing.T) {
		plans := []setOpArmPlan{arm(dec(9, 2)), arm(dec(18, 4))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(plans[0].coerce) != 1 {
			t.Fatalf("the narrow arm got %d coercions, want 1: %+v", len(plans[0].coerce), plans[0].coerce)
		}
		if plans[0].coerce[0] != (DecimalCoercion{Name: "v", Precision: 18, Scale: 4}) {
			t.Errorf("the narrow arm's coercion = %+v, want DECIMAL(18,4) on v", plans[0].coerce[0])
		}
		if len(plans[1].coerce) != 0 {
			t.Errorf("the arm already carrying the output type got %d coercions: %+v",
				len(plans[1].coerce), plans[1].coerce)
		}
	})

	// Neither arm carrying the output type means BOTH move: (18,2) with
	// (9,4) resolves to (20,4), which is neither.
	t.Run("both_arms_move_when_neither_is_the_target", func(t *testing.T) {
		plans := []setOpArmPlan{arm(dec(18, 2)), arm(dec(9, 4))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION"); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.coerce) != 1 {
				t.Fatalf("arm %d got %d coercions, want 1: %+v", i, len(p.coerce), p.coerce)
			}
			if p.coerce[0] != (DecimalCoercion{Name: "v", Precision: 20, Scale: 4}) {
				t.Errorf("arm %d coercion = %+v, want DECIMAL(20,4) on v", i, p.coerce[0])
			}
		}
	})

	// An arm whose (p,s) nothing resolved is REFUSED, naming the column.
	//
	// Leaving every arm alone was the answer until #551: a scale guessed here
	// would move values by a power of ten (#533's own failure mode), but so
	// does leaving them — each arm writes its own .wshf at its own scale and
	// the reader of both takes the first header's. ADR-0012 item 12 records
	// that as "the answer is WRONG — not refused"; this is the refusal it
	// calls the honest interim.
	t.Run("an_unresolved_arm_is_refused", func(t *testing.T) {
		plans := []setOpArmPlan{arm(dec(9, 2)), arm(setOpColType{typ: parquet.TypeDecimal, known: true})}
		err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION")
		if err == nil {
			t.Fatalf("an unresolvable DECIMAL target was accepted; the arms then keep their own scales, "+
				"which is a silently wrong answer (#551). coercions: %+v / %+v",
				plans[0].coerce, plans[1].coerce)
		}
		if !strings.Contains(err.Error(), `"v"`) {
			t.Errorf("the refusal must name the column it is about, got: %v", err)
		}
		if !strings.Contains(err.Error(), "arm 2") {
			t.Errorf("the refusal must localize the arm whose (p,s) is unresolved, got: %v", err)
		}
	})

	// The control: BOTH arms unresolved is refused too. Two arms that cannot
	// state a scale are not evidence that they share one — the values arrive
	// at whatever scale their own files declare, and the reader takes the
	// first.
	t.Run("every_arm_unresolved_is_refused_too", func(t *testing.T) {
		unres := setOpColType{typ: parquet.TypeDecimal, known: true}
		plans := []setOpArmPlan{arm(unres), arm(unres)}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION"); err == nil {
			t.Fatal("two unresolvable DECIMAL arms were accepted")
		}
	})
}
