// SPDX-License-Identifier: AGPL-3.0-only

package dagplan

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/logical"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestReconcileEmitsNoCoercionWhenTheArmsAlreadyAgree.
//
// Every coercion costs a fresh vector and a pass over the rows, and — more to
// the point — an OpDecimalCoerce in a fragment is a declaration that the arm's
// values MOVE. An arm already carrying the output type must carry neither. The
// same-(p,s) union is the overwhelmingly common case, and a reconciliation
// that emitted a no-op coercion for it would be paying for #533 on every union
// in the corpus.
func TestReconcileEmitsNoCoercionWhenTheArmsAlreadyAgree(t *testing.T) {
	dec := func(p, s int) physical.SetOpColType {
		return physical.SetOpColType{Typ: parquet.TypeDecimal, Known: true,
			Dec: logical.DecimalMeta{Precision: p, Scale: s}, DecKnown: true}
	}
	arm := func(cts ...physical.SetOpColType) physical.SetOpArmPlan {
		specs := make([]physical.ProjectExprSpec, len(cts))
		for i := range cts {
			specs[i] = physical.ProjectExprSpec{Expr: "c", Name: "v"}
		}
		return physical.SetOpArmPlan{Specs: specs, Types: cts}
	}

	t.Run("identical", func(t *testing.T) {
		plans := []physical.SetOpArmPlan{arm(dec(9, 2)), arm(dec(9, 2))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION", nil); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.Coerce) != 0 {
				t.Errorf("arm %d got %d coercions for a column it already carries: %+v", i, len(p.Coerce), p.Coerce)
			}
		}
	})

	t.Run("three_identical_arms", func(t *testing.T) {
		plans := []physical.SetOpArmPlan{arm(dec(18, 4)), arm(dec(18, 4)), arm(dec(18, 4))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION", nil); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.Coerce) != 0 {
				t.Errorf("arm %d got %d coercions: %+v", i, len(p.Coerce), p.Coerce)
			}
		}
	})

	t.Run("non_decimal_arms_are_untouched", func(t *testing.T) {
		i64 := physical.SetOpColType{Typ: parquet.TypeInt64, Known: true}
		plans := []physical.SetOpArmPlan{arm(i64), arm(i64)}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION", nil); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.Coerce) != 0 {
				t.Errorf("arm %d got %d coercions for an INT64 column: %+v", i, len(p.Coerce), p.Coerce)
			}
		}
	})

	// The control, and the precise rule: ONLY the arm that differs is moved.
	// (9,2) with (18,4) resolves to (18,4), so the second arm already carries
	// the output type and must be left alone even though the operation as a
	// whole needed reconciling — the coercion is per ARM, not per operation.
	t.Run("only_the_differing_arm_is_moved", func(t *testing.T) {
		plans := []physical.SetOpArmPlan{arm(dec(9, 2)), arm(dec(18, 4))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION", nil); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		if len(plans[0].Coerce) != 1 {
			t.Fatalf("the narrow arm got %d coercions, want 1: %+v", len(plans[0].Coerce), plans[0].Coerce)
		}
		if plans[0].Coerce[0] != (physical.DecimalCoercion{Name: "v", Precision: 18, Scale: 4}) {
			t.Errorf("the narrow arm's coercion = %+v, want DECIMAL(18,4) on v", plans[0].Coerce[0])
		}
		if len(plans[1].Coerce) != 0 {
			t.Errorf("the arm already carrying the output type got %d coercions: %+v",
				len(plans[1].Coerce), plans[1].Coerce)
		}
	})

	// Neither arm carrying the output type means BOTH move: (18,2) with
	// (9,4) resolves to (20,4), which is neither.
	t.Run("both_arms_move_when_neither_is_the_target", func(t *testing.T) {
		plans := []physical.SetOpArmPlan{arm(dec(18, 2)), arm(dec(9, 4))}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION", nil); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
		for i, p := range plans {
			if len(p.Coerce) != 1 {
				t.Fatalf("arm %d got %d coercions, want 1: %+v", i, len(p.Coerce), p.Coerce)
			}
			if p.Coerce[0] != (physical.DecimalCoercion{Name: "v", Precision: 20, Scale: 4}) {
				t.Errorf("arm %d coercion = %+v, want DECIMAL(20,4) on v", i, p.Coerce[0])
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
		plans := []physical.SetOpArmPlan{arm(dec(9, 2)), arm(physical.SetOpColType{Typ: parquet.TypeDecimal, Known: true})}
		err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION", nil)
		if err == nil {
			t.Fatalf("an unresolvable DECIMAL target was accepted; the arms then keep their own scales, "+
				"which is a silently wrong answer (#551). coercions: %+v / %+v",
				plans[0].Coerce, plans[1].Coerce)
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
		unres := physical.SetOpColType{Typ: parquet.TypeDecimal, Known: true}
		plans := []physical.SetOpArmPlan{arm(unres), arm(unres)}
		if err := reconcileSetOpArmTypes(plans, []string{"v"}, "UNION", nil); err == nil {
			t.Fatal("two unresolvable DECIMAL arms were accepted")
		}
	})
}
