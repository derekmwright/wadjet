package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func decCol(name string, prec, scale int) parquet.Column {
	return parquet.Column{Name: name, Type: parquet.TypeDecimal, Precision: prec, Scale: scale, Nullable: true}
}

// TestUnifySetOpSchemasDecimalIntegerRung is the single-process half of #547:
// a set operation across a DECIMAL arm and an INTEGER arm must resolve to the
// DECIMAL type PostgreSQL does (numeric ∪ bigint is numeric), in either arm
// order, so the integer arm is coerced INTO the decimal rather than read as an
// unscaled carrier.
func TestUnifySetOpSchemasDecimalIntegerRung(t *testing.T) {
	i64 := parquet.Column{Name: "id", Type: parquet.TypeInt64}
	i32 := parquet.Column{Name: "id", Type: parquet.TypeInt32}
	a := decCol("a", 9, 2)

	for _, tc := range []struct {
		name       string
		left       parquet.Column
		right      parquet.Column
		wantType   parquet.TypeID
		wantScale  int
		wantPrec   int
		wantChange bool
	}{
		// DECIMAL(9,2) ∪ INT64: scale stays 2, precision rebuilt from the
		// widest integer part (INT64's 19 digits) plus scale 2 = 21.
		{"decimal_first_int64", a, i64, parquet.TypeDecimal, 2, 21, true},
		{"int64_first_decimal", i64, a, parquet.TypeDecimal, 2, 21, true},
		{"decimal_first_int32", a, i32, parquet.TypeDecimal, 2, 12, true},
		{"int32_first_decimal", i32, a, parquet.TypeDecimal, 2, 12, true},
		// Two integers are left alone — FromRows moves no value between them.
		{"int32_int64", parquet.Column{Name: "id", Type: parquet.TypeInt32}, i64, parquet.TypeInt32, 0, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := unifySetOpSchemas([]parquet.Column{tc.left}, []parquet.Column{tc.right})
			got := out[0]
			if !tc.wantChange {
				if got.Type != tc.left.Type {
					t.Fatalf("expected %s left unchanged, got %s", tc.left.Type, got.Type)
				}
				return
			}
			if got.Type != tc.wantType {
				t.Fatalf("type: got %s want %s", got.Type, tc.wantType)
			}
			if got.Scale != tc.wantScale || got.Precision != tc.wantPrec {
				t.Fatalf("(p,s): got (%d,%d) want (%d,%d)", got.Precision, got.Scale, tc.wantPrec, tc.wantScale)
			}
			if got.Name != tc.left.Name {
				t.Fatalf("result must take the first arm's name %q, got %q", tc.left.Name, got.Name)
			}
		})
	}
}

// TestCoerceSetOpArmRowsIntegerNotShrunk is the corruption itself: an integer
// box read into a DECIMAL vector as an unscaled carrier divides it by 10^scale
// (1 -> 0.01). coerceSetOpArmRows rewrites the box to decimal text so FromRows
// reads it at the unified scale, and the value survives intact.
func TestCoerceSetOpArmRowsIntegerNotShrunk(t *testing.T) {
	src := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
	target := []parquet.Column{decCol("id", 21, 2)}

	rows := []map[string]any{{"id": int64(1)}, {"id": int64(9)}, {"id": int64(-5)}, {"id": nil}}
	if _, err := coerceSetOpArmRows(rows, src, target); err != nil {
		t.Fatalf("in-range coercion errored: %v", err)
	}

	b := batch.FromRows(target, rows)
	col := b.Columns[0]
	want := []struct {
		text string
		null bool
	}{{"1.00", false}, {"9.00", false}, {"-5.00", false}, {"", true}}
	for i, w := range want {
		if w.null {
			if !col.Nulls.IsNull(i) {
				t.Errorf("row %d: expected NULL", i)
			}
			continue
		}
		got := col.GetValue(i)
		if s, ok := got.(string); !ok || s != w.text {
			t.Errorf("row %d: got %v, want %q (a raw int box would have shrunk it 100x)", i, got, w.text)
		}
	}
}

// TestCoerceSetOpArmRowsOverflowErrors is the soundness bar (#547 review): a
// value the unified DECIMAL cannot hold must ERROR at the coercion — the same
// refusal the stage DAG raises (exec.coerceDecimalVector) — never a silently
// saturated Int128Max routed through DecimalTextAt's comparison parser. Both
// bands the DAG rejects are covered: the Int128 carrier and the declared
// precision. In the overflow band wadjet errors where PostgreSQL's unbounded
// numeric answers, the finite-DECIMAL residual tracked by #552.
func TestCoerceSetOpArmRowsOverflowErrors(t *testing.T) {
	for _, tc := range []struct {
		name    string
		val     int64
		target  parquet.Column
		wantErr bool
	}{
		// DECIMAL(38,30) ∪ BIGINT: 1e9 at scale 30 is 1e39 — 40 digits, no
		// Int128 at all (carrier band).
		{"carrier_overflow", 1_000_000_000, decCol("id", 38, 30), true},
		// DECIMAL(38,20) ∪ BIGINT: 1.5e18 at scale 20 is 1.5e38 — 39 digits,
		// fits the Int128 carrier but exceeds the declared precision 38's
		// 10^38 bound (the sub-carrier precision band).
		{"precision_overflow", 1_500_000_000_000_000_000, decCol("id", 38, 20), true},
		// Just inside the same declared type: 3 at scale 30 is 31 digits,
		// well within both bounds.
		{"in_range_high_scale", 3, decCol("id", 38, 30), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}
			rows := []map[string]any{{"id": tc.val}}
			_, err := coerceSetOpArmRows(rows, src, []parquet.Column{tc.target})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("value %d into %v: expected a numeric field overflow, got none", tc.val, tc.target)
				}
				if !strings.Contains(err.Error(), "numeric field overflow") {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("value %d into %v: unexpected error %v", tc.val, tc.target, err)
			}
		})
	}
}
