package physical

import (
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func f64Col(name string) parquet.Column {
	return parquet.Column{Name: name, Type: parquet.TypeFloat64, Nullable: true}
}

func f32Col(name string) parquet.Column {
	return parquet.Column{Name: name, Type: parquet.TypeFloat32, Nullable: true}
}

// TestUnifySetOpSchemasWidensEveryRung is #541's TYPE half: the single-process
// path resolves a set operation's output type through the SAME two functions
// the stage DAG uses — setOpWiden for the ladder and setOpDecimalTarget for
// the DECIMAL rung — so the two paths cannot answer with different types for
// the same pair of arms, in either arm order.
//
// Before this the adapter built its result under the FIRST arm's schema for
// every pair whose TypeIDs differed, which made the arm ORDER decide the type
// (and, for a DECIMAL arm under a FLOAT64 first arm, failed the query
// outright on the #361 store guard).
func TestUnifySetOpSchemasWidensEveryRung(t *testing.T) {
	i32 := parquet.Column{Name: "v", Type: parquet.TypeInt32}
	i64 := parquet.Column{Name: "v", Type: parquet.TypeInt64}
	f32 := parquet.Column{Name: "v", Type: parquet.TypeFloat32, Nullable: true}
	f64 := f64Col("v")
	d92 := decCol("v", 9, 2)
	d184 := decCol("v", 18, 4)

	for _, tc := range []struct {
		name      string
		left      parquet.Column
		right     parquet.Column
		wantType  parquet.TypeID
		wantPrec  int
		wantScale int
	}{
		// float8 is the PREFERRED type of PostgreSQL's numeric category, so
		// `numeric ∪ double precision` is double precision either way round.
		// The DECIMAL-first order was the silent TYPE divergence (#541 shape
		// 3); the FLOAT64-first order was the loud store failure (shape 2).
		{"decimal_then_double", d92, f64, parquet.TypeFloat64, 0, 0},
		{"double_then_decimal", f64, d92, parquet.TypeFloat64, 0, 0},
		{"bigint_then_double", i64, f64, parquet.TypeFloat64, 0, 0},
		{"double_then_bigint", f64, i64, parquet.TypeFloat64, 0, 0},
		{"real_then_double", f32, f64, parquet.TypeFloat64, 0, 0},
		{"double_then_real", f64, f32, parquet.TypeFloat64, 0, 0},
		// float4 is PREFERRED too, so it beats every EXACT type and only
		// float8 beats it — `real ∪ numeric` and `real ∪ integer/bigint` are
		// real on live postgres:17, in either order. Widening them to float8
		// would re-render a real's 0.1 as 0.10000000149011612.
		{"decimal_then_real", d92, f32, parquet.TypeFloat32, 0, 0},
		{"real_then_decimal", f32, d92, parquet.TypeFloat32, 0, 0},
		{"int_then_real", i32, f32, parquet.TypeFloat32, 0, 0},
		{"real_then_int", f32, i32, parquet.TypeFloat32, 0, 0},
		{"bigint_then_real", i64, f32, parquet.TypeFloat32, 0, 0},
		{"real_then_bigint", f32, i64, parquet.TypeFloat32, 0, 0},
		// `integer ∪ bigint` is bigint.
		{"int_then_bigint", i32, i64, parquet.TypeInt64, 0, 0},
		// Two DECIMALs take the DAG's rule: max scale, precision rebuilt from
		// the widest INTEGER part. (9,2) has 7 integer digits, (18,4) has 14,
		// so the answer is 14+4 = DECIMAL(18,4) — and (18,4) beside (9,2)
		// resolves to the same type in the other order.
		{"decimal_then_wider_decimal", d92, d184, parquet.TypeDecimal, 18, 4},
		{"wider_decimal_then_decimal", d184, d92, parquet.TypeDecimal, 18, 4},
		// The rebuilt integer part is the point of not using max(precision):
		// DECIMAL(18,2) has 16 integer digits and DECIMAL(9,4) needs scale 4,
		// so the values need 20 digits, not the 18 max(precision) declares.
		{"integer_part_beats_max_precision", decCol("v", 18, 2), decCol("v", 9, 4),
			parquet.TypeDecimal, 20, 4},
		// numeric ∪ bigint is numeric: INT64's whole range is 19 digits, plus
		// the DECIMAL's scale.
		{"decimal_then_bigint", d92, i64, parquet.TypeDecimal, 21, 2},
		{"bigint_then_decimal", i64, d92, parquet.TypeDecimal, 21, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out := unifySetOpSchemas([]parquet.Column{tc.left}, []parquet.Column{tc.right})
			got := out[0]
			if got.Type != tc.wantType {
				t.Fatalf("type: got %s, want %s", got.Type, tc.wantType)
			}
			if got.Type == parquet.TypeDecimal && (got.Precision != tc.wantPrec || got.Scale != tc.wantScale) {
				t.Fatalf("(p,s): got (%d,%d), want (%d,%d)", got.Precision, got.Scale, tc.wantPrec, tc.wantScale)
			}
			if got.Name != tc.left.Name {
				t.Fatalf("the result takes the FIRST arm's name %q, got %q", tc.left.Name, got.Name)
			}
			// The DAG's own answer for the same pair, through the functions
			// reconcileSetOpArmTypes calls. Drift between the two paths is
			// what #541 asked to make impossible.
			lc, ok1 := setOpColTypeFromColumn(tc.left)
			rc, ok2 := setOpColTypeFromColumn(tc.right)
			if !ok1 || !ok2 {
				t.Fatalf("the ladder must resolve both arms")
			}
			dagType, ok := setOpWiden(lc.typ, rc.typ)
			if !ok || dagType != got.Type {
				t.Fatalf("the stage DAG resolves %s, the local path %s", dagType, got.Type)
			}
		})
	}
}

// TestUnifySetOpSchemasLeavesUnresolvableArmsAlone is the deliberate
// non-change: a computed DECIMAL expression carries no declared (p,s) (#555,
// being fixed in the declared-type layer), and a non-numeric pair is not on
// the ladder at all. Both keep today's behaviour rather than having a type
// guessed for them.
func TestUnifySetOpSchemasLeavesUnresolvableArmsAlone(t *testing.T) {
	unconstrained := parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 0, Scale: 4, Nullable: true}
	str := parquet.Column{Name: "v", Type: parquet.TypeString}

	// A DECIMAL with no declared precision beside an INTEGER: nothing to
	// rebuild a precision from, so the column is left as written.
	out := unifySetOpSchemas([]parquet.Column{unconstrained},
		[]parquet.Column{{Name: "v", Type: parquet.TypeInt64}})
	if out[0].Type != parquet.TypeDecimal || out[0].Precision != 0 {
		t.Errorf("an unconstrained DECIMAL arm must be left alone, got %+v", out[0])
	}

	// Two DECIMALs, one unconstrained: the max(scale) fallback still applies,
	// because #532's truncation happens whether or not a precision was
	// declared and max(scale) moves no value.
	out = unifySetOpSchemas([]parquet.Column{decCol("v", 9, 2)}, []parquet.Column{unconstrained})
	if out[0].Scale != 4 {
		t.Errorf("two DECIMAL arms must still widen to max(scale) 4, got scale %d", out[0].Scale)
	}

	// A non-numeric pair is off the ladder entirely.
	out = unifySetOpSchemas([]parquet.Column{str}, []parquet.Column{{Name: "v", Type: parquet.TypeInt64}})
	if out[0].Type != parquet.TypeString {
		t.Errorf("a STRING arm must be left alone, got %s", out[0].Type)
	}
}

// TestCoerceSetOpArmRowsMovesEveryRung is #541's VALUE half. The boxes are not
// uniform — a DECIMAL is its rendered TEXT, an integer a raw int64, a float a
// float64 — so a widened column needs each arm's box MOVED into the shape the
// unified column reads, not merely relabelled.
func TestCoerceSetOpArmRowsMovesEveryRung(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  parquet.Column
		dst  parquet.Column
		in   any
		want any
	}{
		// The loud half of #541: a DECIMAL box under a FLOAT64 result used to
		// reach batch.FromRows as a string and die on the #361 store guard
		// ("cannot store string into FLOAT64 vector").
		{"decimal_text_to_double", decCol("v", 9, 2), f64Col("v"), "12.75", 12.75},
		{"bigint_to_double", parquet.Column{Name: "v", Type: parquet.TypeInt64}, f64Col("v"), int64(7), float64(7)},
		{"real_to_double", parquet.Column{Name: "v", Type: parquet.TypeFloat32}, f64Col("v"), float32(0.5), 0.5},
		// The float32 rung. The box has to be narrowed HERE, not left as a
		// float64 for SetValue to narrow at the store: the dedup key reads
		// the box (keyValueText's TypeFloat32 arm), so an un-narrowed arm
		// would key as a different number from an arm that arrived real.
		{"decimal_text_to_real", decCol("v", 9, 2), f32Col("v"), "12.75", float32(12.75)},
		{"bigint_to_real", parquet.Column{Name: "v", Type: parquet.TypeInt64}, f32Col("v"), int64(7), float32(7)},
		{"double_to_real", f64Col("v"), f32Col("v"), 0.5, float32(0.5)},
		// `integer ∪ bigint`: the value does not move, the box does.
		{"int_to_bigint", parquet.Column{Name: "v", Type: parquet.TypeInt32},
			parquet.Column{Name: "v", Type: parquet.TypeInt64}, int32(3), int64(3)},
		// The silent half of #547/#541: an integer box read into a DECIMAL
		// vector as an unscaled carrier is 1 -> 0.01.
		{"bigint_to_decimal", parquet.Column{Name: "v", Type: parquet.TypeInt64}, decCol("v", 21, 2), int64(1), "1"},
		// A DECIMAL arm under a wider DECIMAL keeps its exact text: FromRows
		// re-reads it at the unified scale, which is never smaller.
		{"decimal_to_wider_decimal", decCol("v", 9, 2), decCol("v", 18, 4), "12.75", "12.75"},
		// A NULL is a member like any other and is never converted.
		{"null_untouched", decCol("v", 9, 2), f64Col("v"), nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []map[string]any{{"v": tc.in}}
			out, err := coerceSetOpArmRows(rows, []parquet.Column{tc.src}, []parquet.Column{tc.dst})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := out[0]["v"]; got != tc.want {
				t.Fatalf("got %#v (%T), want %#v (%T)", got, got, tc.want, tc.want)
			}
		})
	}
}

// TestCoerceSetOpArmRowsDecimalToDoubleThenStores is the end-to-end shape of
// #541's second repro: `double precision UNION ALL numeric` failed the query
// on the store guard. With the arm moved, batch.FromRows builds the FLOAT64
// column the unified schema declares.
func TestCoerceSetOpArmRowsDecimalToDoubleThenStores(t *testing.T) {
	src := []parquet.Column{decCol("v", 9, 2)}
	target := []parquet.Column{f64Col("v")}
	rows := []map[string]any{{"v": "12.75"}, {"v": "-0.50"}, {"v": nil}}

	out, err := coerceSetOpArmRows(rows, src, target)
	if err != nil {
		t.Fatalf("coercing a DECIMAL arm to double: %v", err)
	}
	b := batch.FromRows(target, out)
	want := []struct {
		f    float64
		null bool
	}{{12.75, false}, {-0.5, false}, {0, true}}
	for i, w := range want {
		if w.null {
			if !b.Columns[0].Nulls.IsNull(i) {
				t.Errorf("row %d: expected NULL", i)
			}
			continue
		}
		if got := b.Columns[0].Float64Data[i]; got != w.f {
			t.Errorf("row %d: got %v, want %v", i, got, w.f)
		}
	}
}

// TestCoerceSetOpArmRowsDecimalOverflowIsAn22003Error is #553 caught one layer
// earlier than batch.FromRowsChecked: the DECIMAL arm whose value has no
// Int128 at the union's scale, or none inside its declared precision, is
// refused by name with PostgreSQL's SQLSTATE — never saturated to Int128Max.
func TestCoerceSetOpArmRowsDecimalOverflowIsAn22003Error(t *testing.T) {
	// DECIMAL(38,0) holding 10^30 beside DECIMAL(11,10) resolves to
	// DECIMAL(38,10), and 10^30 at scale 10 needs 10^40 — no Int128 (#552's
	// shape, #553's value).
	const e30 = "1000000000000000000000000000000"
	for _, tc := range []struct {
		name    string
		text    string
		dst     parquet.Column
		wantErr bool
	}{
		{"past_the_carrier", e30, decCol("v", 38, 10), true},
		// Inside the carrier, outside the declaration: 10^30 at scale 8 is
		// 10^38, which an Int128 holds and a DECIMAL(38,8) does not.
		{"past_the_declared_precision", e30, decCol("v", 38, 8), true},
		{"in_range", "12.75", decCol("v", 38, 10), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := []map[string]any{{"v": tc.text}}
			_, err := coerceSetOpArmRows(rows, []parquet.Column{decCol("v", 38, 0)}, []parquet.Column{tc.dst})
			if !tc.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("%s into %v answered instead of failing; saturating it to Int128Max is "+
					"exactly the silent corruption of #553", tc.text, tc.dst)
			}
			if !strings.Contains(err.Error(), "numeric field overflow") {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := sqlerr.StateOf(err); got != "22003" {
				t.Fatalf("SQLSTATE = %q, want 22003 (numeric_value_out_of_range); err = %v", got, err)
			}
		})
	}
}

// TestSetOpCheckedIntDecimalTextCarriesSQLSTATE pins the SQLSTATE on the
// integer rung's overflow too — it was a bare fmt.Errorf, so it reached
// clients as the internal-error class (ADR-0024 item 4).
func TestSetOpCheckedIntDecimalTextCarriesSQLSTATE(t *testing.T) {
	// DECIMAL(38,30) ∪ BIGINT: 1e9 at scale 30 is 1e39, no Int128.
	_, err := setOpCheckedIntDecimalText(int64(1_000_000_000), "v", 38, 30)
	if err == nil {
		t.Fatal("1e9 at scale 30 has no Int128 and must be refused")
	}
	if got := sqlerr.StateOf(err); got != "22003" {
		t.Fatalf("SQLSTATE = %q, want 22003; err = %v", got, err)
	}
}

// TestSetOpKeyerNeverKeysASaturatedDecimal is the key-side twin of #553.
//
// keyValueText resolved a DECIMAL box through the SATURATING parser and
// discarded ScaledDecimal.Sat, so every value with no Int128 at the column's
// scale would have keyed as Int128Max — one key for an unbounded set of
// distinct numbers, which merges members of a UNION and matches unequal rows
// in an INTERSECT.
//
// It cannot fire through the adapter, because the text it is handed is
// FormatDecimal output for a value the column already holds. That is an
// argument about the caller, not a property of the keyer, and this test makes
// it a property: two distinct out-of-range texts must key apart.
func TestSetOpKeyerNeverKeysASaturatedDecimal(t *testing.T) {
	col := decCol("v", 38, 10)
	// Both need more than 38 digits at scale 10, so both used to saturate to
	// Int128Max — and they are different numbers.
	a := keyValueText(&col, "1000000000000000000000000000000")
	b := keyValueText(&col, "2000000000000000000000000000000")
	if a == b {
		t.Fatalf("two distinct out-of-range values share the key %q: a saturating parse makes "+
			"Int128Max the key for every one of them, which merges them in a UNION", a)
	}
	// In-range values are unaffected, and still key by NUMBER rather than by
	// rendering: 12.75 and 12.7500 are one member (#474).
	narrow, wide := decCol("v", 9, 2), decCol("v", 18, 4)
	if keyValueText(&narrow, "12.75") != keyValueText(&wide, "12.7500") {
		t.Error("12.75 at scale 2 and 12.7500 at scale 4 are one value and must be one key")
	}
}

// TestSetOpDecimalFitsPrecisionChecksThe38DigitBound is the R12 correction: a
// declared precision past the carrier's width used to mean "no bound to
// check", which admitted exactly the values that have no carrier. It is now
// checked against 10^38, the widest bound an Int128 can express.
func TestSetOpDecimalFitsPrecisionChecksThe38DigitBound(t *testing.T) {
	// 10^38, which is outside DECIMAL(38) and outside every wider declaration
	// an Int128 could stand behind.
	tenPow38, ok := batch.Int128From(1).MulPow10(38)
	if !ok {
		t.Fatal("10^38 must have an Int128")
	}
	for _, p := range []int{38, 39, 50} {
		if setOpDecimalFits(tenPow38, p) {
			t.Errorf("precision %d admitted 10^38; past 38 the bound clamps to the carrier's "+
				"own width rather than being skipped", p)
		}
	}
	// One below the bound still fits.
	just, _ := tenPow38.AddChecked(batch.Int128From(-1))
	if !setOpDecimalFits(just, 38) {
		t.Error("10^38 - 1 is the largest DECIMAL(38) value and must fit")
	}
	// Precision 0 is #458's unconstrained sentinel: no bound to apply.
	if !setOpDecimalFits(tenPow38, 0) {
		t.Error("an unconstrained declaration has no bound to check")
	}
}

// TestSetOpDecimalTextRefusesTheSpecialsWithOneSQLSTATE is #534's review
// finding R4: two value-producing readers, one classification.
//
// batch.ParseDecimalStringChecked answers 22003 for a NaN/±Infinity spelling —
// PostgreSQL reads all three as `numeric` VALUES, so the text is not an
// input-syntax error but a value this carrier has no bit pattern for
// (ADR-0024 item 6). This site read the same text through DecimalTextAt, which
// does not know them, and fell into its 22P02 arm: the identical box reaching
// the identical column answered a different SQLSTATE depending on which of the
// two readers saw it, which a client branching on the code cannot see past.
// Both now classify through batch.DecimalSpecialValueError.
func TestSetOpDecimalTextRefusesTheSpecialsWithOneSQLSTATE(t *testing.T) {
	dst := decCol("v", 38, 10)
	for _, text := range []string{"NaN", "nan", " NaN ", "Infinity", "inf", "+Infinity", "-Infinity", "-inf"} {
		t.Run(text, func(t *testing.T) {
			_, err := setOpCheckedDecimalText(text, "v", dst.Precision, dst.Scale)
			if err == nil {
				t.Fatalf("%q was accepted as a stored value; the carrier has no bit pattern for it", text)
			}
			if got := sqlerr.StateOf(err); got != "22003" {
				t.Errorf("SQLSTATE = %q, want 22003 — batch.ParseDecimalStringChecked answers 22003 "+
					"for this same text; err = %v", got, err)
			}
			if !strings.Contains(err.Error(), "ADR-0024 item 6") {
				t.Errorf("error does not name the record that decided it: %v", err)
			}

			// The two readers must agree, which is the actual invariant: the
			// same text through the checked writer's reader.
			_, cerr := batch.ParseDecimalStringChecked(text, dst.Scale)
			if cerr == nil || sqlerr.StateOf(cerr) != sqlerr.StateOf(err) {
				t.Errorf("the two value readers disagree on %q: set-op %v / checked %v", text, err, cerr)
			}

			// And through the adapter, which is how a real query reaches it.
			rows := []map[string]any{{"v": text}}
			_, aerr := coerceSetOpArmRows(rows, []parquet.Column{decCol("v", 38, 0)}, []parquet.Column{dst})
			if aerr == nil {
				t.Errorf("coerceSetOpArmRows accepted %q as a stored value", text)
			} else if got := sqlerr.StateOf(aerr); got != "22003" {
				t.Errorf("adapter SQLSTATE = %q, want 22003; err = %v", got, aerr)
			}
		})
	}

	// The boundary: text that names no number at all stays 22P02 on both, and
	// a spelling PostgreSQL's numeric refuses is that case, not a special.
	for _, text := range []string{"abc", "+NaN", "-NaN", "Infin", "infinit"} {
		_, err := setOpCheckedDecimalText(text, "v", dst.Precision, dst.Scale)
		if err == nil {
			t.Errorf("%q was accepted as a stored value", text)
			continue
		}
		if got := sqlerr.StateOf(err); got != "22P02" {
			t.Errorf("%q: SQLSTATE = %q, want 22P02; err = %v", text, got, err)
		}
	}

	// An ordinary in-range value is untouched.
	if got, err := setOpCheckedDecimalText("12.75", "v", dst.Precision, dst.Scale); err != nil || got != "12.75" {
		t.Errorf(`setOpCheckedDecimalText("12.75") = %v, %v`, got, err)
	}
}
