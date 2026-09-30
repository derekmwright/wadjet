// SPDX-License-Identifier: MIT

package expr

import (
	"math"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #679's producer half, per type.
//
// The re-run substitutes an outer row's values into the subquery's WHERE as
// literal TEXT, and the renderer used to read the Go BOX: an unrecognized box
// went through `fmt.Sprint` into a QUOTED STRING. `batch.Vector.GetValue`
// hands a DECIMAL back as its rendered text, so `a.w_d2 = b.k` became
// `'2.00' = b.k` and raised 22P02 for a query PostgreSQL answers.
//
// Nine of the eighteen flat types box as a string or a bare int64 that says
// nothing about which type it is, so this table is the fix's boundary written
// as a fixture: every type this engine has, and what its literal must READ AS.
// The CASTs are load-bearing — a bare numeric literal is float8 in this
// dialect, so a REAL or a DECIMAL rendered without one is compared as a
// double (measured: `c_f32 = 0.14285715` matches 0 rows where the column's own
// value matches 1).
func TestOuterLiteralRendersEveryTypeAsItsOwnType(t *testing.T) {
	for _, tc := range []struct {
		name  string
		typ   batch.TypeID
		scale int
		set   any
		want  string
	}{
		{"bool_true", batch.TypeBool, 0, true, "true"},
		{"bool_false", batch.TypeBool, 0, false, "false"},
		{"int32", batch.TypeInt32, 0, int32(-7), "__column_value(cast(-7 as integer))"},
		{"int64", batch.TypeInt64, 0, int64(9007199254740993), "__column_value(cast(9007199254740993 as bigint))"},
		{"port", batch.TypePort, 0, int32(1025), "1025"},
		{"protocol", batch.TypeProtocol, 0, int32(6), "6"},
		{"duration", batch.TypeDuration, 0, int64(1000000), "1000000"},
		// FormatFloat with -1 precision, which is the shortest text that
		// reads back as the SAME float64. `%g`'s default would round.
		{"float64", batch.TypeFloat64, 0, 0.3333333333333333,
			"__column_value(cast('0.3333333333333333' as double precision))"},
		{"float64_negative", batch.TypeFloat64, 0, -1e-20,
			"__column_value(cast('-1e-20' as double precision))"},
		{"float32", batch.TypeFloat32, 0, float32(0.14285715),
			"__column_value(cast('0.14285715' as real))"},
		{"string", batch.TypeString, 0, "s-000001", "'s-000001'"},
		{"string_with_a_quote", batch.TypeString, 0, "o'brien", "'o''brien'"},
		{"bytes", batch.TypeBytes, 0, []byte("bytes-000001-x"), "'bytes-000001-x'"},
		{"timestamp", batch.TypeTimestamp, 0, int64(1699999999000),
			"__column_value(cast('2023-11-14 22:13:19' as timestamp))"},
		// Sub-second precision has to survive: batch.FormatTimestamp keeps
		// three fractional digits only when there are any, and a rendering
		// that dropped them would compare a different instant.
		{"timestamp_with_millis", batch.TypeTimestamp, 0, int64(1699999999123),
			"__column_value(cast('2023-11-14 22:13:19.123' as timestamp))"},
		{"date", batch.TypeDate, 0, int32(15007), "__column_value(cast('2011-02-02' as date))"},
		{"ipv4", batch.TypeIPv4, 0, "10.0.0.1", "'10.0.0.1'"},
		{"ipv6", batch.TypeIPv6, 0, "2001:db8::1", "'2001:db8::1'"},
		{"cidr", batch.TypeCIDR, 0, "192.168.0.2/24", "'192.168.0.2/24'"},
		{"mac", batch.TypeMAC, 0, "aa:bb:cc:00:00:01", "'aa:bb:cc:00:00:01'"},
		{"uuid", batch.TypeUUID, 0, "00000000-0000-4000-8000-000000000001",
			"'00000000-0000-4000-8000-000000000001'"},
		// The column's OWN scale, which is what decides whether the
		// comparison is exact. Precision 38 is the Int128 carrier's width, so
		// it never narrows a value the column could hold.
		{"decimal_scale_4", batch.TypeDecimal, 4, "1.0001",
			"__column_value(cast('1.0001' as decimal(38, 4)))"},
		{"decimal_scale_2", batch.TypeDecimal, 2, "2.00",
			"__column_value(cast('2.00' as decimal(38, 2)))"},
		{"decimal_negative", batch.TypeDecimal, 2, "-0.01",
			"__column_value(cast('-0.01' as decimal(38, 2)))"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := batch.NewVectorWithScale(tc.typ, 1, tc.scale)
			v.SetValue(0, tc.set)
			lit, err := outerLiteral(v, 0)
			if err != nil {
				t.Fatalf("outerLiteral: %v", err)
			}
			if got := lit.String(); got != tc.want {
				t.Errorf("outerLiteral = %q, want %q", got, tc.want)
			}
		})
	}
}

// A NULL is a NULL OF THE COLUMN'S TYPE wherever the value's own spelling is a
// CAST or a typed literal, and the bare `null` where the value is a quoted
// string (SQL's unknown) or has no spelling: the outer row holds a typed NULL,
// every comparison over it is UNKNOWN, and the arithmetic around it keeps its
// type — `sum(x.v + o.i)` over a NULL int4 is NULL on PostgreSQL, where an
// untyped null answered the sum of x.v (outerNull).
func TestOuterLiteralRendersNullForEveryType(t *testing.T) {
	for _, c := range []struct {
		typ  batch.TypeID
		want string
	}{
		{batch.TypeBool, "__column_value(cast(null as boolean))"},
		{batch.TypeInt32, "__column_value(cast(null as integer))"},
		{batch.TypeInt64, "__column_value(cast(null as bigint))"},
		{batch.TypeFloat32, "__column_value(cast(null as real))"},
		{batch.TypeFloat64, "__column_value(cast(null as double precision))"},
		{batch.TypeTimestamp, "__column_value(cast(null as timestamp))"},
		{batch.TypeDate, "__column_value(cast(null as date))"},
		{batch.TypeDecimal, "__column_value(cast(null as decimal(38, 0)))"},
		// An ARRAY vector with no element declaration has no cast spelling.
		{batch.TypeArray, "null"},
		{batch.TypeString, "null"}, {batch.TypeBytes, "null"}, {batch.TypeUUID, "null"},
		{batch.TypeIPv4, "null"}, {batch.TypeIPv6, "null"}, {batch.TypeCIDR, "null"},
		{batch.TypeMAC, "null"}, {batch.TypePort, "null"}, {batch.TypeProtocol, "null"},
		{batch.TypeDuration, "null"}, {batch.TypeRow, "null"}, {batch.TypeMap, "null"},
		{batch.TypeVector, "null"},
	} {
		t.Run(c.typ.String(), func(t *testing.T) {
			v := batch.NewVector(c.typ, 1)
			v.Nulls.SetNull(0)
			lit, err := outerLiteral(v, 0)
			if err != nil {
				t.Fatalf("a NULL outer value must render, whatever its type: %v", err)
			}
			if got := lit.String(); got != c.want {
				t.Errorf("outerLiteral = %q, want %q", got, c.want)
			}
		})
	}
}

// The REFUSALS, which are the fix's other half: a value with no literal
// spelling that reads back as the same value FAILS the query rather than
// being rendered as something else (protocol item 8 — an obviously-loud
// failure beats a plausible wrong number).
//
// Method 10: each of these is a "cannot happen" the renderer asserts, and
// each gets a fixture that attempts it.
func TestOuterLiteralRefusesValuesWithNoLiteralSpelling(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  batch.TypeID
		set  any
	}{
		// No numeric literal in this dialect — the same bound ADR-0021 §2
		// records for the IN-set materialization.
		{"float64_nan", batch.TypeFloat64, math.NaN()},
		{"float64_inf", batch.TypeFloat64, math.Inf(1)},
		{"float64_neg_inf", batch.TypeFloat64, math.Inf(-1)},
		{"float32_nan", batch.TypeFloat32, float32(math.NaN())},
		// The only bytea spelling the parser accepts is a quoted string, and
		// these bytes do not survive it: a NUL cannot travel through the
		// wire's text format at all (#570) and invalid UTF-8 comes back as
		// different bytes.
		{"bytes_with_a_nul", batch.TypeBytes, []byte{0x41, 0x00, 0x42}},
		{"bytes_invalid_utf8", batch.TypeBytes, []byte{0xff, 0xfe, 0x00, 0x41}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := batch.NewVector(tc.typ, 1)
			v.SetValue(0, tc.set)
			assertUnrenderable(t, v)
		})
	}

	// ROW, MAP and VECTOR have no literal at all. An ARRAY has one since arc CW
	// round 5 — its typed array literal (ArrayValueLiteral), which reads back
	// as the same value — so it is asserted rendered, not refused. They are
	// built through a batch rather than SetValue, which the container vectors
	// do not take.
	t.Run("containers", func(t *testing.T) {
		schema := []parquet.Column{
			{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
				ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
			{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "a", Type: parquet.TypeString, Nullable: true},
			}},
			{Name: "c_map", Type: parquet.TypeMap, Nullable: true,
				ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
					{Name: "key", Type: parquet.TypeString},
					{Name: "value", Type: parquet.TypeInt64, Nullable: true},
				}}},
			{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 4},
		}
		b := batch.NewRecordBatch(schema, 1)
		b.Len = 1
		for i, col := range b.Columns {
			if col.Nulls.IsNullFast(0) {
				// A NULL container renders as `null`, which is correct and
				// covered above; give it a value so the refusal is reached.
				col.Nulls.SetValid(0)
			}
			if schema[i].Type == parquet.TypeArray {
				t.Run(schema[i].Name, func(t *testing.T) {
					lit, err := outerLiteral(col, 0)
					if err != nil || lit.String() != "__column_value(cast('{}' as TEXT[]))" {
						t.Fatalf("outerLiteral(empty text[]) = %v, %v; want the typed literal __column_value(cast('{}' as TEXT[]))", lit, err)
					}
				})
				continue
			}
			t.Run(schema[i].Name, func(t *testing.T) { assertUnrenderable(t, col) })
		}
	})
}

func assertUnrenderable(t *testing.T, v *batch.Vector) {
	t.Helper()
	lit, err := outerLiteral(v, 0)
	if err == nil {
		t.Fatalf("outerLiteral rendered %q; a value with no literal spelling that reads "+
			"back as the same value must FAIL the query, never be rendered as something else",
			lit.String())
	}
	var unrenderable *UnrenderableOuterValueError
	if !asUnrenderable(err, &unrenderable) {
		t.Fatalf("error %v is not an *UnrenderableOuterValueError", err)
	}
	if got := sqlerr.StateOf(err); got != "0A000" {
		t.Errorf("SQLSTATE %q, want 0A000 (feature_not_supported): the query is legal SQL "+
			"this engine has no lowering for", got)
	}
	if !strings.Contains(err.Error(), "rewrite the correlation as a join") {
		t.Errorf("error %q does not say what the user can do about it", err)
	}
}

func asUnrenderable(err error, target **UnrenderableOuterValueError) bool {
	u, ok := err.(*UnrenderableOuterValueError)
	if ok {
		*target = u
	}
	return ok
}
