package coordinator

import (
	"math"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The NUMERIC-VALUES fixture (arc NV): the edge magnitudes no other corpus in
// this package carries.
//
// It rides in tmdTables() the way every fixture above it does, and it is here
// rather than folded into the type matrix because the type matrix is a
// ROUND-TRIP corpus: a NaN or a 1e308 in c_f64 would move the expected value
// of every aggregate in every gate that walks it, and the compaction and CTAS
// gates that #1091 names walk it too. The class those gates are missing is the
// SAME class, and closing it there is #1091's own half.
//
// Four tables, one question each:
//
//	nvovf   the float RANGE: two rows at 1e308 so a SUM leaves float8, a
//	        1e-300 so a product flushes to zero, and a pair of ordinary rows
//	        in a second group so the refusal can be shown to be per-query and
//	        not per-table (#1082).
//	nvspec  the values that ARRIVE non-finite. PostgreSQL stores ±Infinity and
//	        NaN in a double precision column and every operator over one
//	        answers, so these rows are what separates a refusal from an
//	        operand exemption.
//	nvedge  the integers and DECIMALs at their carriers' edges — 2^53±1,
//	        int4's and int8's extremes, a DECIMAL(30,2) past a double's reach
//	        and a DECIMAL(38,10) at the 128-bit carrier's — plus a PORT and a
//	        PROTOCOL column, whose arithmetic is int4's (#1000).
//	nvreal  the REAL accumulation: 16777216 is 2^24, where a real stops
//	        counting by ones, so the values after it decide whether the total
//	        was carried at float4's width or at float8's (#950).
const (
	nvOvfTable  = "nvovf"
	nvSpecTable = "nvspec"
	nvEdgeTable = "nvedge"
	nvRealTable = "nvreal"
)

func nvFloatSchema(name string) parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "f8", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "f4", Type: parquet.TypeFloat32, Nullable: true},
	}}
}

func nvOvfSchema() parquet.Schema  { return nvFloatSchema(nvOvfTable) }
func nvSpecSchema() parquet.Schema { return nvFloatSchema(nvSpecTable) }

func nvOvfData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "g": int32(1), "f8": 1e308, "f4": float32(1e38)},
		{"id": int64(2), "g": int32(1), "f8": 1e308, "f4": float32(1e38)},
		{"id": int64(3), "g": int32(2), "f8": 1e-300, "f4": float32(1e-44)},
		{"id": int64(4), "g": int32(2), "f8": 2.5, "f4": float32(2.5)},
	}
}

func nvSpecData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "g": int32(1), "f8": math.Inf(1), "f4": float32(math.Inf(1))},
		{"id": int64(2), "g": int32(1), "f8": math.Inf(-1), "f4": float32(math.Inf(-1))},
		{"id": int64(3), "g": int32(2), "f8": math.NaN(), "f4": float32(math.NaN())},
		{"id": int64(4), "g": int32(2), "f8": 1.0, "f4": float32(1.0)},
	}
}

func nvEdgeSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "i4", Type: parquet.TypeInt32, Nullable: true},
		{Name: "i8", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d302", Type: parquet.TypeDecimal, Precision: 30, Scale: 2, Nullable: true},
		{Name: "d3810", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
		{Name: "pt", Type: parquet.TypePort, Nullable: true},
		{Name: "pr", Type: parquet.TypeProtocol, Nullable: true},
	}}
}

// nvDec boxes a decimal as the parquet writer's verbatim input, so the fixture
// says what it means rather than routing its own values through ParseFloat.
//
// It panics rather than taking a *testing.T because tmdTables() — the shared
// corpus every suite in this package loads — takes none, and a fixture literal
// that does not parse is a compile-time mistake wearing a runtime coat.
func nvDec(s string, p, sc int) any {
	d, err := parquet.DecimalValueFromText(s, p, sc)
	if err != nil {
		panic("nv fixture: " + s + ": " + err.Error())
	}
	return d
}

func nvEdgeData() []map[string]any {
	const wide = "9999999999999999999999999999.9999999999"
	return []map[string]any{
		{"id": int64(1), "g": int32(1), "i4": int32(2147483647), "i8": int64(9007199254740993),
			"d302": nvDec("9007199254740993.25", 30, 2), "d3810": nvDec(wide, 38, 10),
			"pt": int32(65535), "pr": int32(255)},
		{"id": int64(2), "g": int32(1), "i4": int32(-2147483648), "i8": int64(-9007199254740993),
			"d302": nvDec("-1.25", 30, 2), "d3810": nvDec("-0.0000000001", 38, 10),
			"pt": int32(80), "pr": int32(6)},
		{"id": int64(3), "g": int32(2), "i4": int32(0), "i8": int64(0),
			"d302": nvDec("0.00", 30, 2), "d3810": nvDec("0.0000000000", 38, 10),
			"pt": int32(0), "pr": int32(0)},
		{"id": int64(4), "g": int32(2), "i4": int32(1), "i8": int64(1),
			"d302": nvDec("1.00", 30, 2), "d3810": nvDec("1.0000000000", 38, 10),
			"pt": int32(443), "pr": int32(17)},
		{"id": int64(5), "g": int32(2), "i4": int32(2), "i8": int64(9223372036854775807),
			"d302": nvDec("2.50", 30, 2), "d3810": nvDec("2.5000000000", 38, 10),
			"pt": int32(1), "pr": int32(1)},
	}
}

func nvRealSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "r", Type: parquet.TypeFloat32, Nullable: true},
	}}
}

func nvRealData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "g": int32(1), "r": float32(2)},
		{"id": int64(2), "g": int32(1), "r": float32(0.1)},
		{"id": int64(3), "g": int32(1), "r": float32(12.75)},
		{"id": int64(4), "g": int32(1), "r": float32(16777216)},
		{"id": int64(5), "g": int32(1), "r": float32(-20)},
		{"id": int64(6), "g": int32(2), "r": float32(0)},
		{"id": int64(7), "g": int32(2), "r": float32(12.75)},
		{"id": int64(8), "g": int32(2), "r": float32(1.5)},
		{"id": int64(9), "g": int32(2), "r": nil},
	}
}

// nvFoldSchema is the DEFERRED half's fixture: a DECIMAL at the carrier's
// declared width beside one at an ordinary scale, so the shapes #712 and #764
// name are expressible (see TestADecimalFoldStillRendersAtTheFoldsScale).
func nvFoldSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "d30", Type: parquet.TypeDecimal, Precision: 38, Scale: 30, Nullable: true},
		{Name: "d152", Type: parquet.TypeDecimal, Precision: 15, Scale: 2, Nullable: true},
	}}
}

const nvFoldTable = "nvfold"

func nvFoldData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "g": int32(1), "d30": nvDec("1.5", 38, 30), "d152": nvDec("12.75", 15, 2)},
		{"id": int64(2), "g": int32(1), "d30": nil, "d152": nil},
		{"id": int64(3), "g": int32(2), "d30": nvDec("2.25", 38, 30), "d152": nvDec("1.00", 15, 2)},
		{"id": int64(4), "g": int32(2), "d30": nvDec("-3.5", 38, 30), "d152": nvDec("-3.50", 15, 2)},
	}
}
