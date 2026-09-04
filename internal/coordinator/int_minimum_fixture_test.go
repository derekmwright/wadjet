package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The INTEGER-MINIMUM fixture (round-1 review, P1): one row holding each
// integer type's most negative value, where |v| has no room in the type.
//
// Two's complement has one more negative value than positive, so `ABS(min)`
// and `-min` are the two integer expressions with a right type and no value.
// PostgreSQL 17.11 raises for both — measured live, `--locale=C` oracle
// container:
//
//	SELECT ABS(x) FROM (VALUES (-2147483648::int4)) v(x)  -> 22003 integer out of range
//	SELECT ABS(x) FROM (VALUES (-9223372036854775808::int8)) v(x)
//	                                                      -> 22003 bigint out of range
//
// No fixture in this package could ask that question. `i32wide` tops out at
// ±2 000 000 000 — a fifth of the way from the int4 floor — and `bigsum`'s
// column is chosen so its SUM overflows while every VALUE fits. The defect
// this fixture exists for is in the VALUE and not in an accumulator, so a
// fixture whose values all fit cannot see it: `ABS(w)` over i32wide is
// 2 000 000 000 on every path, right on all of them.
//
// The second and third rows are the boundary. Row 2 holds min+1, where the
// absolute value DOES fit and must answer — a rule that refused the whole
// negative half would pass a gate with only row 1 in it. Row 3 is NULL, so
// the refusal cannot come from the null mask instead of the value.
const iminTable = "intmin"

func iminSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "i64", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func iminData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "i32": int32(-2147483648), "i64": int64(-9223372036854775808)},
		{"id": int64(2), "i32": int32(-2147483647), "i64": int64(-9223372036854775807)},
		{"id": int64(3), "i32": nil, "i64": nil},
	}
}
