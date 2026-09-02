package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The WIDE-INT32 fixture (review round 3, F1): an INTEGER column whose SUM
// leaves int32 while every value fits it.
//
// It exists because `bigsum` cannot see this defect and `typemx` cannot either.
// The batch sum kernel was generic over the COLUMN's width — `sumSlice[int32]`
// accumulated a batch in int32 and widened afterwards — so a sum that leaves
// int32 within one batch wrapped before anything looked. typemx's c_i32 tops
// out near 15 000 and its whole column sums to 36 198 630, three orders of
// magnitude inside int32; the wrap needed values near 2^31.
//
// The row path never had the defect (sumRowInt32 widens per row), so the two
// paths answered the same query differently, and the grouped form was right
// while the ungrouped one was wrong — the flat SoA scatter has always used an
// int64 array.
//
// Six rows:
//
//   - four at 2 000 000 000 in two groups, so the ungrouped sum (8e9 over
//     those four) and each group's sum (4e9) both leave int32 and the grouped
//     and ungrouped forms are different questions;
//   - one NEGATIVE of the same magnitude, so a rule that only works for
//     non-negative values cannot pass and the total is not simply the maximum;
//   - one NULL, so the count and the AVG denominator are not the row count.
//
// PostgreSQL 17 over these rows (oracle container `wadjet-pg-oracle`,
// --locale=C), taken from the live server:
//
//	sum(w)                   6000000000            bigint
//	sum(w) where g < 2       8000000000            bigint
//	g=0 4000000000  g=1 4000000000  g=2 -2000000000
//	count(w)                 5
//	avg(w)                   1200000000.00000000   numeric
//	min(w) -2000000000  max(w) 2000000000          integer
//	sum(-w)                  -6000000000           bigint
//	sum(case when w is null then 0 else w end)      6000000000   bigint
//
// `sum(w * 2)` is NOT gated here: PostgreSQL computes `w * 2` in int4 and
// raises `integer out of range`, while wadjet computes every integer
// expression in int64 and answers 12000000000. That is ADR-0024's recorded
// widening divergence, not a value defect, and gating it would assert
// PostgreSQL's error rather than its number.
const i32wideTable = "i32wide"

func i32wideSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "w", Type: parquet.TypeInt32, Nullable: true},
	}}
}

func i32wideData() []map[string]any {
	src := []struct {
		id   int64
		g    int32
		w    int32
		null bool
	}{
		{id: 1, g: 0, w: 2000000000},
		{id: 2, g: 0, w: 2000000000},
		{id: 3, g: 1, w: 2000000000},
		{id: 4, g: 1, w: 2000000000},
		{id: 5, g: 2, w: -2000000000},
		{id: 6, g: 2, null: true},
	}
	rows := make([]map[string]any, 0, len(src))
	for _, r := range src {
		m := map[string]any{"id": r.id, "g": r.g}
		if !r.null {
			m["w"] = r.w
		}
		rows = append(rows, m)
	}
	return rows
}
