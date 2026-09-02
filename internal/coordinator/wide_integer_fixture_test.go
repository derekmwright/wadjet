package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The WIDE-INTEGER fixture (#784): a BIGINT column whose every value is past
// 2^53 and whose SUM is past 2^63.
//
// It exists because neither existing fixture can tell the right rule from the
// wrong one. typemx's c_i64 tops out near 5e9, so a float64 accumulator holds
// every value and every partial sum EXACTLY and answers correctly — the
// fixture passes for the wrong reason (correctness-fix protocol rule 2). What
// distinguishes the rules is a value a double cannot name and a total an int64
// cannot hold.
//
// The eight rows are chosen so that:
//
//   - every non-NULL value is at or past 2^53, so a float64 accumulation drops
//     integer digits (2^53+1 is the canonical one: a double rounds it to 2^53);
//   - the total is EXACTLY 2^64, so an int64 accumulator wraps to 0 — the
//     loudest possible reading of "the wrapped total is a different number
//     wearing the right type" (ADR-0024 item 4);
//   - both signs appear, so a rule that only works for non-negative values
//     cannot pass;
//   - one row is NULL, so the count and the AVG denominator are not the row
//     count;
//   - two groups split the total unevenly, so the grouped and ungrouped forms
//     are different questions.
//
// PostgreSQL 17 over these rows (oracle container, --locale=C):
//
//	sum(b)                    18446744073709551616   numeric
//	avg(b)                    2635249153387078802    numeric
//	count(b)                  7
//	g=0  sum 9232379236109516801   avg 3077459745369838934   count 3
//	g=1  sum 9214364837600034815   avg 2303591209400008704   count 4
//	sum(g)                    4                      bigint
//	min(b) -9007199254740993   max(b) 9223372036854775807
//
// The AVG renderings above are PostgreSQL's own division scale at work: the
// quotient already carries 19 integer digits, so `select_div_scale` asks for no
// fraction digits at all and ROUNDS. Over typemx's smaller values the same
// function renders 16. That magnitude dependence is exactly what ADR-0024
// declined to adopt, so wadjet answers at the fixed batch.AvgScale(0) = 4 and
// the two agree to min(scale) — ADR-0012 item 9's class.
const bsumTable = "bigsum"

func bsumSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func bsumData() []map[string]any {
	src := []struct {
		id   int64
		g    int32
		b    int64
		null bool
	}{
		{id: 1, g: 0, b: 9007199254740993},    // 2^53 + 1: no double names it
		{id: 2, g: 0, b: 4611686018427387904}, // 2^62
		{id: 3, g: 0, b: 4611686018427387904}, // 2^62 again: the pair is 2^63
		{id: 4, g: 1, b: -9007199254740993},   // the negative twin
		{id: 5, g: 1, b: 9223372036854775807}, // int64 max
		{id: 6, g: 1, b: 1},                   // the +1 that pushes the total past int64
		{id: 7, g: 0, null: true},             // NULL: not part of any aggregate's input
		{id: 8, g: 1, b: 0},                   // zero contributes to the count, not the sum
	}
	rows := make([]map[string]any, 0, len(src))
	for _, r := range src {
		m := map[string]any{"id": r.id, "g": r.g}
		if !r.null {
			m["b"] = r.b
		}
		rows = append(rows, m)
	}
	return rows
}
