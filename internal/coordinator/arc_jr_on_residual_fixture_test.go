// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// THE ARC JR FIXTURE — an outer join's ON residual, on five arms.
//
// Three relations of one shape, so every join kind can be spelled over the
// same two sides and the empty-side cells differ only in which table the
// alias names. The ROWS are chosen so that one query exercises every match
// disposition at once, rather than needing a fixture per disposition:
//
//	l.id 1, 2   duplicate key 1 on the probe, against 101/102 on the build:
//	            one probe row's chain can be PARTIALLY accepted.
//	l.id 3      key 2, one candidate (103) — accepted or rejected whole.
//	l.id 4      key 3, NO candidate at all, and a NULL `n`.
//	l.id 5      NULL key: never matches, so it is padded on every LEFT/FULL.
//	l.id 6      NULL `s`: a residual over it is UNKNOWN, which REJECTS, and
//	            the probe row is then padded — not dropped.
//	r.id 105    key 5, no probe partner: the RIGHT/FULL unmatched flush.
//	r.id 106    NULL key and NULL `s`.
//
// jr_e is empty, and is the empty BUILD side and the empty PROBE side of the
// same query text.
//
// PostgreSQL 17.11's answers for every cell were taken from a
// postgres:17-alpine container standing alone (`--locale=C`, text columns
// `COLLATE "C"`, since wadjet compares strings by bytes), loaded with exactly
// these rows; the commands and answers are in the arc's pg_answers.tsv.

const (
	jrProbeTable = "jr_l"
	jrBuildTable = "jr_r"
	jrEmptyTable = "jr_e"
)

func jrSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func jrRow(id int64, k any, s any, n any) map[string]any {
	return map[string]any{"id": id, "k": k, "s": s, "n": n}
}

func jrProbeData() []map[string]any {
	return []map[string]any{
		jrRow(1, int64(1), "alpha", int64(10)),
		jrRow(2, int64(1), "beta", int64(20)),
		jrRow(3, int64(2), "gamma", int64(30)),
		jrRow(4, int64(3), "delta", nil),
		jrRow(5, nil, "eps", int64(50)),
		jrRow(6, int64(4), nil, int64(60)),
	}
}

func jrBuildData() []map[string]any {
	return []map[string]any{
		jrRow(101, int64(1), "alpha", int64(10)),
		jrRow(102, int64(1), "ALPHA", int64(15)),
		jrRow(103, int64(2), "gamma2", int64(30)),
		jrRow(104, int64(4), "zeta", int64(61)),
		jrRow(105, int64(5), "omega", int64(70)),
		jrRow(106, nil, nil, nil),
	}
}
