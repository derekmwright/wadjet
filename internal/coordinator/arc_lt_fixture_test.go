// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ARC LT fixture: an outer relation and an inner relation built so that a
// body evaluated ONCE over the whole inner relation answers differently from
// one evaluated PER OUTER ROW, in every shape the seam table writes.
//
// It rides in tmdTables() for the reason every fixture there does. The LATERAL
// fixture beside it cannot stand in, because #1019's family turns on things
// lat_ord/lat_item do not have:
//
//   - lt_o holds key 1 TWICE (ids 1 and 2). A bound applied per outer row
//     yields each of them its own top-N; a DISTINCT pushed above the join
//     would collapse the pair. A rewrite that is right per KEY and wrong per
//     ROW answers the same on lat_ord, where every key is one row.
//   - lt_i holds a TIE (v=10 twice under key 1), so an `ORDER BY v LIMIT 1`
//     that projects v is deterministic on both engines while one that
//     projects id is not — ADR-0013's class, kept out of the wants on purpose.
//   - lt_o has a key with NO inner row (3) and a NULL key (row 5); lt_i has a
//     NULL key (row 7). A LEFT lateral pads the first two; an equality never
//     matches the third.
//   - both relations publish `id`, so a lifted predicate over `i.id` names a
//     column the enclosing relation contests (#1130's shape).
//   - `total` and `v` are chosen so an inequality correlation (`i.v >
//     o.total`) selects a different inner subset for every outer row, which
//     is what makes "per outer row" and "once over the relation" differ under
//     a bound.
const (
	ltOTable = "lt_o"
	ltITable = "lt_i"
)

func ltOSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "total", Type: parquet.TypeInt64},
	}}
}

func ltOData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "k": int64(1), "total": int64(15)},
		{"id": int64(2), "k": int64(1), "total": int64(25)},
		{"id": int64(3), "k": int64(2), "total": int64(35)},
		{"id": int64(4), "k": int64(3), "total": int64(10)},
		{"id": int64(5), "k": nil, "total": int64(5)},
	}
}

func ltISchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64},
		{Name: "tag", Type: parquet.TypeString},
	}}
}

func ltIData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "k": int64(1), "v": int64(10), "tag": "a"},
		{"id": int64(2), "k": int64(1), "v": int64(10), "tag": "b"},
		{"id": int64(3), "k": int64(1), "v": int64(30), "tag": "a"},
		{"id": int64(4), "k": int64(2), "v": int64(20), "tag": "x"},
		{"id": int64(5), "k": int64(2), "v": int64(40), "tag": "x"},
		{"id": int64(6), "k": int64(2), "v": int64(60), "tag": "y"},
		{"id": int64(7), "k": nil, "v": int64(70), "tag": "n"},
	}
}
