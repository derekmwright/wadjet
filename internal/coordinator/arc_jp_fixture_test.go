// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ARC JP fixture (#1299): one outer relation and three item relations,
// all four with the SAME column names (id, oid, v), so a join arm told apart from its siblings by
// a column name rather than by its qualifier binds a sibling's column — and
// answers differently, because the data makes every such mix-up visible:
//
//   - jp_o holds oid 1 TWICE (ids 1 and 2), an oid with no item in jp_j (3),
//     and a NULL key; an arm keyed on the wrong relation pairs a different
//     outer row's items.
//   - jp_i, jp_j and jp_k give each key a DIFFERENT item set, and each holds a
//     NULL key and a key no outer row has (9, 8, 4), so LEFT and RIGHT arms
//     pad on both sides.
//   - `v` repeats across keys, so a per-arm filter on `v` keeps rows of more
//     than one key, and one on `id` keeps a different set per arm.
//   - jp_o's `v` equals its `oid`, so the seam's crossName spelling
//     `a.oid = o.v` asks for the same rows as `a.oid = o.oid` — while an item
//     relation's own `v` (5, 6, 7) matches no oid at all, so a `v` bound to
//     the wrong relation answers differently.
const (
	jpOTable = "jp_o"
	jpITable = "jp_i"
	jpJTable = "jp_j"
	jpKTable = "jp_k"
)

// jpOSchema is the item schema plus `k`, a copy of `oid` under a name NO item
// relation has — the key spelling the issue's reproduction wrote.
func jpOSchema() parquet.Schema {
	s := jpItemSchema()
	s.Columns = append(s.Columns, parquet.Column{Name: "k", Type: parquet.TypeInt64, Nullable: true})
	return s
}

func jpOData() []map[string]any {
	rows := jpItemRows(
		[3]any{int64(1), int64(1), int64(1)}, [3]any{int64(2), int64(1), int64(1)},
		[3]any{int64(3), int64(2), int64(2)}, [3]any{int64(4), int64(3), int64(3)},
		[3]any{int64(5), nil, nil})
	for _, r := range rows {
		r["k"] = r["oid"]
	}
	return rows
}

func jpItemSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "oid", Type: parquet.TypeInt64, Nullable: true},
		{Name: "v", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func jpItemRows(rows ...[3]any) []map[string]any {
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		out = append(out, map[string]any{"id": r[0], "oid": r[1], "v": r[2]})
	}
	return out
}

func jpIData() []map[string]any {
	return jpItemRows(
		[3]any{int64(1), int64(1), int64(5)}, [3]any{int64(2), int64(1), int64(6)},
		[3]any{int64(3), int64(2), int64(5)}, [3]any{int64(4), int64(2), int64(7)},
		[3]any{int64(5), int64(3), int64(6)}, [3]any{int64(6), nil, int64(5)},
		[3]any{int64(7), int64(9), int64(7)})
}

func jpJData() []map[string]any {
	return jpItemRows(
		[3]any{int64(1), int64(1), int64(6)}, [3]any{int64(2), int64(2), int64(5)},
		[3]any{int64(3), int64(2), int64(6)}, [3]any{int64(4), int64(4), int64(7)},
		[3]any{int64(5), nil, int64(6)})
}

func jpKData() []map[string]any {
	return jpItemRows(
		[3]any{int64(1), int64(1), int64(7)}, [3]any{int64(2), int64(1), int64(5)},
		[3]any{int64(3), int64(3), int64(6)}, [3]any{int64(4), int64(9), int64(5)})
}
