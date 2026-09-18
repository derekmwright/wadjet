// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// The ARC PS fixture: two relations that share EXACTLY the join key's name and
// nothing else.
//
// That is the whole point of it. Every other two-relation fixture in this
// package — zzp/zzj, lat_ord/lat_item — shares a second column name, and a
// bare `*` over a join expands to QUALIFIED references, so a reference to a
// name BOTH arms publish binds whichever side the plan put it on (#706's
// standing family, visible with no USING clause at all). A USING merge
// measured over such a pair cannot tell a right merge from a wrong binding.
//
// The rows are the ones arc PS measured against live PostgreSQL 17.11: one id
// only on the left, one on both, one only on the right, so an INNER, LEFT,
// RIGHT and FULL join over the same pair each answer a different row set and
// the FULL join's merged key is the NULL-extended side's for one row.
const (
	psaTable = "psa"
	psbTable = "psb"
)

func psaSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "a", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func psbSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "b", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func psaData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "a": int64(10)},
		{"id": int64(2), "a": int64(20)},
	}
}

func psbData() []map[string]any {
	return []map[string]any{
		{"id": int64(2), "b": int64(200)},
		{"id": int64(3), "b": int64(300)},
	}
}
