// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The ARC DC fixture: four relations built so that WHERE a subquery's outer
// reference sits decides the answer.
//
// It rides in tmdTables() for the reason every fixture there does — only the
// arc-DC gate names these tables, and no type-matrix corpus entry does. Neither
// the type matrix nor the LATERAL fixture can stand in for it:
//
//   - lat_ord/lat_item have no NULL anywhere, so no cell over them can say what
//     a NULL outer key, a NULL inner key or NOT IN's three-valued answer does;
//   - lat_item's order_id has no key present TWICE with different payloads, so a
//     semi join's build side cannot be told from a deduplicated one;
//   - dc_in holds a key (5) the join partner dc_side does not, which is what
//     makes an inner JOIN inside the body drop a row and a LEFT JOIN pad one —
//     the difference an outer reference written in the ON changes.
//
// dc_out.total and dc_in.amt are chosen so an OUTER-ONLY condition (o.total >
// 100) selects a DIFFERENT set of outer rows from any inner condition, and so
// that stripping its qualifier — reading `total > 100` against the inner
// relation, which has no `total` — cannot silently coincide with the right
// answer.
const (
	dcOutTable  = "dc_out"
	dcInTable   = "dc_in"
	dcSideTable = "dc_side"
	dcNulTable  = "dc_nul"
)

func dcOutSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "grp", Type: parquet.TypeInt64},
		{Name: "total", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func dcOutData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "grp": int64(10), "total": int64(100)},
		{"id": int64(2), "grp": int64(10), "total": int64(200)},
		{"id": int64(3), "grp": int64(20), "total": int64(50)},
		{"id": int64(9), "grp": int64(30), "total": int64(10)},
		{"id": nil, "grp": int64(20), "total": int64(300)},
	}
}

func dcInSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "tag", Type: parquet.TypeInt64},
		{Name: "amt", Type: parquet.TypeInt64, Nullable: true},
	}}
}

// dcInData holds k=1 TWICE with different amounts: a build side that keeps both
// answers differently from one that keeps either.
func dcInData() []map[string]any {
	return []map[string]any{
		{"k": int64(1), "tag": int64(10), "amt": int64(50)},
		{"k": int64(1), "tag": int64(10), "amt": int64(150)},
		{"k": int64(2), "tag": int64(20), "amt": int64(250)},
		{"k": int64(5), "tag": int64(30), "amt": int64(20)},
	}
}

// dcSideSchema carries an `id` column on purpose: the enclosing join
// `dc_out o JOIN dc_side s ON s.j = o.id` then emits TWO columns named `id`,
// which is the probe-side name collision ADR-0021 §1p is about. dc_side has no
// j=5, so b.k=5 finds no partner.
func dcSideSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "j", Type: parquet.TypeInt64, Nullable: true},
		{Name: "amt2", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func dcSideData() []map[string]any {
	return []map[string]any{
		{"id": int64(7), "j": int64(1), "amt2": int64(60)},
		{"id": int64(8), "j": int64(2), "amt2": int64(220)},
		{"id": int64(9), "j": int64(9), "amt2": int64(400)},
	}
}

// dcNul is the NULL-in-the-inner-key relation. NOT IN over it is UNKNOWN for
// every probe that does not match, which is the one thing an anti join cannot
// express (ADR-0021 §1f).
func dcNulSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64, Nullable: true},
		{Name: "amt", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func dcNulData() []map[string]any {
	return []map[string]any{
		{"k": int64(1), "amt": int64(10)},
		{"k": nil, "amt": int64(20)},
	}
}
