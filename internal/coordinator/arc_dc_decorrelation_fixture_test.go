// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The DC fixture separates outer-only predicates from inner predicates.
// NULL keys occur in dc_out and dc_nul; dc_in stays NULL-free so its
// NOT IN cells are not all UNKNOWN. dc_in repeats key 1 with different amounts
// and has key 5 absent from dc_side. These distinguish residual filtering,
// NOT IN null handling and outer-join padding (ADR-0021 §1r).
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
