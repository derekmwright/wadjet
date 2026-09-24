// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The LT fixture distinguishes a per-outer-row body from one global body.
// Duplicate outer keys preserve multiplicity; tied inner values exercise
// partial ordering: v=10 twice under key 1 is deliberate. Under ORDER BY v
// LIMIT 1, wants project v, never id (ADR-0013 nondeterminism).
// Unmatched and NULL keys exercise padding; shared id
// names exercise lifted-reference ownership. Different totals select
// different inner subsets under inequality correlations (ADR-0021 §1s).
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
