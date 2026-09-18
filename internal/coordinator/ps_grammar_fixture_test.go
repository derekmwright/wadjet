// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import "github.com/derekmwright/wadjet/internal/storage/parquet"

// The ARC PS fixture: two relations that share EXACTLY the join key's name and
// nothing else.
//
// That is the whole point of it. Every other two-relation fixture in this
// package — zzp/zzj, lat_ord/lat_item — shares a second column name, so a
// merge measured over one of those pairs is also measuring what a reference
// to the SHARED name binds, and the two questions cannot be told apart in one
// cell. This pair separates them.
//
// The stronger claim this comment used to make — that such a reference "binds
// whichever side the plan put it on" (#706 read through a star) — is false at
// 563aa517 and arc SR measured it out: over zzp/zzj, whose two `d92` columns
// differ in VALUE and in SCALE, a bare star publishes each arm's own column
// on all five arms, with `ON` and with `USING` (#1177).
//
// The rows are the ones arc PS measured against live PostgreSQL 17.11: one id
// only on the left, one on both, one only on the right, so an INNER, LEFT,
// RIGHT and FULL join over the same pair each answer a different row set and
// the FULL join's merged key is the NULL-extended side's for one row.
const (
	psaTable = "psa"
	psbTable = "psb"
	pscTable = "psc"
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

// psc is the DISCRIMINATING window fixture: DUPLICATE keys on the right arm,
// and no key the left arm matches.
//
// It exists because a window key over `psb RIGHT JOIN psa USING (id)` cannot
// tell a right binding from a wrong one — `psa.id` is 1, 2 and `psb.id` is
// NULL, 2, and BOTH give partitions of size one, so `COUNT(*) OVER (PARTITION
// BY id)` answers 1 either way. Over `psb RIGHT JOIN psc USING (id)` the
// merged key partitions {5,5} and {6} while the left arm's is one partition of
// three NULLs, and the two answers are 2,2,1 against 3,3,3. That is the
// fixture the round-2 review's B1-r2 needed and this arc's round-2 table did
// not have (review round 2).
func pscSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c", Type: parquet.TypeInt64, Nullable: true},
	}}
}

func pscData() []map[string]any {
	return []map[string]any{
		{"id": int64(5), "c": int64(50)},
		{"id": int64(5), "c": int64(51)},
		{"id": int64(6), "c": int64(60)},
	}
}
