package typematrix

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The #589 fixture: the parquet-inexpressible types INSIDE containers.
//
// Parquet has no annotation for IPv6 or UUID and spells CIDR as plain UTF8
// text, so all three survive a round trip only because the writer stamps the
// declared schema into the footer and the reader overlays it back. That
// overlay used to stop at the top level, so the identical value inside a ROW,
// an ARRAY or a MAP recovered as STRING: the row reader boxed sixteen intact
// bytes as a Go string, batch.Vector.SetValue handed the string to
// net.ParseIP, and the value read back as the EMPTY STRING. Silently — "" is
// indistinguishable from a real empty value.
//
// The main matrix cannot see this. Its container columns (c_arr, c_row,
// c_rownest, c_map) carry STRING and INT64 leaves only, which are exactly the
// types parquet CAN annotate, so no gate built on it has ever put one of the
// nine below a container. This fixture is that gap: the same value written to
// a top-level column AND into every container position, so the flat column is
// the anchor and any container that disagrees with it has lost the value.
//
// Anchoring on the flat column rather than on a literal is deliberate. A
// differential between two engines can agree while BOTH are wrong; a
// comparison against the position the overlay always covered cannot.

// NestedDeclared is the fixture table name, and NestedDeclaredRows its size —
// past 2×DefaultBatchSize is unnecessary here (the batch-reuse question is
// the main matrix's), but several parquet row groups is not.
const (
	NestedDeclared     = "typemx_ndecl"
	NestedDeclaredRows = 600
)

func ndeclIPv6(i int) string { return fmt.Sprintf("2001:db8::%x", i) }
func ndeclUUID(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012x", i) }

// ndeclCIDR cycles a canonical network, a HOST-BEARING prefix and a /32 host
// route, the spellings #492 and #546 turn on — inside a container this time.
func ndeclCIDR(i int) string {
	switch i % 3 {
	case 0:
		return fmt.Sprintf("10.%d.0.0/16", i%256)
	case 1:
		return fmt.Sprintf("10.%d.0.%d/16", i%256, 1+i%200)
	default:
		return fmt.Sprintf("172.16.%d.%d/32", (i/256)%256, i%256)
	}
}

// NestedDeclaredSchema writes IPv6, UUID and CIDR as a top-level column AND
// as a ROW field, a nested ROW field, an ARRAY element and a MAP value.
func NestedDeclaredSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
		{Name: "flat_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "flat_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "flat_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "f_ipv6", Type: parquet.TypeIPv6, Nullable: true},
			{Name: "f_uuid", Type: parquet.TypeUUID, Nullable: true},
			{Name: "f_cidr", Type: parquet.TypeCIDR, Nullable: true},
			{Name: "f_nested", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
				{Name: "n_ipv6", Type: parquet.TypeIPv6, Nullable: true},
				{Name: "n_uuid", Type: parquet.TypeUUID, Nullable: true},
			}},
		}},
		{Name: "arr_ipv6", Type: parquet.TypeArray, Nullable: true, ElementType: &parquet.Column{
			Name: "element", Type: parquet.TypeIPv6, Nullable: true}},
		{Name: "arr_uuid", Type: parquet.TypeArray, Nullable: true, ElementType: &parquet.Column{
			Name: "element", Type: parquet.TypeUUID, Nullable: true}},
		{Name: "m_ipv6", Type: parquet.TypeMap, Nullable: true, ElementType: &parquet.Column{
			Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeIPv6, Nullable: true},
			}}},
	}}
}

// NestedDeclaredData puts row i's value in every position at once, so the
// flat column and every container leaf beside it must agree.
//
// Two NULL states on different strides: every fifth row NULLs the containers
// themselves and every seventh gives a PRESENT container a NULL leaf. The
// flat columns NULL on the same rows, so the anchor holds in both — a reader
// that turned a NULL into "" fails the comparison exactly as one that turned
// a value into "" does.
func NestedDeclaredData(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		ip, uu, cd := ndeclIPv6(i), ndeclUUID(i), ndeclCIDR(i)
		r := map[string]any{
			"id": int64(i), "g": int32(i % Groups),
			"flat_ipv6": ip, "flat_uuid": uu, "flat_cidr": cd,
		}
		switch {
		case i%5 == 4:
			r["rw"], r["arr_ipv6"], r["arr_uuid"], r["m_ipv6"] = nil, nil, nil, nil
			r["flat_ipv6"], r["flat_uuid"], r["flat_cidr"] = nil, nil, nil
		case i%7 == 6:
			r["rw"] = map[string]any{"f_ipv6": nil, "f_uuid": nil, "f_cidr": nil,
				"f_nested": map[string]any{"n_ipv6": nil, "n_uuid": nil}}
			r["arr_ipv6"] = []any{nil}
			r["arr_uuid"] = []any{nil}
			r["m_ipv6"] = map[string]any{"k": nil}
			r["flat_ipv6"], r["flat_uuid"], r["flat_cidr"] = nil, nil, nil
		default:
			r["rw"] = map[string]any{"f_ipv6": ip, "f_uuid": uu, "f_cidr": cd,
				"f_nested": map[string]any{"n_ipv6": ip, "n_uuid": uu}}
			r["arr_ipv6"] = []any{ip}
			r["arr_uuid"] = []any{uu}
			r["m_ipv6"] = map[string]any{"k": ip}
		}
		rows[i] = r
	}
	return rows
}

// NestedDeclaredCorpus is the shape corpus: every way a query can reach one
// of these values — the whole container, a field path into it, an element,
// and the consumers that RE-KEY or RE-ORDER it (GROUP BY, ORDER BY, WHERE).
//
// Two shapes are deliberately absent, and neither is this fixture's to fix:
//
//   - `GROUP BY rw.f_ipv6` and `SELECT DISTINCT rw.f_ipv6` fail on BOTH paths
//     with "GROUP BY key is not a column of its input". A field PATH cannot be
//     a grouping key today (#568's family) whatever type the field has.
//   - `m['k']` answers NULL for EVERY map, MAP(STRING,INT64) included: the
//     engine materializes a MAP as an ARRAY of key/value ROWs, so element_at's
//     string-key arm never sees a Go map to look the key up in.
//   - a QUALIFIED field path (`a.rw.f_uuid`) does not parse — "syntax error at
//     or near \".\"" — so the join entries key on the flat column and CARRY the
//     container as payload instead.
func NestedDeclaredCorpus() []Query {
	t := NestedDeclared
	q := func(name, sql string) Query {
		return Query{Name: name, SQL: sql, Mode: oracle.CmpOrdered}
	}
	return []Query{
		q("ndecl_select_flat", "SELECT id, flat_ipv6, flat_uuid, flat_cidr FROM "+t+" ORDER BY id"),
		q("ndecl_select_row", "SELECT id, rw FROM "+t+" ORDER BY id"),
		q("ndecl_select_row_field", "SELECT id, rw.f_ipv6, rw.f_uuid, rw.f_cidr FROM "+t+" ORDER BY id"),
		q("ndecl_select_nested_row_field", "SELECT id, rw.f_nested FROM "+t+" ORDER BY id"),
		q("ndecl_select_array_ipv6", "SELECT id, arr_ipv6 FROM "+t+" ORDER BY id"),
		q("ndecl_select_array_uuid", "SELECT id, arr_uuid FROM "+t+" ORDER BY id"),
		q("ndecl_select_array_element", "SELECT id, arr_uuid[1] FROM "+t+" ORDER BY id"),
		q("ndecl_select_map", "SELECT id, m_ipv6 FROM "+t+" ORDER BY id"),
		q("ndecl_where_row_field", "SELECT id FROM "+t+" WHERE rw.f_ipv6 = '2001:db8::11' ORDER BY id"),
		q("ndecl_where_flat", "SELECT id FROM "+t+" WHERE flat_ipv6 = '2001:db8::11' ORDER BY id"),
		q("ndecl_orderby_row_field", "SELECT id FROM "+t+" ORDER BY rw.f_uuid DESC, id DESC"),
		q("ndecl_orderby_row", "SELECT id FROM "+t+" ORDER BY rw DESC, id DESC"),
		q("ndecl_groupby_row", "SELECT rw, COUNT(*) AS n FROM "+t+" GROUP BY rw ORDER BY 1"),
		q("ndecl_groupby_array", "SELECT arr_uuid, COUNT(*) AS n FROM "+t+" GROUP BY arr_uuid ORDER BY 1"),
		q("ndecl_groupby_map", "SELECT m_ipv6, COUNT(*) AS n FROM "+t+" GROUP BY m_ipv6 ORDER BY 1"),
		q("ndecl_count_row_by_group", "SELECT g, COUNT(rw) AS n FROM "+t+" GROUP BY g ORDER BY g"),
		q("ndecl_minmax_flat", "SELECT MIN(flat_ipv6) AS lo, MAX(flat_uuid) AS hi FROM "+t),
		q("ndecl_where_row_field_not_null", "SELECT id FROM "+t+" WHERE rw.f_uuid IS NOT NULL ORDER BY id"),
		// A container carried across a JOIN payload — the path that
		// serializes it for a shuffle on the distributed arm.
		q("ndecl_join_carry_row",
			"SELECT a.id, b.rw FROM "+t+" a JOIN "+t+" b ON a.flat_uuid = b.flat_uuid ORDER BY a.id"),
		q("ndecl_join_carry_array",
			"SELECT a.id, b.arr_uuid FROM "+t+" a JOIN "+t+" b ON a.id = b.id ORDER BY a.id"),
		{Name: "ndecl_union_all_row", Mode: oracle.CmpUnordered,
			SQL: "SELECT id, rw FROM " + t + " UNION ALL SELECT id, rw FROM " + t},
	}
}
