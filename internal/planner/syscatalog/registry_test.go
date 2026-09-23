// SPDX-License-Identifier: MIT

package syscatalog

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The registry is PostgreSQL 17.11's own relation list, read off its catalog
// (pg17_relations.tsv): 142 pg_catalog tables and views and 69
// information_schema ones, and this server's information_schema.alerts.
func TestTheRegistryIsPostgreSQLsRelationList(t *testing.T) {
	counts := map[string]int{}
	for _, r := range Relations() {
		counts[r.Schema]++
		if len(r.Columns) == 0 || len(r.Columns) != len(r.PgTypes) || len(r.Columns) != len(r.NotNull) {
			t.Errorf("%s: %d columns, %d types, %d not-null flags", r.FuncName(),
				len(r.Columns), len(r.PgTypes), len(r.NotNull))
		}
	}
	if counts[SchemaPgCatalog] != 142 || counts[SchemaInformationSchema] != 70 {
		t.Fatalf("relations per schema = %v, want pg_catalog 142, information_schema 69+alerts", counts)
	}
	for _, want := range []struct {
		name string
		oid  int64
		cols int
	}{
		{"pg_catalog.pg_class", 1259, 33}, {"pg_catalog.pg_attribute", 1249, 26},
		{"pg_catalog.pg_type", 1247, 32}, {"pg_catalog.pg_namespace", 2615, 4},
		{"information_schema.columns", 13405, 44},
	} {
		r, ok := ByFuncName(want.name)
		if !ok || r.OID != want.oid || len(r.Columns) != want.cols {
			t.Errorf("%s: %+v, want oid %d with %d columns", want.name, r, want.oid, want.cols)
		}
		if byOID, ok := ByOID(want.oid); !ok || byOID != r {
			t.Errorf("ByOID(%d) does not find %s", want.oid, want.name)
		}
	}
}

// Resolve is PostgreSQL's name lookup: pg_catalog before every schema for an
// unqualified name, information_schema only when named, a quoted name exact.
func TestResolveFollowsPostgreSQLsSearchPath(t *testing.T) {
	for _, tc := range []struct {
		qual, name string
		want       string
	}{
		{"", "pg_class", "pg_catalog.pg_class"},
		{"pg_catalog", "pg_class", "pg_catalog.pg_class"},
		{"wadjet.pg_catalog", "pg_class", "pg_catalog.pg_class"},
		{"information_schema", "columns", "information_schema.columns"},
		{"", "columns", ""},               // information_schema is not on the path
		{"public", "pg_class", ""},        // a user relation's name
		{"", "PG_CLASS", ""},              // a quoted, unfolded spelling
		{"pg_catalog", "no_such_rel", ""}, // not a system relation
		{"otherdb.pg_catalog", "pg_class", ""},
	} {
		r, ok := Resolve(tc.qual, tc.name, Database)
		got := ""
		if ok {
			got = r.FuncName()
		}
		if got != tc.want {
			t.Errorf("Resolve(%q, %q) = %q, want %q", tc.qual, tc.name, got, tc.want)
		}
	}
}

// TypeOf is the wire's declaration restated for the catalog (the pgwire
// package holds the two equal per engine type); here, the catalog-only facts.
func TestTypeOfDescribesWhatTheWireDeclares(t *testing.T) {
	for _, tc := range []struct {
		col             parquet.Column
		oid             int64
		typname, format string
		typmod          int32
	}{
		{parquet.Column{Type: parquet.TypeInt64}, 20, "int8", "bigint", -1},
		{parquet.Column{Type: parquet.TypeInt32}, 23, "int4", "integer", -1},
		{parquet.Column{Type: parquet.TypeString}, 25, "text", "text", -1},
		{parquet.Column{Type: parquet.TypeFloat64}, 701, "float8", "double precision", -1},
		{parquet.Column{Type: parquet.TypeDecimal, Precision: 10, Scale: 2}, 1700, "numeric", "numeric(10,2)", 655366},
		{parquet.Column{Type: parquet.TypeDecimal}, 1700, "numeric", "numeric", -1},
		{parquet.Column{Type: parquet.TypeTimestamp}, 1114, "timestamp", "timestamp without time zone", -1},
		{parquet.Column{Type: parquet.TypeIPv4}, 25, "text", "text", -1},
		{parquet.Column{Type: parquet.TypePort}, 23, "int4", "integer", -1},
		{parquet.Column{Type: parquet.TypeArray, ElementType: &parquet.Column{Type: parquet.TypeInt64}},
			1016, "_int8", "bigint[]", -1},
		{parquet.Column{Type: parquet.TypeArray, ElementType: &parquet.Column{Type: parquet.TypeRow}},
			25, "text", "text", -1},
	} {
		info := TypeOf(tc.col)
		if info.OID != tc.oid || info.Typname != tc.typname || info.FormatType() != tc.format ||
			info.Typmod != tc.typmod {
			t.Errorf("TypeOf(%v) = oid %d %s %q typmod %d; want %d %s %q %d", tc.col.Type,
				info.OID, info.Typname, info.FormatType(), info.Typmod,
				tc.oid, tc.typname, tc.format, tc.typmod)
		}
	}
}
