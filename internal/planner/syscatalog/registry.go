// SPDX-License-Identifier: MIT

// Package syscatalog is the system catalog as RELATIONS: pg_catalog and
// information_schema, answered by the engine like any other FROM item.
//
// A client — psql's \d family, pgJDBC's DatabaseMetaData, DataGrip, Superset,
// Metabase — reads the catalog before it sends a single query of its own, and
// it reads it with ordinary SQL: WHERE, JOIN, COUNT, ORDER BY, LIMIT. The
// catalog used to be a canned responder in the pgwire layer that matched the
// statement's TEXT and answered a precomputed row set, so every predicate it
// had not been taught to recognise was silently ignored (`WHERE table_name =
// 'nosuch'` listed every column of every table, #1251) and every shape it had
// not been taught to answer came back as the raw rows (`SELECT COUNT(*)`).
// Here a catalog relation is a SOURCE the planner scans: its rows are
// materialized from the storage catalog when the statement runs, and
// everything above the scan is the ordinary engine (ADR-0044).
//
// The relations and their columns are PostgreSQL 17's own, read off a live
// 17.11 catalog into pg17_relations.tsv — every column of every pg_catalog
// table and view and every information_schema view, so a client that selects
// a column this server has no value for gets NULL rather than 42703. Which of
// them carry rows is the population in rows.go; the rest are relations this
// server has no objects for (triggers, constraints, indexes, rules,
// publications, …) and are empty, which for those is the true answer.
package syscatalog

import (
	_ "embed"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The two system schemas and the one user schema. The OIDs are PostgreSQL's:
// pg_catalog and public are fixed at 11 and 2200 by initdb, and
// information_schema's is the one 17.11's initdb assigns (13317).
const (
	// Database is this server's one database — expr.SessionCatalog, which
	// this package cannot import (the expression package imports the parser,
	// which imports this); a test holds the two equal.
	Database = "wadjet"

	SchemaPgCatalog         = "pg_catalog"
	SchemaInformationSchema = "information_schema"

	NamespacePgCatalog         int64 = 11
	NamespacePublic            int64 = 2200
	NamespaceInformationSchema int64 = 13317

	// BootstrapRoleOID is the owner every object reports: PostgreSQL's
	// bootstrap superuser OID, which pg_roles/pg_user name as this
	// connection's identity (there is one role here).
	BootstrapRoleOID int64 = 10
)

// Relation is one system relation: where it lives, what PostgreSQL calls its
// kind, its OID, and its columns as this engine types them.
type Relation struct {
	Schema string
	Name   string
	// Kind is pg_class.relkind: 'r' for a catalog table, 'v' for a view.
	Kind byte
	OID  int64
	// Columns are the relation's columns in PostgreSQL's attnum order, typed
	// by engineColumn from PostgreSQL's own type.
	Columns []parquet.Column
	// PgTypes is PostgreSQL's declared type of each column, parallel to
	// Columns — kept for the one place that needs the original spelling.
	PgTypes []string
	// NotNull is PostgreSQL's attnotnull for each column, parallel to
	// Columns: what pg_attribute and information_schema.columns report.
	NotNull []bool
}

// FuncName is the name the relation travels under once the parser has
// resolved it: a table-function FROM item whose name is the relation's
// schema-qualified name. No user-callable function has a dot in its name, so
// the spelling cannot collide with one.
func (r *Relation) FuncName() string { return r.Schema + "." + r.Name }

// ColumnIndex is the position of the named column, or -1.
func (r *Relation) ColumnIndex(name string) int {
	for i, c := range r.Columns {
		if c.Name == name {
			return i
		}
	}
	return -1
}

//go:embed pg17_relations.tsv
var relationsTSV string

var (
	loadOnce  sync.Once
	relations []*Relation
	byFunc    map[string]*Relation
	byOID     map[int64]*Relation
)

// Relations is every system relation, in (schema, name) order.
func Relations() []*Relation {
	loadOnce.Do(load)
	return relations
}

func load() {
	byFunc = map[string]*Relation{}
	byOID = map[int64]*Relation{}
	for _, line := range strings.Split(strings.TrimSpace(relationsTSV), "\n") {
		f := strings.Split(line, "\t")
		if len(f) != 8 {
			panic(fmt.Sprintf("syscatalog: malformed relation line %q", line))
		}
		schema, name, kind, typ := f[0], f[1], f[2], f[6]
		oid, err := strconv.ParseInt(f[3], 10, 64)
		if err != nil {
			panic(fmt.Sprintf("syscatalog: bad oid in %q", line))
		}
		key := schema + "." + name
		r := byFunc[key]
		if r == nil {
			r = &Relation{Schema: schema, Name: name, Kind: kind[0], OID: oid}
			byFunc[key] = r
			byOID[oid] = r
			relations = append(relations, r)
		}
		r.Columns = append(r.Columns, engineColumn(f[5], typ))
		r.PgTypes = append(r.PgTypes, typ)
		r.NotNull = append(r.NotNull, f[7] == "t")
	}
	// information_schema.alerts is this server's own: the alerts CREATE
	// ALERT defined (it is not a PostgreSQL relation). It is added after the
	// PostgreSQL set so it sorts where information_schema's views do.
	alerts := &Relation{Schema: SchemaInformationSchema, Name: "alerts", Kind: 'v',
		OID: ObjectOID(SchemaInformationSchema + ".alerts")}
	for _, c := range []struct{ name, typ string }{
		{"name", "text"}, {"interval_seconds", "bigint"}, {"enabled", "boolean"},
		{"webhook_url", "text"}, {"insert_into_table", "text"},
		{"last_evaluated_at", "timestamp without time zone"},
	} {
		col := engineColumn(c.name, c.typ)
		if c.typ == "timestamp without time zone" {
			col.Type = parquet.TypeTimestamp
		}
		alerts.Columns = append(alerts.Columns, col)
		alerts.PgTypes = append(alerts.PgTypes, c.typ)
		alerts.NotNull = append(alerts.NotNull, false)
	}
	byFunc[alerts.FuncName()] = alerts
	byOID[alerts.OID] = alerts
	relations = append(relations, alerts)
}

// ByFuncName resolves the name a resolved system relation travels under
// (Relation.FuncName). The match is exact and lower-case: the parser writes
// it, not a client.
func ByFuncName(name string) (*Relation, bool) {
	loadOnce.Do(load)
	r, ok := byFunc[name]
	return r, ok
}

// ByOID resolves a system relation by its OID.
func ByOID(oid int64) (*Relation, bool) {
	loadOnce.Do(load)
	r, ok := byOID[oid]
	return r, ok
}

// Resolve answers which system relation a FROM item names, the way
// PostgreSQL's name lookup does:
//
//   - `pg_catalog.x` and `information_schema.x` name that schema's relation,
//     and so do `<database>.pg_catalog.x` and `<database>.information_schema.x`
//     for this server's one database;
//   - an UNQUALIFIED name is looked up in pg_catalog, which PostgreSQL
//     searches before every schema on the search path — `pg_class` is
//     pg_catalog.pg_class even when public holds a table of that name.
//     information_schema is not on the path, so `columns` alone is never
//     information_schema.columns.
//
// Names are matched EXACTLY: the caller folds an unquoted identifier to lower
// case the way PostgreSQL does and passes a quoted one as written, so
// `"PG_CLASS"` names no system relation, as it names none there. A name this
// returns false for is not a system relation, and resolves as it always did.
func Resolve(qualifier, name, database string) (*Relation, bool) {
	q := qualifier
	if database != "" {
		q = strings.TrimPrefix(q, database+".")
	}
	switch q {
	case "":
		return ByFuncName(SchemaPgCatalog + "." + name)
	case SchemaPgCatalog, SchemaInformationSchema:
		return ByFuncName(q + "." + name)
	}
	return nil, false
}

// IsSystemSchema reports whether a (folded) schema name is one of the two
// system schemas, so a qualified name under one that names no relation there
// is refused as the missing relation it is rather than as an unknown schema.
func IsSystemSchema(qualifier, database string) bool {
	q := qualifier
	if database != "" {
		q = strings.TrimPrefix(q, database+".")
	}
	return q == SchemaPgCatalog || q == SchemaInformationSchema
}

// engineColumn types one catalog column. The engine has no oid, name,
// "char", regproc or domain types; each is carried by the engine type whose
// VALUES and COMPARISONS are the ones a client relies on:
//
//   - an OID (oid, regproc-as-number, xid) is a 64-bit integer, so a join on
//     one — `c.relnamespace = n.oid` — and a literal comparison — `c.oid =
//     '16385'` — are integer comparisons, as they are in PostgreSQL;
//   - name, "char", the reg* renderings and every PostgreSQL-internal text
//     form (pg_node_tree, pg_lsn, …) are text;
//   - smallint and information_schema.cardinal_number are integer, the
//     narrowest integer this engine has;
//   - an array type is an ARRAY of its element, and int2vector/oidvector are
//     the arrays they are.
//
// The declaration a client reads on the wire is then the one every other
// relation of that engine type declares, which is the invariant the catalog
// and the wire must share (the #834 lesson).
func engineColumn(name, pgType string) parquet.Column {
	if strings.HasSuffix(pgType, "[]") {
		elem := engineColumn("element", strings.TrimSuffix(pgType, "[]"))
		return parquet.Column{Name: name, Type: parquet.TypeArray, Nullable: true, ElementType: &elem}
	}
	switch pgType {
	case "int2vector":
		elem := parquet.Column{Name: "element", Type: parquet.TypeInt32, Nullable: true}
		return parquet.Column{Name: name, Type: parquet.TypeArray, Nullable: true, ElementType: &elem}
	case "oidvector":
		elem := parquet.Column{Name: "element", Type: parquet.TypeInt64, Nullable: true}
		return parquet.Column{Name: name, Type: parquet.TypeArray, Nullable: true, ElementType: &elem}
	}
	var t parquet.TypeID
	switch pgType {
	case "oid", "xid", "bigint":
		t = parquet.TypeInt64
	case "smallint", "integer", "information_schema.cardinal_number":
		t = parquet.TypeInt32
	case "boolean":
		t = parquet.TypeBool
	case "real":
		t = parquet.TypeFloat32
	case "double precision", "numeric":
		t = parquet.TypeFloat64
	case "timestamp with time zone", "information_schema.time_stamp":
		t = parquet.TypeTimestamp
	case "bytea":
		t = parquet.TypeBytes
	default:
		// name, text, "char", regproc, regtype, inet, interval, anyarray,
		// pg_node_tree, pg_lsn, pg_ndistinct, pg_dependencies, pg_mcv_list,
		// aclitem, and the information_schema text domains.
		t = parquet.TypeString
	}
	return parquet.Column{Name: name, Type: t, Nullable: true}
}

// ObjectOID is the OID a user object reports: a deterministic hash of its
// name, inside PostgreSQL's OID range and above the 16384 its system objects
// end at. A client that caches an OID and looks it up again finds it; a
// client that holds one in an int4 or an oid column can.
func ObjectOID(name string) int64 {
	const base = 16384
	h := uint32(2166136261) // FNV-1a
	for _, c := range name {
		h = (h ^ uint32(c)) * 16777619
	}
	return base + int64(h%(1<<31-base))
}
