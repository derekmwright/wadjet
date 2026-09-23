// SPDX-License-Identifier: MIT

package sysrows

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/syscatalog"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// snapshot is the catalog one statement reads: every relation this identity
// may see — its own tables and the system relations — with their columns.
// It is taken once per catalog scan, when the scan starts, so a statement
// that joins two catalog relations reads two snapshots of one catalog; the
// storage catalog does not change under a running statement for any reason
// a client could observe (DDL is a separate statement).
type snapshot struct {
	user    string
	session syscatalog.Session
	rels    []*relInfo
}

// relInfo is one relation as the catalog describes it.
type relInfo struct {
	schema string
	nsOID  int64
	name   string
	oid    int64
	kind   string // "r" or "v"
	user   bool   // a user table, not a system relation
	cols   []colInfo
}

// colInfo is one column: its attnum is its position in the relation's
// DECLARATION, so a column hidden from this identity leaves a gap rather than
// renumbering the ones after it (PostgreSQL's information_schema does the
// same for a column without privileges).
type colInfo struct {
	attnum  int32
	name    string
	col     parquet.Column
	info    syscatalog.TypeInfo
	notNull bool
}

// takeSnapshot reads the storage catalog through the identity's view.
func takeSnapshot(ctx context.Context, cat *catalog.Catalog, rel *syscatalog.Relation) (*snapshot, error) {
	access, ok := syscatalog.AccessFromContext(ctx)
	if !ok {
		// Every SELECT door installs the view where it authorizes the
		// statement's table functions. A catalog scan without one was
		// reached by a path that made no access decision, and answering it
		// would publish every relation to whoever asked — so it refuses.
		return nil, sqlerr.New("42501",
			"permission denied for %s.%s: no access decision was made for this statement",
			rel.Schema, rel.Name)
	}
	user := access.User
	if user == "" {
		user = expr.SessionUser
	}
	s := &snapshot{user: user, session: syscatalog.SessionFromContext(ctx)}

	var tables []string
	if cat != nil {
		names, err := cat.ListTables(ctx)
		if err != nil {
			return nil, fmt.Errorf("reading the catalog for %s.%s: %w", rel.Schema, rel.Name, err)
		}
		tables = append(tables, names...)
	}
	if access.VisibleTables != nil {
		tables = access.VisibleTables(ctx, tables)
	}
	sort.Strings(tables)
	for _, name := range tables {
		meta, err := cat.GetTable(ctx, name)
		if err != nil || meta == nil {
			// Dropped between the listing and the read: not in this
			// snapshot, as it would not be a moment later.
			continue
		}
		var denied map[string]bool
		if access.DeniedColumns != nil {
			denied = access.DeniedColumns(ctx, name)
		}
		ri := &relInfo{schema: "public", nsOID: syscatalog.NamespacePublic, name: name,
			oid: syscatalog.ObjectOID(name), kind: "r", user: true}
		for i, c := range meta.Schema.Columns {
			if denied[strings.ToLower(c.Name)] {
				continue
			}
			ri.cols = append(ri.cols, colInfo{attnum: int32(i + 1), name: c.Name, col: c,
				info: syscatalog.TypeOf(c), notNull: !c.Nullable})
		}
		s.rels = append(s.rels, ri)
	}
	for _, sr := range syscatalog.Relations() {
		ns := syscatalog.NamespacePgCatalog
		if sr.Schema == syscatalog.SchemaInformationSchema {
			ns = syscatalog.NamespaceInformationSchema
		}
		ri := &relInfo{schema: sr.Schema, nsOID: ns, name: sr.Name, oid: sr.OID, kind: string(sr.Kind)}
		for i, c := range sr.Columns {
			ri.cols = append(ri.cols, colInfo{attnum: int32(i + 1), name: c.Name, col: c,
				info: syscatalog.TypeOf(c), notNull: sr.NotNull[i]})
		}
		s.rels = append(s.rels, ri)
	}
	return s, nil
}

// superuserOnly are the relations PostgreSQL grants no SELECT on to an
// ordinary role. This server's one role is not a superuser (pg_roles says
// so), so reading one is PostgreSQL's own refusal rather than an answer.
var superuserOnly = map[string]string{
	"pg_catalog.pg_authid":       "table",
	"pg_catalog.pg_shadow":       "view",
	"pg_catalog.pg_statistic":    "table",
	"pg_catalog.pg_user_mapping": "table",
	"pg_catalog.pg_largeobject":  "table",
}

// rowsFor materializes one system relation's rows. A relation with no entry
// here is one this server has no objects for, and has no rows.
func rowsFor(ctx context.Context, cat *catalog.Catalog, rel *syscatalog.Relation) ([]map[string]any, error) {
	if kind, ok := superuserOnly[rel.FuncName()]; ok {
		return nil, sqlerr.New("42501", "permission denied for %s %s", kind, rel.Name)
	}
	if rel.FuncName() == "information_schema.alerts" {
		return alertRows(ctx, cat, rel)
	}
	pop, ok := populations[rel.FuncName()]
	if !ok {
		return nil, nil
	}
	s, err := takeSnapshot(ctx, cat, rel)
	if err != nil {
		return nil, err
	}
	return pop(s), nil
}

var populations = map[string]func(*snapshot) []map[string]any{
	"pg_catalog.pg_namespace":              pgNamespace,
	"pg_catalog.pg_class":                  pgClass,
	"pg_catalog.pg_attribute":              pgAttribute,
	"pg_catalog.pg_type":                   pgType,
	"pg_catalog.pg_database":               pgDatabase,
	"pg_catalog.pg_tables":                 pgTables,
	"pg_catalog.pg_views":                  pgViews,
	"pg_catalog.pg_roles":                  pgRoles,
	"pg_catalog.pg_user":                   pgUser,
	"pg_catalog.pg_stat_ssl":               pgStatSSL,
	"pg_catalog.pg_tablespace":             pgTablespace,
	"pg_catalog.pg_settings":               pgSettings,
	"information_schema.schemata":          isSchemata,
	"information_schema.tables":            isTables,
	"information_schema.columns":           isColumns,
	"information_schema.views":             isViews,
	"information_schema.table_constraints": isTableConstraints,
	"information_schema.check_constraints": isCheckConstraints,
}

func namespaces() []struct {
	oid  int64
	name string
} {
	return []struct {
		oid  int64
		name string
	}{
		{syscatalog.NamespacePgCatalog, syscatalog.SchemaPgCatalog},
		{syscatalog.NamespacePublic, expr.SessionSchema},
		{syscatalog.NamespaceInformationSchema, syscatalog.SchemaInformationSchema},
	}
}

func pgNamespace(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, ns := range namespaces() {
		out = append(out, map[string]any{
			"oid": ns.oid, "nspname": ns.name, "nspowner": syscatalog.BootstrapRoleOID,
		})
	}
	return out
}

func pgClass(s *snapshot) []map[string]any {
	out := make([]map[string]any, 0, len(s.rels))
	for _, r := range s.rels {
		replident := "d"
		if r.kind == "v" {
			replident = "n"
		}
		out = append(out, map[string]any{
			"oid": r.oid, "relname": r.name, "relnamespace": r.nsOID,
			"reltype": int64(0), "reloftype": int64(0), "relowner": syscatalog.BootstrapRoleOID,
			"relam": int64(0), "relfilenode": int64(0), "reltablespace": int64(0),
			"relpages": int32(0), "reltuples": float32(-1), "relallvisible": int32(0),
			"reltoastrelid": int64(0), "relhasindex": false, "relisshared": false,
			"relpersistence": "p", "relkind": r.kind, "relnatts": int32(len(r.cols)),
			"relchecks": int32(0), "relhasrules": false, "relhastriggers": false,
			"relhassubclass": false, "relrowsecurity": false, "relforcerowsecurity": false,
			"relispopulated": true, "relreplident": replident, "relispartition": false,
			"relrewrite": int64(0),
		})
	}
	return out
}

func pgAttribute(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, r := range s.rels {
		for _, c := range r.cols {
			out = append(out, map[string]any{
				"attrelid": r.oid, "attname": c.name, "atttypid": c.info.OID,
				"attlen": int32(c.info.Len), "attnum": c.attnum, "attcacheoff": int32(-1),
				"atttypmod": c.info.Typmod, "attndims": c.info.Ndims, "attbyval": c.info.ByVal,
				"attalign": c.info.Align, "attstorage": c.info.Storage, "attcompression": "",
				"attnotnull": c.notNull, "atthasdef": false, "atthasmissing": false,
				"attidentity": "", "attgenerated": "", "attisdropped": false,
				"attislocal": true, "attinhcount": int32(0), "attcollation": c.info.Collation,
			})
		}
	}
	return out
}

func pgType(s *snapshot) []map[string]any {
	rel, _ := syscatalog.ByFuncName("pg_catalog.pg_type")
	var out []map[string]any
	for _, raw := range syscatalog.PgTypeRows() {
		row := make(map[string]any, len(raw))
		for i, c := range rel.Columns {
			v, ok := raw[c.Name]
			if !ok || v == "" {
				continue // typdefaultbin, typdefault, typacl: NULL for every one
			}
			row[c.Name] = typedValue(c, rel.PgTypes[i], v)
		}
		out = append(out, row)
	}
	return out
}

// typedValue converts one PostgreSQL text rendering to the engine column's
// value.
func typedValue(c parquet.Column, pgType, v string) any {
	switch c.Type {
	case parquet.TypeInt64:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	case parquet.TypeInt32:
		n, _ := strconv.ParseInt(v, 10, 32)
		return int32(n)
	case parquet.TypeBool:
		return v == "t"
	}
	return v
}

func pgDatabase(s *snapshot) []map[string]any {
	return []map[string]any{{
		"oid": syscatalog.ObjectOID(expr.SessionCatalog), "datname": expr.SessionCatalog,
		"datdba": syscatalog.BootstrapRoleOID, "encoding": int32(6), "datlocprovider": "c",
		"datistemplate": false, "datallowconn": true, "dathasloginevt": false,
		"datconnlimit":  int32(-1),
		"dattablespace": int64(1663), "datcollate": "en_US.UTF-8", "datctype": "en_US.UTF-8",
	}}
}

func pgTables(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, r := range s.rels {
		if r.kind != "r" {
			continue
		}
		out = append(out, map[string]any{
			"schemaname": r.schema, "tablename": r.name, "tableowner": s.user,
			"hasindexes": false, "hasrules": false, "hastriggers": false, "rowsecurity": false,
		})
	}
	return out
}

func pgViews(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, r := range s.rels {
		if r.kind != "v" {
			continue
		}
		out = append(out, map[string]any{
			"schemaname": r.schema, "viewname": r.name, "viewowner": s.user,
		})
	}
	return out
}

// pgRoles is the one role: this connection's identity, which is not a
// superuser — authorization here is the policy engine, not a cluster-wide
// bit, and a client asking whether it may do anything it likes is owed the
// honest no.
func pgRoles(s *snapshot) []map[string]any {
	return []map[string]any{{
		"rolname": s.user, "rolsuper": false, "rolinherit": true, "rolcreaterole": false,
		"rolcreatedb": false, "rolcanlogin": true, "rolreplication": false,
		"rolconnlimit": int32(-1), "rolpassword": "********", "rolbypassrls": false,
		"oid": syscatalog.BootstrapRoleOID,
	}}
}

func pgUser(s *snapshot) []map[string]any {
	return []map[string]any{{
		"usename": s.user, "usesysid": syscatalog.BootstrapRoleOID, "usecreatedb": false,
		"usesuper": false, "userepl": false, "usebypassrls": false, "passwd": "********",
	}}
}

// pgStatSSL is this connection's row. PostgreSQL lists every backend; a
// statement here sees its own, which is the row a client asks for (pgJDBC:
// `WHERE pid = pg_backend_pid()`, the same pid that function reports).
func pgStatSSL(s *snapshot) []map[string]any {
	row := map[string]any{"pid": int32(os.Getpid()), "ssl": s.session.SSL}
	if s.session.SSL {
		if s.session.SSLVersion != "" {
			row["version"] = s.session.SSLVersion
		}
		if s.session.SSLCipher != "" {
			row["cipher"] = s.session.SSLCipher
		}
	}
	return []map[string]any{row}
}

func pgTablespace(s *snapshot) []map[string]any {
	return []map[string]any{
		{"oid": int64(1663), "spcname": "pg_default", "spcowner": syscatalog.BootstrapRoleOID},
		{"oid": int64(1664), "spcname": "pg_global", "spcowner": syscatalog.BootstrapRoleOID},
	}
}

// pgSettings lists the settings current_setting() answers, with their
// values; the descriptive columns (category, description, bounds) are not
// modelled and are NULL.
func pgSettings(s *snapshot) []map[string]any {
	settings := expr.Settings()
	names := make([]string, 0, len(settings))
	for n := range settings {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]map[string]any, 0, len(names))
	for _, n := range names {
		out = append(out, map[string]any{"name": n, "setting": settings[n]})
	}
	return out
}

func isSchemata(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, ns := range namespaces() {
		out = append(out, map[string]any{
			"catalog_name": expr.SessionCatalog, "schema_name": ns.name, "schema_owner": s.user,
		})
	}
	return out
}

func isTables(s *snapshot) []map[string]any {
	out := make([]map[string]any, 0, len(s.rels))
	for _, r := range s.rels {
		typ, insertable := "BASE TABLE", "YES"
		if r.kind == "v" {
			typ, insertable = "VIEW", "NO"
		}
		out = append(out, map[string]any{
			"table_catalog": expr.SessionCatalog, "table_schema": r.schema,
			"table_name": r.name, "table_type": typ, "is_insertable_into": insertable,
			"is_typed": "NO",
		})
	}
	return out
}

func yesNo(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

func isColumns(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, r := range s.rels {
		for _, c := range r.cols {
			row := map[string]any{
				"table_catalog": expr.SessionCatalog, "table_schema": r.schema,
				"table_name": r.name, "column_name": c.name, "ordinal_position": c.attnum,
				"is_nullable": yesNo(!c.notNull), "data_type": c.info.DataType,
				"udt_catalog": expr.SessionCatalog, "udt_schema": syscatalog.SchemaPgCatalog,
				"udt_name": c.info.Typname, "dtd_identifier": strconv.Itoa(int(c.attnum)),
				"is_self_referencing": "NO", "is_identity": "NO", "identity_cycle": "NO",
				"is_generated": "NEVER", "is_updatable": yesNo(r.kind == "r"),
			}
			for k, v := range map[string]any{
				"numeric_precision":       c.info.NumericPrecision,
				"numeric_precision_radix": c.info.NumericPrecisionRadix,
				"numeric_scale":           c.info.NumericScale,
				"datetime_precision":      c.info.DatetimePrecision,
				"character_octet_length":  c.info.CharOctetLength,
			} {
				if v != nil {
					row[k] = v
				}
			}
			out = append(out, row)
		}
	}
	return out
}

func isViews(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, r := range s.rels {
		if r.kind != "v" {
			continue
		}
		out = append(out, map[string]any{
			"table_catalog": expr.SessionCatalog, "table_schema": r.schema, "table_name": r.name,
			"check_option": "NONE", "is_updatable": "NO", "is_insertable_into": "NO",
			"is_trigger_updatable": "NO", "is_trigger_deletable": "NO",
			"is_trigger_insertable_into": "NO",
		})
	}
	return out
}

// notNullName is the name PostgreSQL 17 gives a NOT NULL column's implicit
// check constraint in information_schema: <nspoid>_<reloid>_<attnum>_not_null.
func notNullName(r *relInfo, c colInfo) string {
	return fmt.Sprintf("%d_%d_%d_not_null", r.nsOID, r.oid, c.attnum)
}

// isTableConstraints lists the one kind of constraint this server has: a
// user table's NOT NULL column, which PostgreSQL reports as a CHECK.
func isTableConstraints(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, r := range s.rels {
		if !r.user {
			continue
		}
		for _, c := range r.cols {
			if !c.notNull {
				continue
			}
			out = append(out, map[string]any{
				"constraint_catalog": expr.SessionCatalog, "constraint_schema": r.schema,
				"constraint_name": notNullName(r, c), "table_catalog": expr.SessionCatalog,
				"table_schema": r.schema, "table_name": r.name, "constraint_type": "CHECK",
				"is_deferrable": "NO", "initially_deferred": "NO", "enforced": "YES",
			})
		}
	}
	return out
}

func isCheckConstraints(s *snapshot) []map[string]any {
	var out []map[string]any
	for _, r := range s.rels {
		if !r.user {
			continue
		}
		for _, c := range r.cols {
			if !c.notNull {
				continue
			}
			out = append(out, map[string]any{
				"constraint_catalog": expr.SessionCatalog, "constraint_schema": r.schema,
				"constraint_name": notNullName(r, c),
				"check_clause":    expr.QuoteIdent(c.name) + " IS NOT NULL",
			})
		}
	}
	return out
}

// alertRows lists the alerts CREATE ALERT defined — the rows the canned
// information_schema.alerts answer carried, as a relation. It shows no more
// than that answer did: the alert's schedule, destination and state, not its
// query or its definer.
func alertRows(ctx context.Context, cat *catalog.Catalog, rel *syscatalog.Relation) ([]map[string]any, error) {
	if _, ok := syscatalog.AccessFromContext(ctx); !ok {
		return nil, sqlerr.New("42501",
			"permission denied for %s.%s: no access decision was made for this statement",
			rel.Schema, rel.Name)
	}
	if cat == nil {
		return nil, nil
	}
	alerts, err := cat.ListAlerts(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading alerts: %w", err)
	}
	out := make([]map[string]any, 0, len(alerts))
	for _, a := range alerts {
		row := map[string]any{
			"name": a.Name, "interval_seconds": a.IntervalSeconds, "enabled": a.Enabled,
			"webhook_url": a.WebhookURL, "insert_into_table": a.InsertIntoTable,
		}
		if !a.LastEvaluatedAt.IsZero() {
			row["last_evaluated_at"] = a.LastEvaluatedAt.UTC().UnixMilli()
		}
		out = append(out, row)
	}
	return out, nil
}
