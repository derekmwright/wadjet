package pgwire

import "testing"

// The select-list scanner must survive what real clients send. The DataGrip
// database-listing query is the motivating case: multiline, ::casts, a
// function call with a qualified argument, and a quoted alias — the old
// substring splitter glued the whole FROM clause into the last column name
// and mapped oid::bigint to nothing ("Bad value for type int : f" in
// pgJDBC as the row shifted).
func TestSelectItemsRealClientShapes(t *testing.T) {
	dg := "select N.oid::bigint as id,\n" +
		"       datname as name,\n" +
		"       D.description,\n" +
		"       datistemplate as is_template,\n" +
		"       datallowconn as allow_connections,\n" +
		"       pg_catalog.pg_get_userbyid(N.datdba) as \"owner\"\n" +
		"from pg_catalog.pg_database N\n" +
		"  left join pg_catalog.pg_shdescription D on N.oid = D.objoid\n" +
		"order by N.oid"
	items := selectItems(dg)
	wantLabels := []string{"id", "name", "description", "is_template", "allow_connections", "owner"}
	wantExprs := []string{"oid", "name", "description", "is_template", "allow_connections", "pg_get_userbyid(n.datdba)"}
	if len(items) != len(wantLabels) {
		t.Fatalf("items = %d, want %d: %+v", len(items), len(wantLabels), items)
	}
	for i := range items {
		if items[i].label != wantLabels[i] {
			t.Fatalf("label[%d] = %q, want %q", i, items[i].label, wantLabels[i])
		}
	}
	if items[0].expr != "oid" {
		t.Fatalf("cast-stripped expr = %q, want oid", items[0].expr)
	}
	_ = wantExprs

	// Commas inside function calls stay inside their entry.
	items = selectItems("SELECT format_type(a.atttypid, a.atttypmod) AS t, relname FROM pg_class")
	if len(items) != 2 || items[0].label != "t" || items[1].expr != "relname" {
		t.Fatalf("paren commas: %+v", items)
	}
	// CAST(x AS int8) is not an alias split.
	items = selectItems("SELECT CAST(oid AS int8) FROM pg_database")
	if len(items) != 1 || items[0].label == "int8" {
		t.Fatalf("CAST alias confusion: %+v", items)
	}
	// A quoted alias containing FROM does not end the list.
	items = selectItems(`SELECT datname AS "from list", oid FROM pg_database`)
	if len(items) != 2 || items[0].label != "from list" {
		t.Fatalf("quoted FROM alias: %+v", items)
	}
	// End-to-end through the answer builder: the DataGrip query maps.
	ans := catalogRowAnswer(dg, pgDatabaseAttrs(), []string{"oid", "datname"})
	if ans == nil {
		t.Fatal("DataGrip database query declined")
	}
	if len(ans.cols) != 6 || ans.cols[0] != "id" || ans.cols[5] != "owner" {
		t.Fatalf("answer cols = %v", ans.cols)
	}
	row := ans.rows[0]
	if row["id"] == nil || row["name"] == nil {
		t.Fatalf("id/name unmapped: %+v", row)
	}
	if row["is_template"] != false || row["allow_connections"] != true {
		t.Fatalf("bool attrs wrong: %+v", row)
	}
	// The declared OIDs must match the value types: a bool declared as text
	// (25) sends pgJDBC's typed readers through getInt("f") — the 2026-08-18
	// DataGrip "Bad value for type int : f" failure.
	if got := ans.colOID("is_template"); got != 16 {
		t.Fatalf("is_template OID = %d, want 16 (bool)", got)
	}
	if got := ans.colOID("id"); got != 20 {
		t.Fatalf("id OID = %d, want 20 (int8)", got)
	}
	if got := ans.colOID("owner"); got != 25 {
		t.Fatalf("owner (all-NULL) OID = %d, want 25 (text)", got)
	}
	if got := formatPgValue(row["is_template"]); got != "f" {
		t.Fatalf("bool text = %q, want \"f\"", got)
	}
}

// A pg_class listing answers every column the client selected, one row per
// table. The branch used to hardcode a single "relname" column, so a client
// asking for relname and relkind together was described one column and sent
// one — DataGrip reads relkind to tell a table from a view.
func TestCatalogRowsAnswerPgClassShape(t *testing.T) {
	rows := []map[string]any{pgClassAttrs("lineitem"), pgClassAttrs("orders")}
	fallback := []string{"oid", "relname", "relnamespace", "relkind"}

	ans := catalogRowsAnswer(
		"select c.oid, c.relname, c.relkind from pg_catalog.pg_class c where c.relkind in ('r','v')",
		pgClassAttrs(""), rows, fallback)
	if ans == nil {
		t.Fatal("pg_class listing declined")
	}
	if len(ans.cols) != 3 || ans.cols[1] != "relname" || ans.cols[2] != "relkind" {
		t.Fatalf("cols = %v", ans.cols)
	}
	if len(ans.rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(ans.rows))
	}
	if ans.rows[0]["relname"] != "lineitem" || ans.rows[0]["relkind"] != "r" {
		t.Fatalf("row 0 = %+v", ans.rows[0])
	}
	if got := ans.colOID("oid"); got != 20 {
		t.Fatalf("oid column OID = %d, want 20", got)
	}

	// An empty row set still answers with the columns that were asked for:
	// the shape a client was promised cannot depend on how many rows matched.
	empty := catalogRowsAnswer("select relname, relkind from pg_class where relkind = 'r'",
		pgClassAttrs(""), nil, fallback)
	if empty == nil || len(empty.cols) != 2 || len(empty.rows) != 0 {
		t.Fatalf("empty listing = %+v", empty)
	}

	// SELECT * names the relation's own columns.
	star := catalogRowsAnswer("select * from pg_class", pgClassAttrs(""), rows, fallback)
	if star == nil || len(star.cols) != len(fallback) || star.cols[0] != "oid" {
		t.Fatalf("star = %+v", star)
	}

	// A projection of nothing this relation has is declined, not answered.
	if got := catalogRowsAnswer("select count(*) from pg_class", pgClassAttrs(""), rows, fallback); got != nil {
		t.Fatalf("aggregate should be declined, got %+v", got)
	}
}

// A pg_attribute query naming one table through a pg_class join gets that
// table's columns, in the shape it asked for. The branch only looked for an
// attrelid parameter, so DataGrip's relname-join form matched nothing and was
// answered with every table's columns — every table in the tree showed every
// column in the database.
func TestShapedAttributeAnswer(t *testing.T) {
	rows := []map[string]any{
		{"attname": "n_nationkey", "atttypid": 20, "attnum": 1, "attnotnull": false,
			"format_type": "bigint", "table_oid": 42},
		{"attname": "n_name", "atttypid": 25, "attnum": 2, "attnotnull": false,
			"format_type": "text", "table_oid": 42},
	}

	ans := shapedAttributeAnswer(
		"select a.attname, a.atttypid, a.attnotnull, a.attnum from pg_catalog.pg_attribute a "+
			"join pg_catalog.pg_class c on a.attrelid = c.oid where c.relname = 'nation'", rows)
	if ans == nil {
		t.Fatal("plain column list should be shaped")
	}
	if len(ans.cols) != 4 || ans.cols[0] != "attname" || ans.cols[3] != "attnum" {
		t.Fatalf("cols = %v", ans.cols)
	}
	if len(ans.rows) != 2 || ans.rows[1]["attnum"] != 2 {
		t.Fatalf("rows = %+v", ans.rows)
	}
	// Typed OIDs so a driver reads attnum as a number and attnotnull as a bool.
	if got := ans.colOID("attnum"); got != 20 {
		t.Fatalf("attnum OID = %d, want 20", got)
	}
	if got := ans.colOID("attnotnull"); got != 16 {
		t.Fatalf("attnotnull OID = %d, want 16", got)
	}

	// SQLAlchemy's query unpacks a fixed tuple of computed values; reshaping
	// it to per-name attributes would hand back NULLs, so it is declined and
	// keeps the 8-column tuple.
	sqlalchemy := "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), " +
		"(SELECT c.relname FROM pg_catalog.pg_class c WHERE c.oid = a.attrelid) " +
		"FROM pg_catalog.pg_attribute a WHERE a.attrelid = 'users'"
	if got := shapedAttributeAnswer(sqlalchemy, rows); got != nil {
		t.Fatalf("expression list should decline reshaping, got cols %v", got.cols)
	}
	if got := shapedAttributeAnswer("select * from pg_attribute", rows); got != nil {
		t.Fatal("SELECT * should keep the fixed tuple")
	}
}
