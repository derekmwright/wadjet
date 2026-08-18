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
