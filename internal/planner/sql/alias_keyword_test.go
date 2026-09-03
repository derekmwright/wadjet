package sql

import "testing"

// TestKeywordAliasAfterAs pins PostgreSQL's rule: writing AS is what removes
// the ambiguity that makes a word reserved, so any keyword is a legal column
// label there. `COUNT(*) AS rows` and `COUNT(x) AS matched` both failed to
// parse with "expected alias after AS", which any BI tool naming an output
// column after a source column named `rows`, `key`, `value` or `end` hits.
func TestKeywordAliasAfterAs(t *testing.T) {
	cases := []struct {
		sql       string
		wantAlias string
	}{
		{"SELECT COUNT(*) AS rows FROM nation", "rows"},
		{"SELECT COUNT(n_name) AS matched FROM nation", "matched"},
		{"SELECT n_name AS value FROM nation", "value"},
		{"SELECT n_name AS key FROM nation", "key"},
		{"SELECT n_name AS end FROM nation", "end"},
		// An unquoted keyword spelling is an unquoted IDENTIFIER, so it
		// folds like one. Measured on PostgreSQL 17: `SELECT 1 AS Desc`
		// publishes `desc` and `SELECT 1 AS "Desc"` publishes `Desc`
		// (#731). Echoing the lexer's uppercased val would still be wrong —
		// that would rename the column to ROWS.
		{"SELECT COUNT(*) AS Rows FROM nation", "rows"},
		{`SELECT COUNT(*) AS "Rows" FROM nation`, "Rows"},
		{"SELECT n_name AS Nm FROM nation", "nm"},
		{`SELECT n_name AS "Nm" FROM nation`, "Nm"},
		// A plain identifier alias is unchanged.
		{"SELECT n_name AS nm FROM nation", "nm"},
	}
	for _, tc := range cases {
		node, err := Parse(tc.sql)
		if err != nil {
			t.Errorf("%s: %v", tc.sql, err)
			continue
		}
		info, err := ExtractSelect(node)
		if err != nil || info == nil {
			t.Errorf("%s: ExtractSelect: %v", tc.sql, err)
			continue
		}
		if len(info.Columns) != 1 {
			t.Errorf("%s: %d columns, want 1", tc.sql, len(info.Columns))
			continue
		}
		if got := info.Columns[0].Alias; got != tc.wantAlias {
			t.Errorf("%s: alias = %q, want %q", tc.sql, got, tc.wantAlias)
		}
	}
}

// TestImplicitAliasStillRejectsKeywords guards the other half: without AS,
// the reserved-word check is what stops the parser swallowing the clause that
// follows, so it must stay in place.
func TestImplicitAliasStillRejectsKeywords(t *testing.T) {
	node, err := Parse("SELECT n_name FROM nation")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := ExtractSelect(node)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if len(info.Tables) == 0 || info.Tables[0].Name != "nation" {
		t.Errorf("FROM was consumed as an alias: tables = %+v", info.Tables)
	}
}
