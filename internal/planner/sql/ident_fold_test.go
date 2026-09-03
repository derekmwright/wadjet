package sql

import "testing"

// An UNQUOTED identifier folds to lower case; a DELIMITED one keeps its
// bytes. PostgreSQL's rule, measured live on postgres:17-alpine (#731):
//
//	SELECT G FROM t          →  reads column g, RowDescription name `g`
//	SELECT "G" FROM t        →  42703 column "G" does not exist
//	SELECT g AS Foo          →  `foo`        SELECT g AS "Foo"  →  `Foo`
//	SELECT 1 AS Desc         →  `desc`       SELECT 1 AS "Desc" →  `Desc`
//	CREATE TABLE t (Ä int)   →  column `Ä`   — the fold is ASCII A-Z ONLY
func TestUnquotedIdentifiersFoldAtTheLexer(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		// The column REFERENCES the parser produced, in order, read through
		// NormalizeIdentRef: Expr keeps a delimited reference's quotes and
		// every consumer strips them, so what this asserts is the NAME.
		want []string
	}{
		{"bare reference", "SELECT G FROM t", []string{"g"}},
		{"already folded", "SELECT g FROM t", []string{"g"}},
		{"delimited", `SELECT "G" FROM t`, []string{"G"}},
		{"delimited already folded", `SELECT "g" FROM t`, []string{"g"}},
		{"qualified", "SELECT T.G FROM t T", []string{"t.g"}},
		{"delimited qualifier", `SELECT "T".G FROM t T`, []string{"T.g"}},
		{"mixed list", `SELECT A, "B", c FROM t`, []string{"a", "B", "c"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			info, err := ExtractSelect(node)
			if err != nil {
				t.Fatalf("ExtractSelect: %v", err)
			}
			if len(info.Columns) != len(tc.want) {
				t.Fatalf("%d columns, want %d", len(info.Columns), len(tc.want))
			}
			for i, w := range tc.want {
				if got := NormalizeIdentRef(info.Columns[i].Expr); got != w {
					t.Errorf("column %d = %q, want %q", i, got, w)
				}
			}
		})
	}
}

// The fold is ASCII A-Z and nothing else. PostgreSQL 17 with a UTF8 server
// encoding stores `CREATE TABLE t (Ä int)` as the column `Ä` and publishes
// `SELECT 1 AS Ä` as `Ä` — strings.ToLower would answer `ä` and invent a name
// no client expects.
func TestFoldIdentIsASCIIOnly(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"WatchID", "watchid"},
		{"g", "g"},
		{"G", "g"},
		{"_X1", "_x1"},
		{"Ä", "Ä"},
		{"ÄB", "Äb"},
		{"id.orig_h", "id.orig_h"},
		{"", ""},
	} {
		if got := FoldIdent(tc.in); got != tc.want {
			t.Errorf("FoldIdent(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
	node, err := Parse(`SELECT Ä FROM t`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := ExtractSelect(node)
	if err != nil {
		t.Fatalf("ExtractSelect: %v", err)
	}
	if got := NormalizeIdentRef(info.Columns[0].Expr); got != "Ä" {
		t.Errorf("non-ASCII identifier folded to %q, want Ä", got)
	}
}

// Parse(QuoteIdent(n)) == n, for every name a schema can hold. The planner
// renders a reference and re-parses it at a dozen sites; once the lexer
// folds, a bare rendering of `WatchID` comes back as `watchid` — a different
// column — so QuoteIdent has to delimit it.
func TestQuoteIdentRoundTripsUnderTheFold(t *testing.T) {
	for _, name := range []string{
		"l_orderkey", "WatchID", "CounterID", "_x1", "id.orig_h", "src host",
		"select", "1st", `a"b`, "Ä", "MiXeD_case_1",
	} {
		qual, got, ok := SplitIdentRef(QuoteIdent(name))
		if !ok || qual != "" || got != name {
			t.Errorf("SplitIdentRef(QuoteIdent(%q)) = (%q, %q, %v), want (\"\", %q, true)",
				name, qual, got, ok, name)
		}
		// The same round trip through the QUERY lexer, which is the one that
		// folds: rendering into SQL and re-parsing must name the same column.
		node, err := Parse("SELECT " + QuoteIdent(name) + " FROM t")
		if err != nil {
			t.Fatalf("SELECT %s: %v", QuoteIdent(name), err)
		}
		info, err := ExtractSelect(node)
		if err != nil {
			t.Fatalf("ExtractSelect: %v", err)
		}
		if got := NormalizeIdentRef(info.Columns[0].Expr); got != name {
			t.Errorf("re-parsed %q as %q, want %q", QuoteIdent(name), got, name)
		}
	}
}

// SplitIdentRef and NormalizeIdentRef are asked to SPLIT a name, not to bind
// one: they lex a string a caller already holds — often a schema's own
// CamelCase spelling — so they must not fold it.
func TestIdentSplittersDoNotFold(t *testing.T) {
	for _, tc := range []struct{ in, qual, name string }{
		{"WatchID", "", "WatchID"},
		{"h.WatchID", "h", "WatchID"},
		{`"id.orig_h"`, "", "id.orig_h"},
		{`"my tbl"."C"`, "my tbl", "C"},
	} {
		qual, name, ok := SplitIdentRef(tc.in)
		if !ok || qual != tc.qual || name != tc.name {
			t.Errorf("SplitIdentRef(%q) = (%q, %q, %v), want (%q, %q, true)",
				tc.in, qual, name, ok, tc.qual, tc.name)
		}
	}
	if got := NormalizeIdentRef("WatchID"); got != "WatchID" {
		t.Errorf("NormalizeIdentRef(WatchID) = %q, want WatchID", got)
	}
	if got := NormalizeIdentRef(`"WatchID"`); got != "WatchID" {
		t.Errorf(`NormalizeIdentRef("WatchID") = %q, want WatchID`, got)
	}
}

// A DDL declaration takes the same rule: an unquoted column name arrives
// folded, and a delimited one keeps its bytes. Before #731 the DDL path
// lowercased BOTH, so a table could not be declared with a mixed-case column
// at all — while a parquet-registered one carries them routinely.
func TestDDLIdentifiersFollowTheSameRule(t *testing.T) {
	node, err := Parse(`CREATE TABLE Hits (WatchID BIGINT, "UserID" BIGINT, "id.orig_h" VARCHAR)`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	ct := node.CreateTable
	if ct == nil {
		t.Fatal("no CreateTableInfo")
	}
	if ct.Name != "hits" {
		t.Errorf("table name = %q, want hits", ct.Name)
	}
	want := []struct{ name, typ string }{
		{"watchid", "BIGINT"}, {"UserID", "BIGINT"}, {"id.orig_h", "VARCHAR"},
	}
	if len(ct.Columns) != len(want) {
		t.Fatalf("%d columns, want %d", len(ct.Columns), len(want))
	}
	for i, w := range want {
		if ct.Columns[i].Name != w.name {
			t.Errorf("column %d name = %q, want %q", i, ct.Columns[i].Name, w.name)
		}
		// The TYPE is not an identifier reference and keeps its spelling.
		if ct.Columns[i].Type != w.typ {
			t.Errorf("column %d type = %q, want %q", i, ct.Columns[i].Type, w.typ)
		}
	}
}

// A syntax error echoes the text the client SENT. PostgreSQL does
// (`syntax error at or near "TOKENS"`), and naming a folded spelling the
// client never typed sends them looking for the wrong word.
func TestSyntaxErrorsEchoTheClientsSpelling(t *testing.T) {
	_, err := Parse("SELECT n_name FROM nation ORDER BY n_name WAT")
	if err == nil {
		t.Fatal("expected a syntax error")
	}
	if !contains(err.Error(), `"WAT"`) {
		t.Errorf("error does not echo the client's spelling: %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
