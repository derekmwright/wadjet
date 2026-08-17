package sql

import (
	"strings"
	"testing"
)

// --- Lexer ---

func TestLexQuotedIdent(t *testing.T) {
	tests := []struct {
		name  string
		input string
		val   string
	}{
		{"simple", `"col"`, "col"},
		{"embedded dot", `"id.orig_h"`, "id.orig_h"},
		{"embedded space", `"src host"`, "src host"},
		{"case preserved", `"CounterID"`, "CounterID"},
		{"keyword spelling", `"select"`, "select"},
		{"escaped quote", `"a""b"`, `a"b`},
		{"only an escaped quote", `""""`, `"`},
		{"unicode", `"üñî"`, "üñî"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := collectTokens(tt.input)
			if len(tokens) != 2 {
				t.Fatalf("expected ident + EOF, got %d tokens: %v", len(tokens), tokens)
			}
			if tokens[0].typ != TokenIdent {
				t.Fatalf("expected TokenIdent, got %d (%q)", tokens[0].typ, tokens[0].val)
			}
			if !tokens[0].quoted {
				t.Errorf("expected quoted=true for %s", tt.input)
			}
			if tokens[0].val != tt.val {
				t.Errorf("value: got %q, want %q", tokens[0].val, tt.val)
			}
		})
	}
}

// TestLexQuotedIdentIsOneToken pins the core semantic: the dots inside a
// delimited identifier are part of the name, so no TokenDot is produced.
func TestLexQuotedIdentIsOneToken(t *testing.T) {
	tokens := collectTokens(`"id.orig_h"`)
	for _, tok := range tokens {
		if tok.typ == TokenDot {
			t.Fatalf("quoted identifier split on '.': %v", tokens)
		}
	}

	// The unquoted spelling still splits into qualifier, dot, column.
	unquoted := collectTokens(`id.orig_h`)
	want := []TokenType{TokenIdent, TokenDot, TokenIdent, TokenEOF}
	if len(unquoted) != len(want) {
		t.Fatalf("expected %d tokens, got %d: %v", len(want), len(unquoted), unquoted)
	}
	for i, exp := range want {
		if unquoted[i].typ != exp {
			t.Fatalf("token %d: got %d, want %d", i, unquoted[i].typ, exp)
		}
	}
	if unquoted[0].quoted {
		t.Error("unquoted identifier marked as quoted")
	}
}

func TestLexQuotedIdentErrors(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantMsg string
	}{
		{"unterminated", `SELECT "col FROM t`, "unterminated quoted identifier"},
		{"unterminated after escape", `"a""`, "unterminated quoted identifier"},
		{"zero length", `SELECT "" FROM t`, "zero-length quoted identifier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens := collectTokens(tt.input)
			last := tokens[len(tokens)-1]
			if last.typ != TokenError {
				t.Fatalf("expected TokenError, got %d (%q)", last.typ, last.val)
			}
			if last.val != tt.wantMsg {
				t.Errorf("message: got %q, want %q", last.val, tt.wantMsg)
			}
		})
	}
}

// TestLexErrorfInterpolatesArgs is the regression test for the error helper
// emitting its format string verbatim: an unsupported character used to be
// reported as the literal text "unexpected character: %c".
func TestLexErrorfInterpolatesArgs(t *testing.T) {
	tokens := collectTokens("SELECT # FROM t")
	last := tokens[len(tokens)-1]
	if last.typ != TokenError {
		t.Fatalf("expected TokenError, got %d (%q)", last.typ, last.val)
	}
	if strings.Contains(last.val, "%c") || strings.Contains(last.val, "%") {
		t.Fatalf("format verb reached the message: %q", last.val)
	}
	if !strings.Contains(last.val, "#") {
		t.Errorf("message does not name the offending character: %q", last.val)
	}
}

// TestParseErrorSurfacesLexerMessage checks the interpolated message reaches
// the caller of Parse rather than being replaced by a generic one.
func TestParseErrorSurfacesLexerMessage(t *testing.T) {
	_, err := Parse("SELECT # FROM t")
	if err == nil {
		t.Fatal("expected a parse error")
	}
	if strings.Contains(err.Error(), "%c") {
		t.Errorf("uninterpolated format verb in parse error: %v", err)
	}
}

// --- Parser ---

func TestParseQuotedColumnRef(t *testing.T) {
	parsed, err := Parse(`SELECT "id.orig_h" FROM conn`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info := parsed.SelectInfo
	if info == nil || len(info.Columns) != 1 {
		t.Fatalf("expected 1 column, got %+v", info)
	}
	col := info.Columns[0]
	if col.ColumnRef != "id.orig_h" {
		t.Errorf("ColumnRef: got %q, want %q", col.ColumnRef, "id.orig_h")
	}
	if col.TableRef != "" {
		t.Errorf("TableRef: got %q, want empty — a quoted identifier is never qualifier.column", col.TableRef)
	}
	ref, ok := col.ASTExpr.(*ColRef)
	if !ok {
		t.Fatalf("expected *ColRef, got %T", col.ASTExpr)
	}
	if ref.Table != "" || ref.Column != "id.orig_h" {
		t.Errorf("ColRef: got {Table:%q Column:%q}, want {Table:\"\" Column:\"id.orig_h\"}", ref.Table, ref.Column)
	}
}

// TestParseUnquotedDottedRefStillSplits pins the pre-existing behaviour the
// quoted form sits alongside: without quotes, id.orig_h is a qualified
// reference (which the engine then resolves against the flat column).
func TestParseUnquotedDottedRefStillSplits(t *testing.T) {
	parsed, err := Parse(`SELECT id.orig_h FROM conn`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	col := parsed.SelectInfo.Columns[0]
	if col.TableRef != "id" || col.ColumnRef != "orig_h" {
		t.Errorf("got {TableRef:%q ColumnRef:%q}, want {\"id\", \"orig_h\"}", col.TableRef, col.ColumnRef)
	}
}

func TestParseQuotedIdentifierCasePreserved(t *testing.T) {
	parsed, err := Parse(`SELECT "MixedCase" FROM t`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := parsed.SelectInfo.Columns[0].ColumnRef; got != "MixedCase" {
		t.Errorf("got %q, want %q — quoted identifiers are verbatim", got, "MixedCase")
	}
}

func TestParseQuotedAlias(t *testing.T) {
	parsed, err := Parse(`SELECT "id.orig_h" AS "src host", COUNT(*) AS "n rows" FROM conn`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	cols := parsed.SelectInfo.Columns
	if len(cols) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(cols))
	}
	if cols[0].Alias != "src host" {
		t.Errorf("alias 0: got %q, want %q", cols[0].Alias, "src host")
	}
	if cols[1].Alias != "n rows" {
		t.Errorf("alias 1: got %q, want %q", cols[1].Alias, "n rows")
	}
}

func TestParseQuotedGroupByAndOrderBy(t *testing.T) {
	parsed, err := Parse(`SELECT "id.orig_h", COUNT(*) AS n FROM conn GROUP BY "id.orig_h" ORDER BY "id.orig_h" DESC`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info := parsed.SelectInfo
	if len(info.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY item, got %v", info.GroupBy)
	}
	if got := NormalizeIdentRef(info.GroupBy[0]); got != "id.orig_h" {
		t.Errorf("GROUP BY: got %q, want %q", got, "id.orig_h")
	}
	if len(info.OrderBy) != 1 || !info.OrderBy[0].Desc {
		t.Fatalf("expected 1 descending ORDER BY item, got %v", info.OrderBy)
	}
	if got := NormalizeIdentRef(info.OrderBy[0].Column); got != "id.orig_h" {
		t.Errorf("ORDER BY: got %q, want %q", got, "id.orig_h")
	}
}

func TestParseMixedQuotedAndUnquoted(t *testing.T) {
	parsed, err := Parse(`SELECT uid, "id.orig_h", proto FROM conn WHERE "id.resp_p" = 443 AND proto = 'tcp'`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := []string{"uid", "id.orig_h", "proto"}
	cols := parsed.SelectInfo.Columns
	if len(cols) != len(want) {
		t.Fatalf("expected %d columns, got %d", len(want), len(cols))
	}
	for i, w := range want {
		if cols[i].ColumnRef != w {
			t.Errorf("column %d: got %q, want %q", i, cols[i].ColumnRef, w)
		}
	}
	if parsed.SelectInfo.WhereExpr == nil {
		t.Fatal("expected a WHERE expression")
	}
}

func TestParseQuotedTableName(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantTable string
		wantAlias string
	}{
		{"bare", `SELECT "c1" FROM "conn"`, "conn", "conn"},
		{"aliased", `SELECT c."c1" FROM "conn" AS "c"`, "conn", "c"},
		{"case preserved", `SELECT * FROM "ConnLog"`, "ConnLog", "ConnLog"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			tables := parsed.SelectInfo.Tables
			if len(tables) != 1 {
				t.Fatalf("expected 1 table, got %v", tables)
			}
			if tables[0].Name != tt.wantTable {
				t.Errorf("table name: got %q, want %q", tables[0].Name, tt.wantTable)
			}
			if tables[0].Alias != tt.wantAlias {
				t.Errorf("table alias: got %q, want %q", tables[0].Alias, tt.wantAlias)
			}
		})
	}
}

// TestParseQuotedKeywordIdentifier covers the identifier spellings that
// collide with the grammar: quoted, they are ordinary names.
func TestParseQuotedKeywordIdentifier(t *testing.T) {
	for _, sql := range []string{
		`SELECT "select", "from", "group" FROM t`,
		`SELECT "interval" FROM t`,
		`SELECT "current_date" FROM t`,
		`SELECT x AS "qualify" FROM t`,
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("parse %q: %v", sql, err)
		}
	}

	parsed, err := Parse(`SELECT "interval" FROM t`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := parsed.SelectInfo.Columns[0].ColumnRef; got != "interval" {
		t.Errorf("got ColumnRef %q, want a plain column reference to %q", got, "interval")
	}
}

// TestParseToolGeneratedShape is the query shape BI tools and JDBC metadata
// builders emit: every identifier quoted, positional GROUP BY / ORDER BY.
func TestParseToolGeneratedShape(t *testing.T) {
	parsed, err := Parse(`SELECT "id.orig_h", COUNT(*) AS "cnt" FROM "conn" WHERE "proto" = 'tcp' GROUP BY 1 ORDER BY 2 DESC LIMIT 10`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info := parsed.SelectInfo
	if info.Tables[0].Name != "conn" {
		t.Errorf("table: got %q, want conn", info.Tables[0].Name)
	}
	if info.Columns[0].ColumnRef != "id.orig_h" {
		t.Errorf("column 0: got %q, want id.orig_h", info.Columns[0].ColumnRef)
	}
	if info.Columns[1].Alias != "cnt" {
		t.Errorf("column 1 alias: got %q, want cnt", info.Columns[1].Alias)
	}
	if info.Limit != "10" {
		t.Errorf("limit: got %q, want 10", info.Limit)
	}
}

// --- Printing / round-trip ---

func TestQuoteIdent(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"l_orderkey", "l_orderkey"},
		{"CounterID", "CounterID"},
		{"_x1", "_x1"},
		{"id.orig_h", `"id.orig_h"`},
		{"src host", `"src host"`},
		{"select", `"select"`},
		{"1st", `"1st"`},
		{`a"b`, `"a""b"`},
		{"", `""`},
	}
	for _, tt := range tests {
		if got := QuoteIdent(tt.in); got != tt.want {
			t.Errorf("QuoteIdent(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestSplitIdentRef(t *testing.T) {
	tests := []struct {
		in   string
		qual string
		name string
		ok   bool
	}{
		{"l_orderkey", "", "l_orderkey", true},
		{"o.o_custkey", "o", "o_custkey", true},
		{`"id.orig_h"`, "", "id.orig_h", true},
		{`"my tbl"."c"`, "my tbl", "c", true},
		{`t."id.orig_h"`, "t", "id.orig_h", true},
		{"a.b.c", "", "", false},
		{"count(*)", "", "", false},
		{"a + b", "", "", false},
		{"'literal'", "", "", false},
	}
	for _, tt := range tests {
		qual, name, ok := SplitIdentRef(tt.in)
		if ok != tt.ok || qual != tt.qual || name != tt.name {
			t.Errorf("SplitIdentRef(%q) = (%q, %q, %v), want (%q, %q, %v)",
				tt.in, qual, name, ok, tt.qual, tt.name, tt.ok)
		}
	}
}

// TestQuotedColRefRoundTrip checks that printing an expression containing a
// delimited identifier and re-parsing it yields the same AST — the property
// the plan layers rely on when they serialize and re-read expressions.
func TestQuotedColRefRoundTrip(t *testing.T) {
	for _, name := range []string{"id.orig_h", "src host", "select", "plain"} {
		ref := &ColRef{Column: name}
		printed := ref.String()
		parsed, err := Parse("SELECT 1 FROM t WHERE " + printed + " = 1")
		if err != nil {
			t.Fatalf("re-parsing %q: %v", printed, err)
		}
		cmp, ok := parsed.SelectInfo.WhereExpr.(*CmpExpr)
		if !ok {
			t.Fatalf("expected *CmpExpr, got %T", parsed.SelectInfo.WhereExpr)
		}
		got, ok := cmp.Left.(*ColRef)
		if !ok {
			t.Fatalf("expected *ColRef on the left, got %T", cmp.Left)
		}
		if name == "id.orig_h" || name == "src host" || name == "select" {
			if got.Table != "" || got.Column != name {
				t.Errorf("round-trip of %q gave {Table:%q Column:%q}", name, got.Table, got.Column)
			}
		}
		if reprinted := got.String(); reprinted != printed {
			t.Errorf("printing is not a fixed point: %q then %q", printed, reprinted)
		}
	}
}
