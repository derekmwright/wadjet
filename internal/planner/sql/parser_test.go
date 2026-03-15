package sql

import (
	"testing"
)

func TestParse_Select(t *testing.T) {
	tests := []struct {
		sql     string
		wantErr bool
	}{
		{"SELECT * FROM events", false},
		{"SELECT id, name FROM users WHERE id > 5", false},
		{"SELECT user_id, SUM(amount) FROM events GROUP BY user_id", false},
		{"SELECT * FROM events ORDER BY ts DESC LIMIT 10", false},
		{"SELECT e.user_id, SUM(e.amount) FROM events e JOIN users u ON e.user_id = u.user_id WHERE e.ts >= '2026-03-01' GROUP BY e.user_id ORDER BY SUM(e.amount) DESC LIMIT 20", false},
		{"INVALID SQL", true},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Type != QuerySelect {
				t.Fatalf("expected SELECT, got %v", parsed.Type)
			}
		})
	}
}

func TestExtractSelect(t *testing.T) {
	parsed, err := Parse("SELECT user_id, SUM(amount) as total FROM events WHERE year = '2026' GROUP BY user_id ORDER BY total DESC LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Tables) != 1 || info.Tables[0].Name != "events" {
		t.Fatalf("expected table 'events', got %v", info.Tables)
	}

	if len(info.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(info.Columns))
	}

	if !info.Columns[1].IsAgg || info.Columns[1].AggFunc != "sum" {
		t.Fatalf("expected SUM aggregate, got %v", info.Columns[1])
	}

	if len(info.GroupBy) != 1 {
		t.Fatalf("expected 1 GROUP BY, got %d", len(info.GroupBy))
	}

	if len(info.OrderBy) != 1 || !info.OrderBy[0].Desc {
		t.Fatalf("expected 1 ORDER BY DESC, got %v", info.OrderBy)
	}

	if info.Limit != "10" {
		t.Fatalf("expected LIMIT 10, got %s", info.Limit)
	}
}

func TestExtractJoin(t *testing.T) {
	parsed, err := Parse("SELECT e.user_id FROM events e JOIN users u ON e.user_id = u.user_id")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if len(info.Joins) != 1 {
		t.Fatalf("expected 1 join, got %d", len(info.Joins))
	}

	if info.Joins[0].RightTable != "users" {
		t.Fatalf("expected right table 'users', got %s", info.Joins[0].RightTable)
	}
}

func TestParseExplain(t *testing.T) {
	parsed, err := Parse("EXPLAIN SELECT * FROM events WHERE id > 5")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != QueryExplain {
		t.Fatalf("expected QueryExplain, got %v", parsed.Type)
	}
	if parsed.Explain == nil {
		t.Fatal("Explain info is nil")
	}
	if parsed.Explain.Verbose {
		t.Error("should not be verbose")
	}
	if parsed.Explain.InnerSQL != "SELECT * FROM events WHERE id > 5" {
		t.Errorf("unexpected inner SQL: %s", parsed.Explain.InnerSQL)
	}
	// AST should be the inner SELECT
	if parsed.AST == nil {
		t.Error("AST should contain inner SELECT")
	}
}

func TestParseExplainVerbose(t *testing.T) {
	parsed, err := Parse("EXPLAIN VERBOSE SELECT id FROM events")
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != QueryExplain {
		t.Fatalf("expected QueryExplain, got %v", parsed.Type)
	}
	if !parsed.Explain.Verbose {
		t.Error("should be verbose")
	}
}

func TestParseDescribe(t *testing.T) {
	tests := []struct {
		sql       string
		tableName string
	}{
		{"DESCRIBE events", "events"},
		{"DESC events", "events"},
		{"describe users;", "users"},
		{"SHOW COLUMNS FROM events", "events"},
		{"show columns from users;", "users"},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Type != QueryDescribe {
				t.Fatalf("expected QueryDescribe, got %v", parsed.Type)
			}
			if parsed.Describe == nil {
				t.Fatal("Describe info is nil")
			}
			if parsed.Describe.TableName != tt.tableName {
				t.Errorf("expected table %q, got %q", tt.tableName, parsed.Describe.TableName)
			}
		})
	}
}

func TestParseDescribeEmpty(t *testing.T) {
	_, err := Parse("DESCRIBE ")
	if err == nil {
		t.Error("expected error for empty DESCRIBE")
	}
}

func TestParseCreateFunction(t *testing.T) {
	tests := []struct {
		sql     string
		name    string
		params  []string
		body    string
		replace bool
		locked  bool
	}{
		{
			sql:    "CREATE FUNCTION double(x) AS x * 2",
			name:   "double",
			params: []string{"x"},
			body:   "x * 2",
		},
		{
			sql:     "CREATE OR REPLACE FUNCTION weighted(val, weight) AS val * weight + 10",
			name:    "weighted",
			params:  []string{"val", "weight"},
			body:    "val * weight + 10",
			replace: true,
		},
		{
			sql:    "CREATE FUNCTION classify(score) AS CASE WHEN score >= 90 THEN 'A' ELSE 'B' END WITH LOCK",
			name:   "classify",
			params: []string{"score"},
			body:   "CASE WHEN score >= 90 THEN 'A' ELSE 'B' END",
			locked: true,
		},
		{
			sql:    "CREATE FUNCTION no_params() AS 42;",
			name:   "no_params",
			params: nil,
			body:   "42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if parsed.Type != QueryCreateFunction {
				t.Fatalf("expected QueryCreateFunction, got %v", parsed.Type)
			}
			cf := parsed.CreateFunction
			if cf == nil {
				t.Fatal("CreateFunction is nil")
			}
			if cf.Name != tt.name {
				t.Errorf("name: got %q, want %q", cf.Name, tt.name)
			}
			if len(cf.Params) != len(tt.params) {
				t.Fatalf("params: got %v, want %v", cf.Params, tt.params)
			}
			for i := range cf.Params {
				if cf.Params[i] != tt.params[i] {
					t.Errorf("params[%d]: got %q, want %q", i, cf.Params[i], tt.params[i])
				}
			}
			if cf.Body != tt.body {
				t.Errorf("body: got %q, want %q", cf.Body, tt.body)
			}
			if cf.Replace != tt.replace {
				t.Errorf("replace: got %v, want %v", cf.Replace, tt.replace)
			}
			if cf.Locked != tt.locked {
				t.Errorf("locked: got %v, want %v", cf.Locked, tt.locked)
			}
		})
	}
}

func TestParseDropFunction(t *testing.T) {
	tests := []struct {
		sql      string
		name     string
		ifExists bool
	}{
		{"DROP FUNCTION my_func", "my_func", false},
		{"DROP FUNCTION IF EXISTS my_func", "my_func", true},
		{"DROP FUNCTION my_func;", "my_func", false},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			if parsed.Type != QueryDropFunction {
				t.Fatalf("expected QueryDropFunction, got %v", parsed.Type)
			}
			df := parsed.DropFunction
			if df.Name != tt.name {
				t.Errorf("name: got %q, want %q", df.Name, tt.name)
			}
			if df.IfExists != tt.ifExists {
				t.Errorf("ifExists: got %v, want %v", df.IfExists, tt.ifExists)
			}
		})
	}
}

func TestParseShowFunctions(t *testing.T) {
	for _, sql := range []string{"SHOW FUNCTIONS", "SHOW FUNCTIONS;"} {
		parsed, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		if parsed.Type != QueryShowFunctions {
			t.Fatalf("expected QueryShowFunctions, got %v", parsed.Type)
		}
	}
}

func TestParseUnion(t *testing.T) {
	tests := []struct {
		sql     string
		wantAll bool
	}{
		{"SELECT id FROM events UNION SELECT id FROM users", false},
		{"SELECT id FROM events UNION ALL SELECT id FROM users", true},
	}

	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			parsed, err := Parse(tt.sql)
			if err != nil {
				t.Fatal(err)
			}
			if parsed.Type != QuerySelect {
				t.Fatalf("expected QuerySelect, got %v", parsed.Type)
			}
		})
	}
}

func TestExtractUnion(t *testing.T) {
	parsed, err := Parse("SELECT id, name FROM events UNION SELECT id, name FROM users")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected Union to be non-nil")
	}
	if info.Union.All {
		t.Error("expected UNION (not UNION ALL)")
	}

	// Left side should have table "events"
	if len(info.Union.Left.Tables) != 1 || info.Union.Left.Tables[0].Name != "events" {
		t.Errorf("left tables: got %v, want [events]", info.Union.Left.Tables)
	}

	// Right side should have table "users"
	if len(info.Union.Right.Tables) != 1 || info.Union.Right.Tables[0].Name != "users" {
		t.Errorf("right tables: got %v, want [users]", info.Union.Right.Tables)
	}
}

func TestExtractUnionAll(t *testing.T) {
	parsed, err := Parse("SELECT id FROM events UNION ALL SELECT id FROM users")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected Union to be non-nil")
	}
	if !info.Union.All {
		t.Error("expected UNION ALL")
	}
}

func TestExtractUnionWithOrderByAndLimit(t *testing.T) {
	parsed, err := Parse("SELECT id FROM events UNION SELECT id FROM users ORDER BY id LIMIT 10")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected Union to be non-nil")
	}

	if len(info.OrderBy) != 1 {
		t.Fatalf("expected 1 ORDER BY, got %d", len(info.OrderBy))
	}
	if info.Limit != "10" {
		t.Errorf("expected LIMIT 10, got %s", info.Limit)
	}
}

func TestExtractNestedUnion(t *testing.T) {
	// A UNION B UNION ALL C is parsed as (A UNION B) UNION ALL C
	parsed, err := Parse("SELECT id FROM a UNION SELECT id FROM b UNION ALL SELECT id FROM c")
	if err != nil {
		t.Fatal(err)
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	if info.Union == nil {
		t.Fatal("expected outer Union to be non-nil")
	}
	if !info.Union.All {
		t.Error("expected outer to be UNION ALL")
	}

	// The left side of the outer UNION ALL should itself be a UNION
	if info.Union.Left.Union == nil {
		t.Fatal("expected inner Union to be non-nil on the left side")
	}
	if info.Union.Left.Union.All {
		t.Error("expected inner to be UNION (not ALL)")
	}
}

// Edge cases that the old substring-slicing parser would fail on.

func TestParseCreateFunctionParensInBody(t *testing.T) {
	// Old parser: strings.Index(rest, ")") found the ) from f(score), not the param list close
	sql := "CREATE FUNCTION classify(score) AS CASE WHEN score >= 90 THEN f(score) ELSE 'low' END WITH LOCK"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	if cf.Name != "classify" {
		t.Errorf("name: got %q, want 'classify'", cf.Name)
	}
	if len(cf.Params) != 1 || cf.Params[0] != "score" {
		t.Errorf("params: got %v, want [score]", cf.Params)
	}
	expected := "CASE WHEN score >= 90 THEN f(score) ELSE 'low' END"
	if cf.Body != expected {
		t.Errorf("body: got %q, want %q", cf.Body, expected)
	}
	if !cf.Locked {
		t.Error("expected Locked=true")
	}
}

func TestParseCreateFunctionWithLockInStringLiteral(t *testing.T) {
	// Old parser: strings.HasSuffix(upperBody, " WITH LOCK") would false-positive
	sql := "CREATE FUNCTION flagged(x) AS CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	expected := "CASE WHEN x = 'WITH LOCK' THEN 1 ELSE 0 END"
	if cf.Body != expected {
		t.Errorf("body: got %q, want %q", cf.Body, expected)
	}
	if cf.Locked {
		t.Error("expected Locked=false (WITH LOCK was inside a string literal)")
	}
}

func TestParseCreateFunctionEscapedQuotes(t *testing.T) {
	sql := "CREATE FUNCTION greet(name) AS 'hello, it''s ' || name"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	if cf.Name != "greet" {
		t.Errorf("name: got %q, want 'greet'", cf.Name)
	}
	expected := "'hello, it''s ' || name"
	if cf.Body != expected {
		t.Errorf("body: got %q, want %q", cf.Body, expected)
	}
}

func TestParseCreateFunctionNestedParens(t *testing.T) {
	sql := "CREATE FUNCTION deep(x) AS f(g(h(x, 1), 2), 3);"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	cf := parsed.CreateFunction
	if cf.Body != "f(g(h(x, 1), 2), 3)" {
		t.Errorf("body: got %q", cf.Body)
	}
}

func TestParseCreateFunctionErrors(t *testing.T) {
	tests := []string{
		"CREATE FUNCTION",
		"CREATE FUNCTION AS x",
		"CREATE FUNCTION fn AS x",     // no parens
		"CREATE FUNCTION fn(x) x * 2", // missing AS
		"CREATE FUNCTION fn(x) AS",    // empty body
	}
	for _, sql := range tests {
		t.Run(sql, func(t *testing.T) {
			_, err := Parse(sql)
			if err == nil {
				t.Errorf("expected error for %q", sql)
			}
		})
	}
}
