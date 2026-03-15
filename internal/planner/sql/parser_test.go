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
