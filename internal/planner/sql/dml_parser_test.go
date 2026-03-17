package sql

import (
	"testing"
)

func TestParseDelete_Basic(t *testing.T) {
	q, err := Parse("DELETE FROM users WHERE id = 5")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryDelete {
		t.Fatalf("expected QueryDelete, got %v", q.Type)
	}
	if q.Delete.Table != "users" {
		t.Fatalf("expected table 'users', got %q", q.Delete.Table)
	}
	if q.Delete.WhereSQL != "id = 5" {
		t.Fatalf("expected WHERE 'id = 5', got %q", q.Delete.WhereSQL)
	}
}

func TestParseDelete_NoWhere(t *testing.T) {
	q, err := Parse("DELETE FROM events")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryDelete {
		t.Fatalf("expected QueryDelete, got %v", q.Type)
	}
	if q.Delete.Table != "events" {
		t.Fatalf("expected table 'events', got %q", q.Delete.Table)
	}
	if q.Delete.WhereSQL != "" {
		t.Fatalf("expected empty WHERE, got %q", q.Delete.WhereSQL)
	}
}

func TestParseDelete_ComplexWhere(t *testing.T) {
	q, err := Parse("DELETE FROM users WHERE name = 'Alice' AND status != 'active'")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Delete.WhereSQL != "name = 'Alice' AND status != 'active'" {
		t.Fatalf("expected compound WHERE, got %q", q.Delete.WhereSQL)
	}
}

func TestParseUpdate_Basic(t *testing.T) {
	q, err := Parse("UPDATE users SET name = 'Bob' WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryUpdate {
		t.Fatalf("expected QueryUpdate, got %v", q.Type)
	}
	if q.Update.Table != "users" {
		t.Fatalf("expected table 'users', got %q", q.Update.Table)
	}
	if len(q.Update.SetClauses) != 1 {
		t.Fatalf("expected 1 SET clause, got %d", len(q.Update.SetClauses))
	}
	if q.Update.SetClauses[0].Column != "name" {
		t.Fatalf("expected column 'name', got %q", q.Update.SetClauses[0].Column)
	}
	if q.Update.SetClauses[0].Value != "Bob" {
		t.Fatalf("expected value \"Bob\", got %q", q.Update.SetClauses[0].Value)
	}
	if q.Update.WhereSQL != "id = 1" {
		t.Fatalf("expected WHERE 'id = 1', got %q", q.Update.WhereSQL)
	}
}

func TestParseUpdate_MultipleSets(t *testing.T) {
	q, err := Parse("UPDATE users SET name = 'Bob', age = 30, status = 'active' WHERE id = 1")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if len(q.Update.SetClauses) != 3 {
		t.Fatalf("expected 3 SET clauses, got %d", len(q.Update.SetClauses))
	}
	expected := []struct {
		col string
		val string
	}{
		{"name", "Bob"},
		{"age", "30"},
		{"status", "active"},
	}
	for i, exp := range expected {
		if q.Update.SetClauses[i].Column != exp.col {
			t.Errorf("clause %d: expected column %q, got %q", i, exp.col, q.Update.SetClauses[i].Column)
		}
		if q.Update.SetClauses[i].Value != exp.val {
			t.Errorf("clause %d: expected value %q, got %q", i, exp.val, q.Update.SetClauses[i].Value)
		}
	}
}

func TestParseUpdate_NoWhere(t *testing.T) {
	q, err := Parse("UPDATE events SET status = 'archived'")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Update.Table != "events" {
		t.Fatalf("expected table 'events', got %q", q.Update.Table)
	}
	if q.Update.WhereSQL != "" {
		t.Fatalf("expected empty WHERE, got %q", q.Update.WhereSQL)
	}
}

func TestParseInsert_Basic(t *testing.T) {
	q, err := Parse("INSERT INTO users (name, age) VALUES ('Alice', 30)")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if q.Type != QueryInsert {
		t.Fatalf("expected QueryInsert, got %v", q.Type)
	}
	if q.Insert.Table != "users" {
		t.Fatalf("expected table 'users', got %q", q.Insert.Table)
	}
	if len(q.Insert.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(q.Insert.Columns))
	}
	if q.Insert.Columns[0] != "name" || q.Insert.Columns[1] != "age" {
		t.Fatalf("expected columns [name, age], got %v", q.Insert.Columns)
	}
	if len(q.Insert.Values) != 1 {
		t.Fatalf("expected 1 value row, got %d", len(q.Insert.Values))
	}
	if len(q.Insert.Values[0]) != 2 {
		t.Fatalf("expected 2 values, got %d", len(q.Insert.Values[0]))
	}
}

func TestParseInsert_MultipleRows(t *testing.T) {
	q, err := Parse("INSERT INTO users (name, age) VALUES ('Alice', 30), ('Bob', 25), ('Charlie', 35)")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if len(q.Insert.Values) != 3 {
		t.Fatalf("expected 3 value rows, got %d", len(q.Insert.Values))
	}
	if q.Insert.Values[1][0] != "Bob" {
		t.Fatalf("expected row 1 val 0 = \"Bob\", got %q", q.Insert.Values[1][0])
	}
}

func TestParseInsert_NoColumns(t *testing.T) {
	q, err := Parse("INSERT INTO events VALUES ('click', 100)")
	if err != nil {
		t.Fatalf("Parse() error: %v", err)
	}
	if len(q.Insert.Columns) != 0 {
		t.Fatalf("expected 0 columns (implicit), got %d", len(q.Insert.Columns))
	}
	if len(q.Insert.Values) != 1 {
		t.Fatalf("expected 1 value row, got %d", len(q.Insert.Values))
	}
}

func TestParseDelete_MissingFrom(t *testing.T) {
	_, err := Parse("DELETE users WHERE id = 1")
	if err == nil {
		t.Fatal("expected error for missing FROM")
	}
}

func TestParseUpdate_MissingSet(t *testing.T) {
	_, err := Parse("UPDATE users name = 'Bob'")
	if err == nil {
		t.Fatal("expected error for missing SET")
	}
}

func TestParseInsert_MissingValues(t *testing.T) {
	_, err := Parse("INSERT INTO users (name)")
	if err == nil {
		t.Fatal("expected error for missing VALUES")
	}
}
