package format

import (
	"bytes"
	"strings"
	"testing"
)

var testColumns = []string{"id", "name", "amount"}
var testRows = []map[string]any{
	{"id": int64(1), "name": "alice", "amount": 95.5},
	{"id": int64(2), "name": "bob", "amount": nil},
	{"id": int64(3), "name": "carol", "amount": 78.0},
}

func TestTableFormat(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, Table, testColumns, testRows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()

	// Should contain header separator
	if !strings.Contains(out, "+") {
		t.Error("missing table borders")
	}
	// Should contain column names
	if !strings.Contains(out, "id") || !strings.Contains(out, "name") || !strings.Contains(out, "amount") {
		t.Error("missing column names")
	}
	// Should contain data
	if !strings.Contains(out, "alice") || !strings.Contains(out, "bob") {
		t.Error("missing data values")
	}
	// Should show NULL for nil
	if !strings.Contains(out, "NULL") {
		t.Error("nil should display as NULL")
	}
	// Should show row count
	if !strings.Contains(out, "(3 rows)") {
		t.Error("missing row count")
	}
}

func TestTableFormatEmpty(t *testing.T) {
	var buf bytes.Buffer
	Write(&buf, Table, nil, nil)
	if !strings.Contains(buf.String(), "(0 rows)") {
		t.Error("empty result should show (0 rows)")
	}
}

func TestTableFormatTruncation(t *testing.T) {
	cols := []string{"val"}
	rows := []map[string]any{
		{"val": strings.Repeat("x", 100)},
	}
	var buf bytes.Buffer
	Write(&buf, Table, cols, rows)
	out := buf.String()
	if !strings.Contains(out, "...") {
		t.Error("long values should be truncated with ...")
	}
}

func TestJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, JSON, testColumns, testRows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if !strings.Contains(out, `"alice"`) {
		t.Error("JSON should contain data values")
	}
}

func TestJSONFormatEmpty(t *testing.T) {
	var buf bytes.Buffer
	Write(&buf, JSON, nil, nil)
	if !strings.Contains(buf.String(), "[]") {
		t.Error("empty JSON should be []")
	}
}

func TestCSVFormat(t *testing.T) {
	var buf bytes.Buffer
	if err := Write(&buf, CSV, testColumns, testRows); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 4 { // header + 3 rows
		t.Fatalf("expected 4 lines, got %d", len(lines))
	}
	if lines[0] != "id,name,amount" {
		t.Errorf("unexpected header: %s", lines[0])
	}
	if !strings.Contains(lines[2], "NULL") {
		t.Error("nil should be NULL in CSV")
	}
}

func TestTableFormatVisual(t *testing.T) {
	cols := []string{"id", "name", "amount", "status"}
	rows := []map[string]any{
		{"id": int64(1), "name": "alice", "amount": 95.5, "status": "active"},
		{"id": int64(2), "name": "bob", "amount": nil, "status": "inactive"},
		{"id": int64(3), "name": "carol_with_a_really_long_name_that_should_truncate", "amount": 78.0, "status": "active"},
	}
	var buf bytes.Buffer
	Write(&buf, Table, cols, rows)
	t.Log("\n" + buf.String())
}

func TestParseFormat(t *testing.T) {
	tests := []struct {
		input string
		want  Format
		err   bool
	}{
		{"table", Table, false},
		{"TABLE", Table, false},
		{"json", JSON, false},
		{"csv", CSV, false},
		{"xml", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseFormat(tt.input)
		if tt.err {
			if err == nil {
				t.Errorf("ParseFormat(%q): expected error", tt.input)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseFormat(%q): %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseFormat(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}
