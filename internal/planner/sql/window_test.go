package sql

import (
	"testing"
)

func TestParseWindowSelect(t *testing.T) {
	sql := "SELECT user_id, ROW_NUMBER() OVER (ORDER BY ts DESC) as rn FROM events"
	parsed, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Type != QuerySelect {
		t.Fatalf("expected QuerySelect, got %v", parsed.Type)
	}
	if len(parsed.Windows) != 1 {
		t.Fatalf("expected 1 window spec, got %d", len(parsed.Windows))
	}

	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}
	if len(info.Windows) != 1 {
		t.Fatalf("expected 1 window in SelectInfo, got %d", len(info.Windows))
	}

	// Find the window column
	found := false
	for _, col := range info.Columns {
		if col.Alias == "rn" && col.IsWindow {
			found = true
			if col.WindowSpec == nil {
				t.Error("window column has nil WindowSpec")
			}
		}
	}
	if !found {
		t.Error("expected to find a window column with alias 'rn'")
	}
}
