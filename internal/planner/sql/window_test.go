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

// Every SelectInfo carries ITS OWN window columns, through the whole
// set-operation tree (#733, #746).
//
// The post-parse collector read the OUTERMOST SelectInfo's columns only, and a
// set operation's outermost SelectInfo has none: they live on the arms. So an
// arm's Windows was always nil, and it is the list the logical builder gates
// window planning on — an arm whose SELECT list is a bare window got no Window
// node at all and its projection was left reading the alias off the arm's
// INPUT.
func TestEverySetOperationArmCarriesItsOwnWindowSpecs(t *testing.T) {
	for _, tc := range []struct {
		name              string
		sql               string
		stmt, left, right int
	}{
		{"window in the left arm only",
			"SELECT id, SUM(a) OVER () AS s FROM t UNION ALL SELECT id, b AS s FROM t", 1, 1, 0},
		{"window in the right arm only",
			"SELECT id, b AS s FROM t UNION ALL SELECT id, SUM(a) OVER () AS s FROM t", 1, 0, 1},
		{"a window in each arm",
			"SELECT SUM(a) OVER () AS s FROM t UNION ALL SELECT SUM(b) OVER () AS s FROM t", 2, 1, 1},
		{"INTERSECT", "SELECT SUM(a) OVER () AS s FROM t INTERSECT SELECT b FROM t", 1, 1, 0},
		{"EXCEPT", "SELECT SUM(a) OVER () AS s FROM t EXCEPT SELECT b FROM t", 1, 1, 0},
		{"no window anywhere", "SELECT a FROM t UNION ALL SELECT b FROM t", 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parsed, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			info, err := ExtractSelect(parsed)
			if err != nil {
				t.Fatalf("extract: %v", err)
			}
			if len(parsed.Windows) != tc.stmt {
				t.Errorf("statement carries %d window specs, want %d", len(parsed.Windows), tc.stmt)
			}
			if info.Union == nil {
				t.Fatalf("not parsed as a set operation")
			}
			if got := len(info.Union.Left.Windows); got != tc.left {
				t.Errorf("left arm carries %d window specs, want %d — the builder gates window "+
					"planning on this list, so an arm missing it plans no Window node (#733)", got, tc.left)
			}
			if got := len(info.Union.Right.Windows); got != tc.right {
				t.Errorf("right arm carries %d window specs, want %d", got, tc.right)
			}
		})
	}
}

// A chain of set operations parses left-deep, so the leftmost arm is two
// levels down. Its windows have to reach it too.
func TestAWindowInTheLeftmostArmOfAUnionChainIsCollected(t *testing.T) {
	parsed, err := Parse("SELECT SUM(a) OVER () AS s FROM t UNION ALL SELECT b FROM t UNION ALL SELECT c FROM t")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	inner := info.Union.Left
	if inner.Union == nil {
		t.Fatalf("expected the left arm to be a nested set operation, got a plain SELECT")
	}
	if got := len(inner.Union.Left.Windows); got != 1 {
		t.Errorf("the chain's leftmost arm carries %d window specs, want 1", got)
	}
}
