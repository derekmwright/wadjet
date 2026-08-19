package sql

import (
	"testing"
)

func TestFindCorrelatedRefs(t *testing.T) {
	outerTables := map[string]bool{"o": true, "orders": true}

	tests := []struct {
		name    string
		sql     string
		want    int // expected number of outer refs
		wantCol string
	}{
		{
			name:    "uncorrelated",
			sql:     "SELECT AVG(amount) FROM orders",
			want:    0,
			wantCol: "",
		},
		{
			name:    "correlated WHERE",
			sql:     "SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id",
			want:    1,
			wantCol: "customer_id",
		},
		{
			name:    "correlated multiple refs",
			sql:     "SELECT 1 FROM items WHERE items.order_id = o.id AND items.status = o.status",
			want:    2,
			wantCol: "id",
		},
		{
			name:    "inner table same name as outer",
			sql:     "SELECT 1 FROM orders WHERE id = 1",
			want:    0,
			wantCol: "",
		},
		{
			name:    "correlated EXISTS pattern",
			sql:     "SELECT 1 FROM line_items li WHERE li.order_id = o.id",
			want:    1,
			wantCol: "id",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refs, err := FindCorrelatedRefs(tt.sql, outerTables)
			if err != nil {
				t.Fatalf("FindCorrelatedRefs: %v", err)
			}
			if len(refs) != tt.want {
				t.Fatalf("got %d refs, want %d; refs=%+v", len(refs), tt.want, refs)
			}
			if tt.want > 0 && refs[0].Column != tt.wantCol {
				t.Fatalf("got column %q, want %q", refs[0].Column, tt.wantCol)
			}
		})
	}
}

// TestFindCorrelatedRefsInnerScopeWins pins the SQL scoping rule for an
// unqualified name inside a subquery: it binds to the subquery's own FROM
// first, and only a name the subquery does NOT supply is a reference to the
// outer query.
//
// Regression for issue #334. Before the fix the only shadowing check compared
// the OUTER table's identifier against the INNER table names, so
//
//	SELECT COUNT(*) FROM customer c1
//	WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer)
//
// was reported correlated purely because the outer table carried an alias:
// with `FROM customer` (identifier "customer") the check happened to match an
// inner table of the same name and the query escaped; with `FROM customer c1`
// (identifier "c1") it did not. The subquery was then re-executed once per
// outer row — and, on a parallel pipeline, concurrently re-planned through a
// shared planner, killing the process with "concurrent map writes".
func TestFindCorrelatedRefsInnerScopeWins(t *testing.T) {
	// The subquery's own FROM supplies these.
	customerCols := TableColumns(func(table string) []string {
		if table == "customer" {
			return []string{"c_custkey", "c_acctbal", "c_nationkey"}
		}
		return nil
	})

	tests := []struct {
		name        string
		outerTables map[string]bool
		outerCols   map[string]string
		innerCols   TableColumns
		sql         string
		want        int
	}{
		{
			// The reported bug: outer table aliased, inner name supplied by
			// the subquery's own FROM. Not correlated.
			name:        "aliased outer, inner FROM supplies the name",
			outerTables: map[string]bool{"c1": true, "customer": true},
			outerCols:   map[string]string{"c_acctbal": "c1", "c_nationkey": "c1"},
			innerCols:   customerCols,
			sql:         "SELECT AVG(c_acctbal) FROM customer",
			want:        0,
		},
		{
			// Same query without a resolver: the old identifier-comparison
			// fallback still calls it correlated. Pinned so the fallback's
			// limit is visible rather than assumed absent.
			name:        "aliased outer, no resolver, falls back",
			outerTables: map[string]bool{"c1": true, "customer": true},
			outerCols:   map[string]string{"c_acctbal": "c1"},
			innerCols:   nil,
			sql:         "SELECT AVG(c_acctbal) FROM customer",
			want:        1,
		},
		{
			// Unaliased outer: this shape escaped even before the fix,
			// because "customer" matched an inner table by spelling.
			name:        "unaliased outer stays uncorrelated",
			outerTables: map[string]bool{"customer": true},
			outerCols:   map[string]string{"c_acctbal": "customer"},
			innerCols:   customerCols,
			sql:         "SELECT AVG(c_acctbal) FROM customer",
			want:        0,
		},
		{
			// Qualifying the inner table always worked — the qualified branch
			// never consults the outer column map.
			name:        "inner table aliased and qualified",
			outerTables: map[string]bool{"c1": true, "customer": true},
			outerCols:   map[string]string{"c_acctbal": "c1"},
			innerCols:   customerCols,
			sql:         "SELECT AVG(sq.c_acctbal) FROM customer sq",
			want:        0,
		},
		{
			// The scoping fix must not over-correct: a QUALIFIED reference to
			// the outer table is correlated no matter what the inner FROM
			// supplies, and c_nationkey is supplied by both scopes here.
			name:        "qualified outer ref stays correlated",
			outerTables: map[string]bool{"c1": true, "customer": true},
			outerCols:   map[string]string{"c_nationkey": "c1"},
			innerCols:   customerCols,
			sql:         "SELECT AVG(c_acctbal) FROM customer WHERE c_nationkey = c1.c_nationkey",
			want:        1,
		},
		{
			// An unqualified name the inner FROM does NOT supply is a genuine
			// outer reference and must still be reported.
			name:        "unqualified name absent from inner FROM is correlated",
			outerTables: map[string]bool{"o1": true, "orders": true},
			outerCols:   map[string]string{"o_orderkey": "o1"},
			innerCols:   customerCols,
			sql:         "SELECT AVG(c_acctbal) FROM customer WHERE c_custkey = o_orderkey",
			want:        1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refs, err := FindCorrelatedRefsWithScope(tt.sql, tt.outerTables, tt.outerCols, tt.innerCols)
			if err != nil {
				t.Fatalf("FindCorrelatedRefsWithScope: %v", err)
			}
			if len(refs) != tt.want {
				t.Fatalf("got %d outer refs, want %d; refs=%+v", len(refs), tt.want, refs)
			}
		})
	}
}

func TestRewriteOuterRefs(t *testing.T) {
	outerTables := map[string]bool{"o": true}
	vals := map[string]any{
		"o.customer_id": int64(42),
	}

	// Parse a WHERE expression with an outer reference
	expr, err := ParseExpression("customer_id = o.customer_id")
	if err != nil {
		t.Fatal(err)
	}

	rewritten := RewriteOuterRefs(expr, outerTables, vals)
	result := rewritten.String()
	if result != "customer_id = 42" {
		t.Fatalf("expected 'customer_id = 42', got %q", result)
	}
}

func TestRewriteOuterRefsNull(t *testing.T) {
	outerTables := map[string]bool{"o": true}
	vals := map[string]any{
		"o.id": nil,
	}

	expr, err := ParseExpression("order_id = o.id")
	if err != nil {
		t.Fatal(err)
	}

	rewritten := RewriteOuterRefs(expr, outerTables, vals)
	result := rewritten.String()
	if result != "order_id = null" {
		t.Fatalf("expected 'order_id = null', got %q", result)
	}
}

func TestRewriteOuterRefsString(t *testing.T) {
	outerTables := map[string]bool{"o": true}
	vals := map[string]any{
		"o.status": "active",
	}

	expr, err := ParseExpression("status = o.status")
	if err != nil {
		t.Fatal(err)
	}

	rewritten := RewriteOuterRefs(expr, outerTables, vals)
	result := rewritten.String()
	if result != "status = 'active'" {
		t.Fatalf("expected \"status = 'active'\", got %q", result)
	}
}

func TestRebuildSQL(t *testing.T) {
	parsed, err := Parse("SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id")
	if err != nil {
		t.Fatal(err)
	}
	info, err := ExtractSelect(parsed)
	if err != nil {
		t.Fatal(err)
	}

	// Rewrite the WHERE clause
	outerTables := map[string]bool{"o": true}
	vals := map[string]any{"o.customer_id": int64(42)}
	rewrittenWhere := RewriteOuterRefs(info.WhereExpr, outerTables, vals)

	sql := RebuildSQL(info, rewrittenWhere)

	// Should contain the rewritten WHERE clause
	if sql != "SELECT avg(amount) FROM orders WHERE customer_id = 42" {
		t.Fatalf("unexpected rebuilt SQL: %q", sql)
	}
}
