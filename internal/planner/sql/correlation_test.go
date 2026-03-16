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
