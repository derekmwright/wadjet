package physical

import (
	"context"
	"fmt"
	"strings"
	"testing"

	plansql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// fakeCatalog is a minimal tableColumnSource for binder tests.
type fakeCatalog struct {
	tables map[string][]string
}

func (f *fakeCatalog) GetTable(_ context.Context, name string) (*catalog.TableMeta, error) {
	cols, ok := f.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("table %q not found", name)
	}
	schema := parquet.Schema{}
	for _, c := range cols {
		schema.Columns = append(schema.Columns, parquet.Column{Name: c})
	}
	return &catalog.TableMeta{Name: name, Schema: schema}, nil
}

func mustExtract(t *testing.T, sql string) *plansql.SelectInfo {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	return info
}

func TestValidateColumns(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{
		"events": {"id", "ts", "attrs", "region", "amount"},
		"other":  {"eid", "val"},
	}}

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// --- Issue #147 core targets ---
		{"where typo", "SELECT id FROM events WHERE nosuchcol > 90", true},
		{"select typo (legacy projection path)", "SELECT nosuchcol FROM events", true},
		{"dotted ROW access resolves", "SELECT attrs.score FROM events", false},
		{"where valid", "SELECT id FROM events WHERE id > 90", false},

		// --- qualified references ---
		{"qualified typo on known table", "SELECT events.nosuchcol FROM events", true},
		{"qualified valid", "SELECT events.id FROM events", false},
		{"alias qualified valid", "SELECT e.id FROM events e", false},
		{"alias qualified typo", "SELECT e.nosuchcol FROM events e", true},

		// --- clauses ---
		{"group by typo", "SELECT region FROM events GROUP BY nope", true},
		{"group by valid", "SELECT region FROM events GROUP BY region", false},
		{"group by alias", "SELECT id AS x FROM events GROUP BY x", false},
		{"group by ordinal", "SELECT id, count(*) FROM events GROUP BY 1", false},
		{"order by typo", "SELECT id FROM events ORDER BY nope", true},
		{"order by alias", "SELECT id AS x FROM events ORDER BY x", false},
		{"order by table col not selected", "SELECT id FROM events ORDER BY ts", false},
		{"having typo", "SELECT region, count(*) c FROM events GROUP BY region HAVING nope > 5", true},
		{"having valid", "SELECT region, count(*) c FROM events GROUP BY region HAVING count(*) > 5", false},
		{"agg arg typo", "SELECT sum(nosuchcol) FROM events", true},
		{"agg arg valid", "SELECT sum(amount) FROM events", false},

		// --- joins / multi-table ---
		{"join bare cols valid", "SELECT id, val FROM events JOIN other ON id = eid", false},
		{"join qualified valid", "SELECT a.id, b.val FROM events a JOIN other b ON a.id = b.eid", false},
		{"join bare typo", "SELECT id, badcol FROM events a JOIN other b ON a.id = b.eid", true},

		// --- open schema (escape hatch) ---
		{"unregistered table is open", "SELECT anything FROM mystery_table", false},
		{"unregistered table qualified", "SELECT m.anything FROM mystery_table m", false},

		// --- CTEs ---
		{"cte output valid", "WITH c AS (SELECT id FROM events) SELECT id FROM c", false},
		{"cte output typo", "WITH c AS (SELECT id FROM events) SELECT nosuchcol FROM c", true},
		{"cte body typo", "WITH c AS (SELECT nosuchcol FROM events) SELECT id FROM c", true},
		{"cte explicit columns", "WITH c(x) AS (SELECT id FROM events) SELECT x FROM c", false},

		// --- derived tables ---
		{"derived output valid", "SELECT id FROM (SELECT id FROM events) d", false},
		{"derived output typo", "SELECT nosuchcol FROM (SELECT id FROM events) d", true},
		{"derived body typo", "SELECT id FROM (SELECT nosuchcol FROM events) d", true},
		{"derived star is open", "SELECT nosuchcol FROM (SELECT * FROM events) d", false},

		// --- subqueries / correlation ---
		{"correlated outer ref ok", "SELECT id FROM events e WHERE EXISTS (SELECT 1 FROM other o WHERE o.eid = e.id)", false},
		{"subquery internal typo", "SELECT id FROM events WHERE id IN (SELECT nosuchcol FROM other)", true},

		// --- star / table-less ---
		{"select star", "SELECT * FROM events", false},
		{"select star plus valid", "SELECT *, region FROM events", false},
		{"table-less select literal", "SELECT 1 + 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := mustExtract(t, tt.sql)
			err := validateColumns(context.Background(), cat, info)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for %q, got nil", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for %q: %v", tt.sql, err)
			}
		})
	}
}

func TestValidateColumnsErrorNamesColumn(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{"events": {"id", "ts"}}}
	info := mustExtract(t, "SELECT id FROM events WHERE typocol > 1")
	err := validateColumns(context.Background(), cat, info)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "typocol") {
		t.Errorf("error should name the column, got: %v", err)
	}
}
