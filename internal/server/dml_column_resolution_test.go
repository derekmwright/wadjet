package server

import (
	"context"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The HTTP twin of wadjet.TestDMLUnknownColumnIsUndefinedColumn and
// TestDMLSetExpressionIsEvaluated. These executors are a second copy of the
// embedded ones and carried both defects (#678).
func TestHTTPDMLColumnResolution(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		sql   string
		state string // "" means the statement must SUCCEED
		want  any    // expected value of column s or n afterwards
		col   string
	}{
		{name: "UPDATE SET target", sql: "UPDATE r678 SET nosuchcol = 1", state: "42703"},
		{name: "UPDATE WHERE", sql: "UPDATE r678 SET n = 2 WHERE nosuchcol = 1", state: "42703"},
		{name: "DELETE WHERE", sql: "DELETE FROM r678 WHERE nosuchcol = 1", state: "42703"},
		{name: "SET expression over the column being set",
			sql: "UPDATE r678 SET s = UPPER(s)", col: "s", want: "AB"},
		{name: "SET arithmetic", sql: "UPDATE r678 SET n = n + 1", col: "n", want: int64(11)},
		{name: "SET string literal", sql: "UPDATE r678 SET s = 'literal'", col: "s", want: "literal"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cat := visibilityCatalog(t)
			schema := parquet.Schema{Columns: []parquet.Column{
				{Name: "id", Type: parquet.TypeInt64},
				{Name: "n", Type: parquet.TypeInt64, Nullable: true},
				{Name: "s", Type: parquet.TypeString, Nullable: true},
			}}
			if err := cat.CreateTable(ctx, "r678", schema, nil); err != nil {
				t.Fatal(err)
			}
			ing := ingest.New(cat, "r678", schema, nil, ingest.DefaultConfig())
			if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "n": int64(10), "s": "ab"}}); err != nil {
				t.Fatal(err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}

			parsed, err := plansql.Parse(tc.sql)
			if err != nil {
				t.Fatalf("parsing %q: %v", tc.sql, err)
			}
			_, execErr := runHTTPDML(ctx, cat, parsed)
			if tc.state != "" {
				if execErr == nil {
					t.Fatalf("%s succeeded; want %s", tc.sql, tc.state)
				}
				if got := sqlerr.StateOf(execErr); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, execErr)
				}
				return
			}
			if execErr != nil {
				t.Fatalf("%s: %v", tc.sql, execErr)
			}
			live := liveRows(t, cat, "r678", schema)
			if len(live) != 1 {
				t.Fatalf("%d live rows after %s, want 1: %v", len(live), tc.sql, live)
			}
			if got := live[0][tc.col]; got != tc.want {
				t.Errorf("%s: %s = %v (%T), want %v", tc.sql, tc.col, got, got, tc.want)
			}
		})
	}
}
