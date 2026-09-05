package mcp

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// dupNameDB is one row whose two columns hold DIFFERENT values, so a cell that
// carries its neighbour's value is visible in the JSON rather than merely
// unproven: abs(a) is 1 and abs(b) is 2.
func dupNameDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt64},
	}}
	if err := db.Catalog().CreateTable(ctx, "dupname", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("dupname", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{{"a": int64(-1), "b": int64(-2)}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// TestMCPQueryRowsArePositional is the regression gate for the MCP query
// tool boxing its compact row array by column NAME.
//
// `SELECT abs(a), abs(b)` is TWO output columns both called `abs` —
// PostgreSQL names them that way and #513 made this engine agree — and
// QueryResult.Rows is a map, which can hold only one of them. Building the
// compact array as `vals[j] = row[col]` therefore read the SAME map entry for
// both positions: over a = -1, b = -2 the tool answered
// `{"columns":["abs","abs"],"rows":[[2,2]]}`, where the second column's value
// is reported under the first column's position and abs(a) = 1 appears
// nowhere. QueryResult.Cells reads the positional form the engine
// materialises exactly for this case.
//
// The class is #513's and it is already closed on the gRPC door
// (rowsToProtoWithValues) and the HTTP one; `wadjet mcp` is the third shipped
// door and was still open.
func TestMCPQueryRowsArePositional(t *testing.T) {
	ctx := context.Background()
	srv := NewServer(dupNameDB(t), nil)

	for _, tt := range []struct {
		name    string
		sql     string
		columns []string
		rows    [][]float64
	}{
		{
			name: "two output columns share one name",
			sql:  "SELECT abs(a), abs(b) FROM dupname",
			// Both named `abs`, so the map form loses one of them.
			columns: []string{"abs", "abs"},
			rows:    [][]float64{{1, 2}},
		},
		{
			// The control: unique names, where Rows is exact and Cells falls
			// back to the same map lookup. The payload must be unchanged.
			name:    "unique names are unchanged",
			sql:     "SELECT a, b FROM dupname",
			columns: []string{"a", "b"},
			rows:    [][]float64{{-1, -2}},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			out := mcpQueryJSON(t, srv, ctx, tt.sql)

			var gotCols []string
			if err := json.Unmarshal(out["columns"], &gotCols); err != nil {
				t.Fatalf("columns: %v", err)
			}
			if len(gotCols) != len(tt.columns) {
				t.Fatalf("columns = %v, want %v", gotCols, tt.columns)
			}
			for i := range gotCols {
				if gotCols[i] != tt.columns[i] {
					t.Fatalf("columns = %v, want %v", gotCols, tt.columns)
				}
			}

			var gotRows [][]float64
			if err := json.Unmarshal(out["rows"], &gotRows); err != nil {
				t.Fatalf("rows: %v", err)
			}
			if len(gotRows) != len(tt.rows) {
				t.Fatalf("rows = %v, want %v", gotRows, tt.rows)
			}
			for i := range gotRows {
				if len(gotRows[i]) != len(tt.rows[i]) {
					t.Fatalf("row %d = %v, want %v — one cell per DECLARED column",
						i, gotRows[i], tt.rows[i])
				}
				for j := range gotRows[i] {
					if gotRows[i][j] != tt.rows[i][j] {
						t.Fatalf("rows = %v, want %v: cell [%d][%d] carries %v, so the compact "+
							"array was boxed by column NAME and a duplicate name collapsed two "+
							"columns onto one value", gotRows, tt.rows, i, j, gotRows[i][j])
					}
				}
			}
		})
	}
}

// mcpQueryJSON runs the query tool and returns its JSON payload, which is the
// document an MCP client actually reads.
func mcpQueryJSON(t *testing.T, srv *Server, ctx context.Context, sql string) map[string]json.RawMessage {
	t.Helper()
	res := srv.toolQuery(ctx, map[string]any{"sql": sql})
	if res.IsError || len(res.Content) == 0 {
		t.Fatalf("query tool refused %q: %+v", sql, res.Content)
	}
	var out map[string]json.RawMessage
	if err := json.Unmarshal([]byte(res.Content[0].Text), &out); err != nil {
		t.Fatalf("query tool output for %q is not JSON: %v\n%s", sql, err, res.Content[0].Text)
	}
	return out
}
