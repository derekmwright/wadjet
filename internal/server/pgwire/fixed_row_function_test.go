package pgwire

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func TestFixedRowFunctionWire(t *testing.T) {
	fields := []parquet.Column{{Name: "a", Type: parquet.TypeInt64}, {Name: "b", Type: parquet.TypeString}}
	expr.RegisterFunc("a3_fixed_row", func(_ []any) any { return map[string]any{"a": int64(1), "b": "x"} }, expr.RetRow(fields))
	t.Cleanup(func() { expr.DefaultRegistry.Unregister("a3_fixed_row") })
	_, srv := setupRealDB(t)
	c := connectPgconn(t, srv.Addr())
	for _, f := range []int16{0, 1} {
		r := c.ExecParams(context.Background(), `SELECT a3_fixed_row(id) AS r FROM users LIMIT 1`, nil, nil, nil, []int16{f}).Read()
		if r.Err != nil || len(r.Rows) != 1 || len(r.FieldDescriptions) != 1 {
			t.Fatalf("result %v rows=%q", r.Err, r.Rows)
		}
		if r.FieldDescriptions[0].DataTypeOID != 25 || string(r.Rows[0][0]) != "(1,x)" {
			t.Fatalf("OID=%d row=%q", r.FieldDescriptions[0].DataTypeOID, r.Rows[0])
		}
	}
}
