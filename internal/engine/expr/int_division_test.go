package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestIntegerDivisionLiterals pins PostgreSQL's integer division on literal
// operands: int / int truncates toward zero (#369, ADR-0012). Float operands
// on either side keep float division — FloatDivisionControl in the oracle
// corpus is the entry that must not move.
func TestIntegerDivisionLiterals(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want any
	}{
		{"7/2", int64(3)},
		{"(-7)/2", int64(-3)},
		{"7/(-2)", int64(-3)},
		{"(-7)/(-2)", int64(3)},
		{"7%3", int64(1)},
		{"6/3", int64(2)},
		// A float operand makes it float division in both engines.
		{"7.0/2", 3.5},
		{"7/2.0", 3.5},
		{"7.0/2.0", 3.5},
		// Division by zero stays NULL (wadjet has no error channel here;
		// PostgreSQL raises 22012, pinned separately on the wire arm).
		{"7/0", nil},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			if got := e.Eval(b, 0); got != c.want {
				t.Errorf("Eval(%q) = %#v (%T), want %#v", c.sql, got, got, c.want)
			}
		})
	}
}

// intDivBatch is two int64 columns a = [7, -7, 0, NULL] over b-col = 2, one
// int32 column, and one float64 column.
func intDivBatch() *batch.RecordBatch {
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeInt64},
		{Name: "b", Type: parquet.TypeInt32},
		{Name: "f", Type: parquet.TypeFloat64},
	}
	b := batch.NewRecordBatch(schema, 4)
	for i, v := range []any{int64(7), int64(-7), int64(0), nil} {
		b.Columns[0].SetValue(i, v)
	}
	for i, v := range []any{int32(2), int32(2), int32(2), int32(2)} {
		b.Columns[1].SetValue(i, v)
	}
	for i, v := range []any{7.0, -7.0, 0.0, 7.0} {
		b.Columns[2].SetValue(i, v)
	}
	b.Len = 4
	return b
}

// TestIntegerDivisionOverColumns pins integer division through the
// mode-resolved column path (BinOpNumeric): integer-typed columns divide as
// integers; a float column on either side keeps float division.
func TestIntegerDivisionOverColumns(t *testing.T) {
	b := intDivBatch()
	cases := []struct {
		sql  string
		row  int
		want any
	}{
		{"a / 2", 0, int64(3)},
		{"a / 2", 1, int64(-3)},
		{"a / 2", 2, int64(0)},
		{"a / 2", 3, nil}, // NULL propagates
		{"a / b", 0, int64(3)},
		{"a / b", 1, int64(-3)},
		{"(a + 1) / 2", 0, int64(4)},
		{"a / 0", 0, nil}, // division by zero → NULL
		// Float operand on either side: float division, unchanged.
		{"f / 2", 0, 3.5},
		{"a / 2.0", 0, 3.5},
		{"f / b", 1, -3.5},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			got := e.Eval(b, c.row)
			// The int-arith kill switch moves integer results between int64
			// and float64 REPRESENTATIONS; it must never move the VALUE
			// (3 vs 3.5 is semantics — divTrunc). With the switch on, an
			// integer answer must be int64.
			wantInt, wantIsInt := c.want.(int64)
			if wantIsInt {
				switch g := got.(type) {
				case int64:
					if g != wantInt {
						t.Errorf("Eval(%q) row %d = %d, want %d", c.sql, c.row, g, wantInt)
					}
					return
				case float64:
					if IntArithOn() {
						t.Errorf("Eval(%q) row %d = %#v (float64), want int64 %d with the int-arith switch on", c.sql, c.row, g, wantInt)
					} else if g != float64(wantInt) {
						t.Errorf("Eval(%q) row %d = %v, want value %d", c.sql, c.row, g, wantInt)
					}
					return
				}
			}
			if got != c.want {
				t.Errorf("Eval(%q) row %d = %#v (%T), want %#v", c.sql, c.row, got, got, c.want)
			}
		})
	}
}

// TestUnaryMinusPreservesInteger: negating an integer stays an integer, which
// is what routes (-7)/2 onto the integer division path.
func TestUnaryMinusPreservesInteger(t *testing.T) {
	b := intDivBatch()
	e := compileExprSQL(t, "-a")
	if got := e.Eval(b, 0); got != int64(-7) {
		t.Errorf("Eval(-a) = %#v (%T), want int64(-7)", got, got)
	}
	if got := e.Eval(b, 3); got != nil {
		t.Errorf("Eval(-a) over NULL = %#v, want nil", got)
	}
	ef := compileExprSQL(t, "-f")
	if got := ef.Eval(b, 0); got != -7.0 {
		t.Errorf("Eval(-f) = %#v (%T), want -7.0", got, got)
	}
}

// TestGenericBinOpIntegerDivision pins integer division on the generic boxed
// BinOp node, which is where operands that carry no typed protocol (a CAST,
// a negated column) arrive.
func TestGenericBinOpIntegerDivision(t *testing.T) {
	b := testBatch()
	cases := []struct {
		sql  string
		want any
	}{
		{"CAST(7 AS integer) / 2", int64(3)},
		{"CAST('7' AS integer) / CAST('2' AS integer)", int64(3)},
	}
	for _, c := range cases {
		t.Run(c.sql, func(t *testing.T) {
			e := compileExprSQL(t, c.sql)
			if got := e.Eval(b, 0); got != c.want {
				t.Errorf("Eval(%q) = %#v (%T), want %#v", c.sql, got, got, c.want)
			}
		})
	}
}
