package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestConcatOperator pins `||` as string concatenation (#328). It used to
// compile to a generic BinOp whose Eval knows only arithmetic and answered
// NULL for every row, with no error anywhere — and when BOTH operands were
// columns it took the numeric path instead, since a column satisfies
// Float64Expr regardless of its type.
func TestConcatOperator(t *testing.T) {
	b := batch.NewRecordBatch([]parquet.Column{
		{Name: "fname", Type: parquet.TypeString},
		{Name: "lname", Type: parquet.TypeString},
		{Name: "n", Type: parquet.TypeInt64},
	}, 3)
	fnames := []string{"ada", "alan", ""}
	lnames := []string{"lovelace", "turing", "x"}
	for i := range fnames {
		b.Columns[0].SetValue(i, fnames[i])
		b.Columns[1].SetValue(i, lnames[i])
		b.Columns[2].SetValue(i, int64(i+1))
	}
	b.Columns[1].Nulls.SetNull(2) // NULL || 'x' is NULL in SQL

	tests := []struct {
		name string
		sql  string
		want []any
	}{
		{"column and literal", "fname || '!'",
			[]any{"ada!", "alan!", "!"}},
		{"two columns — the case that took the numeric path", "fname || lname",
			[]any{"adalovelace", "alanturing", nil}},
		{"three operands", "fname || '-' || fname",
			[]any{"ada-ada", "alan-alan", "-"}},
		{"numeric operand coerces to text", "fname || n",
			[]any{"ada1", "alan2", "3"}},
		{"NULL propagates", "lname || '!'",
			[]any{"lovelace!", "turing!", nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(tt.sql)
			if err != nil {
				t.Fatalf("parsing %q: %v", tt.sql, err)
			}
			e, err := Compile(node)
			if err != nil {
				t.Fatalf("compiling %q: %v", tt.sql, err)
			}
			for row, want := range tt.want {
				got := e.Eval(b, row)
				if got != want {
					t.Errorf("%s row %d = %#v, want %#v", tt.sql, row, got, want)
				}
			}
		})
	}
}
