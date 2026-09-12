package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

func TestExplicitRowFieldKeepsItsMapFunctionArgument(t *testing.T) {
	RegisterFunc("a3b_map", func(_ []any) any { return map[string]any{"major": int64(7)} }, RetMap)
	defer DefaultRegistry.Unregister("a3b_map")
	ast, err := plansql.ParseExpression("row_field(a3b_map(), 'major')")
	if err != nil {
		t.Fatal(err)
	}
	compiled, err := Compile(ast)
	if err != nil {
		t.Fatal(err)
	}
	if got := compiled.Eval(nil, 0); got != int64(7) {
		t.Fatalf("explicit row_field over MAP = %v", got)
	}
}

func TestPostfixFieldCompilerUsesResolvedTypes(t *testing.T) {
	for _, tc := range []struct{ sql, typ string }{
		{"(coalesce(v,v)).major", "text"},
		{"(length(v)).major", "integer"},
		{"(least(id,id)).major", "bigint"},
		{"(greatest(id,id)).major", "bigint"},
		{"(nullif(id,id)).major", "bigint"},
		{"(count(*)).major", "bigint"},
		{"(json_extract(v,'$.a')).major", "text"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			ast, e := plansql.ParseExpression(tc.sql)
			if e != nil {
				t.Fatal(e)
			}
			_, e = CompileWithColumnTypes(ast, nil, map[string]batch.TypeID{"v": batch.TypeString, "id": batch.TypeInt64})
			want := "column notation .major applied to type " + tc.typ + ", which is not a composite type"
			if sqlerr.StateOf(e) != "42809" || e.Error() != want {
				t.Fatalf("got %v want 42809 %s", e, want)
			}
		})
	}
}
