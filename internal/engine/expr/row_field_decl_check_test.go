package expr

import (
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
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
