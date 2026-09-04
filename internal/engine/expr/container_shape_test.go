package expr

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// csBatch is a one-row batch carrying the container shapes #669 and #635 are
// about: an ARRAY of ARRAY of DECIMAL(9,2), a MAP whose KEY is DECIMAL(18,4),
// and a MAP with string keys beside it as the control.
func csBatch(tb testing.TB) *batch.RecordBatch {
	tb.Helper()
	schema := []parquet.Column{
		{Name: "aa", Type: parquet.TypeArray, Nullable: true, ElementType: &parquet.Column{
			Name: "element", Type: parquet.TypeArray, Nullable: true, ElementType: &parquet.Column{
				Name: "element", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true}}},
		{Name: "mk", Type: parquet.TypeMap, Nullable: true, ElementType: &parquet.Column{
			Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeDecimal, Precision: 18, Scale: 4},
				{Name: "value", Type: parquet.TypeString, Nullable: true},
			}}},
		{Name: "ms", Type: parquet.TypeMap, Nullable: true, ElementType: &parquet.Column{
			Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeString, Nullable: true},
			}}},
	}
	return batch.FromRows(schema, []map[string]any{{
		"aa": []any{[]any{12.75, 1.5}, []any{2.25}},
		"mk": map[string]any{"12.75": "twelve", "1.5": "one-five"},
		"ms": map[string]any{"1.50": "text-key"},
	}})
}

func csEval(tb testing.TB, b *batch.RecordBatch, sql string) any {
	tb.Helper()
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		tb.Fatalf("parse %q: %v", sql, err)
	}
	e, err := Compile(node)
	if err != nil {
		tb.Fatalf("compile %q: %v", sql, err)
	}
	return e.Eval(b, 0)
}

// #669 item 2: a MAP with a DECIMAL key never matched, because the lookup key
// was spelled as the query wrote it and the stored key at the key column's
// own scale — "12.75" against "12.7500" at (18,4). Two spellings of one number
// are one key (ADR-0012 item 8, the rule AppendDecimalKey already applies to a
// stored DECIMAL).
//
// #635: element_at over a container expression that is not a bare column
// routed to POSITIONAL array indexing and answered NULL.
func TestElementAtResolvesTheContainerItIsGiven(t *testing.T) {
	b := csBatch(t)
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		// The DECIMAL key, in both spellings and at both scales.
		{"decimal_key_unquoted", `element_at(mk, 12.75)`, "twelve"},
		{"decimal_key_quoted", `element_at(mk, '12.75')`, "twelve"},
		{"decimal_key_at_the_columns_scale", `element_at(mk, 12.7500)`, "twelve"},
		{"decimal_key_trailing_zero_spelling", `element_at(mk, 1.50)`, "one-five"},
		{"decimal_key_absent", `element_at(mk, 9.99)`, nil},
		// A STRING-keyed map keeps matching by its BYTES: "1.50" and "1.5"
		// are two keys there, and reading them as numbers would be the same
		// defect pointing the other way.
		{"string_key_exact", `element_at(ms, '1.50')`, "text-key"},
		{"string_key_other_spelling", `element_at(ms, '1.5')`, nil},
		// #635: the container is a COALESCE, a CASE, a GREATEST — not a
		// column. Each answered NULL before, by indexing the entry rows.
		{"map_under_coalesce", `element_at(COALESCE(ms, ms), '1.50')`, "text-key"},
		{"map_under_case", `element_at(CASE WHEN 1 = 1 THEN ms ELSE ms END, '1.50')`, "text-key"},
		{"map_under_nullif", `element_at(NULLIF(ms, ms), '1.50')`, nil},
		{"decimal_key_map_under_coalesce", `element_at(COALESCE(mk, mk), 12.75)`, "twelve"},
		// A nested ARRAY still indexes positionally at every depth.
		{"nested_array", `element_at(element_at(aa, 1), 1)`, "12.75"},
		{"nested_array_second", `element_at(element_at(aa, 1), 2)`, "1.50"},
		{"nested_array_outer_second", `element_at(element_at(aa, 2), 1)`, "2.25"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := csEval(t, b, c.sql); got != c.want {
				t.Errorf("%s = %#v, want %#v", c.sql, got, c.want)
			}
		})
	}
}

// #669 item 1: the element KIND is read from the container's declared element
// vector at any depth, so a DECIMAL element compares as the number it is.
// `GREATEST(element_at(element_at(aa,1),1), '2')` answered "2" — the byte
// order of two rendered numbers, where "2" sorts above "12.75".
func TestNestedContainerElementIsClassifiedByItsDeclaredType(t *testing.T) {
	b := csBatch(t)
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		{"greatest_nested_decimal_element", `GREATEST(element_at(element_at(aa, 1), 1), '2')`, "12.75"},
		{"least_nested_decimal_element", `LEAST(element_at(element_at(aa, 1), 1), '2')`, "2"},
		{"greatest_map_decimal_value_is_text", `GREATEST(element_at(ms, '1.50'), 'a')`, "text-key"},
	} {
		t.Run(c.name, func(t *testing.T) {
			if got := csEval(t, b, c.sql); got != c.want {
				t.Errorf("%s = %#v, want %#v (live PostgreSQL 17.11 compares the numbers)", c.sql, got, c.want)
			}
		})
	}

	// The classification itself, at the seam: boxDecimal for the nested
	// element, and boxUnknown for a container whose shape nothing declares.
	if k, settled := elementOperandKind(mustCompileExpr(t, `element_at(aa, 1)`).(*elementAtExpr).arg0, b); !settled {
		t.Errorf("a resolved column's element kind is unsettled")
	} else if k != boxUnknown {
		// element_at(aa,1) lifts an ARRAY out of an ARRAY, and an ARRAY has
		// no comparison kind of its own.
		t.Errorf("the element of an ARRAY of ARRAY classified %v, want boxUnknown", k)
	}
	inner := mustCompileExpr(t, `element_at(element_at(aa, 1), 1)`).(*elementAtExpr)
	if k, settled := elementOperandKind(inner.arg0, b); !settled || k != boxDecimal {
		t.Errorf("the DECIMAL element two levels down classified %v (settled=%v), want boxDecimal",
			k, settled)
	}
}

func mustCompileExpr(tb testing.TB, sql string) Expr {
	tb.Helper()
	node, err := plansql.ParseExpression(sql)
	if err != nil {
		tb.Fatalf("parse %q: %v", sql, err)
	}
	e, err := Compile(node)
	if err != nil {
		tb.Fatalf("compile %q: %v", sql, err)
	}
	return e
}
