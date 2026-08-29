package expr

import (
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// filterColumnsBatch carries the four spellings the guard has to tell apart:
// a plain column, a column that is entirely NULL, a ROW column reached by a
// field path, and nothing at all.
func filterColumnsBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "c_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c_null", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
	}
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, int64(1))
	b.Columns[0].SetValue(1, int64(2))
	b.Columns[1].SetValue(0, nil)
	b.Columns[1].SetValue(1, nil)
	b.Columns[2].SetValue(0, map[string]any{"a": "x", "b": int64(9)})
	b.Columns[2].SetValue(1, nil)
	b.Len = 2
	return b
}

// TestCheckFilterColumnsRefusesOnlyAbsentNames is the row path's half of the
// #147 guard: an absent NAME is a query error, and a column whose VALUES are
// all NULL is not. Collapsing the two would make the guard fire on ordinary
// data, which is why it is a schema test and never a value test.
func TestCheckFilterColumnsRefusesOnlyAbsentNames(t *testing.T) {
	b := filterColumnsBatch(t)
	for _, c := range []struct {
		name    string
		refs    []string
		wantErr string
	}{
		{"present", []string{"c_i64"}, ""},
		{"allNull", []string{"c_null"}, ""},
		{"qualified", []string{"t.c_i64"}, ""},
		{"rowFieldPath", []string{"c_row.b"}, ""},
		{"rowContainer", []string{"c_row"}, ""},
		{"none", []string{}, ""},
		{"absentBare", []string{"v"}, `"v"`},
		{"absentQualified", []string{"c.v"}, `"c.v"`},
		{"absentRowField", []string{"c_row.nosuch"}, ""},
		{"absentAmongPresent", []string{"c_i64", "c_null", "v"}, `"v"`},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := CheckFilterColumns(b, c.refs)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("CheckFilterColumns(%v) = %v, want no error", c.refs, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("CheckFilterColumns(%v) returned no error; an absent column is the "+
					"#147 failure mode and must be loud", c.refs)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error %q does not name the missing column %s", err, c.wantErr)
			}
			if got := sqlerr.StateOf(err); got != "42703" {
				t.Fatalf("SQLSTATE is %q, want 42703 (undefined_column)", got)
			}
		})
	}
}

// A ROW field path resolves to its CONTAINER, which is what ColRef does — the
// field's own existence is settled per row by fieldVector, not by the schema.
// This pins that the guard does not overreach into it: a wrong field name is a
// separate defect class (#604) with its own 42703 at plan time.
func TestCheckFilterColumnsDoesNotJudgeRowFields(t *testing.T) {
	if err := CheckFilterColumns(filterColumnsBatch(t), []string{"c_row.nosuch"}); err != nil {
		t.Fatalf("a ROW field path resolves to its container; got %v", err)
	}
}

func TestFilterColumnRefsSpellsReferencesTheWayColRefResolvesThem(t *testing.T) {
	for _, c := range []struct {
		name string
		sql  string
		want []string
		ok   bool
	}{
		{"bare", "v > 0", []string{"v"}, true},
		{"qualified", "c.v > 0", []string{"c.v"}, true},
		{"rowField", "c_row.b > 0", []string{"c_row.b"}, true},
		{"function", "COALESCE(v, w) > 0", []string{"v", "w"}, true},
		{"between", "v BETWEEN a AND b", []string{"v", "a", "b"}, true},
		{"caseWhen", "CASE WHEN a THEN b ELSE c END", []string{"a", "b", "c"}, true},
		{"inList", "v IN (1, 2)", []string{"v"}, true},
		{"literalOnly", "1 = 1", nil, true},
		// The declines: a name inside one of these may legitimately resolve
		// outside the batch, so the caller must not run the guard at all.
		{"subquery", "v > (SELECT MAX(x) FROM t)", nil, false},
		{"exists", "EXISTS (SELECT 1 FROM t WHERE t.k = v)", nil, false},
		{"inSubquery", "v IN (SELECT x FROM t)", nil, false},
	} {
		c := c
		t.Run(c.name, func(t *testing.T) {
			node, err := plansql.ParseExpression(c.sql)
			if err != nil {
				t.Fatalf("parse %q: %v", c.sql, err)
			}
			got, ok := FilterColumnRefs(node)
			if ok != c.ok {
				t.Fatalf("FilterColumnRefs(%q) ok=%v, want %v (refs %v)", c.sql, ok, c.ok, got)
			}
			if !ok {
				return
			}
			// The SET is what the guard reads; the walk order is not a
			// contract.
			if len(got) != len(c.want) {
				t.Fatalf("FilterColumnRefs(%q) = %v, want %v", c.sql, got, c.want)
			}
			g, w := append([]string(nil), got...), append([]string(nil), c.want...)
			sort.Strings(g)
			sort.Strings(w)
			for i := range g {
				if !strings.EqualFold(g[i], w[i]) {
					t.Fatalf("FilterColumnRefs(%q) = %v, want %v", c.sql, got, c.want)
				}
			}
		})
	}
}
