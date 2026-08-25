package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
)

// TestRowFieldPathDeclaredTypeOnTheDAG is #568's stage-DAG arm: a ROW field
// path must reach the client under the FIELD's type, not STRING.
//
// coordinator.TestTypeMatrixTwoPath compares the two paths' rows for the same
// corpus, but it compares them through oracle.Compare, which renders every
// cell with fmt.Sprint — int64(11) and the string "11" are the same cell
// there. The whole of #568 is invisible to that comparison, so the TYPES are
// asserted here, on the arm where a wrong declaration is also the wire's
// answer: pgwire builds RowDescription's OID from this schema, and a client
// that binds an int8 column reading text is broken by it.
func TestRowFieldPathDeclaredTypeOnTheDAG(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	coord := tmdCluster(t, ctx)

	t.Run("projection", func(t *testing.T) {
		res, err := coord.ExecuteSQL(ctx, fmt.Sprintf(
			`SELECT id, c_row.a AS a, c_row.b AS b FROM %s WHERE id < 40 AND c_row IS NOT NULL ORDER BY id`,
			typematrix.Nested))
		if err != nil {
			t.Fatalf("ExecuteSQL: %v", err)
		}
		const want = "id:INT64(0,0),a:STRING(0,0),b:INT64(0,0)"
		if got := describeSchema(res.OutputSchema()); got != want {
			t.Errorf("declared schema = %q, want %q — a ROW field path declared STRING is OID 25 on the wire (#568)",
				got, want)
		}
		rows, err := res.Rows()
		if err != nil {
			t.Fatalf("Rows: %v", err)
		}
		if len(rows) == 0 {
			t.Fatal("no rows; the assertions below would be vacuous")
		}
		sawB := false
		for _, r := range rows {
			if r["a"] != nil {
				if _, ok := r["a"].(string); !ok {
					t.Fatalf("a = %#v (%T), want string", r["a"], r["a"])
				}
			}
			if r["b"] == nil {
				continue
			}
			sawB = true
			if _, ok := r["b"].(int64); !ok {
				t.Fatalf("b = %#v (%T), want int64 — an INT64 ROW field came back as text", r["b"], r["b"])
			}
		}
		if !sawB {
			t.Fatal("every c_row.b was NULL; the type assertion never ran")
		}
	})

	// GROUP BY and MIN/MAX over a field path could not run AT ALL before the
	// fix: the aggregate resolves its inputs by name through
	// columnIndexFallback, which has no ROW arm, so the query failed with
	// `GROUP BY key "c_row.b" is not a column of its input (input has: ...)`.
	t.Run("group_and_aggregate", func(t *testing.T) {
		for _, tc := range []struct{ name, sql, want string }{
			{"group", fmt.Sprintf(
				`SELECT c_row.b AS k, COUNT(*) AS n FROM %s WHERE id < 40 AND c_row.b IS NOT NULL GROUP BY c_row.b ORDER BY k`,
				typematrix.Nested), "k:INT64(0,0),n:INT64(0,0)"},
			{"minmax", fmt.Sprintf(
				`SELECT MIN(c_row.b) AS lo, MAX(c_row.b) AS hi FROM %s`, typematrix.Nested),
				"lo:INT64(0,0),hi:INT64(0,0)"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				res, err := coord.ExecuteSQL(ctx, tc.sql)
				if err != nil {
					t.Fatalf("%s: %v", tc.sql, err)
				}
				if got := describeSchema(res.OutputSchema()); got != tc.want {
					t.Errorf("declared schema = %q, want %q\n  %s", got, tc.want, tc.sql)
				}
				rows, err := res.Rows()
				if err != nil {
					t.Fatalf("Rows: %v", err)
				}
				if len(rows) == 0 {
					t.Fatalf("no rows: %s", tc.sql)
				}
				for _, r := range rows {
					for _, c := range []string{"k", "lo", "hi"} {
						v, present := r[c]
						if !present || v == nil {
							continue
						}
						if _, ok := v.(int64); !ok {
							t.Fatalf("%s = %#v (%T), want int64\n  %s", c, v, v, tc.sql)
						}
					}
				}
			})
		}
	})

	// A field that is itself a container keeps its own shape: the value used
	// to arrive as the Go rendering of a map, written into a STRING vector.
	t.Run("container_field", func(t *testing.T) {
		res, err := coord.ExecuteSQL(ctx, fmt.Sprintf(
			`SELECT id, c_rownest.s AS s FROM %s WHERE id < 40 AND c_rownest IS NOT NULL ORDER BY id`,
			typematrix.Nested))
		if err != nil {
			t.Fatalf("ExecuteSQL: %v", err)
		}
		const want = "id:INT64(0,0),s:ROW(0,0)"
		if got := describeSchema(res.OutputSchema()); got != want {
			t.Errorf("declared schema = %q, want %q", got, want)
		}
		rows, err := res.Rows()
		if err != nil {
			t.Fatalf("Rows: %v", err)
		}
		saw := false
		for _, r := range rows {
			if r["s"] == nil {
				continue
			}
			saw = true
			if _, ok := r["s"].(map[string]any); !ok {
				t.Fatalf("s = %#v (%T), want map[string]any — a ROW field stringified", r["s"], r["s"])
			}
		}
		if !saw {
			t.Fatal("every c_rownest.s was NULL; the type assertion never ran")
		}
	})
}
