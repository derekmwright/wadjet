package wadjet

import (
	"context"
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A DML statement naming a column that does not exist is 42703, on every
// clause it can be named in.
//
// `UPDATE t SET nosuchcol = 1` reported "UPDATE 1": the assignment was
// dropped into a map nothing read and the matched rows were rewritten
// unchanged. `UPDATE t SET n = 1 WHERE nosuchcol = 1` reported "UPDATE 0":
// the reference evaluated to NULL on every row, so the statement quietly did
// nothing. Both are 42703 in PostgreSQL 17 (verified live), and both were
// SILENT because the DML doors do not go through the planner and had no
// name-resolution step at all (#678, the #653 class on the DML path).
func TestDMLUnknownColumnIsUndefinedColumn(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"UPDATE SET target", "UPDATE t678 SET nosuchcol = 1"},
		{"UPDATE SET target beside a real one", "UPDATE t678 SET n = 2, nosuchcol = 1"},
		{"UPDATE WHERE", "UPDATE t678 SET n = 2 WHERE nosuchcol = 1"},
		{"UPDATE WHERE inside a function", "UPDATE t678 SET n = 2 WHERE UPPER(nosuchcol) = 'X'"},
		{"UPDATE WHERE inside a conjunction", "UPDATE t678 SET n = 2 WHERE id = 1 AND nosuchcol = 1"},
		{"UPDATE WHERE inside CASE", "UPDATE t678 SET n = 2 WHERE CASE WHEN nosuchcol > 0 THEN true ELSE false END"},
		{"UPDATE WHERE inside BETWEEN", "UPDATE t678 SET n = 2 WHERE nosuchcol BETWEEN 1 AND 2"},
		{"UPDATE WHERE inside IN", "UPDATE t678 SET n = 2 WHERE nosuchcol IN (1, 2)"},
		{"UPDATE WHERE inside IS NULL", "UPDATE t678 SET n = 2 WHERE nosuchcol IS NULL"},
		{"UPDATE SET expression", "UPDATE t678 SET n = nosuchcol + 1"},
		{"DELETE WHERE", "DELETE FROM t678 WHERE nosuchcol = 1"},
		{"DELETE WHERE negated", "DELETE FROM t678 WHERE NOT (nosuchcol = 1)"},
		{"MERGE SET target", "MERGE INTO t678 t USING s678 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET nosuchcol = 1"},
		{"MERGE INSERT column",
			"MERGE INTO t678 t USING s678 s ON t.id = s.id WHEN NOT MATCHED THEN INSERT (nosuchcol) VALUES (1)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := resolutionDB(t)
			_, err := db.Execute(ctx, tc.sql)
			if err == nil {
				q, _ := db.Query(ctx, "SELECT id, n, s FROM t678")
				t.Fatalf("%s succeeded; want 42703. Table is now %v", tc.sql, q.Rows)
			}
			if got := sqlerr.StateOf(err); got != "42703" {
				t.Errorf("%s: SQLSTATE %q, want 42703 (err: %v)", tc.sql, got, err)
			}
		})
	}
}

// A SET whose value is an EXPRESSION is evaluated, as PostgreSQL evaluates it.
//
// It used to be read only as a LITERAL, through a converter whose STRING arm
// cannot fail — so for a STRING target the literal path always won and
// `SET s = UPPER(s)` stored the eight characters "UPPER(s)" as the value. For
// a typed column the same expression was an error, which at least was loud.
func TestDMLSetExpressionIsEvaluated(t *testing.T) {
	for _, tc := range []struct {
		name string
		sql  string
		want map[string]any
	}{
		{"a function over the column being set", "UPDATE t678 SET s = UPPER(s)",
			map[string]any{"s": "AB"}},
		{"arithmetic over the column being set", "UPDATE t678 SET n = n + 1",
			map[string]any{"n": int64(11)}},
		{"a function over another column", "UPDATE t678 SET s = CONCAT(s, '!')",
			map[string]any{"s": "ab!"}},
		{"a CASE expression", "UPDATE t678 SET n = CASE WHEN id = 1 THEN 99 ELSE 0 END",
			map[string]any{"n": int64(99)}},
		{"a string literal is still a literal", "UPDATE t678 SET s = 'literal'",
			map[string]any{"s": "literal"}},
		{"a string literal that looks like a column", "UPDATE t678 SET s = 'n'",
			map[string]any{"s": "n"}},
		{"a negative number literal", "UPDATE t678 SET n = -5",
			map[string]any{"n": int64(-5)}},
		{"NULL", "UPDATE t678 SET s = NULL", map[string]any{"s": nil}},
		// The expression engine has ONE numeric result box per family, so a
		// function over an integer column evaluates to float64 and
		// ingest.checkType refuses it into an INT64 column outright
		// ("expected integer, got float64"). The narrowing is exact here, so
		// it is made rather than refused.
		{"a function whose result box is a float", "UPDATE t678 SET n = ABS(0 - 3)",
			map[string]any{"n": int64(3)}},
		{"a rounded function", "UPDATE t678 SET n = ROUND(2.6)",
			map[string]any{"n": int64(3)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := resolutionDB(t)
			if _, err := db.Execute(ctx, tc.sql); err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			q, err := db.Query(ctx, "SELECT id, n, s FROM t678")
			if err != nil {
				t.Fatal(err)
			}
			if len(q.Rows) != 1 {
				t.Fatalf("%d rows after %s, want 1: %v", len(q.Rows), tc.sql, q.Rows)
			}
			for col, want := range tc.want {
				got := q.Rows[0][col]
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("%s: %s = %v (%T), want %v", tc.sql, col, got, got, want)
				}
			}
		})
	}
}

// A computed value assigned to an integer column follows PostgreSQL's
// assignment cast: it ROUNDS half away from zero, and only a value outside the
// column's range is 22003. Every expectation below was read off
// postgres:17-alpine.
func TestDMLSetExpressionFollowsPostgresAssignmentCast(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sql   string
		want  string // "" when the statement must be refused
		state string
	}{
		{name: "a fractional result rounds", sql: "UPDATE t678 SET n = 1 + 0.5", want: "2"},
		{name: "rounding is half AWAY from zero", sql: "UPDATE t678 SET n = 0 - 2.5", want: "-3"},
		{name: "rounding down", sql: "UPDATE t678 SET n = 1 + 1.4", want: "2"},
		{name: "integer division stays integer", sql: "UPDATE t678 SET n = 5 / 2", want: "2"},
		// An EXPRESSION, not a bare literal: a bare 1e30 takes the literal
		// path, where the refusal comes from the integer parser instead
		// (a divergence from PostgreSQL, which rounds a bare 2.4 into a
		// bigint column and stores 2 — recorded, not fixed here).
		{name: "past the column's range", sql: "UPDATE t678 SET n = 0 + 1e30", state: "22003"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := resolutionDB(t)
			_, err := db.Execute(ctx, tc.sql)
			if tc.state != "" {
				if err == nil {
					q, _ := db.Query(ctx, "SELECT id, n FROM t678")
					t.Fatalf("%s succeeded; want %s. Table is now %v", tc.sql, tc.state, q.Rows)
				}
				if got := sqlerr.StateOf(err); got != tc.state {
					t.Errorf("%s: SQLSTATE %q, want %q (err: %v)", tc.sql, got, tc.state, err)
				}
				// A refused statement changed nothing.
				q, qerr := db.Query(ctx, "SELECT id, n FROM t678")
				if qerr != nil {
					t.Fatal(qerr)
				}
				if len(q.Rows) != 1 || fmt.Sprint(q.Rows[0]["n"]) != "10" {
					t.Errorf("the refused %s left %v, want the original single row with n=10", tc.sql, q.Rows)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", tc.sql, err)
			}
			q, qerr := db.Query(ctx, "SELECT id, n FROM t678")
			if qerr != nil {
				t.Fatal(qerr)
			}
			if len(q.Rows) != 1 {
				t.Fatalf("%d rows after %s, want 1: %v", len(q.Rows), tc.sql, q.Rows)
			}
			if got := fmt.Sprint(q.Rows[0]["n"]); got != tc.want {
				t.Errorf("%s stored n = %s, want %s (PostgreSQL 17)", tc.sql, got, tc.want)
			}
		})
	}
}

// MERGE's SET resolver had the same STRING hole and one more: it stored the
// expression's SOURCE TEXT. `SET s = UPPER(s.name)` wrote the twelve
// characters "UPPER(s.name)" into a STRING column.
func TestMergeSetExpressionIsEvaluated(t *testing.T) {
	for _, tc := range []struct {
		name string
		set  string
		want string
	}{
		{"a function over a source column", "s = UPPER(s.name)", "BOB"},
		{"a function over a target column", "s = UPPER(t.s)", "AB"},
		{"concatenation across both sides", "s = CONCAT(t.s, s.name)", "abbob"},
		{"a plain source column reference", "s = s.name", "bob"},
		{"a plain literal", "s = 'lit'", "lit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := resolutionDB(t)
			sql := "MERGE INTO t678 t USING s678 s ON t.id = s.id WHEN MATCHED THEN UPDATE SET " + tc.set
			if _, err := db.Execute(ctx, sql); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			q, err := db.Query(ctx, "SELECT id, s FROM t678")
			if err != nil {
				t.Fatal(err)
			}
			if len(q.Rows) != 1 {
				t.Fatalf("%d rows after the MERGE, want 1: %v", len(q.Rows), q.Rows)
			}
			if got := fmt.Sprint(q.Rows[0]["s"]); got != tc.want {
				t.Errorf("%s stored %q, want %q", tc.set, got, tc.want)
			}
		})
	}
}

// resolutionDB is one row of a target table and one row of a MERGE source
// that joins to it.
func resolutionDB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	for _, ddl := range []string{
		"CREATE TABLE t678 (id INT64, n INT64, s STRING)",
		"CREATE TABLE s678 (id INT64, name STRING)",
	} {
		if _, err := db.Query(ctx, ddl); err != nil {
			t.Fatal(err)
		}
	}
	for _, dml := range []string{
		"INSERT INTO t678 VALUES (1, 10, 'ab')",
		"INSERT INTO s678 VALUES (1, 'bob')",
		// A source row that does NOT match, so a WHEN NOT MATCHED clause has
		// something to fire on.
		"INSERT INTO s678 VALUES (2, 'zed')",
	} {
		if _, err := db.Execute(ctx, dml); err != nil {
			t.Fatal(err)
		}
	}
	return db
}
