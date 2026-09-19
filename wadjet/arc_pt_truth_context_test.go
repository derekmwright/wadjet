// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// ARC PT — A CALL IN A TRUTH CONTEXT IS TYPED BY ITS DECLARATION.
//
// Found by this arc's own DML gate: `#` made `WHERE id > 0 AND n # 3`
// PARSEABLE, and the clause then deleted every row of a table PostgreSQL
// 17.11 leaves untouched (42804, `argument of AND must be type boolean, not
// type bigint`). The operator was the route; the hole was older and wider —
// `checkBooleanContext` could type a column, a literal and arithmetic, and a
// CALL not at all, so `WHERE upper(s)` answered where the server refuses, and
// `a ^ b` (arc PS's `power(a, b)`) had the same shape as `a # b`.
//
// Two changes close it. The truth-context walk reads the registry's DECLARED
// return type for a call — only a FIXED declaration answers, so a polymorphic
// one is still left alone — and a DML predicate, which is compiled and never
// planned (ADR-0031), runs the same walk before it reads a row.
//
// Every message below is PostgreSQL 17.11's own, measured over the same rows.
func TestArcPTACallInATruthContextIsTypedByItsDeclaration(t *testing.T) {
	ctx := context.Background()
	db := ptOpenFixture(t, ctx)

	for _, c := range []struct {
		name, sql, state, msg string
		want                  string
	}{
		// ---- the operator rewrites: an OPERATOR reaches the walk as a call
		{name: "xor_under_an_and", sql: `SELECT COUNT(*) AS v FROM ptt WHERE id > 0 AND n # 3`,
			state: "42804", msg: "argument of AND must be type boolean, not type bigint"},
		{name: "power_under_an_and", sql: `SELECT COUNT(*) AS v FROM ptt WHERE id > 0 AND 2 ^ 3`,
			state: "42804", msg: "argument of AND must be type boolean, not type double precision"},
		{name: "xor_in_a_where", sql: `SELECT COUNT(*) AS v FROM ptt WHERE n # 3`,
			state: "42804", msg: "argument of WHERE must be type boolean, not type bigint"},

		// ---- every other call whose declaration is fixed and not boolean
		{name: "text_function_in_a_where", sql: `SELECT COUNT(*) AS v FROM ptt WHERE upper(s)`,
			state: "42804", msg: "argument of WHERE must be type boolean, not type text"},
		{name: "integer_function_in_a_where", sql: `SELECT COUNT(*) AS v FROM ptt WHERE length(s)`,
			state: "42804", msg: "argument of WHERE must be type boolean, not type integer"},
		{name: "text_function_under_a_not", sql: `SELECT COUNT(*) AS v FROM ptt WHERE NOT upper(s)`,
			state: "42804", msg: "argument of NOT must be type boolean, not type text"},
		{name: "text_function_in_a_case_when",
			sql:   `SELECT CASE WHEN upper(s) THEN 1 ELSE 0 END AS v FROM ptt`,
			state: "42804", msg: "argument of CASE/WHEN must be type boolean, not type text"},

		// ---- the boolean-returning calls still ANSWER -------------------
		{name: "boolean_function_in_a_where",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE starts_with(s, 'a')`, want: "2"},
		{name: "similar_to_in_a_where",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE s SIMILAR TO 'a%'`, want: "2"},
		{name: "like_escape_in_a_where",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE s LIKE 'a!%b' ESCAPE '!'`, want: "1"},
		{name: "regexp_like_in_a_where",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE regexp_like(s, 'a')`, want: "2"},
		{name: "xor_under_a_comparison",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE (n # 3) = 6`, want: "1"},
		{name: "a_polymorphic_call_is_left_alone",
			sql: `SELECT COUNT(*) AS v FROM ptt WHERE COALESCE(id > 0, false)`, want: "4"},
	} {
		t.Run(c.name, func(t *testing.T) {
			got, err := ptScalar(ctx, db, c.sql)
			if c.state != "" {
				if err == nil {
					t.Fatalf("answered %q where PostgreSQL 17.11 raises %s %s\n  SQL: %s",
						got, c.state, c.msg, c.sql)
				}
				if st := sqlerr.StateOf(err); st != c.state {
					t.Errorf("SQLSTATE %q, want %q: %v", st, c.state, err)
				}
				if !strings.Contains(err.Error(), c.msg) {
					t.Errorf("message\n  got  %v\n  want PostgreSQL 17.11's %q", err, c.msg)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused a statement PostgreSQL 17.11 answers %s: %v\n  SQL: %s",
					c.want, err, c.sql)
			}
			if got != c.want {
				t.Errorf("answered %q, want %q\n  SQL: %s", got, c.want, c.sql)
			}
		})
	}
}

// TestArcPTADMLPredicateIsHeldToTheSameRule is the half that matters most: a
// DELETE whose WHERE is not a boolean must not remove a row.
func TestArcPTADMLPredicateIsHeldToTheSameRule(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name, sql, state, msg, tag string
		rows                       []string
	}{
		{name: "an_integer_conjunct_deletes_nothing",
			sql:   `DELETE FROM bc WHERE id > 0 AND n # 3`,
			state: "42804", msg: "argument of AND must be type boolean, not type bigint"},
		{name: "an_integer_conjunct_updates_nothing",
			sql:   `UPDATE bc SET n = 1 WHERE id > 0 AND n # 3`,
			state: "42804", msg: "argument of AND must be type boolean, not type bigint"},
		{name: "a_text_function_predicate_deletes_nothing",
			sql:   `DELETE FROM bc WHERE upper(s)`,
			state: "42804", msg: "argument of WHERE must be type boolean, not type text"},
		{name: "a_bare_integer_column_deletes_nothing",
			sql:   `DELETE FROM bc WHERE n`,
			state: "42804", msg: "argument of WHERE must be type boolean, not type bigint"},
		{name: "the_rule_survives_an_alias",
			sql:   `DELETE FROM bc AS a WHERE a.id > 0 AND a.n # 3`,
			state: "42804", msg: "argument of AND must be type boolean, not type bigint"},
		// And the ordinary predicates still run, the new spellings included.
		{name: "an_ordinary_predicate_still_deletes",
			sql: `DELETE FROM bc WHERE id = 1`, tag: "DELETE 1", rows: []string{"2:0:b"}},
		{name: "similar_to_still_deletes",
			sql: `DELETE FROM bc WHERE s SIMILAR TO 'a'`, tag: "DELETE 1", rows: []string{"2:0:b"}},
		{name: "like_escape_still_deletes",
			sql: `DELETE FROM bc WHERE s LIKE 'a' ESCAPE '!'`, tag: "DELETE 1", rows: []string{"2:0:b"}},
		{name: "a_xor_under_a_comparison_still_deletes",
			sql: `DELETE FROM bc WHERE (n # 3) = 6`, tag: "DELETE 1", rows: []string{"2:0:b"}},
		{name: "a_xor_in_a_set_value_still_updates",
			sql: `UPDATE bc SET n = n # 1 WHERE id = 1`, tag: "UPDATE 1",
			rows: []string{"1:4:a", "2:0:b"}},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, rows := ptOpenDMLFixture(t, ctx)
			res, err := db.Execute(ctx, c.sql)
			if c.state != "" {
				if err == nil {
					t.Fatalf("answered %s %d where PostgreSQL 17.11 raises %s %s; bc is now %v",
						res.Command, res.RowsAffected, c.state, c.msg, ptDMLRows(t, ctx, db))
				}
				if st := sqlerr.StateOf(err); st != c.state {
					t.Errorf("SQLSTATE %q, want %q: %v", st, c.state, err)
				}
				if !strings.Contains(err.Error(), c.msg) {
					t.Errorf("message\n  got  %v\n  want PostgreSQL 17.11's %q", err, c.msg)
				}
				if after := ptDMLRows(t, ctx, db); strings.Join(after, " ") != strings.Join(rows, " ") {
					t.Errorf("the refused statement changed bc: %v -> %v", rows, after)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: %v", c.sql, err)
			}
			if got := res.Command + " " + ptFormat(res.RowsAffected); got != c.tag {
				t.Errorf("command tag %q, want %q", got, c.tag)
			}
			if after := ptDMLRows(t, ctx, db); strings.Join(after, " ") != strings.Join(c.rows, " ") {
				t.Errorf("rows after\n  got  %v\n  want %v", after, c.rows)
			}
		})
	}
}

func ptOpenDMLFixture(t *testing.T, ctx context.Context) (*DB, []string) {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "bc", sch, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("bc", sch, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int64(1), "n": int64(5), "s": "a"},
		{"id": int64(2), "n": int64(0), "s": "b"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db, ptDMLRows(t, ctx, db)
}

func ptDMLRows(t *testing.T, ctx context.Context, db *DB) []string {
	t.Helper()
	res, err := db.Query(ctx, `SELECT id, n, s FROM bc ORDER BY id`)
	if err != nil {
		t.Fatalf("reading bc: %v", err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		out = append(out, ptFormat(r["id"])+":"+ptFormat(r["n"])+":"+ptFormat(r["s"]))
	}
	return out
}
