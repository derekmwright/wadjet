// SPDX-License-Identifier: AGPL-3.0-only

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/server/pgwire"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
	"github.com/jackc/pgx/v5"
)

// ARC PT round 2 — THE DML TRUTH CONTEXT, ONE CELL PER AST NODE KIND PER DOOR.
//
// Round 1 closed the CALL-with-a-fixed-declaration route into the DML
// predicate and left the rest of the language open, and the review measured
// what that cost: `DELETE FROM t WHERE id > 0 AND CASE WHEN id > 0 THEN 1 ELSE
// 0 END` EMPTIED the table on all three DML doors where PostgreSQL 17.11
// raises 42804 — and the same predicate selected ZERO rows through SELECT. A
// statement that selects nothing removed everything.
//
// The rule is total now, so this table is the enumeration that keeps it total:
// every expression node kind the parser can put in a predicate, on every DML
// door, with PostgreSQL 17.11's verdict measured live. A cell that answers
// where the server refuses is a wrong row set whether it removes rows or not,
// so BOTH are asserted: the SQLSTATE, and that the four rows are still there.
//
// The probe this grew from is the reviewer's `ptrev_dml_truth_doors_test.go`.
func TestArcPTTheDMLTruthContextRefusesEveryNodeKindOnEveryDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up a pgwire and an HTTP door")
	}
	rig := ptDoorRigUp(t)

	for _, c := range []struct {
		kind, sql, state, pg string
		// tag is set where PostgreSQL ANSWERS: the command tag and the rows
		// that must survive.
		tag       string
		surviving int
	}{
		// ---- literals ---------------------------------------------------
		{kind: "literal/integer", sql: `DELETE FROM %s WHERE 1`,
			state: "42804", pg: "42804 argument of WHERE must be type boolean, not type integer"},
		{kind: "literal/numeric", sql: `DELETE FROM %s WHERE 1.5`,
			state: "42804", pg: "42804 … not type numeric"},
		{kind: "literal/text_not_boolean", sql: `DELETE FROM %s WHERE 'abc'`,
			state: "22P02", pg: `22P02 invalid input syntax for type boolean: "abc"`},
		{kind: "literal/text_true", sql: `DELETE FROM %s WHERE 'true'`,
			tag: "DELETE 4", surviving: 0, pg: "DELETE 4 — the boolean input function reads it"},
		{kind: "literal/text_false", sql: `DELETE FROM %s WHERE 'no'`,
			tag: "DELETE 0", surviving: 4, pg: "DELETE 0"},
		{kind: "literal/null", sql: `DELETE FROM %s WHERE NULL`,
			tag: "DELETE 0", surviving: 4, pg: "DELETE 0"},
		{kind: "literal/boolean", sql: `DELETE FROM %s WHERE true`,
			tag: "DELETE 4", surviving: 0, pg: "DELETE 4"},

		// ---- columns ----------------------------------------------------
		{kind: "column/bigint", sql: `DELETE FROM %s WHERE n`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "column/text", sql: `DELETE FROM %s WHERE s`,
			state: "42804", pg: "42804 … not type text"},
		{kind: "column/boolean", sql: `DELETE FROM %s WHERE b`,
			tag: "DELETE 2", surviving: 2, pg: "DELETE 2"},
		{kind: "column/qualified_under_an_alias", sql: `DELETE FROM %s AS a WHERE a.n`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "column/bare_under_an_alias", sql: `DELETE FROM %s AS a WHERE n`,
			state: "42804", pg: "42804 … not type bigint"},

		// ---- calls ------------------------------------------------------
		{kind: "call/fixed_text", sql: `DELETE FROM %s WHERE upper(s)`,
			state: "42804", pg: "42804 … not type text"},
		{kind: "call/fixed_integer", sql: `DELETE FROM %s WHERE length(s)`,
			state: "42804", pg: "42804 … not type integer"},
		{kind: "call/polymorphic_coalesce", sql: `DELETE FROM %s WHERE COALESCE(n, 1)`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "call/polymorphic_greatest", sql: `DELETE FROM %s WHERE GREATEST(n, 1)`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "call/polymorphic_least", sql: `DELETE FROM %s WHERE LEAST(n, 1)`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "call/polymorphic_nullif", sql: `DELETE FROM %s WHERE NULLIF(n, 1)`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "call/polymorphic_over_a_boolean", sql: `DELETE FROM %s WHERE COALESCE(b, false)`,
			tag: "DELETE 2", surviving: 2, pg: "DELETE 2 — a boolean argument is a boolean result"},
		{kind: "call/boolean", sql: `DELETE FROM %s WHERE starts_with(s, 'a')`,
			tag: "DELETE 1", surviving: 3, pg: "DELETE 1"},
		{kind: "call/aggregate", sql: `DELETE FROM %s WHERE SUM(n)`,
			state: "42803", pg: "42803 aggregate functions are not allowed in WHERE"},
		{kind: "call/window", sql: `DELETE FROM %s WHERE COUNT(*) OVER ()`,
			state: "42P20", pg: "42P20 window functions are not allowed in WHERE"},

		// ---- CASE, CAST -------------------------------------------------
		{kind: "case/searched_integer", sql: `DELETE FROM %s WHERE CASE WHEN id > 0 THEN 1 ELSE 0 END`,
			state: "42804", pg: "42804 … not type integer"},
		{kind: "case/simple_integer", sql: `DELETE FROM %s WHERE CASE n WHEN 1 THEN 1 ELSE 0 END`,
			state: "42804", pg: "42804 … not type integer"},
		{kind: "case/searched_boolean", sql: `DELETE FROM %s WHERE CASE WHEN id > 0 THEN true ELSE false END`,
			tag: "DELETE 4", surviving: 0, pg: "DELETE 4"},
		{kind: "case/when_is_not_boolean", sql: `DELETE FROM %s WHERE CASE WHEN upper(s) THEN true ELSE false END`,
			state: "42804", pg: "42804 argument of CASE/WHEN must be type boolean, not type text"},
		{kind: "cast/bigint", sql: `DELETE FROM %s WHERE CAST(n AS BIGINT)`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "cast/text_null", sql: `DELETE FROM %s WHERE CAST(NULL AS VARCHAR)`,
			state: "42804", pg: "42804 … not type character varying"},
		{kind: "cast/boolean", sql: `DELETE FROM %s WHERE CAST('t' AS BOOLEAN)`,
			tag: "DELETE 4", surviving: 0, pg: "DELETE 4"},

		// ---- arithmetic, constructors, subqueries -----------------------
		{kind: "arithmetic/plus", sql: `DELETE FROM %s WHERE n + 1`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "arithmetic/unary_minus", sql: `DELETE FROM %s WHERE -n`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "arithmetic/concat", sql: `DELETE FROM %s WHERE s || 'x'`,
			state: "42804", pg: "42804 … not type text"},
		{kind: "operator/xor", sql: `DELETE FROM %s WHERE n # 3`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "array/constructor", sql: `DELETE FROM %s WHERE ARRAY[1,2]`,
			state: "42804", pg: "42804 … not type integer[]"},
		{kind: "interval/literal", sql: `DELETE FROM %s WHERE INTERVAL '1' DAY`,
			state: "42804", pg: "42804 … not type interval"},
		{kind: "subquery/scalar_integer", sql: `DELETE FROM %s WHERE (SELECT 1)`,
			state: "42804", pg: "42804 … not type integer"},
		{kind: "subquery/scalar_aggregate", sql: `DELETE FROM %s WHERE (SELECT COUNT(*) FROM %s)`,
			state: "42804", pg: "42804 … not type bigint"},

		// ---- containers: a field path, a subscript, the container itself
		// (round-2 review, P2-r2: `(r).a` reaches the walk as a ColRef whose
		// QUALIFIER is a ROW column, so the relation lookup missed it and
		// `WHERE id > 0 AND (r).a` removed a row PostgreSQL refuses to touch).
		{kind: "rowfield/bigint", sql: `DELETE FROM %s WHERE (r).a`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "rowfield/boolean", sql: `DELETE FROM %s WHERE (r).ok`,
			tag: "DELETE 2", surviving: 2, pg: "answers — a boolean field IS a boolean"},
		{kind: "rowfield/under_a_conjunction", sql: `DELETE FROM %s WHERE id > 0 AND (r).a`,
			state: "42804", pg: "42804 argument of AND … not type bigint"},
		{kind: "rowfield/boolean_under_a_conjunction",
			sql: `DELETE FROM %s WHERE id > 0 AND (r).ok`,
			tag: "DELETE 2", surviving: 2, pg: "answers"},
		{kind: "row/column", sql: `DELETE FROM %s WHERE r`,
			state: "42804", pg: "42804 … not type record"},
		{kind: "array/column", sql: `DELETE FROM %s WHERE arr`,
			state: "42804", pg: "42804 … not type bigint[]"},
		{kind: "array/subscript", sql: `DELETE FROM %s WHERE arr[1]`,
			state: "42804", pg: "42804 … not type bigint (the ELEMENT type)"},
		{kind: "array/subscript_under_a_conjunction",
			sql:   `DELETE FROM %s WHERE id > 0 AND arr[1]`,
			state: "42804", pg: "42804 argument of AND … not type bigint"},
		{kind: "map/column", sql: `DELETE FROM %s WHERE m`,
			state: "42804", pg: "42804 (wadjet-native; a map is not boolean)"},
		{kind: "map/subscript", sql: `DELETE FROM %s WHERE m['k']`,
			state: "42804", pg: "42804 … not type bigint (the VALUE type)"},

		// ---- the refusal ORDER, which is PostgreSQL's (round-2 review,
		// P3-r2): a WINDOW is refused before names resolve and an AGGREGATE
		// after, and the DML and SELECT doors report the same class.
		{kind: "order/aggregate_over_an_unknown_column",
			sql:   `DELETE FROM %s WHERE SUM(zz)`,
			state: "42703", pg: `42703 column "zz" does not exist`},
		{kind: "order/unknown_column_left_of_an_aggregate",
			sql:   `DELETE FROM %s WHERE zz > 0 AND SUM(n)`,
			state: "42703", pg: `42703 column "zz" does not exist`},
		{kind: "order/window_over_an_unknown_column",
			sql:   `DELETE FROM %s WHERE COUNT(*) OVER (PARTITION BY zz)`,
			state: "42P20", pg: "42P20 — the window is refused BEFORE names resolve"},
		{kind: "order/unknown_column_alone", sql: `DELETE FROM %s WHERE zz`,
			state: "42703", pg: `42703 column "zz" does not exist`},

		// ---- the same kinds UNDER a conjunction, which is the lethal
		// position: the per-row closure reads a non-boolean as false only at
		// the TOP of the clause, so an integer under an AND matched EVERY row.
		{kind: "and/case", sql: `DELETE FROM %s WHERE id > 0 AND CASE WHEN id > 0 THEN 1 ELSE 0 END`,
			state: "42804", pg: "42804 argument of AND must be type boolean, not type integer"},
		{kind: "and/greatest", sql: `DELETE FROM %s WHERE id > 0 AND GREATEST(n, 1)`,
			state: "42804", pg: "42804 argument of AND … not type bigint"},
		{kind: "and/cast", sql: `DELETE FROM %s WHERE id > 0 AND CAST(n AS BIGINT)`,
			state: "42804", pg: "42804 argument of AND … not type bigint"},
		{kind: "and/coalesce", sql: `DELETE FROM %s WHERE id > 0 AND COALESCE(n, 1)`,
			state: "42804", pg: "42804 argument of AND … not type bigint"},
		{kind: "and/array", sql: `DELETE FROM %s WHERE id > 0 AND ARRAY[1]`,
			state: "42804", pg: "42804 argument of AND … not type integer[]"},
		{kind: "or/arithmetic", sql: `DELETE FROM %s WHERE id > 0 OR n + 1`,
			state: "42804", pg: "42804 argument of OR … not type bigint"},
		{kind: "not/integer", sql: `DELETE FROM %s WHERE NOT n`,
			state: "42804", pg: "42804 argument of NOT … not type bigint"},

		// ---- UPDATE takes the same rule ---------------------------------
		{kind: "update/and_case", sql: `UPDATE %s SET n = 9 WHERE id > 0 AND CASE WHEN id > 0 THEN 1 ELSE 0 END`,
			state: "42804", pg: "42804 argument of AND … not type integer"},
		{kind: "update/cast", sql: `UPDATE %s SET n = 9 WHERE CAST(n AS BIGINT)`,
			state: "42804", pg: "42804 … not type bigint"},
		{kind: "update/text_true", sql: `UPDATE %s SET n = 9 WHERE 't'`,
			tag: "UPDATE 4", surviving: 4, pg: "UPDATE 4"},

		// ---- the controls: shapes that must keep working -----------------
		{kind: "control/comparison", sql: `DELETE FROM %s WHERE id = 1`,
			tag: "DELETE 1", surviving: 3, pg: "DELETE 1"},
		{kind: "control/in_list", sql: `DELETE FROM %s WHERE id > 0 AND (n IN (1, 5))`,
			tag: "DELETE 2", surviving: 2, pg: "DELETE 2"},
		{kind: "control/similar_to", sql: `DELETE FROM %s WHERE s SIMILAR TO 'a'`,
			tag: "DELETE 1", surviving: 3, pg: "DELETE 1"},
		{kind: "control/like_escape", sql: `DELETE FROM %s WHERE s LIKE 'a' ESCAPE '!'`,
			tag: "DELETE 1", surviving: 3, pg: "DELETE 1"},
		{kind: "control/xor_compared", sql: `DELETE FROM %s WHERE (n # 3) = 6`,
			tag: "DELETE 1", surviving: 3, pg: "DELETE 1"},
		{kind: "control/case_compared", sql: `DELETE FROM %s WHERE CASE WHEN id > 0 THEN 1 ELSE 0 END = 1`,
			tag: "DELETE 4", surviving: 0, pg: "DELETE 4"},
	} {
		t.Run(strings.ReplaceAll(c.kind, "/", "_"), func(t *testing.T) {
			for _, door := range rig.doors {
				tbl := ptDoorTable(t, rig, c.kind, door.name)
				sql := c.sql
				if strings.Count(sql, "%s") == 2 {
					sql = fmt.Sprintf(sql, tbl, tbl)
				} else {
					sql = fmt.Sprintf(sql, tbl)
				}
				tag, err := door.exec(t, sql)
				left := rig.surviving(t, tbl)
				if c.state != "" {
					if err == nil {
						t.Errorf("%s / %s ANSWERED %q and left %d of 4 rows; PostgreSQL 17.11 "+
							"refuses this with %s\n  SQL: %s", c.kind, door.name, tag, left, c.pg, sql)
						continue
					}
					if st := ptDoorState(err); st != c.state {
						t.Errorf("%s / %s: SQLSTATE %q, want %q (PostgreSQL 17.11: %s)\n  err: %v",
							c.kind, door.name, st, c.state, c.pg, err)
					}
					if left != 4 {
						t.Errorf("%s / %s: the REFUSED statement changed the table — %d of 4 rows left",
							c.kind, door.name, left)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s / %s: %v — PostgreSQL 17.11 answers %s\n  SQL: %s",
						c.kind, door.name, err, c.pg, sql)
					continue
				}
				if tag != c.tag {
					t.Errorf("%s / %s: command tag %q, want %q (PostgreSQL 17.11: %s)",
						c.kind, door.name, tag, c.tag, c.pg)
				}
				if left != c.surviving {
					t.Errorf("%s / %s: %d of 4 rows left, want %d", c.kind, door.name, left, c.surviving)
				}
			}
		})
	}
}

// TestArcPTTheSELECTWhereTakesTheSameRule is the other half of the review's
// sharper statement: the SAME predicate through a SELECT. A silent zero rows
// is a wrong row set too, so every kind above that refuses on the DML doors
// refuses here.
func TestArcPTTheSELECTWhereTakesTheSameRule(t *testing.T) {
	rig := ptDoorRigUp(t)
	tbl := ptDoorTable(t, rig, "select", "embedded")
	for _, c := range []struct{ kind, sql, state, pg string }{
		{"and_case", `SELECT COUNT(*) AS v FROM %s WHERE id > 0 AND CASE WHEN id > 0 THEN 1 ELSE 0 END`,
			"42804", "42804 integer"},
		{"where_case", `SELECT COUNT(*) AS v FROM %s WHERE CASE WHEN id > 0 THEN 1 ELSE 0 END`,
			"42804", "42804 integer"},
		{"simple_case", `SELECT COUNT(*) AS v FROM %s WHERE CASE n WHEN 1 THEN 1 ELSE 0 END`,
			"42804", "42804 integer"},
		{"cast", `SELECT COUNT(*) AS v FROM %s WHERE CAST(n AS BIGINT)`, "42804", "42804 bigint"},
		{"cast_null_text", `SELECT COUNT(*) AS v FROM %s WHERE CAST(NULL AS VARCHAR)`,
			"42804", "42804 character varying"},
		{"greatest", `SELECT COUNT(*) AS v FROM %s WHERE GREATEST(n, 1)`, "42804", "42804 bigint"},
		{"coalesce", `SELECT COUNT(*) AS v FROM %s WHERE COALESCE(n, 1)`, "42804", "42804 bigint"},
		{"nullif", `SELECT COUNT(*) AS v FROM %s WHERE NULLIF(n, 1)`, "42804", "42804 bigint"},
		{"array", `SELECT COUNT(*) AS v FROM %s WHERE ARRAY[1]`, "42804", "42804 integer[]"},
		{"interval", `SELECT COUNT(*) AS v FROM %s WHERE INTERVAL '1' DAY`, "42804", "42804 interval"},
		{"window", `SELECT COUNT(*) AS v FROM %s WHERE COUNT(*) OVER ()`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"window_in_a_join_on", `SELECT COUNT(*) AS v FROM %s a JOIN %s c ON (COUNT(*) OVER ()) > 0`,
			"42P20", "42P20 window functions are not allowed in JOIN conditions"},
		{"aggregate", `SELECT COUNT(*) AS v FROM %s WHERE SUM(n)`,
			"42803", "42803 aggregate functions are not allowed in WHERE"},
		{"rowfield", `SELECT COUNT(*) AS v FROM %s WHERE (r).a`, "42804", "42804 bigint"},
		{"array_subscript", `SELECT COUNT(*) AS v FROM %s WHERE arr[1]`, "42804", "42804 bigint"},
		{"map_subscript", `SELECT COUNT(*) AS v FROM %s WHERE m['k']`, "42804", "42804 bigint"},
		{"row_column", `SELECT COUNT(*) AS v FROM %s WHERE r`, "42804", "42804 record"},
		// The ORDER, on this door too: the same classes the DML door reports.
		{"order_aggregate_over_an_unknown_column",
			`SELECT COUNT(*) AS v FROM %s WHERE SUM(zz)`, "42703", `42703 column "zz" does not exist`},
		{"order_unknown_column_left_of_an_aggregate",
			`SELECT COUNT(*) AS v FROM %s WHERE zz > 0 AND SUM(n)`, "42703", "42703"},
		{"order_window_over_an_unknown_column",
			`SELECT COUNT(*) AS v FROM %s WHERE COUNT(*) OVER (PARTITION BY zz)`,
			"42P20", "42P20 — before names resolve"},
		// A SUBQUERY's own row-filtering clause is the same clause one level
		// down, and the server refuses it there too (#1125). The class is the
		// subquery's, not the outer statement's, and it is raised at plan
		// time — the five-arm half is the L1 table's EXISTS/*/winarg cells.
		{"window_in_an_exists_subquerys_where",
			`SELECT COUNT(*) AS v FROM %s WHERE EXISTS (SELECT 1 FROM %s z WHERE SUM(z.n) OVER () > 0)`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
		{"window_in_an_in_subquerys_where",
			`SELECT COUNT(*) AS v FROM %s WHERE n IN (SELECT z.n FROM %s z WHERE SUM(z.n) OVER () > 0)`,
			"42P20", "42P20 window functions are not allowed in WHERE"},
	} {
		t.Run(c.kind, func(t *testing.T) {
			sql := c.sql
			if strings.Count(sql, "%s") == 2 {
				sql = fmt.Sprintf(sql, tbl, tbl)
			} else {
				sql = fmt.Sprintf(sql, tbl)
			}
			res, err := rig.db.Query(rig.ctx, sql)
			if err == nil {
				t.Fatalf("%s ANSWERED %v where PostgreSQL 17.11 raises %s\n  SQL: %s",
					c.kind, res.Rows, c.pg, sql)
			}
			if st := sqlerr.StateOf(err); st != c.state {
				t.Errorf("%s: SQLSTATE %q, want %q (PostgreSQL 17.11: %s)\n  err: %v",
					c.kind, st, c.state, c.pg, err)
			}
		})
	}
}

// TestArcPTAMergeWhenConditionTakesTheSameRule is the fourth DML verb. Its
// bespoke type check typed a literal, a column, arithmetic and a CAST and
// nothing else, so a CALL, a CASE or a polymorphic call in a `WHEN … AND`
// fell through and the clause FIRED.
func TestArcPTAMergeWhenConditionTakesTheSameRule(t *testing.T) {
	rig := ptDoorRigUp(t)
	for _, c := range []struct{ kind, cond, state, pg string }{
		{"column", `s.n`, "42804", "42804 argument of WHEN must be type boolean, not type bigint"},
		{"call", `upper(s.s)`, "42804", "42804 … not type text"},
		{"case", `CASE WHEN s.n > 1 THEN 1 ELSE 0 END`, "42804", "42804 … not type integer"},
		{"polymorphic", `COALESCE(s.n, 1)`, "42804", "42804 … not type bigint"},
		{"cast", `CAST(s.n AS BIGINT)`, "42804", "42804 … not type bigint"},
		{"array", `ARRAY[1]`, "42804", "42804 … not type integer[]"},
		{"aggregate", `SUM(s.n)`, "42803", "42803 aggregate functions are not allowed"},
	} {
		t.Run(c.kind, func(t *testing.T) {
			tgt, src := ptMergeTables(t, rig, c.kind)
			sql := fmt.Sprintf(
				`MERGE INTO %s t USING %s s ON t.id = s.id WHEN MATCHED AND %s THEN DELETE`,
				tgt, src, c.cond)
			_, err := rig.db.Execute(rig.ctx, sql)
			if err == nil {
				t.Fatalf("%s ANSWERED where PostgreSQL 17.11 raises %s; %d of 2 target rows left\n  SQL: %s",
					c.kind, c.pg, rig.surviving(t, tgt), sql)
			}
			if st := sqlerr.StateOf(err); st != c.state {
				t.Errorf("%s: SQLSTATE %q, want %q (PostgreSQL 17.11: %s)\n  err: %v",
					c.kind, st, c.state, c.pg, err)
			}
			if left := rig.surviving(t, tgt); left != 2 {
				t.Errorf("%s: the REFUSED MERGE changed the target — %d of 2 rows left", c.kind, left)
			}
		})
	}
}

// --- the rig: three DML doors over one embedded database --------------------

type ptDoor struct {
	name string
	exec func(t *testing.T, sql string) (string, error)
}

type ptDoorRig struct {
	db    *wadjet.DB
	doors []ptDoor
	ctx   context.Context
	seq   int
}

func ptDoorRigUp(t *testing.T) *ptDoorRig {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	srv := New(Config{Addr: ":0", Catalog: db.Catalog()}, nil)
	hs := httptest.NewServer(srv.Mux())
	t.Cleanup(hs.Close)

	pg := pgwire.NewServer(db, pgwire.Config{}, nil)
	if err := pg.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pg.Shutdown)

	rig := &ptDoorRig{db: db, ctx: ctx}
	rig.doors = []ptDoor{
		{name: "embedded", exec: func(t *testing.T, sql string) (string, error) {
			res, err := db.Execute(ctx, sql)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s %d", res.Command, res.RowsAffected), nil
		}},
		{name: "pgwire", exec: func(t *testing.T, sql string) (string, error) {
			conn, err := pgx.Connect(ctx, fmt.Sprintf(
				"postgres://wadjet:wadjet@%s/wadjet?sslmode=disable", pg.Addr()))
			if err != nil {
				return "", err
			}
			defer conn.Close(ctx)
			tag, err := conn.Exec(ctx, sql)
			if err != nil {
				return "", err
			}
			return tag.String(), nil
		}},
		{name: "http", exec: func(t *testing.T, sql string) (string, error) {
			body, _ := json.Marshal(map[string]string{"sql": sql})
			resp, err := http.Post(hs.URL+"/v1/queries", "application/json", bytes.NewReader(body))
			if err != nil {
				return "", err
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)
			var out struct {
				Rows     []map[string]any `json:"rows"`
				Error    string           `json:"error"`
				SQLState string           `json:"sqlstate"`
			}
			if uerr := json.Unmarshal(raw, &out); uerr != nil {
				return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, raw)
			}
			if out.Error != "" {
				// The door carries the SQLSTATE in its own field, so the
				// census can hold it to the same code the other two report.
				return "", sqlerr.Wrap(out.SQLState, fmt.Errorf("%s", out.Error))
			}
			if len(out.Rows) == 0 {
				return "", fmt.Errorf("the HTTP door returned no tag: %s", raw)
			}
			return fmt.Sprint(out.Rows[0]["result"]), nil
		}},
	}
	return rig
}

// ptDoorTable ingests a FRESH four-row table per (cell, door), so what a
// statement removed is read from the table it ran against and nothing else.
func ptDoorTable(t *testing.T, rig *ptDoorRig, kind, door string) string {
	t.Helper()
	rig.seq++
	name := fmt.Sprintf("dtc%03d", rig.seq)
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
		{Name: "b", Type: parquet.TypeBool},
		// The CONTAINER columns: a ROW whose fields are a bigint and a
		// boolean, an ARRAY of bigint and a MAP of text→bigint. A field path,
		// a subscript and the container itself are three more node kinds, and
		// the boolean field is the one shape that must still ANSWER.
		{Name: "r", Type: parquet.TypeRow, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeInt64},
			{Name: "ok", Type: parquet.TypeBool},
		}},
		{Name: "arr", Type: parquet.TypeArray,
			ElementType: &parquet.Column{Name: "e", Type: parquet.TypeInt64}},
		{Name: "m", Type: parquet.TypeMap, ElementType: &parquet.Column{
			Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64},
			}}},
	}}
	if err := rig.db.CreateTable(rig.ctx, name, sch, nil); err != nil {
		t.Fatal(err)
	}
	ing := rig.db.NewIngester(name, sch, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	row := func(id, n int64, s string, b bool) map[string]any {
		return map[string]any{"id": id, "n": n, "s": s, "b": b,
			"r":   map[string]any{"a": n, "ok": b},
			"arr": []any{n, int64(9)},
			"m":   map[string]any{"k": n}}
	}
	if err := ing.Ingest(rig.ctx, []map[string]any{
		row(1, 5, "a", true), row(2, 0, "b", false),
		row(3, 7, "c", true), row(4, 1, "d", false),
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(rig.ctx); err != nil {
		t.Fatal(err)
	}
	return name
}

func ptMergeTables(t *testing.T, rig *ptDoorRig, kind string) (string, string) {
	t.Helper()
	rig.seq++
	tgt := fmt.Sprintf("mtg%03d", rig.seq)
	src := fmt.Sprintf("msr%03d", rig.seq)
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "n", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}}
	for name, rows := range map[string][]map[string]any{
		tgt: {{"id": int64(1), "n": int64(5), "s": "a"}, {"id": int64(2), "n": int64(0), "s": "b"}},
		src: {{"id": int64(1), "n": int64(50), "s": "x"}, {"id": int64(3), "n": int64(30), "s": "z"}},
	} {
		if err := rig.db.CreateTable(rig.ctx, name, sch, nil); err != nil {
			t.Fatal(err)
		}
		ing := rig.db.NewIngester(name, sch, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
		if err := ing.Ingest(rig.ctx, rows); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(rig.ctx); err != nil {
			t.Fatal(err)
		}
	}
	return tgt, src
}

func (r *ptDoorRig) surviving(t *testing.T, name string) int {
	t.Helper()
	res, err := r.db.Query(r.ctx, "SELECT COUNT(*) AS v FROM "+name)
	if err != nil {
		t.Fatalf("counting %s: %v", name, err)
	}
	n := 0
	fmt.Sscanf(fmt.Sprint(res.Rows[0]["v"]), "%d", &n)
	return n
}

// ptDoorState reads the SQLSTATE a door reported. The embedded door carries it
// on the error; pgwire and HTTP carry it in the message they transport, so the
// code is read out of the text there.
func ptDoorState(err error) string {
	if st := sqlerr.StateOf(err); st != "" {
		return st
	}
	msg := err.Error()
	for _, code := range []string{"42804", "42803", "42P20", "22P02", "42601", "22025", "0A000"} {
		if strings.Contains(msg, code) {
			return code
		}
	}
	// pgx reports the SQLSTATE in its own field; pgconn.PgError's Error()
	// prints "(SQLSTATE XXXXX)".
	if i := strings.Index(msg, "SQLSTATE "); i >= 0 && len(msg) >= i+14 {
		return msg[i+9 : i+14]
	}
	return ""
}
