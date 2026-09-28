// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle/intround"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// An integer assignment rounds a fractional value by PostgreSQL's TYPE of the
// source — a numeric half away from zero, a float8 half to even — not by the
// float64 this engine computes division, SQRT, POWER, EXP, LN, LOG and EXTRACT
// in (#1353). Six spellings of 2.5 stored 3, 3, 3, 2, 2, 2 into one INTEGER
// column; PostgreSQL 17.11 stores 3 for all six.
//
// The table is intround's: the operator and function grammar over every
// operand category, the CASE family and the plan constructs, on the VALUES,
// INSERT … SELECT, UPDATE and MERGE doors, full rows asserted against the
// answers measured on PostgreSQL.
func TestIntegerAssignmentRoundsByPostgresTypeOnEveryDoor(t *testing.T) {
	ctx := context.Background()
	cells := append(append(intround.Cells(), intround.DialectCells()...), intround.MergeSourceCells()...)
	failed := 0
	for _, c := range cells {
		// A fresh fixture per cell: a table rewritten thousands of times over
		// grows a file per statement, and the cells are independent.
		got, err := intRoundCell(ctx, c)
		key := c.Name + " [" + c.Door + "]"
		pin, pinned := intRoundKnownRefusals[key]
		if !pinned {
			pin, pinned = intround.MergeSourceRefusal(c.Name)
		}
		if pinned {
			if err == nil || !strings.Contains(err.Error(), pin) {
				failed++
				t.Errorf("%s: the pinned refusal moved (stored %s, err %v); if it now stores %s, "+
					"delete the pin", key, got, err, c.Want)
			}
			continue
		}
		if err != nil {
			failed++
			t.Errorf("%s: %v", key, err)
			continue
		}
		if got != c.Want {
			failed++
			t.Errorf("%s: stored %s, PostgreSQL stores %s", key, got, c.Want)
		}
	}
	if failed > 0 {
		t.Logf("%d of %d cells differ from PostgreSQL 17.11", failed, len(cells))
	}
}

// intRoundKnownRefusals are cells that refuse BEFORE any assignment: SELECT
// GREATEST(numeric, float8) whose numeric arm wins, and SELECT NULLIF(numeric
// column, float8 expression), fail on the evaluator's float8 output vector
// (the #361 guard), on any SELECT, where PostgreSQL answers the double. A separate defect from the rounding, recorded for
// filing; the UPDATE, MERGE and VALUES doors of the same spelling answer and
// are asserted. A pin that starts agreeing fails — delete it.
var intRoundKnownRefusals = map[string]string{
	"fold/GREATEST(numcol,f8col-1) [select]":     "cannot store string into FLOAT64 vector",
	"fold/GREATEST(div_numcol,f8col-1) [select]": "cannot store string into FLOAT64 vector",
	"fold/NULLIF(numcol,f8col+9) [select]":       "cannot store string into FLOAT64 vector",
	"fold/NULLIF(numcol,f8lit) [select]":         "cannot store string into FLOAT64 vector",
	"fold/NULLIF(div_numcol,f8col+9) [select]":   "cannot store string into FLOAT64 vector",
	"fold/NULLIF(div_numcol,f8lit) [select]":     "cannot store string into FLOAT64 vector",
}

func intRoundCell(ctx context.Context, c intround.Cell) (string, error) {
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		return "", err
	}
	defer db.Close()
	for _, s := range intround.Fixture {
		if err := intRoundExec(ctx, db, s); err != nil {
			return "", fmt.Errorf("fixture %s: %w", s, err)
		}
	}
	return intRoundRun(ctx, db, c)
}

// intRoundExec runs DDL through Query and DML through Execute, the two
// entries the embedded API has for them.
func intRoundExec(ctx context.Context, db *DB, s string) error {
	if strings.HasPrefix(s, "CREATE") {
		_, err := db.Query(ctx, s)
		return err
	}
	_, err := db.Execute(ctx, s)
	return err
}

func intRoundRun(ctx context.Context, db *DB, c intround.Cell) (string, error) {
	for _, s := range c.Stmts {
		if _, err := db.Execute(ctx, s); err != nil {
			return "", fmt.Errorf("%s: %w", s, err)
		}
	}
	q, err := db.Query(ctx, c.Read)
	if err != nil {
		return "", err
	}
	rows := make([]string, 0, len(q.Rows))
	for i := range q.Rows {
		cells := q.Cells(i)
		part := make([]string, len(cells))
		for j, v := range cells {
			if v == nil {
				part[j] = "NULL"
				continue
			}
			part[j] = fmt.Sprint(v)
		}
		rows = append(rows, strings.Join(part, ":"))
	}
	return strings.Join(rows, " "), nil
}

// The six spellings the issue measured, as a regression of their own.
func TestIntegerAssignmentOfSixSpellingsOfTwoPointFive(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, s := range []string{
		"CREATE TABLE hn (tag TEXT, n INTEGER)",
		"INSERT INTO hn VALUES ('div', 5 / 2.0), ('sqrt', SQRT(6.25)), ('pow', POWER(2.5, 1)), " +
			"('exp', 2.5 * 1), ('lit', 2.5), ('abs', ABS(2.5))",
	} {
		if err := intRoundExec(ctx, db, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	q, err := db.Query(ctx, "SELECT tag, n FROM hn ORDER BY tag")
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for i := range q.Rows {
		c := q.Cells(i)
		got = append(got, fmt.Sprintf("%v=%v", c[0], c[1]))
	}
	// PostgreSQL 17.11: every one of them is numeric, and 2.5 rounds to 3.
	if want := "abs=3 div=3 exp=3 lit=3 pow=3 sqrt=3"; strings.Join(got, " ") != want {
		t.Fatalf("stored %s, PostgreSQL stores %s", strings.Join(got, " "), want)
	}
}

// The declaration half of the same table: for every operator and
// double-precision function over every operand category, the rounding rule
// the assignment picks must be the one PostgreSQL's type picks — half to even
// for a pg_typeof() of double precision, half away from zero for numeric.
// An integer or bigint result has no fraction to round, and is not asked.
func TestIntegerAssignmentRuleFollowsPgTypeof(t *testing.T) {
	schema := []parquet.Column{
		{Name: "i", Type: parquet.TypeInt32},
		{Name: "b", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 10, Scale: 2},
		{Name: "f", Type: parquet.TypeFloat64},
	}
	asked, wrong := 0, 0
	for _, c := range intround.TypeCells() {
		var wantFloat bool
		switch c.PGType {
		case "double precision":
			wantFloat = true
		case "numeric":
		default:
			continue
		}
		node, err := plansql.ParseExpression(c.Expr)
		if err != nil {
			t.Fatalf("%s: %v", c.Expr, err)
		}
		// A value this engine carries as an integer has no fraction to
		// round whatever PostgreSQL calls it: NULLIF(5, f) is 5 or NULL,
		// float8 there and int4 here.
		if d, conf := physical.DeclaredTypeOfNode(node, schema); conf == expr.Decided &&
			(d.ID == parquet.TypeInt32 || d.ID == parquet.TypeInt64) {
			continue
		}
		asked++
		if got := dmlSourceIsFloat(node, schema, nil, nil); got != wantFloat {
			wrong++
			rule := map[bool]string{true: "half to even (float8)", false: "half away from zero (numeric)"}
			t.Errorf("%s: the assignment rounds %s; pg_typeof is %s", c.Expr, rule[got], c.PGType)
		}
	}
	if wrong > 0 {
		t.Logf("%d of %d expressions round by a rule their PostgreSQL type does not", wrong, asked)
	}
}

// A MERGE subquery source that publishes one name twice has no one
// declaration for it, and PostgreSQL 17.11 refuses a reference to that name
// 42702 on every clause kind, qualified or bare. This engine read whichever
// copy the merged row held, undeclared, so a float8 pair rounded by the
// numeric rule: `SET n = src.y` over 2.5, 0.5, -2.5, -0.5 stored 3, 1, -3, -1.
// An unreferenced duplicate does not stop the statement (PostgreSQL answers
// 2, 0, -2, 0 there too).
func TestMergeSubquerySourceNamePublishedTwiceIsAmbiguous(t *testing.T) {
	ctx := context.Background()
	const src = "(SELECT a.id, a.y AS v, a.y, b.y FROM s a JOIN s b ON a.id = b.id) src ON t.id = src.id"
	for _, c := range []struct {
		name, merge, want string
	}{
		{"qualified", "MERGE INTO t USING " + src + " WHEN MATCHED THEN UPDATE SET n = src.y", "42702"},
		{"bare", "MERGE INTO t USING " + src + " WHEN MATCHED THEN UPDATE SET n = y", "42702"},
		{"insert", "MERGE INTO t USING " + src + " WHEN NOT MATCHED THEN INSERT (id, n) VALUES (src.id, src.y)", "42702"},
		{"condition", "MERGE INTO t USING " + src + " WHEN MATCHED AND src.y > 0 THEN UPDATE SET n = src.v", "42702"},
		{"unreferenced", "MERGE INTO t USING " + src + " WHEN MATCHED THEN UPDATE SET n = src.v", "1:2 2:0 3:-2 4:0"},
		// The ON clause names the duplicate: PostgreSQL refuses it as any
		// other reference; this engine matched on the merged row's last copy
		// and wrote (1:2 2:0 over ids 1, 2 — or nothing for `id + 2`, or four
		// INSERTs).
		{"on", "MERGE INTO t USING (SELECT a.id, b.id, a.y FROM s a JOIN s b ON a.id = b.id) src ON t.id = src.id " +
			"WHEN MATCHED THEN UPDATE SET n = src.y", "42702"},
		{"on-alias", "MERGE INTO t USING (SELECT id, id + 2 AS id, y FROM s) src ON t.id = src.id " +
			"WHEN MATCHED THEN UPDATE SET n = src.y", "42702"},
		{"on-alias-first", "MERGE INTO t USING (SELECT id + 2 AS id, id, y FROM s) src ON src.id = t.id " +
			"WHEN MATCHED THEN UPDATE SET n = src.y", "42702"},
		{"on-insert", "MERGE INTO t USING (SELECT id, id + 2 AS id, y FROM s) src ON t.id = src.id " +
			"WHEN NOT MATCHED THEN INSERT (id, n) VALUES (7, src.y)", "42702"},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, s := range []string{
				"CREATE TABLE s (id INTEGER, y DOUBLE)",
				"INSERT INTO s VALUES (1, 2.5), (2, 0.5), (3, -2.5), (4, -0.5)",
				"CREATE TABLE t (id INTEGER, n INTEGER)",
				"INSERT INTO t SELECT id, 0 FROM s",
			} {
				if err := intRoundExec(ctx, db, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			stmts := []string{c.merge}
			if c.name == "insert" {
				stmts = []string{"DELETE FROM t", c.merge}
			}
			got, err := intRoundRun(ctx, db, intround.Cell{Stmts: stmts, Read: "SELECT id, n FROM t ORDER BY id"})
			if err != nil {
				got = err.Error()
				if strings.Contains(got, "is ambiguous") {
					got = "42702"
				}
			}
			if got != c.want {
				t.Fatalf("got %s, PostgreSQL 17.11: %s", got, c.want)
			}
		})
	}
}

// The MERGE action's expression table (intround.MergeSetCells), every form ×
// source type × action × source relation into INTEGER and BIGINT, against
// PostgreSQL 17.11's stored rows or SQLSTATE. A cell PostgreSQL refuses must
// refuse with its SQLSTATE and write nothing; a cell PostgreSQL answers must
// store its rows, or refuse loudly where this engine cannot evaluate the form
// (intround.MergeSetRefusal, with its SQLSTATE) — never a different value.
func TestMergeSetExpressionTableOnEveryForm(t *testing.T) {
	ctx := context.Background()
	cells := intround.MergeSetCells()
	failed := 0
	for _, c := range cells {
		got, state, err := mergeSetCell(ctx, c)
		if msg := mergeSetVerdict(c, got, state, err); msg != "" {
			failed++
			t.Errorf("%s: %s", c.Name, msg)
		}
	}
	if failed > 0 {
		t.Logf("%d of %d MERGE SET cells differ from PostgreSQL 17.11", failed, len(cells))
	}
}

// mergeSetVerdict is one cell's disagreement with PostgreSQL, or "".
func mergeSetVerdict(c intround.Cell, got, state string, err error) string {
	if pin, ok := intround.MergeSetRefusal(c); ok {
		if err == nil || state != pin {
			return fmt.Sprintf("the pinned %s refusal moved (stored %s, err %v); if it now stores %s, delete the pin",
				pin, got, err, c.Want)
		}
		return ""
	}
	if c.WantErr != "" {
		if err == nil {
			return fmt.Sprintf("stored %s where PostgreSQL raises %s", got, c.WantErr)
		}
		if state != c.WantErr {
			return fmt.Sprintf("raised %s (%v) where PostgreSQL raises %s", state, err, c.WantErr)
		}
		return ""
	}
	if err != nil {
		return fmt.Sprintf("raised %s (%v) where PostgreSQL stores %s", state, err, c.Want)
	}
	if got != c.Want {
		return fmt.Sprintf("stored %s, PostgreSQL stores %s", got, c.Want)
	}
	return ""
}

func mergeSetCell(ctx context.Context, c intround.Cell) (string, string, error) {
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		return "", "", err
	}
	defer db.Close()
	for _, s := range intround.MergeSetFixture {
		if err := intRoundExec(ctx, db, s); err != nil {
			return "", "", fmt.Errorf("fixture %s: %w", s, err)
		}
	}
	for _, s := range c.Stmts {
		if _, err := db.Execute(ctx, s); err != nil {
			// A refused statement writes nothing: read the target back to
			// prove it, since a refusal after a partial write is a write.
			got, rerr := intRoundRun(ctx, db, intround.Cell{Read: c.Read})
			if rerr == nil && got != mergeSetUntouched(c) {
				// never equal to a SQLSTATE, so no verdict accepts it
				return got, "wrote-then-" + sqlerr.StateOf(err), fmt.Errorf("refused AFTER writing %s: %w", got, err)
			}
			return "", sqlerr.StateOf(err), err
		}
	}
	got, err := intRoundRun(ctx, db, intround.Cell{Read: c.Read})
	return got, "", err
}

// mergeSetUntouched is t's rows after the cell's reset statement alone.
func mergeSetUntouched(c intround.Cell) string {
	if c.Door == "merge-insert" {
		return ""
	}
	return "1:0:0 2:0:0 3:0:0 4:0:0"
}

// A JSON field read — `j->>'k'` (text in PostgreSQL) and `j->'k'` (json) — is
// not assigned to an integer column without a cast: PostgreSQL 17.11 raises
// 42804 on every write door, and INSERT … SELECT here already did, from the
// text its plan declares. UPDATE, MERGE and VALUES read the registry's dynamic
// declaration as undecided and stored the JSON number rounded: `SET n =
// j->>'k'` over {"k": 2.5} stored 3. The engine's json_extract spellings are
// the same reads and refuse alike. An explicit CAST(j->>'k' AS INTEGER) is
// the one face left (PostgreSQL 22P02 for the text "2.5"; this engine answers
// 2), pinned.
func TestJSONFieldReadAssignedToIntegerIsText(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct {
		name  string
		stmts []string
		want  string
	}{
		{"update-text-arrow", []string{"UPDATE js SET n = j->>'k'"}, "42804"},
		{"update-json-arrow", []string{"UPDATE js SET n = j->'k'"}, "42804"},
		{"update-json_extract_scalar", []string{"UPDATE js SET n = json_extract_scalar(j, '$.k')"}, "42804"},
		{"merge-update", []string{"MERGE INTO t USING js ON t.id = js.id WHEN MATCHED THEN UPDATE SET n = js.j->>'k'"}, "42804"},
		{"merge-insert", []string{"DELETE FROM t",
			"MERGE INTO t USING js ON t.id = js.id WHEN NOT MATCHED THEN INSERT (id, n) VALUES (js.id, js.j->>'k')"}, "42804"},
		{"values", []string{"INSERT INTO t (id, n) VALUES (9, json_extract('{\"k\": 2.5}', '$.k'))"}, "42804"},
		{"select", []string{"INSERT INTO t (id, n) SELECT id, j->>'k' FROM js"}, "42804"},
		{"cast-numeric", []string{"UPDATE js SET n = CAST(j->>'k' AS NUMERIC)"}, "js 1:3 2:-1"},
		// PIN (filing candidate): PostgreSQL raises 22P02, "2.5" is not an
		// integer's text; the cast reads the JSON number.
		{"cast-integer", []string{"UPDATE js SET n = CAST(j->>'k' AS INTEGER)"}, "js 1:2 2:0"},
	} {
		t.Run(c.name, func(t *testing.T) {
			db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			for _, s := range []string{
				"CREATE TABLE js (id INTEGER, j VARCHAR, n INTEGER)",
				`INSERT INTO js VALUES (1, '{"k": 2.5}', 0), (2, '{"k": -0.5}', 0)`,
				"CREATE TABLE t (id INTEGER, n INTEGER)",
				"INSERT INTO t VALUES (1, 0), (2, 0)",
			} {
				if err := intRoundExec(ctx, db, s); err != nil {
					t.Fatalf("%s: %v", s, err)
				}
			}
			read := "SELECT id, n FROM t ORDER BY id"
			if strings.HasPrefix(c.want, "js ") {
				read = "SELECT id, n FROM js ORDER BY id"
			}
			got, err := intRoundRun(ctx, db, intround.Cell{Stmts: c.stmts, Read: read})
			if err != nil {
				got = sqlerr.StateOf(err)
			} else if strings.HasPrefix(c.want, "js ") {
				got = "js " + got
			}
			if got != c.want {
				t.Fatalf("got %s (%v), PostgreSQL 17.11: %s", got, err, c.want)
			}
		})
	}
}

// An aggregate or a window function where a write door evaluates one row —
// UPDATE SET, INSERT VALUES, a MERGE action's SET or VALUES, a MERGE WHEN
// condition — is PostgreSQL's 42803 / 42P20, and nothing is written. An
// aggregate used to evaluate as a scalar call over the one row and answer
// NULL, so `UPDATE t SET n = MAX(f)` overwrote every row with NULL.
func TestAggregateOrWindowInAWriteDoorRefuses(t *testing.T) {
	ctx := context.Background()
	for _, c := range []struct{ stmt, want string }{
		{"UPDATE t SET n4 = MAX(tf)", "42803"},
		{"UPDATE t SET n4 = COALESCE(MAX(tf), 0) + 1", "42803"},
		{"UPDATE t SET n4 = MAX(tf) OVER ()", "42P20"},
		{"INSERT INTO t (id, n4) VALUES (9, MAX(1))", "42803"},
		{"INSERT INTO t (id, n4) VALUES (9, MAX(1) OVER ())", "42P20"},
		{"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND MAX(s.f) > 0 THEN UPDATE SET n4 = 1", "42803"},
		{"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED AND MAX(s.f) OVER () > 0 THEN UPDATE SET n4 = 1", "42P20"},
		{"MERGE INTO t USING s ON t.id = s.id WHEN MATCHED THEN UPDATE SET n4 = MAX(s.f) + 1", "42803"},
		// a subquery's aggregate is its own statement's
		{"UPDATE t SET n4 = 7 WHERE id = (SELECT MAX(id) FROM s)", "1:0:0 2:0:0 3:0:0 4:7:0"},
	} {
		db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range intround.MergeSetFixture {
			if err := intRoundExec(ctx, db, s); err != nil {
				t.Fatal(err)
			}
		}
		got, err := intRoundRun(ctx, db, intround.Cell{Stmts: []string{c.stmt}, Read: "SELECT id, n4, n8 FROM t ORDER BY id"})
		if err != nil {
			got = sqlerr.StateOf(err)
			if after, rerr := intRoundRun(ctx, db, intround.Cell{Read: "SELECT id, n4, n8 FROM t ORDER BY id"}); rerr != nil ||
				after != "1:0:0 2:0:0 3:0:0 4:0:0" {
				got += " after writing " + after
			}
		}
		if got != c.want {
			t.Errorf("%s: got %s (%v), PostgreSQL 17.11: %s", c.stmt, got, err, c.want)
		}
		db.Close()
	}
}
