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
	cells := append(intround.Cells(), intround.DialectCells()...)
	failed := 0
	for _, c := range cells {
		// A fresh fixture per cell: a table rewritten thousands of times over
		// grows a file per statement, and the cells are independent.
		got, err := intRoundCell(ctx, c)
		key := c.Name + " [" + c.Door + "]"
		if pin, pinned := intRoundKnownRefusals[key]; pinned {
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
		if got := dmlSourceIsFloat(node, schema); got != wantFloat {
			wrong++
			rule := map[bool]string{true: "half to even (float8)", false: "half away from zero (numeric)"}
			t.Errorf("%s: the assignment rounds %s; pg_typeof is %s", c.Expr, rule[got], c.PGType)
		}
	}
	if wrong > 0 {
		t.Logf("%d of %d expressions round by a rule their PostgreSQL type does not", wrong, asked)
	}
}
