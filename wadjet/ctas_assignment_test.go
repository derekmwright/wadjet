// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TWO WRITE DOORS, ONE NUMBER (#1024 round-2 review B1/B2).
//
// `INSERT … VALUES` and `INSERT INTO … SELECT` write into the same column of
// the same table, and the value they store must be the same value. It was not:
// the query door handed the query's BOX to the writer unconverted, and
// `parquet.DecimalValueFromBox` reads an integer box as the already-UNSCALED
// carrier (ADR-0018 §4), so a BIGINT 5 into `DECIMAL(18,4)` stored **0.0005**
// where the VALUES door and PostgreSQL 17.11 store 5.0000. The same door stored
// `PORT 500000` and `PROTOCOL 5000` — values no port or protocol number can be —
// because the writer's leaf check range-checks the int32 CARRIER and not the
// stored width, which is the hole #814 closed for the literal door and
// `assignIntegerValue` closed for the computed one.
//
// The oracle here is the VALUES door, deliberately. It is pre-existing, gated,
// and carries the assignment casts PostgreSQL performs (#647, #678, #699,
// #814); a new door that disagrees with it disagrees with PostgreSQL. The cells
// where PostgreSQL's own stored value was measured carry it beside them.
func TestBothWriteDoorsStoreTheSameNumber(t *testing.T) {
	ctx := context.Background()

	// Every target declaration a query-sourced write may assign into, and the
	// source types `ingest.AssignableToColumn` admits for it.
	targets := []struct {
		name string
		col  parquet.Column
		// pg is PostgreSQL 17.11's stored value for the integer source 5,
		// measured in this arc's own container; "" where the pair has no
		// PostgreSQL equivalent (PORT, PROTOCOL are wadjet types).
		pg string
	}{
		{"int32", parquet.Column{Name: "v", Type: parquet.TypeInt32, Nullable: true}, "5"},
		{"int64", parquet.Column{Name: "v", Type: parquet.TypeInt64, Nullable: true}, "5"},
		{"port", parquet.Column{Name: "v", Type: parquet.TypePort, Nullable: true}, ""},
		{"protocol", parquet.Column{Name: "v", Type: parquet.TypeProtocol, Nullable: true}, ""},
		{"float32", parquet.Column{Name: "v", Type: parquet.TypeFloat32, Nullable: true}, "5"},
		{"float64", parquet.Column{Name: "v", Type: parquet.TypeFloat64, Nullable: true}, "5"},
		{"decimal_9_2", parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true}, "5.00"},
		{"decimal_18_4", parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}, "5.0000"},
		{"decimal_38_10", parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true}, "5.0000000000"},
	}
	// Source expressions over the fixture, each with the literal the VALUES
	// door writes for the SAME number.
	sources := []struct {
		name    string
		expr    string
		literal string
	}{
		{"int32", "i32", "5"},
		{"int64", "i64", "5"},
		{"float32", "f32", "5.0"},
		{"float64", "f64", "5.0"},
	}

	db := ctasAssignDB(t, ctx)
	for _, tgt := range targets {
		for _, src := range sources {
			t.Run(tgt.name+"_from_"+src.name, func(t *testing.T) {
				vTable := fmt.Sprintf("vv_%s_%s", tgt.name, src.name)
				qTable := fmt.Sprintf("qq_%s_%s", tgt.name, src.name)
				for _, tbl := range []string{vTable, qTable} {
					if err := db.CreateTable(ctx, tbl,
						parquet.Schema{Columns: []parquet.Column{tgt.col}}, nil); err != nil {
						t.Fatal(err)
					}
				}
				_, vErr := db.Execute(ctx, fmt.Sprintf("INSERT INTO %s (v) VALUES (%s)", vTable, src.literal))
				_, qErr := db.Execute(ctx, fmt.Sprintf("INSERT INTO %s (v) SELECT %s FROM asrc", qTable, src.expr))

				// A pair one door refuses the other must refuse, and with the
				// same class: one converter, one answer.
				if (vErr == nil) != (qErr == nil) {
					t.Fatalf("the doors disagree about whether this is writable:\n  VALUES %v\n  SELECT %v", vErr, qErr)
				}
				if vErr != nil {
					if sqlerr.StateOf(vErr) != sqlerr.StateOf(qErr) {
						t.Errorf("SQLSTATE %q through VALUES, %q through SELECT (%v / %v)",
							sqlerr.StateOf(vErr), sqlerr.StateOf(qErr), vErr, qErr)
					}
					return
				}
				want := ctasScalarText(t, ctx, db, "SELECT v FROM "+vTable)
				got := ctasScalarText(t, ctx, db, "SELECT v FROM "+qTable)
				if got != want {
					t.Errorf("stored %q through INSERT … SELECT, %q through INSERT … VALUES", got, want)
				}
				if tgt.pg != "" && src.name == "int64" && got != tgt.pg {
					t.Errorf("stored %q; PostgreSQL 17.11 stores %q for the same statement", got, tgt.pg)
				}
			})
		}
	}
}

// The three ranges a query-sourced write must refuse, each with the door that
// has refused it since before this arc.
//
// PORT is stored as a uint16 and PROTOCOL as a uint8, and the writer's leaf
// check range-checks neither — it checks the int32 carrier. The literal door
// (#814) and the computed door (`assignIntegerValue`) each close that hole
// themselves; the query door now reaches the same converter.
func TestAQuerySourcedWriteRefusesTheSameRangesTheOtherDoorsDo(t *testing.T) {
	ctx := context.Background()
	db := ctasAssignDB(t, ctx)

	cases := []struct {
		name    string
		col     parquet.Column
		expr    string
		literal string
		says    string
	}{
		{"PortAboveItsDomain", parquet.Column{Name: "v", Type: parquet.TypePort, Nullable: true},
			"i64 * 100000", "500000", "PORT value 500000 out of range"},
		{"ProtocolAboveItsDomain", parquet.Column{Name: "v", Type: parquet.TypeProtocol, Nullable: true},
			"i64 * 1000", "5000", "PROTOCOL value 5000 out of range"},
		{"Int32Overflow", parquet.Column{Name: "v", Type: parquet.TypeInt32, Nullable: true},
			"i64 * 1000000000", "5000000000", "out of range"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vTable, qTable := "rv_"+tc.name, "rq_"+tc.name
			for _, tbl := range []string{vTable, qTable} {
				if err := db.CreateTable(ctx, tbl,
					parquet.Schema{Columns: []parquet.Column{tc.col}}, nil); err != nil {
					t.Fatal(err)
				}
			}
			_, vErr := db.Execute(ctx, fmt.Sprintf("INSERT INTO %s (v) VALUES (%s)", vTable, tc.literal))
			_, qErr := db.Execute(ctx, fmt.Sprintf("INSERT INTO %s (v) SELECT %s FROM asrc", qTable, tc.expr))
			if vErr == nil {
				t.Fatalf("the VALUES door accepted %s — this fixture no longer says anything", tc.literal)
			}
			if qErr == nil {
				t.Fatalf("the query door STORED a value outside the column's domain; "+
					"the VALUES door refuses it: %v", vErr)
			}
			for _, err := range []error{vErr, qErr} {
				if !strings.Contains(err.Error(), tc.says) {
					t.Errorf("the refusal does not say %q: %v", tc.says, err)
				}
			}
			if n := ctasScalar(t, ctx, db, "SELECT COUNT(*) AS c FROM "+qTable); n != 0 {
				t.Errorf("the refused statement wrote %d rows", n)
			}
		})
	}
}

// A CTAS needs no conversion, and this is the fixture that says why: its target
// columns ARE the query's declared output, so a DECIMAL column of a created
// table is declared from the same expression that produced the value. Eleven
// shapes whose DECLARATION is DECIMAL(p,s>0) while a row's branch is an
// integer store what the query answered.
func TestACreatedTableNeedsNoAssignmentConversion(t *testing.T) {
	ctx := context.Background()
	db := ctasAssignDB(t, ctx)
	shapes := []string{
		`SELECT CASE WHEN i64 = 5 THEN i64 ELSE d END AS v FROM asrc`,
		`SELECT COALESCE(d, i64) AS v FROM asrc`,
		`SELECT GREATEST(d, i64) AS v FROM asrc`,
		`SELECT LEAST(d, i64) AS v FROM asrc`,
		`SELECT d AS v FROM asrc UNION ALL SELECT i64 FROM asrc`,
		`SELECT CAST(i64 AS DECIMAL(18,4)) AS v FROM asrc`,
		`SELECT SUM(d) AS v FROM asrc`,
		`SELECT AVG(d) AS v FROM asrc`,
		`SELECT d * 2 AS v FROM asrc`,
	}
	for i, sql := range shapes {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			tbl := fmt.Sprintf("cdec%d", i)
			if _, err := db.Query(ctx, fmt.Sprintf("CREATE TABLE %s AS %s", tbl, sql)); err != nil {
				t.Fatalf("%s: %v", sql, err)
			}
			ctasCompareQueries(t, ctx, db, sql, "SELECT v FROM "+tbl)
		})
	}
}

func ctasAssignDB(t *testing.T, ctx context.Context) *DB {
	t.Helper()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 12, Scale: 3, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "asrc", schema, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO asrc (i32, i64, f32, f64, d) VALUES (5, 5, 5.0, 5.0, 5.000)`); err != nil {
		t.Fatal(err)
	}
	return db
}

// ctasScalarText renders a one-cell answer the way a client reads it, so two
// doors' stored values are compared as VALUES and not as Go boxes.
func ctasScalarText(t *testing.T, ctx context.Context, db *DB, sql string) string {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("%s answered %d rows", sql, len(res.Rows))
	}
	return fmt.Sprint(res.Cells(0)[0])
}

// The DECIMAL SOURCE, which is the pair the narrow rule was written for.
//
// A Decimal128 box carries an unscaled integer and no scale, so handing one to
// a column at a different scale stores 1.500 as 0.1500 — which is why
// `AssignableToColumn` refused DECIMAL→DECIMAL outright in round 1. It does not
// have to: the query path boxes a DECIMAL cell as its decimal TEXT, and the
// engine's assignment converter reads text as the VALUE. This fixture is the
// proof, against the VALUES door and against PostgreSQL 17.11's measured
// stored value for the identical statement.
func TestADecimalSourceIsAssignedAtItsValue(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	if err := db.CreateTable(ctx, "dsrc", parquet.Schema{Columns: []parquet.Column{
		{Name: "d", Type: parquet.TypeDecimal, Precision: 12, Scale: 3, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, `INSERT INTO dsrc (d) VALUES (1.500)`); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		col  parquet.Column
		pg   string // PostgreSQL 17.11's stored value for the same statement
	}{
		{"decimal_9_2", parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true}, "1.50"},
		{"decimal_18_4", parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true}, "1.5000"},
		{"decimal_38_10", parquet.Column{Name: "v", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true}, "1.5000000000"},
		{"float32", parquet.Column{Name: "v", Type: parquet.TypeFloat32, Nullable: true}, "1.5"},
		{"float64", parquet.Column{Name: "v", Type: parquet.TypeFloat64, Nullable: true}, "1.5"},
		{"int64", parquet.Column{Name: "v", Type: parquet.TypeInt64, Nullable: true}, "2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			vTable, qTable := "dv_"+tc.name, "dq_"+tc.name
			for _, tbl := range []string{vTable, qTable} {
				if err := db.CreateTable(ctx, tbl,
					parquet.Schema{Columns: []parquet.Column{tc.col}}, nil); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := db.Execute(ctx, fmt.Sprintf("INSERT INTO %s (v) VALUES (1.500)", vTable)); err != nil {
				t.Fatalf("the VALUES door: %v", err)
			}
			if _, err := db.Execute(ctx, fmt.Sprintf("INSERT INTO %s (v) SELECT d FROM dsrc", qTable)); err != nil {
				t.Fatalf("the query door: %v", err)
			}
			want := ctasScalarText(t, ctx, db, "SELECT v FROM "+vTable)
			got := ctasScalarText(t, ctx, db, "SELECT v FROM "+qTable)
			if got != want {
				t.Errorf("stored %q through INSERT … SELECT, %q through INSERT … VALUES", got, want)
			}
			if got != tc.pg {
				t.Errorf("stored %q; PostgreSQL 17.11 stores %q for the same statement", got, tc.pg)
			}
		})
	}
}
