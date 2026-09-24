// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestOneAssignmentTableOnEveryDoor is arc VL round 3's assignment gate: every
// source class × every target type, on INSERT … SELECT, UPDATE … SET, MERGE
// and INSERT … VALUES, against the answer PostgreSQL 17.11 gives for the SAME
// statement (measured once, into assignmentCells below: the stored value read
// back as text, or the SQLSTATE of the refusal).
//
// It is the round-2 review's P2 made a gate: the doors used to disagree with
// each other — VALUES and SET stored `'5' || ”` into a bigint while INSERT …
// SELECT refused it, and INSERT … SELECT refused a bigint into TEXT while
// VALUES and SET rendered it. One table (ingest.AssignableToColumn, asked by
// every door) answers one way, and that way is PostgreSQL's.
//
// The sources are columns of every class, a text expression, date arithmetic
// over a column and over an integer column (round-2 B1/B2), a typed NULL and a
// cast; VALUES takes the constant spellings of the same classes. A DATE
// source's arithmetic declares DATE and produces a DATE, so it assigns as one
// (arc VL round 3's first half).
func TestOneAssignmentTableOnEveryDoor(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	cols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "b", Type: parquet.TypeBool, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "ip", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
	}
	for _, name := range []string{"src", "t"} {
		if err := db.CreateTable(ctx, name, parquet.Schema{Columns: cols}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Execute(ctx, "INSERT INTO src (id, i, n, f, dec, s, b, d, ts, ip, u) VALUES "+
		"(1, 7, 9, 1.5, 2.25, '5', true, '2026-03-03', '2026-03-03 10:20:30', '10.0.0.1', "+
		"'00000000-0000-0000-0000-000000000001')"); err != nil {
		t.Fatal(err)
	}
	colType := map[string]parquet.TypeID{}
	for _, c := range cols {
		colType[c.Name] = c.Type
	}
	srcExpr := map[string]string{
		"s_concat": "s || ''", "d_plus1": "d + 1", "d_plus_i": "d + i",
		"null_int": "CAST(NULL AS INTEGER)", "ts_date": "CAST(ts AS DATE)",
	}
	valuesExpr := map[string]string{
		"int_expr": "1 + 1", "big_cast": "CAST(5000000000 AS BIGINT)",
		"float_cast": "CAST(1.5 AS DOUBLE PRECISION)", "num_cast": "CAST(2.25 AS NUMERIC(10,2))",
		"text_cast": "CAST('5' AS TEXT)", "bool_expr": "(1 = 1)",
		"date_expr": "DATE '2026-01-01' + 1", "date_nested": "(DATE '2026-01-01' + 1) + CAST(1 AS INTEGER)",
		"ts_lit":    "TIMESTAMP '2026-01-01 10:20:30'",
		"uuid_cast": "CAST('00000000-0000-0000-0000-000000000002' AS UUID)",
		"null_int":  "CAST(NULL AS INTEGER)", "int_lit": "5", "bool_lit": "TRUE",
	}
	colWord := regexp.MustCompile(`\b(i|n|f|dec|s|b|d|ts|ip|u)\b`)
	mismatches := 0
	for _, c := range assignmentCells {
		var stmts []string
		switch c.door {
		case "values":
			stmts = []string{fmt.Sprintf("INSERT INTO t (id, %s) VALUES (1, %s)", c.tgt, valuesExpr[c.src])}
		default:
			e, ok := srcExpr[c.src]
			if !ok {
				e = c.src
			}
			switch c.door {
			case "insert_select":
				stmts = []string{fmt.Sprintf("INSERT INTO t (id, %s) SELECT 1, %s FROM src", c.tgt, e)}
			case "update":
				stmts = []string{"INSERT INTO t SELECT * FROM src",
					fmt.Sprintf("UPDATE t SET %s = %s", c.tgt, e)}
			case "merge":
				stmts = []string{"INSERT INTO t (id) VALUES (1)",
					fmt.Sprintf("MERGE INTO t USING src x ON t.id = x.id WHEN MATCHED THEN UPDATE SET %s = %s",
						c.tgt, colWord.ReplaceAllString(e, "x.$1"))}
			}
		}
		if _, err := db.Execute(ctx, "DELETE FROM t"); err != nil {
			t.Fatal(err)
		}
		var got, gotKind string
		var serr error
		for _, st := range stmts {
			if _, serr = db.Execute(ctx, st); serr != nil {
				break
			}
		}
		if serr != nil {
			gotKind, got = "err", sqlerr.StateOf(serr)
			if got == "" {
				got = "NO-SQLSTATE: " + serr.Error()
			}
		} else {
			res, err := db.Query(ctx, "SELECT "+c.tgt+" FROM t")
			if err != nil || len(res.Rows) != 1 {
				t.Fatalf("%s/%s→%s: reading back: %v %v", c.door, c.src, c.tgt, err, res)
			}
			gotKind, got = "ok", assignmentShown(res.Rows[0][c.tgt], colType[c.tgt])
		}
		if gotKind != c.kind || got != c.want {
			mismatches++
			t.Errorf("%s: %s → %s: got %s %q, PostgreSQL 17.11 %s %q\n  %s",
				c.door, c.src, c.tgt, gotKind, got, c.kind, c.want, strings.Join(stmts, "; "))
		}
	}
	t.Logf("%d cells, %d disagree with PostgreSQL", len(assignmentCells), mismatches)
}

// assignmentShown renders a read-back value the way the PostgreSQL side of the
// table was read (text, `true`/`false`, host(inet), to_char for a timestamp).
func assignmentShown(v any, t parquet.TypeID) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case bool:
		return strconv.FormatBool(x)
	case float64:
		// float8out's shortest form: positional below 1e15 (5000000000,
		// not 5e+09).
		if a := math.Abs(x); a != 0 && (a < 1e-4 || a >= 1e15) {
			return strconv.FormatFloat(x, 'g', -1, 64)
		}
		return strconv.FormatFloat(x, 'f', -1, 64)
	case int64:
		if t == parquet.TypeTimestamp {
			return batch.FormatTimestamp(x)
		}
		return strconv.FormatInt(x, 10)
	}
	return fmt.Sprint(v)
}

type assignmentCell struct{ door, src, tgt, kind, want string }

// Measured on PostgreSQL 17.11 (wadjet-pg-vl) by the arc VL round-3 generator:
// door, source, target column, ok|err, the value read back as text or the
// SQLSTATE.
var assignmentCells = []assignmentCell{
	{"insert_select", "i", "i", "ok", "7"},
	{"update", "i", "i", "ok", "7"},
	{"merge", "i", "i", "ok", "7"},
	{"insert_select", "i", "n", "ok", "7"},
	{"update", "i", "n", "ok", "7"},
	{"merge", "i", "n", "ok", "7"},
	{"insert_select", "i", "f", "ok", "7"},
	{"update", "i", "f", "ok", "7"},
	{"merge", "i", "f", "ok", "7"},
	{"insert_select", "i", "dec", "ok", "7.00"},
	{"update", "i", "dec", "ok", "7.00"},
	{"merge", "i", "dec", "ok", "7.00"},
	{"insert_select", "i", "s", "ok", "7"},
	{"update", "i", "s", "ok", "7"},
	{"merge", "i", "s", "ok", "7"},
	{"insert_select", "i", "b", "err", "42804"},
	{"update", "i", "b", "err", "42804"},
	{"merge", "i", "b", "err", "42804"},
	{"insert_select", "i", "d", "err", "42804"},
	{"update", "i", "d", "err", "42804"},
	{"merge", "i", "d", "err", "42804"},
	{"insert_select", "i", "ts", "err", "42804"},
	{"update", "i", "ts", "err", "42804"},
	{"merge", "i", "ts", "err", "42804"},
	{"insert_select", "i", "ip", "err", "42804"},
	{"update", "i", "ip", "err", "42804"},
	{"merge", "i", "ip", "err", "42804"},
	{"insert_select", "i", "u", "err", "42804"},
	{"update", "i", "u", "err", "42804"},
	{"merge", "i", "u", "err", "42804"},
	{"insert_select", "n", "i", "ok", "9"},
	{"update", "n", "i", "ok", "9"},
	{"merge", "n", "i", "ok", "9"},
	{"insert_select", "n", "n", "ok", "9"},
	{"update", "n", "n", "ok", "9"},
	{"merge", "n", "n", "ok", "9"},
	{"insert_select", "n", "f", "ok", "9"},
	{"update", "n", "f", "ok", "9"},
	{"merge", "n", "f", "ok", "9"},
	{"insert_select", "n", "dec", "ok", "9.00"},
	{"update", "n", "dec", "ok", "9.00"},
	{"merge", "n", "dec", "ok", "9.00"},
	{"insert_select", "n", "s", "ok", "9"},
	{"update", "n", "s", "ok", "9"},
	{"merge", "n", "s", "ok", "9"},
	{"insert_select", "n", "b", "err", "42804"},
	{"update", "n", "b", "err", "42804"},
	{"merge", "n", "b", "err", "42804"},
	{"insert_select", "n", "d", "err", "42804"},
	{"update", "n", "d", "err", "42804"},
	{"merge", "n", "d", "err", "42804"},
	{"insert_select", "n", "ts", "err", "42804"},
	{"update", "n", "ts", "err", "42804"},
	{"merge", "n", "ts", "err", "42804"},
	{"insert_select", "n", "ip", "err", "42804"},
	{"update", "n", "ip", "err", "42804"},
	{"merge", "n", "ip", "err", "42804"},
	{"insert_select", "n", "u", "err", "42804"},
	{"update", "n", "u", "err", "42804"},
	{"merge", "n", "u", "err", "42804"},
	{"insert_select", "f", "i", "ok", "2"},
	{"update", "f", "i", "ok", "2"},
	{"merge", "f", "i", "ok", "2"},
	{"insert_select", "f", "n", "ok", "2"},
	{"update", "f", "n", "ok", "2"},
	{"merge", "f", "n", "ok", "2"},
	{"insert_select", "f", "f", "ok", "1.5"},
	{"update", "f", "f", "ok", "1.5"},
	{"merge", "f", "f", "ok", "1.5"},
	{"insert_select", "f", "dec", "ok", "1.50"},
	{"update", "f", "dec", "ok", "1.50"},
	{"merge", "f", "dec", "ok", "1.50"},
	{"insert_select", "f", "s", "ok", "1.5"},
	{"update", "f", "s", "ok", "1.5"},
	{"merge", "f", "s", "ok", "1.5"},
	{"insert_select", "f", "b", "err", "42804"},
	{"update", "f", "b", "err", "42804"},
	{"merge", "f", "b", "err", "42804"},
	{"insert_select", "f", "d", "err", "42804"},
	{"update", "f", "d", "err", "42804"},
	{"merge", "f", "d", "err", "42804"},
	{"insert_select", "f", "ts", "err", "42804"},
	{"update", "f", "ts", "err", "42804"},
	{"merge", "f", "ts", "err", "42804"},
	{"insert_select", "f", "ip", "err", "42804"},
	{"update", "f", "ip", "err", "42804"},
	{"merge", "f", "ip", "err", "42804"},
	{"insert_select", "f", "u", "err", "42804"},
	{"update", "f", "u", "err", "42804"},
	{"merge", "f", "u", "err", "42804"},
	{"insert_select", "dec", "i", "ok", "2"},
	{"update", "dec", "i", "ok", "2"},
	{"merge", "dec", "i", "ok", "2"},
	{"insert_select", "dec", "n", "ok", "2"},
	{"update", "dec", "n", "ok", "2"},
	{"merge", "dec", "n", "ok", "2"},
	{"insert_select", "dec", "f", "ok", "2.25"},
	{"update", "dec", "f", "ok", "2.25"},
	{"merge", "dec", "f", "ok", "2.25"},
	{"insert_select", "dec", "dec", "ok", "2.25"},
	{"update", "dec", "dec", "ok", "2.25"},
	{"merge", "dec", "dec", "ok", "2.25"},
	{"insert_select", "dec", "s", "ok", "2.25"},
	{"update", "dec", "s", "ok", "2.25"},
	{"merge", "dec", "s", "ok", "2.25"},
	{"insert_select", "dec", "b", "err", "42804"},
	{"update", "dec", "b", "err", "42804"},
	{"merge", "dec", "b", "err", "42804"},
	{"insert_select", "dec", "d", "err", "42804"},
	{"update", "dec", "d", "err", "42804"},
	{"merge", "dec", "d", "err", "42804"},
	{"insert_select", "dec", "ts", "err", "42804"},
	{"update", "dec", "ts", "err", "42804"},
	{"merge", "dec", "ts", "err", "42804"},
	{"insert_select", "dec", "ip", "err", "42804"},
	{"update", "dec", "ip", "err", "42804"},
	{"merge", "dec", "ip", "err", "42804"},
	{"insert_select", "dec", "u", "err", "42804"},
	{"update", "dec", "u", "err", "42804"},
	{"merge", "dec", "u", "err", "42804"},
	{"insert_select", "s", "i", "err", "42804"},
	{"update", "s", "i", "err", "42804"},
	{"merge", "s", "i", "err", "42804"},
	{"insert_select", "s", "n", "err", "42804"},
	{"update", "s", "n", "err", "42804"},
	{"merge", "s", "n", "err", "42804"},
	{"insert_select", "s", "f", "err", "42804"},
	{"update", "s", "f", "err", "42804"},
	{"merge", "s", "f", "err", "42804"},
	{"insert_select", "s", "dec", "err", "42804"},
	{"update", "s", "dec", "err", "42804"},
	{"merge", "s", "dec", "err", "42804"},
	{"insert_select", "s", "s", "ok", "5"},
	{"update", "s", "s", "ok", "5"},
	{"merge", "s", "s", "ok", "5"},
	{"insert_select", "s", "b", "err", "42804"},
	{"update", "s", "b", "err", "42804"},
	{"merge", "s", "b", "err", "42804"},
	{"insert_select", "s", "d", "err", "42804"},
	{"update", "s", "d", "err", "42804"},
	{"merge", "s", "d", "err", "42804"},
	{"insert_select", "s", "ts", "err", "42804"},
	{"update", "s", "ts", "err", "42804"},
	{"merge", "s", "ts", "err", "42804"},
	{"insert_select", "s", "ip", "err", "42804"},
	{"update", "s", "ip", "err", "42804"},
	{"merge", "s", "ip", "err", "42804"},
	{"insert_select", "s", "u", "err", "42804"},
	{"update", "s", "u", "err", "42804"},
	{"merge", "s", "u", "err", "42804"},
	{"insert_select", "b", "i", "err", "42804"},
	{"update", "b", "i", "err", "42804"},
	{"merge", "b", "i", "err", "42804"},
	{"insert_select", "b", "n", "err", "42804"},
	{"update", "b", "n", "err", "42804"},
	{"merge", "b", "n", "err", "42804"},
	{"insert_select", "b", "f", "err", "42804"},
	{"update", "b", "f", "err", "42804"},
	{"merge", "b", "f", "err", "42804"},
	{"insert_select", "b", "dec", "err", "42804"},
	{"update", "b", "dec", "err", "42804"},
	{"merge", "b", "dec", "err", "42804"},
	{"insert_select", "b", "s", "ok", "true"},
	{"update", "b", "s", "ok", "true"},
	{"merge", "b", "s", "ok", "true"},
	{"insert_select", "b", "b", "ok", "true"},
	{"update", "b", "b", "ok", "true"},
	{"merge", "b", "b", "ok", "true"},
	{"insert_select", "b", "d", "err", "42804"},
	{"update", "b", "d", "err", "42804"},
	{"merge", "b", "d", "err", "42804"},
	{"insert_select", "b", "ts", "err", "42804"},
	{"update", "b", "ts", "err", "42804"},
	{"merge", "b", "ts", "err", "42804"},
	{"insert_select", "b", "ip", "err", "42804"},
	{"update", "b", "ip", "err", "42804"},
	{"merge", "b", "ip", "err", "42804"},
	{"insert_select", "b", "u", "err", "42804"},
	{"update", "b", "u", "err", "42804"},
	{"merge", "b", "u", "err", "42804"},
	{"insert_select", "d", "i", "err", "42804"},
	{"update", "d", "i", "err", "42804"},
	{"merge", "d", "i", "err", "42804"},
	{"insert_select", "d", "n", "err", "42804"},
	{"update", "d", "n", "err", "42804"},
	{"merge", "d", "n", "err", "42804"},
	{"insert_select", "d", "f", "err", "42804"},
	{"update", "d", "f", "err", "42804"},
	{"merge", "d", "f", "err", "42804"},
	{"insert_select", "d", "dec", "err", "42804"},
	{"update", "d", "dec", "err", "42804"},
	{"merge", "d", "dec", "err", "42804"},
	{"insert_select", "d", "s", "ok", "2026-03-03"},
	{"update", "d", "s", "ok", "2026-03-03"},
	{"merge", "d", "s", "ok", "2026-03-03"},
	{"insert_select", "d", "b", "err", "42804"},
	{"update", "d", "b", "err", "42804"},
	{"merge", "d", "b", "err", "42804"},
	{"insert_select", "d", "d", "ok", "2026-03-03"},
	{"update", "d", "d", "ok", "2026-03-03"},
	{"merge", "d", "d", "ok", "2026-03-03"},
	{"insert_select", "d", "ts", "ok", "2026-03-03 00:00:00"},
	{"update", "d", "ts", "ok", "2026-03-03 00:00:00"},
	{"merge", "d", "ts", "ok", "2026-03-03 00:00:00"},
	{"insert_select", "d", "ip", "err", "42804"},
	{"update", "d", "ip", "err", "42804"},
	{"merge", "d", "ip", "err", "42804"},
	{"insert_select", "d", "u", "err", "42804"},
	{"update", "d", "u", "err", "42804"},
	{"merge", "d", "u", "err", "42804"},
	{"insert_select", "ts", "i", "err", "42804"},
	{"update", "ts", "i", "err", "42804"},
	{"merge", "ts", "i", "err", "42804"},
	{"insert_select", "ts", "n", "err", "42804"},
	{"update", "ts", "n", "err", "42804"},
	{"merge", "ts", "n", "err", "42804"},
	{"insert_select", "ts", "f", "err", "42804"},
	{"update", "ts", "f", "err", "42804"},
	{"merge", "ts", "f", "err", "42804"},
	{"insert_select", "ts", "dec", "err", "42804"},
	{"update", "ts", "dec", "err", "42804"},
	{"merge", "ts", "dec", "err", "42804"},
	{"insert_select", "ts", "s", "ok", "2026-03-03 10:20:30"},
	{"update", "ts", "s", "ok", "2026-03-03 10:20:30"},
	{"merge", "ts", "s", "ok", "2026-03-03 10:20:30"},
	{"insert_select", "ts", "b", "err", "42804"},
	{"update", "ts", "b", "err", "42804"},
	{"merge", "ts", "b", "err", "42804"},
	{"insert_select", "ts", "d", "ok", "2026-03-03"},
	{"update", "ts", "d", "ok", "2026-03-03"},
	{"merge", "ts", "d", "ok", "2026-03-03"},
	{"insert_select", "ts", "ts", "ok", "2026-03-03 10:20:30"},
	{"update", "ts", "ts", "ok", "2026-03-03 10:20:30"},
	{"merge", "ts", "ts", "ok", "2026-03-03 10:20:30"},
	{"insert_select", "ts", "ip", "err", "42804"},
	{"update", "ts", "ip", "err", "42804"},
	{"merge", "ts", "ip", "err", "42804"},
	{"insert_select", "ts", "u", "err", "42804"},
	{"update", "ts", "u", "err", "42804"},
	{"merge", "ts", "u", "err", "42804"},
	{"insert_select", "ip", "i", "err", "42804"},
	{"update", "ip", "i", "err", "42804"},
	{"merge", "ip", "i", "err", "42804"},
	{"insert_select", "ip", "n", "err", "42804"},
	{"update", "ip", "n", "err", "42804"},
	{"merge", "ip", "n", "err", "42804"},
	{"insert_select", "ip", "f", "err", "42804"},
	{"update", "ip", "f", "err", "42804"},
	{"merge", "ip", "f", "err", "42804"},
	{"insert_select", "ip", "dec", "err", "42804"},
	{"update", "ip", "dec", "err", "42804"},
	{"merge", "ip", "dec", "err", "42804"},
	{"insert_select", "ip", "s", "ok", "10.0.0.1/32"},
	{"update", "ip", "s", "ok", "10.0.0.1/32"},
	{"merge", "ip", "s", "ok", "10.0.0.1/32"},
	{"insert_select", "ip", "b", "err", "42804"},
	{"update", "ip", "b", "err", "42804"},
	{"merge", "ip", "b", "err", "42804"},
	{"insert_select", "ip", "d", "err", "42804"},
	{"update", "ip", "d", "err", "42804"},
	{"merge", "ip", "d", "err", "42804"},
	{"insert_select", "ip", "ts", "err", "42804"},
	{"update", "ip", "ts", "err", "42804"},
	{"merge", "ip", "ts", "err", "42804"},
	{"insert_select", "ip", "ip", "ok", "10.0.0.1"},
	{"update", "ip", "ip", "ok", "10.0.0.1"},
	{"merge", "ip", "ip", "ok", "10.0.0.1"},
	{"insert_select", "ip", "u", "err", "42804"},
	{"update", "ip", "u", "err", "42804"},
	{"merge", "ip", "u", "err", "42804"},
	{"insert_select", "u", "i", "err", "42804"},
	{"update", "u", "i", "err", "42804"},
	{"merge", "u", "i", "err", "42804"},
	{"insert_select", "u", "n", "err", "42804"},
	{"update", "u", "n", "err", "42804"},
	{"merge", "u", "n", "err", "42804"},
	{"insert_select", "u", "f", "err", "42804"},
	{"update", "u", "f", "err", "42804"},
	{"merge", "u", "f", "err", "42804"},
	{"insert_select", "u", "dec", "err", "42804"},
	{"update", "u", "dec", "err", "42804"},
	{"merge", "u", "dec", "err", "42804"},
	{"insert_select", "u", "s", "ok", "00000000-0000-0000-0000-000000000001"},
	{"update", "u", "s", "ok", "00000000-0000-0000-0000-000000000001"},
	{"merge", "u", "s", "ok", "00000000-0000-0000-0000-000000000001"},
	{"insert_select", "u", "b", "err", "42804"},
	{"update", "u", "b", "err", "42804"},
	{"merge", "u", "b", "err", "42804"},
	{"insert_select", "u", "d", "err", "42804"},
	{"update", "u", "d", "err", "42804"},
	{"merge", "u", "d", "err", "42804"},
	{"insert_select", "u", "ts", "err", "42804"},
	{"update", "u", "ts", "err", "42804"},
	{"merge", "u", "ts", "err", "42804"},
	{"insert_select", "u", "ip", "err", "42804"},
	{"update", "u", "ip", "err", "42804"},
	{"merge", "u", "ip", "err", "42804"},
	{"insert_select", "u", "u", "ok", "00000000-0000-0000-0000-000000000001"},
	{"update", "u", "u", "ok", "00000000-0000-0000-0000-000000000001"},
	{"merge", "u", "u", "ok", "00000000-0000-0000-0000-000000000001"},
	{"insert_select", "s_concat", "i", "err", "42804"},
	{"update", "s_concat", "i", "err", "42804"},
	{"merge", "s_concat", "i", "err", "42804"},
	{"insert_select", "s_concat", "n", "err", "42804"},
	{"update", "s_concat", "n", "err", "42804"},
	{"merge", "s_concat", "n", "err", "42804"},
	{"insert_select", "s_concat", "f", "err", "42804"},
	{"update", "s_concat", "f", "err", "42804"},
	{"merge", "s_concat", "f", "err", "42804"},
	{"insert_select", "s_concat", "dec", "err", "42804"},
	{"update", "s_concat", "dec", "err", "42804"},
	{"merge", "s_concat", "dec", "err", "42804"},
	{"insert_select", "s_concat", "s", "ok", "5"},
	{"update", "s_concat", "s", "ok", "5"},
	{"merge", "s_concat", "s", "ok", "5"},
	{"insert_select", "s_concat", "b", "err", "42804"},
	{"update", "s_concat", "b", "err", "42804"},
	{"merge", "s_concat", "b", "err", "42804"},
	{"insert_select", "s_concat", "d", "err", "42804"},
	{"update", "s_concat", "d", "err", "42804"},
	{"merge", "s_concat", "d", "err", "42804"},
	{"insert_select", "s_concat", "ts", "err", "42804"},
	{"update", "s_concat", "ts", "err", "42804"},
	{"merge", "s_concat", "ts", "err", "42804"},
	{"insert_select", "s_concat", "ip", "err", "42804"},
	{"update", "s_concat", "ip", "err", "42804"},
	{"merge", "s_concat", "ip", "err", "42804"},
	{"insert_select", "s_concat", "u", "err", "42804"},
	{"update", "s_concat", "u", "err", "42804"},
	{"merge", "s_concat", "u", "err", "42804"},
	{"insert_select", "d_plus1", "i", "err", "42804"},
	{"update", "d_plus1", "i", "err", "42804"},
	{"merge", "d_plus1", "i", "err", "42804"},
	{"insert_select", "d_plus1", "n", "err", "42804"},
	{"update", "d_plus1", "n", "err", "42804"},
	{"merge", "d_plus1", "n", "err", "42804"},
	{"insert_select", "d_plus1", "f", "err", "42804"},
	{"update", "d_plus1", "f", "err", "42804"},
	{"merge", "d_plus1", "f", "err", "42804"},
	{"insert_select", "d_plus1", "dec", "err", "42804"},
	{"update", "d_plus1", "dec", "err", "42804"},
	{"merge", "d_plus1", "dec", "err", "42804"},
	{"insert_select", "d_plus1", "s", "ok", "2026-03-04"},
	{"update", "d_plus1", "s", "ok", "2026-03-04"},
	{"merge", "d_plus1", "s", "ok", "2026-03-04"},
	{"insert_select", "d_plus1", "b", "err", "42804"},
	{"update", "d_plus1", "b", "err", "42804"},
	{"merge", "d_plus1", "b", "err", "42804"},
	{"insert_select", "d_plus1", "d", "ok", "2026-03-04"},
	{"update", "d_plus1", "d", "ok", "2026-03-04"},
	{"merge", "d_plus1", "d", "ok", "2026-03-04"},
	{"insert_select", "d_plus1", "ts", "ok", "2026-03-04 00:00:00"},
	{"update", "d_plus1", "ts", "ok", "2026-03-04 00:00:00"},
	{"merge", "d_plus1", "ts", "ok", "2026-03-04 00:00:00"},
	{"insert_select", "d_plus1", "ip", "err", "42804"},
	{"update", "d_plus1", "ip", "err", "42804"},
	{"merge", "d_plus1", "ip", "err", "42804"},
	{"insert_select", "d_plus1", "u", "err", "42804"},
	{"update", "d_plus1", "u", "err", "42804"},
	{"merge", "d_plus1", "u", "err", "42804"},
	{"insert_select", "d_plus_i", "i", "err", "42804"},
	{"update", "d_plus_i", "i", "err", "42804"},
	{"merge", "d_plus_i", "i", "err", "42804"},
	{"insert_select", "d_plus_i", "n", "err", "42804"},
	{"update", "d_plus_i", "n", "err", "42804"},
	{"merge", "d_plus_i", "n", "err", "42804"},
	{"insert_select", "d_plus_i", "f", "err", "42804"},
	{"update", "d_plus_i", "f", "err", "42804"},
	{"merge", "d_plus_i", "f", "err", "42804"},
	{"insert_select", "d_plus_i", "dec", "err", "42804"},
	{"update", "d_plus_i", "dec", "err", "42804"},
	{"merge", "d_plus_i", "dec", "err", "42804"},
	{"insert_select", "d_plus_i", "s", "ok", "2026-03-10"},
	{"update", "d_plus_i", "s", "ok", "2026-03-10"},
	{"merge", "d_plus_i", "s", "ok", "2026-03-10"},
	{"insert_select", "d_plus_i", "b", "err", "42804"},
	{"update", "d_plus_i", "b", "err", "42804"},
	{"merge", "d_plus_i", "b", "err", "42804"},
	{"insert_select", "d_plus_i", "d", "ok", "2026-03-10"},
	{"update", "d_plus_i", "d", "ok", "2026-03-10"},
	{"merge", "d_plus_i", "d", "ok", "2026-03-10"},
	{"insert_select", "d_plus_i", "ts", "ok", "2026-03-10 00:00:00"},
	{"update", "d_plus_i", "ts", "ok", "2026-03-10 00:00:00"},
	{"merge", "d_plus_i", "ts", "ok", "2026-03-10 00:00:00"},
	{"insert_select", "d_plus_i", "ip", "err", "42804"},
	{"update", "d_plus_i", "ip", "err", "42804"},
	{"merge", "d_plus_i", "ip", "err", "42804"},
	{"insert_select", "d_plus_i", "u", "err", "42804"},
	{"update", "d_plus_i", "u", "err", "42804"},
	{"merge", "d_plus_i", "u", "err", "42804"},
	{"insert_select", "null_int", "i", "ok", "<nil>"},
	{"update", "null_int", "i", "ok", "<nil>"},
	{"merge", "null_int", "i", "ok", "<nil>"},
	{"insert_select", "null_int", "n", "ok", "<nil>"},
	{"update", "null_int", "n", "ok", "<nil>"},
	{"merge", "null_int", "n", "ok", "<nil>"},
	{"insert_select", "null_int", "f", "ok", "<nil>"},
	{"update", "null_int", "f", "ok", "<nil>"},
	{"merge", "null_int", "f", "ok", "<nil>"},
	{"insert_select", "null_int", "dec", "ok", "<nil>"},
	{"update", "null_int", "dec", "ok", "<nil>"},
	{"merge", "null_int", "dec", "ok", "<nil>"},
	{"insert_select", "null_int", "s", "ok", "<nil>"},
	{"update", "null_int", "s", "ok", "<nil>"},
	{"merge", "null_int", "s", "ok", "<nil>"},
	{"insert_select", "null_int", "b", "err", "42804"},
	{"update", "null_int", "b", "err", "42804"},
	{"merge", "null_int", "b", "err", "42804"},
	{"insert_select", "null_int", "d", "err", "42804"},
	{"update", "null_int", "d", "err", "42804"},
	{"merge", "null_int", "d", "err", "42804"},
	{"insert_select", "null_int", "ts", "err", "42804"},
	{"update", "null_int", "ts", "err", "42804"},
	{"merge", "null_int", "ts", "err", "42804"},
	{"insert_select", "null_int", "ip", "err", "42804"},
	{"update", "null_int", "ip", "err", "42804"},
	{"merge", "null_int", "ip", "err", "42804"},
	{"insert_select", "null_int", "u", "err", "42804"},
	{"update", "null_int", "u", "err", "42804"},
	{"merge", "null_int", "u", "err", "42804"},
	{"insert_select", "ts_date", "i", "err", "42804"},
	{"update", "ts_date", "i", "err", "42804"},
	{"merge", "ts_date", "i", "err", "42804"},
	{"insert_select", "ts_date", "n", "err", "42804"},
	{"update", "ts_date", "n", "err", "42804"},
	{"merge", "ts_date", "n", "err", "42804"},
	{"insert_select", "ts_date", "f", "err", "42804"},
	{"update", "ts_date", "f", "err", "42804"},
	{"merge", "ts_date", "f", "err", "42804"},
	{"insert_select", "ts_date", "dec", "err", "42804"},
	{"update", "ts_date", "dec", "err", "42804"},
	{"merge", "ts_date", "dec", "err", "42804"},
	{"insert_select", "ts_date", "s", "ok", "2026-03-03"},
	{"update", "ts_date", "s", "ok", "2026-03-03"},
	{"merge", "ts_date", "s", "ok", "2026-03-03"},
	{"insert_select", "ts_date", "b", "err", "42804"},
	{"update", "ts_date", "b", "err", "42804"},
	{"merge", "ts_date", "b", "err", "42804"},
	{"insert_select", "ts_date", "d", "ok", "2026-03-03"},
	{"update", "ts_date", "d", "ok", "2026-03-03"},
	{"merge", "ts_date", "d", "ok", "2026-03-03"},
	{"insert_select", "ts_date", "ts", "ok", "2026-03-03 00:00:00"},
	{"update", "ts_date", "ts", "ok", "2026-03-03 00:00:00"},
	{"merge", "ts_date", "ts", "ok", "2026-03-03 00:00:00"},
	{"insert_select", "ts_date", "ip", "err", "42804"},
	{"update", "ts_date", "ip", "err", "42804"},
	{"merge", "ts_date", "ip", "err", "42804"},
	{"insert_select", "ts_date", "u", "err", "42804"},
	{"update", "ts_date", "u", "err", "42804"},
	{"merge", "ts_date", "u", "err", "42804"},
	{"values", "int_expr", "i", "ok", "2"},
	{"values", "int_expr", "n", "ok", "2"},
	{"values", "int_expr", "f", "ok", "2"},
	{"values", "int_expr", "dec", "ok", "2.00"},
	{"values", "int_expr", "s", "ok", "2"},
	{"values", "int_expr", "b", "err", "42804"},
	{"values", "int_expr", "d", "err", "42804"},
	{"values", "int_expr", "ts", "err", "42804"},
	{"values", "int_expr", "ip", "err", "42804"},
	{"values", "int_expr", "u", "err", "42804"},
	{"values", "big_cast", "i", "err", "22003"},
	{"values", "big_cast", "n", "ok", "5000000000"},
	{"values", "big_cast", "f", "ok", "5000000000"},
	{"values", "big_cast", "dec", "err", "22003"},
	{"values", "big_cast", "s", "ok", "5000000000"},
	{"values", "big_cast", "b", "err", "42804"},
	{"values", "big_cast", "d", "err", "42804"},
	{"values", "big_cast", "ts", "err", "42804"},
	{"values", "big_cast", "ip", "err", "42804"},
	{"values", "big_cast", "u", "err", "42804"},
	{"values", "float_cast", "i", "ok", "2"},
	{"values", "float_cast", "n", "ok", "2"},
	{"values", "float_cast", "f", "ok", "1.5"},
	{"values", "float_cast", "dec", "ok", "1.50"},
	{"values", "float_cast", "s", "ok", "1.5"},
	{"values", "float_cast", "b", "err", "42804"},
	{"values", "float_cast", "d", "err", "42804"},
	{"values", "float_cast", "ts", "err", "42804"},
	{"values", "float_cast", "ip", "err", "42804"},
	{"values", "float_cast", "u", "err", "42804"},
	{"values", "num_cast", "i", "ok", "2"},
	{"values", "num_cast", "n", "ok", "2"},
	{"values", "num_cast", "f", "ok", "2.25"},
	{"values", "num_cast", "dec", "ok", "2.25"},
	{"values", "num_cast", "s", "ok", "2.25"},
	{"values", "num_cast", "b", "err", "42804"},
	{"values", "num_cast", "d", "err", "42804"},
	{"values", "num_cast", "ts", "err", "42804"},
	{"values", "num_cast", "ip", "err", "42804"},
	{"values", "num_cast", "u", "err", "42804"},
	{"values", "text_cast", "i", "err", "42804"},
	{"values", "text_cast", "n", "err", "42804"},
	{"values", "text_cast", "f", "err", "42804"},
	{"values", "text_cast", "dec", "err", "42804"},
	{"values", "text_cast", "s", "ok", "5"},
	{"values", "text_cast", "b", "err", "42804"},
	{"values", "text_cast", "d", "err", "42804"},
	{"values", "text_cast", "ts", "err", "42804"},
	{"values", "text_cast", "ip", "err", "42804"},
	{"values", "text_cast", "u", "err", "42804"},
	{"values", "bool_expr", "i", "err", "42804"},
	{"values", "bool_expr", "n", "err", "42804"},
	{"values", "bool_expr", "f", "err", "42804"},
	{"values", "bool_expr", "dec", "err", "42804"},
	{"values", "bool_expr", "s", "ok", "true"},
	{"values", "bool_expr", "b", "ok", "true"},
	{"values", "bool_expr", "d", "err", "42804"},
	{"values", "bool_expr", "ts", "err", "42804"},
	{"values", "bool_expr", "ip", "err", "42804"},
	{"values", "bool_expr", "u", "err", "42804"},
	{"values", "date_expr", "i", "err", "42804"},
	{"values", "date_expr", "n", "err", "42804"},
	{"values", "date_expr", "f", "err", "42804"},
	{"values", "date_expr", "dec", "err", "42804"},
	{"values", "date_expr", "s", "ok", "2026-01-02"},
	{"values", "date_expr", "b", "err", "42804"},
	{"values", "date_expr", "d", "ok", "2026-01-02"},
	{"values", "date_expr", "ts", "ok", "2026-01-02 00:00:00"},
	{"values", "date_expr", "ip", "err", "42804"},
	{"values", "date_expr", "u", "err", "42804"},
	{"values", "date_nested", "i", "err", "42804"},
	{"values", "date_nested", "n", "err", "42804"},
	{"values", "date_nested", "f", "err", "42804"},
	{"values", "date_nested", "dec", "err", "42804"},
	{"values", "date_nested", "s", "ok", "2026-01-03"},
	{"values", "date_nested", "b", "err", "42804"},
	{"values", "date_nested", "d", "ok", "2026-01-03"},
	{"values", "date_nested", "ts", "ok", "2026-01-03 00:00:00"},
	{"values", "date_nested", "ip", "err", "42804"},
	{"values", "date_nested", "u", "err", "42804"},
	{"values", "ts_lit", "i", "err", "42804"},
	{"values", "ts_lit", "n", "err", "42804"},
	{"values", "ts_lit", "f", "err", "42804"},
	{"values", "ts_lit", "dec", "err", "42804"},
	{"values", "ts_lit", "s", "ok", "2026-01-01 10:20:30"},
	{"values", "ts_lit", "b", "err", "42804"},
	{"values", "ts_lit", "d", "ok", "2026-01-01"},
	{"values", "ts_lit", "ts", "ok", "2026-01-01 10:20:30"},
	{"values", "ts_lit", "ip", "err", "42804"},
	{"values", "ts_lit", "u", "err", "42804"},
	{"values", "uuid_cast", "i", "err", "42804"},
	{"values", "uuid_cast", "n", "err", "42804"},
	{"values", "uuid_cast", "f", "err", "42804"},
	{"values", "uuid_cast", "dec", "err", "42804"},
	{"values", "uuid_cast", "s", "ok", "00000000-0000-0000-0000-000000000002"},
	{"values", "uuid_cast", "b", "err", "42804"},
	{"values", "uuid_cast", "d", "err", "42804"},
	{"values", "uuid_cast", "ts", "err", "42804"},
	{"values", "uuid_cast", "ip", "err", "42804"},
	{"values", "uuid_cast", "u", "ok", "00000000-0000-0000-0000-000000000002"},
	{"values", "null_int", "i", "ok", "<nil>"},
	{"values", "null_int", "n", "ok", "<nil>"},
	{"values", "null_int", "f", "ok", "<nil>"},
	{"values", "null_int", "dec", "ok", "<nil>"},
	{"values", "null_int", "s", "ok", "<nil>"},
	{"values", "null_int", "b", "err", "42804"},
	{"values", "null_int", "d", "err", "42804"},
	{"values", "null_int", "ts", "err", "42804"},
	{"values", "null_int", "ip", "err", "42804"},
	{"values", "null_int", "u", "err", "42804"},
	{"values", "int_lit", "i", "ok", "5"},
	{"values", "int_lit", "n", "ok", "5"},
	{"values", "int_lit", "f", "ok", "5"},
	{"values", "int_lit", "dec", "ok", "5.00"},
	{"values", "int_lit", "s", "ok", "5"},
	{"values", "int_lit", "b", "err", "42804"},
	{"values", "int_lit", "d", "err", "42804"},
	{"values", "int_lit", "ts", "err", "42804"},
	{"values", "int_lit", "ip", "err", "42804"},
	{"values", "int_lit", "u", "err", "42804"},
	{"values", "bool_lit", "i", "err", "42804"},
	{"values", "bool_lit", "n", "err", "42804"},
	{"values", "bool_lit", "f", "err", "42804"},
	{"values", "bool_lit", "dec", "err", "42804"},
	{"values", "bool_lit", "s", "ok", "true"},
	{"values", "bool_lit", "b", "ok", "true"},
	{"values", "bool_lit", "d", "err", "42804"},
	{"values", "bool_lit", "ts", "err", "42804"},
	{"values", "bool_lit", "ip", "err", "42804"},
	{"values", "bool_lit", "u", "err", "42804"},
}

// TestTemporalCTASStoresTheDeclaredType is the CTAS door of the same rule: a
// CREATE TABLE … AS over date arithmetic stores the type PostgreSQL 17.11
// declares for it (measured: format_type over the created table) and the
// value, where the arithmetic's operand is a column, a nested sum or an
// integer column — the spellings whose declaration used to depend on a bare
// number literal (round-2 review B1). `d - d` is PostgreSQL's integer; this
// engine's day count is bigint (a recorded width divergence).
func TestTemporalCTASStoresTheDeclaredType(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "src", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "d", Type: parquet.TypeDate, Nullable: true},
		{Name: "ts", Type: parquet.TypeTimestamp, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO src (id, i, d, ts) VALUES (1, 2, '2026-03-03', '2026-03-03 10:20:30')"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "CREATE TABLE c AS SELECT d + 1 AS a, d + i AS b, (d + 1) + 1 AS c1, "+
		"d + INTERVAL '1 day' AS e, ts + INTERVAL '1 hour' AS g, d - d AS h, CAST(ts AS DATE) + 1 AS k FROM src"); err != nil {
		t.Fatal(err)
	}
	meta, err := db.Catalog().GetTable(ctx, "c")
	if err != nil {
		t.Fatal(err)
	}
	res, err := db.Query(ctx, "SELECT a, b, c1, e, g, h, k FROM c")
	if err != nil || len(res.Rows) != 1 {
		t.Fatalf("reading c: %v %v", err, res)
	}
	want := map[string]struct {
		typ parquet.TypeID
		val string
	}{
		"a": {parquet.TypeDate, "2026-03-04"}, "b": {parquet.TypeDate, "2026-03-05"},
		"c1": {parquet.TypeDate, "2026-03-05"}, "e": {parquet.TypeTimestamp, "2026-03-04 00:00:00"},
		"g": {parquet.TypeTimestamp, "2026-03-03 11:20:30"}, "h": {parquet.TypeInt64, "0"},
		"k": {parquet.TypeDate, "2026-03-04"},
	}
	for _, c := range meta.Schema.Columns {
		w, ok := want[c.Name]
		if !ok {
			continue
		}
		if c.Type != w.typ {
			t.Errorf("CTAS column %s stored %s, want %s", c.Name, c.Type, w.typ)
		}
		if got := assignmentShown(res.Rows[0][c.Name], c.Type); got != w.val {
			t.Errorf("CTAS column %s = %q, want %q", c.Name, got, w.val)
		}
	}
}

// TestTypedTextSourcesReadAsUnknownLiterals pins the ONE superset the
// assignment table keeps for a declared-TEXT source: a call the registry
// declares text for a network or UUID value (expr.DeclaresTextForTypedValue —
// uuid(), int_to_ip, …) is read by the target column's input function, as an
// unknown-typed literal is, on every door. A genuine text expression into the
// same columns is PostgreSQL's 42804 (TestOneAssignmentTableOnEveryDoor).
func TestTypedTextSourcesReadAsUnknownLiterals(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.CreateTable(ctx, "tt", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "ip", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}, nil); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ sql, state string }{
		{"INSERT INTO tt (id, ip, u, s) VALUES (1, int_to_ip(167772161), uuid(), int_to_ip(167772161))", ""},
		{"INSERT INTO tt (id, ip, u, s) SELECT 2, int_to_ip(167772162), uuid(), mask_ip('10.1.2.3', 8)", ""},
		{"UPDATE tt SET ip = int_to_ip(167772163) WHERE id = 1", ""},
		{"MERGE INTO tt USING tt x ON tt.id = x.id WHEN MATCHED THEN UPDATE SET u = uuid()", ""},
		// Read by the column's grammar, so text that names no address is
		// the grammar's 22P02 — never a stored text.
		{"INSERT INTO tt (id, u) VALUES (3, int_to_ip(1))", "22P02"},
		{"INSERT INTO tt (id, u) SELECT 4, int_to_ip(1)", "22P02"},
	} {
		_, err := db.Execute(ctx, tc.sql)
		if got := sqlerr.StateOf(err); (err != nil) != (tc.state != "") || got != tc.state {
			t.Errorf("%s: err=%v (SQLSTATE %q), want %q", tc.sql, err, got, tc.state)
		}
	}
	res, err := db.Query(ctx, "SELECT id, ip, s FROM tt ORDER BY id")
	if err != nil || len(res.Rows) != 2 {
		t.Fatalf("reading tt: %v %v", err, res)
	}
	if got := fmt.Sprint(res.Rows[0]["ip"], " ", res.Rows[0]["s"], " ", res.Rows[1]["ip"]); got != "10.0.0.3 10.0.0.1 10.0.0.2" {
		t.Errorf("stored %q, want \"10.0.0.3 10.0.0.1 10.0.0.2\"", got)
	}
}
