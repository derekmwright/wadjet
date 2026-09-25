// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestAssignmentDoorsAgree is arc VL round 4's door-diff gate (round-3 review
// B2 / P2): every source × every target column, written through EVERY write
// door — INSERT … VALUES, INSERT … SELECT, UPDATE … SET, MERGE … UPDATE SET,
// MERGE … INSERT VALUES — must store the same value or raise the same
// SQLSTATE on all of them (zero door differences), and that answer must be
// PostgreSQL 17.11's (assignmentDoorCells, measured by the same statements
// there; PostgreSQL itself has no door split on any of these cells). The
// sources are constants of every spelling — numeric literals whose scale
// matters (`2.50`, `1.10`), unknown-typed quoted literals (`'yes'`, `'t'`,
// `'2.5'`), booleans, NULLs, typed literals, casts, expressions — which run on
// all five doors, and column expressions, which run on the four doors that
// have a FROM. Then CTAS: `CREATE TABLE … AS SELECT <constant>` stores a
// declared type and a value, and every door assigning the same constant into
// a column of that type must store the same value.
//
// At round 3's tip the doors split: `2.50` / `1.10` into TEXT stored `2.5` /
// `1.1` on INSERT … SELECT only, `2.5` into INTEGER / BIGINT stored 2 there
// and 3 elsewhere, `'t'` / `'yes'` into BOOLEAN were 42804 on INSERT … SELECT
// and code-less strconv errors on the other doors.
func TestAssignmentDoorsAgree(t *testing.T) {
	ctx := context.Background()
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
	openDB := func(t *testing.T) *DB {
		db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		for _, name := range []string{"src", "t"} {
			if err := db.CreateTable(ctx, name, parquet.Schema{Columns: cols}, nil); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.CreateTable(ctx, "one", parquet.Schema{Columns: cols[:1]}, nil); err != nil {
			t.Fatal(err)
		}
		for _, st := range []string{
			"INSERT INTO src (id, i, n, f, dec, s, b, d, ts, ip, u) VALUES (1, 7, 9, 1.5, 2.25, '5', true, " +
				"'2026-03-03', '2026-03-03 10:20:30', '10.0.0.1', '00000000-0000-0000-0000-000000000001')",
			"INSERT INTO one (id) VALUES (1)",
		} {
			if _, err := db.Execute(ctx, st); err != nil {
				t.Fatal(err)
			}
		}
		return db
	}
	colType := map[string]parquet.TypeID{}
	for _, c := range cols {
		colType[c.Name] = c.Type
	}
	columnSource := map[string]bool{}
	for _, e := range []string{"i", "n", "f", "dec", "s", "b", "d", "ts", "ip", "u", "d + 1", "s || ''", "f * 2", "dec + 1"} {
		columnSource[e] = true
	}
	qualify := regexp.MustCompile(`\b(i|n|f|dec|s|b|d|ts|ip|u)\b`)
	doorStatements := func(e, tgt string) map[string][]string {
		if !columnSource[e] {
			return map[string][]string{
				"values":        {fmt.Sprintf("INSERT INTO t (id, %s) VALUES (1, %s)", tgt, e)},
				"insert_select": {fmt.Sprintf("INSERT INTO t (id, %s) SELECT 1, %s", tgt, e)},
				"update":        {"INSERT INTO t (id) VALUES (1)", fmt.Sprintf("UPDATE t SET %s = %s", tgt, e)},
				"merge": {"INSERT INTO t (id) VALUES (1)",
					fmt.Sprintf("MERGE INTO t USING one x ON t.id = x.id WHEN MATCHED THEN UPDATE SET %s = %s", tgt, e)},
				"merge_insert": {fmt.Sprintf("MERGE INTO t USING one x ON t.id = x.id WHEN NOT MATCHED THEN INSERT (id, %s) VALUES (1, %s)", tgt, e)},
			}
		}
		x := qualify.ReplaceAllString(e, "x.$1")
		return map[string][]string{
			"insert_select": {fmt.Sprintf("INSERT INTO t (id, %s) SELECT 1, %s FROM src", tgt, e)},
			"update":        {"INSERT INTO t SELECT * FROM src", fmt.Sprintf("UPDATE t SET %s = %s", tgt, e)},
			"merge": {"INSERT INTO t (id) VALUES (1)",
				fmt.Sprintf("MERGE INTO t USING src x ON t.id = x.id WHEN MATCHED THEN UPDATE SET %s = %s", tgt, x)},
			"merge_insert": {fmt.Sprintf("MERGE INTO t USING src x ON t.id = x.id WHEN NOT MATCHED THEN INSERT (id, %s) VALUES (1, %s)", tgt, x)},
		}
	}
	var mu sync.Mutex
	byCell := map[[2]string]string{}
	splits, pgDiffs, statements := 0, 0, 0
	// One database per target column, the columns in parallel: every cell
	// still runs every door against a fresh one-row table.
	t.Run("doors", func(t *testing.T) {
		for _, target := range cols[1:] {
			tgt := target.Name
			t.Run(tgt, func(t *testing.T) {
				t.Parallel()
				db := openDB(t)
				answer := func(stmts []string) string {
					if _, err := db.Execute(ctx, "DELETE FROM t"); err != nil {
						t.Fatal(err)
					}
					for _, st := range stmts {
						if _, err := db.Execute(ctx, st); err != nil {
							if s := sqlerr.StateOf(err); s != "" {
								return "err " + s
							}
							return "err NO-SQLSTATE: " + err.Error()
						}
					}
					res, err := db.Query(ctx, "SELECT "+tgt+" FROM t")
					if err != nil || len(res.Rows) != 1 {
						t.Fatalf("reading back %s: %v %v", tgt, err, res)
					}
					return "ok " + assignmentShown(res.Rows[0][tgt], colType[tgt])
				}
				for _, c := range assignmentDoorCells {
					if c.tgt != tgt {
						continue
					}
					per := map[string]string{}
					for door, stmts := range doorStatements(c.src, c.tgt) {
						per[door] = answer(stmts)
					}
					mu.Lock()
					statements += len(per)
					mu.Unlock()
					distinct := map[string]bool{}
					for _, v := range per {
						distinct[v] = true
					}
					if len(distinct) != 1 {
						var doors []string
						for d, v := range per {
							doors = append(doors, d+": "+v)
						}
						sort.Strings(doors)
						mu.Lock()
						splits++
						mu.Unlock()
						t.Errorf("DOOR SPLIT %s → %s (PostgreSQL %s %s):\n  %s", c.src, c.tgt, c.kind, c.want, strings.Join(doors, "\n  "))
						continue
					}
					got := per["insert_select"]
					mu.Lock()
					byCell[[2]string{c.src, c.tgt}] = got
					mu.Unlock()
					want := c.kind + " " + c.want
					reason, divergent := assignmentDoorDivergences[c.src+" → "+c.tgt]
					switch {
					case got != want && !divergent:
						mu.Lock()
						pgDiffs++
						mu.Unlock()
						t.Errorf("%s → %s: every door %q, PostgreSQL 17.11 %q", c.src, c.tgt, got, want)
					case got == want && divergent:
						t.Errorf("%s → %s now agrees with PostgreSQL (%q): delete its divergence entry (%s)", c.src, c.tgt, got, reason)
					}
				}
			})
		}
	})
	// CTAS: the declared type and value it stores, against PostgreSQL's, and
	// against every other door assigning the same constant into that type.
	db := openDB(t)
	ctasTargets := map[parquet.TypeID]string{}
	for _, c := range cols[1:] {
		ctasTargets[c.Type] = c.Name
	}
	for k, c := range assignmentCTASCells {
		statements++
		name := fmt.Sprintf("ctas_%d", k)
		var got, gotType string
		if _, err := db.Execute(ctx, "CREATE TABLE "+name+" AS SELECT "+c.src+" AS v"); err != nil {
			got, gotType = "err "+sqlerr.StateOf(err), sqlerr.StateOf(err)
		} else {
			meta, err := db.Catalog().GetTable(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			typ := meta.Schema.Columns[0].Type
			res, err := db.Query(ctx, "SELECT v FROM "+name)
			if err != nil || len(res.Rows) != 1 {
				t.Fatalf("reading CTAS %s for %s: %v", name, c.src, err)
			}
			got, gotType = "ok "+assignmentShown(res.Rows[0]["v"], typ), physical.PgTypeName(typ)
			ctasCol := meta.Schema.Columns[0]
			sameDecl := typ != parquet.TypeDecimal || (ctasCol.Precision == 10 && ctasCol.Scale == 2)
			if tgt, ok := ctasTargets[typ]; ok && sameDecl {
				if door, ok := byCell[[2]string{c.src, tgt}]; ok && door != got {
					splits++
					t.Errorf("DOOR SPLIT %s: CTAS stores %s %q, the other doors store %q into %s", c.src, gotType, got, door, tgt)
				}
			}
		}
		want, wantType := c.kind+" "+c.want, c.typ
		if c.kind == "err" {
			want = "err " + c.typ
		}
		reason, divergent := assignmentCTASDivergences[c.src]
		agrees := got == want && pgTypeSpelling(gotType) == pgTypeSpelling(wantType)
		switch {
		case !agrees && !divergent:
			pgDiffs++
			t.Errorf("CTAS %s: %s %q, PostgreSQL 17.11 %s %q", c.src, gotType, got, wantType, want)
		case agrees && divergent:
			t.Errorf("CTAS %s now agrees with PostgreSQL: delete its divergence entry (%s)", c.src, reason)
		}
	}
	t.Logf("%d cells, %d statements: %d door splits, %d unlisted PostgreSQL differences, %d listed divergences",
		len(assignmentDoorCells)+len(assignmentCTASCells), statements, splits, pgDiffs,
		len(assignmentDoorDivergences)+len(assignmentCTASDivergences))
}

// pgTypeSpelling folds a type name to the family both sides spell alike.
func pgTypeSpelling(s string) string {
	if strings.HasPrefix(s, "numeric") {
		return "numeric"
	}
	return s
}

// assignmentDoorDivergences are the cells where every door agrees and the
// answer is not PostgreSQL's, each with its mechanism. A cell that starts
// agreeing fails the gate until its entry is deleted.
var assignmentDoorDivergences = map[string]string{}

// assignmentCTASDivergences is the same list for the CTAS cells: the column
// TYPE a CTAS declares for a constant, not the assignment (every door stores
// the same value into a column of that type). Both are the SELECT list's
// declaration lane, recorded as filing candidates: a negated integer literal,
// integer arithmetic and CAST(… AS INTEGER) are typed bigint where
// PostgreSQL types integer (a width, the value is the same). A decimal
// literal was on this list (typed double precision, so `2.50` stored 2.5)
// until arc VL round 5 typed it numeric.
var assignmentCTASDivergences = map[string]string{
	"-5":                    "negated integer literal typed bigint",
	"1 + 1":                 "integer arithmetic typed bigint",
	"CAST(NULL AS INTEGER)": "CAST(… AS INTEGER) typed bigint",
}

type assignmentDoorCell struct{ src, tgt, kind, want string }

type assignmentCTASCell struct{ src, kind, typ, want string }

// Measured on PostgreSQL 17.11 (wadjet-pg-vl) by arc VL round 4's generator
// (the same statements on the five doors; no PostgreSQL door split): source,
// target column, ok|err, the value read back as text or the SQLSTATE.
var assignmentDoorCells = []assignmentDoorCell{
	{"5", "i", "ok", "5"},
	{"5", "n", "ok", "5"},
	{"5", "f", "ok", "5"},
	{"5", "dec", "ok", "5.00"},
	{"5", "s", "ok", "5"},
	{"5", "b", "err", "42804"},
	{"5", "d", "err", "42804"},
	{"5", "ts", "err", "42804"},
	{"5", "ip", "err", "42804"},
	{"5", "u", "err", "42804"},
	{"-5", "i", "ok", "-5"},
	{"-5", "n", "ok", "-5"},
	{"-5", "f", "ok", "-5"},
	{"-5", "dec", "ok", "-5.00"},
	{"-5", "s", "ok", "-5"},
	{"-5", "b", "err", "42804"},
	{"-5", "d", "err", "42804"},
	{"-5", "ts", "err", "42804"},
	{"-5", "ip", "err", "42804"},
	{"-5", "u", "err", "42804"},
	{"2.5", "i", "ok", "3"},
	{"2.5", "n", "ok", "3"},
	{"2.5", "f", "ok", "2.5"},
	{"2.5", "dec", "ok", "2.50"},
	{"2.5", "s", "ok", "2.5"},
	{"2.5", "b", "err", "42804"},
	{"2.5", "d", "err", "42804"},
	{"2.5", "ts", "err", "42804"},
	{"2.5", "ip", "err", "42804"},
	{"2.5", "u", "err", "42804"},
	{"2.50", "i", "ok", "3"},
	{"2.50", "n", "ok", "3"},
	{"2.50", "f", "ok", "2.5"},
	{"2.50", "dec", "ok", "2.50"},
	{"2.50", "s", "ok", "2.50"},
	{"2.50", "b", "err", "42804"},
	{"2.50", "d", "err", "42804"},
	{"2.50", "ts", "err", "42804"},
	{"2.50", "ip", "err", "42804"},
	{"2.50", "u", "err", "42804"},
	{"1.10", "i", "ok", "1"},
	{"1.10", "n", "ok", "1"},
	{"1.10", "f", "ok", "1.1"},
	{"1.10", "dec", "ok", "1.10"},
	{"1.10", "s", "ok", "1.10"},
	{"1.10", "b", "err", "42804"},
	{"1.10", "d", "err", "42804"},
	{"1.10", "ts", "err", "42804"},
	{"1.10", "ip", "err", "42804"},
	{"1.10", "u", "err", "42804"},
	{"-2.50", "i", "ok", "-3"},
	{"-2.50", "n", "ok", "-3"},
	{"-2.50", "f", "ok", "-2.5"},
	{"-2.50", "dec", "ok", "-2.50"},
	{"-2.50", "s", "ok", "-2.50"},
	{"-2.50", "b", "err", "42804"},
	{"-2.50", "d", "err", "42804"},
	{"-2.50", "ts", "err", "42804"},
	{"-2.50", "ip", "err", "42804"},
	{"-2.50", "u", "err", "42804"},
	{"0.5", "i", "ok", "1"},
	{"0.5", "n", "ok", "1"},
	{"0.5", "f", "ok", "0.5"},
	{"0.5", "dec", "ok", "0.50"},
	{"0.5", "s", "ok", "0.5"},
	{"0.5", "b", "err", "42804"},
	{"0.5", "d", "err", "42804"},
	{"0.5", "ts", "err", "42804"},
	{"0.5", "ip", "err", "42804"},
	{"0.5", "u", "err", "42804"},
	{"5000000000", "i", "err", "22003"},
	{"5000000000", "n", "ok", "5000000000"},
	{"5000000000", "f", "ok", "5000000000"},
	{"5000000000", "dec", "err", "22003"},
	{"5000000000", "s", "ok", "5000000000"},
	{"5000000000", "b", "err", "42804"},
	{"5000000000", "d", "err", "42804"},
	{"5000000000", "ts", "err", "42804"},
	{"5000000000", "ip", "err", "42804"},
	{"5000000000", "u", "err", "42804"},
	{"1e3", "i", "ok", "1000"},
	{"1e3", "n", "ok", "1000"},
	{"1e3", "f", "ok", "1000"},
	{"1e3", "dec", "ok", "1000.00"},
	{"1e3", "s", "ok", "1000"},
	{"1e3", "b", "err", "42804"},
	{"1e3", "d", "err", "42804"},
	{"1e3", "ts", "err", "42804"},
	{"1e3", "ip", "err", "42804"},
	{"1e3", "u", "err", "42804"},
	{"1.10 + 0", "i", "ok", "1"},
	{"1.10 + 0", "n", "ok", "1"},
	{"1.10 + 0", "f", "ok", "1.1"},
	{"1.10 + 0", "dec", "ok", "1.10"},
	{"1.10 + 0", "s", "ok", "1.10"},
	{"1.10 + 0", "b", "err", "42804"},
	{"1.10 + 0", "d", "err", "42804"},
	{"1.10 + 0", "ts", "err", "42804"},
	{"1.10 + 0", "ip", "err", "42804"},
	{"1.10 + 0", "u", "err", "42804"},
	{"2.5 + 0", "i", "ok", "3"},
	{"2.5 + 0", "n", "ok", "3"},
	{"2.5 + 0", "f", "ok", "2.5"},
	{"2.5 + 0", "dec", "ok", "2.50"},
	{"2.5 + 0", "s", "ok", "2.5"},
	{"2.5 + 0", "b", "err", "42804"},
	{"2.5 + 0", "d", "err", "42804"},
	{"2.5 + 0", "ts", "err", "42804"},
	{"2.5 + 0", "ip", "err", "42804"},
	{"2.5 + 0", "u", "err", "42804"},
	{"1 + 1", "i", "ok", "2"},
	{"1 + 1", "n", "ok", "2"},
	{"1 + 1", "f", "ok", "2"},
	{"1 + 1", "dec", "ok", "2.00"},
	{"1 + 1", "s", "ok", "2"},
	{"1 + 1", "b", "err", "42804"},
	{"1 + 1", "d", "err", "42804"},
	{"1 + 1", "ts", "err", "42804"},
	{"1 + 1", "ip", "err", "42804"},
	{"1 + 1", "u", "err", "42804"},
	{"CAST(2.50 AS NUMERIC(10,2))", "i", "ok", "3"},
	{"CAST(2.50 AS NUMERIC(10,2))", "n", "ok", "3"},
	{"CAST(2.50 AS NUMERIC(10,2))", "f", "ok", "2.5"},
	{"CAST(2.50 AS NUMERIC(10,2))", "dec", "ok", "2.50"},
	{"CAST(2.50 AS NUMERIC(10,2))", "s", "ok", "2.50"},
	{"CAST(2.50 AS NUMERIC(10,2))", "b", "err", "42804"},
	{"CAST(2.50 AS NUMERIC(10,2))", "d", "err", "42804"},
	{"CAST(2.50 AS NUMERIC(10,2))", "ts", "err", "42804"},
	{"CAST(2.50 AS NUMERIC(10,2))", "ip", "err", "42804"},
	{"CAST(2.50 AS NUMERIC(10,2))", "u", "err", "42804"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "i", "ok", "2"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "n", "ok", "2"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "f", "ok", "1.5"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "dec", "ok", "1.50"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "s", "ok", "1.5"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "b", "err", "42804"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "d", "err", "42804"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "ts", "err", "42804"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "ip", "err", "42804"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "u", "err", "42804"},
	{"CAST(5000000000 AS BIGINT)", "i", "err", "22003"},
	{"CAST(5000000000 AS BIGINT)", "n", "ok", "5000000000"},
	{"CAST(5000000000 AS BIGINT)", "f", "ok", "5000000000"},
	{"CAST(5000000000 AS BIGINT)", "dec", "err", "22003"},
	{"CAST(5000000000 AS BIGINT)", "s", "ok", "5000000000"},
	{"CAST(5000000000 AS BIGINT)", "b", "err", "42804"},
	{"CAST(5000000000 AS BIGINT)", "d", "err", "42804"},
	{"CAST(5000000000 AS BIGINT)", "ts", "err", "42804"},
	{"CAST(5000000000 AS BIGINT)", "ip", "err", "42804"},
	{"CAST(5000000000 AS BIGINT)", "u", "err", "42804"},
	{"'5'", "i", "ok", "5"},
	{"'5'", "n", "ok", "5"},
	{"'5'", "f", "ok", "5"},
	{"'5'", "dec", "ok", "5.00"},
	{"'5'", "s", "ok", "5"},
	{"'5'", "b", "err", "22P02"},
	{"'5'", "d", "err", "22007"},
	{"'5'", "ts", "err", "22007"},
	{"'5'", "ip", "err", "22P02"},
	{"'5'", "u", "err", "22P02"},
	{"'2.5'", "i", "err", "22P02"},
	{"'2.5'", "n", "err", "22P02"},
	{"'2.5'", "f", "ok", "2.5"},
	{"'2.5'", "dec", "ok", "2.50"},
	{"'2.5'", "s", "ok", "2.5"},
	{"'2.5'", "b", "err", "22P02"},
	{"'2.5'", "d", "err", "22007"},
	{"'2.5'", "ts", "err", "22007"},
	{"'2.5'", "ip", "err", "22P02"},
	{"'2.5'", "u", "err", "22P02"},
	{"'yes'", "i", "err", "22P02"},
	{"'yes'", "n", "err", "22P02"},
	{"'yes'", "f", "err", "22P02"},
	{"'yes'", "dec", "err", "22P02"},
	{"'yes'", "s", "ok", "yes"},
	{"'yes'", "b", "ok", "true"},
	{"'yes'", "d", "err", "22007"},
	{"'yes'", "ts", "err", "22007"},
	{"'yes'", "ip", "err", "22P02"},
	{"'yes'", "u", "err", "22P02"},
	{"'no'", "i", "err", "22P02"},
	{"'no'", "n", "err", "22P02"},
	{"'no'", "f", "err", "22P02"},
	{"'no'", "dec", "err", "22P02"},
	{"'no'", "s", "ok", "no"},
	{"'no'", "b", "ok", "false"},
	{"'no'", "d", "err", "22007"},
	{"'no'", "ts", "err", "22007"},
	{"'no'", "ip", "err", "22P02"},
	{"'no'", "u", "err", "22P02"},
	{"'off'", "i", "err", "22P02"},
	{"'off'", "n", "err", "22P02"},
	{"'off'", "f", "err", "22P02"},
	{"'off'", "dec", "err", "22P02"},
	{"'off'", "s", "ok", "off"},
	{"'off'", "b", "ok", "false"},
	{"'off'", "d", "err", "22007"},
	{"'off'", "ts", "err", "22007"},
	{"'off'", "ip", "err", "22P02"},
	{"'off'", "u", "err", "22P02"},
	{"'on'", "i", "err", "22P02"},
	{"'on'", "n", "err", "22P02"},
	{"'on'", "f", "err", "22P02"},
	{"'on'", "dec", "err", "22P02"},
	{"'on'", "s", "ok", "on"},
	{"'on'", "b", "ok", "true"},
	{"'on'", "d", "err", "22007"},
	{"'on'", "ts", "err", "22007"},
	{"'on'", "ip", "err", "22P02"},
	{"'on'", "u", "err", "22P02"},
	{"'t'", "i", "err", "22P02"},
	{"'t'", "n", "err", "22P02"},
	{"'t'", "f", "err", "22P02"},
	{"'t'", "dec", "err", "22P02"},
	{"'t'", "s", "ok", "t"},
	{"'t'", "b", "ok", "true"},
	{"'t'", "d", "err", "22007"},
	{"'t'", "ts", "err", "22007"},
	{"'t'", "ip", "err", "22P02"},
	{"'t'", "u", "err", "22P02"},
	{"'f'", "i", "err", "22P02"},
	{"'f'", "n", "err", "22P02"},
	{"'f'", "f", "err", "22P02"},
	{"'f'", "dec", "err", "22P02"},
	{"'f'", "s", "ok", "f"},
	{"'f'", "b", "ok", "false"},
	{"'f'", "d", "err", "22007"},
	{"'f'", "ts", "err", "22007"},
	{"'f'", "ip", "err", "22P02"},
	{"'f'", "u", "err", "22P02"},
	{"'TRUE'", "i", "err", "22P02"},
	{"'TRUE'", "n", "err", "22P02"},
	{"'TRUE'", "f", "err", "22P02"},
	{"'TRUE'", "dec", "err", "22P02"},
	{"'TRUE'", "s", "ok", "TRUE"},
	{"'TRUE'", "b", "ok", "true"},
	{"'TRUE'", "d", "err", "22007"},
	{"'TRUE'", "ts", "err", "22007"},
	{"'TRUE'", "ip", "err", "22P02"},
	{"'TRUE'", "u", "err", "22P02"},
	{"'x'", "i", "err", "22P02"},
	{"'x'", "n", "err", "22P02"},
	{"'x'", "f", "err", "22P02"},
	{"'x'", "dec", "err", "22P02"},
	{"'x'", "s", "ok", "x"},
	{"'x'", "b", "err", "22P02"},
	{"'x'", "d", "err", "22007"},
	{"'x'", "ts", "err", "22007"},
	{"'x'", "ip", "err", "22P02"},
	{"'x'", "u", "err", "22P02"},
	{"''", "i", "err", "22P02"},
	{"''", "n", "err", "22P02"},
	{"''", "f", "err", "22P02"},
	{"''", "dec", "err", "22P02"},
	{"''", "s", "ok", ""},
	{"''", "b", "err", "22P02"},
	{"''", "d", "err", "22007"},
	{"''", "ts", "err", "22007"},
	{"''", "ip", "err", "22P02"},
	{"''", "u", "err", "22P02"},
	{"'2026-03-03'", "i", "err", "22P02"},
	{"'2026-03-03'", "n", "err", "22P02"},
	{"'2026-03-03'", "f", "err", "22P02"},
	{"'2026-03-03'", "dec", "err", "22P02"},
	{"'2026-03-03'", "s", "ok", "2026-03-03"},
	{"'2026-03-03'", "b", "err", "22P02"},
	{"'2026-03-03'", "d", "ok", "2026-03-03"},
	{"'2026-03-03'", "ts", "ok", "2026-03-03 00:00:00"},
	{"'2026-03-03'", "ip", "err", "22P02"},
	{"'2026-03-03'", "u", "err", "22P02"},
	{"'2026-03-03 10:20:30'", "i", "err", "22P02"},
	{"'2026-03-03 10:20:30'", "n", "err", "22P02"},
	{"'2026-03-03 10:20:30'", "f", "err", "22P02"},
	{"'2026-03-03 10:20:30'", "dec", "err", "22P02"},
	{"'2026-03-03 10:20:30'", "s", "ok", "2026-03-03 10:20:30"},
	{"'2026-03-03 10:20:30'", "b", "err", "22P02"},
	{"'2026-03-03 10:20:30'", "d", "ok", "2026-03-03"},
	{"'2026-03-03 10:20:30'", "ts", "ok", "2026-03-03 10:20:30"},
	{"'2026-03-03 10:20:30'", "ip", "err", "22P02"},
	{"'2026-03-03 10:20:30'", "u", "err", "22P02"},
	{"'10.0.0.1'", "i", "err", "22P02"},
	{"'10.0.0.1'", "n", "err", "22P02"},
	{"'10.0.0.1'", "f", "err", "22P02"},
	{"'10.0.0.1'", "dec", "err", "22P02"},
	{"'10.0.0.1'", "s", "ok", "10.0.0.1"},
	{"'10.0.0.1'", "b", "err", "22P02"},
	{"'10.0.0.1'", "d", "err", "22007"},
	{"'10.0.0.1'", "ts", "err", "22007"},
	{"'10.0.0.1'", "ip", "ok", "10.0.0.1"},
	{"'10.0.0.1'", "u", "err", "22P02"},
	{"'00000000-0000-0000-0000-000000000001'", "i", "err", "22P02"},
	{"'00000000-0000-0000-0000-000000000001'", "n", "err", "22P02"},
	{"'00000000-0000-0000-0000-000000000001'", "f", "err", "22P02"},
	{"'00000000-0000-0000-0000-000000000001'", "dec", "err", "22P02"},
	{"'00000000-0000-0000-0000-000000000001'", "s", "ok", "00000000-0000-0000-0000-000000000001"},
	{"'00000000-0000-0000-0000-000000000001'", "b", "err", "22P02"},
	{"'00000000-0000-0000-0000-000000000001'", "d", "err", "22007"},
	{"'00000000-0000-0000-0000-000000000001'", "ts", "err", "22007"},
	{"'00000000-0000-0000-0000-000000000001'", "ip", "err", "22P02"},
	{"'00000000-0000-0000-0000-000000000001'", "u", "ok", "00000000-0000-0000-0000-000000000001"},
	{"TRUE", "i", "err", "42804"},
	{"TRUE", "n", "err", "42804"},
	{"TRUE", "f", "err", "42804"},
	{"TRUE", "dec", "err", "42804"},
	{"TRUE", "s", "ok", "true"},
	{"TRUE", "b", "ok", "true"},
	{"TRUE", "d", "err", "42804"},
	{"TRUE", "ts", "err", "42804"},
	{"TRUE", "ip", "err", "42804"},
	{"TRUE", "u", "err", "42804"},
	{"FALSE", "i", "err", "42804"},
	{"FALSE", "n", "err", "42804"},
	{"FALSE", "f", "err", "42804"},
	{"FALSE", "dec", "err", "42804"},
	{"FALSE", "s", "ok", "false"},
	{"FALSE", "b", "ok", "false"},
	{"FALSE", "d", "err", "42804"},
	{"FALSE", "ts", "err", "42804"},
	{"FALSE", "ip", "err", "42804"},
	{"FALSE", "u", "err", "42804"},
	{"(1 = 1)", "i", "err", "42804"},
	{"(1 = 1)", "n", "err", "42804"},
	{"(1 = 1)", "f", "err", "42804"},
	{"(1 = 1)", "dec", "err", "42804"},
	{"(1 = 1)", "s", "ok", "true"},
	{"(1 = 1)", "b", "ok", "true"},
	{"(1 = 1)", "d", "err", "42804"},
	{"(1 = 1)", "ts", "err", "42804"},
	{"(1 = 1)", "ip", "err", "42804"},
	{"(1 = 1)", "u", "err", "42804"},
	{"NULL", "i", "ok", "<nil>"},
	{"NULL", "n", "ok", "<nil>"},
	{"NULL", "f", "ok", "<nil>"},
	{"NULL", "dec", "ok", "<nil>"},
	{"NULL", "s", "ok", "<nil>"},
	{"NULL", "b", "ok", "<nil>"},
	{"NULL", "d", "ok", "<nil>"},
	{"NULL", "ts", "ok", "<nil>"},
	{"NULL", "ip", "ok", "<nil>"},
	{"NULL", "u", "ok", "<nil>"},
	{"CAST(NULL AS INTEGER)", "i", "ok", "<nil>"},
	{"CAST(NULL AS INTEGER)", "n", "ok", "<nil>"},
	{"CAST(NULL AS INTEGER)", "f", "ok", "<nil>"},
	{"CAST(NULL AS INTEGER)", "dec", "ok", "<nil>"},
	{"CAST(NULL AS INTEGER)", "s", "ok", "<nil>"},
	{"CAST(NULL AS INTEGER)", "b", "err", "42804"},
	{"CAST(NULL AS INTEGER)", "d", "err", "42804"},
	{"CAST(NULL AS INTEGER)", "ts", "err", "42804"},
	{"CAST(NULL AS INTEGER)", "ip", "err", "42804"},
	{"CAST(NULL AS INTEGER)", "u", "err", "42804"},
	{"DATE '2026-03-03'", "i", "err", "42804"},
	{"DATE '2026-03-03'", "n", "err", "42804"},
	{"DATE '2026-03-03'", "f", "err", "42804"},
	{"DATE '2026-03-03'", "dec", "err", "42804"},
	{"DATE '2026-03-03'", "s", "ok", "2026-03-03"},
	{"DATE '2026-03-03'", "b", "err", "42804"},
	{"DATE '2026-03-03'", "d", "ok", "2026-03-03"},
	{"DATE '2026-03-03'", "ts", "ok", "2026-03-03 00:00:00"},
	{"DATE '2026-03-03'", "ip", "err", "42804"},
	{"DATE '2026-03-03'", "u", "err", "42804"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "i", "err", "42804"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "n", "err", "42804"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "f", "err", "42804"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "dec", "err", "42804"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "s", "ok", "2026-03-03 10:20:30"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "b", "err", "42804"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "d", "ok", "2026-03-03"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "ts", "ok", "2026-03-03 10:20:30"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "ip", "err", "42804"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "u", "err", "42804"},
	{"DATE '2026-03-03' + 1", "i", "err", "42804"},
	{"DATE '2026-03-03' + 1", "n", "err", "42804"},
	{"DATE '2026-03-03' + 1", "f", "err", "42804"},
	{"DATE '2026-03-03' + 1", "dec", "err", "42804"},
	{"DATE '2026-03-03' + 1", "s", "ok", "2026-03-04"},
	{"DATE '2026-03-03' + 1", "b", "err", "42804"},
	{"DATE '2026-03-03' + 1", "d", "ok", "2026-03-04"},
	{"DATE '2026-03-03' + 1", "ts", "ok", "2026-03-04 00:00:00"},
	{"DATE '2026-03-03' + 1", "ip", "err", "42804"},
	{"DATE '2026-03-03' + 1", "u", "err", "42804"},
	{"CAST('2026-03-03' AS DATE)", "i", "err", "42804"},
	{"CAST('2026-03-03' AS DATE)", "n", "err", "42804"},
	{"CAST('2026-03-03' AS DATE)", "f", "err", "42804"},
	{"CAST('2026-03-03' AS DATE)", "dec", "err", "42804"},
	{"CAST('2026-03-03' AS DATE)", "s", "ok", "2026-03-03"},
	{"CAST('2026-03-03' AS DATE)", "b", "err", "42804"},
	{"CAST('2026-03-03' AS DATE)", "d", "ok", "2026-03-03"},
	{"CAST('2026-03-03' AS DATE)", "ts", "ok", "2026-03-03 00:00:00"},
	{"CAST('2026-03-03' AS DATE)", "ip", "err", "42804"},
	{"CAST('2026-03-03' AS DATE)", "u", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "i", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "n", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "f", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "dec", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "s", "ok", "00000000-0000-0000-0000-000000000002"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "b", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "d", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "ts", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "ip", "err", "42804"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "u", "ok", "00000000-0000-0000-0000-000000000002"},
	{"'5' || ''", "i", "err", "42804"},
	{"'5' || ''", "n", "err", "42804"},
	{"'5' || ''", "f", "err", "42804"},
	{"'5' || ''", "dec", "err", "42804"},
	{"'5' || ''", "s", "ok", "5"},
	{"'5' || ''", "b", "err", "42804"},
	{"'5' || ''", "d", "err", "42804"},
	{"'5' || ''", "ts", "err", "42804"},
	{"'5' || ''", "ip", "err", "42804"},
	{"'5' || ''", "u", "err", "42804"},
	{"upper('x')", "i", "err", "42804"},
	{"upper('x')", "n", "err", "42804"},
	{"upper('x')", "f", "err", "42804"},
	{"upper('x')", "dec", "err", "42804"},
	{"upper('x')", "s", "ok", "X"},
	{"upper('x')", "b", "err", "42804"},
	{"upper('x')", "d", "err", "42804"},
	{"upper('x')", "ts", "err", "42804"},
	{"upper('x')", "ip", "err", "42804"},
	{"upper('x')", "u", "err", "42804"},
	{"i", "i", "ok", "7"},
	{"i", "n", "ok", "7"},
	{"i", "f", "ok", "7"},
	{"i", "dec", "ok", "7.00"},
	{"i", "s", "ok", "7"},
	{"i", "b", "err", "42804"},
	{"i", "d", "err", "42804"},
	{"i", "ts", "err", "42804"},
	{"i", "ip", "err", "42804"},
	{"i", "u", "err", "42804"},
	{"n", "i", "ok", "9"},
	{"n", "n", "ok", "9"},
	{"n", "f", "ok", "9"},
	{"n", "dec", "ok", "9.00"},
	{"n", "s", "ok", "9"},
	{"n", "b", "err", "42804"},
	{"n", "d", "err", "42804"},
	{"n", "ts", "err", "42804"},
	{"n", "ip", "err", "42804"},
	{"n", "u", "err", "42804"},
	{"f", "i", "ok", "2"},
	{"f", "n", "ok", "2"},
	{"f", "f", "ok", "1.5"},
	{"f", "dec", "ok", "1.50"},
	{"f", "s", "ok", "1.5"},
	{"f", "b", "err", "42804"},
	{"f", "d", "err", "42804"},
	{"f", "ts", "err", "42804"},
	{"f", "ip", "err", "42804"},
	{"f", "u", "err", "42804"},
	{"dec", "i", "ok", "2"},
	{"dec", "n", "ok", "2"},
	{"dec", "f", "ok", "2.25"},
	{"dec", "dec", "ok", "2.25"},
	{"dec", "s", "ok", "2.25"},
	{"dec", "b", "err", "42804"},
	{"dec", "d", "err", "42804"},
	{"dec", "ts", "err", "42804"},
	{"dec", "ip", "err", "42804"},
	{"dec", "u", "err", "42804"},
	{"s", "i", "err", "42804"},
	{"s", "n", "err", "42804"},
	{"s", "f", "err", "42804"},
	{"s", "dec", "err", "42804"},
	{"s", "s", "ok", "5"},
	{"s", "b", "err", "42804"},
	{"s", "d", "err", "42804"},
	{"s", "ts", "err", "42804"},
	{"s", "ip", "err", "42804"},
	{"s", "u", "err", "42804"},
	{"b", "i", "err", "42804"},
	{"b", "n", "err", "42804"},
	{"b", "f", "err", "42804"},
	{"b", "dec", "err", "42804"},
	{"b", "s", "ok", "true"},
	{"b", "b", "ok", "true"},
	{"b", "d", "err", "42804"},
	{"b", "ts", "err", "42804"},
	{"b", "ip", "err", "42804"},
	{"b", "u", "err", "42804"},
	{"d", "i", "err", "42804"},
	{"d", "n", "err", "42804"},
	{"d", "f", "err", "42804"},
	{"d", "dec", "err", "42804"},
	{"d", "s", "ok", "2026-03-03"},
	{"d", "b", "err", "42804"},
	{"d", "d", "ok", "2026-03-03"},
	{"d", "ts", "ok", "2026-03-03 00:00:00"},
	{"d", "ip", "err", "42804"},
	{"d", "u", "err", "42804"},
	{"ts", "i", "err", "42804"},
	{"ts", "n", "err", "42804"},
	{"ts", "f", "err", "42804"},
	{"ts", "dec", "err", "42804"},
	{"ts", "s", "ok", "2026-03-03 10:20:30"},
	{"ts", "b", "err", "42804"},
	{"ts", "d", "ok", "2026-03-03"},
	{"ts", "ts", "ok", "2026-03-03 10:20:30"},
	{"ts", "ip", "err", "42804"},
	{"ts", "u", "err", "42804"},
	{"ip", "i", "err", "42804"},
	{"ip", "n", "err", "42804"},
	{"ip", "f", "err", "42804"},
	{"ip", "dec", "err", "42804"},
	{"ip", "s", "ok", "10.0.0.1/32"},
	{"ip", "b", "err", "42804"},
	{"ip", "d", "err", "42804"},
	{"ip", "ts", "err", "42804"},
	{"ip", "ip", "ok", "10.0.0.1"},
	{"ip", "u", "err", "42804"},
	{"u", "i", "err", "42804"},
	{"u", "n", "err", "42804"},
	{"u", "f", "err", "42804"},
	{"u", "dec", "err", "42804"},
	{"u", "s", "ok", "00000000-0000-0000-0000-000000000001"},
	{"u", "b", "err", "42804"},
	{"u", "d", "err", "42804"},
	{"u", "ts", "err", "42804"},
	{"u", "ip", "err", "42804"},
	{"u", "u", "ok", "00000000-0000-0000-0000-000000000001"},
	{"d + 1", "i", "err", "42804"},
	{"d + 1", "n", "err", "42804"},
	{"d + 1", "f", "err", "42804"},
	{"d + 1", "dec", "err", "42804"},
	{"d + 1", "s", "ok", "2026-03-04"},
	{"d + 1", "b", "err", "42804"},
	{"d + 1", "d", "ok", "2026-03-04"},
	{"d + 1", "ts", "ok", "2026-03-04 00:00:00"},
	{"d + 1", "ip", "err", "42804"},
	{"d + 1", "u", "err", "42804"},
	{"s || ''", "i", "err", "42804"},
	{"s || ''", "n", "err", "42804"},
	{"s || ''", "f", "err", "42804"},
	{"s || ''", "dec", "err", "42804"},
	{"s || ''", "s", "ok", "5"},
	{"s || ''", "b", "err", "42804"},
	{"s || ''", "d", "err", "42804"},
	{"s || ''", "ts", "err", "42804"},
	{"s || ''", "ip", "err", "42804"},
	{"s || ''", "u", "err", "42804"},
	{"f * 2", "i", "ok", "3"},
	{"f * 2", "n", "ok", "3"},
	{"f * 2", "f", "ok", "3"},
	{"f * 2", "dec", "ok", "3.00"},
	{"f * 2", "s", "ok", "3"},
	{"f * 2", "b", "err", "42804"},
	{"f * 2", "d", "err", "42804"},
	{"f * 2", "ts", "err", "42804"},
	{"f * 2", "ip", "err", "42804"},
	{"f * 2", "u", "err", "42804"},
	{"dec + 1", "i", "ok", "3"},
	{"dec + 1", "n", "ok", "3"},
	{"dec + 1", "f", "ok", "3.25"},
	{"dec + 1", "dec", "ok", "3.25"},
	{"dec + 1", "s", "ok", "3.25"},
	{"dec + 1", "b", "err", "42804"},
	{"dec + 1", "d", "err", "42804"},
	{"dec + 1", "ts", "err", "42804"},
	{"dec + 1", "ip", "err", "42804"},
	{"dec + 1", "u", "err", "42804"},
}

// CTAS on PostgreSQL 17.11: source, ok|err, format_type of the created column
// (the SQLSTATE for err), the value as text.
var assignmentCTASCells = []assignmentCTASCell{
	{"5", "ok", "integer", "5"},
	{"-5", "ok", "integer", "-5"},
	{"2.5", "ok", "numeric", "2.5"},
	{"2.50", "ok", "numeric", "2.50"},
	{"1.10", "ok", "numeric", "1.10"},
	{"-2.50", "ok", "numeric", "-2.50"},
	{"0.5", "ok", "numeric", "0.5"},
	{"5000000000", "ok", "bigint", "5000000000"},
	{"1e3", "ok", "numeric", "1000"},
	{"1.10 + 0", "ok", "numeric", "1.10"},
	{"2.5 + 0", "ok", "numeric", "2.5"},
	{"1 + 1", "ok", "integer", "2"},
	{"CAST(2.50 AS NUMERIC(10,2))", "ok", "numeric(10,2)", "2.50"},
	{"CAST(1.5 AS DOUBLE PRECISION)", "ok", "double precision", "1.5"},
	{"CAST(5000000000 AS BIGINT)", "ok", "bigint", "5000000000"},
	{"'5'", "ok", "text", "5"},
	{"'2.5'", "ok", "text", "2.5"},
	{"'yes'", "ok", "text", "yes"},
	{"'no'", "ok", "text", "no"},
	{"'off'", "ok", "text", "off"},
	{"'on'", "ok", "text", "on"},
	{"'t'", "ok", "text", "t"},
	{"'f'", "ok", "text", "f"},
	{"'TRUE'", "ok", "text", "TRUE"},
	{"'x'", "ok", "text", "x"},
	{"''", "ok", "text", ""},
	{"'2026-03-03'", "ok", "text", "2026-03-03"},
	{"'2026-03-03 10:20:30'", "ok", "text", "2026-03-03 10:20:30"},
	{"'10.0.0.1'", "ok", "text", "10.0.0.1"},
	{"'00000000-0000-0000-0000-000000000001'", "ok", "text", "00000000-0000-0000-0000-000000000001"},
	{"TRUE", "ok", "boolean", "true"},
	{"FALSE", "ok", "boolean", "false"},
	{"(1 = 1)", "ok", "boolean", "true"},
	{"NULL", "ok", "text", "<nil>"},
	{"CAST(NULL AS INTEGER)", "ok", "integer", "<nil>"},
	{"DATE '2026-03-03'", "ok", "date", "2026-03-03"},
	{"TIMESTAMP '2026-03-03 10:20:30'", "ok", "timestamp without time zone", "2026-03-03 10:20:30"},
	{"DATE '2026-03-03' + 1", "ok", "date", "2026-03-04"},
	{"CAST('2026-03-03' AS DATE)", "ok", "date", "2026-03-03"},
	{"CAST('00000000-0000-0000-0000-000000000002' AS UUID)", "ok", "uuid", "00000000-0000-0000-0000-000000000002"},
	{"'5' || ''", "ok", "text", "5"},
	{"upper('x')", "ok", "text", "X"},
}
