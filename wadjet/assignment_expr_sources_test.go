// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestAssignmentExpressionSourcesAgreeWithPostgreSQL is the SOURCE axis the
// door-diff gate (TestAssignmentDoorsAgree) lacked (round-4 review B2): a
// numeric constant INSIDE an expression — CASE, COALESCE, GREATEST, NULLIF,
// arithmetic — and reached THROUGH a construct — a CTE, a derived table, a
// VALUES list, MERGE's USING (SELECT …) — assigned to INTEGER, BIGINT,
// DOUBLE, NUMERIC(10,2) and TEXT through every write door. Each cell must
// store the same value on all nine doors AND that value must be PostgreSQL
// 17.11's: a gate that compares the doors only cannot see every door being
// equally wrong, which is what round 4 shipped (`CASE WHEN true THEN 2.50
// END` stored `2.5` into TEXT and 2 into INTEGER on every door — a decimal
// constant was declared double precision one expression deeper than the bare
// constant the assignment function read by its spelling). A decimal literal
// now declares its spelling's numeric wherever it sits (arc VL round 5).
//
// Cells measured on PostgreSQL 17.11 (wadjet-pg-vl) with these statements on
// all nine doors; PostgreSQL itself splits on none.
func TestAssignmentExpressionSourcesAgreeWithPostgreSQL(t *testing.T) {
	ctx := context.Background()
	cols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "i", Type: parquet.TypeInt32, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "dec", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}
	colType := map[string]parquet.TypeID{}
	for _, c := range cols {
		colType[c.Name] = c.Type
	}
	doors := map[string][]string{
		"values":             {"INSERT INTO t (id, {T}) VALUES (1, {E})"},
		"insert_select":      {"INSERT INTO t (id, {T}) SELECT 1, {E}"},
		"insert_cte":         {"INSERT INTO t (id, {T}) WITH c AS (SELECT {E} AS x) SELECT 1, x FROM c"},
		"insert_derived":     {"INSERT INTO t (id, {T}) SELECT 1, x FROM (SELECT {E} AS x) q"},
		"insert_values_list": {"INSERT INTO t (id, {T}) SELECT * FROM (VALUES (1, {E})) v"},
		"update":             {"INSERT INTO t (id) VALUES (1)", "UPDATE t SET {T} = {E}"},
		"merge_set": {"INSERT INTO t (id) VALUES (1)",
			"MERGE INTO t USING one x ON t.id = x.id WHEN MATCHED THEN UPDATE SET {T} = {E}"},
		"merge_using_select": {"INSERT INTO t (id) VALUES (1)",
			"MERGE INTO t USING (SELECT 1 AS id, {E} AS x) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET {T} = s.x"},
		"merge_insert": {"MERGE INTO t USING one x ON t.id = x.id WHEN NOT MATCHED THEN INSERT (id, {T}) VALUES (1, {E})"},
	}
	openDB := func(t *testing.T) *DB {
		db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		if err := db.CreateTable(ctx, "t", parquet.Schema{Columns: cols}, nil); err != nil {
			t.Fatal(err)
		}
		if err := db.CreateTable(ctx, "one", parquet.Schema{Columns: cols[:1]}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := db.Execute(ctx, "INSERT INTO one (id) VALUES (1)"); err != nil {
			t.Fatal(err)
		}
		return db
	}
	// One fresh database per (source, target) cell — a table rewritten
	// thousands of times over grows the history every DELETE reads — with
	// the targets in parallel.
	answer := func(t *testing.T, db *DB, stmts []string, src, tgt string) string {
		if _, err := db.Execute(ctx, "DELETE FROM t"); err != nil {
			t.Fatal(err)
		}
		for _, st := range stmts {
			st = strings.NewReplacer("{T}", tgt, "{E}", src).Replace(st)
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
	var mu sync.Mutex
	splits, pgDiffs, statements := 0, 0, 0
	t.Run("targets", func(t *testing.T) {
		for _, target := range cols[1:] {
			tgt := target.Name
			t.Run(tgt, func(t *testing.T) {
				t.Parallel()
				for _, c := range assignmentExprSourceCells {
					if c.tgt != tgt {
						continue
					}
					db := openDB(t)
					per := map[string]string{}
					for door, stmts := range doors {
						per[door] = answer(t, db, stmts, c.src, c.tgt)
					}
					distinct := map[string]bool{}
					for _, v := range per {
						distinct[v] = true
					}
					mu.Lock()
					statements += len(per)
					mu.Unlock()
					if len(distinct) > 1 {
						var lines []string
						for d, v := range per {
							lines = append(lines, d+": "+v)
						}
						sort.Strings(lines)
						mu.Lock()
						splits++
						mu.Unlock()
						t.Errorf("DOOR SPLIT %s → %s (PostgreSQL 17.11 %q on every door):\n  %s",
							c.src, c.tgt, c.want, strings.Join(lines, "\n  "))
						continue
					}
					got := per["values"]
					reason, divergent := assignmentExprSourceDivergences[c.src+" → "+c.tgt]
					switch {
					case got != c.want && !divergent:
						mu.Lock()
						pgDiffs++
						mu.Unlock()
						t.Errorf("%s → %s: every door %q, PostgreSQL 17.11 %q", c.src, c.tgt, got, c.want)
					case got == c.want && divergent:
						t.Errorf("%s → %s now agrees with PostgreSQL (%q): delete its divergence entry (%s)",
							c.src, c.tgt, got, reason)
					}
				}
			})
		}
	})
	t.Logf("%d cells × %d doors = %d statements: %d door splits, %d unlisted PostgreSQL differences, %d listed",
		len(assignmentExprSourceCells), len(doors), statements, splits, pgDiffs, len(assignmentExprSourceDivergences))
}

// assignmentExprSourceDivergences are the cells where every door agrees and
// the answer is not PostgreSQL's, each with its mechanism; a cell that starts
// agreeing fails the gate until its entry is deleted. All three are ADR-0024's
// recorded #764 class: PostgreSQL gives a numeric constant typmod -1, so a
// choice over constants of different scales prints EACH value at its own
// scale, while a wadjet DECIMAL column has one scale — the same number with
// trailing zeros.
var assignmentExprSourceDivergences = map[string]string{
	"GREATEST(-2.50, 0.1) → s": "#764: one scale per column (0.10 for 0.1)",
	"GREATEST(1e3, 0.1) → s":   "#764: one scale per column (1000.0 for 1000)",
	"GREATEST(5, 0.1) → s":     "#764: one scale per column (5.0 for 5)",
}

type assignmentExprSourceCell struct{ src, tgt, want string }

// Measured on PostgreSQL 17.11 (wadjet-pg-vl) by arc VL round 5's generator.
var assignmentExprSourceCells = []assignmentExprSourceCell{
	{"2.50", "i", "ok 3"},
	{"2.50", "n", "ok 3"},
	{"2.50", "f", "ok 2.5"},
	{"2.50", "dec", "ok 2.50"},
	{"2.50", "s", "ok 2.50"},
	{"CASE WHEN true THEN 2.50 END", "i", "ok 3"},
	{"CASE WHEN true THEN 2.50 END", "n", "ok 3"},
	{"CASE WHEN true THEN 2.50 END", "f", "ok 2.5"},
	{"CASE WHEN true THEN 2.50 END", "dec", "ok 2.50"},
	{"CASE WHEN true THEN 2.50 END", "s", "ok 2.50"},
	{"COALESCE(2.50, 1)", "i", "ok 3"},
	{"COALESCE(2.50, 1)", "n", "ok 3"},
	{"COALESCE(2.50, 1)", "f", "ok 2.5"},
	{"COALESCE(2.50, 1)", "dec", "ok 2.50"},
	{"COALESCE(2.50, 1)", "s", "ok 2.50"},
	{"CASE WHEN false THEN 1 ELSE 2.50 END", "i", "ok 3"},
	{"CASE WHEN false THEN 1 ELSE 2.50 END", "n", "ok 3"},
	{"CASE WHEN false THEN 1 ELSE 2.50 END", "f", "ok 2.5"},
	{"CASE WHEN false THEN 1 ELSE 2.50 END", "dec", "ok 2.50"},
	{"CASE WHEN false THEN 1 ELSE 2.50 END", "s", "ok 2.50"},
	{"GREATEST(2.50, 0.1)", "i", "ok 3"},
	{"GREATEST(2.50, 0.1)", "n", "ok 3"},
	{"GREATEST(2.50, 0.1)", "f", "ok 2.5"},
	{"GREATEST(2.50, 0.1)", "dec", "ok 2.50"},
	{"GREATEST(2.50, 0.1)", "s", "ok 2.50"},
	{"(2.50 + 0)", "i", "ok 3"},
	{"(2.50 + 0)", "n", "ok 3"},
	{"(2.50 + 0)", "f", "ok 2.5"},
	{"(2.50 + 0)", "dec", "ok 2.50"},
	{"(2.50 + 0)", "s", "ok 2.50"},
	{"NULLIF(2.50, 0)", "i", "ok 3"},
	{"NULLIF(2.50, 0)", "n", "ok 3"},
	{"NULLIF(2.50, 0)", "f", "ok 2.5"},
	{"NULLIF(2.50, 0)", "dec", "ok 2.50"},
	{"NULLIF(2.50, 0)", "s", "ok 2.50"},
	{"2.5", "i", "ok 3"},
	{"2.5", "n", "ok 3"},
	{"2.5", "f", "ok 2.5"},
	{"2.5", "dec", "ok 2.50"},
	{"2.5", "s", "ok 2.5"},
	{"CASE WHEN true THEN 2.5 END", "i", "ok 3"},
	{"CASE WHEN true THEN 2.5 END", "n", "ok 3"},
	{"CASE WHEN true THEN 2.5 END", "f", "ok 2.5"},
	{"CASE WHEN true THEN 2.5 END", "dec", "ok 2.50"},
	{"CASE WHEN true THEN 2.5 END", "s", "ok 2.5"},
	{"COALESCE(2.5, 1)", "i", "ok 3"},
	{"COALESCE(2.5, 1)", "n", "ok 3"},
	{"COALESCE(2.5, 1)", "f", "ok 2.5"},
	{"COALESCE(2.5, 1)", "dec", "ok 2.50"},
	{"COALESCE(2.5, 1)", "s", "ok 2.5"},
	{"CASE WHEN false THEN 1 ELSE 2.5 END", "i", "ok 3"},
	{"CASE WHEN false THEN 1 ELSE 2.5 END", "n", "ok 3"},
	{"CASE WHEN false THEN 1 ELSE 2.5 END", "f", "ok 2.5"},
	{"CASE WHEN false THEN 1 ELSE 2.5 END", "dec", "ok 2.50"},
	{"CASE WHEN false THEN 1 ELSE 2.5 END", "s", "ok 2.5"},
	{"GREATEST(2.5, 0.1)", "i", "ok 3"},
	{"GREATEST(2.5, 0.1)", "n", "ok 3"},
	{"GREATEST(2.5, 0.1)", "f", "ok 2.5"},
	{"GREATEST(2.5, 0.1)", "dec", "ok 2.50"},
	{"GREATEST(2.5, 0.1)", "s", "ok 2.5"},
	{"(2.5 + 0)", "i", "ok 3"},
	{"(2.5 + 0)", "n", "ok 3"},
	{"(2.5 + 0)", "f", "ok 2.5"},
	{"(2.5 + 0)", "dec", "ok 2.50"},
	{"(2.5 + 0)", "s", "ok 2.5"},
	{"NULLIF(2.5, 0)", "i", "ok 3"},
	{"NULLIF(2.5, 0)", "n", "ok 3"},
	{"NULLIF(2.5, 0)", "f", "ok 2.5"},
	{"NULLIF(2.5, 0)", "dec", "ok 2.50"},
	{"NULLIF(2.5, 0)", "s", "ok 2.5"},
	{"-2.50", "i", "ok -3"},
	{"-2.50", "n", "ok -3"},
	{"-2.50", "f", "ok -2.5"},
	{"-2.50", "dec", "ok -2.50"},
	{"-2.50", "s", "ok -2.50"},
	{"CASE WHEN true THEN -2.50 END", "i", "ok -3"},
	{"CASE WHEN true THEN -2.50 END", "n", "ok -3"},
	{"CASE WHEN true THEN -2.50 END", "f", "ok -2.5"},
	{"CASE WHEN true THEN -2.50 END", "dec", "ok -2.50"},
	{"CASE WHEN true THEN -2.50 END", "s", "ok -2.50"},
	{"COALESCE(-2.50, 1)", "i", "ok -3"},
	{"COALESCE(-2.50, 1)", "n", "ok -3"},
	{"COALESCE(-2.50, 1)", "f", "ok -2.5"},
	{"COALESCE(-2.50, 1)", "dec", "ok -2.50"},
	{"COALESCE(-2.50, 1)", "s", "ok -2.50"},
	{"CASE WHEN false THEN 1 ELSE -2.50 END", "i", "ok -3"},
	{"CASE WHEN false THEN 1 ELSE -2.50 END", "n", "ok -3"},
	{"CASE WHEN false THEN 1 ELSE -2.50 END", "f", "ok -2.5"},
	{"CASE WHEN false THEN 1 ELSE -2.50 END", "dec", "ok -2.50"},
	{"CASE WHEN false THEN 1 ELSE -2.50 END", "s", "ok -2.50"},
	{"GREATEST(-2.50, 0.1)", "i", "ok 0"},
	{"GREATEST(-2.50, 0.1)", "n", "ok 0"},
	{"GREATEST(-2.50, 0.1)", "f", "ok 0.1"},
	{"GREATEST(-2.50, 0.1)", "dec", "ok 0.10"},
	{"GREATEST(-2.50, 0.1)", "s", "ok 0.1"},
	{"(-2.50 + 0)", "i", "ok -3"},
	{"(-2.50 + 0)", "n", "ok -3"},
	{"(-2.50 + 0)", "f", "ok -2.5"},
	{"(-2.50 + 0)", "dec", "ok -2.50"},
	{"(-2.50 + 0)", "s", "ok -2.50"},
	{"NULLIF(-2.50, 0)", "i", "ok -3"},
	{"NULLIF(-2.50, 0)", "n", "ok -3"},
	{"NULLIF(-2.50, 0)", "f", "ok -2.5"},
	{"NULLIF(-2.50, 0)", "dec", "ok -2.50"},
	{"NULLIF(-2.50, 0)", "s", "ok -2.50"},
	{"1.10", "i", "ok 1"},
	{"1.10", "n", "ok 1"},
	{"1.10", "f", "ok 1.1"},
	{"1.10", "dec", "ok 1.10"},
	{"1.10", "s", "ok 1.10"},
	{"CASE WHEN true THEN 1.10 END", "i", "ok 1"},
	{"CASE WHEN true THEN 1.10 END", "n", "ok 1"},
	{"CASE WHEN true THEN 1.10 END", "f", "ok 1.1"},
	{"CASE WHEN true THEN 1.10 END", "dec", "ok 1.10"},
	{"CASE WHEN true THEN 1.10 END", "s", "ok 1.10"},
	{"COALESCE(1.10, 1)", "i", "ok 1"},
	{"COALESCE(1.10, 1)", "n", "ok 1"},
	{"COALESCE(1.10, 1)", "f", "ok 1.1"},
	{"COALESCE(1.10, 1)", "dec", "ok 1.10"},
	{"COALESCE(1.10, 1)", "s", "ok 1.10"},
	{"CASE WHEN false THEN 1 ELSE 1.10 END", "i", "ok 1"},
	{"CASE WHEN false THEN 1 ELSE 1.10 END", "n", "ok 1"},
	{"CASE WHEN false THEN 1 ELSE 1.10 END", "f", "ok 1.1"},
	{"CASE WHEN false THEN 1 ELSE 1.10 END", "dec", "ok 1.10"},
	{"CASE WHEN false THEN 1 ELSE 1.10 END", "s", "ok 1.10"},
	{"GREATEST(1.10, 0.1)", "i", "ok 1"},
	{"GREATEST(1.10, 0.1)", "n", "ok 1"},
	{"GREATEST(1.10, 0.1)", "f", "ok 1.1"},
	{"GREATEST(1.10, 0.1)", "dec", "ok 1.10"},
	{"GREATEST(1.10, 0.1)", "s", "ok 1.10"},
	{"(1.10 + 0)", "i", "ok 1"},
	{"(1.10 + 0)", "n", "ok 1"},
	{"(1.10 + 0)", "f", "ok 1.1"},
	{"(1.10 + 0)", "dec", "ok 1.10"},
	{"(1.10 + 0)", "s", "ok 1.10"},
	{"NULLIF(1.10, 0)", "i", "ok 1"},
	{"NULLIF(1.10, 0)", "n", "ok 1"},
	{"NULLIF(1.10, 0)", "f", "ok 1.1"},
	{"NULLIF(1.10, 0)", "dec", "ok 1.10"},
	{"NULLIF(1.10, 0)", "s", "ok 1.10"},
	{"0.5", "i", "ok 1"},
	{"0.5", "n", "ok 1"},
	{"0.5", "f", "ok 0.5"},
	{"0.5", "dec", "ok 0.50"},
	{"0.5", "s", "ok 0.5"},
	{"CASE WHEN true THEN 0.5 END", "i", "ok 1"},
	{"CASE WHEN true THEN 0.5 END", "n", "ok 1"},
	{"CASE WHEN true THEN 0.5 END", "f", "ok 0.5"},
	{"CASE WHEN true THEN 0.5 END", "dec", "ok 0.50"},
	{"CASE WHEN true THEN 0.5 END", "s", "ok 0.5"},
	{"COALESCE(0.5, 1)", "i", "ok 1"},
	{"COALESCE(0.5, 1)", "n", "ok 1"},
	{"COALESCE(0.5, 1)", "f", "ok 0.5"},
	{"COALESCE(0.5, 1)", "dec", "ok 0.50"},
	{"COALESCE(0.5, 1)", "s", "ok 0.5"},
	{"CASE WHEN false THEN 1 ELSE 0.5 END", "i", "ok 1"},
	{"CASE WHEN false THEN 1 ELSE 0.5 END", "n", "ok 1"},
	{"CASE WHEN false THEN 1 ELSE 0.5 END", "f", "ok 0.5"},
	{"CASE WHEN false THEN 1 ELSE 0.5 END", "dec", "ok 0.50"},
	{"CASE WHEN false THEN 1 ELSE 0.5 END", "s", "ok 0.5"},
	{"GREATEST(0.5, 0.1)", "i", "ok 1"},
	{"GREATEST(0.5, 0.1)", "n", "ok 1"},
	{"GREATEST(0.5, 0.1)", "f", "ok 0.5"},
	{"GREATEST(0.5, 0.1)", "dec", "ok 0.50"},
	{"GREATEST(0.5, 0.1)", "s", "ok 0.5"},
	{"(0.5 + 0)", "i", "ok 1"},
	{"(0.5 + 0)", "n", "ok 1"},
	{"(0.5 + 0)", "f", "ok 0.5"},
	{"(0.5 + 0)", "dec", "ok 0.50"},
	{"(0.5 + 0)", "s", "ok 0.5"},
	{"NULLIF(0.5, 0)", "i", "ok 1"},
	{"NULLIF(0.5, 0)", "n", "ok 1"},
	{"NULLIF(0.5, 0)", "f", "ok 0.5"},
	{"NULLIF(0.5, 0)", "dec", "ok 0.50"},
	{"NULLIF(0.5, 0)", "s", "ok 0.5"},
	{"1e3", "i", "ok 1000"},
	{"1e3", "n", "ok 1000"},
	{"1e3", "f", "ok 1000"},
	{"1e3", "dec", "ok 1000.00"},
	{"1e3", "s", "ok 1000"},
	{"CASE WHEN true THEN 1e3 END", "i", "ok 1000"},
	{"CASE WHEN true THEN 1e3 END", "n", "ok 1000"},
	{"CASE WHEN true THEN 1e3 END", "f", "ok 1000"},
	{"CASE WHEN true THEN 1e3 END", "dec", "ok 1000.00"},
	{"CASE WHEN true THEN 1e3 END", "s", "ok 1000"},
	{"COALESCE(1e3, 1)", "i", "ok 1000"},
	{"COALESCE(1e3, 1)", "n", "ok 1000"},
	{"COALESCE(1e3, 1)", "f", "ok 1000"},
	{"COALESCE(1e3, 1)", "dec", "ok 1000.00"},
	{"COALESCE(1e3, 1)", "s", "ok 1000"},
	{"CASE WHEN false THEN 1 ELSE 1e3 END", "i", "ok 1000"},
	{"CASE WHEN false THEN 1 ELSE 1e3 END", "n", "ok 1000"},
	{"CASE WHEN false THEN 1 ELSE 1e3 END", "f", "ok 1000"},
	{"CASE WHEN false THEN 1 ELSE 1e3 END", "dec", "ok 1000.00"},
	{"CASE WHEN false THEN 1 ELSE 1e3 END", "s", "ok 1000"},
	{"GREATEST(1e3, 0.1)", "i", "ok 1000"},
	{"GREATEST(1e3, 0.1)", "n", "ok 1000"},
	{"GREATEST(1e3, 0.1)", "f", "ok 1000"},
	{"GREATEST(1e3, 0.1)", "dec", "ok 1000.00"},
	{"GREATEST(1e3, 0.1)", "s", "ok 1000"},
	{"(1e3 + 0)", "i", "ok 1000"},
	{"(1e3 + 0)", "n", "ok 1000"},
	{"(1e3 + 0)", "f", "ok 1000"},
	{"(1e3 + 0)", "dec", "ok 1000.00"},
	{"(1e3 + 0)", "s", "ok 1000"},
	{"NULLIF(1e3, 0)", "i", "ok 1000"},
	{"NULLIF(1e3, 0)", "n", "ok 1000"},
	{"NULLIF(1e3, 0)", "f", "ok 1000"},
	{"NULLIF(1e3, 0)", "dec", "ok 1000.00"},
	{"NULLIF(1e3, 0)", "s", "ok 1000"},
	{"5", "i", "ok 5"},
	{"5", "n", "ok 5"},
	{"5", "f", "ok 5"},
	{"5", "dec", "ok 5.00"},
	{"5", "s", "ok 5"},
	{"CASE WHEN true THEN 5 END", "i", "ok 5"},
	{"CASE WHEN true THEN 5 END", "n", "ok 5"},
	{"CASE WHEN true THEN 5 END", "f", "ok 5"},
	{"CASE WHEN true THEN 5 END", "dec", "ok 5.00"},
	{"CASE WHEN true THEN 5 END", "s", "ok 5"},
	{"COALESCE(5, 1)", "i", "ok 5"},
	{"COALESCE(5, 1)", "n", "ok 5"},
	{"COALESCE(5, 1)", "f", "ok 5"},
	{"COALESCE(5, 1)", "dec", "ok 5.00"},
	{"COALESCE(5, 1)", "s", "ok 5"},
	{"CASE WHEN false THEN 1 ELSE 5 END", "i", "ok 5"},
	{"CASE WHEN false THEN 1 ELSE 5 END", "n", "ok 5"},
	{"CASE WHEN false THEN 1 ELSE 5 END", "f", "ok 5"},
	{"CASE WHEN false THEN 1 ELSE 5 END", "dec", "ok 5.00"},
	{"CASE WHEN false THEN 1 ELSE 5 END", "s", "ok 5"},
	{"GREATEST(5, 0.1)", "i", "ok 5"},
	{"GREATEST(5, 0.1)", "n", "ok 5"},
	{"GREATEST(5, 0.1)", "f", "ok 5"},
	{"GREATEST(5, 0.1)", "dec", "ok 5.00"},
	{"GREATEST(5, 0.1)", "s", "ok 5"},
	{"(5 + 0)", "i", "ok 5"},
	{"(5 + 0)", "n", "ok 5"},
	{"(5 + 0)", "f", "ok 5"},
	{"(5 + 0)", "dec", "ok 5.00"},
	{"(5 + 0)", "s", "ok 5"},
	{"NULLIF(5, 0)", "i", "ok 5"},
	{"NULLIF(5, 0)", "n", "ok 5"},
	{"NULLIF(5, 0)", "f", "ok 5"},
	{"NULLIF(5, 0)", "dec", "ok 5.00"},
	{"NULLIF(5, 0)", "s", "ok 5"},
}
