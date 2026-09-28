// SPDX-License-Identifier: MIT

// Package intround is the coverage table for assigning a fractional value to
// an integer column, and the PostgreSQL 17.11 answer for every cell of it.
//
// PostgreSQL rounds that assignment by the SOURCE's TYPE: a numeric half AWAY
// from zero, a float8 half to EVEN. This engine computes some numeric
// expressions in float64 (division, SQRT, POWER, EXP, LN, LOG, EXTRACT —
// ADR-0024's recorded divergence), so the carrier cannot say which rule
// applies; the declaration's PostgreSQL category does
// (docs/internals/dml-integer-assignment-rounding.md, #1353).
//
// The table is the grammar, not the issue's examples: every arithmetic
// operator and unary minus over every operand category (integer literal and
// column, numeric literal and column, float8 literal and column, a
// float-carried numeric), every function the registry declares double
// precision that PostgreSQL spells over those categories, the CASE family, and
// the plan constructs a value can pass through before it meets the target
// (derived table, CTE, set operation, aggregate, window, join, DISTINCT,
// scalar subquery). Every expression evaluates to exactly 0.5 over the
// fixture, and each cell stores it plus an integer k into an INTEGER and a
// BIGINT column for k ∈ {-3, -1, 0, 1, 2, 3} — halves of both parities, where
// the two rules disagree on -2.5, -0.5, 0.5 and 2.5 and agree on 1.5 and 3.5 —
// bare (0.5: 1 or 0), and as a non-half control (2.4).
//
// testdata/cells.json is generated and measured by testdata/gen_cells.py
// against postgres:17-alpine; a cell PostgreSQL refuses (float8 % integer is
// 42883 there) is left out of the door it refuses on. testdata/pg_typeof.tsv
// (gen_typeof.py) is the declaration half: pg_typeof() of the operator and
// function grammar over every operand category.
package intround

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strings"
)

//go:embed testdata/cells.json
var cellsJSON []byte

//go:embed testdata/pg_typeof.tsv
var typeofTSV string

// TypeCell is one expression and the type pg_typeof() names for it on
// PostgreSQL 17.11, over m(i integer, b bigint, d numeric(10,2), f float8).
type TypeCell struct {
	Expr   string
	PGType string // integer, bigint, numeric, double precision
}

// TypeCells is the declaration half of the table: every arithmetic operator
// and double-precision function over every operand category, and the type
// PostgreSQL gives it — the fact an integer assignment's rounding rule reads.
func TypeCells() []TypeCell {
	var out []TypeCell
	for _, line := range strings.Split(strings.TrimSpace(typeofTSV), "\n") {
		e, t, ok := strings.Cut(line, "\t")
		if !ok {
			panic("intround: malformed pg_typeof.tsv line " + line)
		}
		out = append(out, TypeCell{Expr: e, PGType: t})
	}
	return out
}

// Fixture is the engine's spelling of the tables the answers were measured
// over. src and t_u carry k (the offset), i = 2, d = 0.50 and f = 0.5.
var Fixture = []string{
	"CREATE TABLE src (id INTEGER, k INTEGER, i INTEGER, d NUMERIC(10,2), f DOUBLE)",
	"INSERT INTO src VALUES " + srcRows,
	"CREATE TABLE t_v (id INTEGER, n4 INTEGER, n8 BIGINT)",
	"CREATE TABLE t_s (id INTEGER, n4 INTEGER, n8 BIGINT)",
	"CREATE TABLE t_u (id INTEGER, k INTEGER, i INTEGER, d NUMERIC(10,2), f DOUBLE, n4 INTEGER, n8 BIGINT)",
	"INSERT INTO t_u (id, k, i, d, f) SELECT id, k, i, d, f FROM src",
	"CREATE TABLE t_m (id INTEGER, n4 INTEGER, n8 BIGINT)",
	"INSERT INTO t_m (id) SELECT id FROM src",
}

const srcRows = "(1, -3, 2, 0.50, 0.5), (2, -1, 2, 0.50, 0.5), (3, 0, 2, 0.50, 0.5), (4, 1, 2, 0.50, 0.5), " +
	"(5, 2, 2, 0.50, 0.5), (6, 3, 2, 0.50, 0.5), (7, 0, 2, 0.50, 0.5), (8, 0, 2, 0.50, 0.5)"

// ks is the offset each fixture row adds, by id.
var ks = []int{-3, -1, 0, 1, 2, 3}

// Cell is one source spelling on one door: the statements to run, the query
// that reads the target back, and PostgreSQL's rows as "id:n4:n8 …".
type Cell struct {
	Name  string
	Door  string // values, select, update, merge
	Stmts []string
	Read  string
	Want  string
}

type template struct {
	Name    string   `json:"n"`
	Doors   string   `json:"d"`
	Want    string   `json:"w"`
	Expr    string   `json:"e"`
	Selects []string `json:"q"`
}

// Cells is the whole table, one cell per (source, door).
func Cells() []Cell {
	var ts []template
	if err := json.Unmarshal(cellsJSON, &ts); err != nil {
		panic(fmt.Sprintf("intround: %v", err))
	}
	var out []Cell
	for _, t := range ts {
		var want []string
		for _, r := range strings.Fields(t.Want) {
			id, v, _ := strings.Cut(r, ":")
			want = append(want, id+":"+v+":"+v)
		}
		w := strings.Join(want, " ")
		if len(t.Selects) > 0 {
			stmts := []string{"DELETE FROM t_s"}
			for _, s := range t.Selects {
				stmts = append(stmts, "INSERT INTO t_s (id, n4, n8) "+s)
			}
			out = append(out, Cell{Name: t.Name, Door: "select", Stmts: stmts, Read: readOf("t_s"), Want: w})
			continue
		}
		for _, d := range t.Doors {
			out = append(out, Cell{Name: t.Name, Door: doorName(d), Stmts: stmtsOf(d, t.Expr), Read: readOf(tableOf(d)), Want: w})
		}
	}
	return out
}

func doorName(d rune) string {
	switch d {
	case 'v':
		return "values"
	case 's':
		return "select"
	case 'u':
		return "update"
	}
	return "merge"
}

func tableOf(d rune) string {
	switch d {
	case 'v':
		return "t_v"
	case 's':
		return "t_s"
	case 'u':
		return "t_u"
	}
	return "t_m"
}

func readOf(table string) string { return "SELECT id, n4, n8 FROM " + table + " ORDER BY id" }

// stmtsOf spells one expression on one door, exactly as testdata/gen_cells.py
// did when it measured the answer.
func stmtsOf(d rune, e string) []string {
	half := "(" + e + ") + k"
	ctl := "(" + e + ") - 0.1 + 2"
	switch d {
	case 'v':
		rows := make([]string, 0, 8)
		for i, k := range ks {
			ex := fmt.Sprintf("(%s) + %d", e, k)
			if k < 0 {
				ex = fmt.Sprintf("(%s) - %d", e, -k)
			}
			rows = append(rows, fmt.Sprintf("(%d, %s, %s)", i+1, ex, ex))
		}
		rows = append(rows, fmt.Sprintf("(7, %s, %s)", e, e), fmt.Sprintf("(8, %s, %s)", ctl, ctl))
		return []string{"DELETE FROM t_v", "INSERT INTO t_v (id, n4, n8) VALUES " + strings.Join(rows, ", ")}
	case 's':
		return []string{"DELETE FROM t_s",
			"INSERT INTO t_s (id, n4, n8) SELECT id, " + half + ", " + half + " FROM src WHERE id <= 6",
			"INSERT INTO t_s (id, n4, n8) SELECT id, " + e + ", " + e + " FROM src WHERE id = 7",
			"INSERT INTO t_s (id, n4, n8) SELECT id, " + ctl + ", " + ctl + " FROM src WHERE id = 8"}
	case 'u':
		return []string{"UPDATE t_u SET n4 = NULL, n8 = NULL",
			"UPDATE t_u SET n4 = " + half + ", n8 = " + half + " WHERE id <= 6",
			"UPDATE t_u SET n4 = " + e + ", n8 = " + e + " WHERE id = 7",
			"UPDATE t_u SET n4 = " + ctl + ", n8 = " + ctl + " WHERE id = 8"}
	}
	m := "MERGE INTO t_m USING src ON t_m.id = src.id WHEN MATCHED AND src.id "
	return []string{"UPDATE t_m SET n4 = NULL, n8 = NULL",
		m + "<= 6 THEN UPDATE SET n4 = " + half + ", n8 = " + half,
		m + "= 7 THEN UPDATE SET n4 = " + e + ", n8 = " + e,
		m + "= 8 THEN UPDATE SET n4 = " + ctl + ", n8 = " + ctl}
}
