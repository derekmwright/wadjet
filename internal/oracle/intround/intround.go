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

//go:embed testdata/merge_cells.json
var mergeCellsJSON []byte

//go:embed testdata/mergeset_cells.json
var mergeSetCellsJSON []byte

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
	// The array-element cells' table (MergeSourceCells): a float8[] and a
	// numeric[] column holding f + k and d + k.
	"CREATE TABLE t_a AS SELECT id, ARRAY[f + k] AS af, ARRAY[d + k] AS ad, id * 0 AS n4, id * 0 AS n8 FROM src",
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
	// WantErr is the SQLSTATE PostgreSQL raises for the cell, which writes
	// nothing (MergeSetCells); Want is empty then.
	WantErr string
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

// DialectCells are INSERT … SELECT cells whose source this engine spells
// differently from PostgreSQL, so gen_cells.py cannot run one text on both:
// `unnest(v1, v2, …)` is this engine's variadic form of PostgreSQL's
// `unnest(ARRAY[v1, v2, …])`. Want is what PostgreSQL 17.11 stores for the
// ARRAY spelling — the same statement with `unnest(ARRAY[…])` in place of
// `unnest(…)` — measured once and written here: the column is numeric there
// (an unquoted fractional literal is), so 0.5 + k rounds half away from zero.
func DialectCells() []Cell {
	const want = "1:-3:-3 2:-1:-1 3:1:1 4:2:2 5:3:3 6:4:4 7:1:1"
	var out []Cell
	for _, u := range []struct{ name, args string }{
		{"numlit", "0.5"}, {"numlit-exp", "5e-1"}, {"numlit-pair", "0.5, 1.5"},
	} {
		for _, form := range []struct{ name, from, where string }{
			{"unnest", "unnest(" + u.args + ") AS u(x)", "x < 1"},
			{"unnest-ordinality", "unnest(" + u.args + ") WITH ORDINALITY AS u(x, o)", "o = 1"},
		} {
			out = append(out, Cell{
				Name: "shape/" + form.name + "/" + u.name,
				Door: "select",
				Stmts: []string{"DELETE FROM t_s",
					"INSERT INTO t_s (id, n4, n8) SELECT id, x + k, x + k FROM src CROSS JOIN " + form.from +
						" WHERE id <= 7 AND " + form.where},
				Read: readOf("t_s"),
				Want: want,
			})
		}
	}
	return out
}

// MergeSourceCells are MERGE cells whose value arrives through the SOURCE
// relation rather than the target's namespace: a derived table, a derived
// table over one, a CTE inside the source, UNION ALL, a join and a VALUES list,
// each over float8 and numeric spellings, with UPDATE and INSERT actions and
// qualified and bare references, into INTEGER and BIGINT. The source's
// declaration is the one its own plan emits, category included, exactly as an
// INSERT … SELECT reads it. Beside them, the array-element cells read a
// float8[] and a numeric[] column's element by subscript on UPDATE, MERGE
// (catalog and subquery source) and INSERT … SELECT: the element's type is
// the column's declared element. testdata/merge_cells.json is generated and
// measured on PostgreSQL 17.11 by testdata/gen_merge_cells.py.
func MergeSourceCells() []Cell {
	var ms []struct {
		Name  string   `json:"n"`
		Stmts []string `json:"s"`
		Want  string   `json:"w"`
		Read  string   `json:"r"`
	}
	if err := json.Unmarshal(mergeCellsJSON, &ms); err != nil {
		panic(fmt.Sprintf("intround: %v", err))
	}
	out := make([]Cell, 0, len(ms))
	for _, m := range ms {
		door, table := "merge", "t_m"
		if m.Read != "" {
			// an array-element cell: its door is the name's last segment
			door, table = m.Name[strings.LastIndexByte(m.Name, '/')+1:], m.Read
		}
		out = append(out, Cell{Name: m.Name, Door: door, Stmts: m.Stmts, Read: readOf(table), Want: m.Want})
	}
	return out
}

// MergeSourceRefusal names the error a MERGE-source cell refuses with before
// any value is assigned, where PostgreSQL answers — none of them about the
// rounding, each recorded for filing:
//
//   - a CTE the statement itself names (`WITH c AS (…) MERGE INTO t USING c
//     …`) and a VALUES list aliased with a column list (`USING (VALUES …) AS
//     s2(id, v)`) do not parse here; the same sources spelled inside a
//     subquery are asserted;
//   - an expression OVER a subquery source (`SET n4 = s2.v * 1`, a
//     subscript `s2.af[1]`) is 0A000: MERGE evaluates an expression only
//     over a catalog source's rows.
//
// A pin that starts answering fails: delete it.
func MergeSourceRefusal(name string) (string, bool) {
	switch {
	case strings.HasPrefix(name, "merge-source/with-merge/"):
		return `syntax error at or near "MERGE"`, true
	case strings.HasPrefix(name, "merge-source/values/"):
		return "expected ON", true
	case strings.HasSuffix(name, "/update-computed"), strings.HasSuffix(name, "/merge-derived"):
		return "has no declared schema to resolve it against", true
	}
	return "", false
}

// MergeSetFixture is the engine's spelling of the tables MergeSetCells were
// measured over (testdata/gen_mergeset_cells.py spells DOUBLE PRECISION where
// this engine's DDL spells DOUBLE): a source s whose float8 f, numeric d,
// integer i and text x hold 2.5, 0.5, -2.5, -0.5 (i: 5, 1, -5, -1) with a
// one-element array of each, and a target t whose INTEGER n4 and BIGINT n8
// take the value, beside a copy of each source column (tf, td, ti, tx).
var MergeSetFixture = []string{
	"CREATE TABLE s0 (id INTEGER, f DOUBLE, d NUMERIC(10,2), i INTEGER, x TEXT)",
	"INSERT INTO s0 VALUES (1, 2.5, 2.50, 5, '2.5'), (2, 0.5, 0.50, 1, '0.5'), " +
		"(3, -2.5, -2.50, -5, '-2.5'), (4, -0.5, -0.50, -1, '-0.5')",
	"CREATE TABLE s AS SELECT id, f, d, i, x, ARRAY[f] AS af, ARRAY[d] AS ad, ARRAY[i] AS ai, ARRAY[x] AS ax FROM s0",
	"CREATE TABLE t (id INTEGER, n4 INTEGER, n8 BIGINT, tf DOUBLE, td NUMERIC(10,2), ti INTEGER, tx TEXT)",
	"INSERT INTO t SELECT id, 0, 0, f, d, i, x FROM s0",
}

// MergeSetCells is the MERGE action's expression table, enumerated once:
// every expression FORM a WHEN MATCHED UPDATE SET or a WHEN NOT MATCHED
// INSERT VALUES assigns (a bare target column, a bare and a qualified source
// column, arithmetic, a scalar subquery uncorrelated / correlated on the
// source / correlated on the target / over an aggregate / over a CAST / over
// a literal, CASE, COALESCE, NULLIF, an array element, a CAST to float8 and to
// numeric, a literal, an aggregate, a window function) × the source's type
// (float8, numeric, integer, text) × the action × the source relation (the
// catalog table, a subquery over it), into an INTEGER and a BIGINT at once.
// Each cell is PostgreSQL 17.11's stored rows (Want) or its SQLSTATE
// (WantErr) — a statement PostgreSQL refuses writes nothing, and neither may
// this engine. testdata/mergeset_cells.json is generated and measured by
// testdata/gen_mergeset_cells.py; the read-back is t's id, n4, n8.
func MergeSetCells() []Cell {
	var ms []struct {
		Name  string   `json:"n"`
		Stmts []string `json:"s"`
		Want  string   `json:"w"`
		Err   string   `json:"e"`
	}
	if err := json.Unmarshal(mergeSetCellsJSON, &ms); err != nil {
		panic(fmt.Sprintf("intround: %v", err))
	}
	out := make([]Cell, 0, len(ms))
	for _, m := range ms {
		door := m.Name[strings.LastIndexByte(m.Name, '/')+1:]
		out = append(out, Cell{Name: m.Name, Door: "merge-" + door, Stmts: m.Stmts,
			Read: readOf("t"), Want: m.Want, WantErr: m.Err})
	}
	return out
}

// MergeSetRefusal is the SQLSTATE a MERGE SET cell refuses with where
// PostgreSQL answers: an expression OVER a subquery source — anything but a
// bare target column, a bare or qualified reference to one of the source's
// columns, or a literal that is no CAST (float8's is one) — is 0A000 ("MERGE
// cannot evaluate …: the source has no declared schema to resolve it
// against", #1398): MERGE evaluates an expression only over a catalog
// source's rows. Loud, never a value; a cell PostgreSQL refuses is not pinned
// (this engine raises PostgreSQL's SQLSTATE there, before the 0A000). A pin
// that starts answering fails: delete it.
func MergeSetRefusal(c Cell) (string, bool) {
	parts := strings.Split(c.Name, "/") // merge-set/<form>/<type>/<relation>/<action>
	if len(parts) != 5 || parts[3] != "subquery" || c.WantErr != "" {
		return "", false
	}
	switch parts[1] {
	case "bare-target", "bare-source", "qualified":
		return "", false
	case "literal":
		// a bare constant is not an expression over the source; float8's
		// literal is a CAST, and is
		return "0A000", parts[2] == "f8"
	}
	return "0A000", true
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
