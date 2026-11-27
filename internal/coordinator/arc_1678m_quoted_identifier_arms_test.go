// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// qiTables is the fixture for #1678 line m: ej_t / ej_u (line j's tables),
// qm_t whose COLUMN names need quotes (for case, a space, a dot, a quote and a
// reserved word), "Qm" a table whose NAME needs quotes and that carries two
// columns differing only by case ("Val" and val), and qm, the table "Qm"
// folds away from. The PostgreSQL DDL is the header of
// testdata/arc_1678m_quoted_identifier_pg17.tsv.
func qiTables() []tmdTable {
	col := func(n string, ty parquet.TypeID) parquet.Column {
		return parquet.Column{Name: n, Type: ty, Nullable: true}
	}
	i := func(v int64) any { return v }
	tbl := func(name string, cols []parquet.Column, rows [][]any) tmdTable {
		tb := tmdTable{name: name, schema: parquet.Schema{Columns: cols}}
		for _, r := range rows {
			m := map[string]any{}
			for k, c := range cols {
				m[c.Name] = r[k]
			}
			tb.rows = append(tb.rows, m)
		}
		return tb
	}
	qmT := tbl("qm_t", []parquet.Column{
		col("id", parquet.TypeInt64), col("Val", parquet.TypeInt64), col("my col", parquet.TypeString),
		col("a.b", parquet.TypeInt64), col(`a"b`, parquet.TypeInt64), col("order", parquet.TypeInt64),
		col("k", parquet.TypeString),
	}, [][]any{
		{i(1), i(10), "x", i(100), i(1000), i(7), "p"},
		{i(2), i(20), "y", i(200), i(2000), i(6), "q"},
		{i(3), i(30), nil, i(300), i(3000), i(5), "p"},
		{i(4), nil, "x", nil, i(4000), i(4), "r"},
		{i(5), i(50), "z", i(500), nil, i(3), nil},
	})
	qmQ := tbl("Qm", []parquet.Column{
		col("id", parquet.TypeInt64), col("Val", parquet.TypeInt64), col("k", parquet.TypeString),
	}, [][]any{
		{i(1), i(11), "p"},
		{i(2), i(22), "q"},
		{i(3), i(33), "s"},
		{i(6), i(66), "p"},
	})
	qm := tbl("qm", []parquet.Column{col("id", parquet.TypeInt64), col("val", parquet.TypeInt64)}, [][]any{
		{i(1), i(100)}, {i(2), i(200)}, {i(7), i(700)},
	})
	return append(ejTables(), qmT, qmQ, qm)
}

func qiCells(t *testing.T) []ejCell {
	t.Helper()
	f, err := os.Open("testdata/arc_1678m_quoted_identifier_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []ejCell
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.SplitN(line, "\t", 3)
		if len(p) != 3 {
			t.Fatalf("malformed cell line %q", line)
		}
		out = append(out, ejCell{p[0], p[1], p[2]})
	}
	if len(out) != 68 {
		t.Fatalf("read %d cells, want 68", len(out))
	}
	return out
}

var (
	qiAllArms = []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "dag-skew",
		"fastpath", "async", "async-probesplit"}
	qiDAGArms   = []string{"dag", "dag-shuffled", "dag-morsel4", "dag-skew"}
	qiAsyncArms = []string{"async", "async-probesplit"}
	// qiNotAsync is every arm but the asynchronous door.
	qiNotAsync = []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "dag-skew", "fastpath"}
)

func qiPins(rows []struct {
	cell string
	arms []string
	pin  string
},
) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, r := range rows {
		if out[r.cell] == nil {
			out[r.cell] = map[string]string{}
		}
		for _, a := range r.arms {
			out[r.cell][a] = r.pin
		}
	}
	return out
}

// qiKnownRefused are the arms that REFUSE a cell PostgreSQL answers, each the
// same at the base and not this line's (#1678 line m): the asynchronous
// door's plan-time gather check splits a name at its first dot and keeps a
// computed item's lowercased text, so `"a.b"`, `"x.y".id` and `"Bq".id + 0`
// find no emitted column ('computed no SELECT list'); the stage DAG refuses a
// WHERE over a quoted column name (42703); two SELECT aliases differing only
// by case under ORDER BY are refused by the unpartitioned DAG arms (0A000);
// and a derived table publishing "X" beside "x" is refused on every arm as
// ambiguous (42702: the engine's identity rule holds them one name). A pin
// is a fragment of the error and fails when the arm's answer moves.
var qiKnownRefused = qiPins([]struct {
	cell string
	arms []string
	pin  string
}{
	{"ra/selCaseExpr", qiAsyncArms, "computed no SELECT list"},
	{"ra/selDot", qiAsyncArms, "computed no SELECT list"},
	{"cn/plain", qiAsyncArms, "computed no SELECT list"},
	{"cn/joinDot", qiAsyncArms, "computed no SELECT list"},
	{"cn/where", qiDAGArms, "42703"},
	{"cn/caseColsWhere", qiDAGArms, "42703"},
	{"cal/orderBy", []string{"dag", "dag-morsel4"}, "0A000"},
	{"cal/orderByRev", []string{"dag", "dag-morsel4"}, "0A000"},
	{"cal/derived", qiAllArms, "42702"},
})

// qiKnownWrong pins the arms that answer a cell WRONG for a reason that is
// not the gather's rename comparison this line repairs, each measured at the
// base and the tip (the tip's answer is pinned; the note records both):
//   - ra/selDot: a relation alias containing a dot ("x.y") answers no rows on
//     every arm that runs it, single-process included.
//   - ra/foldMismatch, cn/caseCols, cn/foldMismatch: the binder resolves an
//     identifier whose case does not match (`Bq.id` against the alias "Bq",
//     `m."Val"` against the column val, `VAL` against "Val") where PostgreSQL
//     refuses it (42P01 / 42703) — every arm, single-process included; the
//     catalog's identity rule (a schema cannot hold two columns differing
//     only by case) is the column half of it.
//   - ra/window, ra/windowKey, cn/windowJoin: a window over a join whose
//     argument or PARTITION BY names a quoted build-side reference reads the
//     probe's column of that name — every arm, single-process included.
//   - ra/setop: a set-operation arm over a join projects a quoted build-side
//     reference as the text `"bq.id"` (the arm projection is built in
//     internal/planner/physical), and the four DAG arms read the probe's id.
//   - cal/orderBy: SELECT aliases "X" and "x" under ORDER BY "x" sort by "X"
//     on every arm that runs it.
var qiKnownWrong = qiPins([]struct {
	cell string
	arms []string
	pin  string
}{
	{"ra/selDot", qiNotAsync, "rows=0 "},
	{"ra/foldMismatch", qiAllArms, "rows=4 1,1 | 2,2 | 3,3 | 7,7"},
	{"ra/window", qiAllArms, "rows=4 1,8 | 2,2 | 3,3 | 7,8"},
	{"ra/windowKey", qiAllArms, "rows=4 1,1 | 2,1 | 3,1 | 7,1"},
	{"ra/setop", qiDAGArms, "rows=5 1 | 2 | 3 | 7 | 8"},
	{"cn/caseCols", []string{"single", "spilled512k", "fastpath", "async", "async-probesplit"}, "rows=2 1,10,10 | 2,20,20"},
	{"cn/caseCols", qiDAGArms, "rows=2 1,10,100 | 2,20,200"},
	{"cn/foldMismatch", qiAllArms, "rows=5 10 | 20 | 30 | 50 | NULL"},
	{"cn/windowJoin", qiAllArms, "rows=3 1,40 | 2,20 | 3,40"},
	{"cal/orderBy", []string{"single", "spilled512k", "dag-shuffled", "dag-skew", "fastpath", "async", "async-probesplit"}, "rows=2 3,2 | 7,6"},
})

// A QUOTED OR CASE-SIGNIFICANT IDENTIFIER NAMES THE SAME COLUMN ON EVERY ARM
// (#1678 line m). `SELECT a.id, "Bq".id FROM ej_t a JOIN ej_u "Bq" ON …`
// answered the PROBE's id in the second column on the four DAG arms: the
// gather's OutputRename carried the item's rendered text lowercased,
// `"bq".id`, which names no column of the join's stream (`Bq.id`), and the
// qualified-to-bare fallback took the bare `id`, the probe's. The gather now
// receives the parsed identifiers (dagplan.OutputRename.SourceQualifier /
// SourceColumn) and compares them as PostgreSQL does — a quoted qualifier
// exactly, so "Bq" and "bq" are two relations — and an ORDER BY term that is a
// qualified identifier reference keys the sort by the identifier, not by its
// delimited text.
//
// The matrix: an identifier needing quotes for case, a space, a dot, a quote
// and a reserved word, as a relation alias, a table name, a column name and a
// column alias, in the SELECT list, WHERE, ON, GROUP BY, ORDER BY, an
// aggregate argument, a window, a set-operation arm, DISTINCT and an outer
// join; two aliases differing only by case, both present; a quoted alias
// equal to an unquoted one after folding; a case mismatch PostgreSQL refuses;
// and unquoted controls — against PostgreSQL 17.11
// (testdata/arc_1678m_quoted_identifier_pg17.tsv). Cells still wrong or
// refused for a reason that is not this line's are pinned (qiKnownWrong,
// qiKnownRefused). A cell PostgreSQL refuses passes on an arm that refuses
// with the same SQLSTATE.
func TestArc1678mQuotedIdentifierEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: nine arms over the quoted-identifier table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	cells := qiCells(t)
	arms := ejArmsOver(t, ctx, qiTables())
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			for _, a := range arms {
				got := a.run(c.sql)
				if pin, ok := qiKnownWrong[c.name][a.name]; ok {
					if got != pin {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  pin  %s (a known wrong answer moved: re-measure against PostgreSQL 17.11's %s)",
							c.sql, a.name, got, pin, c.want)
					}
					continue
				}
				if frag, ok := qiKnownRefused[c.name][a.name]; ok {
					if !strings.HasPrefix(got, "ERR") || !strings.Contains(got, frag) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  pin  ERR containing %q (moved: re-measure against PostgreSQL 17.11's %s)",
							c.sql, a.name, got, frag, c.want)
					}
					continue
				}
				if state, ok := strings.CutPrefix(c.want, "ERROR "); ok {
					state, _, _ = strings.Cut(state, ":")
					if !strings.HasPrefix(got, "ERR "+state) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  want an error with SQLSTATE %s (PostgreSQL 17.11: %s)", c.sql, a.name, got, state, c.want)
					}
					continue
				}
				if got != c.want {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.sql, a.name, got, c.want)
				}
			}
		})
	}
}

// THE GATHER RESOLVES AN IDENTIFIER SOURCE BY ITS PARSED PARTS (#1678 line
// m), without a cluster. A join stream qualifies its build side's colliding
// columns by the relation's name as parsed (`Bq.id`); the SELECT item
// `"Bq".id` arrives as qualifier `Bq` and column `id`.
func TestGatherRenameComparesIdentifiers(t *testing.T) {
	ident := func(q, c string) dagplan.OutputRename {
		from := c
		if q != "" {
			from = q + "." + c
		}
		return dagplan.OutputRename{From: from, To: c, SourceQualifier: q, SourceColumn: c}
	}
	for _, tc := range []struct {
		name    string
		columns []string
		renames []dagplan.OutputRename
		want    []int
	}{
		{"a quoted build alias reads the build's column, not the probe's",
			[]string{"id", "s", "Bq.id", "Bq.s"},
			[]dagplan.OutputRename{ident("a", "id"), ident("Bq", "id")}, []int{0, 2}},
		{"two aliases differing only by case are two relations",
			[]string{"id", "n", "Bq.id", "Bq.n", "bq.id", "bq.n"},
			[]dagplan.OutputRename{ident("a", "id"), ident("bq", "id"), ident("Bq", "id")}, []int{0, 4, 2}},
		{"a qualifier differing by case does not take another relation's column",
			[]string{"id", "Val", "qm.id", "qm.val"},
			[]dagplan.OutputRename{ident("qm", "val"), ident("Qm", "Val")}, []int{3, 1}},
		{"the column half still answers case-insensitively",
			[]string{"id", "m.Val"},
			[]dagplan.OutputRename{ident("m", "val")}, []int{1}},
		{"a qualifier containing a dot is split where it was parsed",
			[]string{"id", "x.y.id"},
			[]dagplan.OutputRename{ident("a", "id"), ident("x.y", "id")}, []int{0, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := renameSourceIndices(tc.columns, tc.renames)
			for i := range tc.want {
				if i >= len(got) || got[i] != tc.want[i] {
					t.Fatalf("columns %v: got %v, want %v", tc.columns, got, tc.want)
				}
			}
		})
	}
}
