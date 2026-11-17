// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// WHAT THE TWO-LEVEL DISTINCT REWRITE RECOMBINES, AND WHERE IT IS READ FROM,
// ON EVERY ARM (#1696).
//
// The two-level DISTINCT rewrite turns a plain aggregate beside exactly one
// COUNT(DISTINCT x) into two stacked aggregates: level 1 groups by x and
// computes a partial per (K, x), level 2 recombines the partials — a COUNT as
// the SUM of its partial counts, a SUM as the SUM of its partial sums, AVG as
// a (SUM, COUNT) pair divided in a finalizing projection.
//
// The table: a plain aggregate {COUNT(*), COUNT(x), SUM(int), SUM(bigint),
// SUM(numeric), SUM(double), AVG(int), AVG(numeric), MIN, MAX} × {ungrouped,
// GROUP BY g} × the reader {the statement itself, a scalar subquery in the
// SELECT list, a scalar subquery in WHERE, a derived table, a CTE, UNION ALL}
// × the output names {distinct aliases, both outputs under one alias, the
// plain one under a column's name, no aliases} × {rows, zero rows}, beside one
// COUNT(DISTINCT x). The readers of a derived table rename its columns by
// POSITION (`AS q(c1, c2)`), because a name two outputs share cannot be
// selected by name. Controls the rewrite does not apply to — the plain
// aggregate alone, beside TWO COUNT(DISTINCT)s, and a lone COUNT(DISTINCT) —
// ride the same readers at the top level and as a scalar subquery.
//
// testdata/count_distinct_recombined_pg17.tsv is PostgreSQL 17.11's answer,
// value and declared type; testdata/count_distinct_recombined_kept.tsv lists
// the cells an arm answers otherwise, each with its reason.

// crTable is cz_t's shape with one column per summed type: x integer, xb
// bigint (one value past int4), xd numeric(10,2), xf double precision whose
// values are exact in binary so every summation order agrees.
func crTable() tmdTable {
	tbl := tmdTable{name: "cr_t", schema: parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32, Nullable: true},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
		{Name: "x", Type: parquet.TypeInt32, Nullable: true},
		{Name: "xb", Type: parquet.TypeInt64, Nullable: true},
		{Name: "xd", Type: parquet.TypeDecimal, Precision: 10, Scale: 2, Nullable: true},
		{Name: "xf", Type: parquet.TypeFloat64, Nullable: true},
	}}}
	for _, r := range [][6]any{
		{int32(1), int32(1), int32(10), int64(10000000000), 1.25, 0.5},
		{int32(2), int32(1), int32(10), nil, 2.50, nil},
		{int32(3), int32(2), int32(20), int64(20), nil, 1.25},
		{int32(4), int32(2), int32(30), int64(30), 3.75, 2.5},
		{int32(5), int32(3), int32(30), int64(-5), 4.00, 3.0},
		{int32(6), nil, int32(40), int64(40), 0.10, 0.125},
	} {
		tbl.rows = append(tbl.rows, map[string]any{"id": r[0], "g": r[1], "x": r[2], "xb": r[3], "xd": r[4], "xf": r[5]})
	}
	return tbl
}

const crPGFixture = `DROP TABLE IF EXISTS cr_t;
CREATE TABLE cr_t (id integer, g integer, x integer, xb bigint, xd numeric(10,2), xf double precision);
INSERT INTO cr_t VALUES (1,1,10,10000000000,1.25,0.5),(2,1,10,NULL,2.50,NULL),(3,2,20,20,NULL,1.25),(4,2,30,30,3.75,2.5),(5,3,30,-5,4.00,3.0),(6,NULL,40,40,0.10,0.125);`

func crCells() []cwCell {
	plains := []struct{ name, agg string }{
		{"countStar", "COUNT(*)"}, {"countX", "COUNT(x)"},
		{"sumInt", "SUM(x)"}, {"sumBig", "SUM(xb)"}, {"sumDec", "SUM(xd)"}, {"sumFloat", "SUM(xf)"},
		{"avgInt", "AVG(x)"}, {"avgDec", "AVG(xd)"},
		{"min", "MIN(x)"}, {"max", "MAX(x)"},
	}
	// The select list's two outputs: the plain aggregate, then the distinct.
	names := []struct{ name, plain, distinct string }{
		{"distinctAliases", " AS a", " AS b"},
		{"sharedAlias", " AS a", " AS a"},
		{"columnName", " AS x", " AS b"},
		{"noAliases", "", ""},
	}
	inputs := []struct{ name, where string }{{"rows", ""}, {"zero", " WHERE id < 0"}}
	groupings := []struct{ name, clause string }{{"ungrouped", ""}, {"grouped", " GROUP BY g"}}
	// Each reader wraps the two-output statement Q.
	readers := []struct{ name, f string }{
		{"top", "%s"},
		{"scalarSelect", "SELECT (SELECT q.c1 FROM (%s) AS q(c1, c2) ORDER BY q.c1 LIMIT 1) AS s"},
		{"scalarWhere", "SELECT id FROM cr_t WHERE id < (SELECT q.c1 FROM (%s) AS q(c1, c2) ORDER BY q.c1 LIMIT 1) + 3"},
		{"derived", "SELECT q.c1, q.c2 FROM (%s) AS q(c1, c2)"},
		{"cte", "WITH q(c1, c2) AS (%s) SELECT q.c1, q.c2 FROM q"},
		{"unionAll", "%s UNION ALL %s"},
	}
	// The companion: the rewrite's shape, and two controls it declines.
	companions := []struct{ name, list string }{
		{"oneDistinct", "COUNT(DISTINCT x)"},
		{"twoDistinct", "COUNT(DISTINCT x), COUNT(DISTINCT xb)"},
		{"plainAlone", ""},
	}
	var out []cwCell
	for _, comp := range companions {
		for _, rd := range readers {
			if comp.name != "oneDistinct" && rd.name != "top" && rd.name != "scalarSelect" {
				continue
			}
			for _, nm := range names {
				if comp.name != "oneDistinct" && nm.name != "distinctAliases" && nm.name != "sharedAlias" {
					continue
				}
				for _, p := range plains {
					for _, gr := range groupings {
						for _, in := range inputs {
							list := p.agg + nm.plain
							switch comp.name {
							case "oneDistinct":
								list += ", " + comp.list + nm.distinct
							case "twoDistinct":
								// The second distinct output shares the first's
								// name only where the name axis says so.
								second := " AS c"
								if nm.name == "sharedAlias" {
									second = nm.distinct
								}
								list += ", COUNT(DISTINCT x)" + nm.distinct + ", COUNT(DISTINCT xb)" + second
							case "plainAlone":
								// A second plain output keeps the reader's two
								// columns; neither is DISTINCT.
								list += ", COUNT(x)" + nm.distinct
							}
							q := "SELECT " + list + " FROM cr_t" + in.where + gr.clause
							var sql string
							switch rd.name {
							case "unionAll":
								sql = fmt.Sprintf(rd.f, q, "SELECT "+list+" FROM cr_t WHERE id < 0"+gr.clause)
							default:
								if comp.name == "twoDistinct" && rd.name != "top" {
									sql = strings.Replace(fmt.Sprintf(rd.f, q), "q(c1, c2)", "q(c1, c2, c3)", 1)
								} else {
									sql = fmt.Sprintf(rd.f, q)
								}
							}
							name := strings.Join([]string{comp.name, rd.name, nm.name, p.name, gr.name, in.name}, "/")
							out = append(out, cwCell{name, sql})
						}
					}
				}
			}
		}
	}
	// A lone COUNT(DISTINCT): the rewrite declines (nothing to recombine).
	for _, in := range inputs {
		out = append(out,
			cwCell{"loneDistinct/top/" + in.name, "SELECT COUNT(DISTINCT x) AS a FROM cr_t" + in.where},
			cwCell{"loneDistinct/scalarSelect/" + in.name, "SELECT (SELECT COUNT(DISTINCT x) FROM cr_t" + in.where + ") AS s"},
		)
	}
	// Three outputs, where one name is shared beside the item the finalizing
	// projection computes; a HAVING over the shared name; a CTE read twice,
	// whose body's projection is the CTE's terminal.
	for _, in := range inputs {
		f := "FROM cr_t" + in.where
		for _, e := range []struct{ name, sql string }{
			{"countBesidePair", "SELECT COUNT(*) AS c, MIN(x) AS a, COUNT(DISTINCT x) AS a " + f},
			{"avgBesidePair", "SELECT AVG(x) AS v, SUM(x) AS a, COUNT(DISTINCT x) AS a " + f},
			{"avgSharedWithDistinct", "SELECT AVG(x) AS a, COUNT(*) AS c, COUNT(DISTINCT x) AS a " + f},
			{"twoCountsShared", "SELECT COUNT(*) AS a, COUNT(x) AS a, COUNT(DISTINCT x) AS b " + f},
			{"distinctFirst", "SELECT COUNT(DISTINCT x) AS a, COUNT(*) AS a " + f},
			{"sharedHaving", "SELECT COUNT(*) AS a, COUNT(DISTINCT x) AS a " + f + " HAVING COUNT(*) >= 0"},
			{"cteTwice", "WITH q AS (SELECT COUNT(*) AS a, COUNT(DISTINCT x) AS b " + f + ") SELECT q1.a, q2.b FROM q q1, q q2"},
			{"cteTwiceShared", "WITH q(c1, c2) AS (SELECT COUNT(*) AS a, COUNT(DISTINCT x) AS a " + f + ") SELECT q1.c1, q2.c2 FROM q q1, q q2"},
		} {
			out = append(out, cwCell{"extra/" + e.name + "/" + in.name, e.sql})
		}
	}
	// Arithmetic over a recombined aggregate: the declared type is what the
	// operator reads, so declaring COUNT and SUM(int) bigint (as PostgreSQL
	// does) turns a numeric division into an integer one and a numeric
	// product into a bigint one that can overflow.
	for _, in := range inputs {
		f := "FROM cr_t" + in.where
		for _, e := range []struct{ name, sql string }{
			{"sumDiv3", "SELECT SUM(x) / 3 AS a, COUNT(DISTINCT x) AS b " + f},
			{"countDiv4", "SELECT COUNT(*) / 4 AS a, COUNT(DISTINCT x) AS b " + f},
			{"countTimes4e18", "SELECT COUNT(*) * 4e18 AS a, COUNT(DISTINCT x) AS b " + f},
			{"countTimesBigint", "SELECT COUNT(*) * 4000000000000000000 AS a, COUNT(DISTINCT x) AS b " + f},
			{"sumDivDistinct", "SELECT SUM(x) / COUNT(DISTINCT x) AS a " + f},
			{"scalarSumDiv3", "SELECT (SELECT SUM(x) / 3 " + f + " HAVING COUNT(DISTINCT x) > 0) AS s"},
		} {
			out = append(out, cwCell{"arith/" + e.name + "/" + in.name, e.sql})
		}
	}
	return out
}

const (
	crAnswersPath = "testdata/count_distinct_recombined_pg17.tsv"
	crKeptPath    = "testdata/count_distinct_recombined_kept.tsv"
)

// TestCountDistinctRecombinedMeasure runs every cell against PostgreSQL
// (CZ_PG_DSN) and rewrites the answer file; skipped otherwise.
func TestCountDistinctRecombinedMeasure(t *testing.T) {
	dsn := os.Getenv("CZ_PG_DSN")
	if dsn == "" {
		t.Skip("CZ_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgconn.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s';"+crPGFixture).ReadAll(); err != nil {
		t.Fatal(err)
	}
	var b strings.Builder
	b.WriteString("# PostgreSQL 17.11 answers for count_distinct_recombined_arms_test.go (TestCountDistinctRecombinedMeasure).\n")
	for _, c := range crCells() {
		r := conn.ExecParams(ctx, c.sql, nil, nil, nil, nil).Read()
		var ans string
		if r.Err != nil {
			pe, ok := r.Err.(*pgconn.PgError)
			if !ok {
				t.Fatalf("%s: %v", c.name, r.Err)
			}
			ans = "ERR " + pe.Code
		} else {
			header := make([]string, len(r.FieldDescriptions))
			for i, fd := range r.FieldDescriptions {
				header[i] = fd.Name + ":" + ftPGTypeName(fd.DataTypeOID)
			}
			rows := make([]string, len(r.Rows))
			for i, row := range r.Rows {
				f := make([]string, len(row))
				for j, v := range row {
					if v == nil {
						f[j] = "NULL"
					} else {
						f[j] = string(v)
					}
				}
				rows[i] = strings.Join(f, ",")
			}
			ans = czRender(ftRender(header, rows))
		}
		b.WriteString(c.name + "\t" + ans + "\n")
	}
	if err := os.WriteFile(crAnswersPath, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCountDistinctRecombinedDump writes every arm's answer for every cell
// (CR_DUMP=<path>): the base / tip measurement the landing note compares.
func TestCountDistinctRecombinedDump(t *testing.T) {
	path := os.Getenv("CR_DUMP")
	if path == "" {
		t.Skip("CR_DUMP unset")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := czArmsOver(t, ctx, crTable())
	cells := crCells()
	got := cwRunAll(t, arms, cells)
	var b strings.Builder
	for _, c := range cells {
		for i, arm := range arms {
			b.WriteString(c.name + "\t" + arm.name + "\t" + got[c.name][i] + "\n")
		}
	}
	if err := os.WriteFile(path, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestCountDistinctRecombinedEveryArm holds every cell on every arm to
// PostgreSQL's answer, or to its kept line (the reader and families of
// TestCountDistinctZeroRowsEveryArm; doubles within czClose's ulps).
func TestCountDistinctRecombinedEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: five arms over the recombined-aggregate table")
	}
	answers := cwReadTSV(t, crAnswersPath)
	kept := czReadKeptFrom(t, crKeptPath)
	cells := crCells()
	known := map[string]bool{}
	for _, c := range cells {
		if _, ok := answers[c.name]; !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		known[c.name] = true
	}
	for k := range kept {
		cell, _, _ := strings.Cut(k, "@")
		if !known[cell] {
			t.Fatalf("kept line %s names no generated cell", k)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := czArmsOver(t, ctx, crTable())
	got := cwRunAll(t, arms, cells)
	agree, keptN := 0, 0
	for _, c := range cells {
		pg := answers[c.name]
		for i, arm := range arms {
			ans := got[c.name][i]
			if k, ok := czKeptFor(kept, c.name, arm.name); ok {
				keptN++
				if czClose(ans, pg) {
					t.Errorf("%s [%s]: the kept line now agrees with PostgreSQL (%s): delete it\n  sql %s", c.name, arm.name, k.why, c.sql)
				} else if !czClose(ans, k.want) {
					t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  kept %s (%s)", c.name, arm.name, c.sql, ans, k.want, k.why)
				}
				continue
			}
			if !czClose(ans, pg) {
				t.Errorf("%s [%s]\n  sql  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, ans, pg)
				continue
			}
			agree++
		}
	}
	if len(cells) < 1000 || agree < 3000 {
		t.Fatalf("%d cells, %d (cell, arm) agreeing, %d kept: the table must discriminate", len(cells), agree, keptN)
	}
	t.Logf("%d cells × %d arms: %d agree with PostgreSQL 17.11, %d kept", len(cells), len(arms), agree, keptN)
}
