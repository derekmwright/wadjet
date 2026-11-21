// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A CORRELATED SCALAR SUBQUERY COMPARED IN A PREDICATE, WHOSE BODY CARRIES
// GROUP BY, HAVING, ORDER BY + LIMIT/OFFSET, DISTINCT OR A GROUPING
// CONSTRUCT, ON ALL FIVE ARMS (#1701).
//
// The WHERE-comparison decorrelation (logical.tryDecorrelateScalarSubquery)
// rewrites `p OP (SELECT agg FROM i WHERE i.k = o.k ...)` into a LEFT join
// against the aggregate grouped by the correlation key. That is the body's
// meaning only when the body is ONE aggregate over its filtered rows,
// ungrouped or grouped on exactly the correlation key; any clause that
// changes which row, or how many rows, the body answers was dropped. Every want is PostgreSQL 17.11's
// (testdata/where_compare_scalar_pg17.tsv, regenerated with
// WC_PG_DSN=<dsn> -run TestWhereCompareScalarGenerate).
//
//	wc_i:  k int | a int | c int   (1,10,1) (1,10,2) (1,20,3) (2,30,2) (NULL,40,1)
//	wc_m1: k = 2, a = 30   (one inner row; one on (k, a))       p in 0,1,2,3,NULL
//	wc_mn: k = 1, a = 10   (three inner rows; two on (k, a))    p in 0,1,2,3,NULL
//	wc_nm: k = 3, a = 10   (no inner row)                       p in 0,1,2,3,NULL
//	wc_nk: k NULL, a = 10  (matches nothing)                    p in 0,1,2,3,NULL
//	wc_one: u int   (one row, a JOIN ON partner)
//	wc_j:  k int | c int   (1,1) (1,2) (2,1) (2,3) (NULL,2)   (a second inner
//	       table, read only by the stacked cells, where_compare_stacked_arms_test.go)

func wcTables() []tmdTable {
	i32 := func(v int) any { return int32(v) }
	iSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		{Name: "a", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c", Type: parquet.TypeInt32, Nullable: true},
	}}
	oSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		{Name: "a", Type: parquet.TypeInt32, Nullable: true},
		{Name: "p", Type: parquet.TypeInt32, Nullable: true},
	}}
	uSchema := parquet.Schema{Columns: []parquet.Column{{Name: "u", Type: parquet.TypeInt32, Nullable: true}}}
	jSchema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c", Type: parquet.TypeInt32, Nullable: true},
	}}
	iRows := []map[string]any{
		{"k": i32(1), "a": i32(10), "c": i32(1)},
		{"k": i32(1), "a": i32(10), "c": i32(2)},
		{"k": i32(1), "a": i32(20), "c": i32(3)},
		{"k": i32(2), "a": i32(30), "c": i32(2)},
		{"k": nil, "a": i32(40), "c": i32(1)},
	}
	outer := func(k, a any) []map[string]any {
		var rows []map[string]any
		for _, p := range []any{i32(0), i32(1), i32(2), i32(3), nil} {
			rows = append(rows, map[string]any{"k": k, "a": a, "p": p})
		}
		return rows
	}
	return []tmdTable{
		{"wc_i", iSchema, iRows},
		{"wc_m1", oSchema, outer(i32(2), i32(30))},
		{"wc_mn", oSchema, outer(i32(1), i32(10))},
		{"wc_nm", oSchema, outer(i32(3), i32(10))},
		{"wc_nk", oSchema, outer(nil, i32(10))},
		{"wc_one", uSchema, []map[string]any{{"u": i32(1)}}},
		{"wc_j", jSchema, []map[string]any{
			{"k": i32(1), "c": i32(1)}, {"k": i32(1), "c": i32(2)},
			{"k": i32(2), "c": i32(1)}, {"k": i32(2), "c": i32(3)},
			{"k": nil, "c": i32(2)},
		}},
	}
}

const wcPGFixture = `DROP TABLE IF EXISTS wc_i; DROP TABLE IF EXISTS wc_m1; DROP TABLE IF EXISTS wc_mn;
DROP TABLE IF EXISTS wc_nm; DROP TABLE IF EXISTS wc_nk; DROP TABLE IF EXISTS wc_one; DROP TABLE IF EXISTS wc_j;
CREATE TABLE wc_i (k integer, a integer, c integer);
CREATE TABLE wc_m1 (k integer, a integer, p integer);
CREATE TABLE wc_mn (k integer, a integer, p integer);
CREATE TABLE wc_nm (k integer, a integer, p integer);
CREATE TABLE wc_nk (k integer, a integer, p integer);
CREATE TABLE wc_one (u integer);
CREATE TABLE wc_j (k integer, c integer);
INSERT INTO wc_i VALUES (1,10,1),(1,10,2),(1,20,3),(2,30,2),(NULL,40,1);
INSERT INTO wc_m1 VALUES (2,30,0),(2,30,1),(2,30,2),(2,30,3),(2,30,NULL);
INSERT INTO wc_mn VALUES (1,10,0),(1,10,1),(1,10,2),(1,10,3),(1,10,NULL);
INSERT INTO wc_nm VALUES (3,10,0),(3,10,1),(3,10,2),(3,10,3),(3,10,NULL);
INSERT INTO wc_nk VALUES (NULL,10,0),(NULL,10,1),(NULL,10,2),(NULL,10,3),(NULL,10,NULL);
INSERT INTO wc_one VALUES (1);
INSERT INTO wc_j VALUES (1,1),(1,2),(2,1),(2,3),(NULL,2)`

// wcCells is the enumeration: body clause x comparison x context x outer
// input.
func wcCells() []gsCell {
	const from = " FROM wc_i x WHERE x.k = o.k"
	bodies := []struct{ name, sql string }{
		// Controls: one ungrouped aggregate, what the rewrite is right for.
		{"count", "SELECT count(*)" + from},
		{"countExpr", "SELECT count(x.c) + 1" + from},
		{"sum", "SELECT sum(x.c)" + from},
		{"max", "SELECT max(x.c)" + from},
		// No aggregate at all (never decorrelated by this rule).
		{"noagg", "SELECT x.c" + from},
		{"noaggOrderLimit", "SELECT x.c" + from + " ORDER BY x.c DESC LIMIT 1"},
		{"noaggOrderLimitOffset", "SELECT x.c" + from + " ORDER BY x.c DESC LIMIT 1 OFFSET 1"},
		// An aggregate under a clause the rewrite dropped.
		{"groupBy", "SELECT count(*)" + from + " GROUP BY x.a"},
		{"groupByHaving", "SELECT count(*)" + from + " GROUP BY x.a HAVING count(*) > 1"},
		{"having", "SELECT count(*)" + from + " HAVING count(*) > 1"},
		{"groupByOrderLimit", "SELECT count(*)" + from + " GROUP BY x.a ORDER BY count(*) DESC LIMIT 1"},
		{"groupByOrderLimitOffset", "SELECT count(*)" + from + " GROUP BY x.a ORDER BY count(*) DESC LIMIT 1 OFFSET 1"},
		{"aggLimit0", "SELECT max(x.c)" + from + " LIMIT 0"},
		{"aggOffset1", "SELECT max(x.c)" + from + " OFFSET 1"},
		{"aggOrderBy", "SELECT max(x.c)" + from + " ORDER BY 1"},
		{"groupByDistinct", "SELECT DISTINCT count(*)" + from + " GROUP BY x.c"},
		{"aggDistinct", "SELECT DISTINCT max(x.c)" + from},
		{"rollupOrderLimit", "SELECT count(*)" + from + " GROUP BY ROLLUP (x.a) ORDER BY count(*) DESC LIMIT 1"},
		{"setsEmpty", "SELECT count(*)" + from + " GROUP BY GROUPING SETS (())"},
		// GROUP BY exactly the correlation key (round 2): one group when a
		// row matches, none (NULL, COUNT included) when nothing does. The
		// rewrite keeps these.
		{"gkCount", "SELECT count(*)" + from + " GROUP BY x.k"},
		{"gkSum", "SELECT sum(x.c)" + from + " GROUP BY x.k"},
		{"gkMax", "SELECT max(x.c)" + from + " GROUP BY x.k"},
		{"gkCountOrderLimit", "SELECT count(*)" + from + " GROUP BY x.k ORDER BY count(*) DESC LIMIT 1"},
		{"gkSumOrderLimit", "SELECT sum(x.c)" + from + " GROUP BY x.k ORDER BY 1 LIMIT 1"},
		{"gkMaxOrderLimit", "SELECT max(x.c)" + from + " GROUP BY x.k ORDER BY 1 DESC LIMIT 1"},
		{"gkCountLimit", "SELECT count(*)" + from + " GROUP BY x.k LIMIT 1"},
		{"gkParen", "SELECT count(*)" + from + " GROUP BY (x.k)"},
		{"gkDup", "SELECT count(*)" + from + " GROUP BY x.k, x.k"},
		{"gkUnqual", "SELECT count(*)" + from + " GROUP BY k"},
		{"gkNoAliasBare", "SELECT count(*) FROM wc_i WHERE wc_i.k = o.k GROUP BY k"},
		{"gkNoAliasQual", "SELECT count(*) FROM wc_i WHERE k = o.k GROUP BY wc_i.k"},
		{"gk2Both", "SELECT count(*)" + from + " AND x.a = o.a GROUP BY x.k, x.a"},
		{"gk2BothMax", "SELECT max(x.c)" + from + " AND x.a = o.a GROUP BY x.a, x.k"},
		{"gkReadsKey", "SELECT count(*) + x.k" + from + " GROUP BY x.k"},
		// ...and grouped bodies the rewrite declines to the per-row path: a
		// strict subset of the keys, another column, an expression or a
		// constant beside the key (`GROUP BY x.k, random()` forms several
		// groups, PostgreSQL 21000), an enclosing column, a HAVING.
		{"gk2One", "SELECT count(*)" + from + " AND x.a = o.a GROUP BY x.k"},
		{"gkKeyPlusOther", "SELECT count(*)" + from + " GROUP BY x.k, x.a"},
		{"gkExpr", "SELECT count(*)" + from + " GROUP BY x.k + 1"},
		{"gkConst", "SELECT count(*)" + from + " GROUP BY x.k, 1 + 0"},
		{"gkOuter", "SELECT count(*)" + from + " GROUP BY o.k"},
		{"gkHaving", "SELECT count(*)" + from + " GROUP BY x.k HAVING count(*) > 1"},
		// APPROX_DISTINCT is answered as COUNT(DISTINCT x) (#1691):
		// PostgreSQL has no approx_distinct, so its want is the PostgreSQL
		// answer to the same statement with count(DISTINCT x.c) written in
		// its place (wcPGStatement).
		{"approxDistinct", "SELECT approx_distinct(x.c)" + from},
		{"countDistinct", "SELECT count(DISTINCT x.c)" + from},
		// The rewrite computes ONE aggregate: two different ones, or one
		// under a CASE, decline; one under a function stays decorrelated.
		{"multiAgg", "SELECT max(x.c) - min(x.c)" + from},
		{"caseAgg", "SELECT CASE WHEN count(*) = 0 THEN 2 ELSE 1 END" + from},
		{"nestedAbs", "SELECT abs(max(x.c))" + from},
		{"nestedCoalesce", "SELECT coalesce(count(*), 5)" + from},
		{"sameAggTwice", "SELECT sum(x.c) / sum(x.c)" + from},
	}
	cmps := []struct{ name, tmpl string }{
		{"eq", "o.p = ({B})"},
		{"ne", "o.p <> ({B})"},
		{"lt", "o.p < ({B})"},
		{"ge", "o.p >= ({B})"},
		{"isnd", "o.p IS NOT DISTINCT FROM ({B})"},
		{"left", "({B}) = o.p"},
	}
	ctxs := []struct{ name, tmpl string }{
		{"alone", "SELECT o.k, o.p FROM {O} o WHERE {X}"},
		{"and", "SELECT o.k, o.p FROM {O} o WHERE {X} AND o.p >= 1"},
		{"or", "SELECT o.k, o.p FROM {O} o WHERE {X} OR o.p = 0"},
		{"not", "SELECT o.k, o.p FROM {O} o WHERE NOT ({X})"},
		{"having", "SELECT o.k, o.p FROM {O} o GROUP BY o.k, o.p HAVING {X}"},
		{"joinOn", "SELECT o.k, o.p FROM {O} o JOIN wc_one u ON {X}"},
	}
	inputs := []struct{ name, table string }{
		{"m1", "wc_m1"}, {"mn", "wc_mn"}, {"nm", "wc_nm"}, {"nk", "wc_nk"},
	}
	var out []gsCell
	for _, b := range bodies {
		for _, c := range cmps {
			for _, x := range ctxs {
				for _, in := range inputs {
					pred := strings.ReplaceAll(c.tmpl, "{B}", b.sql)
					sql := strings.NewReplacer("{X}", pred, "{O}", in.table).Replace(x.tmpl)
					out = append(out, gsCell{b.name + "/" + c.name + "/" + x.name + "/" + in.name, sql})
				}
			}
		}
	}
	return out
}

// TestWhereCompareScalarGenerate measures PostgreSQL 17.11 for every cell
// (WC_PG_DSN=postgres://...) and writes the answer table.
func TestWhereCompareScalarGenerate(t *testing.T) {
	dsn := os.Getenv("WC_PG_DSN")
	if dsn == "" {
		t.Skip("WC_PG_DSN unset")
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, "SET statement_timeout = '30s'"); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range strings.Split(wcPGFixture, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s: %v", stmt, err)
		}
	}
	var b strings.Builder
	b.WriteString("# name\tPostgreSQL 17.11 answer (sorted rows joined by \" | \"; ERR <SQLSTATE> for a refusal)\n")
	for _, c := range wcCells() {
		ans := func() string {
			rows, err := conn.Query(ctx, wcPGStatement(c.sql))
			if err != nil {
				return gsPGErr(err)
			}
			defer rows.Close()
			var all [][]any
			for rows.Next() {
				v, err := rows.Values()
				if err != nil {
					return gsPGErr(err)
				}
				for j := range v {
					if n, ok := v[j].(pgtype.Numeric); ok {
						if n.Valid {
							dv, _ := n.Value()
							v[j] = dv
						} else {
							v[j] = nil
						}
					}
				}
				all = append(all, v)
			}
			if err := rows.Err(); err != nil {
				return gsPGErr(err)
			}
			return gsRender(all)
		}()
		fmt.Fprintf(&b, "%s\t%s\n", c.name, ans)
	}
	if err := os.WriteFile("testdata/where_compare_scalar_pg17.tsv", []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}

// wcPGStatement is the statement PostgreSQL is asked for a cell: the cell's
// own, with approx_distinct(x) written as count(DISTINCT x), the function
// this engine answers it as (#1691).
func wcPGStatement(sql string) string {
	return strings.ReplaceAll(sql, "approx_distinct(", "count(DISTINCT ")
}

func wcArms(t *testing.T, ctx context.Context) []gsArm {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: 1 << 20, SpillDir: t.TempDir()})
	if err != nil {
		t.Fatalf("open standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range wcTables() {
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	single := func(sql string) (out string) {
		defer func() {
			if r := recover(); r != nil {
				out = fmt.Sprintf("PANIC %v", r)
			}
		}()
		res, err := db.Query(ctx, sql)
		if err != nil {
			return "ERR " + sqlerr.StateOf(err)
		}
		var rows [][]any
		for i := range res.Rows {
			rows = append(rows, res.Cells(i))
		}
		return gsRender(rows)
	}
	spilled := func(sql string) string {
		defer exec.ForceAggDrainEvery(exec.ForceAggDrainEvery(1))
		return single(sql)
	}
	stand := func(opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, wcTables())
		return tmdCoordinator(t, ctx, infra, opts...)
	}
	dag := func(c *Coordinator) func(string) string {
		return func(sql string) string {
			got, err := ssRunDAG(ctx, c, sql, false)
			if err != nil {
				return "ERR " + sqlerr.StateOf(err)
			}
			_, rest, _ := strings.Cut(got, "} rows=")
			n, rows, _ := strings.Cut(rest, " ")
			if n == "0" {
				return "(none)"
			}
			return rows
		}
	}
	coord := stand()
	coordB := stand(func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infraM, nil, wcTables())
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM, func(w *worker.Config) { w.MorselWorkers = 4 })
	return []gsArm{
		{"single", single}, {"spilled", spilled},
		{"dag", dag(coord)}, {"dag-shuffled", dag(coordB)}, {"dag-morsel4", dag(coordM)},
	}
}

// TestWhereCompareScalarEveryArm is the table on five arms. WC_MEASURE=1
// logs every cell instead of asserting; WC_ONLY=<regexp> narrows it.
func TestWhereCompareScalarEveryArm(t *testing.T) {
	if testing.Short() && os.Getenv("WC_ARMS") == "" {
		t.Skip("-short: five arms over the WHERE-comparison scalar subquery table (WC_ARMS=1 runs it)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	answers := wcPGAnswers(t)
	arms := wcArms(t, ctx)
	measure := os.Getenv("WC_MEASURE") != ""
	var only *regexp.Regexp
	if s := os.Getenv("WC_ONLY"); s != "" {
		only = regexp.MustCompile(s)
	}
	for _, c := range wcCells() {
		if only != nil && !only.MatchString(c.name) {
			continue
		}
		pg, ok := answers[c.name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", c.name)
		}
		want, why := pg, ""
		if k, ok := wcKept[c.name]; ok {
			want, why = k.want, k.why
		}
		for _, arm := range arms {
			got := arm.run(c.sql)
			if measure {
				status := "RIGHT"
				if got != pg {
					status = "DIFF"
				}
				t.Logf("CELL\t%s\t%s\t%s\tgot=%s\tpg=%s", status, c.name, arm.name, got, pg)
				continue
			}
			if got != want {
				if why != "" {
					t.Errorf("%s [%s]: %s\n got    %s\n pinned %s (a residual: %s; PostgreSQL 17.11 answers %s)",
						c.name, arm.name, c.sql, got, want, why, pg)
				} else {
					t.Errorf("%s [%s]: %s\n got  %s\n want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, got, want)
				}
			}
		}
	}
}

func wcPGAnswers(t *testing.T) map[string]string {
	t.Helper()
	b, err := os.ReadFile("testdata/where_compare_scalar_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, want, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("malformed answer line %q", line)
		}
		out[name] = want
	}
	return out
}

// wcKept is a cell this engine answers differently from PostgreSQL 17.11 at
// the base and at the fix alike, pinned at this engine's answer.
var wcKept = map[string]struct{ want, why string }{}
