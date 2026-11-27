// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// ejTables is the fixture for #1678 line j: ej_t the probe-side table, ej_u
// a second table with the SAME column names (so a bare `s` names a column on
// both arms), ej_w one whose names collide with nothing. The PostgreSQL DDL
// is the header of testdata/arc_1678j_expr_on_derived_pg17.tsv.
func ejTables() []tmdTable {
	col := func(n string, ty parquet.TypeID) parquet.Column {
		return parquet.Column{Name: n, Type: ty, Nullable: true}
	}
	sch := parquet.Schema{Columns: []parquet.Column{col("id", parquet.TypeInt64), col("s", parquet.TypeString), col("n", parquet.TypeInt64)}}
	mk := func(name string, ss, ns []any) tmdTable {
		tb := tmdTable{name: name, schema: sch}
		for i := range ss {
			tb.rows = append(tb.rows, map[string]any{"id": int64(i + 1), "s": ss[i], "n": ns[i]})
		}
		return tb
	}
	t := mk("ej_t", []any{"a", "A", "b", nil, "c", "B", "a", "d"},
		[]any{int64(2), int64(3), int64(1), nil, int64(6), int64(2), int64(8), int64(5)})
	u := mk("ej_u", []any{"A", "b", "x", nil, "C", "a"},
		[]any{int64(3), int64(1), int64(9), int64(4), nil, int64(7)})
	w := tmdTable{name: "ej_w", schema: parquet.Schema{Columns: []parquet.Column{col("wid", parquet.TypeInt64), col("w", parquet.TypeString)}}}
	for i, s := range []any{"A", "b", "x", nil, "C", "a"} {
		w.rows = append(w.rows, map[string]any{"wid": int64(i + 1), "w": s})
	}
	return []tmdTable{t, u, w}
}

type ejCell struct{ name, sql, want string }

func ejCells(t *testing.T) []ejCell {
	t.Helper()
	f, err := os.Open("testdata/arc_1678j_expr_on_derived_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []ejCell
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<20)
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
	if len(out) != 179 {
		t.Fatalf("read %d cells, want 179", len(out))
	}
	return out
}

// ejRender renders rows the way the PostgreSQL table was rendered (psql -At
// -F , with NULL spelled NULL): each row's cells joined by ',', rows sorted.
func ejRender(rows [][]any) string {
	out := make([]string, len(rows))
	for i, r := range rows {
		f := make([]string, len(r))
		for j, v := range r {
			if v == nil {
				f[j] = "NULL"
			} else {
				f[j] = fmt.Sprint(v)
			}
		}
		out[i] = strings.Join(f, ",")
	}
	sort.Strings(out)
	return fmt.Sprintf("rows=%d %s", len(out), strings.Join(out, " | "))
}

type ejArm struct {
	name string
	run  func(string) string
}

// ejArms is every door the statement can reach: the embedded engine in memory
// and spilled, the stage DAG broadcast, shuffled, morsel-parallel and with the
// skew split armed, the coordinator's local fast path, and the asynchronous
// submit door with and without its probe split.
func ejArms(t *testing.T, ctx context.Context) []ejArm {
	t.Helper()
	return ejArmsOver(t, ctx, ejTables())
}

// ejArmsOver is ejArms over any fixture (#1678 line m's quoted-identifier
// matrix adds tables whose names and columns need quotes).
func ejArmsOver(t *testing.T, ctx context.Context, tables []tmdTable) []ejArm {
	t.Helper()
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tables)
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	single := func(budget int64) func(string) string {
		cfg := wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"}
		if budget > 0 {
			cfg.MemoryBudget = budget
			cfg.SpillDir = t.TempDir()
		}
		db, err := wadjet.Open(ctx, cfg)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { db.Close() })
		for _, tbl := range tables {
			if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
				t.Fatal(err)
			}
			ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: 2})
			if err := ing.Ingest(ctx, tbl.rows); err != nil {
				t.Fatal(err)
			}
			if err := ing.FlushAll(ctx); err != nil {
				t.Fatal(err)
			}
		}
		return func(s string) (out string) {
			defer func() {
				if r := recover(); r != nil {
					out = fmt.Sprintf("PANIC %v", r)
				}
			}()
			res, err := db.Query(ctx, s)
			if err != nil {
				return tbErr(err)
			}
			rows := make([][]any, len(res.Rows))
			for i := range res.Rows {
				rows[i] = res.Cells(i)
			}
			return ejRender(rows)
		}
	}
	collect := func(c *Coordinator, sql string, res *SQLResult) (out string) {
		defer func() { armDumpRecord(c, sql, nil, out, nil) }()
		if res.Error != "" {
			return "ERR (uncoded) " + res.Error
		}
		rows, err := tbdCollect(ctx, res)
		if err != nil {
			return tbErr(err)
		}
		return ejRender(rows)
	}
	dag := func(c *Coordinator) func(string) string {
		return func(s string) (out string) {
			defer func() {
				if r := recover(); r != nil {
					out = fmt.Sprintf("PANIC %v", r)
				}
			}()
			res, err := c.ExecuteSQL(ctx, s)
			if err != nil {
				return tbErr(err)
			}
			return collect(c, s, res)
		}
	}
	async := func(c *Coordinator, probeSplit bool) func(string) string {
		return func(s string) (out string) {
			defer func() {
				if r := recover(); r != nil {
					out = fmt.Sprintf("PANIC %v", r)
				}
			}()
			if probeSplit {
				o := dagplan.ProbeSplitMinBytes
				dagplan.ProbeSplitMinBytes = 1
				defer func() { dagplan.ProbeSplitMinBytes = o }()
			}
			qid, _, err := c.SubmitSQL(ctx, s)
			if err != nil {
				return tbErr(err)
			}
			deadline := time.Now().Add(120 * time.Second)
			for time.Now().Before(deadline) {
				if info := c.tracker.Get(qid); info != nil && (info.State == QueryStateCompleted || info.State == QueryStateFailed) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			res, err := c.GetQueryResults(ctx, qid)
			if err != nil {
				return tbErr(err)
			}
			return collect(c, s, res)
		}
	}
	asyncCoord := func() *Coordinator {
		c := stand(nil)
		c.BuildCacheThreshold = 1
		return c
	}
	ot, of := skewSplitTargetBytes, skewSplitMinGroupBytes
	skewSplitTargetBytes, skewSplitMinGroupBytes = 100, 100
	t.Cleanup(func() { skewSplitTargetBytes, skewSplitMinGroupBytes = ot, of })
	return []ejArm{
		{"single", single(0)},
		{"spilled512k", single(512 * 1024)},
		{"dag", dag(armDumpNamed("dag", stand(nil)))},
		{"dag-shuffled", dag(armDumpNamed("dag-shuffled", stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })))},
		{"dag-morsel4", dag(armDumpNamed("dag-morsel4", stand(func(w *worker.Config) { w.MorselWorkers = 4 })))},
		{"dag-skew", dag(armDumpNamed("dag-skew", stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1; c.SkewSplit = true })))},
		{"fastpath", dag(armDumpNamed("fastpath", stand(nil, func(c *Config) { c.LocalFastPathBytes = 64 << 20 })))},
		{"async", async(armDumpNamed("async", asyncCoord()), false)},
		{"async-probesplit", async(armDumpNamed("async-probesplit", asyncCoord()), true)},
	}
}

// ejKnown are the cells an arm answers differently from PostgreSQL for a
// reason that is NOT this line's: the asynchronous door refuses a correlated
// EXISTS / NOT EXISTS it cannot run per row (a loud 'per-row execution'
// refusal), and refuses a SELECT list naming a computed column of a nested
// derived table (#656's 'computed no SELECT list'); both the same at the base.
// The parser refuses a schema-qualified COLUMN reference (`public.ej_u.s`)
// on every arm (42601), where PostgreSQL answers it. And a plain LEFT JOIN
// that selects a derived table's computed column fails on the two partitioned
// DAG arms: a task whose build partition is empty shapes its side from the
// declared side schema, which names the computed column first and omits the
// source column the stream carries, and the gather refuses its file (ADR-0010,
// uncoded). That last one is the declared-schema defect behind
// dagplan.ErrOuterJoinDerivedFilterDistributed, reached here with no filter at
// all; it is the same at the base and is not this line's to repair.
// A pin fails when the arm's answer moves.
var ejKnown = func() map[string]map[string]string {
	noList := map[string]string{"async": "computed no SELECT list", "async-probesplit": "computed no SELECT list"}
	declared := map[string]string{"dag-shuffled": "(ADR-0010)", "dag-skew": "(ADR-0010)"}
	parse := map[string]string{}
	for _, a := range []string{"single", "spilled512k", "dag", "dag-shuffled", "dag-morsel4", "dag-skew",
		"fastpath", "async", "async-probesplit"} {
		parse[a] = "42601"
	}
	return map[string]map[string]string{
		"kind/exists":      {"async": "per-row execution", "async-probesplit": "per-row execution"},
		"kind/notExists":   {"async": "per-row execution", "async-probesplit": "per-row execution"},
		"rhs/nestedExpr":   noList,
		"cte/joinDefK":     noList,
		"inl/joinDefK":     noList,
		"inl/nestedK":      noList,
		"qual/schemaTable": parse,
		"od/leftSelectK":   declared,
		"od/leftWhereBS":   declared,
	}
}()

// ejKnownWrong pins the cells whose arms still answer WRONG, the same at the
// base, for a defect that is not this line's filter rule (#1678 line j,
// round 3; each is a line for the architect to file, with its mechanism in
// the round-3 note). A pin is the arm's exact answer, so it fails when the
// answer moves at all — to PostgreSQL's (unpin it) or to another wrong one.
//   - sl/*: the stage DAG's SELECT list or aggregate input reads a build-side
//     column of a derived table by a name its join's
//     stream does not carry — the gather's OutputRename asks for
//     `n2.n_nationkey` where the stream carries `x.n_nationkey` — and the
//     qualifier strip hands it the PROBE's column of that name. (sl/quotedAlias
//     answers PostgreSQL's rows since #1678 line m: the gather compares the
//     parsed identifiers, not the lowercased text `"bq".id`.)
//   - cx/cteJoinTwice: a CTE over a join read on both sides; the probe arm's
//     definitions are qualified by the CTE body's inner alias, which the
//     build's columns also carry on the stream.
//   - open/*: at the base every arm, single-process included, was wrong:
//     logical filter pushdown moved an outer predicate under a derived
//     table's Project onto its inner scan when a name matched the table's
//     INNER scope. #1678 line k stopped that (the join-side attribution ends
//     at the block) and every arm but the stage DAG now answers PostgreSQL's
//     rows. What stays wrong is the four DAG arms of an inner alias equal to
//     the outer alias: the join stage's build alias is the build STREAM's
//     (the inner scan's `a`), the probe carries `a` too, so the qualified
//     respelling `upper(a.s) = (upper(a.s))` and the gather's `a.s` read the
//     wrong side.
var ejKnownWrong = func() map[string]map[string]string {
	dagArms := []string{"dag", "dag-shuffled", "dag-morsel4", "dag-skew"}
	out := map[string]map[string]string{}
	for _, p := range []struct {
		cell   string
		arms   []string
		answer string
	}{
		{"cx/cteJoinTwice", dagArms, "rows=0 "},
		{"open/aliasIsProbe", dagArms, "rows=40 1,1 | 1,2 | 1,3 | 1,4 | 1,5 | 1,6 | 1,7 | 1,8 | 2,1 | 2,2 | 2,3 | 2,4 | 2,5 | 2,6 | 2,7 | 2,8 | 3,1 | 3,2 | 3,3 | 3,4 | 3,5 | 3,6 | 3,7 | 3,8 | 5,1 | 5,2 | 5,3 | 5,4 | 5,5 | 5,6 | 5,7 | 5,8 | 6,1 | 6,2 | 6,3 | 6,4 | 6,5 | 6,6 | 6,7 | 6,8"},
		{"open/aliasIsProbeSelect", dagArms, "rows=6 1,A,1,A | 2,b,2,B | 3,x,3,X | 4,NULL,4,NULL | 5,C,5,C | 6,a,6,A"},
		{"sl/cteJoinDefIds", dagArms, "rows=0 "},
		{"sl/inlNestedIds", dagArms, "rows=4 1,1 | 2,2 | 3,3 | 7,7"},
		{"sl/inlNestedIdsExpr", dagArms, "rows=9 1,1 | 1,1 | 2,2 | 2,2 | 3,3 | 5,5 | 6,6 | 7,7 | 7,7"},
		{"sl/innerAliasIds", dagArms, "rows=4 1,1 | 2,2 | 3,3 | 7,7"},
		{"sl/r0Shape", dagArms, "rows=1 8"},
	} {
		if out[p.cell] == nil {
			out[p.cell] = map[string]string{}
		}
		for _, a := range p.arms {
			out[p.cell][a] = p.answer
		}
	}
	return out
}()

// A JOIN'S ON CONDITION THAT COMPARES AN EXPRESSION WITH A DERIVED TABLE'S
// COLUMN IS APPLIED ON EVERY ARM (#1678 line j). The ON's left side {bare
// column, upper(), arithmetic, CAST, CASE, constant} × the right side {base
// table, bare rename, expression, a subquery with WHERE / GROUP BY / DISTINCT
// / LIMIT, a CTE, a UNION ALL, a nested derived table, a derived table over a
// join} × the side the derived table is written on and both sides derived ×
// INNER, LEFT, RIGHT, FULL, EXISTS, NOT EXISTS, IN, NOT IN × an added column
// equality, an inequality, an OR, a WHERE beside it, against PostgreSQL 17.11
// (testdata/arc_1678j_expr_on_derived_pg17.tsv).
//
// At the base the stage DAG answered every INNER cell whose ON (or WHERE)
// named a BUILD-arm derived column defined over a column the probe also
// carries — `upper(a.s) = b.k` with `b.k = upper(s)` arrived on the join stage
// as `upper(a.s) = upper(s)`, and the bare `s` bound the PROBE's: the probe
// compared with itself, every pair with a non-NULL probe value admitted.
//
// Round 2's cells: a CTE read on both sides of the join — twice, three times,
// joined to a base table and to itself, over a join, nested — beside the same
// shapes written as inline derived tables, which round 1 already answered
// (it told the arms apart by definition NODE, and a CTE shares its nodes); a
// definition qualified by its table's name, by a schema-qualified FROM, by an
// alias inside the table, and unqualified; a WHERE above a LEFT, RIGHT or
// FULL join comparing a probe expression with the derived column, the table
// on either side, with and without the `IS NULL` disjuncts that keep the
// NULL-padded rows (those run single-process now,
// dagplan.ErrOuterJoinDerivedFilterDistributed); and the review's probe
// shapes — constant definitions, a window, an aggregate, ORDER BY with LIMIT
// above the join, a three-way join with two expression conjuncts, a column
// list on the table, a definition's bare column also selected, a self-join
// with both sides derived.
//
// Round 3's cells: the derived table (or CTE) is the build — or the probe —
// of a LOWER join and the filter rides an upper join, two and three joins
// deep, with the derived table at the bottom and in the middle, inner and
// outer joins at each level (d2*, d3*: wrong on the four DAG arms at the base
// and at round 2, whose qualifier reached only the first join); the
// counterexamples the every-depth rule was tried on (cx/*: a join inside the
// build arm, with and without a probe column of the same name, a filter
// above a LIMIT, DISTINCT or window over the join, an aggregate above, a
// semi-join beside, RIGHT and FULL lower joins, a rename, an inner alias and
// a table-qualified definition below, a comma join); computed definitions —
// non-strict and strict — of a derived table an outer join PADS, read by the
// filter of that join or of an inner join above it (ns/*: those run
// single-process now, the route widened from the carrier join to every outer
// join the filter's resolution crosses); and, pinned as still wrong
// (ejKnownWrong), the SELECT-list build-column defect (sl/*) and, on the DAG
// arms only since #1678 line k, the build-stream alias collision (open/*).
func TestArc1678jExprOnDerivedEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: nine arms over the expression-against-derived-column ON table")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	cells := ejCells(t)
	arms := ejArms(t, ctx)
	for _, c := range cells {
		t.Run(c.name, func(t *testing.T) {
			for _, a := range arms {
				got := a.run(c.sql)
				if pin, ok := ejKnownWrong[c.name][a.name]; ok {
					if got != pin {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  pin  %s (a known wrong answer moved: re-measure against PostgreSQL 17.11's %s)",
							c.sql, a.name, got, pin, c.want)
					}
					continue
				}
				if frag, ok := ejKnown[c.name][a.name]; ok {
					if !strings.HasPrefix(got, "ERR") || !strings.Contains(got, frag) {
						t.Errorf("%s\n  arm  %s\n  got  %s\n  pin  ERR containing %q (moved: re-measure against PostgreSQL 17.11's %s)",
							c.sql, a.name, got, frag, c.want)
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

// A ROUTED STATEMENT PAST THE LOCAL BUDGET REFUSES WITH THE BUDGET ERROR AND
// DOES NOT FALL BACK TO THE DAG (#1678 line j, round 3). The outer-join route
// (dagplan.ErrOuterJoinDerivedFilterDistributed) runs on the coordinator-local
// pipeline under the refused-plan contract: the pipeline's memory pool lets
// its breakers spill, and the collected result is capped (8 × the fast-path
// threshold, or localResultBudgetOverride). PlanDistributed produced no stage
// DAG for the statement, so there is nothing to fall back to, and a fallback
// to a re-planned DAG would answer the very rows the route exists to keep
// off it: the statement must FAIL with the budget error, naming the remedy,
// and count one route. With the budget restored the same coordinator answers
// PostgreSQL's rows by the same route.
func TestArc1678jOuterJoinRouteOverBudgetRefuses(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: a coordinator with workers")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	t.Cleanup(cancel)
	infra := tmdInfra(t, ctx)
	tmdWriteTableList(t, ctx, infra, nil, ejTables())
	c := tmdCoordinatorWithWorkers(t, ctx, infra, nil)
	const sql = `SELECT a.id, b.id FROM ej_t a LEFT JOIN (SELECT id, upper(s) AS k FROM ej_u) b ON a.id = b.id WHERE upper(a.s) <> b.k`
	const want = "rows=3 2,2 | 3,3 | 6,6" // PostgreSQL 17.11 (ow/left)

	before := c.OuterJoinDerivedFilterLocalRoutes()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("within budget: %v", err)
	}
	rows, err := tbdCollect(ctx, res)
	if err != nil {
		t.Fatal(err)
	}
	if got := ejRender(rows); got != want {
		t.Fatalf("within budget: got %s, want %s", got, want)
	}
	if n := c.OuterJoinDerivedFilterLocalRoutes() - before; n != 1 {
		t.Fatalf("within budget: %d outer-join routes, want 1 (the statement must take the route)", n)
	}

	c.localResultBudgetOverride = 1
	t.Cleanup(func() { c.localResultBudgetOverride = 0 })
	before = c.OuterJoinDerivedFilterLocalRoutes()
	res, err = c.ExecuteSQL(ctx, sql)
	if err == nil {
		got := "(no result)"
		if res != nil {
			if rows, cerr := tbdCollect(ctx, res); cerr == nil {
				got = ejRender(rows)
			}
		}
		t.Fatalf("past the budget the routed statement answered %s; it must refuse with the budget error, not fall back", got)
	}
	if !errors.Is(err, exec.ErrCollectBudget) || !strings.Contains(err.Error(), "exceeded the local budget") ||
		!strings.Contains(err.Error(), "--local-fastpath-bytes") {
		t.Fatalf("past the budget: %v, want the local budget error naming --local-fastpath-bytes", err)
	}
	if n := c.OuterJoinDerivedFilterLocalRoutes() - before; n != 1 {
		t.Fatalf("past the budget: %d outer-join routes, want 1", n)
	}
}
