// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
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
	if len(out) != 89 {
		t.Fatalf("read %d cells, want 89", len(out))
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
	tables := ejTables()
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
