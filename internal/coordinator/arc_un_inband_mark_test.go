// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// THE UNCONSTRAINED MARK TRAVELS WITH THE COLUMN, BY POSITION (ADR-0024 §10;
// ADR-0010's 2026-10-05 amendment). A `.wshf` exchange file carries each
// DECIMAL column's mark in its header's precision byte, so a stage reads the
// mark of the column it reads — never of another column of the same name.
// Round 4 carried the mark beside the file and stamped it back by NAME: a
// set operation whose arms publish `v` from a NUMERIC column and from a
// NUMERIC(10,2) column printed the constrained arm's `2.50` as `2.5` on the
// DAG arms, and the asynchronous door, which no stamp reached, printed a
// marked column's stored scale (`1.0000000000`) and, with its probe split,
// counted 0 rows where PostgreSQL counts 10 (c43).
//
// Every statement of testdata/arc_un_inband_cells.tsv — same-name arms of
// two typmods through UNION ALL / UNION / INTERSECT / EXCEPT, joins whose two
// sides both publish `v` (one marked), a CTE read twice, renamed and swapped
// columns, the asynchronous door's bare reads and its probe split — answers
// the same on eleven arms: single, spilled, dag, dag-shuffled, dag-morsel4,
// dag-eager (streaming exchange + eager dispatch), dag-skew, dag-aggsplit,
// the fast path, the asynchronous door, and the asynchronous door with its
// probe split forced. That answer is PostgreSQL 17.11's
// (testdata/arc_un_inband_pg17.tsv) or the kept one below.
func TestArcUNInBandMarkEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: eleven arms over the unconstrained mark")
	}
	cells := wdConsumerTable(t, "testdata/arc_un_inband_cells.tsv")
	pg := map[string]string{}
	for _, a := range wdConsumerTable(t, "testdata/arc_un_inband_pg17.tsv") {
		pg[a[0]] = a[1]
	}
	kept := unibKept()
	armKept := unibArmKept()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := unibArms(t, ctx)
	var gen *os.File
	if p := os.Getenv("UN_INBAND_GEN"); p != "" {
		var err error
		if gen, err = os.Create(p); err != nil {
			t.Fatal(err)
		}
		defer gen.Close()
	}
	typePrefix := regexp.MustCompile(`^type=\S* `)
	asserted := 0
	for _, c := range cells {
		name, sql := c[0], c[1]
		want, ok := pg[name]
		if !ok {
			t.Fatalf("cell %s has no PostgreSQL answer: re-measure the table", name)
		}
		if k, ok := kept[name]; ok {
			want = k.want
		}
		t.Run(name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(sql)
				got := ""
				if err != nil {
					got = "ERR " + strings.SplitN(err.Error(), "\n", 2)[0]
				} else {
					got = typePrefix.ReplaceAllString(unRender(res), "")
				}
				if gen != nil {
					fmt.Fprintf(gen, "%s\t%s\t%s\n", name, arm.name, got)
				}
				w := want
				if k, ok := armKept[name]; ok {
					if a, ok := k.arms[arm.name]; ok {
						w = a
					}
				}
				asserted++
				if msg, ok := strings.CutPrefix(w, "ERR "); ok && err != nil && strings.Contains(err.Error(), msg) {
					continue
				}
				if got != w {
					t.Errorf("%s\n  arm  %s\n  got  %s\n  want %s", sql, arm.name, got, w)
				}
			}
		})
	}
	if asserted != len(cells)*len(arms) {
		t.Fatalf("only %d (cell, arm) answers asserted", asserted)
	}
}

// unibKept is the kept answer, the same on every arm, where it is not
// PostgreSQL's.
func unibKept() map[string]struct{ want, why string } {
	return map[string]struct{ want, why string }{
		"c08":  {"rows=12 0.0000000000 | 0.0000000001 | 1.0000000000 | 1.0000000000 | 1.0000000000 | 1.5000000000 | 1.5000000000 | 2.5000000000 | 7.0000000000 | 7.0000000000 | NULL | NULL", unibR18},
		"c10":  {"rows=1 0", unibR18},
		"c26b": {"rows=4 1.0000000000 | 1.5000000000 | 7.0000000000 | NULL", unibR18},
		"c36":  {"rows=2 1.5000000000 | 1.5000000000", unibR18},
		"c37":  {"rows=7 0.0000000000 | 0.0000000001 | 1.0000000000 | 1.5000000000 | 2.5000000000 | 7.0000000000 | NULL", unibR18},
		"v1":   {"rows=12 0.0000000000 | 0.0000000001 | 1.0000000000 | 1.0000000000 | 1.0000000000 | 1.0000000000 | 1.5000000000 | 1.5000000000 | 7.0000000000 | 7.0000000000 | NULL | NULL", unibR18},
		"v2":   {"rows=12 0.0000000000 | 0.0000000001 | 1.0000000000 | 1.0000000000 | 1.0000000000 | 1.5000000000 | 1.5000000000 | 2.5000000000 | 7.0000000000 | 7.0000000000 | NULL | NULL", unibR18},
		"v4":   {"rows=2 0.0000000000 | 2.5000000000", unibR18},
		"v5":   {"rows=12 0.0000000000 | 0.0000000001 | 1.0000000000 | 1.0000000000 | 1.0000000000 | 1.5000000000 | 1.5000000000 | 2.5000000000 | 7.0000000000 | 7.0000000000 | NULL | NULL", unibR18},
		"v6":   {"rows=1 9", unibR18},
		"v7":   {"rows=2 2.5000000000 | 2.5000000000", unibR18},
		"e01":  {"rows=12 0.0000000000 | 0.0000000001 | 1.0000000000 | 1.0000000000 | 1.0000000000 | 1.5000000000 | 1.5000000000 | 2.5000000000 | 7.0000000000 | 7.0000000000 | NULL | NULL", unibR18},
		"e04":  {"rows=2 0.0000000000 | 2.5000000000", unibR18},
		"e12":  {"rows=11 0.0000000000 | 0.0000000001 | 1.0000000000 | 1.0000000000 | 1.5000000000 | 1.5000000000 | 2.5000000000 | 7.0000000000 | 7.0000000000 | NULL | NULL", unibR18},
	}
}

// unibR18: the set operation's column is created unconstrained only when
// every arm's is (ADR-0024 §10); over a NUMERIC(10,2) arm it is the union's
// DECIMAL(38,10) and prints that one scale (numeric-decimal r18), the same on
// every arm. PostgreSQL prints each value at its own arm's scale.
const unibR18 = "r18: a set operation over a column of another declaration prints the result column's one scale"

// unibArmKept is an asynchronous arm's answer where it is not the other
// arms', the same at c67ebf5b: an error substring ("ERR ...") or the rows.
// None of them involves the mark; each is a filing candidate in the arc's
// notes.
func unibArmKept() map[string]struct {
	why  string
	arms map[string]string
} {
	return map[string]struct {
		why  string
		arms map[string]string
	}{
		"c15": {unibGroupByMerge, map[string]string{"async-probesplit": "ERR is not in the partial result schema"}},
		"c19": {unibGroupByMerge, map[string]string{"async-probesplit": "ERR is not in the partial result schema"}},
		"c22": {unibProbeSplitCount, map[string]string{"async-probesplit": "rows=15 {\"v\" : 0.0000000001} | {\"v\" : 0.0000000001} | {\"v\" : 0.0000000001} | {\"v\" : 1.5} | {\"v\" : 1.5} | {\"v\" : 1.5} | {\"v\" : 1} | {\"v\" : 1} | {\"v\" : 1} | {\"v\" : 7} | {\"v\" : 7} | {\"v\" : 7} | {\"v\" : null} | {\"v\" : null} | {\"v\" : null}"}},
		"c24": {unibProbeSplitCount, map[string]string{"async-probesplit": "rows=1 12"}},
		"c25": {unibNoSelectList, map[string]string{"async": "ERR the stage DAG computed no SELECT list for this shape", "async-probesplit": "ERR the stage DAG computed no SELECT list for this shape"}},
		"c30": {unibNoSelectList, map[string]string{"async": "ERR the stage DAG computed no SELECT list for this shape", "async-probesplit": "ERR the stage DAG computed no SELECT list for this shape"}},
		"c31": {unibProbeSplitCount, map[string]string{"async-probesplit": "rows=3 0.0000000001 | 0.0000000001 | 0.0000000001"}},
		"c39": {unibNoSelectList, map[string]string{"async": "ERR the stage DAG computed no SELECT list for this shape", "async-probesplit": "ERR the stage DAG computed no SELECT list for this shape"}},
		"c41": {unibGroupByMerge, map[string]string{"async-probesplit": "ERR is not in the partial result schema"}},
		"c42": {unibGroupByMerge, map[string]string{"async-probesplit": "ERR is not in the partial result schema"}},
		"c44": {unibGroupByMerge, map[string]string{"async-probesplit": "ERR is not in the partial result schema"}},
		"c45": {unibGroupByMerge, map[string]string{"async-probesplit": "ERR is not in the partial result schema"}},
		"c47": {unibGroupByMerge, map[string]string{"async-probesplit": "ERR is not in the partial result schema"}},
		"e08": {unibSwappedRename, map[string]string{"async": "rows=6 0.75,NULL | 1.25,NULL | 10.00,NULL | 2.50,NULL | 3.33,NULL | NULL,NULL", "async-probesplit": "rows=6 0.75,NULL | 1.25,NULL | 10.00,NULL | 2.50,NULL | 3.33,NULL | NULL,NULL"}},
	}
}

const (
	unibNoSelectList    = "the asynchronous door refuses a shape whose gather renames a computed column (#656), at c67ebf5b too (review r4 N3)"
	unibGroupByMerge    = "the forced probe split cannot merge a GROUP BY over a text key, at c67ebf5b too (review r4 N2)"
	unibProbeSplitCount = "the forced probe split merges no DISTINCT / GROUP BY / MIN: one row per worker, at c67ebf5b too (review r4 N2)"
	unibSwappedRename   = "the asynchronous door answers NULL for the second of two swapped names under a GROUP BY, at c67ebf5b too (UN-F10)"
)

var unibVals = []string{"1", "1.5", "7", "0.0000000001", "NULL"}
var unibNVals = []string{"1.00", "1.50", "7.25", "NULL"}

func unibBigRow(i int) (g int, v, n string) {
	g = 1
	if i%10 == 0 {
		g = (i/10)%7 + 2
	}
	return g, unibVals[i%5], unibNVals[i%4]
}

// unibFixture is unPGFixture plus rv_n (a NUMERIC(10,2) column named v),
// rv_big (3000 rows; eight files on the DAG arms, a hot key g = 1) and rv_k.
func unibFixture() []string {
	out := append([]string(nil), unPGFixture...)
	out = append(out,
		"CREATE TABLE rv_n (id BIGINT, g BIGINT, v NUMERIC(10,2))",
		"INSERT INTO rv_n VALUES (1,1,1.00),(2,1,1.50),(3,2,7.00),(4,2,0.00),(5,3,NULL),(6,3,2.50)",
		"CREATE TABLE rv_big (id BIGINT, g BIGINT, v NUMERIC, n NUMERIC(10,2))",
		"CREATE TABLE rv_k (id BIGINT, v NUMERIC, k NUMERIC(10,2))",
	)
	var b strings.Builder
	b.WriteString("INSERT INTO rv_big VALUES ")
	for i := 1; i <= 3000; i++ {
		g, v, n := unibBigRow(i)
		if i > 1 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d,%d,%s,%s)", i, g, v, n)
	}
	out = append(out, b.String())
	b.Reset()
	b.WriteString("INSERT INTO rv_k VALUES ")
	for i := 1; i <= 50; i++ {
		if i > 1 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, "(%d,%s,%s)", i, unibVals[i%5], unibNVals[i%4])
	}
	out = append(out, b.String())
	return out
}

func unibTables(t *testing.T) (small []tmdTable, big tmdTable) {
	t.Helper()
	decl := func(name, typ string) parquet.Column {
		c, err := parquet.DeclaredColumn(name, typ, true)
		if err != nil {
			t.Fatalf("declare %s %s: %v", name, typ, err)
		}
		return c
	}
	cell := func(c parquet.Column, s string) any {
		if s == "NULL" {
			return nil
		}
		if c.Type == parquet.TypeDecimal {
			return dtpDecimal128(unUnscaled(t, s, c.Scale))
		}
		n, _ := strconv.ParseInt(s, 10, 64)
		return n
	}
	mk := func(name string, cols []parquet.Column, rows [][]string) tmdTable {
		tb := tmdTable{name: name, schema: parquet.Schema{Columns: cols}}
		for _, r := range rows {
			row := map[string]any{}
			for i, c := range cols {
				row[c.Name] = cell(c, r[i])
			}
			tb.rows = append(tb.rows, row)
		}
		return tb
	}
	small = unTables(t)
	small = append(small, mk("rv_n", []parquet.Column{decl("id", "BIGINT"), decl("g", "BIGINT"), decl("v", "NUMERIC(10,2)")},
		[][]string{{"1", "1", "1.00"}, {"2", "1", "1.50"}, {"3", "2", "7.00"}, {"4", "2", "0.00"}, {"5", "3", "NULL"}, {"6", "3", "2.50"}}))
	var kr [][]string
	for i := 1; i <= 50; i++ {
		kr = append(kr, []string{strconv.Itoa(i), unibVals[i%5], unibNVals[i%4]})
	}
	small = append(small, mk("rv_k", []parquet.Column{decl("id", "BIGINT"), decl("v", "NUMERIC"), decl("k", "NUMERIC(10,2)")}, kr))
	var br [][]string
	for i := 1; i <= 3000; i++ {
		g, v, n := unibBigRow(i)
		br = append(br, []string{strconv.Itoa(i), strconv.Itoa(g), v, n})
	}
	big = mk("rv_big", []parquet.Column{decl("id", "BIGINT"), decl("g", "BIGINT"), decl("v", "NUMERIC"), decl("n", "NUMERIC(10,2)")}, br)
	return small, big
}

func unibWriteChunks(t *testing.T, ctx context.Context, infra tmdInfraT, tbl tmdTable, chunks int) {
	t.Helper()
	if err := infra.cat.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
		t.Fatal(err)
	}
	per := (len(tbl.rows) + chunks - 1) / chunks
	var entries []catalog.FileEntry
	for c := 0; c < chunks; c++ {
		lo, hi := c*per, min(c*per+per, len(tbl.rows))
		var buf bytes.Buffer
		pw, err := parquet.NewWriter(&buf, tbl.schema, parquet.DefaultWriterConfig())
		if err != nil {
			t.Fatal(err)
		}
		if err := pw.WriteRows(tbl.rows[lo:hi]); err != nil {
			t.Fatal(err)
		}
		if err := pw.Close(); err != nil {
			t.Fatal(err)
		}
		path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", tbl.name, c)
		if _, err := infra.store.Put(ctx, "test", path, bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
		entries = append(entries, catalog.FileEntry{Path: path, SizeBytes: int64(buf.Len()), NumRows: int64(hi - lo), CreatedAt: time.Now()})
	}
	if err := infra.cat.AddFiles(ctx, tbl.name, map[string]string{}, "tables/"+tbl.name+"/", entries); err != nil {
		t.Fatal(err)
	}
}

// unibArms is the eleven arms. Each DAG arm is a coordinator with three
// workers over the same files; the asynchronous arms submit the statement
// and read its result the way the HTTP async door does (SubmitSQL,
// GetQueryResults), the second with its probe split forced
// (dagplan.ProbeSplitMinBytes = 1, BuildCacheThreshold = 1: rv_big's eight
// files split over the workers, rv_k pre-scanned into the build cache).
func unibArms(t *testing.T, ctx context.Context) []wdArm {
	t.Helper()
	small, big := unibTables(t)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, small)
		unibWriteChunks(t, ctx, infra, big, 8)
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	single := func(budget int64) func(string) (wdResult, error) {
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
		for _, q := range unibFixture() {
			if _, err := db.Query(ctx, q); err != nil {
				t.Fatalf("%.80s: %v", q, err)
			}
		}
		return func(sql string) (res wdResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := db.Query(ctx, sql)
			if qerr != nil {
				return wdResult{}, qerr
			}
			res.schema = out.OutputSchema
			for i := range out.Rows {
				res.rows = append(res.rows, out.Cells(i))
			}
			return res, nil
		}
	}
	collect := func(out *SQLResult) (res wdResult, err error) {
		if out.Error != "" {
			return wdResult{}, fmt.Errorf("%s", out.Error)
		}
		res.schema = append([]parquet.Column(nil), out.OutputSchema()...)
		if st := out.Stream(); st != nil {
			defer st.Close()
			for {
				bb, berr := st.Next(ctx)
				if berr != nil {
					return wdResult{}, berr
				}
				if bb == nil {
					return res, nil
				}
				res.rows = append(res.rows, bb.ToRowValues()...)
			}
		}
		rows, rerr := out.Rows()
		if rerr != nil {
			return wdResult{}, rerr
		}
		for _, r := range rows {
			cells := make([]any, len(res.schema))
			for j, col := range res.schema {
				cells[j] = r[col.Name]
			}
			res.rows = append(res.rows, cells)
		}
		return res, nil
	}
	runDAG := func(c *Coordinator) func(string) (wdResult, error) {
		return func(sql string) (res wdResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			out, qerr := c.ExecuteSQL(ctx, sql)
			if qerr != nil {
				return wdResult{}, qerr
			}
			return collect(out)
		}
	}
	runAsync := func(c *Coordinator, probeSplit bool) func(string) (wdResult, error) {
		return func(sql string) (res wdResult, err error) {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("PANIC: %v", r)
				}
			}()
			if probeSplit {
				o := dagplan.ProbeSplitMinBytes
				dagplan.ProbeSplitMinBytes = 1
				defer func() { dagplan.ProbeSplitMinBytes = o }()
			}
			qid, _, serr := c.SubmitSQL(ctx, sql)
			if serr != nil {
				return wdResult{}, serr
			}
			deadline := time.Now().Add(120 * time.Second)
			for time.Now().Before(deadline) {
				if info := c.tracker.Get(qid); info != nil && (info.State == QueryStateCompleted || info.State == QueryStateFailed) {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			out, gerr := c.GetQueryResults(ctx, qid)
			if gerr != nil {
				return wdResult{}, gerr
			}
			return collect(out)
		}
	}
	asyncCoord := func() *Coordinator {
		c := stand(nil)
		c.BuildCacheThreshold = 1
		return c
	}
	eager := stand(nil, func(c *Config) {
		c.BroadcastBytesOverride = 1
		c.StreamingExchange = true
		c.EagerDispatch = true
		c.SkewSplit = true
	})
	ot, of := skewSplitTargetBytes, skewSplitMinGroupBytes
	skewSplitTargetBytes, skewSplitMinGroupBytes = 100, 100
	t.Cleanup(func() { skewSplitTargetBytes, skewSplitMinGroupBytes = ot, of })
	return []wdArm{
		{"single", single(0), nil},
		{"spilled512k", single(512 * 1024), nil},
		{"dag", runDAG(stand(nil)), nil},
		{"dag-shuffled", runDAG(stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })), nil},
		{"dag-morsel4", runDAG(stand(func(w *worker.Config) { w.MorselWorkers = 4 })), nil},
		{"dag-eager", runDAG(eager), nil},
		{"dag-skew", runDAG(stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1; c.SkewSplit = true })), nil},
		{"dag-aggsplit", runDAG(stand(nil, func(c *Config) { c.AggPartialSplit = true })), nil},
		{"fastpath", runDAG(stand(nil, func(c *Config) { c.LocalFastPathBytes = 64 << 20 })), nil},
		{"async", runAsync(asyncCoord(), false), nil},
		{"async-probesplit", runAsync(asyncCoord(), true), nil},
	}
}
