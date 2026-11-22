// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"bufio"
	"context"
	"fmt"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/benchmarks/tpch"
	"github.com/derekmwright/wadjet/internal/coordinator/dagplan"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// R0's TPC-H differential and cost baseline (epic #1678, ADR-0051).
//
// TPC-H SF0.01's 22 queries on the stage DAG and on localD (the DAG's own D
// run single-process), in REPORT mode: there is no PostgreSQL oracle in this
// table, so each query is classified DAG-versus-localD only — agree, differ,
// routed (the DAG arm answered from L: not DAG evidence) or localD not
// established (late subplan) — and the test asserts that classification equals
// testdata/r0_tpch_pins.tsv, not that differences are zero. Float values are
// compared to a relative 1e-9 (ADR-0013 classes 4 and 9: summation order).
//
// R0_TPCH_REPORT=1 writes the report as r0_tpch_report.tsv; R0_COST_WRITE=1
// writes the cost baseline rows (testdata/r0_cost_baseline.tsv's format, no
// header) as r0_cost_baseline_tpch.tsv, which is recorded, not asserted:
// planning time is wall time. Each goes to a fresh directory whose path the
// test logs (r0WriteArtifact; a writer takes no path).

// r0Rendered is an in-process result: column names, declared types, rows.
type r0Rendered struct {
	err   string
	cols  []string
	types []string
	rows  [][]string
}

// r0TaskIDs blanks the task ids a DAG failure's message names, which differ
// per run.
var r0TaskIDs = regexp.MustCompile(`task [0-9a-f]{8}`)

func r0Render(res *SQLResult, err error) r0Rendered {
	if err != nil {
		return r0Rendered{err: r0TaskIDs.ReplaceAllString(tbErr(err), "task ?")}
	}
	if res == nil {
		return r0Rendered{err: "nil result"}
	}
	if res.Error != "" {
		return r0Rendered{err: r0TaskIDs.ReplaceAllString("ERR (uncoded) "+res.Error, "task ?")}
	}
	var out r0Rendered
	for _, c := range res.OutputSchema() {
		out.cols = append(out.cols, c.Name)
		ty := fmt.Sprintf("%d", c.Type)
		if c.Type == parquet.TypeDecimal {
			ty += fmt.Sprintf("(%d,%d)", c.Precision, c.Scale)
		}
		out.types = append(out.types, ty)
	}
	add := func(vals []any) {
		row := make([]string, len(vals))
		for i, v := range vals {
			switch x := v.(type) {
			case nil:
				row[i] = "NULL"
			case float64:
				row[i] = "f:" + strconv.FormatFloat(x, 'g', -1, 64)
			case float32:
				row[i] = "f:" + strconv.FormatFloat(float64(x), 'g', -1, 32)
			default:
				row[i] = fmt.Sprint(x)
			}
		}
		out.rows = append(out.rows, row)
	}
	if st := res.Stream(); st != nil {
		defer st.Close()
		for {
			bb, berr := st.Next(context.Background())
			if berr != nil {
				return r0Rendered{err: tbErr(berr)}
			}
			if bb == nil {
				break
			}
			for _, r := range bb.ToRowValues() {
				add(r)
			}
		}
	} else {
		rows, rerr := res.Rows()
		if rerr != nil {
			return r0Rendered{err: tbErr(rerr)}
		}
		for _, r := range rows {
			vals := make([]any, len(out.cols))
			for j, c := range out.cols {
				vals[j] = r[c]
			}
			add(vals)
		}
	}
	return out
}

// r0SameRendered compares two in-process results as bags (ordered when
// ordered), floats to a relative 1e-9; "" means equal.
func r0SameRendered(a, b r0Rendered, ordered bool) string {
	if a.err != "" || b.err != "" {
		if a.err == b.err {
			return ""
		}
		return "error"
	}
	if strings.Join(a.cols, ",") != strings.Join(b.cols, ",") {
		return "names"
	}
	if strings.Join(a.types, ",") != strings.Join(b.types, ",") {
		return "types"
	}
	if len(a.rows) != len(b.rows) {
		return "rowcount"
	}
	key := func(r []string) string {
		k := make([]string, len(r))
		for i, v := range r {
			if strings.HasPrefix(v, "f:") {
				f, _ := strconv.ParseFloat(v[2:], 64)
				k[i] = strconv.FormatFloat(f, 'e', 6, 64)
			} else {
				k[i] = v
			}
		}
		return strings.Join(k, "|")
	}
	ar, br := append([][]string(nil), a.rows...), append([][]string(nil), b.rows...)
	if !ordered {
		sort.Slice(ar, func(i, j int) bool { return key(ar[i]) < key(ar[j]) })
		sort.Slice(br, func(i, j int) bool { return key(br[i]) < key(br[j]) })
	}
	for i := range ar {
		for j := range ar[i] {
			x, y := ar[i][j], br[i][j]
			if x == y {
				continue
			}
			if strings.HasPrefix(x, "f:") && strings.HasPrefix(y, "f:") {
				fx, _ := strconv.ParseFloat(x[2:], 64)
				fy, _ := strconv.ParseFloat(y[2:], 64)
				if math.Abs(fx-fy) <= 1e-9*math.Max(math.Abs(fx), math.Abs(fy)) {
					continue
				}
			}
			return "values"
		}
	}
	return ""
}

// r0TPCHClass classifies one TPC-H query DAG-versus-localD. A refusal both
// paths share is "refused-alike", never "agree" (TestR0TPCHCommonRefusalFails):
// two equal errors are not an agreement about an answer.
func r0TPCHClass(routed bool, status string, dag, ld r0Rendered, ordered bool) string {
	switch {
	case routed:
		return "routed"
	case status != "established":
		return "localD-not-established(" + status + ")"
	case dag.err != "" && dag.err == ld.err:
		return "refused-alike"
	case dag.err != "" || ld.err != "":
		return "differ(error)"
	case r0SameRendered(dag, ld, ordered) == "":
		return "agree"
	}
	return "differ(" + r0SameRendered(dag, ld, ordered) + ")"
}

// r0Cost is one statement's cost baseline from its stage graph.
func r0Cost(stages []dagplan.Stage) string {
	var exch, scans, files, tasks int
	var bytes int64
	types := map[string]int{}
	for _, s := range stages {
		types[s.Type]++
		if strings.HasPrefix(s.Type, "exchange") {
			exch++
		}
		if len(s.ScanFiles) > 0 || s.TableName != "" {
			scans++
		}
		files += len(s.ScanFiles)
		for _, b := range s.ScanFileSizes {
			bytes += b
		}
		tasks += s.Tasks
	}
	var tl []string
	for k, v := range types {
		tl = append(tl, fmt.Sprintf("%s=%d", k, v))
	}
	sort.Strings(tl)
	return fmt.Sprintf("stages=%d\texchanges=%d\tscan_stages=%d\tselected_files=%d\tselected_bytes=%d\tplanned_tasks=%d\tstage_types=%s",
		len(stages), exch, scans, files, bytes, tasks, strings.Join(tl, ","))
}

func r0ReadKV(t *testing.T, path string) map[string]string {
	t.Helper()
	out := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, _ := strings.Cut(line, "\t")
		out[k] = v
	}
	return out
}

func TestR0TPCHDifferential(t *testing.T) {
	r0RequireWriterUsage(t, "R0_TPCH_REPORT", "R0_COST_WRITE")
	if testing.Short() {
		t.Skip("-short: TPC-H SF0.01 on the DAG and on localD")
	}
	_, coord := setupTPCHDistributed(t)
	if coord.localFastPathBytes() != 0 {
		t.Fatalf("fast path on: %d", coord.localFastPathBytes())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	pins := r0ReadKV(t, "testdata/r0_tpch_pins.tsv")
	var qs []int
	for n := range tpch.TPCHQueries {
		qs = append(qs, n)
	}
	sort.Ints(qs)
	var report, cost []string
	counts := map[string]int{}
	for _, n := range qs {
		q := tpch.TPCHQueries[n]
		name := fmt.Sprintf("Q%02d", n)
		before, _ := r0RouteSum(coord)
		dres, derr := coord.ExecuteSQL(ctx, q.SQL)
		after, routes := r0RouteSum(coord)
		dag := r0Render(dres, derr)
		lres, rec, lerr := r0LocalD(coord, ctx, q.SQL)
		ld := r0Render(lres, lerr)
		ordered := strings.Contains(strings.ToUpper(q.SQL), "ORDER BY")
		class := r0TPCHClass(after != before, rec.Status, dag, ld, ordered)
		counts[strings.SplitN(class, "(", 2)[0]]++
		cmp := r0SameRendered(dag, ld, ordered)
		if cmp == "" {
			cmp = "equal"
		}
		// cmp is reported even where the class says the comparison is not
		// evidence (routed, late subplan).
		note := fmt.Sprintf("dag_rows=%d localD_rows=%d answers=%s", len(dag.rows), len(ld.rows), cmp)
		if routes != "" && after != before {
			note += " routes=" + routes
		}
		if rec.LateSubplan != "" {
			note += " late=" + strings.ReplaceAll(rec.LateSubplan, " ", "_")
		}
		got := class
		if want, ok := pins[name]; !ok {
			t.Errorf("%s: no pin (measured %s)", name, got)
		} else if want != got {
			t.Errorf("%s: classified %s, pinned %s (%s)", name, got, want, note)
		}
		report = append(report, fmt.Sprintf("tpch\t%s\t%s\t%s\tdag_err=%q localD_err=%q", name, class, note, dag.err, ld.err))
		cost = append(cost, fmt.Sprintf("tpch\t%s\t%s\tplan_ms=%.2f\tstage_plan_ms=%.2f", name, r0Cost(rec.Stages),
			float64(rec.PlanTime.Microseconds())/1000, float64(rec.StagePlanTime.Microseconds())/1000))
	}
	t.Logf("TPC-H SF0.01: %v of %d", counts, len(qs))
	r0WriteArtifact(t, "R0_TPCH_REPORT", "r0_tpch_report.tsv", strings.Join(report, "\n")+"\n")
	r0WriteArtifact(t, "R0_COST_WRITE", "r0_cost_baseline_tpch.tsv", strings.Join(cost, "\n")+"\n")
}

// TestR0CorpusCostBaseline writes the corpus statements' cost rows
// (R0_COST_WRITE=1: r0_cost_baseline_corpus.tsv) from the stage graph localD captures on the dag arm's
// configuration. Recorded, not asserted.
func TestR0CorpusCostBaseline(t *testing.T) {
	r0RequireWriterUsage(t, "R0_COST_WRITE")
	if !r0Writer(t, "R0_COST_WRITE") || testing.Short() {
		t.Skip("R0_COST_WRITE not 1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	cells := r0ReadCells(t)
	var rows []string
	for _, fx := range []string{"own", "uniq", "dup"} {
		c := r0Cluster(t, ctx, fx, nil)
		for _, cell := range cells {
			if !containsString(cell.fixtures, fx) {
				continue
			}
			_, rec, _ := r0LocalD(c, ctx, cell.sql)
			if rec.Frozen == nil {
				rows = append(rows, fmt.Sprintf("corpus/%s\t%s\tnot planned (%s)", fx, cell.name, rec.Status))
				continue
			}
			rows = append(rows, fmt.Sprintf("corpus/%s\t%s\t%s\tplan_ms=%.2f\tstage_plan_ms=%.2f", fx, cell.name, r0Cost(rec.Stages),
				float64(rec.PlanTime.Microseconds())/1000, float64(rec.StagePlanTime.Microseconds())/1000))
		}
	}
	r0WriteArtifact(t, "R0_COST_WRITE", "r0_cost_baseline_corpus.tsv", strings.Join(rows, "\n")+"\n")
}

// TestR0PartitionPruningThroughDerivedAlias keeps the judge's probe 5 as a
// test (ADR-0051 RR9): a table partitioned by year, ingested as the judge did
// (one file per partition), and a predicate on a DERIVED ALIAS of the
// partition column prunes the coordinator's scan-file list from both files to
// the 2026 file before dispatch. PostgreSQL 17.11 answers 3,60 unfiltered and
// 2,50 filtered (measured for R0: 1678-r0_evidence/partition_pg17.txt).
func TestR0PartitionPruningThroughDerivedAlias(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: stands a cluster up")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	infra := tmdInfra(t, ctx)
	db, err := wadjet.Open(ctx, wadjet.Config{Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "year", Type: parquet.TypeString}, {Name: "v", Type: parquet.TypeInt64}}}
	if err := db.CreateTable(ctx, "r0_parts", schema, []string{"year"}); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("r0_parts", schema, []string{"year"}, ingest.Config{MaxBufferRows: 100, RowGroupSize: 4096})
	if err := ing.Ingest(ctx, []map[string]any{{"year": "2025", "v": int64(10)}, {"year": "2026", "v": int64(20)}, {"year": "2026", "v": int64(30)}}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	c := tmdCoordinator(t, ctx, infra)
	if c.localFastPathBytes() != 0 {
		t.Fatal("fast path on")
	}
	probe := func(sql string) (files int, bytes int64, answer string) {
		_, rec, err := r0LocalD(c, ctx, sql)
		if err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
		for _, s := range rec.Stages {
			if s.TableName == "r0_parts" {
				files += len(s.ScanFiles)
				for _, b := range s.ScanFileSizes {
					bytes += b
				}
			}
		}
		res, err := c.ExecuteSQL(ctx, sql)
		r := r0Render(res, err)
		if r.err != "" {
			t.Fatalf("%s: %s", sql, r.err)
		}
		var rows []string
		for _, row := range r.rows {
			rows = append(rows, strings.Join(row, ","))
		}
		return files, bytes, strings.Join(rows, ";")
	}
	all, allBytes, allAns := probe("SELECT count(*),sum(v) FROM r0_parts")
	pruned, prunedBytes, prunedAns := probe("SELECT count(*),sum(v) FROM (SELECT year AS y,v FROM r0_parts) s WHERE y='2026'")
	t.Logf("unfiltered: %d files / %d bytes, %s; derived alias: %d files / %d bytes, %s", all, allBytes, allAns, pruned, prunedBytes, prunedAns)
	if all != 2 || allAns != "3,60" {
		t.Fatalf("unfiltered scan selected %d files answering %s, want 2 files answering 3,60", all, allAns)
	}
	if pruned != 1 || prunedAns != "2,50" {
		t.Fatalf("derived-alias predicate selected %d files answering %s, want 1 file answering 2,50 (PostgreSQL)", pruned, prunedAns)
	}
}
