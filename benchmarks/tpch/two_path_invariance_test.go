// Package tpch's two-path invariance gate gives one statement one answer.
//
// The coordinator answers a query on one of two engines: the small-query
// local fast path (in-process pipeline, taken when post-pruning scan bytes
// stay under LocalFastPathBytes) or the distributed stage DAG. Routing
// between them is invisible to the client and depends on data size, so a
// defect on either engine alone is a query that returns different rows
// depending on how much data happens to sit behind it.
//
// That is not hypothetical. A bare `LIMIT n` (no ORDER BY) was ignored
// outright on the DAG — `SELECT x FROM t LIMIT 3` returned every row (fixed
// in c5e77cf, regression test in internal/coordinator/bare_limit_e2e_test.go).
// It hid because small scans take the fast path, which bounds limits
// correctly, and because all five TPC-H corpus queries with a LIMIT also
// carry an ORDER BY — so the benchmark suite only ever exercised the working
// shape. Nothing asserted that the two paths agree.
//
// TestTwoPathInvariance is that assertion: the TPC-H corpus plus the shapes
// the corpus lacks, every one run through both engines against the same
// catalog and data, results required to match.
//
// The gate is RED on arrival. Four divergences it found on first run, none
// of which any existing suite catches — row counts are identical in all
// four, and the harness value-signature gate is blind to three of them
// (order-insensitive, and string columns contribute nothing):
//
//	Q05, CrossTableEqualityFilter — the stage DAG drops a WHERE equality
//	between two joined tables that is not itself a join condition
//	(`c_nationkey = s_nationkey`), answering with exactly the result the
//	predicate-free query produces: 60000 rows joined instead of 2450, Q05
//	revenues ~25× inflated. baseline-local-small.json records q05
//	value_sig c1:8.056844756e+07, which is the inflated sum to the digit —
//	the wrong answer has been the recorded distributed baseline since
//	2026-08-06, while DuckDB validated only the row count (5 either way).
//
//	Q09, AliasedGroupKeyOrderBy — the stage DAG drops ORDER BY when the
//	SELECT list renames the grouped column. `... o_orderpriority AS p ...
//	ORDER BY p` comes back unsorted; the same query without `AS p`
//	(GroupKeyOrderBy) sorts correctly on both paths. Rows and values match;
//	only the sequence is lost.
//
//	Q07 — the stage DAG returns NULL for the first alias of a self-joined
//	table (`n1.n_name AS supp_nation`, with n2.n_name populated). FIXED
//	(#314): a join qualifies only its BUILD side's colliding columns, so the
//	alias that lands on the probe side ships under the bare name and every
//	DAG-side lookup of the qualified one — the gather's SELECT-list rename
//	and the worker's pre-aggregate projection — missed, because those two
//	were the only name resolutions in the engine not going through
//	columnIndexFallback's qualified↔bare fallback.
//
//	Q07 tripped BOTH open defects at once, which is why it is the corpus's
//	only entry that needed two fixes to go green: #314 for its VALUES, and
//	#313 for its row SEQUENCE — it orders by three SELECT-list aliases
//	(supp_nation, cust_nation, l_year), and until #313 the DAG produced no
//	stage emitting those names, so all three sort keys resolved to nothing.
//	Q07_noorder below holds the VALUES half on its own, so #314 stays gated
//	whatever happens to the ordering.
//
//	StarPlusColumnFull — the single-process path (local fast path AND the
//	embedded engine) NULLs every star-expanded column when the select list
//	mixes a star with a named column: `SELECT nation.*, n_name FROM nation`
//	returns values only for n_name. `SELECT nation.*` alone (StarOnly) is
//	correct, and the DAG is correct in both shapes. This is the one
//	divergence where arm A is the wrong side.
//
// Each of those subtests names the arm and the first differing row. Fixing
// a bug turns its subtest green; nothing here should be loosened to make
// the suite pass.
package tpch

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// twoPathCmp selects how strictly the two arms' results are compared.
type twoPathCmp int

const (
	// cmpUnordered: no top-level ORDER BY, so row order carries no meaning.
	// Both sides are canonicalized and sorted before comparing — a path that
	// emits the same rows in a different order is correct.
	cmpUnordered twoPathCmp = iota
	// cmpOrdered: a top-level ORDER BY makes the row sequence part of the
	// answer, so rows are compared positionally.
	cmpOrdered
	// cmpCount: compare row counts only. Two cases need it. (1) A LIMIT
	// truncates at a boundary SQL does not disambiguate: a bare LIMIT lets
	// the engine return ANY n rows, and even with an ORDER BY the rows tied
	// at the cut are interchangeable — so which rows come back is genuinely
	// non-deterministic between two correct engines, while the row COUNT is
	// not. Running the verbatim LIMIT query still proves the limit binds on
	// both paths (the bare-LIMIT bug was exactly a count divergence: 25 vs
	// 3). The stripped form of the same query is enqueued separately for the
	// full row-level compare. (2) Q02/Q22 select on a float threshold, so
	// borderline rows shift in and out with accumulation order between two
	// correct runs — the same tolerance TestTPCHQueries and the optimization
	// oracle already apply.
	cmpCount
)

// twoPathQuery is one corpus entry.
type twoPathQuery struct {
	name string
	sql  string
	cmp  twoPathCmp
	// knownBug names the open issue for a divergence this suite found and
	// that is not yet fixed. The case is skipped rather than deleted or
	// loosened: the assertion stays exactly as written, so removing the
	// field is the whole of "the fix landed". Empty for every other case.
	knownBug string
	// limit, cmpCount only: the trailing LIMIT n both arms must respect
	// (0 when the entry is count-compared for another reason).
	limit int
	// tolerance, cmpCount only: allowed difference in row count.
	tolerance int
	// expectRows: the query is known to match data at SF0.01, so an empty
	// result is a failure. Off for the TPC-H entries — Q18 legitimately
	// returns 0 rows at this scale.
	expectRows bool
	// localRoute: arm B must answer this query via the #359 route — the
	// stage DAG refuses a per-row correlated subquery
	// (physical.ErrCorrelatedSubqueryDistributed) and the coordinator runs
	// it on its local single-process pipeline. The runner asserts the route
	// engaged for these entries AND for no others: an over-broad refusal
	// silently downgrades distributed queries to single-process, and
	// nothing else in this suite would notice (the answers stay right).
	localRoute bool
	// assertA is an ABSOLUTE assertion on arm A's rows, run in addition to
	// the two-arm compare. The compare's contract is "the paths agree", not
	// "the answer is right" — a defect that hits both engines the same way
	// passes it while both arms are wrong. #320 was exactly that: an ORDER BY
	// over an expression was ignored on the single-process pipeline AND on the
	// stage DAG, so the arms agreed on an arbitrary order. Every entry whose
	// bug could be symmetric carries one of these.
	assertA func(tb testing.TB, rows []map[string]any)
	// wantCols is the exact column list BOTH arms must return, and wantRows
	// the exact row count (0 = unasserted). The two-arm compare cannot stand
	// in for either: it realigns arm B onto arm A's columns, so an arm B
	// carrying EXTRA columns passes it, and a count both arms get wrong the
	// same way passes it too. #346 needed both halves — the DAG returned one
	// arm of a union, which is the wrong row count AND that table's full
	// width, and the earlier pinning of it nearly retired itself because a
	// two-row wrong answer happened to carry the right two values.
	wantCols []string
	wantRows int
}

// assertOrderedBy checks that rows really are in the order the query asked
// for, by recomputing the sort key from the columns the result carries. The
// key is often NOT one of those columns — an ORDER BY expression is dropped
// from the output on purpose — so each caller supplies the derivation.
func assertOrderedBy[T cmp.Ordered](tb testing.TB, rows []map[string]any, desc bool, label string, keyOf func(map[string]any) T) {
	tb.Helper()
	if len(rows) < 2 {
		tb.Errorf("arm A (single-process) returned %d rows — too few for the row sequence to prove anything about %s", len(rows), label)
		return
	}
	if i, ok := firstOutOfOrder(rows, desc, keyOf); !ok {
		dir := "ascending"
		if desc {
			dir = "descending"
		}
		tb.Errorf("arm A (single-process) is not in %s order by %s: row %d has key %v, row %d has %v\n"+
			"  the ORDER BY was not honoured — rows came back in an arbitrary sequence",
			dir, label, i-1, keyOf(rows[i-1]), i, keyOf(rows[i]))
	}
}

// firstOutOfOrder reports the first row that breaks the ordering, or ok=true
// when the whole sequence holds.
func firstOutOfOrder[T cmp.Ordered](rows []map[string]any, desc bool, keyOf func(map[string]any) T) (int, bool) {
	for i := 1; i < len(rows); i++ {
		prev, cur := keyOf(rows[i-1]), keyOf(rows[i])
		if (!desc && cur < prev) || (desc && cur > prev) {
			return i, false
		}
	}
	return 0, true
}

// cellText renders a result cell as a string for key derivation.
func cellText(row map[string]any, col string) string {
	v, ok := lookupCell(row, col)
	if !ok {
		return ""
	}
	return fmt.Sprint(v)
}

// cellNum renders a result cell as a float for key derivation. Reports 0 for
// a cell that is missing or non-numeric, which shows up as a flat key and
// fails the ordering assertion rather than passing it vacuously.
func cellNum(row map[string]any, col string) float64 {
	v, ok := lookupCell(row, col)
	if !ok {
		return 0
	}
	switch n := v.(type) {
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	case float32:
		return float64(n)
	case float64:
		return n
	}
	f, err := strconv.ParseFloat(fmt.Sprint(v), 64)
	if err != nil {
		return 0
	}
	return f
}

// The variance-family reference (#339). Nothing below reads a constant: the
// expected value is a Welford pass computed here, in the test, over the very
// rows the fixture generator produced for the arm under test. A hand-copied
// number would have to be re-derived whenever the fixture changes, and the
// original defect was precisely a plausible-looking number.

// sf001Fixture is the deterministic SF0.01 fixture the two-path cluster is
// loaded from (Generate seeds off the scale factor), materialized once.
var sf001Fixture = sync.OnceValue(func() map[string][]map[string]any { return Generate(SF001) })

func sf001Table(tb testing.TB, table string) []map[string]any {
	tb.Helper()
	rows := sf001Fixture()[table]
	if len(rows) == 0 {
		tb.Fatalf("fixture has no rows for table %q", table)
	}
	return rows
}

// sf001Column pulls one numeric column out of the fixture.
func sf001Column(tb testing.TB, table, col string) []float64 {
	tb.Helper()
	rows := sf001Table(tb, table)
	out := make([]float64, 0, len(rows))
	for _, r := range rows {
		v, ok := r[col]
		if !ok {
			tb.Fatalf("fixture table %q has no column %q", table, col)
		}
		out = append(out, toFloat(v))
	}
	return out
}

func toFloat(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int64:
		return float64(n)
	case int32:
		return float64(n)
	case int:
		return float64(n)
	}
	f, _ := strconv.ParseFloat(fmt.Sprint(v), 64)
	return f
}

// varSamp / varPop select the divisor: n-1 for the sample form (what
// STDDEV and VARIANCE mean), n for the population form.
const (
	varSamp = true
	varPop  = false
)

// welfordVariance is the reference accumulator: Welford's online algorithm,
// one sequential pass, no partials and therefore no merge to get wrong.
func welfordVariance(vals []float64, sample bool) float64 {
	var n int64
	var mean, m2 float64
	for _, x := range vals {
		n++
		d := x - mean
		mean += d / float64(n)
		m2 += d * (x - mean)
	}
	if sample {
		if n < 2 {
			return math.NaN()
		}
		return m2 / float64(n-1)
	}
	if n == 0 {
		return math.NaN()
	}
	return m2 / float64(n)
}

func identityFloat(f float64) float64 { return f }

// assertVarianceValue checks one result cell against the reference pass over
// the fixture column. finish is math.Sqrt for a STDDEV and identityFloat for
// a VARIANCE.
func assertVarianceValue(tb testing.TB, rows []map[string]any, col string, sample bool,
	finish func(float64) float64, table, fixtureCol string) {
	tb.Helper()
	if len(rows) != 1 {
		tb.Fatalf("%d rows, want 1", len(rows))
	}
	vals := sf001Column(tb, table, fixtureCol)
	want := finish(welfordVariance(vals, sample))
	got := cellNum(rows[0], col)
	if rel := math.Abs(got-want) / math.Abs(want); rel > 1e-9 {
		form := "sample"
		if !sample {
			form = "population"
		}
		tb.Errorf("%s = %.17g, want %.17g (%s, Welford over the %d fixture values of %s.%s; rel err %g)\n"+
			"  a merge that keeps only one partial's state lands a few tenths of a percent off",
			col, got, want, form, len(vals), table, fixtureCol, rel)
	}
}

// nationOfComment recovers a nation's name from the fixture's n_comment
// ("Nation ALGERIA comment", datagen.go). It lets a query ORDER BY n_name
// without selecting it and still be checked absolutely.
func nationOfComment(row map[string]any) string {
	c := cellText(row, "n_comment")
	c = strings.TrimPrefix(c, "Nation ")
	return strings.TrimSuffix(c, " comment")
}

// TestTwoPathInvariance runs every corpus query through both coordinator
// execution paths — arm A on the single-process engine (the local fast
// path, or the embedded engine for the queries the fast path declines to
// route), arm B on the distributed stage DAG — against one shared catalog,
// object store, and worker pool, and requires identical results. Both arms
// consume the identical optimized logical plan, so any divergence is a
// routing or execution bug, and the failure message names the query, the
// arm, and the first differing row.
func TestTwoPathInvariance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	fast, dag := setupTwoPathCluster(t, ctx)
	// Stand-in for arm A on queries the fast path declines to route (see
	// below): the embedded engine is the same physical planner and
	// single-process pipeline the fast path runs, over the same
	// deterministic SF0.01 fixture.
	embedded := setupTPCH(t, SF001)
	corpus := twoPathCorpus()
	t.Logf("two-path invariance: %d queries × 2 arms (A: single-process, B: stage DAG)", len(corpus))

	for _, q := range corpus {
		t.Run(q.name, func(t *testing.T) {
			if q.knownBug != "" {
				t.Skipf("known divergence, tracked in %s — assertion left intact; "+
					"delete the knownBug field when the fix lands", q.knownBug)
			}
			hitsBefore := fast.LocalFastPathHits()
			localRows, localCols, localErr := runArm(t, ctx, fast, q.sql)
			routesBefore := dag.CorrelatedLocalRoutes()
			dagRows, dagCols, dagErr := runArm(t, ctx, dag, q.sql)

			// The #359 route must engage exactly for the entries that
			// declare it. A correlated query running the stage DAG is the
			// old silent-0 defect; a NON-correlated query taking the route
			// is an over-broad refusal downgrading distributed execution to
			// single-process — correct answers, so only this counter sees
			// it.
			routed := dag.CorrelatedLocalRoutes() - routesBefore
			if q.localRoute && routed == 0 {
				t.Errorf("arm B ran the stage DAG for a per-row correlated subquery — the #359 refusal did not fire")
			}
			if !q.localRoute && routed != 0 {
				t.Errorf("arm B routed this query to the coordinator-local pipeline — the #359 refusal fired for a shape the stage DAG can run")
			}

			// The fast path declines any plan it cannot size-estimate —
			// today that is every plan still carrying a subquery predicate
			// (Q11/Q15/Q22). The coordinator then answers on the DAG, so
			// what arm A just returned IS the DAG's answer and comparing
			// it would be vacuous. Re-run arm A on the embedded
			// single-process engine instead: same engine the fast path
			// uses, so the invariant under test is unchanged and the
			// corpus stays fully covered.
			//
			// A fast-path plan or execution FAILURE lands here too (it
			// also falls back silently — #308), which is why the subtest
			// logs the substitution rather than passing quietly.
			if fast.LocalFastPathHits()-hitsBefore != 1 && localErr == nil {
				t.Logf("local fast path declined this query; arm A re-run on the embedded single-process engine")
				res, err := embedded.Query(ctx, q.sql)
				if err != nil {
					localRows, localCols, localErr = nil, nil, err
				} else {
					localRows, localCols, localErr = res.Rows, res.Columns, nil
				}
			}

			// A query that answers on one path and errors on the other is
			// the sharpest form of the divergence this test exists to
			// catch.
			switch {
			case localErr != nil && dagErr != nil:
				t.Fatalf("both arms failed\n  SQL: %s\n  A (single-process): %v\n  B (stage DAG): %v", q.sql, localErr, dagErr)
			case localErr != nil:
				t.Fatalf("arm A (single-process) failed while arm B (stage DAG) returned %d rows\n  SQL: %s\n  error: %v", len(dagRows), q.sql, localErr)
			case dagErr != nil:
				t.Fatalf("arm B (stage DAG) failed while arm A (single-process) returned %d rows\n  SQL: %s\n  error: %v", len(localRows), q.sql, dagErr)
			}

			compareArms(t, q, localRows, localCols, dagRows, dagCols)
			if q.assertA != nil {
				q.assertA(t, localRows)
			}
		})
	}

	// Engagement checks: neither arm may be the other engine in disguise.
	if n := dag.LocalFastPathHits(); n != 0 {
		t.Errorf("arm B took the local fast path %d times — LocalFastPathBytes=0 must force the stage DAG", n)
	}
	if n := fast.LocalFastPathHits(); n == 0 {
		t.Error("no query took the local fast path — arm A never exercised it, so this gate proved nothing")
	} else {
		t.Logf("local fast path served %d of %d arm-A queries; the rest ran on the embedded engine", n, len(corpus))
	}
}

// compareArms asserts arm A (single-process) and arm B (stage DAG) agree,
// at the strictness the query's shape allows.
func compareArms(t *testing.T, q twoPathQuery, localRows []map[string]any, localCols []string, dagRows []map[string]any, dagCols []string) {
	t.Helper()

	// A materialized ORDER BY term (#320) is the planner's own column: it is
	// computed, sorted on, and must be gone before the client sees the result.
	// Both engines drop it in a different place — the gather's projection on
	// arm B, a trim operator on arm A — so both are checked.
	for arm, cols := range map[string][]string{"A (single-process)": localCols, "B (stage DAG)": dagCols} {
		for _, c := range cols {
			if strings.HasPrefix(c, "__sortkey_") {
				t.Errorf("arm %s returned the planner's materialized sort column %q; the client asked for %v\n  SQL: %s",
					arm, c, localCols, q.sql)
			}
		}
		if len(q.wantCols) > 0 && !sameColumns(cols, q.wantCols) {
			t.Errorf("arm %s returned columns %v, the query selects %v\n  SQL: %s", arm, cols, q.wantCols, q.sql)
		}
	}
	if q.wantRows > 0 {
		for arm, rows := range map[string]int{"A (single-process)": len(localRows), "B (stage DAG)": len(dagRows)} {
			if rows != q.wantRows {
				t.Errorf("arm %s returned %d rows, want %d\n  SQL: %s", arm, rows, q.wantRows, q.sql)
			}
		}
	}

	if q.cmp == cmpCount {
		if diff := len(dagRows) - len(localRows); diff < -q.tolerance || diff > q.tolerance {
			t.Errorf("row count differs: A (single-process) %d, B (stage DAG) %d (±%d allowed)\n  SQL: %s",
				len(localRows), len(dagRows), q.tolerance, q.sql)
		}
		if q.limit > 0 {
			if len(localRows) > q.limit {
				t.Errorf("arm A (single-process) returned %d rows for LIMIT %d\n  SQL: %s", len(localRows), q.limit, q.sql)
			}
			if len(dagRows) > q.limit {
				t.Errorf("arm B (stage DAG) returned %d rows for LIMIT %d\n  SQL: %s", len(dagRows), q.limit, q.sql)
			}
		}
		if q.expectRows && (len(localRows) == 0 || len(dagRows) == 0) {
			t.Errorf("empty result (A %d rows, B %d rows) — the limit must bound the result, not empty it\n  SQL: %s",
				len(localRows), len(dagRows), q.sql)
		}
		return
	}

	// Render both arms against arm A's column list so a divergence is
	// reported in the columns the client asked for. realign resolves arm
	// B's keys onto those names; a column arm B does not carry at all is a
	// failure, not a silently-null cell, or the comparison would pass
	// vacuously.
	aligned, missing := realign(dagRows, localCols)
	if missing != "" {
		t.Errorf("arm B (stage DAG) has no column %q (A: %v, B: %v)\n  SQL: %s", missing, localCols, dagCols, q.sql)
		return
	}
	a := &oracle.Result{Columns: localCols, Rows: localRows}
	b := &oracle.Result{Columns: localCols, Rows: aligned}

	var canonA, canonB *oracle.Canon
	if q.cmp == cmpOrdered {
		canonA, canonB = oracle.CanonicalizeOrdered(a), oracle.CanonicalizeOrdered(b)
	} else {
		canonA, canonB = oracle.Canonicalize(a), oracle.Canonicalize(b)
	}
	if diff := canonA.Diff(canonB, oracle.Query{Name: q.name}); diff != "" {
		t.Errorf("arms disagree (baseline = A, single-process; got = B, stage DAG)\n  SQL: %s\n%s", q.sql, diff)
	}
}

// sameColumns compares a result's column list against the one the query
// selects, case-insensitively and allowing the DAG's table-qualified
// spelling ("nation.n_name" for "n_name"). Order and count must match: an
// extra column is a planner artefact reaching the client.
func sameColumns(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		g, w := strings.ToLower(got[i]), strings.ToLower(want[i])
		if g == w {
			continue
		}
		if dot := strings.LastIndexByte(g, '.'); dot >= 0 && g[dot+1:] == w {
			continue
		}
		return false
	}
	return true
}

// realign re-keys rows onto the requested column names. The two paths
// qualify join output columns differently (arm B's gather can carry the
// table-qualified form), so a lookup falls back to the unqualified suffix in
// both directions. Returns the first column it cannot resolve.
func realign(rows []map[string]any, cols []string) ([]map[string]any, string) {
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		aligned := make(map[string]any, len(cols))
		for _, col := range cols {
			v, ok := lookupCell(row, col)
			if !ok {
				return nil, col
			}
			aligned[col] = v
		}
		out[i] = aligned
	}
	return out, ""
}

func lookupCell(row map[string]any, col string) (any, bool) {
	if v, ok := row[col]; ok {
		return v, true
	}
	if dot := strings.LastIndexByte(col, '.'); dot >= 0 {
		if v, ok := row[col[dot+1:]]; ok {
			return v, true
		}
	}
	for k, v := range row {
		if dot := strings.LastIndexByte(k, '.'); dot >= 0 && k[dot+1:] == col {
			return v, true
		}
	}
	return nil, false
}

// runArm executes one query on one coordinator and materializes its rows.
func runArm(tb testing.TB, ctx context.Context, c *coordinator.Coordinator, sql string) ([]map[string]any, []string, error) {
	tb.Helper()
	res, err := c.ExecuteSQL(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	defer res.Close()
	if res.Error != "" {
		return nil, nil, errors.New(res.Error)
	}
	rows, err := res.Rows()
	if err != nil {
		return nil, nil, fmt.Errorf("materializing rows: %w", err)
	}
	return rows, res.Columns, nil
}

var (
	trailingLimitRe = regexp.MustCompile(`(?i)\s+LIMIT\s+(\d+)\s*;?\s*$`)
	orderByRe       = regexp.MustCompile(`(?i)ORDER\s+BY`)
)

// trailingLimit returns the query's top-level LIMIT, or 0.
func trailingLimit(sql string) int {
	m := trailingLimitRe.FindStringSubmatch(sql)
	if m == nil {
		return 0
	}
	n, err := strconv.Atoi(m[1])
	if err != nil {
		return 0
	}
	return n
}

// hasTopLevelOrderBy reports whether the statement has an ORDER BY of its
// own: one at paren depth 0. A subquery's sits inside the parens that close
// it, and text inside a string literal is not SQL at all.
//
// The decision matters beyond this suite — the DuckDB ground-truth gate uses
// it to choose an order-sensitive fingerprint — so it is decided by
// structure and not by "is there a ')' after it": `ORDER BY LENGTH(n_name)`
// is a top-level ORDER BY whose own parens would fool that test into calling
// the row sequence meaningless, which is exactly the blind spot #320 shipped
// through.
func hasTopLevelOrderBy(sql string) bool {
	depth := parenDepths(sql)
	for _, loc := range orderByRe.FindAllStringIndex(sql, -1) {
		if depth[loc[0]] == 0 {
			return true
		}
	}
	return false
}

// parenDepths returns each byte's paren nesting depth, with bytes inside a
// single-quoted string literal marked -1 so neither their parens nor their
// text count as SQL.
func parenDepths(sql string) []int {
	out := make([]int, len(sql))
	depth := 0
	inString := false
	for i := 0; i < len(sql); i++ {
		c := sql[i]
		if inString {
			out[i] = -1
			if c == '\'' {
				inString = false
			}
			continue
		}
		switch c {
		case '\'':
			inString = true
			out[i] = -1
			continue
		case '(':
			depth++
		case ')':
			depth--
		}
		out[i] = depth
	}
	return out
}

// twoPathCorpus is the TPC-H corpus plus the shapes it lacks.
func twoPathCorpus() []twoPathQuery {
	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)

	out := make([]twoPathQuery, 0, len(nums)+8)
	for _, n := range nums {
		name := fmt.Sprintf("Q%02d", n)
		sql := TPCHQueries[n].SQL
		cmp := cmpUnordered
		if hasTopLevelOrderBy(sql) {
			cmp = cmpOrdered
		}
		// Q02/Q22 pick rows by comparing against a float aggregate, so
		// membership at the threshold legitimately shifts between two
		// correct runs (same relaxation as TestTPCHQueries and
		// TestTPCHOptimizationInvariance).
		tolerance := 0
		if n == 2 || n == 22 {
			cmp, tolerance = cmpCount, 4
		}
		// Corpus queries whose two paths are known to disagree today. Each
		// is a real defect with an open issue, minimally reproduced by a
		// hand-written case above; the corpus entry stays here so the fix
		// is proven against the actual TPC-H query, not only the reduction.
		knownBug := map[int]string{
			// Empty: #312 (dropped join predicate), #313 (aliased ORDER BY),
			// #314 (self-joined alias) and #315 (star expansion) are all
			// fixed. Add an entry only alongside an open issue, and delete it
			// in the commit that fixes it.
		}[n]
		if lim := trailingLimit(sql); lim > 0 {
			// Full-row compare on the stripped form (stronger and
			// tie-immune) plus a count compare on the verbatim form, which
			// is what pins the limit itself on both paths.
			stripped := strings.TrimRight(trailingLimitRe.ReplaceAllString(sql, ""), " \t\n")
			out = append(out,
				twoPathQuery{name: name + "_nolimit", sql: stripped, cmp: cmp, tolerance: tolerance, knownBug: knownBug},
				twoPathQuery{name: name, sql: sql, cmp: cmpCount, limit: lim, tolerance: tolerance, knownBug: knownBug})
			continue
		}
		out = append(out, twoPathQuery{name: name, sql: sql, cmp: cmp, tolerance: tolerance, knownBug: knownBug})
	}

	// Shapes the TPC-H corpus does not contain. Every LIMIT in TPC-H sits
	// under an ORDER BY, which is why a dropped bare LIMIT survived 22
	// queries of coverage; the rest are the projection and name-resolution
	// forms that broke alongside it. All stay cheap at SF0.01.
	out = append(out,
		// Bare LIMIT, the c5e77cf bug: no ORDER BY, so nothing in the plan
		// but the limit itself bounds the result.
		twoPathQuery{name: "BareLimit", sql: "SELECT l_orderkey FROM lineitem LIMIT 7", cmp: cmpCount, limit: 7, expectRows: true},
		// Bare LIMIT under a filter: the limit must survive predicate
		// pushdown rewriting the scan.
		twoPathQuery{name: "BareLimitFilter", sql: "SELECT o_orderkey, o_totalprice FROM orders WHERE o_orderstatus = 'O' LIMIT 5", cmp: cmpCount, limit: 5, expectRows: true},
		// Star sharing the select list with a named column: both paths must
		// expand the star to the same column set in the same place. Run
		// twice — bounded, and unbounded so the expansion is compared cell
		// by cell rather than just counted.
		twoPathQuery{name: "StarPlusColumn", sql: "SELECT nation.*, n_name FROM nation LIMIT 4", cmp: cmpCount, limit: 4, expectRows: true},
		twoPathQuery{name: "StarPlusColumnFull", sql: "SELECT nation.*, n_name FROM nation", cmp: cmpUnordered},
		// Schema-qualified table name: name resolution runs separately per
		// path, so `public.nation` must bind on both. No LIMIT — this one
		// is compared row for row.
		twoPathQuery{name: "SchemaQualified", sql: "SELECT n_nationkey, n_name, n_regionkey FROM public.nation", cmp: cmpUnordered},
		// Scalar function over a string column, bare LIMIT: the projection
		// is computed in the scan fragment on the DAG and in the pipeline
		// locally.
		twoPathQuery{name: "LengthProjection", sql: "SELECT LENGTH(c_comment) AS comment_len FROM customer LIMIT 10", cmp: cmpCount, limit: 10, expectRows: true},
		// Aggregate under a bare LIMIT: the limit applies above a pipeline
		// breaker, which is a different attachment point than the scan.
		twoPathQuery{name: "AggregateBareLimit", sql: "SELECT l_returnflag, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag LIMIT 2", cmp: cmpCount, limit: 2, expectRows: true},
		// Star with no companion column — the control for StarPlusColumn
		// above. This shape agrees; adding one named column does not.
		twoPathQuery{name: "StarOnly", sql: "SELECT nation.* FROM nation", cmp: cmpUnordered},
		// Minimal repro pair for the ORDER BY divergence Q09 trips: the
		// only difference between these two is `AS p` on the grouped
		// column. Without the rename both paths sort; with it the DAG
		// returns the rows unsorted.
		twoPathQuery{name: "GroupKeyOrderBy", sql: "SELECT o_orderpriority, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY o_orderpriority", cmp: cmpOrdered},
		// The aggregate-free sibling of AliasedGroupKeyOrderBy. #313's fix
		// resolves an aliased sort key against the aggregate below it, which
		// is what makes it decidable at walkStages time; with no aggregate
		// the correct spelling depends on attachScanSelectProjections, which
		// runs later — and used to fire only for a SELECT list carrying an
		// expression, so this shape came back unsorted (#316). Adding an
		// expression made it correct by accident, which is the clue and the
		// control immediately below.
		twoPathQuery{name: "AliasedSortNoAggregate", sql: "SELECT o_orderpriority AS p FROM orders ORDER BY p", cmp: cmpOrdered},
		// The alias shadows another SELECT item's source column, so the sort
		// key names a column that exists in the scan's input under a
		// different meaning. Materializing the alias must also settle the
		// gather's rename: with both spellings in play, a stale source→alias
		// pair re-renames the column the fragment already renamed and the
		// result comes back with two columns named "c".
		twoPathQuery{name: "AliasedSortShadowsColumn", sql: "SELECT n_name AS n_comment, n_comment AS c FROM nation ORDER BY n_comment", cmp: cmpOrdered},
		// The same shadow with the two columns DIFFERENTLY TYPED (#323).
		// Project resolved an output column's type by looking its OUTPUT NAME
		// up in the input schema before consulting the source it renames, so
		// the alias took the shadowed int column's type while every value path
		// read the string source: the single-process arm returned an all-zero
		// column (wrong data, no error) and the DAG paired a string DirectCopy
		// with an int output vector and panicked in BulkCopy. Both arms are
		// wrong in different ways, so the compare catches it — and assertA
		// pins the answer absolutely, since a symmetric regression that made
		// both arms return zeros would still agree.
		twoPathQuery{name: "AliasedSortShadowsTypedColumn", cmp: cmpOrdered,
			sql: "SELECT n_name AS n_regionkey, n_regionkey AS r FROM nation ORDER BY n_regionkey",
			assertA: func(tb testing.TB, rows []map[string]any) {
				for i, r := range rows {
					v := cellText(r, "n_regionkey")
					if _, err := strconv.ParseFloat(v, 64); err == nil {
						tb.Errorf("row %d: the alias n_regionkey holds %q — a number, so the projection returned the "+
							"shadowed n_regionkey column instead of the n_name it renames", i, v)
						break
					}
				}
				// The sort key names the alias, so it must order the projected
				// n_name values, not the region keys it shadows.
				assertOrderedBy(tb, rows, false, "n_name projected as n_regionkey", func(r map[string]any) string {
					return cellText(r, "n_regionkey")
				})
			}},
		// The COMPUTED half of the same shadowing defect (#327): the value
		// comes from an expression, so an input column sharing the alias
		// describes a different column entirely. Typing from it returned an
		// all-zero int column here and panicked the DAG's fragment on the
		// concatenation form.
		twoPathQuery{name: "ComputedAliasShadowsTypedColumn", cmp: cmpOrdered,
			sql: "SELECT UPPER(n_name) AS n_regionkey, n_regionkey AS r FROM nation ORDER BY r, n_regionkey",
			assertA: func(tb testing.TB, rows []map[string]any) {
				for i, r := range rows {
					v := cellText(r, "n_regionkey")
					if _, err := strconv.ParseFloat(v, 64); err == nil {
						tb.Errorf("row %d: the alias n_regionkey holds %q — a number, so the computed "+
							"UPPER(n_name) was typed from the shadowed n_regionkey column", i, v)
						break
					}
					if v != strings.ToUpper(v) {
						tb.Errorf("row %d: n_regionkey = %q is not upper-cased", i, v)
						break
					}
				}
			}},
		// || is string concatenation, not arithmetic (#328). Every row came
		// back NULL: the evaluator had no case for the operator and the
		// planner declared its output Float64.
		twoPathQuery{name: "ConcatOperator", cmp: cmpOrdered,
			sql: "SELECT n_name || '-' || n_name AS doubled FROM nation ORDER BY n_name",
			assertA: func(tb testing.TB, rows []map[string]any) {
				for i, r := range rows {
					v := cellText(r, "doubled")
					if v == "" || v == "<nil>" || !strings.Contains(v, "-") {
						tb.Errorf("row %d: n_name || '-' || n_name = %q, want the two names joined by a dash", i, v)
						break
					}
				}
			}},
		// Two sort keys, only the first aliased: materializing the alias must
		// not cost the plan the second key.
		twoPathQuery{name: "AliasedSortMixedKeys", sql: "SELECT o_orderpriority AS p, o_orderstatus FROM orders ORDER BY p, o_orderstatus", cmp: cmpOrdered},
		// Same shape over a join — the alias is materialized on the join
		// stage rather than the scan.
		twoPathQuery{name: "AliasedSortNoAggregateJoin", sql: "SELECT n_name AS nm FROM nation JOIN region ON n_regionkey = r_regionkey ORDER BY nm", cmp: cmpOrdered},
		twoPathQuery{name: "AliasedSortWithExpr", sql: "SELECT n_name AS nm, UPPER(n_comment) AS uc FROM nation ORDER BY nm", cmp: cmpOrdered},
		twoPathQuery{name: "AliasedGroupKeyOrderBy", sql: "SELECT o_orderpriority AS p, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY p", cmp: cmpOrdered},
		// Minimal repro for the Q05 divergence: a WHERE equality between
		// two joined tables that is not itself a join condition. The DAG
		// answers this with the count it would produce with the predicate
		// removed entirely.
		twoPathQuery{name: "CrossTableEqualityFilter", sql: `SELECT COUNT(*) AS c FROM customer
			JOIN orders ON c_custkey = o_custkey
			JOIN lineitem ON l_orderkey = o_orderkey
			JOIN supplier ON l_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			JOIN region ON n_regionkey = r_regionkey
			WHERE c_nationkey = s_nationkey`, cmp: cmpUnordered},
		// Minimal repro trio for the Q07 divergence (#314): two aliases of one
		// table, both n_names projected. A join qualifies only the BUILD
		// side's colliding columns, so whichever alias lands on the PROBE side
		// ships under the bare "n_name" while the other becomes "n2.n_name" —
		// and the SELECT list names both by their aliases. The DAG resolved
		// those names by exact match only, so the probe-side alias resolved to
		// nothing: the gather projection collapsed to rename-only and that
		// column left the result under its raw worker name.
		//
		// Run both join orders: which alias is unqualified is decided by which
		// one the planner puts on the probe side, so a fix that only handles
		// one direction passes one of these and fails the other.
		twoPathQuery{name: "SelfJoinAliasedColumns", cmp: cmpUnordered,
			sql: "SELECT n1.n_name AS supp, n2.n_name AS cust FROM nation n1 JOIN nation n2 ON n1.n_regionkey = n2.n_regionkey"},
		twoPathQuery{name: "SelfJoinAliasedColumnsReversed", cmp: cmpUnordered,
			sql: "SELECT n1.n_name AS supp, n2.n_name AS cust FROM nation n2 JOIN nation n1 ON n1.n_regionkey = n2.n_regionkey"},
		// The same self-join under a GROUP BY with a DERIVED key. That is the
		// shape Q07 actually takes, and it fails differently: the derived key
		// makes the worker build a pre-aggregate projection over the group
		// columns, so the unresolved alias is emitted as a real but all-NULL
		// column instead of vanishing. Row count and revenue match; only the
		// first alias's values are gone — which is why every existing gate
		// (row counts, the harness value signature, DuckDB row-count checks)
		// stayed green through it.
		twoPathQuery{name: "SelfJoinDerivedGroupKey", cmp: cmpUnordered,
			sql: `SELECT n1.n_name AS supp, SUBSTR(n2.n_name, 1, 3) AS cust3, COUNT(*) AS c
				FROM nation n1 JOIN nation n2 ON n1.n_regionkey = n2.n_regionkey
				GROUP BY n1.n_name, SUBSTR(n2.n_name, 1, 3)`,
		},
		// Q07 itself, with its trailing ORDER BY stripped. The verbatim entry
		// above compares ordered, so it also depends on #313's aliased-sort-key
		// fix; this one isolates the VALUES, which is the #314 half. Same split
		// the LIMIT entries use — assert what the other defect does not cover
		// rather than loosening either assertion. Keep it after #313 lands: an
		// ordered compare that regressed to unordered would still pass Q07, and
		// this entry would not.
		twoPathQuery{name: "Q07_noorder", cmp: cmpUnordered, sql: stripTrailingOrderBy(TPCHQueries[7].SQL)},

		// Set operations (#346). walkStages emitted each arm's stages and no
		// merge — "each side runs independently; merge results at the end",
		// with nothing merging — so the terminal gather attached to whichever
		// arm was emitted last and the DAG answered a union with ONE arm's
		// raw, unprojected scan: half the rows, and that table's full column
		// list. Both halves are asserted absolutely below (wantRows and
		// wantCols), because the arm-vs-arm compare alone would accept an
		// extra column and, on the ORDER BY entry, a wrong answer that
		// happens to carry the right values.
		//
		// The plain concatenation: same table twice, so the count is the
		// whole assertion — five rows is one arm, ten is the union.
		twoPathQuery{name: "UnionAll", cmp: cmpUnordered,
			sql:      "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region",
			wantCols: []string{"r_regionkey"}, wantRows: 10},
		// A projection over the union: each arm's SELECT list has to be
		// APPLIED, not merely renamed at the gather. The arms also disagree
		// on type here — arm 1 computes a float, arm 2 copies an int32 — and
		// two .wshf files declaring different types for one column is a
		// decoding error, not a union: this panicked the gather task before
		// the planner reconciled the arms.
		twoPathQuery{name: "UnionAllProjection", cmp: cmpUnordered,
			sql:      "SELECT r_regionkey + 100 AS k FROM region UNION ALL SELECT n_nationkey AS k FROM nation",
			wantCols: []string{"k"}, wantRows: 30},
		// ORDER BY over the WHOLE set operation, keyed on a column only the
		// first arm computes. assertA pins the sequence absolutely: an
		// ordering both arms lost would satisfy the compare.
		twoPathQuery{name: "UnionAllOrderBy", cmp: cmpOrdered,
			sql:      "SELECT UPPER(r_name) AS nm FROM region UNION ALL SELECT n_name FROM nation ORDER BY nm",
			wantCols: []string{"nm"}, wantRows: 30,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "nm over both arms", func(r map[string]any) string {
					return cellText(r, "nm")
				})
			}},
		// LIMIT over the set operation, in both forms: the stripped query
		// compared row for row (and counted absolutely), the verbatim one
		// count-compared, which is what pins the limit itself. Ten rows back
		// from a LIMIT 4 is the un-limited concatenation; five is one arm.
		twoPathQuery{name: "UnionAllOrderByLimit_nolimit", cmp: cmpOrdered,
			sql:      "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1",
			wantCols: []string{"r_regionkey"}, wantRows: 10},
		twoPathQuery{name: "UnionAllOrderByLimit", cmp: cmpCount, limit: 4, expectRows: true,
			sql:      "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1 LIMIT 4",
			wantCols: []string{"r_regionkey"}, wantRows: 4},
		// UNION without ALL: the dedup runs ACROSS the arms, so returning
		// each arm's own distinct set is not enough. The two halves partition
		// nation's 25 rows, whose five region keys each appear on both sides
		// of the split — 5 rows is the answer, 10 is per-arm dedup.
		twoPathQuery{name: "UnionDistinct", cmp: cmpUnordered,
			sql: "SELECT n_regionkey FROM nation WHERE n_nationkey < 5 UNION " +
				"SELECT n_regionkey FROM nation WHERE n_nationkey >= 5",
			wantCols: []string{"n_regionkey"}, wantRows: 5},
		// A WHERE above the set operation. walkStages pushes a Filter onto
		// the stage it just emitted, which is now the union stage — if that
		// stage's fragment does not run the predicate it is silently dropped
		// and the whole concatenation comes back (30 rows, not 6).
		twoPathQuery{name: "UnionAllFilteredAbove", cmp: cmpUnordered,
			sql: "SELECT k FROM (SELECT r_regionkey AS k FROM region UNION ALL " +
				"SELECT n_nationkey AS k FROM nation) u WHERE k < 3",
			wantCols: []string{"k"}, wantRows: 6},
		// Three arms: SQL parses the chain left-deep, so the outer union's
		// first arm is another union with no SELECT list of its own. The
		// result names come from the leftmost arm of the whole chain.
		twoPathQuery{name: "UnionAllThreeArms", cmp: cmpUnordered,
			sql: "SELECT r_regionkey FROM region UNION ALL SELECT n_regionkey FROM nation " +
				"UNION ALL SELECT r_regionkey FROM region",
			wantCols: []string{"r_regionkey"}, wantRows: 35},
	)

	// #329: an UNGROUPED aggregate returns exactly one row for any input,
	// including none — SUM over the empty set is NULL, and SQL has no reading
	// under which the answer is zero rows. The stage DAG lost that row when
	// the aggregate's input was an empty JOIN result: the fragment
	// short-circuited on "no input files" before building a pipeline, and the
	// carve-out that let COUNT through covered nothing else, because the wire
	// AggSpec carried no output type and a mistyped identity row is worse than
	// none. Q17 came back with 0 rows against a fixture whose part predicate
	// matched nothing.
	//
	// The compare is the assertion (arm A was right all along), and every
	// entry also carries an absolute assertA: "the paths agree" would be
	// satisfied by both returning nothing, which is exactly the wrong answer.
	// The cluster runs three workers over three chunks per table, so ONE row
	// out is also the statement that N workers did not each contribute one.
	emptyJoin := func(selectList string) string {
		return "SELECT " + selectList + " FROM orders JOIN customer ON o_custkey = c_custkey WHERE c_nationkey = 999"
	}
	assertOneRow := func(tb testing.TB, rows []map[string]any, check func(testing.TB, map[string]any)) {
		tb.Helper()
		if len(rows) != 1 {
			tb.Fatalf("arm A (single-process) returned %d rows; an ungrouped aggregate owes SQL exactly one row over any input, including none", len(rows))
		}
		check(tb, rows[0])
	}
	assertNull := func(tb testing.TB, row map[string]any, cols ...string) {
		tb.Helper()
		for _, c := range cols {
			v, ok := lookupCell(row, c)
			if !ok {
				tb.Errorf("result has no column %q: %v", c, row)
				continue
			}
			if v != nil {
				tb.Errorf("%s over the empty set = %v, want NULL", c, v)
			}
		}
	}
	out = append(out,
		// The issue's shape, reduced: SUM over a join that matches nothing.
		twoPathQuery{name: "EmptyJoinUngroupedSum", cmp: cmpUnordered,
			sql: emptyJoin("SUM(o_totalprice) AS s"),
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOneRow(tb, rows, func(tb testing.TB, r map[string]any) { assertNull(tb, r, "s") })
			}},
		// Every family at once. This is also the shape the old carve-out
		// failed hardest on: one SUM disqualified the whole fragment, so even
		// the COUNT came back missing instead of 0.
		twoPathQuery{name: "EmptyJoinAggregateFamilies", cmp: cmpUnordered,
			sql: emptyJoin("SUM(o_totalprice) AS s, MIN(o_totalprice) AS mn, MAX(o_totalprice) AS mx, AVG(o_totalprice) AS av, COUNT(*) AS c"),
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOneRow(tb, rows, func(tb testing.TB, r map[string]any) {
					assertNull(tb, r, "s", "mn", "mx", "av")
					if got := cellNum(r, "c"); got != 0 {
						tb.Errorf("COUNT(*) over the empty set = %v, want 0 (never NULL)", r["c"])
					}
					if r["c"] == nil {
						tb.Error("COUNT(*) over the empty set is NULL, want 0")
					}
				})
			}},
		// MIN over a STRING column: its output type follows the input, which
		// is why the planner has to resolve it from the catalog rather than
		// derive it from the function name.
		twoPathQuery{name: "EmptyJoinUngroupedMinString", cmp: cmpUnordered,
			sql: emptyJoin("MIN(c_name) AS m"),
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOneRow(tb, rows, func(tb testing.TB, r map[string]any) { assertNull(tb, r, "m") })
			}},
		// Control: COUNT alone was always correct, and must stay so.
		twoPathQuery{name: "EmptyJoinUngroupedCount", cmp: cmpUnordered,
			sql: emptyJoin("COUNT(*) AS c"),
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOneRow(tb, rows, func(tb testing.TB, r map[string]any) {
					if got := cellNum(r, "c"); got != 0 || r["c"] == nil {
						tb.Errorf("COUNT(*) over the empty set = %v, want 0", r["c"])
					}
				})
			}},
		// Control: the same SUM emptied by a WHERE instead of a join. This
		// shape was correct on every path — the aggregate's fragment had input
		// files, just no surviving rows — and localises the defect to the
		// empty-file-set short-circuit rather than to aggregation itself.
		twoPathQuery{name: "EmptyWhereUngroupedSum", cmp: cmpUnordered,
			sql: "SELECT SUM(o_totalprice) AS s FROM orders WHERE o_custkey = -1",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOneRow(tb, rows, func(tb testing.TB, r map[string]any) { assertNull(tb, r, "s") })
			}},
		// Control on the other side: a GROUPED aggregate over the same empty
		// join owes NO rows. Only the ungrouped one has a row to lose, so a
		// fix that emitted unconditionally would break this.
		twoPathQuery{name: "EmptyJoinGroupedSum", cmp: cmpUnordered,
			sql: "SELECT c_nationkey AS nk, SUM(o_totalprice) AS s FROM orders JOIN customer ON o_custkey = c_custkey WHERE c_nationkey = 999 GROUP BY c_nationkey",
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 0 {
					tb.Errorf("arm A returned %d rows for a GROUP BY over an empty join, want 0 — no input rows, no groups", len(rows))
				}
			}},
		// Q17 itself, the query the issue was reported against, with a part
		// brand no row carries — which is what makes its lineitem⋈part join
		// empty and its `SUM(...) / 7.0` an ungrouped aggregate over nothing.
		// The corpus's own Q17 keeps the verbatim Brand#23, which this
		// fixture DOES match; that is precisely why 22 queries of coverage
		// never reached the shape.
		twoPathQuery{name: "Q17EmptyPartFilter", cmp: cmpUnordered,
			sql: strings.ReplaceAll(TPCHQueries[17].SQL, "Brand#23", "Brand#99"),
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOneRow(tb, rows, func(tb testing.TB, r map[string]any) { assertNull(tb, r, "avg_yearly") })
			}},
		// Multi-task, partially empty: the join matches one customer, so most
		// of the fan-out contributes nothing while one task carries the whole
		// answer. Exactly one row out, with the fixture's real values — a
		// double-counted identity row would show up here as an inflated
		// count, and a lost one as a missing row.
		twoPathQuery{name: "SparseJoinUngroupedAggregate", cmp: cmpUnordered,
			sql: `SELECT SUM(o_totalprice) AS s, COUNT(*) AS c
				FROM orders JOIN customer ON o_custkey = c_custkey WHERE c_custkey = 7`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOneRow(tb, rows, func(tb testing.TB, r map[string]any) {
					if got := cellNum(r, "c"); got != 17 {
						tb.Errorf("COUNT(*) = %v, want 17 (customer 7's orders at SF0.01)", r["c"])
					}
					if got := cellNum(r, "s"); math.Abs(got-4065944.86) > 0.01 {
						tb.Errorf("SUM(o_totalprice) = %v, want 4065944.86", r["s"])
					}
				})
			}},
	)

	// #339: the variance family, whose partial state is a (count, mean, M2)
	// triple rather than a single number, so BOTH merges the engine performs
	// had to be taught to combine it:
	//
	//	arm A — morsel-parallel clones merged into one parent. The clone's
	//	        extraState was dropped, so the parent answered from the rows
	//	        the FIRST clone happened to see: STDDEV(o_totalprice) came
	//	        back 143940.6 (later 144224.1, since which clone wins is a
	//	        scheduling detail) against a true 144048.14.
	//	arm B — partial tasks emitted finished STDDEV values and the final
	//	        stage ran STDDEV again over THOSE: 531.79, the spread of
	//	        three nearly identical numbers.
	//
	// Neither is catchable by a row count, a NULL check, or the arms
	// agreeing with each other, so every entry carries an assertA against a
	// Welford pass computed here over the same fixture. o_totalprice is the
	// discriminating column: mean ~2.5e5 against a spread of ~1.4e5, which
	// is where a sum-of-squares accumulator loses its digits.
	out = append(out,
		twoPathQuery{name: "StddevBigMean", cmp: cmpUnordered,
			sql: "SELECT STDDEV(o_totalprice) AS s FROM orders",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertVarianceValue(tb, rows, "s", varSamp, math.Sqrt, "orders", "o_totalprice")
			}},
		// The whole family in one row, so the sample/population split is
		// pinned by construction rather than by four separate numbers:
		// STDDEV and VARIANCE are the SAMPLE forms (as in DuckDB and
		// PostgreSQL), _pop the population ones, and each stddev is the
		// square root of its own variance.
		twoPathQuery{name: "VarianceFamilySemantics", cmp: cmpUnordered,
			sql: `SELECT STDDEV(o_totalprice) AS s, STDDEV_SAMP(o_totalprice) AS ss,
				STDDEV_POP(o_totalprice) AS sp, VARIANCE(o_totalprice) AS v,
				VAR_SAMP(o_totalprice) AS vs, VAR_POP(o_totalprice) AS vp FROM orders`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				assertVarianceValue(tb, rows, "s", varSamp, math.Sqrt, "orders", "o_totalprice")
				assertVarianceValue(tb, rows, "ss", varSamp, math.Sqrt, "orders", "o_totalprice")
				assertVarianceValue(tb, rows, "sp", varPop, math.Sqrt, "orders", "o_totalprice")
				assertVarianceValue(tb, rows, "v", varSamp, identityFloat, "orders", "o_totalprice")
				assertVarianceValue(tb, rows, "vs", varSamp, identityFloat, "orders", "o_totalprice")
				assertVarianceValue(tb, rows, "vp", varPop, identityFloat, "orders", "o_totalprice")
				// Sample > population, always, and by the exact factor
				// n/(n-1) — which is 1.00003 at 15000 rows, three orders of
				// magnitude smaller than the defect. Stating it here is what
				// rules out "wadjet just returns the other definition".
				r := rows[0]
				vs, vp := cellNum(r, "vs"), cellNum(r, "vp")
				n := float64(len(sf001Column(tb, "orders", "o_totalprice")))
				if want := vp * n / (n - 1); math.Abs(vs-want)/want > 1e-9 {
					tb.Errorf("VAR_SAMP/VAR_POP = %v/%v; sample must be population × n/(n-1) = %v", vs, vp, want)
				}
			}},
		// A column at the other end of the scale: l_discount is 0..0.10, so
		// its spread is the same order as its mean. The reported error went
		// the OTHER way here (0.031507 against 0.0316120), which is what
		// distinguishes accumulated noise from a constant sample/population
		// factor.
		twoPathQuery{name: "StddevSmallSpread", cmp: cmpUnordered,
			sql: "SELECT STDDEV(l_discount) AS s FROM lineitem",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertVarianceValue(tb, rows, "s", varSamp, math.Sqrt, "lineitem", "l_discount")
			}},
		// Grouped, so the merge combines states PER KEY rather than into one
		// global accumulator — the path a distributed shuffle takes.
		twoPathQuery{name: "StddevGrouped", cmp: cmpOrdered,
			sql: `SELECT o_orderstatus AS k, STDDEV(o_totalprice) AS s, COUNT(*) AS c
				FROM orders GROUP BY o_orderstatus ORDER BY k`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) == 0 {
					tb.Fatal("no rows")
				}
				byStatus := map[string][]float64{}
				for _, r := range sf001Table(tb, "orders") {
					byStatus[fmt.Sprint(r["o_orderstatus"])] = append(
						byStatus[fmt.Sprint(r["o_orderstatus"])], toFloat(r["o_totalprice"]))
				}
				for _, r := range rows {
					k := cellText(r, "k")
					vals, ok := byStatus[k]
					if !ok {
						tb.Errorf("result carries group %q, which the fixture does not", k)
						continue
					}
					if got := int(cellNum(r, "c")); got != len(vals) {
						tb.Errorf("group %q: COUNT(*) = %d, fixture has %d rows", k, got, len(vals))
					}
					want := math.Sqrt(welfordVariance(vals, varSamp))
					if got := cellNum(r, "s"); math.Abs(got-want)/want > 1e-9 {
						tb.Errorf("group %q: STDDEV = %.17g, want %.17g (Welford over the group's %d fixture rows)",
							k, got, want, len(vals))
					}
				}
			}},
		// STDDEV over one row has no sample answer: NULL, not 0. The
		// population form of the same one row is 0, not NULL.
		twoPathQuery{name: "StddevSingleRow", cmp: cmpUnordered,
			sql: "SELECT STDDEV(o_totalprice) AS s, STDDEV_POP(o_totalprice) AS sp FROM orders WHERE o_orderkey = 1",
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				if v, _ := lookupCell(rows[0], "s"); v != nil {
					tb.Errorf("STDDEV over one row = %v, want NULL", v)
				}
				if v, _ := lookupCell(rows[0], "sp"); v == nil || cellNum(rows[0], "sp") != 0 {
					tb.Errorf("STDDEV_POP over one row = %v, want 0", v)
				}
			}},
	)

	// #320: ORDER BY over anything that is not a plain SELECT-list column.
	// The third member of the #313/#316 family and the widest — the sort key
	// named a column nothing computed (`year(d)`, `-id`, `a + b`) or one the
	// SELECT-list Project had already dropped (`ORDER BY b` over `SELECT a`),
	// matched nothing, and the sort returned its input untouched on BOTH
	// engines. The two arms therefore agreed while both were unsorted, which
	// is why every entry here carries an assertA: the compare proves the paths
	// agree, assertA proves the answer is right.
	out = append(out,
		// The issue verbatim, in its function form. The key is not in the
		// result at all — it is materialized, sorted on, and dropped.
		twoPathQuery{name: "ExprSortFunction", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name FROM nation ORDER BY LENGTH(n_name), n_name",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "length(n_name)", func(r map[string]any) int {
					return len(cellText(r, "n_name"))
				})
			}},
		// DESC, and over a function whose output is a string rather than a
		// number: the materialized column has to carry its own type, not the
		// sorted column's.
		twoPathQuery{name: "ExprSortFunctionDesc", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name FROM nation ORDER BY UPPER(n_name) DESC",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, true, "upper(n_name)", func(r map[string]any) string {
					return strings.ToUpper(cellText(r, "n_name"))
				})
			}},
		// Arithmetic over two columns, both of them selected — so the rows
		// carry everything needed to recompute the key.
		twoPathQuery{name: "ExprSortArithmetic", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_nationkey, n_regionkey FROM nation ORDER BY n_nationkey + n_regionkey, n_nationkey",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "n_nationkey + n_regionkey", func(r map[string]any) float64 {
					return cellNum(r, "n_nationkey") + cellNum(r, "n_regionkey")
				})
			}},
		// Negation. Spelled `0 - x` rather than `-x`: a unary minus is typed
		// as a string by the projection type inference (#310), which orders
		// "-10" before "-2" — the same result `SELECT -x AS k ... ORDER BY k`
		// gives today, so it is not this bug. The sibling entry below pins
		// the unary spelling to two-path agreement only.
		twoPathQuery{name: "ExprSortNegated", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_nationkey FROM nation ORDER BY 0 - n_nationkey",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, true, "n_nationkey", func(r map[string]any) float64 {
					return cellNum(r, "n_nationkey")
				})
			}},
		twoPathQuery{name: "ExprSortUnaryMinus", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_nationkey FROM nation ORDER BY -n_nationkey",
			assertA: func(tb testing.TB, rows []map[string]any) {
				// Two readings are admissible here, and this bug is about
				// neither. A unary minus is typed as a string by the
				// projection type inference (#310), so the materialized key
				// collates "-0" < "-1" < "-10"; typed numerically it would
				// order 24, 23, 22. `SELECT -n_nationkey AS k ... ORDER BY k`
				// — the spelling that always worked — gives whichever of the
				// two the typing produces, so requiring one specific answer
				// here would gate #310, not #320. What #320 owns is that the
				// key is EVALUATED AND APPLIED at all: before the fix the rows
				// came back in scan order, which is neither.
				_, numeric := firstOutOfOrder(rows, true, func(r map[string]any) float64 {
					return cellNum(r, "n_nationkey")
				})
				_, lexical := firstOutOfOrder(rows, false, func(r map[string]any) string {
					return "-" + cellText(r, "n_nationkey")
				})
				if !numeric && !lexical {
					tb.Errorf("arm A (single-process) is ordered by neither the numeric nor the string reading of -n_nationkey — "+
						"the sort key was not applied at all; first rows: %v", rows[:min(5, len(rows))])
				}
			}},
		// Mixed keys: the first is a plain selected column, the second an
		// expression. Materializing one must not cost the plan the other.
		twoPathQuery{name: "ExprSortMixedKeys", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name, n_regionkey FROM nation ORDER BY n_regionkey, LENGTH(n_name), n_name",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "(n_regionkey, length(n_name))", func(r map[string]any) float64 {
					// One monotone key from both: n_name is at most 25 chars,
					// so the length never carries into the region digit.
					return cellNum(r, "n_regionkey")*100 + float64(len(cellText(r, "n_name")))
				})
			}},
		// The same failure with no expression at all: a plain column the
		// SELECT list does not carry. The Project below the Sort had already
		// narrowed it away, so the key matched nothing — `SELECT a ORDER BY b`
		// came back in scan order. ClickBench Q23/Q25 are this shape.
		twoPathQuery{name: "ColumnSortNotSelected", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_comment FROM nation ORDER BY n_name",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "n_name (recovered from n_comment)", nationOfComment)
			}},
		// Star select: there is no SELECT-list projection to hang the
		// materialized key on, so the planner has to create one that keeps
		// the star.
		twoPathQuery{name: "ExprSortStarSelect", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT * FROM nation ORDER BY LENGTH(n_name), n_name",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "length(n_name)", func(r map[string]any) int {
					return len(cellText(r, "n_name"))
				})
			}},
		// Under a LIMIT, which plans as a top-N rather than a full sort — a
		// separate builder, and one that must materialize the key too.
		twoPathQuery{name: "ExprSortTopN", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name FROM nation ORDER BY LENGTH(n_name) DESC, n_name LIMIT 5",
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 5 {
					tb.Errorf("arm A returned %d rows for LIMIT 5", len(rows))
				}
				assertOrderedBy(tb, rows, true, "length(n_name)", func(r map[string]any) int {
					return len(cellText(r, "n_name"))
				})
				// The top 5 by name length at SF0.01 — a sort that merely
				// ordered the first five arbitrary rows would pass the
				// monotonicity check above but not this one.
				if got := len(cellText(rows[0], "n_name")); got != 14 {
					tb.Errorf("longest nation name in the top-5 is %d chars, want 14 (UNITED KINGDOM)", got)
				}
			}},
		// Over a join, where the materialized key has to land on the join
		// fragment rather than a scan.
		twoPathQuery{name: "ExprSortOverJoin", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name FROM nation JOIN region ON n_regionkey = r_regionkey ORDER BY LENGTH(n_name), n_name",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "length(n_name)", func(r map[string]any) int {
					return len(cellText(r, "n_name"))
				})
			}},
		// Over a GROUP BY, sorting on a grouped column the SELECT list does
		// not carry. The Project below the Sort had narrowed it away, so the
		// single-process path lost the order; the DAG keeps it because its
		// aggregate stage still emits the group key. Materializing the key
		// has to leave that DAG resolution intact — the hidden column maps
		// back through the Project to the group key, not to a name no stage
		// emits.
		twoPathQuery{name: "GroupKeySortNotSelected", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT MIN(n_name) AS m FROM nation GROUP BY n_regionkey ORDER BY n_regionkey",
			assertA: func(tb testing.TB, rows []map[string]any) {
				// One region per row, ordered by regionkey — which is NOT the
				// alphabetical order of the values, so a lost ORDER BY cannot
				// coincide with a sorted-looking result.
				want := []string{"ALGERIA", "ARGENTINA", "CHINA", "FRANCE", "EGYPT"}
				if len(rows) != len(want) {
					tb.Fatalf("arm A returned %d rows, want %d (one per region)", len(rows), len(want))
				}
				for i, w := range want {
					if got := cellText(rows[i], "m"); got != w {
						tb.Errorf("row %d = %q, want %q (regions in n_regionkey order)", i, got, w)
					}
				}
			}},
		// ORDER BY <position>: the ordinal used to reach the sort as the
		// literal "1" and match no column at all.
		twoPathQuery{name: "OrdinalSort", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name, n_regionkey FROM nation ORDER BY 1",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "select item 1 (n_name)", func(r map[string]any) string {
					return cellText(r, "n_name")
				})
			}},
		// The controls: the shapes that already worked must keep working, and
		// they are what made the bug look like an aliasing problem — putting
		// the expression in the SELECT list fixed it.
		twoPathQuery{name: "ExprSortSelected", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name, LENGTH(n_name) AS l FROM nation ORDER BY l, n_name",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "l", func(r map[string]any) float64 {
					return cellNum(r, "l")
				})
			}},
		twoPathQuery{name: "PlainColumnSort", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_name FROM nation ORDER BY n_name DESC",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, true, "n_name", func(r map[string]any) string {
					return cellText(r, "n_name")
				})
			}},
		// Pagination. Both halves of #337 were symmetric — the parser
		// dropped whichever of LIMIT/OFFSET came second and the builder read
		// OFFSET only inside the LIMIT branch, so both paths returned the
		// whole table and agreed with each other — which is why each entry
		// carries an absolute assertA and not only the two-arm compare. The
		// DAG then had a defect of its own on top: its stages never applied
		// the offset at all (#344.2), so `LIMIT 3 OFFSET 5` came back as the
		// first three rows there while arm A skipped correctly.
		twoPathQuery{name: "OffsetAlone", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertFirstKeyAndCount(tb, rows, "n_nationkey", 5, 20)
			}},
		twoPathQuery{name: "OffsetThenLimit", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5 LIMIT 3",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertFirstKeyAndCount(tb, rows, "n_nationkey", 5, 3)
			}},
		twoPathQuery{name: "LimitThenOffset", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertFirstKeyAndCount(tb, rows, "n_nationkey", 5, 3)
			}},
		// The page after the last page. expectRows stays off — emptiness is
		// the answer, and "all 25 rows" is the bug.
		twoPathQuery{name: "OffsetPastEnd", cmp: cmpOrdered,
			sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 100",
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 0 {
					tb.Errorf("got %d rows past the end of the table, want 0", len(rows))
				}
			}},
		twoPathQuery{name: "OffsetOverGroupBy", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY n_regionkey OFFSET 3",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertFirstKeyAndCount(tb, rows, "n_regionkey", 3, 2)
			}},
	)

	// #332: a temporal COLUMN plus or minus an INTERVAL. The operator read its
	// date only from a STRING operand, so a literal worked while a DATE column
	// fell through to numeric arithmetic that discards the interval — the
	// projection carried the column's raw epoch-day number. TPC-H writes every
	// interval against a literal (`DATE '1996-01-01' - INTERVAL '90' DAY`),
	// which is why 22 queries of coverage never reach the column form.
	//
	// Both arms were wrong the same way, so the compare would have passed
	// throughout: assertA owns the answer here, recomputed from the
	// o_orderdate each row carries.
	shiftedDate := func(tb testing.TB, r map[string]any, col string) (time.Time, bool) {
		tb.Helper()
		v := cellText(r, col)
		// A whole-day result renders YYYY-MM-DD; anything else means the
		// interval turned a date into an instant, which none of these do.
		t, err := time.Parse("2006-01-02", v)
		if err != nil {
			tb.Errorf("%s = %q, want a calendar date — a DATE column shifted by a "+
				"whole-day interval is still a date (%v)", col, v, err)
			return time.Time{}, false
		}
		return t, true
	}
	out = append(out,
		twoPathQuery{name: "ColumnIntervalArithmetic", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT o_orderkey, o_orderdate,
					o_orderdate - INTERVAL '90' DAY  AS minus90,
					o_orderdate + INTERVAL '1' MONTH AS plus1m,
					INTERVAL '1' YEAR + o_orderdate  AS plus1y
				FROM orders WHERE o_orderkey < 100 ORDER BY o_orderkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				for i, r := range rows {
					base, ok := shiftedDate(tb, r, "o_orderdate")
					if !ok {
						return
					}
					for _, tc := range []struct {
						col  string
						want time.Time
					}{
						{"minus90", base.AddDate(0, 0, -90)},
						// A month and a year are CALENDAR arithmetic: the same
						// day number of the next month, not 30 days on.
						{"plus1m", base.AddDate(0, 1, 0)},
						{"plus1y", base.AddDate(1, 0, 0)},
					} {
						got, ok := shiftedDate(tb, r, tc.col)
						if !ok {
							return
						}
						if !got.Equal(tc.want) {
							tb.Errorf("row %d: o_orderdate = %s, %s = %s, want %s",
								i, base.Format("2006-01-02"), tc.col,
								got.Format("2006-01-02"), tc.want.Format("2006-01-02"))
							return
						}
					}
				}
			}},
	)

	// #333: a string column through a polymorphic function — coalesce,
	// nullif, greatest, least — was typed numeric and came back as the
	// integer 0 on every row. Both arms were wrong the same way, so the
	// compare alone would have passed the whole bug; every entry here
	// carries an assertA that pins the answer absolutely.
	//
	// The typing is decided once, in the physical planner, and the DAG
	// carries that answer on ProjectSpec.Type — so these are also what
	// catches a fix that reaches only one of the two places the planner
	// computes it (buildProject for arm A, attachScanSelectProjections for
	// the DAG's scan fragments).
	assertNoNumericCell := func(cols ...string) func(testing.TB, []map[string]any) {
		return func(tb testing.TB, rows []map[string]any) {
			tb.Helper()
			for i, r := range rows {
				for _, c := range cols {
					v := cellText(r, c)
					if v == "<nil>" {
						continue
					}
					if _, err := strconv.ParseFloat(v, 64); err == nil {
						tb.Errorf("row %d: %s = %q — a number, so the string column was typed "+
							"numeric and its values were dropped on the way out", i, c, v)
						return
					}
				}
			}
		}
	}
	out = append(out,
		// The four broken shapes and the one that already worked, side by
		// side so a fix that reaches only some of them is visible.
		twoPathQuery{name: "PolymorphicStringColumns", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT n_nationkey, COALESCE(n_name) AS c1, COALESCE(n_name, n_comment) AS c2,
				GREATEST(n_name, n_comment) AS g, LEAST(n_name, n_comment) AS l,
				IFNULL(n_name, n_comment) AS i FROM nation`,
			assertA: assertNoNumericCell("c1", "c2", "g", "l", "i")},
		// nullif on its own: its NULL row is the one cell that is legitimately
		// empty, and the rest must be names.
		twoPathQuery{name: "PolymorphicNullifString", cmp: cmpUnordered, expectRows: true,
			sql:     "SELECT n_nationkey, NULLIF(n_name, 'ALGERIA') AS nu FROM nation",
			assertA: assertNoNumericCell("nu")},
		// The constraint that ruled out flipping those declarations to
		// String. Values, not types, are what both arms must agree on here —
		// and the absolute assertion is that they are the region keys.
		twoPathQuery{name: "PolymorphicNumericColumns", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT n_nationkey, NULLIF(n_nationkey, 1) AS nu, COALESCE(n_regionkey, 0) AS c,
				GREATEST(n_nationkey, n_regionkey) AS g FROM nation`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				for i, r := range rows {
					key := cellNum(r, "n_nationkey")
					if got := cellNum(r, "g"); got < key {
						tb.Errorf("row %d: GREATEST(n_nationkey, n_regionkey) = %v, less than n_nationkey = %v", i, got, key)
						return
					}
					if v := cellText(r, "c"); v == "<nil>" {
						tb.Errorf("row %d: COALESCE(n_regionkey, 0) is NULL", i)
						return
					}
				}
			}},
		// The same family as a GROUP BY key and as an aggregate input. Both
		// are typed in a second place on the DAG — the worker's
		// buildAggInputProjection — so they are where a planner-only fix
		// shows up as a two-path divergence.
		twoPathQuery{name: "PolymorphicGroupKey", cmp: cmpUnordered, expectRows: true,
			sql:     "SELECT COALESCE(n_name, n_comment) AS k, COUNT(*) AS c FROM nation GROUP BY COALESCE(n_name, n_comment)",
			assertA: assertNoNumericCell("k")},
		twoPathQuery{name: "PolymorphicAggregateInput", cmp: cmpUnordered, expectRows: true,
			sql:     "SELECT n_regionkey, MAX(COALESCE(n_name, n_comment)) AS m FROM nation GROUP BY n_regionkey",
			assertA: assertNoNumericCell("m")},
	)

	// #338: a NULL group key reported once per parallel partial instead of
	// once — GROUP BY, DISTINCT and set operations are the places SQL
	// requires NULLs to be treated as EQUAL to each other, so a merge that
	// matches partial states by key has to match a NULL key to a NULL key.
	//
	// Both arms were wrong the same way when it fired (four rows of 375
	// where the answer is one row of 1500, so no row was lost and every
	// row-count gate stayed green), which is why each entry below carries an
	// assertA: "the paths agree" is satisfied by both being wrong.
	//
	// The reduction that fires it needs a LEFT JOIN whose right side matches
	// NOTHING; that shape returns zero rows on the stage DAG for an
	// unrelated defect (a LEFT JOIN with an empty build side degenerates to
	// an inner one there — see the NullGroupKey* entries in duckdbCorpus,
	// where arm A is gated against DuckDB and arm B carries the exemption).
	// Gating it here would gate that defect instead of this one, so what
	// this file carries is the NULL grouping invariant over shapes both
	// engines answer: partial matches, several key columns, and DISTINCT.
	out = append(out,
		// Most customers match no order, so the NULL group and real groups
		// are aggregated side by side. One NULL row, counts summing to 1500.
		twoPathQuery{name: "NullGroupKeyLeftJoin", cmp: cmpUnordered,
			sql: `SELECT o.o_orderstatus AS s, COUNT(*) AS c FROM customer c
				LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 5
				GROUP BY o.o_orderstatus`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				nulls, total := 0, 0.0
				for _, r := range rows {
					if v, ok := lookupCell(r, "s"); ok && v == nil {
						nulls++
						// Assert the VALUE, not just the row count: the
						// failure splits a group's count across partials
						// without losing a row.
						if got := cellNum(r, "c"); got != 1496 {
							tb.Errorf("NULL group COUNT(*) = %v, want 1496", r["c"])
						}
					}
					total += cellNum(r, "c")
				}
				if nulls != 1 {
					tb.Errorf("the NULL group appeared %d times, want exactly 1: %v", nulls, rows)
				}
				if total != 1500 {
					tb.Errorf("counts total %v, want 1500", total)
				}
			}},
		// Two key columns, one NULL and one not — the case a fix narrowed to
		// "the whole key is NULL" leaves split.
		twoPathQuery{name: "NullGroupKeyLeftJoinMultiCol", cmp: cmpUnordered,
			sql: `SELECT c.c_mktsegment AS m, o.o_orderstatus AS s, COUNT(*) AS c FROM customer c
				LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 5
				GROUP BY c.c_mktsegment, o.o_orderstatus`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				total, seen := 0.0, map[string]bool{}
				for _, r := range rows {
					key := cellText(r, "m") + "|" + cellText(r, "s")
					if seen[key] {
						tb.Errorf("group (%s) reported twice — its partial states were not merged", key)
					}
					seen[key] = true
					total += cellNum(r, "c")
				}
				if total != 1500 {
					tb.Errorf("counts total %v, want 1500", total)
				}
			}},
		// DISTINCT groups NULLs together by the same rule, on a code path
		// (GroupByAll) that partitioned aggregation never takes — so the
		// merge is the only thing that can get it right.
		twoPathQuery{name: "NullGroupKeyDistinct", cmp: cmpUnordered,
			sql: `SELECT DISTINCT o.o_orderstatus AS s FROM customer c
				LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 5`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				nulls := 0
				for _, r := range rows {
					if v, ok := lookupCell(r, "s"); ok && v == nil {
						nulls++
					}
				}
				if nulls != 1 {
					tb.Errorf("DISTINCT returned %d NULL rows, want exactly 1: %v", nulls, rows)
				}
				if len(rows) != 4 {
					tb.Errorf("DISTINCT returned %d rows, want 4 (F, O, P and one NULL): %v", len(rows), rows)
				}
			}},
	)

	// #335 and #336: the two places a join predicate went missing. Both were
	// planner defects, so both arms were wrong identically and the compare
	// alone could never have caught them — every entry here carries an
	// absolute assertA naming the number DuckDB gives for the same fixture.
	// What the compare adds is that the two paths must not disagree about
	// them, and they did: `WHERE r.r_regionkey IS NULL` over a LEFT JOIN came
	// back as 25 on the single-process arm and 0 on the DAG, from the same
	// dropped predicate reached through two different plans.
	assertCount := func(col string, want float64) func(testing.TB, []map[string]any) {
		return func(tb testing.TB, rows []map[string]any) {
			tb.Helper()
			if len(rows) != 1 {
				tb.Fatalf("arm A (single-process) returned %d rows, want exactly 1", len(rows))
			}
			if got := cellNum(rows[0], col); got != want {
				tb.Errorf("arm A (single-process) %s = %v, want %v (DuckDB over the same fixture)", col, got, want)
			}
		}
	}
	out = append(out,
		// The issue verbatim: a WHERE over the null-supplying side of a LEFT
		// JOIN was pushed BELOW the NULL-padding, so region was filtered to
		// one row and every unmatched nation padded back in.
		twoPathQuery{name: "OuterWhereOnNullSupplyingSide", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n LEFT JOIN region r
				ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`,
			assertA: assertCount("c", 5)},
		// The anti-join idiom, which the same push destroys in the other
		// direction: IS NULL must stay above a join that stays outer.
		twoPathQuery{name: "OuterWhereAntiJoinIdiom", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n LEFT JOIN region r
				ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3 WHERE r.r_regionkey IS NULL`,
			assertA: assertCount("c", 10)},
		// The same predicate over an INNER join, and over the PRESERVED side
		// of the outer one: the two controls that were correct before the fix
		// and must not move.
		twoPathQuery{name: "InnerWhereSamePredicate", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r
				ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`,
			assertA: assertCount("c", 5)},
		twoPathQuery{name: "OuterWhereOnPreservedSide", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n LEFT JOIN region r
				ON n.n_regionkey = r.r_regionkey WHERE n.n_nationkey < 4`,
			assertA: assertCount("c", 4)},
		// RIGHT and FULL carry the rule with the sides exchanged.
		twoPathQuery{name: "RightJoinWhereOnNullSupplyingSide", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM region r RIGHT JOIN nation n
				ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`,
			assertA: assertCount("c", 5)},
		twoPathQuery{name: "FullJoinWhereOnNullSupplyingSide", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n FULL OUTER JOIN region r
				ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`,
			assertA: assertCount("c", 5)},
		// #336: the self-join deduplication idiom. `a.id < b.id` in ON was
		// dropped by the key parser, so every unordered pair came back twice
		// plus the diagonal.
		// Over nation, not supplier: this suite generates its own fact-table
		// data, so only the fixed TPC-H dimension tables carry a count that
		// can be written down. 5 regions of 5 nations each, one row per
		// unordered same-region pair.
		twoPathQuery{name: "OnClauseColumnConjunct", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation a JOIN nation b
				ON a.n_regionkey = b.n_regionkey AND a.n_nationkey < b.n_nationkey`,
			assertA: assertCount("c", 50)},
		// Its two controls: a column-vs-LITERAL conjunct in ON was honoured,
		// and so was the same column-vs-column conjunct in WHERE. The fix
		// makes ON agree with WHERE; neither control may move.
		twoPathQuery{name: "OnClauseColumnVsLiteral", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation a JOIN nation b
				ON a.n_regionkey = b.n_regionkey AND a.n_nationkey < 5`,
			assertA: assertCount("c", 25)},
		twoPathQuery{name: "WhereClauseColumnConjunct", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation a JOIN nation b
				ON a.n_regionkey = b.n_regionkey WHERE a.n_nationkey < b.n_nationkey`,
			assertA: assertCount("c", 50)},
		// Two divergences this suite found while the above were being
		// written. Neither is a predicate defect — both appear with no WHERE
		// at all — and both were the DAG losing an outer join's NULL-padded
		// rows. Fixed in #352 and #348 respectively; see the block at the end
		// of this corpus for the rest of that family.
		twoPathQuery{name: "RightJoinUnmatchedRows", cmp: cmpUnordered, expectRows: true,
			sql:     "SELECT COUNT(*) AS c FROM region r RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey",
			assertA: assertCount("c", 25)},
		twoPathQuery{name: "LeftJoinEmptyBuildSide", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n LEFT JOIN region r
				ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0`,
			assertA: assertCount("c", 25)},
	)

	// #345: a window VALUE function over a non-float column. LAG, LEAD,
	// FIRST_VALUE, LAST_VALUE and NTH_VALUE return a value taken FROM their
	// input column, so their output type is that column's type — and the
	// planner declared all five float64 from a hand-maintained name list.
	// exec.Window allocates its output vector at the declared type and, unlike
	// exec.Project, corrected nothing at runtime, so every string write was
	// dropped for the integer 0. #333's signature inside the window operator.
	//
	// Every entry carries an assertA: the compare would be satisfied by both
	// arms returning zeros, which is exactly the bug.
	//
	// These were pinned knownBug #349 until the stage DAG grew a window
	// operator: walkStages emitted a window stage, nothing converted it to a
	// fragment, and the task failed with "empty Operators" — so arm B could
	// not answer a window query of any shape. Both arms are gated now.
	assertNoZeroedColumn := func(col string) func(testing.TB, []map[string]any) {
		return func(tb testing.TB, rows []map[string]any) {
			tb.Helper()
			if len(rows) < 2 {
				tb.Fatalf("arm A returned %d rows — too few to say anything about %s", len(rows), col)
			}
			for i, r := range rows {
				v, ok := lookupCell(r, col)
				if !ok {
					tb.Fatalf("result has no column %q: %v", col, r)
				}
				if v == nil {
					continue // LAG's first row, LEAD's last: genuinely NULL
				}
				if s := fmt.Sprint(v); s == "0" {
					tb.Errorf("row %d: %s = 0 — the window's output vector was typed numeric "+
						"and the value write was dropped (#345)", i, col)
					return
				}
			}
		}
	}
	out = append(out,
		twoPathQuery{name: "WindowLagString", cmp: cmpOrdered,
			sql:     "SELECT n_nationkey, n_name, LAG(n_name) OVER (ORDER BY n_nationkey) AS w FROM nation ORDER BY n_nationkey",
			assertA: assertNoZeroedColumn("w")},
		twoPathQuery{name: "WindowLeadString", cmp: cmpOrdered,
			sql:     "SELECT n_nationkey, n_name, LEAD(n_name) OVER (ORDER BY n_nationkey) AS w FROM nation ORDER BY n_nationkey",
			assertA: assertNoZeroedColumn("w")},
		twoPathQuery{name: "WindowFirstValueString", cmp: cmpOrdered,
			sql:     "SELECT n_nationkey, n_name, FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey) AS w FROM nation ORDER BY n_nationkey",
			assertA: assertNoZeroedColumn("w")},
		twoPathQuery{name: "WindowLastValueString", cmp: cmpOrdered,
			sql:     "SELECT n_nationkey, n_name, LAST_VALUE(n_name) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) AS w FROM nation ORDER BY n_nationkey",
			assertA: assertNoZeroedColumn("w")},
		// NTH_VALUE with an explicit frame, and the column named nowhere else
		// in the SELECT list: column pruning read the whole argument string
		// ("n_name, 2") as the column name, so n_name was never marked
		// required and the operator found no input vector to read.
		twoPathQuery{name: "WindowNthValueStringFramed", cmp: cmpOrdered,
			sql: `SELECT n_nationkey, NTH_VALUE(n_name, 2) OVER (ORDER BY n_nationkey
				ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS w
				FROM nation ORDER BY n_nationkey`,
			assertA: assertNoZeroedColumn("w")},
		// A DATE column: its RENDERING is what the declared type buys, so a
		// date typed float64 is not a mis-scaled date, it is 0.
		twoPathQuery{name: "WindowFirstValueDate", cmp: cmpOrdered,
			sql:     "SELECT o_orderkey, FIRST_VALUE(o_orderdate) OVER (ORDER BY o_orderkey) AS w FROM orders ORDER BY o_orderkey",
			assertA: assertNoZeroedColumn("w")},
		// The numeric control: Float64.SetValue has no int32 case either, so
		// a narrow int was dropped exactly like a string.
		twoPathQuery{name: "WindowLagNumeric", cmp: cmpOrdered,
			sql:     "SELECT n_nationkey, LAG(n_nationkey) OVER (ORDER BY n_nationkey DESC) AS w FROM nation ORDER BY n_nationkey",
			assertA: assertNoZeroedColumn("w")},
		// The rank family is input-independent and must be untouched by the
		// fix — it is the half of the name list that stays hand-maintained.
		twoPathQuery{name: "WindowRankFamily", cmp: cmpOrdered,
			sql: `SELECT n_nationkey, ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn,
				RANK() OVER (ORDER BY n_regionkey) AS rk FROM nation ORDER BY n_nationkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				for i, r := range rows {
					if got := cellNum(r, "rn"); got != float64(i+1) {
						tb.Errorf("row %d: ROW_NUMBER() = %v, want %d", i, r["rn"], i+1)
						return
					}
					if cellNum(r, "rk") < 1 {
						tb.Errorf("row %d: RANK() = %v, want ≥ 1", i, r["rk"])
						return
					}
				}
			}},
	)

	// #349: the stage DAG had no window operator at all, so the shapes the
	// #345 entries above happen not to use were untested on arm B — every
	// one of them failed identically ("empty Operators"), which is exactly
	// why a suite of value-function queries was not coverage of the window
	// stage. These are the missing axes: a PARTITION BY (the partition is
	// the unit a distributed window can be split on, so getting it wrong is
	// the scalability decision showing up as a wrong answer), an AGGREGATE
	// window function, and an explicit FRAME.
	//
	// Each carries an assertA computed from the query's own other columns,
	// so the assertion says what the answer IS rather than that two engines
	// agree — the compare alone would pass on two identically wrong arms.
	out = append(out,
		// ROW_NUMBER within a partition: 25 nations across 5 regions, so
		// every region's numbers must be exactly 1..5. A window computed
		// over the wrong grain — the whole input, or a fragment of a
		// partition — cannot produce that.
		twoPathQuery{name: "WindowRowNumberPartitioned", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n_nationkey, n_regionkey,
				ROW_NUMBER() OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) AS rn
				FROM nation ORDER BY n_regionkey, n_nationkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				seen := map[float64]map[float64]bool{}
				for _, r := range rows {
					region, rn := cellNum(r, "n_regionkey"), cellNum(r, "rn")
					if seen[region] == nil {
						seen[region] = map[float64]bool{}
					}
					if seen[region][rn] {
						tb.Errorf("region %v: ROW_NUMBER %v repeats — the partition was split across tasks", region, rn)
						return
					}
					seen[region][rn] = true
				}
				if len(seen) != 5 {
					tb.Errorf("got %d regions, want 5", len(seen))
					return
				}
				for region, nums := range seen {
					for want := 1; want <= len(nums); want++ {
						if !nums[float64(want)] {
							tb.Errorf("region %v: ROW_NUMBER %d missing from %d rows", region, want, len(nums))
							return
						}
					}
				}
			}},
		// RANK partitioned on the same column it orders by: every row of a
		// partition ties, so RANK is 1 on all 25 rows. The assertion is
		// sharper than it looks — RANK skips over the rows it ties with, so
		// computing this over the whole input instead of per partition
		// gives 1, 6, 11, 16, 21, and computing ROW_NUMBER semantics by
		// mistake gives 1..5. Only the right function at the right grain
		// answers 1 everywhere.
		twoPathQuery{name: "WindowRankPartitioned", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n_nationkey, n_regionkey,
				RANK() OVER (PARTITION BY n_regionkey ORDER BY n_regionkey) AS rk
				FROM nation ORDER BY n_regionkey, n_nationkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				for i, r := range rows {
					if got := cellNum(r, "rk"); got != 1 {
						tb.Errorf("row %d: RANK() = %v over an all-tied partition, want 1", i, got)
						return
					}
				}
			}},
		// An AGGREGATE window function over a partition: the value is the
		// partition's whole sum, repeated on each of its rows. Checked
		// against the sum of the same rows' o_totalprice, so the assertion
		// is arithmetic on the query's own output.
		twoPathQuery{name: "WindowSumPartitioned", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT o_orderkey, o_orderstatus, o_totalprice,
				SUM(o_totalprice) OVER (PARTITION BY o_orderstatus) AS w
				FROM orders WHERE o_orderkey <= 500 ORDER BY o_orderkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				totals := map[string]float64{}
				for _, r := range rows {
					s, _ := lookupCell(r, "o_orderstatus")
					totals[fmt.Sprint(s)] += cellNum(r, "o_totalprice")
				}
				for i, r := range rows {
					s, _ := lookupCell(r, "o_orderstatus")
					want, got := totals[fmt.Sprint(s)], cellNum(r, "w")
					if math.Abs(got-want) > 0.01 {
						tb.Errorf("row %d (status %v): SUM() OVER = %v, want the partition total %v",
							i, s, got, want)
						return
					}
				}
			}},
		// An explicit FRAME, carried end to end: the two-row window spec
		// reaches the worker and the stage runs, which is what #349 is
		// about, and both paths must return the same column.
		//
		// This entry carried no assertA while #350 was open — the frame was
		// on the wire and both paths agreed, on the running total from the
		// start of the partition, because exec.Window never read
		// WindowColumn.Frame. Now that it does, the arithmetic is asserted:
		// `ROWS BETWEEN 1 PRECEDING AND CURRENT ROW` is this row's price
		// plus the previous row's, and the first row's frame holds only
		// itself.
		twoPathQuery{name: "WindowSumExplicitFrame", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT o_orderkey, o_totalprice,
				SUM(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w
				FROM orders WHERE o_orderkey <= 500 ORDER BY o_orderkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) < 2 {
					tb.Fatalf("%d rows, want at least 2", len(rows))
				}
				for i, r := range rows {
					want := cellNum(r, "o_totalprice")
					if i > 0 {
						want += cellNum(rows[i-1], "o_totalprice")
					}
					if got := cellNum(r, "w"); math.Abs(got-want) > 1e-6 {
						tb.Fatalf("row %d: SUM() OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) = %v, "+
							"want %v — the frame holds this row and the one before it, not the "+
							"partition so far", i, got, want)
					}
				}
			}},
	)

	// #348/#352 — an outer join over an EMPTY side, and a join with no keys
	// at all. What makes these two-path cases rather than plain correctness
	// cases is that the DAG and the single-process pipeline lose the rows for
	// DIFFERENT reasons: the worker's empty-build short-circuit dropped every
	// probe row where the in-process join dropped only the build side's
	// COLUMNS, and only the in-process path had any code that emitted a
	// RIGHT/FULL join's unmatched build rows at all.
	//
	// Every entry is held absolutely as well as arm-against-arm — by an
	// assertA, or by wantRows/wantCols where the answer is a row set: the
	// compare alone is satisfied by both arms answering 0, which is exactly
	// what several of these did.
	countNulls := func(col string, wantNull, wantNonNull int) func(testing.TB, []map[string]any) {
		return func(tb testing.TB, rows []map[string]any) {
			tb.Helper()
			var nulls, nonNulls int
			for _, r := range rows {
				v, ok := lookupCell(r, col)
				if !ok {
					tb.Fatalf("result has no column %q: %v — an empty join side must leave its "+
						"columns PRESENT and NULL, not absent", col, r)
				}
				if v == nil {
					nulls++
				} else {
					nonNulls++
				}
			}
			if nulls != wantNull || nonNulls != wantNonNull {
				tb.Errorf("column %s: %d NULL / %d non-NULL cells, want %d / %d",
					col, nulls, nonNulls, wantNull, wantNonNull)
			}
		}
	}
	out = append(out,
		// COUNT(*) beside COUNT(col) over the empty build: the pair that
		// separates "the column is NULL" from "the column is absent". The
		// second was 25 on the single-process arm, because an absent column
		// makes COUNT(col) degenerate into COUNT(*).
		twoPathQuery{name: "EmptyBuildLeftJoinCountCol", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c, COUNT(r.r_name) AS m FROM nation n
				LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				if got := cellNum(rows[0], "c"); got != 25 {
					tb.Errorf("COUNT(*) = %v, want 25 — every preserved row survives an empty build", got)
				}
				if got := cellNum(rows[0], "m"); got != 0 {
					tb.Errorf("COUNT(r.r_name) = %v, want 0 — the build column is NULL on every row, "+
						"and COUNT(col) skips NULLs", got)
				}
			}},
		// The same join's values: 25 rows, and the build column PRESENT and
		// NULL on all of them. wantCols pins the column list absolutely,
		// since an absent column is precisely the defect.
		twoPathQuery{name: "EmptyBuildLeftJoinValues", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.r_name FROM nation n
				LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0
				ORDER BY n.n_name`,
			wantRows: 25, wantCols: []string{"n_name", "r_name"},
			assertA: countNulls("r_name", 25, 0)},
		// The anti-join idiom over the empty build — how a query ASKS for the
		// unmatched rows, and the shape a careless fix breaks. Both arms
		// answered 0 against 25 before, because IS NULL cannot match a column
		// that is not there.
		twoPathQuery{name: "EmptyBuildLeftJoinIsNull", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n
				LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0
				WHERE r.r_regionkey IS NULL`,
			assertA: assertCount("c", 25)},
		twoPathQuery{name: "EmptyBuildLeftJoinIsNotNull", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n
				LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0
				WHERE r.r_regionkey IS NOT NULL`,
			assertA: assertCount("c", 0)},
		// The other three join types over the same empty build. INNER and
		// SEMI answer nothing and always did — the short-circuit the LEFT fix
		// narrowed is correct for them. ANTI answers everything and was lost
		// with LEFT: it preserves probe rows the build never matched too.
		twoPathQuery{name: "EmptyBuildInnerJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n
				JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0`,
			assertA: assertCount("c", 0)},
		twoPathQuery{name: "EmptyBuildSemiJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n WHERE EXISTS (
				SELECT 1 FROM region r WHERE r.r_regionkey = n.n_regionkey AND r.r_regionkey < 0)`,
			assertA: assertCount("c", 0)},
		twoPathQuery{name: "EmptyBuildAntiJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n WHERE NOT EXISTS (
				SELECT 1 FROM region r WHERE r.r_regionkey = n.n_regionkey AND r.r_regionkey < 0)`,
			assertA: assertCount("c", 25)},
		// A RIGHT JOIN's preserved rows, by value. The count was right on the
		// single-process arm all along and the values were not: the flush was
		// handed the join's OUTPUT schema as though it were the probe's, so
		// the preserved side's own columns mapped onto the NULL half and
		// every unmatched nation came back nameless.
		twoPathQuery{name: "RightJoinUnmatchedValues", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.r_name FROM region r
				RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey ORDER BY n.n_name`,
			wantRows: 25, wantCols: []string{"n_name", "r_name"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				// The preserved side is never NULL; the other side is NULL on
				// exactly the 20 nations with no region of that key.
				countNulls("n_name", 0, 25)(tb, rows)
				countNulls("r_name", 20, 5)(tb, rows)
			}},
		// A RIGHT JOIN where NOTHING matches: the probe emits no output batch
		// at all, which is what skipped the unmatched flush entirely.
		twoPathQuery{name: "RightJoinNoMatchAtAll", cmp: cmpUnordered, expectRows: true,
			sql:     "SELECT COUNT(*) AS c FROM region r RIGHT JOIN nation n ON r.r_name = n.n_name",
			assertA: assertCount("c", 25)},
		// The mirror of the empty-build case: a RIGHT JOIN whose PROBE side
		// is empty. On the DAG this is the ordinary shape of a shuffle
		// partition that holds build rows and no probe rows.
		twoPathQuery{name: "EmptyProbeRightJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c, COUNT(r.r_name) AS m FROM region r
				RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey AND r.r_regionkey < 0`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				if got := cellNum(rows[0], "c"); got != 25 {
					tb.Errorf("COUNT(*) = %v, want 25 — the preserved side survives an empty probe", got)
				}
				if got := cellNum(rows[0], "m"); got != 0 {
					tb.Errorf("COUNT(r.r_name) = %v, want 0", got)
				}
			}},
		// FULL OUTER carries both halves at once: 25 nations and 5 regions,
		// none matched, 30 rows each NULL-padded on one side.
		twoPathQuery{name: "FullJoinUnmatchedBothSides", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c, COUNT(n.n_name) AS n_rows, COUNT(r.r_name) AS r_rows
				FROM nation n FULL OUTER JOIN region r ON n.n_name = r.r_name`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				for col, want := range map[string]float64{"c": 30, "n_rows": 25, "r_rows": 5} {
					if got := cellNum(rows[0], col); got != want {
						tb.Errorf("%s = %v, want %v", col, got, want)
					}
				}
			}},
		twoPathQuery{name: "FullJoinUnmatchedBothSidesValues", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.r_name FROM nation n FULL OUTER JOIN region r
				ON n.n_name = r.r_name ORDER BY n.n_name, r.r_name`,
			wantRows: 30, wantCols: []string{"n_name", "r_name"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				countNulls("n_name", 5, 25)(tb, rows)
				countNulls("r_name", 25, 5)(tb, rows)
			}},
		// #352.2 — a keyless join. A comma join whose only condition is an
		// inequality is a Cartesian product with a filter above it; the DAG
		// had no operator for a join without equality keys and failed the
		// task outright.
		twoPathQuery{name: "KeylessCrossJoinFilter", cmp: cmpUnordered, expectRows: true,
			sql:     "SELECT COUNT(*) AS c FROM region a, nation b WHERE a.r_regionkey < b.n_nationkey",
			assertA: assertCount("c", 110)},
		twoPathQuery{name: "KeylessCrossJoinValues", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT a.r_name, b.n_name FROM region a, nation b
				WHERE a.r_regionkey < b.n_nationkey AND b.n_nationkey < 3 ORDER BY a.r_name, b.n_name`,
			wantRows: 3, wantCols: []string{"r_name", "n_name"}},
	)

	// #353: the rest of the aggregates that hold extraState — the residual
	// #339 left behind. Two different defects, one per group.
	//
	//	CORR/COVAR_*  decompose exactly, like the variance family, and now
	//	              ship their (count, meanX, meanY, C, M2x, M2y) sextuple
	//	              across the wire (covar_decompose.go).
	//	MEDIAN, PERCENTILE_*, MODE, MIN_BY/MAX_BY, STRING_AGG
	//	              do not decompose at all, and are gated to a
	//	              RawInputAggregate final: one task per group
	//	              (agg_whole_input.go).
	//
	// Every entry carries an assertA computed here from the fixture, because
	// the two arms AGREEING is not the property that matters: before the
	// fix, MODE and MIN_BY agreed on NULL and STRING_AGG agreed on the wrong
	// separator. Arm B additionally answered most of these with the SUM of
	// their first argument, which is what the worker's `default: AggSum`
	// does to a function name it has no case for.
	out = append(out,
		twoPathQuery{name: "CorrCovarFamily", cmp: cmpUnordered,
			sql: `SELECT CORR(o_totalprice, o_custkey) AS c, COVAR_SAMP(o_totalprice, o_custkey) AS cs,
				COVAR_POP(o_totalprice, o_custkey) AS cp FROM orders`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				xs := sf001Column(tb, "orders", "o_totalprice")
				ys := sf001Column(tb, "orders", "o_custkey")
				corr, cs, cp := covarReference(xs, ys)
				assertClose(tb, rows[0], "c", corr, "CORR")
				assertClose(tb, rows[0], "cs", cs, "COVAR_SAMP")
				assertClose(tb, rows[0], "cp", cp, "COVAR_POP")
			}},
		// Grouped, so states combine per key rather than into one global
		// accumulator — the shape a distributed shuffle produces.
		twoPathQuery{name: "CorrGrouped", cmp: cmpOrdered,
			sql: `SELECT o_orderstatus AS k, CORR(o_totalprice, o_custkey) AS c
				FROM orders GROUP BY o_orderstatus ORDER BY k`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				byKey := map[string][2][]float64{}
				for _, r := range sf001Table(tb, "orders") {
					k := fmt.Sprint(r["o_orderstatus"])
					cur := byKey[k]
					byKey[k] = [2][]float64{
						append(cur[0], toFloat(r["o_totalprice"])),
						append(cur[1], toFloat(r["o_custkey"])),
					}
				}
				if len(rows) != len(byKey) {
					tb.Fatalf("%d groups, fixture has %d", len(rows), len(byKey))
				}
				for _, r := range rows {
					k := cellText(r, "k")
					pair, ok := byKey[k]
					if !ok {
						tb.Errorf("result carries group %q, which the fixture does not", k)
						continue
					}
					corr, _, _ := covarReference(pair[0], pair[1])
					assertClose(tb, r, "c", corr, "CORR group "+k)
				}
			}},
		// CORR over one row is NULL (it needs two), and CORR of a column
		// with itself is exactly 1 — a value no accumulation-order
		// difference can drift off.
		twoPathQuery{name: "CorrDegenerate", cmp: cmpUnordered,
			sql: `SELECT CORR(o_totalprice, o_totalprice) AS self,
				COVAR_POP(o_totalprice, o_custkey) AS cp FROM orders WHERE o_orderkey = 1`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				if v, _ := lookupCell(rows[0], "self"); v != nil {
					tb.Errorf("CORR over one row = %v, want NULL", v)
				}
				if v, _ := lookupCell(rows[0], "cp"); v == nil || cellNum(rows[0], "cp") != 0 {
					tb.Errorf("COVAR_POP over one row = %v, want 0", v)
				}
			}},
		twoPathQuery{name: "MedianUngrouped", cmp: cmpUnordered,
			sql: "SELECT MEDIAN(o_totalprice) AS m FROM orders",
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				want := quantileReference(sf001Column(tb, "orders", "o_totalprice"), 0.5)
				assertClose(tb, rows[0], "m", want, "MEDIAN")
			}},
		twoPathQuery{name: "PercentileFamily", cmp: cmpUnordered,
			sql: `SELECT PERCENTILE_CONT(0.9, o_totalprice) AS p90,
				quantile_cont(o_totalprice, 0.9) AS q90 FROM orders`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				want := quantileReference(sf001Column(tb, "orders", "o_totalprice"), 0.9)
				assertClose(tb, rows[0], "p90", want, "PERCENTILE_CONT(0.9)")
				// The two spellings are the same function with the
				// arguments the other way round, so they must not differ.
				assertClose(tb, rows[0], "q90", want, "quantile_cont(0.9)")
			}},
		// MODE over l_linenumber: the winner (1, on every order's first
		// line) beats the runner-up by thousands, so no tie decides it.
		twoPathQuery{name: "ModeNumeric", cmp: cmpUnordered,
			sql: "SELECT MODE(l_linenumber) AS m FROM lineitem",
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				want := modeReference(sf001Column(tb, "lineitem", "l_linenumber"))
				if got := cellNum(rows[0], "m"); got != want {
					tb.Errorf("MODE(l_linenumber) = %v, want %v (most frequent value over the fixture)", got, want)
				}
			}},
		twoPathQuery{name: "MinByMaxBy", cmp: cmpUnordered,
			sql: `SELECT MIN_BY(o_orderpriority, o_totalprice) AS mn,
				MAX_BY(o_orderpriority, o_totalprice) AS mx FROM orders`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				lo, hi := argMinMaxReference(tb, "orders", "o_orderpriority", "o_totalprice")
				if got := cellText(rows[0], "mn"); got != lo {
					tb.Errorf("MIN_BY = %q, want %q (the priority on the cheapest order)", got, lo)
				}
				if got := cellText(rows[0], "mx"); got != hi {
					tb.Errorf("MAX_BY = %q, want %q (the priority on the dearest order)", got, hi)
				}
			}},
		// The deliberate tie: o_shippriority is 0 on all 15000 rows, so
		// every row is tied for both extremes and each partial holds a
		// different candidate. The value column is the group key, so all
		// the tied candidates carry the same value and the answer stays
		// determined — what this pins is that a tie resolves to a tied
		// row's value rather than to NULL, to 0, or to another group's.
		twoPathQuery{name: "MinByMaxByTie", cmp: cmpOrdered,
			sql: `SELECT o_orderpriority AS k, MIN_BY(o_orderpriority, o_shippriority) AS mn,
				MAX_BY(o_orderpriority, o_shippriority) AS mx
				FROM orders GROUP BY o_orderpriority ORDER BY k`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) == 0 {
					tb.Fatal("no rows")
				}
				for _, r := range rows {
					k := cellText(r, "k")
					if got := cellText(r, "mn"); got != k {
						tb.Errorf("group %q: MIN_BY = %q; every row of the group is tied and carries %q", k, got, k)
					}
					if got := cellText(r, "mx"); got != k {
						tb.Errorf("group %q: MAX_BY = %q; every row of the group is tied and carries %q", k, got, k)
					}
				}
			}},
		// STRING_AGG's separator. LENGTH makes it order-independent: the
		// concatenation order of 15000 unordered rows is not part of the
		// answer, but its total length is, and a dropped two-character
		// separator is 14999 characters short.
		twoPathQuery{name: "StringAggSeparator", cmp: cmpUnordered,
			sql: "SELECT LENGTH(STRING_AGG(o_orderpriority, '::')) AS n FROM orders",
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				total, n := 0, 0
				for _, r := range sf001Table(tb, "orders") {
					total += len(fmt.Sprint(r["o_orderpriority"]))
					n++
				}
				want := float64(total + 2*(n-1))
				if got := cellNum(rows[0], "n"); got != want {
					tb.Errorf("LENGTH(STRING_AGG(o_orderpriority, '::')) = %v, want %v "+
						"(%d bytes of values + %d separators of 2)", got, want, total, n-1)
				}
			}},

		// #351 — an ON equality whose operand is an EXPRESSION. Both arms
		// were wrong, so the two-arm compare alone would have passed them:
		// each assertA carries the absolute answer.
		//
		// nation has 5 rows per regionkey 0..4 and region has regionkeys
		// 0..4, so `n_regionkey = r_regionkey + 3` matches regionkeys 3 and
		// 4 — 10 rows. The engine answered 0: the key parser split the text
		// on "=" and gave the executor "r.r_regionkey + 3" as a column name,
		// which resolves to nothing.
		twoPathQuery{name: "ExpressionJoinKeyRight", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey + 3`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 10)
			}},
		twoPathQuery{name: "ExpressionJoinKeyLeft", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey + 3 = r.r_regionkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 10)
			}},
		// Neither operand a column. Both keys resolve to nothing, both hash
		// as the same constant, and every probe row matched every build row:
		// 125 rows — the full cross product — for a 20-row query.
		twoPathQuery{name: "ExpressionJoinKeyBoth", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey + 1 = r.r_regionkey + 2`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 20)
			}},
		// An equality against a LITERAL, the same shape without arithmetic.
		// Five nations carry regionkey 1, and every one of the 5 regions
		// joins to each: 25.
		twoPathQuery{name: "LiteralJoinKey", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey = 1`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 25)
			}},
		// The VALUES, since a count can agree by accident.
		twoPathQuery{name: "ExpressionJoinKeyValues", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.r_name FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey + 3
				ORDER BY n.n_name`,
			wantRows: 10},
		// The expression key inside a three-table join: the residual filter
		// has to survive join reordering, and the remaining real equi-join
		// still has to be planned as one.
		twoPathQuery{name: "ExpressionJoinKeyThreeTable", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey = r.r_regionkey + 3
				JOIN supplier s ON s.s_nationkey = n.n_nationkey`},
		// A non-equi operator that CONTAINS an "=". The text split found
		// that "=" and produced the column name "a.x <". For an inner join
		// the conjunct lifts into the filter above, so the answer is exact:
		// sum over regionkey k of (5 nations x (5-k) regions) = 75.
		twoPathQuery{name: "OnClauseLessOrEqual", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey <= r.r_regionkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 75)
			}},
		twoPathQuery{name: "OnClauseGreaterOrEqual", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey >= r.r_regionkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 75)
			}},
		// 125 pairs less the 25 with equal keys.
		twoPathQuery{name: "OnClauseNotEqual", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON n.n_regionkey != r.r_regionkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 100)
			}},
		// Control: the `1 = 1` ON-TRUE sentinel the optimizer writes when
		// every ON conjunct has been pushed to a child. Not a column
		// reference, and must NOT be refused — region filters to the one
		// AMERICA row and the join is the cross product with it.
		twoPathQuery{name: "OnClauseAllConjunctsPushed", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM nation n JOIN region r ON r.r_regionkey = 1 AND r.r_name = 'AMERICA'`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "c", 25)
			}},

		// #355 — an aggregate over a RENAMED subquery column. DAG-only: a
		// Project emits no stage there, so the rename never happened, the
		// aggregate asked for a column the batch did not have, and
		// HashAggregate answered NULL. o_custkey runs 1..1499 at SF0.01.
		twoPathQuery{name: "MaxOverRenamedSubqueryColumn", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT MAX(n) AS m FROM (SELECT o_custkey AS n FROM orders)",
			assertA: func(tb testing.TB, rows []map[string]any) {
				_, hi := sf001Extent(tb, "orders", "o_custkey")
				assertSingleCount(tb, rows, "m", hi)
			}},
		twoPathQuery{name: "MinOverRenamedSubqueryColumn", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT MIN(n) AS m FROM (SELECT o_custkey AS n FROM orders)",
			assertA: func(tb testing.TB, rows []map[string]any) {
				lo, _ := sf001Extent(tb, "orders", "o_custkey")
				assertSingleCount(tb, rows, "m", lo)
			}},
		twoPathQuery{name: "CountOverRenamedSubqueryColumn", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT COUNT(n) AS m FROM (SELECT o_custkey AS n FROM orders)",
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertSingleCount(tb, rows, "m", float64(len(sf001Table(tb, "orders"))))
			}},
		twoPathQuery{name: "SumOverRenamedSubqueryColumn", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT SUM(n) AS m FROM (SELECT o_custkey AS n FROM orders)",
			assertA: func(tb testing.TB, rows []map[string]any) {
				var want float64
				for _, r := range sf001Table(tb, "orders") {
					want += toFloat(r["o_custkey"])
				}
				assertSingleCount(tb, rows, "m", want)
			}},
		// A CTE rename reaches the same aggregate by a different route.
		twoPathQuery{name: "MaxOverRenamedCTEColumn", cmp: cmpUnordered, expectRows: true,
			sql: "WITH t AS (SELECT o_custkey AS n FROM orders) SELECT MAX(n) AS m FROM t",
			assertA: func(tb testing.TB, rows []map[string]any) {
				_, hi := sf001Extent(tb, "orders", "o_custkey")
				assertSingleCount(tb, rows, "m", hi)
			}},
		// The alias over an EXPRESSION: no source column to read, so the
		// value has to be projected before the aggregate sees it.
		twoPathQuery{name: "MaxOverRenamedSubqueryExpression", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT MAX(n) AS m FROM (SELECT o_custkey * 2 AS n FROM orders)",
			assertA: func(tb testing.TB, rows []map[string]any) {
				_, hi := sf001Extent(tb, "orders", "o_custkey")
				assertSingleCount(tb, rows, "m", 2*hi)
			}},
		// A POLYMORPHIC expression under the alias. Its type is decidable
		// only from the columns it reads, which live below the Project, so
		// typing it against the Project's own output leaves the Float64
		// fallback and every string is dropped — the DAG answered 0 for a
		// nation name (#333's shape reached through #355's rename).
		twoPathQuery{name: "MinOverRenamedSubqueryPolymorphicExpression", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT MIN(u) AS m FROM (SELECT COALESCE(n_name, n_comment) AS u FROM nation)",
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				if got := cellText(rows[0], "m"); got != "ALGERIA" {
					tb.Errorf("MIN = %q, want %q — a numeric fallback type drops every string", got, "ALGERIA")
				}
			}},
		// A GROUP BY key naming a subquery's COMPUTED alias.
		twoPathQuery{name: "GroupByRenamedSubqueryExpression", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT k, COUNT(*) AS c FROM (SELECT UPPER(o_orderstatus) AS k FROM orders)
				GROUP BY k ORDER BY k`,
			wantCols: []string{"k", "c"}, wantRows: 3},
		// A renamed GROUP BY key is the louder half of the same miss: an
		// unresolvable key serializes as a NULL key, so all 15000 rows
		// collapsed into one group named NULL. wantCols pins the OUTPUT
		// name too — resolving the key to its source column must not leak
		// o_orderstatus into the result schema in place of the alias.
		twoPathQuery{name: "GroupByRenamedSubqueryColumn", cmp: cmpOrdered, expectRows: true,
			sql:      `SELECT k, COUNT(*) AS c FROM (SELECT o_orderstatus AS k FROM orders) GROUP BY k ORDER BY k`,
			wantCols: []string{"k", "c"}, wantRows: 3},
		twoPathQuery{name: "GroupByAndAggregateRenamed", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT k, MAX(n) AS m, COUNT(*) AS c
				FROM (SELECT o_orderstatus AS k, o_custkey AS n FROM orders) GROUP BY k ORDER BY k`,
			wantCols: []string{"k", "m", "c"}, wantRows: 3},
		// A two-column aggregate reads InputCol2 through the same lookup.
		twoPathQuery{name: "CorrOverRenamedSubqueryColumns", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT CORR(a, b) AS c FROM (SELECT o_totalprice AS a, o_custkey AS b FROM orders)`},
		// Control: the same subquery without the rename was always correct.
		twoPathQuery{name: "MaxOverUnrenamedSubqueryColumn", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT MAX(o_custkey) AS m FROM (SELECT o_custkey FROM orders)",
			assertA: func(tb testing.TB, rows []map[string]any) {
				_, hi := sf001Extent(tb, "orders", "o_custkey")
				assertSingleCount(tb, rows, "m", hi)
			}},
	)

	// #343 — an explicit NULLS FIRST / NULLS LAST, and #350 — an explicit
	// window frame. Both are "the clause the user wrote is discarded", and
	// both are shapes where the two arms AGREEING proves nothing: before the
	// fixes each path discarded the clause the same way, so every entry here
	// carries an assertA that holds the answer absolutely.
	//
	// The null-placement entries need NULLs the fixture does not contain, so
	// they build them: NULLIF for a key, and a LEFT JOIN that misses for the
	// #343 issue query itself.
	firstNonNull := func(col string) func(testing.TB, []map[string]any) {
		return func(tb testing.TB, rows []map[string]any) {
			tb.Helper()
			if len(rows) == 0 {
				tb.Fatal("no rows")
			}
			if v, _ := lookupCell(rows[0], col); v == nil {
				tb.Errorf("first row's %s is NULL, want a value — NULLS LAST puts NULLs at the END, "+
					"and a DESC key does not reverse that", col)
			}
		}
	}
	firstNull := func(col string) func(testing.TB, []map[string]any) {
		return func(tb testing.TB, rows []map[string]any) {
			tb.Helper()
			if len(rows) == 0 {
				tb.Fatal("no rows")
			}
			if v, _ := lookupCell(rows[0], col); v != nil {
				tb.Errorf("first row's %s is %v, want NULL — NULLS FIRST puts NULLs at the FRONT, "+
					"and a DESC key does not reverse that", col, v)
			}
		}
	}
	out = append(out,
		twoPathQuery{name: "NullsLastOnDescKey", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation
				ORDER BY k DESC NULLS LAST, n_name`,
			assertA: firstNonNull("k")},
		twoPathQuery{name: "NullsFirstOnDescKey", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation
				ORDER BY k DESC NULLS FIRST, n_name`,
			assertA: firstNull("k")},
		twoPathQuery{name: "NullsLastOnAscKey", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation
				ORDER BY k ASC NULLS LAST, n_name`,
			assertA: firstNonNull("k")},
		twoPathQuery{name: "NullsFirstOnAscKey", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation
				ORDER BY k ASC NULLS FIRST, n_name`,
			assertA: firstNull("k")},
		// #343's own query: the NULLs come from a LEFT JOIN that misses, and
		// the key is a string, so the harness value signature is blind to it.
		twoPathQuery{name: "NullsLastLeftJoinMiss", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT o.o_orderstatus AS s, c.c_custkey FROM customer c
				LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 40
				ORDER BY o.o_orderstatus DESC NULLS LAST, c.c_custkey`,
			assertA: firstNonNull("s")},

		// #350 — LAST_VALUE over an explicit whole-partition frame is the
		// partition's last value; without the frame it is the current row's,
		// and both are pinned so a fix cannot trade one for the other.
		twoPathQuery{name: "WindowLastValueFramePair", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n_nationkey, n_name,
				LAST_VALUE(n_name) OVER (ORDER BY n_nationkey
				  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv_all,
				LAST_VALUE(n_name) OVER (ORDER BY n_nationkey) AS lv_default
				FROM nation ORDER BY n_nationkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 25 {
					tb.Fatalf("%d rows, want 25", len(rows))
				}
				last := cellText(rows[len(rows)-1], "n_name")
				for i, r := range rows {
					if got := cellText(r, "lv_all"); got != last {
						tb.Fatalf("row %d: LAST_VALUE over UNBOUNDED FOLLOWING = %q, want %q — "+
							"the explicit frame reaches the partition's end", i, got, last)
					}
					if got, want := cellText(r, "lv_default"), cellText(r, "n_name"); got != want {
						tb.Fatalf("row %d: LAST_VALUE with no frame = %q, want %q — the DEFAULT "+
							"frame ends at the current row and must not change", i, got, want)
					}
				}
			}},
		// A running total against the partition total: the same SUM, the
		// frame the only difference, and a wrong answer is a plausible
		// number rather than an error.
		twoPathQuery{name: "WindowSumFramePair", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n_nationkey, n_regionkey,
				SUM(n_regionkey) OVER (ORDER BY n_nationkey) AS running,
				SUM(n_regionkey) OVER (ORDER BY n_nationkey
				  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total
				FROM nation ORDER BY n_nationkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 25 {
					tb.Fatalf("%d rows, want 25", len(rows))
				}
				var run float64
				for _, r := range rows {
					run += cellNum(r, "n_regionkey")
				}
				var acc float64
				for i, r := range rows {
					acc += cellNum(r, "n_regionkey")
					if got := cellNum(r, "running"); math.Abs(got-acc) > 1e-6 {
						tb.Fatalf("row %d: running SUM = %v, want %v", i, got, acc)
					}
					if got := cellNum(r, "total"); math.Abs(got-run) > 1e-6 {
						tb.Fatalf("row %d: SUM over UNBOUNDED FOLLOWING = %v, want the partition "+
							"total %v", i, got, run)
					}
				}
			}},
		// FIRST_VALUE with a moving lower bound and NTH_VALUE whose frame is
		// too narrow to hold its N — the two shapes that agree with the old
		// frame-blind answer under a whole-partition frame and disagree
		// everywhere else.
		twoPathQuery{name: "WindowMovingLowerBoundFrame", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n_nationkey, n_name,
				FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey
				  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS fv,
				NTH_VALUE(n_name, 2) OVER (ORDER BY n_nationkey
				  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS nv
				FROM nation ORDER BY n_nationkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 25 {
					tb.Fatalf("%d rows, want 25", len(rows))
				}
				for i, r := range rows {
					wantFV := cellText(r, "n_name")
					if i > 0 {
						wantFV = cellText(rows[i-1], "n_name")
					}
					if got := cellText(r, "fv"); got != wantFV {
						tb.Fatalf("row %d: FIRST_VALUE over 1 PRECEDING = %q, want %q", i, got, wantFV)
					}
					v, _ := lookupCell(r, "nv")
					if i == 0 {
						if v != nil {
							tb.Fatalf("row 0: NTH_VALUE(n_name, 2) = %v, want NULL — its frame "+
								"holds one row", v)
						}
						continue
					}
					if got, want := cellText(r, "nv"), cellText(r, "n_name"); got != want {
						tb.Fatalf("row %d: NTH_VALUE(n_name, 2) = %q, want %q — the second row of "+
							"a two-row frame is the current one", i, got, want)
					}
				}
			}},
		// #340 — CAST(x AS DATE). The cast produces a value the projection
		// then has to declare a type for, so the shape has two independent
		// halves that can disagree: what the evaluator computes and what
		// the output column can hold. The stage DAG builds its projections
		// from the same planner rules but materializes every stage to S3 in
		// between, so a date that survives in-process and a date that
		// survives a round trip through a parquet column are two claims.
		twoPathQuery{name: "DateCastLiteralArithmetic", cmp: cmpUnordered,
			sql: `SELECT DATE '1996-01-10' - DATE '1996-01-01' AS gap,
				CAST('1996-01-10' AS DATE) - 1 AS prev, CAST('1996-01-10' AS DATE) + 5 AS nxt,
				CAST('1996-01-10' AS DATE) AS d FROM region WHERE r_regionkey = 0`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				if got := cellNum(rows[0], "gap"); got != 9 {
					tb.Errorf("DATE '1996-01-10' - DATE '1996-01-01' = %v, want 9 "+
						"(0 means both operands were still strings)", got)
				}
				for _, c := range []struct{ col, want string }{
					{"prev", "1996-01-09"}, {"nxt", "1996-01-15"}, {"d", "1996-01-10"},
				} {
					if got := cellText(rows[0], c.col); got != c.want {
						tb.Errorf("%s = %q, want %q", c.col, got, c.want)
					}
				}
			}},
		// The per-row form over real rows: a shipping lag that is the same
		// number on every line is the defect, not an answer.
		twoPathQuery{name: "DateCastShippingLag", cmp: cmpOrdered,
			sql: `SELECT l_orderkey, l_linenumber, l_shipdate, l_receiptdate,
				CAST(l_receiptdate AS DATE) - CAST(l_shipdate AS DATE) AS lag
				FROM lineitem WHERE l_orderkey <= 10 ORDER BY l_orderkey, l_linenumber`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) == 0 {
					tb.Fatal("no rows")
				}
				// The dates themselves are projected beside the lag, so the
				// answer is checked against the calendar rather than against
				// itself. Both operands used to reach the operator as text
				// and ToFloat64 read a YEAR out of each, which is 0 within a
				// year and looks varied across one — the shape a
				// "the answers differ from each other" check would pass.
				distinct := map[float64]bool{}
				for _, r := range rows {
					ship, serr := time.Parse("2006-01-02", cellText(r, "l_shipdate"))
					recv, rerr := time.Parse("2006-01-02", cellText(r, "l_receiptdate"))
					if serr != nil || rerr != nil {
						tb.Fatalf("fixture dates are not calendar dates: %v / %v",
							cellText(r, "l_shipdate"), cellText(r, "l_receiptdate"))
					}
					want := recv.Sub(ship).Hours() / 24
					got := cellNum(r, "lag")
					if got != want {
						tb.Errorf("l_orderkey %v line %v: %s - %s = %v, want %v days",
							cellText(r, "l_orderkey"), cellText(r, "l_linenumber"),
							cellText(r, "l_receiptdate"), cellText(r, "l_shipdate"), got, want)
					}
					distinct[got] = true
				}
				if len(distinct) < 2 {
					tb.Errorf("every row answered the same lag (%v) — a constant column is the defect",
						cellNum(rows[0], "lag"))
				}
			}},
		twoPathQuery{name: "DateCastGroupKey", cmp: cmpOrdered,
			sql: `SELECT CAST(l_shipdate AS DATE) AS k, COUNT(*) AS c FROM lineitem
				WHERE l_shipdate < '1992-01-20' GROUP BY CAST(l_shipdate AS DATE) ORDER BY k`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) == 0 {
					tb.Fatal("no rows")
				}
				for _, r := range rows {
					if got := cellText(r, "k"); len(got) != 10 || got[4] != '-' || got[7] != '-' {
						tb.Fatalf("group key %q is not a calendar date", got)
					}
				}
			}},
		twoPathQuery{name: "DateCastComparison", cmp: cmpUnordered,
			sql: `SELECT COUNT(*) AS c FROM lineitem WHERE CAST(l_shipdate AS DATE) < DATE '1994-01-01'`},
		twoPathQuery{name: "DateCastMinMax", cmp: cmpUnordered,
			sql: `SELECT MIN(CAST(l_shipdate AS DATE)) AS mn, MAX(CAST(l_shipdate AS DATE)) AS mx FROM lineitem`},
		// The TIMESTAMP half, through the shapes that render the same on
		// both paths.
		twoPathQuery{name: "TimestampCastThroughDate", cmp: cmpOrdered,
			sql: `SELECT o_orderkey, CAST(CAST(o_orderdate AS TIMESTAMP) AS DATE) AS d,
				YEAR(CAST(o_orderdate AS TIMESTAMP)) AS y
				FROM orders WHERE o_orderkey <= 10 ORDER BY o_orderkey`},
		twoPathQuery{name: "TimestampCastComparison", cmp: cmpUnordered,
			sql: `SELECT COUNT(*) AS c FROM lineitem
				WHERE CAST(l_shipdate AS TIMESTAMP) < TIMESTAMP '1994-01-01 00:00:00'`},

		// Correlated subqueries reading an outer column the outer query
		// mentions nowhere else. Column pruning dropped it — the pruning walk
		// had no case for a subquery node — the per-row evaluator found no
		// such column in the batch and substituted NULL, and every comparison
		// went UNKNOWN (#347). Arm A answered 0, arm B answered 0, so the
		// two-arm compare alone would have passed all of them: every entry
		// carries an absolute assertion recomputed from the fixture rows.
		//
		// The correlation is NON-EQUI on purpose. An `=` correlation is
		// decorrelated into a join before pruning runs and never executes per
		// row, so an equality version of any of these passes with the bug
		// present and proves nothing.
		//
		// These (and the Correlated* entries appended at the corpus end) run
		// arm B through the #359 route: the stage DAG refuses a
		// per-row correlated subquery (PlanDistributed returns
		// physical.ErrCorrelatedSubqueryDistributed — there is no distributed
		// lowering for a correlation that survives decorrelation) and the
		// coordinator answers on its local single-process pipeline. The
		// localRoute flag makes the runner assert that route engaged. Before
		// it existed, EXISTS failed the task and a correlated scalar was
		// mis-deferred to a producer stage whose dangling outer reference
		// evaluated NULL — arm B answered 0 for every scalar form here,
		// including CorrelatedScalarProjectedOuterCol, which has nothing to
		// do with pruning. #347's own gate is the arm-A half of
		// TestDuckDBCompare plus TestCorrelatedOuterColumnPruning.
		twoPathQuery{name: "CorrelatedScalarUnprojectedOuterCol", cmp: cmpUnordered, expectRows: true, localRoute: true,
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey)`,
			assertA: assertCorrelatedScalarCount("c_nationkey is read only by the subquery")},
		// The control: same correlation, outer column forced into a
		// projection so pruning cannot drop it. Correct before the fix and
		// after — the pair is what localizes the defect to pruning.
		twoPathQuery{name: "CorrelatedScalarProjectedOuterCol", cmp: cmpUnordered, expectRows: true, localRoute: true,
			sql: `SELECT COUNT(*) AS n FROM (SELECT c_nationkey, c_acctbal FROM customer) c1
				WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey)`,
			assertA: assertCorrelatedScalarCount("the same correlation with the column projected")},
		twoPathQuery{name: "CorrelatedExistsUnprojectedOuterCol", cmp: cmpUnordered, expectRows: true, localRoute: true,
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE EXISTS (SELECT 1 FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`,
			assertA: assertCorrelatedExistsCount("EXISTS over an unprojected correlation", false)},
		// The complement, and the reason a row count is not a check: the
		// NULL substitution made this one return the WHOLE table rather than
		// nothing, which looks entirely plausible.
		twoPathQuery{name: "CorrelatedNotExistsUnprojectedOuterCol", cmp: cmpUnordered, expectRows: true, localRoute: true,
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE NOT EXISTS (SELECT 1 FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`,
			assertA: assertCorrelatedExistsCount("NOT EXISTS over an unprojected correlation", true)},

		// #375: a five-table join chain whose unqualified WHERE compares
		// columns of DIFFERENT types across the chain (o_totalprice is
		// FLOAT64, r_regionkey INT32). The vectorized col-col filter kernel
		// resolved from the LEFT column's type only and read the right
		// vector's empty Float64Data — a panic, `index out of range [i] with
		// length 0`. The QUALIFIED spelling of the same WHERE never saw the
		// kernel (dotted names take the row-at-a-time path), which is why the
		// unqualified form is load-bearing here. The absolute answer is
		// pinned against DuckDB by this entry's twin in duckdbCorpus (this
		// suite runs on its own generated data, so no row count is pinned
		// here); pre-fix, BOTH arms panicked, so expectRows plus the two-arm
		// row compare is what this entry holds.
		twoPathQuery{name: "MixedTypeCrossTableFilterJoinChain", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT t4.o_orderkey AS c8
				FROM customer t0
				JOIN nation   t1 ON t0.c_nationkey = t1.n_nationkey
				JOIN region   t2 ON t1.n_regionkey = t2.r_regionkey
				JOIN supplier t3 ON t1.n_nationkey = t3.s_nationkey
				LEFT JOIN orders t4 ON t0.c_custkey = t4.o_custkey
				WHERE o_totalprice <> r_regionkey`},
		// #378: ORDER BY an aliased column that is also the projected column.
		// In a parallel pipeline the primary Sort could finish having
		// consumed nothing itself (warmup batch fully filtered, every source
		// batch claimed by a clone worker), and MergeSink handed it the
		// clones' batches but not their schema — finalize then gathered the
		// merged rows into ZERO output columns: right row count, rows with no
		// columns at all, varying run to run with goroutine scheduling. The
		// assertA half holds even when both arms fail alike: every row must
		// CARRY the projected column, in the asked-for order.
		twoPathQuery{name: "OrderByAliasedJoinColumnAlsoProjected", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT t1.ps_suppkey AS c6
				FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey
				WHERE t1.ps_partkey > 500
				ORDER BY t1.ps_suppkey`,
			wantCols: []string{"c6"}, wantRows: 6000,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				for i, r := range rows {
					if _, ok := r["c6"]; !ok {
						tb.Fatalf("row %d carries no c6 column (%v) — the #378 rows-with-no-columns mode", i, r)
					}
				}
				assertOrderedBy(tb, rows, false, "c6", func(r map[string]any) float64 { return cellNum(r, "c6") })
			}},
		// BOOL_AND/BOOL_OR answered false regardless of input (#371): no
		// boolean-valued node declared a type, so the pre-aggregate
		// projection fell back to Float64 and dropped every boolean write —
		// on BOTH engines, which is why the absolute assertion carries the
		// weight here and the two-arm compare alone would have passed.
		twoPathQuery{name: "BoolAggregates", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT BOOL_AND(n_nationkey >= 0) AS all_nonneg,
				BOOL_OR(n_nationkey > 3) AS any_big,
				BOOL_AND(n_nationkey > 3) AS all_big FROM nation`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("got %d rows, want 1", len(rows))
				}
				if got := cellText(rows[0], "all_nonneg"); got != "true" {
					tb.Errorf("all_nonneg = %s, want true (every n_nationkey is >= 0)", got)
				}
				if got := cellText(rows[0], "any_big"); got != "true" {
					tb.Errorf("any_big = %s, want true (n_nationkey reaches 24)", got)
				}
				if got := cellText(rows[0], "all_big"); got != "false" {
					tb.Errorf("all_big = %s, want false (n_nationkey 0 exists)", got)
				}
			}},
		// The grouped form drives the DAG's partial/final merge of the
		// boolean accumulators (mergeSinkState's bool arm).
		twoPathQuery{name: "BoolAggregatesGrouped", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n_regionkey, BOOL_AND(n_nationkey > 2) AS all_late,
				BOOL_OR(n_nationkey > 20) AS any_tail
				FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 5 {
					tb.Fatalf("got %d rows, want 5 regions", len(rows))
				}
				// Region 0 holds ALGERIA (n_nationkey 0): all_late false.
				// Region 3 (EUROPE) is {6,7,19,22,23}: all_late true and
				// any_tail true (22, 23).
				if got := cellText(rows[0], "all_late"); got != "false" {
					tb.Errorf("region 0 all_late = %s, want false", got)
				}
				if got := cellText(rows[3], "all_late"); got != "true" {
					tb.Errorf("region 3 all_late = %s, want true", got)
				}
				if got := cellText(rows[3], "any_tail"); got != "true" {
					tb.Errorf("region 3 any_tail = %s, want true", got)
				}
			}},
		// MIN/MAX over a string CASE answered the integer 0 (#372): a CASE
		// declared no type, the aggregate's input projection fell back to
		// Float64, and the string branches were dropped. The three control
		// columns were always correct, which localizes the trigger.
		twoPathQuery{name: "MinMaxOverStringCase", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT MIN(n_name) AS bare, MIN(LOWER(n_name)) AS fn,
				MIN(n_name || 'x') AS cat,
				MIN(CASE WHEN n_regionkey = 0 THEN n_name ELSE n_name END) AS case_expr
				FROM nation`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 1 {
					tb.Fatalf("got %d rows, want 1", len(rows))
				}
				for col, want := range map[string]string{
					"bare": "ALGERIA", "fn": "algeria", "cat": "ALGERIAx", "case_expr": "ALGERIA",
				} {
					if got := cellText(rows[0], col); got != want {
						tb.Errorf("%s = %q, want %q", col, got, want)
					}
				}
			}},
		// MIN/MAX over a window resolve from the input column since #361;
		// n_nationkey is INT32, the exact width whose values Vector.SetValue
		// dropped into the old float64-declared output (0 on every row).
		// The running MAX over the ordering key equals the key itself, so a
		// flat-zero column cannot pass the assertion.
		twoPathQuery{name: "WindowMinMaxNarrowInt", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n_nationkey,
				MAX(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) AS mx,
				MIN(n_name) OVER (ORDER BY n_nationkey) AS mn
				FROM nation ORDER BY n_nationkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				if len(rows) != 25 {
					tb.Fatalf("got %d rows, want 25", len(rows))
				}
				for i, r := range rows {
					if key, mx := cellNum(r, "n_nationkey"), cellNum(r, "mx"); mx != key {
						tb.Errorf("row %d: running MAX over the ordering key = %v, want %v", i, mx, key)
					}
					if got := cellText(r, "mn"); got != "ALGERIA" {
						tb.Errorf("row %d: mn = %q, want ALGERIA", i, got)
					}
				}
			}},
		// #359, the remaining correlated shapes. Both ride the same route as
		// the Correlated* family above (PlanDistributed refusal → coordinator
		// -local execution, asserted by localRoute). The SELECT-list form is
		// the one that failed the task at PROJECTION compile pre-fix; the
		// nested form makes the correlation analysis recurse two subqueries
		// deep before it can refuse.
		// The CAST pins the projected value as a number: the single-process
		// pipeline otherwise types a computed subquery column as a STRING,
		// and string cells compare byte-exact — the two arms' AVGs
		// legitimately differ in the last float digits (their fixtures load
		// in different chunk orders), which only the numeric comparison is
		// allowed to absorb.
		twoPathQuery{name: "CorrelatedScalarInSelectList", cmp: cmpOrdered, expectRows: true, localRoute: true,
			sql: `SELECT c_custkey, c_nationkey,
				CAST((SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey) AS DOUBLE) AS below_avg
				FROM customer c1 WHERE c1.c_custkey <= 25 ORDER BY c_custkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 25 {
					tb.Fatalf("%d rows, want 25", len(rows))
				}
				cust := sf001Table(tb, "customer")
				for i, r := range rows {
					key := cellNum(r, "c_nationkey")
					sum, n := 0.0, 0
					for _, o := range cust {
						if toFloat(o["c_nationkey"]) < key {
							sum += toFloat(o["c_acctbal"])
							n++
						}
					}
					v, present := lookupCell(r, "below_avg")
					if !present {
						tb.Fatalf("row %d carries no below_avg column — the correlated projection was dropped", i)
					}
					if n == 0 {
						// Empty inner set: AVG is NULL. The single-process
						// pipeline types the computed column as a string, so
						// NULL may surface as nil or as the empty string.
						if v != nil && cellText(r, "below_avg") != "" {
							tb.Errorf("row %d (nationkey %v): below_avg = %v, want NULL (empty inner set)", i, key, v)
						}
						continue
					}
					want := sum / float64(n)
					if got := cellNum(r, "below_avg"); math.Abs(got-want) > 1e-6*math.Max(1, math.Abs(want)) {
						tb.Errorf("row %d (nationkey %v): below_avg = %v, want %v", i, key, got, want)
					}
				}
			}},
		twoPathQuery{name: "CorrelatedNestedTwoDeep", cmp: cmpUnordered, expectRows: true, localRoute: true,
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_acctbal > (SELECT AVG(c2.c_acctbal) FROM customer c2
					WHERE c2.c_acctbal > (SELECT AVG(c3.c_acctbal) FROM customer c3
						WHERE c3.c_nationkey < c1.c_nationkey))`,
			assertA: assertCorrelatedNestedCount("the outermost row is read two subqueries deep")},
	)

	// INTERSECT / EXCEPT on the stage DAG (#346, second half). Until this
	// landed the DAG refused all four forms at plan time, so every entry
	// here failed with "arm B failed while arm A returned N rows". The DAG
	// lowering is grouped counting — tag each arm's rows, hash the full row
	// across an exchange, SUM the tags per distinct row, emit per the
	// operation's count rule — so what these entries pin is exactly the
	// places that lowering could go quietly wrong: multiplicity (ALL vs
	// distinct differ only when one arm holds duplicates), NULL membership
	// (equal to NULL, like GROUP BY), positional column semantics, empty
	// arms, cross-arm type widening, and operators stacked above the
	// operation. Fixture facts the absolute assertions lean on: nation has
	// 25 rows, exactly 5 per region key 0–4; region has one row per key.
	assertRowCount := func(want int) func(testing.TB, []map[string]any) {
		return func(tb testing.TB, rows []map[string]any) {
			tb.Helper()
			if len(rows) != want {
				tb.Errorf("arm A (single-process) returned %d rows, want %d", len(rows), want)
			}
		}
	}
	out = append(out,
		// Distinct forms collapse duplicates WITHIN the surviving arm too:
		// nation carries every region key five times, and the answer holds
		// each key once. The arms also name their columns differently — the
		// result takes the FIRST arm's name, which wantCols pins on both
		// arms (the compare alone realigns columns and would miss it).
		twoPathQuery{name: "IntersectDistinct", cmp: cmpUnordered, expectRows: true,
			sql:      "SELECT n_regionkey FROM nation INTERSECT SELECT r_regionkey FROM region",
			wantCols: []string{"n_regionkey"}, wantRows: 5,
			assertA: assertRowCount(5)},
		// INTERSECT ALL is min(countA, countB) per row: 5 copies in nation,
		// 1 in region → one copy each. 25 rows back is a missing min; 5 is
		// the answer.
		twoPathQuery{name: "IntersectAllMinCounts", cmp: cmpUnordered, expectRows: true,
			sql:      "SELECT n_regionkey FROM nation INTERSECT ALL SELECT r_regionkey FROM region",
			wantCols: []string{"n_regionkey"}, wantRows: 5,
			assertA: assertRowCount(5)},
		// EXCEPT is distinct rows of A absent from B: keys 0 and 1 are
		// removed by the filtered region arm, keys 2–4 survive once each.
		twoPathQuery{name: "ExceptDistinct", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT n_regionkey FROM nation EXCEPT " +
				"SELECT r_regionkey FROM region WHERE r_regionkey < 2",
			wantCols: []string{"n_regionkey"}, wantRows: 3,
			assertA: assertRowCount(3)},
		// EXCEPT ALL is max(0, countA−countB): 5−1 = 4 copies per key, 20
		// rows. 5 rows is the distinct form leaking in; 25 is a dropped
		// subtraction.
		twoPathQuery{name: "ExceptAllCountDiff", cmp: cmpUnordered, expectRows: true,
			sql:      "SELECT n_regionkey FROM nation EXCEPT ALL SELECT r_regionkey FROM region",
			wantCols: []string{"n_regionkey"}, wantRows: 20,
			assertA: func(tb testing.TB, rows []map[string]any) {
				perKey := map[float64]int{}
				for _, r := range rows {
					perKey[cellNum(r, "n_regionkey")]++
				}
				for k := 0.0; k < 5; k++ {
					if perKey[k] != 4 {
						tb.Errorf("key %v appears %d times, want 4 (5 copies in nation − 1 in region)", k, perKey[k])
					}
				}
			}},
		// NULL is a member like any other and matches the other arm's NULL —
		// the same equality GROUP BY uses, and on the DAG the same property
		// the shuffle hash must preserve for the NULL-carrying rows to meet
		// in one partition. NULLIF sends region key 1 to NULL in both arms:
		// the intersection is {0, 2, 3, 4, NULL}.
		twoPathQuery{name: "IntersectNullsMatch", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT NULLIF(n_regionkey, 1) AS k FROM nation INTERSECT " +
				"SELECT NULLIF(r_regionkey, 1) AS k FROM region",
			wantCols: []string{"k"}, wantRows: 5,
			assertA: func(tb testing.TB, rows []map[string]any) {
				nulls := 0
				for _, r := range rows {
					if v, ok := lookupCell(r, "k"); ok && v == nil {
						nulls++
					}
				}
				if nulls != 1 {
					tb.Errorf("%d NULL rows, want exactly 1 — NULLs must match each other, once", nulls)
				}
			}},
		// An empty arm: INTERSECT with nothing is nothing. wantRows cannot
		// pin zero (0 means unasserted), so the absolute assertion does.
		twoPathQuery{name: "IntersectEmptyArm", cmp: cmpUnordered,
			sql: "SELECT n_regionkey FROM nation WHERE n_nationkey < 0 INTERSECT " +
				"SELECT r_regionkey FROM region",
			assertA: assertRowCount(0)},
		// EXCEPT with an empty B is DISTINCT A — the subtraction of nothing
		// still dedups.
		twoPathQuery{name: "ExceptEmptyRight", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT n_regionkey FROM nation EXCEPT " +
				"SELECT r_regionkey FROM region WHERE r_regionkey < 0",
			wantCols: []string{"n_regionkey"}, wantRows: 5,
			assertA: assertRowCount(5)},
		// EXCEPT with an empty A is empty, whatever B holds.
		twoPathQuery{name: "ExceptEmptyLeft", cmp: cmpUnordered,
			sql: "SELECT n_regionkey FROM nation WHERE n_nationkey < 0 EXCEPT " +
				"SELECT r_regionkey FROM region",
			assertA: assertRowCount(0)},
		// Cross-arm type widening (reconcileSetOpArmTypes): one arm computes
		// an integer, the other a float. Without the reconciling CAST the
		// arms' rows can never be equal on the DAG — two .wshf files
		// declaring different types for one column don't even decode as one
		// stream. Integral values on purpose: equality is the point, not
		// float formatting.
		twoPathQuery{name: "IntersectTypeWidening", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT n_regionkey + 100 AS k FROM nation INTERSECT " +
				"SELECT r_regionkey + 100.0 AS k FROM region",
			wantCols: []string{"k"}, wantRows: 5,
			assertA: assertRowCount(5)},
		// ORDER BY above the operation sorts the WHOLE result by the result
		// column (the first arm's name).
		twoPathQuery{name: "IntersectOrderBy", cmp: cmpOrdered, expectRows: true,
			sql: "SELECT n_regionkey FROM nation INTERSECT SELECT r_regionkey FROM region " +
				"ORDER BY n_regionkey",
			wantCols: []string{"n_regionkey"}, wantRows: 5,
			assertA: func(tb testing.TB, rows []map[string]any) {
				assertOrderedBy(tb, rows, false, "n_regionkey", func(r map[string]any) float64 {
					return cellNum(r, "n_regionkey")
				})
			}},
		// LIMIT above the operation, verbatim (count-compared — a bare cut
		// does not say WHICH rows) and with the limit binding on both arms.
		twoPathQuery{name: "ExceptAllOrderByLimit", cmp: cmpCount, limit: 7, expectRows: true,
			sql: "SELECT n_regionkey FROM nation EXCEPT ALL SELECT r_regionkey FROM region " +
				"ORDER BY n_regionkey LIMIT 7",
			wantCols: []string{"n_regionkey"}, wantRows: 7},
		// A WHERE above the operation names the result column and must run
		// over the operation's OUTPUT — after the counting, not inside an
		// arm. INTERSECT ALL leaves one copy per key here, so the filter's
		// answer is exactly the keys below 3.
		twoPathQuery{name: "IntersectAllFilteredAbove", cmp: cmpUnordered, expectRows: true,
			sql: "SELECT k FROM (SELECT n_regionkey AS k FROM nation INTERSECT ALL " +
				"SELECT r_regionkey FROM region) u WHERE k < 3",
			wantCols: []string{"k"}, wantRows: 3,
			assertA: assertRowCount(3)},
	)

	// #379: DISTINCT over a derived key whose polymorphic type needs the
	// catalog. COALESCE(l_extendedprice, 0) is Float64 because the column
	// is; typed from the expression text alone, the literal decides Int64
	// and the DAG's pre-aggregate projection truncates every float price
	// into the key vector — a fifth of the distinct groups vanish, on the
	// DAG only (fast path 59828 vs DAG 45000 on this fixture; DuckDB truth
	// over the committed bytes is pinned by the DistinctCoalesce* twins in
	// duckdbCorpus). The two-arm compare would catch a recurrence on either
	// arm; the absolute assertion recomputes the distinct set from the
	// fixture rows so a defect that hits both engines the same way cannot
	// slip past as agreement.
	out = append(out,
		// The minimal trigger: no join at all. Exercises the fused
		// scan-aggregate dispatch path.
		twoPathQuery{name: "DistinctCoalesceLiteralScan", cmp: cmpUnordered, expectRows: true,
			sql:      `SELECT DISTINCT COALESCE(l_extendedprice, 0) AS c1 FROM lineitem`,
			wantCols: []string{"c1"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				want := map[float64]bool{}
				for _, r := range sf001Table(tb, "lineitem") {
					want[toFloat(r["l_extendedprice"])] = true
				}
				assertDistinctSet(tb, rows, "c1", want)
			}},
		// The shape the fuzzer found (#379, seed 95): the same key above a
		// LEFT JOIN whose NULL-padded side is what the COALESCE is for.
		// Exercises the chain-terminal partial aggregate (join-fused), so
		// both partial-aggregate dispatch paths stay pinned.
		twoPathQuery{name: "DistinctCoalesceOverLeftJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT DISTINCT COALESCE(t2.l_extendedprice, 0) AS c1
				FROM customer t0
				JOIN orders t1 ON t0.c_custkey = t1.o_custkey
				LEFT JOIN lineitem t2 ON t1.o_orderkey = t2.l_orderkey`,
			wantCols: []string{"c1"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				// LEFT JOIN semantics recomputed from the fixture: prices
				// of lineitems whose order has a customer, plus 0 if any
				// such order has no lineitem at all.
				custKeys := map[float64]bool{}
				for _, r := range sf001Table(tb, "customer") {
					custKeys[toFloat(r["c_custkey"])] = true
				}
				orderKeys := map[float64]bool{}
				for _, r := range sf001Table(tb, "orders") {
					if custKeys[toFloat(r["o_custkey"])] {
						orderKeys[toFloat(r["o_orderkey"])] = true
					}
				}
				want := map[float64]bool{}
				matched := map[float64]bool{}
				for _, r := range sf001Table(tb, "lineitem") {
					ok := toFloat(r["l_orderkey"])
					if orderKeys[ok] {
						want[toFloat(r["l_extendedprice"])] = true
						matched[ok] = true
					}
				}
				if len(matched) < len(orderKeys) {
					want[0] = true // NULL-padded rows COALESCE to 0
				}
				assertDistinctSet(tb, rows, "c1", want)
			}},
	)

	// #358 — the outer-join ON residual: the non-key conjunct runs on the
	// combined row BEFORE the match is accepted, a probe row whose
	// candidates all fail comes back NULL-padded rather than dropped, and a
	// build row is matched only when some probe row passed key AND residual
	// (the RIGHT/FULL unmatched flush reads that bit). Both arms REFUSED
	// every one of these between #351 and #358, so the assertA fixture
	// truths — DuckDB-verified in the cross-engine gate — are what pin the
	// semantics, not mere agreement. The DAG arm additionally polices the
	// replicated-build hazard: RIGHT/FULL never broadcast, and a fix that
	// broke that gate multiplies the unmatched rows by the worker count.
	//
	// #376 — a comma cross join mixed with an ON join: the comma relation
	// contributes no ON clause, and the reorderer's disconnected-relation
	// arm spelled it as an inner join with an EMPTY condition, refused at
	// key extraction on arm A and dispatched as a keyless hash_join arm B's
	// worker rejects. An absent condition IS a cross join (#352 gave the
	// DAG the Cartesian path).
	out = append(out,
		twoPathQuery{name: "OuterJoinResidualLeft", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c, COUNT(r.r_name) AS matched FROM nation n
				LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey`,
			// Every nation is preserved; it carries its region only when the
			// residual held, so `matched` is the fixture count of nations
			// with n_nationkey > n_regionkey.
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				nk := sf001Column(tb, "nation", "n_nationkey")
				rk := sf001Column(tb, "nation", "n_regionkey")
				matched := 0
				for i := range nk {
					if nk[i] > rk[i] {
						matched++
					}
				}
				if got := int(cellNum(rows[0], "c")); got != len(nk) {
					tb.Errorf("c = %d, want %d (every probe row preserved)", got, len(nk))
				}
				if got := int(cellNum(rows[0], "matched")); got != matched {
					tb.Errorf("matched = %d, want %d", got, matched)
				}
			}},
		twoPathQuery{name: "OuterJoinResidualRightFlush", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c, COUNT(r.r_name) AS matched FROM region r
				RIGHT JOIN nation n ON r.r_regionkey = n.n_regionkey AND n.n_nationkey > r.r_regionkey`,
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				if len(rows) != 1 {
					tb.Fatalf("%d rows, want 1", len(rows))
				}
				nk := sf001Column(tb, "nation", "n_nationkey")
				rk := sf001Column(tb, "nation", "n_regionkey")
				matched := 0
				for i := range nk {
					if nk[i] > rk[i] {
						matched++
					}
				}
				if got := int(cellNum(rows[0], "c")); got != len(nk) {
					tb.Errorf("c = %d, want %d (RIGHT preserves every nation)", got, len(nk))
				}
				if got := int(cellNum(rows[0], "matched")); got != matched {
					tb.Errorf("matched = %d, want %d (a 3× answer here is the replicated-build hazard)",
						got, matched)
				}
			}},
		twoPathQuery{name: "OuterJoinResidualFullBothSides", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.r_name FROM nation n
				FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey
				ORDER BY n.n_name, r.r_name`,
			wantCols: []string{"n_name", "r_name"}},
		twoPathQuery{name: "OuterJoinResidualKeyless", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.r_name FROM nation n
				LEFT JOIN region r ON n.n_regionkey = r.r_regionkey + 3 ORDER BY n.n_name`,
			wantRows: 25, wantCols: []string{"n_name", "r_name"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				// Regions 0 and 1 match nations of regionkey 3 and 4 — ten
				// matches, fifteen padded rows.
				countNulls("r_name", 15, 10)(tb, rows)
			}},
		twoPathQuery{name: "CommaJoinAfterOnJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM region t0
				JOIN nation t1 ON t0.r_regionkey = t1.n_regionkey, supplier t2`,
			assertA: assertCount("c", 2500)},
		twoPathQuery{name: "CommaJoinBeforeOnJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM region t0,
				nation t1 JOIN supplier t2 ON t1.n_nationkey = t2.s_nationkey`,
			assertA: assertCount("c", 500)},
		twoPathQuery{name: "CrossJoinMixedWithOnJoin", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM region t0
				JOIN nation t1 ON t0.r_regionkey = t1.n_regionkey CROSS JOIN supplier t2`,
			assertA: assertCount("c", 2500)},
		twoPathQuery{name: "CommaJoinMixtureWhereFilter", cmp: cmpUnordered, expectRows: true,
			sql: `SELECT COUNT(*) AS c FROM region t0
				JOIN nation t1 ON t0.r_regionkey = t1.n_regionkey, supplier t2
				WHERE t1.n_nationkey = t2.s_nationkey`,
			assertA: assertCount("c", 100)},

		// --- #383: a computed subquery projection feeding a join input ---
		//
		// walkStages treats a subquery's Project as a passthrough, so a
		// COMPUTED column existed nowhere on the DAG — the build/probe
		// files never carried it, and the residual (or the projected
		// output, or a sort key) read NULL, silently. Fixed by
		// materializing it into the producing scan fragment
		// (absorbComputedSubqueryProjection); renames still resolve
		// through per consumer.
		twoPathQuery{name: "JoinBuildComputedResidual", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.rk2 FROM nation n
				LEFT JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
				ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.rk2
				ORDER BY n.n_name`,
			wantRows: 25, wantCols: []string{"n_name", "rk2"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				// A NULL residual is a REJECTION (UNKNOWN is not TRUE): the
				// five region-2 nations pad because rk2 is NULL there, and
				// ALGERIA (0 > 0), ARGENTINA (1 > 1) and EGYPT (4 > 4) pad
				// because the residual is plainly false.
				countNulls("rk2", 8, 17)(tb, rows)
			}},
		twoPathQuery{name: "JoinProbeComputedProjected", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT nx.n_name, nx.nk2, r.r_name
				FROM (SELECT n_name, n_regionkey, NULLIF(n_nationkey, 3) AS nk2 FROM nation) nx
				JOIN region r ON nx.n_regionkey = r.r_regionkey ORDER BY nx.n_name`,
			wantRows: 25, wantCols: []string{"n_name", "nk2", "r_name"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				// Exactly CANADA's nationkey is NULLIF'd away.
				countNulls("nk2", 1, 24)(tb, rows)
			}},
		twoPathQuery{name: "SubqueryComputedOrderBy", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT rk2 FROM (SELECT NULLIF(r_regionkey, 2) AS rk2 FROM region) t
				ORDER BY rk2`,
			wantRows: 5, wantCols: []string{"rk2"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				countNulls("rk2", 1, 4)(tb, rows)
				// The sort must actually key on the materialized alias:
				// ascending, NULL last (PostgreSQL default).
				if len(rows) == 5 && rows[4]["rk2"] != nil {
					tb.Errorf("last row rk2 = %v, want NULL last — the ORDER BY did not bind", rows[4]["rk2"])
				}
			}},

		// --- #384: WHERE on a computed subquery alias ---
		//
		// pushdownPredicates' Filter-Project swap used to push the
		// predicate below the computing Project unsubstituted, so the
		// filter named a column its input schema did not carry: the
		// single-process arm errored and the DAG silently returned 0 rows.
		// Fixed by substituting the alias's defining expression into the
		// predicate at the swap (splitFilterForProjectPush). NULLIF keeps
		// three-valued logic honest: rk2 is NULL for region 2, and
		// `rk2 > 1` must reject that row (UNKNOWN is not TRUE), not pass
		// it — 2 rows, not 3.
		twoPathQuery{name: "SubqueryComputedWhere", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT rk2 FROM (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) t
				WHERE rk2 > 1 ORDER BY rk2`,
			wantRows: 2, wantCols: []string{"rk2"},
			assertA: func(tb testing.TB, rows []map[string]any) {
				tb.Helper()
				countNulls("rk2", 0, 2)(tb, rows)
			}},
		twoPathQuery{name: "JoinBuildComputedWhereAbove", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT n.n_name, r.rk2 FROM nation n
				JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
				ON n.n_regionkey = r.r_regionkey WHERE r.rk2 > 1 ORDER BY n.n_name`,
			wantRows: 10, wantCols: []string{"n_name", "rk2"}},
		// A predicate mixing the computed alias with a passthrough source
		// column: only the alias reference is substituted. r_regionkey 2
		// drops (rk2 NULL), the rest satisfy rk2 >= r_regionkey.
		twoPathQuery{name: "SubqueryComputedWhereMixed", cmp: cmpOrdered, expectRows: true,
			sql: `SELECT r_regionkey, rk2 FROM (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) t
				WHERE rk2 >= r_regionkey ORDER BY r_regionkey`,
			wantRows: 4, wantCols: []string{"r_regionkey", "rk2"}},
	)
	return out
}

// assertDistinctSet holds a one-column distinct result against the exact
// value set the fixture determines: same cardinality, every returned value a
// member, no value repeated. Values are compared numerically — the engines
// may render the column as float or string, but a truncated key (the #379
// defect) changes the VALUES and fails membership.
func assertDistinctSet(tb testing.TB, rows []map[string]any, col string, want map[float64]bool) {
	tb.Helper()
	if len(rows) != len(want) {
		tb.Errorf("arm A (single-process) returned %d distinct rows, want %d", len(rows), len(want))
	}
	seen := map[float64]bool{}
	for i, r := range rows {
		v, ok := lookupCell(r, col)
		if !ok {
			tb.Fatalf("row %d carries no %s column", i, col)
		}
		f := toFloat(v)
		if !want[f] {
			tb.Errorf("row %d: %s = %v is not in the fixture's distinct set", i, col, v)
			return
		}
		if seen[f] {
			tb.Errorf("row %d: %s = %v returned twice — DISTINCT did not dedup", i, col, v)
			return
		}
		seen[f] = true
	}
}

// assertSingleCount holds a one-row, one-number answer against the value the
// fixture determines. The two-arm compare cannot do this on its own: a defect
// that hits both engines the same way passes it, and #351 was exactly that
// shape — both arms answered 0 for a 10-row join.
func sf001Extent(tb testing.TB, table, col string) (lo, hi float64) {
	tb.Helper()
	vals := sf001Column(tb, table, col)
	lo, hi = vals[0], vals[0]
	for _, v := range vals[1:] {
		if v < lo {
			lo = v
		}
		if v > hi {
			hi = v
		}
	}
	return lo, hi
}

func assertSingleCount(tb testing.TB, rows []map[string]any, col string, want float64) {
	tb.Helper()
	if len(rows) != 1 {
		tb.Fatalf("%d rows, want 1", len(rows))
	}
	if got := cellNum(rows[0], col); got != want {
		tb.Errorf("%s = %v, want %v", col, got, want)
	}
}

// assertFirstKeyAndCount pins a page: which row it starts at and how many
// rows it holds. Both halves matter — a dropped OFFSET keeps the count right
// while starting at row 0, and a dropped LIMIT keeps the start right while
// running to the end of the table.
func assertFirstKeyAndCount(tb testing.TB, rows []map[string]any, col string, wantFirst float64, wantRows int) {
	tb.Helper()
	if len(rows) != wantRows {
		tb.Errorf("got %d rows, want %d — the page bound was not applied", len(rows), wantRows)
		return
	}
	if got := cellNum(rows[0], col); got != wantFirst {
		tb.Errorf("page starts at %s=%v, want %v — the offset was not applied", col, got, wantFirst)
	}
}

// stripTrailingOrderBy removes a statement's own trailing ORDER BY clause.
// Only valid for a query whose last ORDER BY is top-level and final —
// hasTopLevelOrderBy is the same test — which is why callers name the query.
func stripTrailingOrderBy(sql string) string {
	locs := orderByRe.FindAllStringIndex(sql, -1)
	if len(locs) == 0 {
		return sql
	}
	last := locs[len(locs)-1]
	return strings.TrimRight(sql[:last[0]], " \t\n")
}

// setupTwoPathCluster stands up the cluster over the generated SF0.01
// fixture. See setupCluster.
func setupTwoPathCluster(tb testing.TB, ctx context.Context) (fast, dag *coordinator.Coordinator) {
	tb.Helper()
	return setupCluster(tb, ctx, Generate(SF001))
}

// setupCluster stands up one embedded NATS + MemStore + NATS-KV catalog +
// three workers, loads the supplied table rows, and returns two
// coordinators over that one cluster: fast (fast path enabled, arm A) and
// dag (LocalFastPathBytes=0, so every query takes the stage DAG, arm B).
//
// data is keyed by table name. The DuckDB ground-truth gate passes the rows
// it read out of the committed DuckDB-written parquet, so its distributed
// arm runs over the same bytes its fingerprints were computed from.
func setupCluster(tb testing.TB, ctx context.Context, data map[string][]map[string]any) (fast, dag *coordinator.Coordinator) {
	tb.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = tb.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		tb.Fatalf("embedded nats: %v", err)
	}
	tb.Cleanup(embedded.Shutdown)
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		tb.Fatalf("nats connect: %v", err)
	}
	tb.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		tb.Fatalf("jetstream: %v", err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		tb.Fatalf("streams: %v", err)
	}

	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		tb.Fatalf("make bucket: %v", err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		tb.Fatalf("catalog kv: %v", err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		tb.Fatalf("catalog init: %v", err)
	}
	loadTPCHIntoCatalog(tb, ctx, cat, store, data)

	const workers = 3
	ids := make([]string, workers)
	for i := range ids {
		ids[i] = fmt.Sprintf("two-path-worker-%d", i)
		w := worker.New(worker.Config{
			WorkerID:      ids[i],
			NATSUrl:       embedded.ClientURL(),
			MaxConcurrent: 4,
			CacheBytes:    64 << 20,
		}, store, nc, js, logger)
		wctx, wcancel := context.WithCancel(context.Background())
		tb.Cleanup(wcancel)
		if err := w.Start(wctx); err != nil {
			tb.Fatalf("worker start: %v", err)
		}
		tb.Cleanup(w.Stop)
	}

	fast = coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test",
		LocalFastPathBytes: coordinator.DefaultLocalFastPathBytes,
	}, cat, nc, js, logger)
	// 0 disables the fast path outright, which is what forces arm B onto
	// the stage DAG regardless of how small the scan is.
	dag = coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test",
		LocalFastPathBytes: 0,
	}, cat, nc, js, logger)

	// A worker enters a coordinator's registry by heartbeat, and the
	// worker's own heartbeat goroutine ticks on a 10s cadence. Publish one
	// on each worker's behalf so planning sees the cluster immediately;
	// the workers' own heartbeats keep the entries fresh from there.
	deadline := time.Now().Add(30 * time.Second)
	for {
		for _, id := range ids {
			data, err := distributed.Marshal(distributed.WorkerHeartbeat{
				WorkerID: id, MaxConcurrent: 4, Timestamp: time.Now(),
			})
			if err != nil {
				tb.Fatalf("marshal heartbeat: %v", err)
			}
			if err := nc.Publish(distributed.SubjectHeartbeat, data); err != nil {
				tb.Fatalf("publish heartbeat: %v", err)
			}
		}
		if err := nc.Flush(); err != nil {
			tb.Fatalf("nats flush: %v", err)
		}
		if fast.Workers().Count() >= workers && dag.Workers().Count() >= workers {
			break
		}
		if time.Now().After(deadline) {
			tb.Fatalf("workers never registered: fast=%d dag=%d, want %d",
				fast.Workers().Count(), dag.Workers().Count(), workers)
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fast, dag
}

// loadTPCHIntoCatalog writes every TPC-H table into the object store as
// several parquet files apiece and registers them in the catalog. Multiple
// files per table so the DAG really fans scans out across tasks — a
// single-file table could hide a per-task bug by accident.
func loadTPCHIntoCatalog(tb testing.TB, ctx context.Context, cat *catalog.Catalog, store objstore.Store, data map[string][]map[string]any) {
	tb.Helper()

	names := make([]string, 0, len(AllTables))
	for name := range AllTables {
		names = append(names, name)
	}
	sort.Strings(names)

	const chunks = 3
	for _, name := range names {
		schema := AllTables[name]
		rows := data[name]
		if err := cat.CreateTable(ctx, name, schema, nil); err != nil {
			tb.Fatalf("create table %s: %v", name, err)
		}
		if len(rows) == 0 {
			continue
		}
		per := (len(rows) + chunks - 1) / chunks
		for c := 0; c < chunks; c++ {
			lo, hi := c*per, c*per+per
			if hi > len(rows) {
				hi = len(rows)
			}
			if lo >= hi {
				break
			}
			var buf bytes.Buffer
			pw, err := parquet.NewWriter(&buf, schema, parquet.DefaultWriterConfig())
			if err != nil {
				tb.Fatalf("parquet writer %s: %v", name, err)
			}
			if err := pw.WriteRows(rows[lo:hi]); err != nil {
				tb.Fatalf("write rows %s: %v", name, err)
			}
			if err := pw.Close(); err != nil {
				tb.Fatalf("close writer %s: %v", name, err)
			}
			path := fmt.Sprintf("tables/%s/chunk_%04d.parquet", name, c)
			payload := buf.Bytes()
			if _, err := store.Put(ctx, "test", path, bytes.NewReader(payload), int64(len(payload)), "application/octet-stream"); err != nil {
				tb.Fatalf("put %s: %v", path, err)
			}
			if err := cat.AddFiles(ctx, name, map[string]string{}, fmt.Sprintf("tables/%s/", name), []catalog.FileEntry{{
				Path: path, SizeBytes: int64(len(payload)), NumRows: int64(hi - lo), CreatedAt: time.Now(),
			}}); err != nil {
				tb.Fatalf("add files %s: %v", name, err)
			}
		}
	}
}

// assertClose fails unless row[col] is within a relative 1e-9 of want.
// Aggregates that combine partial states differ from a single-pass
// reference only by accumulation order, which is orders of magnitude below
// this — the defects it guards were 4 to 13 significant figures out.
func assertClose(tb testing.TB, row map[string]any, col string, want float64, what string) {
	tb.Helper()
	v, ok := lookupCell(row, col)
	if !ok || v == nil {
		tb.Errorf("%s = NULL, want %.17g", what, want)
		return
	}
	got := cellNum(row, col)
	denom := math.Abs(want)
	if denom == 0 {
		if got != 0 {
			tb.Errorf("%s = %.17g, want 0", what, got)
		}
		return
	}
	if math.Abs(got-want)/denom > 1e-9 {
		tb.Errorf("%s = %.17g, want %.17g", what, got, want)
	}
}

// covarReference is a single-pass online covariance over the fixture,
// returning (correlation, covar_samp, covar_pop). Independent of the
// engine's own accumulator by construction: it is written here, over the
// values the fixture holds.
func covarReference(xs, ys []float64) (corr, covarSamp, covarPop float64) {
	var n int64
	var meanX, meanY, c, m2x, m2y float64
	for i := range xs {
		n++
		fn := float64(n)
		dx := xs[i] - meanX
		meanX += dx / fn
		dy := ys[i] - meanY
		meanY += dy / fn
		c += dx * (ys[i] - meanY)
		m2x += dx * (xs[i] - meanX)
		m2y += dy * (ys[i] - meanY)
	}
	if n == 0 {
		return 0, 0, 0
	}
	covarPop = c / float64(n)
	if n >= 2 {
		covarSamp = c / float64(n-1)
	}
	if m2x != 0 && m2y != 0 {
		corr = c / math.Sqrt(m2x*m2y)
	}
	return corr, covarSamp, covarPop
}

// quantileReference is the linear-interpolation quantile of vals, the
// definition PERCENTILE_CONT and DuckDB's quantile_cont both use.
func quantileReference(vals []float64, p float64) float64 {
	if len(vals) == 0 {
		return 0
	}
	sorted := append([]float64(nil), vals...)
	sort.Float64s(sorted)
	pos := p * float64(len(sorted)-1)
	lo := int(math.Floor(pos))
	hi := int(math.Ceil(pos))
	if lo == hi {
		return sorted[lo]
	}
	return sorted[lo] + (pos-float64(lo))*(sorted[hi]-sorted[lo])
}

// modeReference is the most frequent value in vals, lowest value winning a
// tie. Callers must pick a column whose winner is unambiguous — a tie is
// not something two engines have to agree about.
func modeReference(vals []float64) float64 {
	counts := map[float64]int{}
	for _, v := range vals {
		counts[v]++
	}
	best, bestN := math.NaN(), -1
	for v, n := range counts {
		if n > bestN || (n == bestN && v < best) {
			best, bestN = v, n
		}
	}
	return best
}

// argMinMaxReference returns the valueCol of the fixture rows carrying the
// smallest and largest orderCol.
func argMinMaxReference(tb testing.TB, table, valueCol, orderCol string) (atMin, atMax string) {
	tb.Helper()
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, r := range sf001Table(tb, table) {
		k := toFloat(r[orderCol])
		if k < lo {
			lo, atMin = k, fmt.Sprint(r[valueCol])
		}
		if k > hi {
			hi, atMax = k, fmt.Sprint(r[valueCol])
		}
	}
	return atMin, atMax
}

// assertCorrelatedScalarCount recomputes
//
//	SELECT COUNT(*) FROM customer c1
//	WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
//	                      WHERE c2.c_nationkey < c1.c_nationkey)
//
// over the fixture rows themselves rather than pinning a copied number, the
// same way every other absolute assertion in this file does. An empty inner
// set makes AVG NULL and the comparison UNKNOWN, so nationkey 0 contributes
// nothing — that rule is half the answer and stating it here is the point.
func assertCorrelatedScalarCount(why string) func(testing.TB, []map[string]any) {
	return func(tb testing.TB, rows []map[string]any) {
		tb.Helper()
		if len(rows) != 1 {
			tb.Fatalf("%d rows, want 1 (%s)", len(rows), why)
		}
		cust := sf001Table(tb, "customer")
		want := 0
		for _, r := range cust {
			key := toFloat(r["c_nationkey"])
			sum, n := 0.0, 0
			for _, o := range cust {
				if toFloat(o["c_nationkey"]) < key {
					sum += toFloat(o["c_acctbal"])
					n++
				}
			}
			if n > 0 && toFloat(r["c_acctbal"]) > sum/float64(n) {
				want++
			}
		}
		if got := cellNum(rows[0], "n"); got != float64(want) {
			tb.Errorf("n = %v, want %d — %s (recomputed over the fixture rows)", got, want, why)
		}
	}
}

// assertCorrelatedNestedCount recomputes the two-deep correlation: the outer
// row's nationkey bounds the inner-inner AVG, whose value filters the middle
// AVG, whose value the outer row is compared against. An empty inner-inner set
// makes the chain NULL end to end, so nationkey 0 contributes nothing.
func assertCorrelatedNestedCount(why string) func(testing.TB, []map[string]any) {
	return func(tb testing.TB, rows []map[string]any) {
		tb.Helper()
		if len(rows) != 1 {
			tb.Fatalf("%d rows, want 1 (%s)", len(rows), why)
		}
		cust := sf001Table(tb, "customer")
		want := 0
		for _, r := range cust {
			key := toFloat(r["c_nationkey"])
			sum, n := 0.0, 0
			for _, o := range cust {
				if toFloat(o["c_nationkey"]) < key {
					sum += toFloat(o["c_acctbal"])
					n++
				}
			}
			if n == 0 {
				continue // inner-inner AVG is NULL → middle filter matches nothing → outer compare UNKNOWN
			}
			threshold := sum / float64(n)
			mSum, m := 0.0, 0
			for _, o := range cust {
				if toFloat(o["c_acctbal"]) > threshold {
					mSum += toFloat(o["c_acctbal"])
					m++
				}
			}
			if m > 0 && toFloat(r["c_acctbal"]) > mSum/float64(m) {
				want++
			}
		}
		if got := cellNum(rows[0], "n"); got != float64(want) {
			tb.Errorf("n = %v, want %d — %s (recomputed over the fixture rows)", got, want, why)
		}
	}
}

// assertCorrelatedExistsCount recomputes the EXISTS / NOT EXISTS pair over the
// fixture rows. The two are complements over the whole table, which is what
// makes the NOT form worth its own entry: the NULL substitution of #347 made
// it return every row, and a row count alone calls that plausible.
func assertCorrelatedExistsCount(why string, not bool) func(testing.TB, []map[string]any) {
	return func(tb testing.TB, rows []map[string]any) {
		tb.Helper()
		if len(rows) != 1 {
			tb.Fatalf("%d rows, want 1 (%s)", len(rows), why)
		}
		cust := sf001Table(tb, "customer")
		want := 0
		for _, r := range cust {
			key := toFloat(r["c_nationkey"])
			found := false
			for _, o := range cust {
				if toFloat(o["c_nationkey"]) < key && toFloat(o["c_acctbal"]) > 9000 {
					found = true
					break
				}
			}
			if found != not {
				want++
			}
		}
		if got := cellNum(rows[0], "n"); got != float64(want) {
			tb.Errorf("n = %v, want %d — %s (recomputed over the fixture rows)", got, want, why)
		}
	}
}
