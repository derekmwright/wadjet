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
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
	// assertA is an ABSOLUTE assertion on arm A's rows, run in addition to
	// the two-arm compare. The compare's contract is "the paths agree", not
	// "the answer is right" — a defect that hits both engines the same way
	// passes it while both arms are wrong. #320 was exactly that: an ORDER BY
	// over an expression was ignored on the single-process pipeline AND on the
	// stage DAG, so the arms agreed on an arbitrary order. Every entry whose
	// bug could be symmetric carries one of these.
	assertA func(tb testing.TB, rows []map[string]any)
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
			dagRows, dagCols, dagErr := runArm(t, ctx, dag, q.sql)

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
	)
	return out
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
