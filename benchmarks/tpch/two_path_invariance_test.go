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
//	table (`n1.n_name AS supp_nation`, with n2.n_name populated).
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

// hasTopLevelOrderBy reports whether the query's last ORDER BY is the
// statement's own and not a subquery's — a subquery's is always followed by
// the ')' that closes it, a top-level one never is.
func hasTopLevelOrderBy(sql string) bool {
	locs := orderByRe.FindAllStringIndex(sql, -1)
	if len(locs) == 0 {
		return false
	}
	last := locs[len(locs)-1]
	return !strings.Contains(sql[last[1]:], ")")
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
			7: "#314", // DAG NULLs n1.n_name (first alias of the self-joined table)
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
		// the correct spelling depends on a pass that runs later, so this
		// shape is still lost on the DAG (#316). Adding an expression to the
		// SELECT list makes it correct by accident, which is the clue.
		twoPathQuery{name: "AliasedSortNoAggregate", sql: "SELECT o_orderpriority AS p FROM orders ORDER BY p", cmp: cmpOrdered, knownBug: "#316"},
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
	)
	return out
}

// setupTwoPathCluster stands up one embedded NATS + MemStore + NATS-KV
// catalog + three workers, loads TPC-H at SF0.01, and returns two
// coordinators over that one cluster: fast (fast path enabled, arm A) and
// dag (LocalFastPathBytes=0, so every query takes the stage DAG, arm B).
func setupTwoPathCluster(tb testing.TB, ctx context.Context) (fast, dag *coordinator.Coordinator) {
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
	loadTPCHIntoCatalog(tb, ctx, cat, store)

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

// loadTPCHIntoCatalog writes every TPC-H table at SF0.01 into the object
// store as several parquet files apiece and registers them in the catalog.
// Multiple files per table so the DAG really fans scans out across tasks —
// a single-file table could hide a per-task bug by accident.
func loadTPCHIntoCatalog(tb testing.TB, ctx context.Context, cat *catalog.Catalog, store objstore.Store) {
	tb.Helper()
	data := Generate(SF001)

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
