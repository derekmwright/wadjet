package tpch

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

const (
	duckdbBin          = "/tmp/duckdb"
	duckdbDataDir      = "duckdb-data"                // committed: benchmarks/tpch/duckdb-data/*.parquet
	duckdbBaselineFile = "baseline-duckdb-sf001.json" // stored DuckDB-output fingerprints
	duckdbNull         = "<NULL>"                     // DuckDB's .nullvalue for CSV output
	floatEps           = 1e-4                         // ULP-level drift between engines, live cell diff only
)

var duckdbTables = []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"}

// The two arms, named once so a knownBugArm cannot drift from the label the
// failure prints.
const (
	armLocal = "A (single-process)"
	armDAG   = "B (stage DAG)"
	// armBoth pins a divergence present on BOTH arms — an engine-wide bug
	// rather than a distribution one. Each arm is still compared, so the
	// subtest fails as soon as either starts agreeing and the pin has to be
	// narrowed or deleted.
	armBoth = "both arms"
)

// emptyJoinAggregateBug is the divergence this gate found on the day it
// gained its distributed arm (2026-08-18), still open.
//
// An ungrouped aggregate returns exactly one row for any input, including no
// input: SUM over the empty set is NULL, and SQL has no reading under which
// the answer is zero rows. On the stage DAG, when the aggregate's input is
// an empty JOIN result, that row is lost — TPC-H Q17 comes back with 0 rows
// against DuckDB's 1 (NULL), and so does the reduction below. It is specific:
// COUNT(*) over the same empty join keeps its row, and the same SUM emptied
// by a WHERE clause instead of a join keeps its row on every path (both are
// corpus entries, gated, as the controls). The single-process engine is
// correct in all three shapes, which is why nothing caught this before the
// DAG was held to ground truth — and the generated SF0.01 fixture the
// two-path suite uses has parts matching Q17's predicate, so its join is not
// empty there and the shape never arises.
// dagNullOrderingBug: distributed.SortKeySpec carries Column and Desc and
// nothing about null placement, so the DAG sorts NULLs first regardless of
// what the SQL asked for. ASC is wrong (SQL says NULLS LAST); DESC is right
// by accident, which is why only the ascending entries below fail.
const dagNullOrderingBug = "the stage DAG ignores NULLS FIRST/LAST and sorts NULLs first unconditionally: " +
	"ascending order is wrong, descending is correct by accident, and an explicit NULLS LAST is dropped. " +
	"Single-process is correct. Tracked as #330."

// coalesceNestedTypeBug: Ret.Resolve reports a same-as-argument FALLBACK as a
// confident answer, so NULLIF tells COALESCE it is Float64 and COALESCE never
// consults the string literal that would have decided it.
const coalesceNestedTypeBug = "COALESCE over a computed string argument is typed numeric, so every row comes back as 0. " +
	"Both arms. Tracked as #331."

const emptyJoinAggregateBug = "an ungrouped aggregate over an EMPTY JOIN loses its mandatory single row on the stage DAG " +
	"(SUM: 0 rows, want 1 row of NULL; COUNT over the same join is correct, and so is the same SUM emptied by a filter). " +
	"Single-process is correct. Tracked as #329."

// TestDuckDBCompare is the cross-engine ground-truth gate: every corpus query
// runs on BOTH Wadjet execution paths and both answers are held against a
// fingerprint of DuckDB's answer, stored in
// benchmarks/tpch/baseline-duckdb-sf001.json and computed over the same
// SF0.01 parquet committed under benchmarks/tpch/duckdb-data/.
//
//	arm A — the single-process engine (wadjet.DB; the same physical planner
//	        and pipeline the coordinator's local fast path runs)
//	arm B — the distributed stage DAG (a real coordinator + three workers
//	        over an embedded NATS, LocalFastPathBytes=0 so nothing can route
//	        around it)
//
// Both arms exist because the gate used to have only arm A, and the five
// wrong-answer bugs of 2026-08-17 were mostly DAG-only. Q05's revenues came
// back ~25x inflated on the DAG (#312) while the single-process path was
// correct, so DuckDB truth existed the whole time and never saw the bug —
// and the recorded distributed baseline had frozen the inflated number, so a
// FIXED engine would have failed that gate. Nothing here is derived from
// Wadjet: the stored file is written only by the regenerate path below, only
// from DuckDB output, and every entry carries source="duckdb" which the
// loader enforces.
//
// The fingerprint (internal/oracle.Fingerprint) covers every column, strings
// and NULLs included, and is order-sensitive exactly when the query has a
// top-level ORDER BY. See duckdbCorpus for how each query's comparison mode
// is decided.
//
// Modes:
//
//	default       — run both arms, compare against the stored DuckDB
//	                fingerprints. No DuckDB binary required. This is the CI
//	                gate.
//	WADJET_DUCKDB_COMPARE=1 — additionally shell out to DuckDB and verify
//	                the stored fingerprint still equals live DuckDB output,
//	                with a cell-by-cell diff when an arm disagrees.
//	                Requires /tmp/duckdb.
//	WADJET_REGENERATE_DUCKDB_BASELINE=1 — rewrite the stored file from live
//	                DuckDB output. Requires /tmp/duckdb.
//
// Setup for the live / regenerate paths:
//
//	wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip
//	unzip -d /tmp /tmp/duckdb.zip
//
// The committed parquet fixtures were produced by
// benchmarks/tpch/duckdb-setup/export-wadjet-shape.sql at SF0.01.
func TestDuckDBCompare(t *testing.T) {
	regenerate := os.Getenv("WADJET_REGENERATE_DUCKDB_BASELINE") == "1"
	liveCompare := regenerate || os.Getenv("WADJET_DUCKDB_COMPARE") == "1"

	dataDir := filepath.Join(".", duckdbDataDir)
	for _, name := range duckdbTables {
		if _, err := os.Stat(filepath.Join(dataDir, name+".parquet")); err != nil {
			t.Fatalf("missing fixture %s/%s.parquet — regenerate via duckdb-setup/export-wadjet-shape.sql: %v",
				dataDir, name, err)
		}
	}
	if liveCompare {
		if _, err := os.Stat(duckdbBin); err != nil {
			t.Fatalf("WADJET_DUCKDB_COMPARE / REGENERATE requires %s: %v", duckdbBin, err)
		}
	}

	corpus := duckdbCorpus()
	if regenerate {
		regenerateDuckDBBaseline(t, corpus)
		return
	}

	stored := loadDuckDBBaseline(t)
	assertBaselineCoversCorpus(t, corpus, stored)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	rows := duckdbFixtureRows(t)
	embedded := ingestDuckDBFixture(t, ctx, rows) // arm A
	_, dag := setupCluster(t, ctx, rows)          // arm B

	duckdbSetup := ""
	if liveCompare {
		duckdbSetup = duckdbViews(t, dataDir)
	}

	matches, ungated := 0, 0
	for _, c := range corpus {
		t.Run(c.name, func(t *testing.T) {
			want := stored[c.name]

			if liveCompare {
				// The stored file is the gate's authority, so its agreement
				// with the engine it claims to come from is itself checked
				// whenever DuckDB is available: this is what would have
				// caught a baseline frozen around a wrong answer.
				dRows, dCols, err := runDuckDB(duckdbSetup, c.sql)
				if err != nil {
					t.Fatalf("duckdb: %v", err)
				}
				assertStoredMatchesLiveDuckDB(t, c, want, dCols, dRows)
			}

			aRows, aCols, aErr := runWadjet(ctx, embedded, c.sql)
			if aErr != nil {
				t.Errorf("arm A (single-process) failed: %v", aErr)
			} else {
				checkArm(t, armLocal, c, want, aRows, aCols)
			}

			bRows, bCols, bErr := runArm(t, ctx, dag, c.sql)
			if bErr != nil {
				t.Errorf("arm B (stage DAG) failed: %v", bErr)
			} else {
				checkArm(t, armDAG, c, want, bRows, bCols)
			}

			if t.Failed() && liveCompare {
				dRows, dCols, err := runDuckDB(duckdbSetup, c.sql)
				if err == nil {
					t.Logf("--- %s: arm A vs live DuckDB, cell by cell ---", c.name)
					reportLiveDiff(t, c.name, aRows, dRows, aCols, dCols)
					t.Logf("--- %s: arm B vs live DuckDB, cell by cell ---", c.name)
					reportLiveDiff(t, c.name, bRows, dRows, bCols, dCols)
				}
			}
			if !t.Failed() {
				matches++
			}
		})
		if c.knownBugArm != "" {
			ungated++
		}
	}
	t.Logf("Summary: %d/%d corpus entries held against the stored DuckDB fingerprint (%d of them on one arm only — see the known divergences above)",
		matches, len(corpus), ungated)

	if n := dag.LocalFastPathHits(); n != 0 {
		t.Errorf("arm B took the local fast path %d times — LocalFastPathBytes=0 must force the stage DAG", n)
	}
}

// duckdbCase is one corpus entry: a query plus how its answer may be
// compared.
type duckdbCase struct {
	name string
	sql  string
	// ordered: the query has a top-level ORDER BY, so the row SEQUENCE is
	// part of the answer and the fingerprint is taken over rows as returned.
	// Without it rows are sorted before digesting, because an engine is free
	// to return them in any order.
	ordered bool
	// countOnly: the row CONTENT is not determined by the query, so only the
	// row count is compared. why says which of the two reasons applies.
	countOnly bool
	why       string
	// limit, countOnly only: the trailing LIMIT the answer must respect.
	limit int
	// tolerance, countOnly only: allowed row-count difference.
	tolerance int
	// knownBugArm names an arm whose answer is known to differ from DuckDB
	// today, and knownBug describes the defect. That arm is not gated — but
	// it is not ignored either: the comparison still runs and the subtest
	// FAILS if the arm starts agreeing, so the exemption cannot outlive the
	// bug. The other arm stays fully gated. Deleting the two fields is the
	// whole of "the fix landed"; the assertion below is untouched.
	knownBugArm string
	knownBug    string
}

// duckdbCorpus is the 22 TPC-H queries plus the shapes they lack, each
// tagged with the strongest comparison its SQL admits.
//
// The mode is derived from the query, not chosen per query:
//
//   - A top-level ORDER BY (hasTopLevelOrderBy, paren-depth aware) makes the
//     comparison order-sensitive. This is the whole of the #313/#316/#320
//     class: those bugs returned the right rows in the wrong sequence, which
//     an order-insensitive digest accepts by construction.
//   - A trailing LIMIT splits into two entries: the stripped query compared
//     row for row (strictly stronger, and immune to the tie at the cut) and
//     the verbatim query compared by row count, which is what pins the limit
//     itself. A bare LIMIT with no ORDER BY is count-only and nothing more —
//     SQL genuinely does not say WHICH rows come back, so a content
//     fingerprint over one engine's arbitrary choice would be fiction.
//   - Q02 and Q22 select rows by comparing against a float aggregate, so
//     membership at the threshold shifts with accumulation order between two
//     correct engines. Same relaxation TestTPCHQueries and the optimization
//     oracle already apply.
func duckdbCorpus() []duckdbCase {
	nums := make([]int, 0, len(TPCHQueries))
	for n := range TPCHQueries {
		nums = append(nums, n)
	}
	sort.Ints(nums)

	out := make([]duckdbCase, 0, len(nums)+10)
	for _, n := range nums {
		name := fmt.Sprintf("Q%02d", n)
		sql := TPCHQueries[n].SQL
		c := duckdbCase{name: name, sql: sql, ordered: hasTopLevelOrderBy(sql)}
		if n == 17 {
			c.knownBugArm, c.knownBug = armDAG, emptyJoinAggregateBug
		}
		if n == 2 || n == 22 {
			c.countOnly, c.tolerance = true, 4
			c.why = "row membership turns on a float threshold; borderline rows shift with accumulation order"
		}
		if lim := trailingLimit(sql); lim > 0 {
			stripped := strings.TrimRight(trailingLimitRe.ReplaceAllString(sql, ""), " \t\n")
			full := c
			full.name, full.sql = name+"_nolimit", stripped
			full.ordered = hasTopLevelOrderBy(stripped)
			out = append(out, full)

			c.countOnly, c.limit = true, lim
			c.why = "rows tied at the LIMIT boundary are interchangeable; the count is not"
			out = append(out, c)
			continue
		}
		out = append(out, c)
	}

	// Shapes the TPC-H corpus does not contain, each covering a comparison
	// mode or a defect class the 22 queries leave dark. All are plain SQL
	// both engines accept, and all stay trivial at SF0.01.
	out = append(out,
		// Order-insensitive with more than one row: every TPC-H query
		// without an ORDER BY returns a single row, so the unordered arm
		// would otherwise never be exercised on a result that could be
		// permuted.
		duckdbCase{name: "GroupNoOrderBy", sql: "SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey"},
		// A bare LIMIT: no ORDER BY, so which rows come back is the engine's
		// choice and only the count is an answer (this is the shape that hid
		// the DAG dropping LIMIT outright, c5e77cf).
		duckdbCase{name: "BareLimit", sql: "SELECT l_orderkey FROM lineitem LIMIT 7",
			countOnly: true, limit: 7, why: "a LIMIT with no ORDER BY picks arbitrary rows by definition"},
		// ORDER BY over an expression that is not in the SELECT list — the
		// #320 shape. Note this is why hasTopLevelOrderBy has to be paren
		// aware: the parens belong to LENGTH(), not to a subquery.
		duckdbCase{name: "OrderByExpression", sql: "SELECT n_name, n_nationkey FROM nation ORDER BY LENGTH(n_name), n_name"},
		// NULL and the empty string in one string column: the value
		// signature that gated #314 skips string columns entirely, and a
		// renderer that folds "" into NULL would call these two answers the
		// same.
		duckdbCase{name: "NullAndEmptyString", sql: "SELECT n_nationkey, CASE WHEN n_nationkey % 3 = 0 THEN NULL WHEN n_nationkey % 3 = 1 THEN '' ELSE n_name END AS mixed FROM nation ORDER BY n_nationkey"},
		// NULL semantics. TPC-H contains no NULLs at all, so every rule
		// below is untested by the 22 queries — and each is a rule an engine
		// can get wrong while returning a plausible-looking answer.
		//
		// Aggregates SKIP nulls, and COUNT(col) is not COUNT(*).
		duckdbCase{name: "NullAggregatesSkipNulls", sql: `SELECT COUNT(*) AS all_rows, COUNT(NULLIF(n_regionkey, 1)) AS non_null,
			SUM(NULLIF(n_regionkey, 1)) AS s, AVG(NULLIF(n_regionkey, 1)) AS a,
			MIN(NULLIF(n_regionkey, 1)) AS mn, MAX(NULLIF(n_regionkey, 1)) AS mx FROM nation`},
		// An aggregate over an all-NULL input is NULL, not zero and not
		// "no rows" — except COUNT, which is 0.
		duckdbCase{name: "NullAllNullAggregate", sql: `SELECT SUM(NULLIF(n_regionkey, n_regionkey)) AS s,
			AVG(NULLIF(n_regionkey, n_regionkey)) AS a, MIN(NULLIF(n_regionkey, n_regionkey)) AS mn,
			COUNT(NULLIF(n_regionkey, n_regionkey)) AS c FROM nation`},
		// NULLs form ONE group, and it survives GROUP BY.
		duckdbCase{name: "NullGroupsTogether", sql: `SELECT NULLIF(n_regionkey, 1) AS k, COUNT(*) AS c
			FROM nation GROUP BY NULLIF(n_regionkey, 1) ORDER BY k`, knownBugArm: armDAG, knownBug: dagNullOrderingBug},
		// Sort placement: PostgreSQL and DuckDB put NULLS LAST for ASC and
		// NULLS FIRST for DESC. Both directions, because a single default
		// can be right by accident.
		duckdbCase{name: "NullOrderingAsc", sql: "SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k, n_name", knownBugArm: armDAG, knownBug: dagNullOrderingBug},
		duckdbCase{name: "NullOrderingDesc", sql: "SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k DESC, n_name"},
		// DISTINCT treats NULLs as equal to each other — the one place SQL
		// does — so exactly one NULL comes back.
		duckdbCase{name: "NullDistinct", sql: "SELECT DISTINCT NULLIF(n_regionkey, 1) AS k FROM nation ORDER BY k", knownBugArm: armDAG, knownBug: dagNullOrderingBug},
		// A comparison against NULL is UNKNOWN, so it neither matches nor
		// anti-matches; only IS NULL finds those rows.
		duckdbCase{name: "NullComparisonNeverMatches", sql: `SELECT
			COUNT(*) FILTER (WHERE NULLIF(n_regionkey, 1) = 1) AS eq,
			COUNT(*) FILTER (WHERE NULLIF(n_regionkey, 1) <> 1) AS ne,
			COUNT(*) FILTER (WHERE NULLIF(n_regionkey, 1) IS NULL) AS isnull FROM nation`},
		// A LEFT JOIN that misses: the right side's columns are NULL, and
		// COUNT(right_col) must not count them. This is how real data grows
		// NULLs even when no column is nullable.
		duckdbCase{name: "LeftJoinMissIsNull", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey + 100 ORDER BY n.n_name`},
		duckdbCase{name: "LeftJoinMissCount", sql: `SELECT COUNT(*) AS rows_out, COUNT(r.r_name) AS matched
			FROM nation n LEFT JOIN region r ON n.n_regionkey = r.r_regionkey + 100`},
		// NULL propagates through arithmetic and string building rather than
		// being treated as an identity element.
		duckdbCase{name: "NullPropagatesThroughExpressions", sql: `SELECT n_nationkey,
			NULLIF(n_regionkey, 1) + 1 AS plus, NULLIF(n_name, 'ALGERIA') || '!' AS cat,
			COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback') AS coalesced
			FROM nation ORDER BY n_nationkey`, knownBugArm: armBoth, knownBug: coalesceNestedTypeBug},
		// Two aliases of one table, both string columns projected — the #314
		// shape, where the DAG returned NULL for the alias that landed on the
		// probe side.
		duckdbCase{name: "SelfJoinAliases", sql: "SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation FROM nation n1 JOIN nation n2 ON n1.n_regionkey = n2.n_regionkey WHERE n1.n_nationkey < 6 AND n2.n_nationkey < 6 ORDER BY supp_nation, cust_nation"},
		// A WHERE equality between two joined tables that is not itself a
		// join condition — the #312 predicate the DAG dropped, reduced to a
		// count so the failure is unmissable.
		duckdbCase{name: "CrossTableEqualityFilter", sql: `SELECT COUNT(*) AS c FROM customer
			JOIN orders ON c_custkey = o_custkey
			JOIN lineitem ON l_orderkey = o_orderkey
			JOIN supplier ON l_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			JOIN region ON n_regionkey = r_regionkey
			WHERE c_nationkey = s_nationkey`},
		// The same predicate carrying a value, so a dropped predicate shows
		// up as a wrong SUM and not only as a wrong count.
		duckdbCase{name: "CrossTableEqualityRevenue", sql: `SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
			FROM customer
			JOIN orders ON c_custkey = o_custkey
			JOIN lineitem ON l_orderkey = o_orderkey
			JOIN supplier ON l_suppkey = s_suppkey
			JOIN nation ON s_nationkey = n_nationkey
			WHERE c_nationkey = s_nationkey
			GROUP BY n_name
			ORDER BY n_name`},
		// An aliased sort key over a grouped column (#313) and without an
		// aggregate under it (#316).
		duckdbCase{name: "AliasedGroupKeyOrderBy", sql: "SELECT o_orderpriority AS p, COUNT(*) AS c FROM orders GROUP BY o_orderpriority ORDER BY p"},
		duckdbCase{name: "AliasedSortNoAggregate", sql: "SELECT DISTINCT o_orderpriority AS p FROM orders ORDER BY p"},
		// Reduction of the Q17 divergence this gate found, plus its two
		// controls. An ungrouped aggregate has exactly one row whatever its
		// input, so an empty input means one row of NULL — SQL has no case
		// where it means no rows at all.
		duckdbCase{name: "EmptyJoinUngroupedSum",
			sql:         "SELECT SUM(l_extendedprice) AS s FROM lineitem JOIN part ON p_partkey = l_partkey WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'",
			knownBugArm: armDAG, knownBug: emptyJoinAggregateBug},
		// Control 1: the same empty join counted instead of summed. The DAG
		// keeps the row here, so the defect is not "empty join loses the
		// stage" in general.
		duckdbCase{name: "EmptyJoinUngroupedCount",
			sql: "SELECT COUNT(*) AS c FROM lineitem JOIN part ON p_partkey = l_partkey WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'"},
		// Control 2: the same SUM emptied by a filter rather than a join.
		// Correct on every path, so the join is the trigger.
		duckdbCase{name: "EmptyScanUngroupedSum",
			sql: "SELECT SUM(l_extendedprice) AS s FROM lineitem WHERE l_orderkey < 0"},
	)
	// The hand-written entries above declare `ordered` implicitly through
	// their SQL; derive it the same way the TPC-H entries do so the two can
	// never drift apart.
	for i := range out {
		if !out[i].countOnly {
			out[i].ordered = hasTopLevelOrderBy(out[i].sql)
		}
	}
	return out
}

// checkArm holds one arm's answer against the stored DuckDB entry.
func checkArm(t *testing.T, arm string, c duckdbCase, want duckdbBaselineEntry, rows []map[string]any, cols []string) {
	t.Helper()
	ok, detail := compareToDuckDB(c, want, rows, cols)

	if c.knownBugArm == arm || c.knownBugArm == armBoth {
		if ok {
			t.Errorf("arm %s now agrees with DuckDB, so this known divergence is FIXED:\n  %s\n"+
				"Delete the knownBugArm/knownBug fields on %s in duckdbCorpus so the arm is gated again.",
				arm, c.knownBug, c.name)
			return
		}
		t.Logf("known divergence, NOT gated on arm %s: %s\n  %s", arm, detail, c.knownBug)
		return
	}
	if !ok {
		mode := "unordered"
		switch {
		case c.countOnly:
			mode = "row count only: " + c.why
		case c.ordered:
			mode = "ordered — a top-level ORDER BY makes the row sequence part of the answer"
		}
		t.Errorf("arm %s diverges from DuckDB [%s]: %s\n  SQL: %s\n  arm's first row: %v\n"+
			"  re-run with WADJET_DUCKDB_COMPARE=1 for a cell-by-cell diff against live DuckDB",
			arm, mode, detail, c.sql, firstRow(rows))
	}
}

// compareToDuckDB is the comparison itself: ok plus a description of the
// first thing that differs.
func compareToDuckDB(c duckdbCase, want duckdbBaselineEntry, rows []map[string]any, cols []string) (bool, string) {
	if c.countOnly {
		if diff := len(rows) - want.RowCount; diff < -c.tolerance || diff > c.tolerance {
			return false, fmt.Sprintf("row count %d, DuckDB %d (±%d allowed)", len(rows), want.RowCount, c.tolerance)
		}
		if c.limit > 0 && len(rows) > c.limit {
			return false, fmt.Sprintf("%d rows returned for LIMIT %d", len(rows), c.limit)
		}
		return true, ""
	}

	// The stored column list is DuckDB's. An arm must carry every one of
	// them (resolved through realign, which allows the DAG's table-qualified
	// spelling) and must not carry anything else — an extra column is a
	// planner artefact leaking to the client, which is what the
	// materialized __sortkey_ columns were.
	if len(cols) != len(want.Columns) {
		return false, fmt.Sprintf("%d columns %v, DuckDB %d %v", len(cols), cols, len(want.Columns), want.Columns)
	}
	aligned, missing := realign(rows, want.Columns)
	if missing != "" {
		return false, fmt.Sprintf("no column %q (arm: %v, DuckDB: %v)", missing, cols, want.Columns)
	}
	got := oracle.FingerprintOf(&oracle.Result{Columns: want.Columns, Rows: aligned}, c.ordered)
	return want.Fingerprint().Match(got)
}

// assertStoredMatchesLiveDuckDB verifies the committed entry against the
// engine it claims to come from. A stored fingerprint that no longer matches
// DuckDB is not a Wadjet failure, and saying so is the point: the Q05 mode
// of failure was a baseline that had drifted away from ground truth.
func assertStoredMatchesLiveDuckDB(t *testing.T, c duckdbCase, want duckdbBaselineEntry, dCols []string, dRows []map[string]string) {
	t.Helper()
	live := duckdbEntry(c, dCols, dRows)
	if live.RowCount != want.RowCount {
		t.Errorf("STORED BASELINE IS NOT DUCKDB'S ANSWER: %s row count %d stored, live DuckDB %d — "+
			"regenerate with WADJET_REGENERATE_DUCKDB_BASELINE=1", c.name, want.RowCount, live.RowCount)
		return
	}
	if c.countOnly {
		return
	}
	if strings.Join(live.Columns, ",") != strings.Join(want.Columns, ",") {
		t.Errorf("STORED BASELINE IS NOT DUCKDB'S ANSWER: %s columns %v stored, live DuckDB %v",
			c.name, want.Columns, live.Columns)
		return
	}
	if ok, detail := want.Fingerprint().Match(live.Fingerprint()); !ok {
		t.Errorf("STORED BASELINE IS NOT DUCKDB'S ANSWER: %s %s — regenerate with WADJET_REGENERATE_DUCKDB_BASELINE=1",
			c.name, detail)
	}
}

// regenerateDuckDBBaseline rewrites the stored file from live DuckDB output.
// Every entry it writes is stamped source="duckdb"; nothing else in the
// package writes this file, so an entry that came from anywhere else cannot
// exist without a hand edit, and loadDuckDBBaseline rejects one that lacks
// the stamp.
func regenerateDuckDBBaseline(t *testing.T, corpus []duckdbCase) {
	t.Helper()
	setup := duckdbViews(t, filepath.Join(".", duckdbDataDir))
	version, err := exec.Command(duckdbBin, "--version").Output()
	if err != nil {
		t.Fatalf("duckdb --version: %v", err)
	}

	entries := make(map[string]duckdbBaselineEntry, len(corpus))
	for _, c := range corpus {
		dRows, dCols, err := runDuckDB(setup, c.sql)
		if err != nil {
			t.Fatalf("%s: duckdb: %v", c.name, err)
		}
		e := duckdbEntry(c, dCols, dRows)
		entries[c.name] = e
		t.Logf("%s (regen from DuckDB): rows=%d %s", c.name, e.RowCount, e.Compare)
	}
	writeDuckDBBaseline(t, duckdbBaseline{
		Generator: fmt.Sprintf("duckdb %s over %s (WADJET_REGENERATE_DUCKDB_BASELINE=1)",
			strings.TrimSpace(string(version)), duckdbDataDir),
		Note:    "Ground truth. Every entry is DuckDB output; nothing here is derived from Wadjet.",
		Queries: entries,
	})
	t.Logf("wrote %s with %d entries", duckdbBaselineFile, len(entries))
}

// duckdbEntry turns one live DuckDB result into a stored entry.
func duckdbEntry(c duckdbCase, cols []string, rows []map[string]string) duckdbBaselineEntry {
	e := duckdbBaselineEntry{Source: "duckdb", RowCount: len(rows), Compare: "rows", Ordered: c.ordered}
	if c.countOnly {
		e.Compare = "count"
		e.Tolerance, e.Limit, e.Why = c.tolerance, c.limit, c.why
		return e
	}
	typed := make([]map[string]any, len(rows))
	for i, r := range rows {
		m := make(map[string]any, len(cols))
		for _, col := range cols {
			m[col] = oracle.TextCell(r[col], duckdbNull)
		}
		typed[i] = m
	}
	e.Columns = append([]string(nil), cols...)
	fp := oracle.FingerprintOf(&oracle.Result{Columns: cols, Rows: typed}, c.ordered)
	e.Fine, e.Coarse = fp.Fine, fp.Coarse
	return e
}

// duckdbViews is the DuckDB preamble registering a view per fixture table.
func duckdbViews(t *testing.T, dataDir string) string {
	t.Helper()
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		t.Fatalf("abs %s: %v", dataDir, err)
	}
	var sb strings.Builder
	for _, name := range duckdbTables {
		fmt.Fprintf(&sb, "CREATE VIEW %s AS SELECT * FROM read_parquet('%s/%s.parquet');\n", name, absDir, name)
	}
	return sb.String()
}

// duckdbFixtureRows reads every committed DuckDB-written parquet through
// Wadjet's reader. Both arms are loaded from these rows, so they answer over
// the same data the stored fingerprints describe.
func duckdbFixtureRows(t *testing.T) map[string][]map[string]any {
	t.Helper()
	dataDir := filepath.Join(".", duckdbDataDir)
	out := make(map[string][]map[string]any, len(AllTables))
	for tableName, schema := range AllTables {
		rows, err := loadParquetRows(filepath.Join(dataDir, tableName+".parquet"), schema)
		if err != nil {
			t.Fatalf("load %s: %v", tableName, err)
		}
		out[tableName] = rows
	}
	return out
}

// ingestDuckDBFixture builds arm A: an embedded single-process DB holding
// the fixture.
func ingestDuckDBFixture(t *testing.T, ctx context.Context, data map[string][]map[string]any) *wadjet.DB {
	t.Helper()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "tpch"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for tableName, schema := range AllTables {
		if err := db.CreateTable(ctx, tableName, schema, nil); err != nil {
			t.Fatalf("create table %s: %v", tableName, err)
		}
		rows := data[tableName]
		ing := db.NewIngester(tableName, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1,
			RowGroupSize:  max(100, len(rows)/4),
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			t.Fatalf("ingest %s: %v", tableName, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tableName, err)
		}
	}
	return db
}

func reportLiveDiff(t *testing.T, name string, wRows []map[string]any, dRows []map[string]string, wCols, dCols []string) {
	t.Helper()
	if len(wRows) != len(dRows) {
		t.Logf("%s row count: wadjet=%d duckdb=%d", name, len(wRows), len(dRows))
		if len(wRows) > 0 {
			t.Logf("  Wadjet first row: %v", wRows[0])
		}
		if len(dRows) > 0 {
			t.Logf("  DuckDB first row: %v", dRows[0])
		}
		return
	}
	cols := wCols
	if len(cols) == 0 {
		cols = dCols
	}
	diff := 0
	for i := range wRows {
		for _, col := range cols {
			w, _ := lookupCell(wRows[i], col)
			if !cellEqual(w, dRows[i][col]) {
				diff++
				if diff <= 3 {
					t.Logf("%s row %d col %s: wadjet=%v duckdb=%v", name, i, col, w, dRows[i][col])
				}
			}
		}
	}
	t.Logf("%s %d cell divergences total (positional; a pure ordering difference shows as many)", name, diff)
}

// runWadjet runs sql via wadjet.DB and returns the typed row slice.
func runWadjet(ctx context.Context, db *wadjet.DB, sql string) ([]map[string]any, []string, error) {
	res, err := db.Query(ctx, sql)
	if err != nil {
		return nil, nil, err
	}
	cols := append([]string(nil), res.Columns...)
	return res.Rows, cols, nil
}

// runDuckDB runs sql via the duckdb CLI subprocess and returns rows as
// (col → string) maps. The DuckDB output is CSV with header.
//
// NULL is encoded as the literal string "<NULL>" (instead of an empty field)
// so the CSV reader doesn't drop rows whose only column is NULL — the Go
// encoding/csv reader skips records with zero fields, which it treats a bare
// "\r\n" as — and so an empty field means the empty STRING and nothing else.
func runDuckDB(setup, sql string) ([]map[string]string, []string, error) {
	script := setup + "\n.mode csv\n.headers on\n.nullvalue " + duckdbNull + "\n" + sql + ";\n"
	cmd := exec.Command(duckdbBin)
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.Output()
	if err != nil {
		return nil, nil, fmt.Errorf("duckdb: %v\nscript: %s", err, script)
	}
	r := csv.NewReader(strings.NewReader(string(out)))
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	records, err := r.ReadAll()
	if err != nil {
		return nil, nil, fmt.Errorf("parse duckdb csv: %v", err)
	}
	if len(records) == 0 {
		return nil, nil, nil
	}
	cols := records[0]
	rows := make([]map[string]string, 0, len(records)-1)
	for _, rec := range records[1:] {
		row := make(map[string]string, len(cols))
		for i, col := range cols {
			if i < len(rec) {
				row[col] = rec[i]
			}
		}
		rows = append(rows, row)
	}
	return rows, cols, nil
}

// cellEqual compares one cell from Wadjet (typed) against one from DuckDB
// (string from CSV). Diagnostics only — the gate itself compares
// fingerprints; this exists to render a readable diff when one fails.
func cellEqual(w any, d string) bool {
	if w == nil {
		return d == duckdbNull
	}
	if d == duckdbNull {
		return false
	}
	ws := canonicalString(w)
	ds := canonicalString(d)
	if ws == ds {
		return true
	}
	wf, wOk := parseFloat(ws)
	df, dOk := parseFloat(ds)
	if wOk && dOk {
		if wf == 0 && df == 0 {
			return true
		}
		denom := math.Max(math.Abs(wf), math.Abs(df))
		if denom == 0 {
			return wf == df
		}
		return math.Abs(wf-df)/denom < floatEps
	}
	return false
}

func canonicalString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(x)
	case bool:
		return strconv.FormatBool(x)
	case int:
		return strconv.Itoa(x)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func parseFloat(s string) (float64, bool) {
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// loadParquetRows reads a DuckDB-written parquet file via Wadjet's reader and
// returns rows ready for Wadjet's Ingester. Replaces the prior JSON path so
// the test exercises Wadjet's parquet decoder against externally-written data
// (the whole point of the cross-engine audit) and removes the 27 MB JSON
// dependency from the committed fixture.
func loadParquetRows(path string, schema parquet.Schema) ([]map[string]any, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	reader, err := parquet.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("parquet reader %s: %w", path, err)
	}
	batches, err := scan.ReadFileBatches(reader, schema.Columns, nil)
	if err != nil {
		return nil, fmt.Errorf("parquet scan %s: %w", path, err)
	}
	var rows []map[string]any
	for _, b := range batches {
		rows = append(rows, b.ToRows()...)
	}
	return rows, nil
}

// duckdbBaseline is the on-disk file: a provenance header plus one entry per
// corpus query.
type duckdbBaseline struct {
	Generator string                         `json:"generator"`
	Note      string                         `json:"note"`
	Queries   map[string]duckdbBaselineEntry `json:"queries"`
}

// duckdbBaselineEntry is one query's ground truth, captured from a live
// DuckDB run over the committed fixture.
type duckdbBaselineEntry struct {
	// Source must be "duckdb". It is the structural guarantee that no entry
	// was ever recorded from Wadjet's own output.
	Source   string `json:"source"`
	RowCount int    `json:"row_count"`
	// Compare is "rows" (full fingerprint) or "count".
	Compare string `json:"compare"`
	// Ordered, rows compare: the fingerprint is order-sensitive.
	Ordered bool     `json:"ordered"`
	Columns []string `json:"columns,omitempty"`
	Fine    string   `json:"fine,omitempty"`
	Coarse  string   `json:"coarse,omitempty"`
	// Tolerance/Limit/Why, count compare only.
	Tolerance int    `json:"tolerance,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	Why       string `json:"why,omitempty"`
}

func (e duckdbBaselineEntry) Fingerprint() oracle.Fingerprint {
	return oracle.Fingerprint{Rows: e.RowCount, Fine: e.Fine, Coarse: e.Coarse}
}

func loadDuckDBBaseline(t *testing.T) map[string]duckdbBaselineEntry {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(".", duckdbBaselineFile))
	if err != nil {
		t.Fatalf("read duckdb baseline: %v", err)
	}
	var b duckdbBaseline
	if err := json.Unmarshal(data, &b); err != nil {
		t.Fatalf("parse duckdb baseline: %v", err)
	}
	if err := validateDuckDBBaseline(b); err != nil {
		t.Fatalf("%s: %v", duckdbBaselineFile, err)
	}
	t.Logf("%s: %d DuckDB-derived entries (%s)", duckdbBaselineFile, len(b.Queries), b.Generator)
	return b.Queries
}

// validateDuckDBBaseline checks the file's provenance and shape. The source
// check is the loud one: an entry that cannot be traced to DuckDB output is
// the Q05 failure mode — a wrong answer frozen as the expectation, against
// which a CORRECT engine fails. Only regenerateDuckDBBaseline writes this
// file and it stamps every entry from DuckDB output, so an unstamped entry
// means someone hand-edited one in.
func validateDuckDBBaseline(b duckdbBaseline) error {
	if len(b.Queries) == 0 {
		return fmt.Errorf("no entries")
	}
	names := make([]string, 0, len(b.Queries))
	for name := range b.Queries {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		e := b.Queries[name]
		if e.Source != "duckdb" {
			return fmt.Errorf("entry %s has source %q, want \"duckdb\" — this baseline is NOT ground truth. "+
				"Regenerate with WADJET_REGENERATE_DUCKDB_BASELINE=1 (needs %s); never hand-write an entry from Wadjet output",
				name, e.Source, duckdbBin)
		}
		switch e.Compare {
		case "rows":
			if e.Fine == "" || e.Coarse == "" || len(e.Columns) == 0 {
				return fmt.Errorf("entry %s compares rows but carries no fingerprint", name)
			}
		case "count":
		default:
			return fmt.Errorf("entry %s has unknown compare mode %q", name, e.Compare)
		}
	}
	return nil
}

func writeDuckDBBaseline(t *testing.T, b duckdbBaseline) {
	t.Helper()
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		t.Fatalf("marshal duckdb baseline: %v", err)
	}
	data = append(data, '\n')
	if err := os.WriteFile(filepath.Join(".", duckdbBaselineFile), data, 0o644); err != nil {
		t.Fatalf("write duckdb baseline: %v", err)
	}
}

// assertBaselineCoversCorpus requires the file and the corpus to name the
// same queries, and each entry to carry the mode its query calls for. A
// corpus query with no entry would otherwise pass unchecked, and a stale
// entry would hide that its query stopped being gated.
func assertBaselineCoversCorpus(t *testing.T, corpus []duckdbCase, stored map[string]duckdbBaselineEntry) {
	t.Helper()
	seen := make(map[string]bool, len(corpus))
	for _, c := range corpus {
		seen[c.name] = true
		e, ok := stored[c.name]
		if !ok {
			t.Fatalf("%s has no entry for %s — regenerate with WADJET_REGENERATE_DUCKDB_BASELINE=1",
				duckdbBaselineFile, c.name)
		}
		wantCompare := "rows"
		if c.countOnly {
			wantCompare = "count"
		}
		if e.Compare != wantCompare {
			t.Fatalf("%s: entry %s stored as %q but the query calls for %q — regenerate the baseline",
				duckdbBaselineFile, c.name, e.Compare, wantCompare)
		}
		if !c.countOnly && e.Ordered != c.ordered {
			t.Fatalf("%s: entry %s stored ordered=%v but the query's top-level ORDER BY says %v — regenerate the baseline",
				duckdbBaselineFile, c.name, e.Ordered, c.ordered)
		}
	}
	for name := range stored {
		if !seen[name] {
			t.Errorf("%s has a stale entry %s that no corpus query claims", duckdbBaselineFile, name)
		}
	}
}
