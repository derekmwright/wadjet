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
	// The BYTES fixture (#570). It is the SAME rows the PostgreSQL arm
	// loads (pgBytesRows), not a second copy: `bytea` appeared nowhere in
	// benchmarks/ before that issue, so neither oracle had a BYTES value to
	// compare, and two hand-kept fixtures would be two questions.
	rows[pgBytesTable] = pgBytesRows()
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
				dRows, dCols, err := runDuckDB(duckdbSetup, c.duckSQL())
				if err != nil {
					t.Fatalf("duckdb: %v", err)
				}
				assertStoredMatchesLiveDuckDB(t, c, want, dCols, dRows)
			}

			aRows, aCols, aErr := runWadjet(ctx, embedded, c.sql)
			if aErr != nil {
				reportArmError(t, armLocal, c, aErr)
			} else {
				checkArm(t, armLocal, c, want, aRows, aCols)
			}

			bRows, bCols, bErr := runArm(t, ctx, dag, c.sql)
			if bErr != nil {
				reportArmError(t, armDAG, c, bErr)
			} else {
				checkArm(t, armDAG, c, want, bRows, bCols)
			}

			if t.Failed() && liveCompare {
				dRows, dCols, err := runDuckDB(duckdbSetup, c.duckSQL())
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
	// duckdbSQL is the DuckDB-dialect spelling of the same question, for
	// the rare entry whose sql MEANS something different in the two
	// engines. ADR-0012 rule 3: on a semantic divergence, configure the
	// oracle — never exempt the entry, which would blind the arm to real
	// bugs in exactly the queries most likely to carry them. Integer
	// division is the case in point: `/` between integers truncates in
	// PostgreSQL (and Wadjet, #369) but stays float in DuckDB, whose
	// truncating spelling is `//`. Empty means sql runs on both engines
	// unchanged.
	duckdbSQL string
}

// duckSQL is the spelling the DuckDB side runs — sql itself unless the
// entry carries a dialect override.
func (c duckdbCase) duckSQL() string {
	if c.duckdbSQL != "" {
		return c.duckdbSQL
	}
	return c.sql
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
		// Integer division truncates toward zero (#369, PostgreSQL
		// semantics per ADR-0012). DuckDB's `/` stays float — its
		// truncating spelling is `//`, with the same toward-zero rule on
		// every sign combination — so this is a semantic divergence of
		// DIALECT, and the duckdbSQL override asks DuckDB the same
		// question in its own spelling instead of exempting the entry.
		duckdbCase{name: "IntegerDivisionDialect",
			sql: `SELECT n_nationkey, n_nationkey/4 AS q, (0 - n_nationkey)/4 AS neg,
				7/2 AS a, (-7)/2 AS b, 7.0/2 AS float_control FROM nation ORDER BY n_nationkey`,
			duckdbSQL: `SELECT n_nationkey, n_nationkey//4 AS q, (0 - n_nationkey)//4 AS neg,
				7//2 AS a, (-7)//2 AS b, 7.0/2 AS float_control FROM nation ORDER BY n_nationkey`},
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
			FROM nation GROUP BY NULLIF(n_regionkey, 1) ORDER BY k`},
		// Sort placement: PostgreSQL and DuckDB put NULLS LAST for ASC and
		// NULLS FIRST for DESC. Both directions, because a single default
		// can be right by accident.
		duckdbCase{name: "NullOrderingAsc", sql: "SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k, n_name"},
		duckdbCase{name: "NullOrderingDesc", sql: "SELECT NULLIF(n_regionkey, 1) AS k, n_name FROM nation ORDER BY k DESC, n_name"},
		// DISTINCT treats NULLs as equal to each other — the one place SQL
		// does — so exactly one NULL comes back.
		duckdbCase{name: "NullDistinct", sql: "SELECT DISTINCT NULLIF(n_regionkey, 1) AS k FROM nation ORDER BY k"},
		// A comparison against NULL is UNKNOWN, so it neither matches nor
		// anti-matches; only IS NULL finds those rows.
		duckdbCase{name: "NullComparisonNeverMatches", sql: `SELECT
			COUNT(*) FILTER (WHERE NULLIF(n_regionkey, 1) = 1) AS eq,
			COUNT(*) FILTER (WHERE NULLIF(n_regionkey, 1) <> 1) AS ne,
			COUNT(*) FILTER (WHERE NULLIF(n_regionkey, 1) IS NULL) AS isnull FROM nation`},
		// A LEFT JOIN that misses: the right side's columns are NULL, and
		// COUNT(right_col) must not count them. This is how real data grows
		// NULLs even when no column is nullable.
		//
		// The miss is spelled as a COLUMN comparison that cannot match (no
		// nation shares a name with a region). It used to be spelled
		// `n.n_regionkey = r.r_regionkey + 100`, which produced the right
		// answer for the wrong reason: the expression reached the executor
		// as a column name, resolved to nothing, and matched nothing — the
		// #351 defect, correct at +100 and wrong at +3. That shape now runs
		// as a keyless outer join with the expression as its probe residual
		// (#358; OuterJoinExpressionKey below carries it), so the NULL
		// semantics this pair exists for are asserted through a plain miss
		// as well.
		duckdbCase{name: "LeftJoinMissIsNull", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_name = r.r_name ORDER BY n.n_name`},
		duckdbCase{name: "LeftJoinMissCount", sql: `SELECT COUNT(*) AS rows_out, COUNT(r.r_name) AS matched
			FROM nation n LEFT JOIN region r ON n.n_name = r.r_name`},
		// NULL propagates through arithmetic and string building rather than
		// being treated as an identity element.
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
			sql: "SELECT SUM(l_extendedprice) AS s FROM lineitem JOIN part ON p_partkey = l_partkey WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'"},
		// Control 1: the same empty join counted instead of summed. The DAG
		// keeps the row here, so the defect is not "empty join loses the
		// stage" in general.
		duckdbCase{name: "EmptyJoinUngroupedCount",
			sql: "SELECT COUNT(*) AS c FROM lineitem JOIN part ON p_partkey = l_partkey WHERE p_brand = 'Brand#23' AND p_container = 'MED BOX'"},
		// Control 2: the same SUM emptied by a filter rather than a join.
		// Correct on every path, so the join is the trigger.
		duckdbCase{name: "EmptyScanUngroupedSum",
			sql: "SELECT SUM(l_extendedprice) AS s FROM lineitem WHERE l_orderkey < 0"},
		// #338: a NULL group key reported once per PARALLEL PARTIAL instead
		// of once. GROUP BY, DISTINCT and set operations are the places SQL
		// requires NULLs to be treated as EQUAL to each other, so a merge
		// that combines worker partials by key has to match a NULL key to a
		// NULL key — and one that appends instead splits the group without
		// losing a row: four rows of 375 for the one row of 1500 below,
		// which every row-count and total-preserving check calls fine.
		//
		// The right side of the join matches nothing, so every group key is
		// NULL. Only tables big enough to run parallel partials can show it
		// (customer is 1500 rows; the NULL* entries above are all over
		// nation's 25 and were green throughout), which is why the whole
		// SF0.01 corpus missed it.
		//
		// Arm B was exempt for a DIFFERENT defect this reduction runs into: on
		// the stage DAG a LEFT JOIN whose build side is empty returned no rows
		// at all — `SELECT COUNT(*) FROM customer LEFT JOIN orders ON ... AND
		// o_orderkey < 0` answered 0 where the truth is 1500, so the outer
		// join degenerated to an inner one. Fixed in #348: the worker's
		// empty-build short-circuit dropped every probe row, which is right
		// for an inner/semi join and destroys a LEFT/FULL/ANTI one.
		duckdbCase{name: "NullGroupKeyEmptyLeftJoin", sql: `SELECT o.o_orderstatus AS s, COUNT(*) AS c
			FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0
			GROUP BY o.o_orderstatus`},
		// The same shape keyed on an INT column, so the fix cannot be
		// specific to the string group-key path.
		duckdbCase{name: "NullGroupKeyEmptyLeftJoinInt", sql: `SELECT o.o_shippriority AS s, COUNT(*) AS c
			FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0
			GROUP BY o.o_shippriority`},
		// Multi-column, with one key column NULL and one not: a fix narrowed
		// to "the whole key is NULL" leaves these five groups split.
		duckdbCase{name: "NullGroupKeyEmptyLeftJoinPartialNull", sql: `SELECT c.c_mktsegment AS m,
			o.o_orderstatus AS s, COUNT(*) AS c
			FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0
			GROUP BY c.c_mktsegment, o.o_orderstatus`},
		// DISTINCT over the same all-NULL column: one NULL row, on the
		// GroupByAll path that partitioned aggregation never takes, so the
		// merge alone has to be right.
		duckdbCase{name: "NullGroupKeyEmptyLeftJoinDistinct", sql: `SELECT DISTINCT o.o_orderstatus AS s
			FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0`},
		// The partially-matching sibling, gated on BOTH arms: the join keeps
		// four orders, so the NULL group is aggregated next to real ones and
		// the DAG answers it correctly. This is the control that says the
		// exemptions above are about the empty build side and nothing else.
		duckdbCase{name: "NullGroupKeyMixedLeftJoin", sql: `SELECT o.o_orderstatus AS s, COUNT(*) AS c
			FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 5
			GROUP BY o.o_orderstatus`},
		// A NULL made by one expression and consumed by another, on a STRING
		// column — the shape whose output type is decided by a function's
		// declaration rather than by any column in the input schema.
		//
		// coalesced returned the integer 0 on every row, on both paths, with
		// no error (#331): coalesce mirrors the type of the first argument
		// anything decides, its first argument is nullif over a bare column,
		// and nullif — able to decide nothing — answered with its numeric
		// fallback as though it were fact. coalesce believed it and never
		// asked the string literal beside it. nested_twice is the same at two
		// levels, null_through_upper carries the NULL out through a
		// fixed-return function instead of consuming it, and numeric_fallback
		// is the control that must stay a number: nothing in it decides a
		// type either, and the numeric fallback is the right answer there.
		// Pagination, held against the engine that defines it. Every one of
		// these came back as the whole 25-row table before #337 — the
		// parser read LIMIT and OFFSET in a fixed order and discarded
		// whichever came second, and the builder read OFFSET only inside the
		// LIMIT branch. The first page looked right, which is why it took a
		// fuzzer to find. The DAG then ignored the OFFSET at its stages
		// (#344.2), so the four spellings have to be checked on both arms
		// and not only on the one that parses them.
		duckdbCase{name: "OffsetAlone", sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5"},
		duckdbCase{name: "OffsetThenLimit", sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 5 LIMIT 3"},
		duckdbCase{name: "LimitThenOffset", sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey LIMIT 3 OFFSET 5"},
		// A page past the last page is empty; returning every row is what
		// the offset bug did. Count-compared because an empty result has no
		// rows to fingerprint — DuckDB's CSV writer emits nothing at all,
		// not even a header — and with tolerance 0 the count IS the whole
		// answer here: 0 rows or a failure, no third outcome.
		duckdbCase{name: "OffsetPastEnd", sql: "SELECT n_nationkey FROM nation ORDER BY n_nationkey OFFSET 100",
			countOnly: true, why: "an empty result has no rows to fingerprint; at tolerance 0 the count is the entire answer"},
		duckdbCase{name: "OffsetOverGroupBy", sql: "SELECT n_regionkey, COUNT(*) AS c FROM nation GROUP BY n_regionkey ORDER BY n_regionkey OFFSET 3"},
		// ORDER BY after a set operation sorts the WHOLE result, not the last
		// arm. The ordinal used to be left unresolved for set operations —
		// positional refs were skipped outright — so the sort key stayed the
		// literal "1", matched no column, and the rows came back in arrival
		// order with no error.
		//
		// These were pinned on the DAG until #346: walkStages emitted each
		// arm's stages and no merge ("each side runs independently; merge
		// results at the end" — nothing merged), so the gather attached to
		// whichever arm was emitted last and the answer was that arm's raw
		// scan, at half the rows and its table's full width. Both arms are
		// gated now — the DAG emits a union stage that projects each arm
		// onto the result columns and concatenates them, and a UNION's
		// dedup rides a GroupByAll aggregate above it.
		duckdbCase{name: "UnionAllOrderByOrdinal",
			sql: "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1"},
		duckdbCase{name: "UnionAllOrderByOrdinalLimit",
			sql: "SELECT r_regionkey FROM region UNION ALL SELECT r_regionkey FROM region ORDER BY 1 LIMIT 3"},
		duckdbCase{name: "UnionOrderByOrdinal",
			sql: "SELECT n_regionkey FROM nation WHERE n_nationkey < 5 UNION " +
				"SELECT n_regionkey FROM nation WHERE n_nationkey >= 5 ORDER BY 1"},
		duckdbCase{name: "UnionOrderByOrdinalLimit",
			sql: "SELECT n_regionkey FROM nation WHERE n_nationkey < 5 UNION " +
				"SELECT n_regionkey FROM nation WHERE n_nationkey >= 5 ORDER BY 1 LIMIT 2"},
		duckdbCase{name: "NullPropagatesThroughExpressions", sql: `SELECT n_nationkey,
			COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback') AS coalesced,
			COALESCE(NULLIF(NULLIF(n_name, 'ALGERIA'), 'BRAZIL'), 'twice') AS nested_twice,
			UPPER(NULLIF(n_name, 'ARGENTINA')) AS null_through_upper,
			COALESCE(NULLIF(n_regionkey, 0), -1) AS numeric_fallback,
			NULLIF(n_regionkey, 1) + 1 AS plus, NULLIF(n_name, 'ALGERIA') || '!' AS cat
			FROM nation ORDER BY n_nationkey`},
		// The same family with nothing but bare columns inside the call, so
		// no literal is present to rescue the type. Every column here came
		// back as the integer 0 (#333): coalesce, nullif, greatest and least
		// declare a numeric fallback, a bare column reference decided
		// nothing, and the fallback stood — a string column typed Float64,
		// whose writes were dropped. ifnull_str is the control that was
		// already right, because ifnull alone declares its fallback String.
		duckdbCase{name: "PolymorphicOverStringColumns", sql: `SELECT n_nationkey,
			NULLIF(n_name, 'ALGERIA') AS nullif_str,
			COALESCE(n_name) AS coalesce_one,
			COALESCE(n_name, n_comment) AS coalesce_two,
			GREATEST(n_name, n_comment) AS greatest_str,
			LEAST(n_name, n_comment) AS least_str,
			IFNULL(n_name, n_comment) AS ifnull_str
			FROM nation ORDER BY n_nationkey`},
		// The constraint that ruled out fixing the above by flipping those
		// declarations to String: the same functions over numeric columns
		// stay numeric. mixed_literal is the debatable case — an int column
		// beside a string literal — where wadjet follows the first argument
		// that decides, which is the column. DuckDB agrees:
		// typeof(COALESCE(42, 'text')) is INTEGER.
		duckdbCase{name: "PolymorphicOverNumericColumns", sql: `SELECT n_nationkey,
			NULLIF(n_nationkey, 1) AS nullif_int,
			COALESCE(n_regionkey, 0) AS coalesce_int,
			GREATEST(n_nationkey, n_regionkey) AS greatest_int,
			LEAST(n_nationkey, n_regionkey) AS least_int,
			COALESCE(n_regionkey, 'text') AS mixed_literal
			FROM nation ORDER BY n_nationkey`},
		// A float column must keep its float, and the fractions are the
		// point: COALESCE(ps_supplycost, 0) answered 771 for 771.64, because
		// the literal 0 was the first argument that decided anything and the
		// column beside it decided nothing at all.
		duckdbCase{name: "PolymorphicOverFloatColumns", sql: `SELECT ps_partkey, ps_suppkey,
			COALESCE(ps_supplycost, 0) AS coalesce_float,
			GREATEST(ps_supplycost, ps_availqty) AS greatest_mixed,
			LEAST(ps_supplycost, ps_availqty) AS least_mixed
			FROM partsupp WHERE ps_partkey <= 20 ORDER BY ps_partkey, ps_suppkey`},
		// The variance family (#339). Both arms answered these with numbers
		// that look like standard deviations — arm A from a fraction of the
		// rows (a partial's state was dropped at every parallel merge, so
		// 143940.6 against a true 144048.14), arm B by re-aggregating the
		// per-task STDDEV values (531.79). A wrong-but-plausible float is
		// exactly what a cross-engine fingerprint is for: the row count is
		// right in both cases and only the value gives it away.
		//
		// o_totalprice is the discriminating column — mean ~2.5e5 against a
		// spread of ~1.4e5, where a sum-of-squares accumulator cancels away
		// its leading digits. The 0.07% error the merge defect produced is
		// visible at both fingerprint precisions (1.43941e+05 / 1.439e+05
		// against 1.44048e+05 / 1.44e+05), so the entry fails on the old
		// implementation rather than passing inside the float tolerance.
		//
		// All six spellings in one row also pins the semantics: DuckDB reads
		// bare STDDEV and VARIANCE as the SAMPLE forms, and the stored
		// fingerprint is DuckDB's, so a Wadjet that switched to population
		// would fail here — the n/(n-1) factor is 1.00003 at 15000 rows,
		// which survives the 6-digit fine precision.
		duckdbCase{name: "VarianceFamily", sql: `SELECT STDDEV(o_totalprice) AS s, STDDEV_SAMP(o_totalprice) AS ss,
			STDDEV_POP(o_totalprice) AS sp, VARIANCE(o_totalprice) AS v,
			VAR_SAMP(o_totalprice) AS vs, VAR_POP(o_totalprice) AS vp FROM orders`},
		// Grouped, so partial states combine per key rather than into one
		// global accumulator — the shape a distributed shuffle produces.
		duckdbCase{name: "StddevGrouped", sql: `SELECT o_orderstatus AS k, STDDEV(o_totalprice) AS s, COUNT(*) AS c
			FROM orders GROUP BY o_orderstatus ORDER BY k`},
		// The other end of the scale: l_discount runs 0..0.10, so its spread
		// is the same order as its mean. The reported error went the other
		// way on this column, which is what tells accumulated noise apart
		// from a constant sample/population factor.
		duckdbCase{name: "StddevSmallSpread", sql: "SELECT STDDEV(l_discount) AS s, VAR_POP(l_discount) AS vp FROM lineitem"},
		// #335 — a WHERE predicate over the NULL-supplying side of an outer
		// join. pushFilterThroughJoin pushed it to whichever child owned its
		// columns without looking at the join type, so the predicate ran
		// BELOW the padding: region was filtered to one row and every
		// unmatched nation was then padded back in. 25 rows for a 5-row
		// answer, on both arms, and 25 again for the IS NULL form whose
		// answer is 0. Reported as a count first (unmissable) and then with
		// values, since a demotion that dropped the right columns instead
		// would keep the count.
		duckdbCase{name: "OuterWhereOnNullSupplyingSide", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`},
		duckdbCase{name: "OuterWhereOnNullSupplyingSideValues", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey WHERE r.r_name = 'EUROPE' ORDER BY n.n_name`},
		// The anti-join idiom, which is the case a careless fix for the
		// above breaks: IS NULL is how a query ASKS for the unmatched rows,
		// so it must neither push below the join nor demote it. The ON's
		// second conjunct narrows region to three of its five rows, so ten
		// nations come back unmatched.
		duckdbCase{name: "OuterWhereAntiJoinIdiom", sql: `SELECT n.n_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3
			WHERE r.r_regionkey IS NULL ORDER BY n.n_name`},
		// The same idiom over a join that matches everything: 0 rows, which
		// is what the pre-fix engine answered with 25.
		duckdbCase{name: "OuterWhereAntiJoinIdiomEmpty", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey IS NULL`},
		// A predicate over the null-supplying side that is neither rejecting
		// nor a bare IS NULL. Both must stay above the join: the OR because
		// one tolerant arm makes the whole thing tolerant, COALESCE because
		// it is not strict in the column it wraps. The ON restricts region to
		// three rows, so the answers mix matched and padded rows — pushing
		// either predicate down would return all 25.
		duckdbCase{name: "OuterWhereNullTolerantOr", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3
			WHERE r.r_regionkey IS NULL OR r.r_regionkey = 1 ORDER BY n.n_name, r.r_name`},
		duckdbCase{name: "OuterWhereCoalesce", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3
			WHERE COALESCE(r.r_name, 'none') = 'none'`},
		// Controls that must not move: the same predicate over an INNER
		// join, and a predicate over the PRESERVED side of the outer join.
		// Both were correct before the fix and stay correct after it.
		duckdbCase{name: "InnerWhereSamePredicate", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`},
		duckdbCase{name: "OuterWhereOnPreservedSide", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey WHERE n.n_nationkey < 4 ORDER BY n.n_name`},
		// RIGHT and FULL carry the same rule with the sides exchanged: a
		// RIGHT JOIN pads its LEFT input, and a FULL JOIN pads both, so a
		// null-rejecting WHERE demotes FULL to the outer join on the other
		// side rather than straight to inner.
		duckdbCase{name: "RightJoinWhereOnNullSupplyingSide", sql: `SELECT COUNT(*) AS c FROM region r
			RIGHT JOIN nation n ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`},
		duckdbCase{name: "FullJoinWhereOnNullSupplyingSide", sql: `SELECT COUNT(*) AS c FROM nation n
			FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey WHERE r.r_regionkey = 2`},
		duckdbCase{name: "FullJoinWhereAntiJoinIdiom", sql: `SELECT COUNT(*) AS c FROM nation n
			FULL OUTER JOIN region r ON r.r_regionkey = n.n_nationkey WHERE r.r_regionkey IS NULL`},
		// The RIGHT JOIN mirror of the anti-join idiom, which is where the
		// gate found that the stage DAG loses a RIGHT JOIN's unmatched rows
		// outright — 0 of the 20 unmatched nations, and 5 rows instead of 25
		// for the same join with no WHERE at all. That is not a predicate
		// defect: it needs no WHERE to appear, and the single-process arm was
		// correct in both forms. Fixed in #352: the worker's fragment path
		// had no equivalent of the single-process joinFlushSource, so nothing
		// ever emitted a RIGHT/FULL join's unmatched build rows.
		duckdbCase{name: "RightJoinWhereAntiJoinIdiom", sql: `SELECT n.n_name FROM region r
			RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey WHERE r.r_regionkey IS NULL ORDER BY n.n_name`},
		// A LEFT JOIN whose build side comes back EMPTY still preserves every
		// left row. The DAG returned none of them — the same "NULL-padded
		// rows are lost" family as the RIGHT JOIN entry above, reached
		// without a RIGHT JOIN. Both halves fixed in #348.
		duckdbCase{name: "LeftJoinEmptyBuildSide", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0`},
		// An expression on one side of an ON equality — the join key the
		// physical planner could not represent either. parseJoinKeys split
		// the condition on "=" and handed the executor "r.r_regionkey + 3"
		// as a COLUMN NAME; nothing resolved, so the join matched nothing.
		// Both arms, and visible only as a wrong count: 0 rows for 10.
		// Fixed in #351 — parseJoinKeys reads the condition structurally,
		// and an operand that is not a bare column is lifted into the filter
		// above the join instead of being passed off as a column name. The
		// rest of the shape is grouped at the end of this corpus.
		duckdbCase{name: "ExpressionJoinKey", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey = r.r_regionkey + 3`},

		// #336 — an ON conjunct comparing two COLUMNS. The physical planner
		// represents a join condition as key pairs: parseJoinKeys keeps the
		// conjuncts containing "=" and discards the rest without a word, so
		// the self-join deduplication idiom (`a.id < b.id`, one row per
		// unordered pair) answered as if its second conjunct were absent —
		// 494 rows for a 197-row query.
		duckdbCase{name: "OnClauseColumnConjunct", sql: `SELECT COUNT(*) AS c FROM supplier a
			JOIN supplier b ON a.s_nationkey = b.s_nationkey AND a.s_suppkey < b.s_suppkey`},
		duckdbCase{name: "OnClauseColumnConjunctValues", sql: `SELECT a.s_suppkey AS lo, b.s_suppkey AS hi
			FROM supplier a JOIN supplier b ON a.s_nationkey = b.s_nationkey AND a.s_suppkey < b.s_suppkey
			WHERE a.s_suppkey < 6 ORDER BY lo, hi`},
		// The same across two tables, so the defect is not specific to a
		// self-join's column qualification.
		duckdbCase{name: "OnClauseCrossTableInequality", sql: `SELECT n.n_name, r.r_name FROM nation n
			JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey ORDER BY n.n_name`},
		// Controls: a column-vs-LITERAL conjunct in ON was always honoured
		// (extractJoinCondPredicates pushes it to the child that owns it),
		// and the same column-vs-column conjunct in WHERE was always
		// honoured. The fix makes ON agree with WHERE; it must not move
		// either control.
		duckdbCase{name: "OnClauseColumnVsLiteral", sql: `SELECT COUNT(*) AS c FROM supplier a
			JOIN supplier b ON a.s_nationkey = b.s_nationkey AND a.s_suppkey < 5`},
		duckdbCase{name: "WhereClauseColumnConjunct", sql: `SELECT COUNT(*) AS c FROM supplier a
			JOIN supplier b ON a.s_nationkey = b.s_nationkey WHERE a.s_suppkey < b.s_suppkey`},
		// The residual on an OUTER join was a DIFFERENT defect from the
		// inner-join case above: an outer join's ON is evaluated BEFORE the
		// NULL-padding, so the residual could not be lifted above the join
		// the way the inner case is — it would delete the unmatched rows the
		// join exists to preserve. HashJoin's Residual field (join.go) now
		// carries exactly this, evaluated during the outer-join probe before
		// padding — both arms answer this correctly and match DuckDB
		// (verified 2026-08-23; this entry's knownBug was stale, gated fully
		// with no exemption, and passing).
		duckdbCase{name: "OuterJoinOnResidual", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey
			WHERE r.r_name IS NULL`},
		// Window VALUE functions over a non-float column (#345). LAG, LEAD,
		// FIRST_VALUE, LAST_VALUE and NTH_VALUE return a value taken FROM
		// their input column, so the output type is that column's type — and
		// the planner declared all five float64 from a hand-maintained name
		// list. exec.Window allocates its output vector at the declared type
		// and, unlike exec.Project, corrected nothing at runtime, so every
		// string write was dropped and the column came back as the integer 0
		// on every row. DuckDB is the only thing here that can say what the
		// answer should have been, since both Wadjet paths were wrong
		// together on the strings and the ranks were right all along.
		//
		// NTH_VALUE names n_name nowhere else in the SELECT list on purpose:
		// column pruning read the whole argument string ("n_name, 2") as the
		// column name, so the real column was never marked required.
		//
		// Both arms are gated. These were pinned on arm B until the stage
		// DAG grew a window operator (#349): walkStages emitted a window
		// stage, nothing converted it to a fragment, and the task failed
		// outright with "empty Operators".
		duckdbCase{name: "WindowValueFunctionsString", sql: `SELECT n_nationkey, n_name,
			LAG(n_name) OVER (ORDER BY n_nationkey) AS lag_name,
			LEAD(n_name) OVER (ORDER BY n_nationkey) AS lead_name,
			FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey) AS first_name,
			LAST_VALUE(n_name) OVER (ORDER BY n_nationkey) AS last_name,
			NTH_VALUE(n_comment, 2) OVER (ORDER BY n_nationkey
			  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nth_comment
			FROM nation ORDER BY n_nationkey`,
		},
		// The same five over a DATE column and an INT column, partitioned.
		// The numeric control is not a formality: Float64.SetValue has no
		// int32 case either, so a narrow int was dropped exactly like a
		// string, and a DATE typed float64 is not a mis-scaled date, it is 0.
		duckdbCase{name: "WindowValueFunctionsDateAndInt", sql: `SELECT o_orderkey, o_orderdate,
			LAG(o_orderdate) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS prev_date,
			FIRST_VALUE(o_orderdate) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS first_date,
			LAG(o_custkey) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey) AS prev_cust
			FROM orders WHERE o_orderkey <= 200 ORDER BY o_orderkey`,
		},
		// The rank family, which is genuinely input-independent and is the
		// half of the name list that stays hand-maintained. It was correct
		// before the fix and must stay correct after it.
		duckdbCase{name: "WindowRankFamily", sql: `SELECT n_nationkey,
			ROW_NUMBER() OVER (ORDER BY n_nationkey) AS rn,
			RANK() OVER (ORDER BY n_regionkey) AS rk,
			DENSE_RANK() OVER (ORDER BY n_regionkey) AS drk,
			NTILE(4) OVER (ORDER BY n_nationkey) AS nt,
			COUNT(*) OVER (ORDER BY n_nationkey) AS running_count
			FROM nation ORDER BY n_nationkey`,
		},
		// LAST_VALUE with an explicit whole-partition frame. The window
		// operator now resolves every frame-sensitive function over its
		// frame, so this is gated on both arms (#350).
		duckdbCase{name: "WindowLastValueExplicitFrame", sql: `SELECT n_nationkey,
			LAST_VALUE(n_name) OVER (ORDER BY n_nationkey
			  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv
			FROM nation ORDER BY n_nationkey`},

		// #348/#352 — an outer join over an EMPTY side. The rows it exists to
		// preserve are exactly the ones neither side's data can produce, so
		// the engine has to know what an absent side's columns are CALLED.
		//
		// COUNT(*) beside COUNT(col) is the discriminating pair: an empty
		// build used to leave the build columns ABSENT from the joined schema
		// rather than NULL, and an absent column reads as NULL through the
		// projection's missing-name fallback while COUNT(col) degenerates to
		// COUNT(*) — 1500 for an answer of 0, on the single-process arm,
		// looking entirely plausible. Both spellings of the column type,
		// because the fallback is per-type.
		duckdbCase{name: "EmptyBuildLeftJoinCountCol", sql: `SELECT COUNT(*) AS rows_out,
			COUNT(o.o_orderstatus) AS matched_str, COUNT(o.o_orderkey) AS matched_int
			FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 0`},
		// The same join asked for its values: 25 preserved rows, every
		// build-side cell NULL. A count alone cannot tell a NULL-padded row
		// from a matched one.
		duckdbCase{name: "EmptyBuildLeftJoinValues", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0 ORDER BY n.n_name`},
		// The anti-join idiom over the empty build, which is how a query ASKS
		// for the unmatched rows and the shape a careless fix breaks: the
		// column has to be NULL, not missing, for IS NULL to find it. 0 on
		// both arms before the fix, against 25.
		duckdbCase{name: "EmptyBuildLeftJoinIsNull", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0
			WHERE r.r_regionkey IS NULL`},
		// Its complement, which must stay 0: a fix that made IS NULL match by
		// making the column always-NULL-looking would keep this at 0 too, so
		// it is the values entry above that separates them.
		duckdbCase{name: "EmptyBuildLeftJoinIsNotNull", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0
			WHERE r.r_regionkey IS NOT NULL`},
		// The other three join types over the same empty build. INNER and
		// SEMI (EXISTS) legitimately answer nothing — the empty-build
		// short-circuit is CORRECT for them and the fix must not cost them —
		// while ANTI (NOT EXISTS) keeps all 25 and was lost with LEFT, since
		// it preserves probe rows the build never matched for the same
		// reason.
		duckdbCase{name: "EmptyBuildInnerJoin", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 0`},
		duckdbCase{name: "EmptyBuildSemiJoin", sql: `SELECT COUNT(*) AS c FROM nation n
			WHERE EXISTS (SELECT 1 FROM region r WHERE r.r_regionkey = n.n_regionkey AND r.r_regionkey < 0)`},
		duckdbCase{name: "EmptyBuildAntiJoin", sql: `SELECT COUNT(*) AS c FROM nation n
			WHERE NOT EXISTS (SELECT 1 FROM region r WHERE r.r_regionkey = n.n_regionkey AND r.r_regionkey < 0)`},
		// The RIGHT JOIN's preserved rows with their VALUES, not their count.
		// The count was right on the single-process arm all along; the values
		// were not — every unmatched nation came back with n_name NULL,
		// because the flush was handed the join's OUTPUT schema as though it
		// were the probe's and mapped the preserved columns onto the NULL
		// half.
		duckdbCase{name: "RightJoinUnmatchedValues", sql: `SELECT n.n_name, r.r_name FROM region r
			RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey ORDER BY n.n_name`},
		// A RIGHT JOIN where NOTHING matches (two disjoint string columns):
		// the probe emits no output batch at all, which is what used to skip
		// the unmatched flush entirely — 0 rows for an answer of 25.
		duckdbCase{name: "RightJoinNoMatchAtAll", sql: `SELECT COUNT(*) AS c FROM region r
			RIGHT JOIN nation n ON r.r_name = n.n_name`},
		// A RIGHT JOIN whose PROBE side is empty: the ON conjunct narrows
		// region to nothing, and all 25 nations survive with the region
		// columns NULL. On the DAG this is the shape where a shuffle
		// partition holds build rows and no probe rows — the ordinary case,
		// not a degenerate one.
		duckdbCase{name: "EmptyProbeRightJoin", sql: `SELECT COUNT(*) AS c, COUNT(r.r_name) AS m
			FROM region r RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey AND r.r_regionkey < 0`},
		// FULL OUTER carries both halves at once: 25 nations and 5 regions,
		// none of them matched, so the answer is 30 rows of which every one
		// is NULL-padded on one side. Values, so a fix that lost the
		// preserved side's identity fails here.
		duckdbCase{name: "FullJoinUnmatchedBothSides", sql: `SELECT n.n_name, r.r_name FROM nation n
			FULL OUTER JOIN region r ON n.n_name = r.r_name ORDER BY n.n_name, r.r_name`},
		duckdbCase{name: "FullJoinUnmatchedBothSidesCount", sql: `SELECT COUNT(*) AS c,
			COUNT(n.n_name) AS n_rows, COUNT(r.r_name) AS r_rows FROM nation n
			FULL OUTER JOIN region r ON n.n_name = r.r_name`},
		// #352.2 — a keyless join: a comma join whose only condition is an
		// inequality. Legal SQL the single-process path runs as a Cartesian
		// product with the predicate above it; the DAG had no operator for a
		// join with no equality keys and failed the task outright with
		// "hash_join_probe: LeftKeys and RightKeys required".
		duckdbCase{name: "KeylessCrossJoinFilter", sql: `SELECT COUNT(*) AS c FROM region a, nation b
			WHERE a.r_regionkey < b.n_nationkey`},
		duckdbCase{name: "KeylessCrossJoinValues", sql: `SELECT a.r_name, b.n_name FROM region a, nation b
			WHERE a.r_regionkey < b.n_nationkey AND b.n_nationkey < 3 ORDER BY a.r_name, b.n_name`},
		// Found while fixing the above: an ON conjunct restricting one side
		// of a FULL OUTER join cannot be pushed into that side's scan the
		// way it can for LEFT (LeftJoinOnConjunctBuildSide below) — a FULL
		// join preserves rows a scan-level push would delete (Regions 3 and
		// 4 would vanish instead of arriving unmatched: 25 rows for an
		// answer of 27). #358's Residual join field (join.go) now carries
		// the conjunct through the probe instead of pushing it, on both
		// arms — this and the sibling family below (#358: the outer-join ON
		// residual, per join type) pin that fix (verified 2026-08-23; this
		// entry's knownBug was stale, gated fully with no exemption, and
		// passing).
		duckdbCase{name: "FullJoinOnConjunctBuildSide", sql: `SELECT COUNT(*) AS c FROM nation n
			FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3`},
		duckdbCase{name: "LeftJoinOnConjunctBuildSide", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND r.r_regionkey < 3`},
		// #353 — the aggregates that #339 left holding a finished value on
		// the stage DAG. Every entry below runs over orders (15000 rows) or
		// lineitem (60175), never nation: at 25 rows one partial covers the
		// whole input and the answer is right whether or not the partials
		// merge, which is the shape that would pass either way.
		//
		// What arm B actually answered before the fix is worth stating,
		// because it is not the "median of medians" the issue predicted: the
		// worker's parseAggFuncString had no case for any of these names, so
		// they fell to its `default: AggSum` and every one of them returned
		// the SUM of its first argument. MEDIAN(o_totalprice) came back
		// 2.127e9 against a true 135698.6.
		//
		// CORR and COVAR now decompose across the wire the way STDDEV does
		// (covar_decompose.go): the partial ships its (count, meanX, meanY,
		// C, M2x, M2y) sextuple, merge stages combine them pairwise, the
		// final stage folds. Both arms were wrong here before — arm A
		// answered NULL, because the SELECT parser dropped the second
		// argument and the covariance state was never updated.
		duckdbCase{name: "CovarianceFamily", sql: `SELECT CORR(o_totalprice, o_custkey) AS c,
			COVAR_SAMP(o_totalprice, o_custkey) AS cs, COVAR_POP(o_totalprice, o_custkey) AS cp
			FROM orders`},
		// Grouped, so states combine per key — the shape a shuffle produces.
		duckdbCase{name: "CovarianceGrouped", sql: `SELECT o_orderstatus AS k,
			CORR(o_totalprice, o_custkey) AS c, COVAR_POP(o_totalprice, o_custkey) AS cp, COUNT(*) AS n
			FROM orders GROUP BY o_orderstatus ORDER BY k`},

		// MEDIAN and the percentile family. These do NOT decompose, so they
		// are gated to a RawInputAggregate final (agg_whole_input.go): one
		// task per group, every row of a group aggregated exactly once.
		duckdbCase{name: "MedianUngrouped", sql: "SELECT MEDIAN(o_totalprice) AS m FROM orders"},
		duckdbCase{name: "MedianGrouped", sql: `SELECT o_orderstatus AS k, MEDIAN(o_totalprice) AS m, COUNT(*) AS n
			FROM orders GROUP BY o_orderstatus ORDER BY k`},
		// QUANTILE_CONT/QUANTILE_DISC is DuckDB's spelling of
		// PERCENTILE_CONT/PERCENTILE_DISC with the arguments reversed, and
		// the only spelling of the percentile family both engines accept:
		// DuckDB's PERCENTILE_CONT is ordered-set syntax (WITHIN GROUP),
		// which this parser does not take, and Wadjet's two-argument form
		// DuckDB does not. Both names now plan to the same aggregate, so
		// this entry is ground truth for PERCENTILE_CONT/_DISC as well.
		//
		// The two functions must not agree: _cont interpolates between the
		// two straddling values (255771.439) and _disc returns an actual
		// member of the input (255762.04).
		duckdbCase{name: "PercentileUngrouped", sql: `SELECT quantile_cont(o_totalprice, 0.9) AS p90,
			quantile_disc(o_totalprice, 0.9) AS d90, quantile_cont(o_totalprice, 0.1) AS p10 FROM orders`},
		duckdbCase{name: "PercentileGrouped", sql: `SELECT o_orderstatus AS k,
			quantile_cont(o_totalprice, 0.75) AS p75, quantile_disc(o_totalprice, 0.25) AS d25
			FROM orders GROUP BY o_orderstatus ORDER BY k`},

		// MODE over l_linenumber: 60175 rows, and the winner (1, on every
		// order's first line) beats the runner-up by thousands, so no tie
		// decides the answer. MODE(o_custkey) would have been useless here —
		// 32 different customers tie at 32 orders each and the two engines
		// pick different ones, which is not a divergence either of them is
		// wrong about.
		duckdbCase{name: "ModeNumeric", sql: "SELECT MODE(l_linenumber) AS m FROM lineitem"},
		duckdbCase{name: "ModeGrouped", sql: `SELECT l_returnflag AS k, MODE(l_linenumber) AS m, COUNT(*) AS n
			FROM lineitem GROUP BY l_returnflag ORDER BY k`},
		// MODE over a STRING column is a separate, pre-existing gap and is
		// pinned rather than fixed: collectState holds float64 values and
		// the resolved extractor for a string column is nil, so nothing is
		// ever collected. Both arms return NULL together, so this is not the
		// distribution defect #353 is about — but it is silent, so it is
		// gated here against DuckDB's answer and the pin fails the moment it
		// starts working.
		duckdbCase{name: "ModeString", sql: "SELECT MODE(o_orderpriority) AS m FROM orders",
			knownBugArm: armBoth,
			knownBug: "MODE over a non-numeric column returns NULL: collectState accumulates float64 and " +
				"resolveFloat64Extractor has no case for a string column, so no value is ever collected. " +
				"Numeric MODE is correct (ModeNumeric above)",
		},

		// MIN_BY/MAX_BY return a value taken from their FIRST argument,
		// selected by their second. Both arguments were broken: the second
		// never reached the planner at all (NULL on both arms), and once it
		// did the output column was still declared float64, so the string it
		// selected was dropped and every row came back 0 — #345's window
		// defect in the aggregate.
		duckdbCase{name: "MinByMaxBy", sql: `SELECT MIN_BY(o_orderpriority, o_totalprice) AS mn,
			MAX_BY(o_orderpriority, o_totalprice) AS mx FROM orders`},
		duckdbCase{name: "MinByMaxByGrouped", sql: `SELECT o_orderstatus AS k,
			MIN_BY(o_orderpriority, o_totalprice) AS mn, MAX_BY(o_orderpriority, o_totalprice) AS mx
			FROM orders GROUP BY o_orderstatus ORDER BY k`},
		// The deliberate tie: o_shippriority is 0 on all 15000 rows, so
		// EVERY row of every group is tied for both the minimum and the
		// maximum and each partial holds a different candidate. The answer
		// stays determined because the value column is the group key, so all
		// the tied candidates carry the same value — which is what makes
		// this comparable across engines at all: SQL does not say which of
		// several tied rows MIN_BY returns, so a tie whose candidates differ
		// in value has no cross-engine answer to hold anything to. What it
		// pins is that a tie resolves to a tied row's value rather than to
		// NULL, to 0, or to a value the merge invented.
		duckdbCase{name: "MinByMaxByTie", sql: `SELECT o_orderpriority AS k,
			MIN_BY(o_orderpriority, o_shippriority) AS mn, MAX_BY(o_orderpriority, o_shippriority) AS mx,
			COUNT(*) AS n FROM orders GROUP BY o_orderpriority ORDER BY k`},

		// STRING_AGG. The separator argument was dropped with every other
		// second argument, so a query asking for '::' got ',' — 14999
		// characters short over orders, and no row count or NULL check sees
		// it. LENGTH() makes the ungrouped case order-independent, which it
		// has to be: STRING_AGG without an ORDER BY concatenates in whatever
		// order rows arrive, and two correct engines need not agree on that.
		duckdbCase{name: "StringAggSeparatorLength",
			sql: "SELECT LENGTH(STRING_AGG(o_orderpriority, '::')) AS n FROM orders"},
		// The grouped case compares the STRING itself, order and all, by
		// aggregating the group key: every value in a group is then
		// identical, so the concatenation is the same string whatever order
		// the 15000 rows arrive in — while still pinning the separator, the
		// element count and the values.
		duckdbCase{name: "StringAggGrouped", sql: `SELECT o_orderstatus AS k,
			STRING_AGG(o_orderstatus, '::') AS s, COUNT(*) AS n
			FROM orders GROUP BY o_orderstatus ORDER BY k`},

		// The gated route over a JOIN rather than a bare scan: the raw rows
		// the final aggregate consumes arrive through an exchange, and the
		// group keys come from a third table. 25 groups over 15000 joined
		// rows, so every group's rows are spread across the exchange's
		// partitions. HAVING on top, since PostFilterExprs run on the final
		// task only and a gated final IS the only task.
		duckdbCase{name: "MedianOverJoin", sql: `SELECT n_name AS k, MEDIAN(o_totalprice) AS m,
			MIN_BY(o_orderpriority, o_totalprice) AS cheapest, COUNT(*) AS n
			FROM orders JOIN customer ON o_custkey = c_custkey
			JOIN nation ON c_nationkey = n_nationkey
			GROUP BY n_name ORDER BY k`},
		duckdbCase{name: "MedianWithHaving", sql: `SELECT o_orderstatus AS k, MEDIAN(o_totalprice) AS m
			FROM orders GROUP BY o_orderstatus HAVING COUNT(*) > 1000 ORDER BY k`},
		// CORR through the same join, where the decomposed state has to
		// survive an exchange as well as the partial/final split.
		duckdbCase{name: "CorrOverJoin", sql: `SELECT n_name AS k, CORR(o_totalprice, o_custkey) AS c
			FROM orders JOIN customer ON o_custkey = c_custkey
			JOIN nation ON c_nationkey = n_nationkey
			GROUP BY n_name ORDER BY k`},

		// #351 — an ON equality whose operand is an EXPRESSION. The key
		// parser split the condition TEXT on "=" and handed the executor
		// "r.r_regionkey + 3" as a COLUMN NAME; an unresolvable name hashes
		// as a constant, so the join matched nothing when the other side was
		// a real column and EVERYTHING when neither was. ExpressionJoinKey
		// above is the reported form; these are the rest of the shape.
		duckdbCase{name: "ExpressionJoinKeyLeftOperand", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey + 3 = r.r_regionkey`},
		// Neither side a column: 125 rows, the full cross product, for a
		// 20-row query — the "matches everything" half of the same defect.
		duckdbCase{name: "ExpressionJoinKeyBothOperands", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey + 1 = r.r_regionkey + 2`},
		// An equality against a LITERAL is the same shape reached without
		// arithmetic. extractJoinCondPredicates would have pushed it to the
		// owning child, but it declines a single-conjunct ON — so this one
		// went all the way to the executor as the key column "1".
		duckdbCase{name: "LiteralJoinKey", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey = 1`},
		// The values, not just the count: a wrong key set is invisible in a
		// COUNT when the cardinalities happen to agree.
		duckdbCase{name: "ExpressionJoinKeyValues", sql: `SELECT n.n_name, r.r_name FROM nation n
			JOIN region r ON n.n_regionkey = r.r_regionkey + 3 ORDER BY n.n_name`},
		// The expression key inside a three-table join, where the residual
		// filter has to survive join reordering.
		duckdbCase{name: "ExpressionJoinKeyThreeTable", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey = r.r_regionkey + 3
			JOIN supplier s ON s.s_nationkey = n.n_nationkey`},
		// A non-equi operator that CONTAINS an "=". The text split found that
		// "=" and produced the column name "a.x <". The inner-join forms are
		// lifted into the filter above the join (#336's machinery), so these
		// must simply be right.
		duckdbCase{name: "OnClauseLessOrEqual", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey <= r.r_regionkey`},
		duckdbCase{name: "OnClauseGreaterOrEqual", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey >= r.r_regionkey`},
		duckdbCase{name: "OnClauseNotEqual", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey != r.r_regionkey`},
		// Control: the `1 = 1` ON-TRUE sentinel the optimizer writes when
		// every ON conjunct has been pushed to a child. It is not a column
		// reference and must NOT be refused — it means the cross product.
		duckdbCase{name: "OnClauseAllConjunctsPushed", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON r.r_regionkey = 1 AND r.r_name = 'AMERICA'`},
		// The OUTER-join half of #351: an outer join's ON is evaluated
		// BEFORE the NULL-padding, so the inner-join route — lift the
		// conjunct into a filter above the join — is not available here; it
		// would delete the rows the join exists to preserve. The old
		// behavior was worse and quieter: the expression reached the
		// executor as a column name, matched nothing, and every left row
		// came back padded — the answer to `+ 100` by luck and wrong for
		// `+ 3`. #358's Residual join field now carries the expression
		// through the probe on both arms instead (verified 2026-08-23; this
		// entry's knownBug was stale, gated fully with no exemption, and
		// passing) — see the #358 family below, which pins this shape on
		// every join disposition.
		duckdbCase{name: "OuterJoinExpressionKey", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey + 3 ORDER BY n.n_name`},

		// #355 — an aggregate over a RENAMED subquery column. walkStages
		// treats a Project as a passthrough, so the rename never happens on
		// the DAG: the scan read o_custkey, the aggregate asked for `n`, and
		// exec.HashAggregate answered the column it could not resolve with
		// NULL. Not aggregate-specific, so all four of them.
		duckdbCase{name: "MaxOverRenamedSubqueryColumn",
			sql: "SELECT MAX(n) AS m FROM (SELECT o_custkey AS n FROM orders)"},
		duckdbCase{name: "MinOverRenamedSubqueryColumn",
			sql: "SELECT MIN(n) AS m FROM (SELECT o_custkey AS n FROM orders)"},
		duckdbCase{name: "SumOverRenamedSubqueryColumn",
			sql: "SELECT SUM(n) AS m FROM (SELECT o_custkey AS n FROM orders)"},
		duckdbCase{name: "CountOverRenamedSubqueryColumn",
			sql: "SELECT COUNT(n) AS m FROM (SELECT o_custkey AS n FROM orders)"},
		// A CTE rename reaches the same aggregate by a different route.
		duckdbCase{name: "MaxOverRenamedCTEColumn",
			sql: "WITH t AS (SELECT o_custkey AS n FROM orders) SELECT MAX(n) AS m FROM t"},
		// The alias over an EXPRESSION, where there is no source column to
		// read and the value has to be projected before aggregating.
		duckdbCase{name: "MaxOverRenamedSubqueryExpression",
			sql: "SELECT MAX(n) AS m FROM (SELECT o_custkey * 2 AS n FROM orders)"},
		// A POLYMORPHIC expression under the alias. Its type is only
		// decidable from the columns it reads, which live below the Project,
		// so typing it against the Project's own output leaves the Float64
		// fallback and drops every string — the #333 shape reached through
		// #355's rename.
		duckdbCase{name: "MinOverRenamedSubqueryPolymorphicExpression",
			sql: "SELECT MIN(u) AS m FROM (SELECT COALESCE(n_name, n_comment) AS u FROM nation)"},
		// A GROUP BY key naming a subquery's computed alias: the key is
		// dispatched under the expression text, which is the spelling the
		// worker's pre-aggregate projection compiles and emits.
		duckdbCase{name: "GroupByRenamedSubqueryExpression", sql: `SELECT k, COUNT(*) AS c
			FROM (SELECT UPPER(o_orderstatus) AS k FROM orders) GROUP BY k ORDER BY k`},
		// A renamed GROUP BY key is the louder half: an unresolvable key
		// serializes as a NULL key, so all 15000 rows collapsed into one
		// group named NULL instead of the three o_orderstatus values.
		duckdbCase{name: "GroupByRenamedSubqueryColumn", sql: `SELECT k, COUNT(*) AS c
			FROM (SELECT o_orderstatus AS k FROM orders) GROUP BY k ORDER BY k`},
		duckdbCase{name: "GroupByAndAggregateRenamed", sql: `SELECT k, MAX(n) AS m, COUNT(*) AS c
			FROM (SELECT o_orderstatus AS k, o_custkey AS n FROM orders) GROUP BY k ORDER BY k`},
		// A two-column aggregate reads InputCol2 through the same lookup.
		duckdbCase{name: "CorrOverRenamedSubqueryColumns", sql: `SELECT CORR(a, b) AS c
			FROM (SELECT o_totalprice AS a, o_custkey AS b FROM orders)`},
		// Control: the same subquery WITHOUT the rename was always correct,
		// which is what made the rename the whole of the defect.
		duckdbCase{name: "MaxOverUnrenamedSubqueryColumn",
			sql: "SELECT MAX(o_custkey) AS m FROM (SELECT o_custkey FROM orders)"},
		// --- #343: ORDER BY ... NULLS FIRST / NULLS LAST ---
		//
		// All four combinations of direction and explicit placement, plus
		// the two defaults. Only the two explicit spellings on a DESC key
		// were wrong (the DESC comparator negated the kernel's null handling
		// along with its values), so four of these six passed before the fix
		// — which is why NullOrderingAsc/Desc above, testing the defaults
		// alone, saw nothing. The key is NULLable via NULLIF; the tiebreak
		// on n_name keeps the row sequence determined.
		duckdbCase{name: "NullOrderingAscNullsFirst", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name
			FROM nation ORDER BY k ASC NULLS FIRST, n_name`},
		duckdbCase{name: "NullOrderingAscNullsLast", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name
			FROM nation ORDER BY k ASC NULLS LAST, n_name`},
		duckdbCase{name: "NullOrderingDescNullsFirst", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name
			FROM nation ORDER BY k DESC NULLS FIRST, n_name`},
		duckdbCase{name: "NullOrderingDescNullsLast", sql: `SELECT NULLIF(n_regionkey, 1) AS k, n_name
			FROM nation ORDER BY k DESC NULLS LAST, n_name`},
		// The issue's own query: a LEFT JOIN that misses is how the NULLs
		// get there, and the string key means the value signature is blind
		// to it. Before the fix the three NULL rows came back first.
		duckdbCase{name: "NullOrderingLeftJoinDescNullsLast", sql: `SELECT o.o_orderstatus AS s, c.c_custkey
			FROM customer c LEFT JOIN orders o ON c.c_custkey = o.o_custkey AND o.o_orderkey < 40
			ORDER BY o.o_orderstatus DESC NULLS LAST, c.c_custkey`},
		// Explicit placement on a SECOND key, whose direction differs from
		// the first: a fix that resolved placement per query rather than per
		// key would answer this one wrong.
		duckdbCase{name: "NullOrderingSecondKey", sql: `SELECT n_regionkey AS r, NULLIF(n_nationkey % 5, 1) AS k, n_name
			FROM nation ORDER BY r DESC, k DESC NULLS LAST, n_name`},

		// --- #350: an explicit window frame is obeyed ---
		//
		// The frame was parsed, carried through the logical plan, put on the
		// exec spec and shipped on the wire, and read by nothing: every
		// value and aggregate function decided on the presence of an ORDER
		// BY alone. Each pair below is the same function with an explicit
		// frame and with none, because only the pair shows that the frame is
		// what moved.
		//
		// n_nationkey is unique, so ROWS and RANGE agree here and the frame
		// is the only variable.
		duckdbCase{name: "WindowLastValueDefaultFrame", sql: `SELECT n_nationkey,
			LAST_VALUE(n_name) OVER (ORDER BY n_nationkey) AS lv
			FROM nation ORDER BY n_nationkey`},
		duckdbCase{name: "WindowFirstValueMovingLowerBound", sql: `SELECT n_nationkey,
			FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS fv1,
			FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS fv2,
			FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey) AS fv_default
			FROM nation ORDER BY n_nationkey`},
		duckdbCase{name: "WindowNthValueFrames", sql: `SELECT n_nationkey,
			NTH_VALUE(n_name, 2) OVER (ORDER BY n_nationkey) AS nv_default,
			NTH_VALUE(n_name, 2) OVER (ORDER BY n_nationkey ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS nv_moving,
			NTH_VALUE(n_name, 2) OVER (ORDER BY n_nationkey
			  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS nv_all
			FROM nation ORDER BY n_nationkey`},
		// A running total against a whole-partition total: the same SUM,
		// differing only in frame, and getting it wrong is a plausible
		// number rather than an error.
		duckdbCase{name: "WindowAggregateFrames", sql: `SELECT n_nationkey,
			SUM(n_regionkey) OVER (ORDER BY n_nationkey) AS running,
			SUM(n_regionkey) OVER (ORDER BY n_nationkey
			  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total,
			SUM(n_regionkey) OVER (ORDER BY n_nationkey ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sliding,
			AVG(n_regionkey) OVER (ORDER BY n_nationkey ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sliding_avg,
			COUNT(*) OVER (ORDER BY n_nationkey ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sliding_count
			FROM nation ORDER BY n_nationkey`},
		// A frame that is EMPTY for the leading rows. SUM and the value
		// functions answer NULL there; COUNT answers 0, the one aggregate an
		// empty frame does not make NULL.
		duckdbCase{name: "WindowEmptyFrame", sql: `SELECT n_nationkey,
			SUM(n_regionkey) OVER (ORDER BY n_nationkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) AS s,
			COUNT(*) OVER (ORDER BY n_nationkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) AS c,
			FIRST_VALUE(n_name) OVER (ORDER BY n_nationkey ROWS BETWEEN 3 PRECEDING AND 2 PRECEDING) AS fv
			FROM nation ORDER BY n_nationkey`},
		// MIN/MAX over a moving lower bound: the value that leaves the frame
		// has to stop winning, which a running accumulator cannot express.
		// Over a FLOAT column — MIN/MAX of a narrow INT window column is a
		// separate, pre-existing silent zero (Vector.SetValue has no int32
		// case for a Float64 output, the #345 family), visible with and
		// without a frame and not this fix's to make.
		duckdbCase{name: "WindowMinMaxFrames", sql: `SELECT o_orderkey,
			MIN(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS mn,
			MAX(o_totalprice) OVER (ORDER BY o_orderkey ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS mx,
			MIN(o_totalprice) OVER (ORDER BY o_orderkey) AS mn_default
			FROM orders WHERE o_orderkey <= 200 ORDER BY o_orderkey`},
		// A frame inside a PARTITION BY, where the bounds must clamp to the
		// partition rather than reach across it.
		duckdbCase{name: "WindowFramePartitioned", sql: `SELECT o_orderkey, o_orderstatus,
			SUM(o_totalprice) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey
			  ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s,
			LAST_VALUE(o_orderstatus) OVER (PARTITION BY o_orderstatus ORDER BY o_orderkey
			  ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lv
			FROM orders WHERE o_orderkey <= 200 ORDER BY o_orderkey`},
		// RANGE mode's defining behaviour: bounds move by ORDER-BY PEER
		// GROUP, not by row. n_regionkey has five rows per value, so every
		// row of a tied group sees the same frame — and the default frame,
		// which is RANGE, inherits that. The engine used to answer both of
		// these row-at-a-time, so a running total over a tied key was wrong
		// by exactly the tie.
		duckdbCase{name: "WindowRangePeerGroups", sql: `SELECT n_nationkey, n_regionkey,
			SUM(n_regionkey) OVER (ORDER BY n_regionkey) AS s_default,
			COUNT(*) OVER (ORDER BY n_regionkey) AS c_default,
			SUM(n_regionkey) OVER (ORDER BY n_regionkey RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s_tail,
			SUM(n_regionkey) OVER (ORDER BY n_regionkey
			  RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS s_all
			FROM nation ORDER BY n_nationkey`},
		// #340 — CAST(x AS DATE) returned its argument unchanged, so nothing
		// downstream knew a date had been asked for and every date rule
		// reasoned about the text. DuckDB is the only thing here that can say
		// what the answers should be, and the wrong ones were all plausible
		// numbers rather than errors or NULLs.
		//
		// The two statements the issue names, run verbatim over one row. gap
		// answered 0 (ToFloat64 read 1996 out of each string and subtracted
		// them) and prev answered 1995 (1996 - 1). Both are the kind of
		// number a reader accepts.
		duckdbCase{name: "DateCastLiteralArithmetic", sql: `SELECT
			DATE '1996-01-10' - DATE '1996-01-01' AS gap,
			CAST('1996-01-10' AS DATE) - 1 AS prev,
			CAST('1996-01-10' AS DATE) + 5 AS nxt,
			CAST('1996-01-10' AS DATE) AS d
			FROM region WHERE r_regionkey = 0`},
		// The per-row form: a shipping lag over real rows, which came back 0
		// on every line of lineitem. A row count, a NULL check and a value
		// signature all pass on a column of zeros — only a cross-engine
		// comparison of the VALUES sees it.
		duckdbCase{name: "DateCastShippingLag", sql: `SELECT l_orderkey, l_linenumber,
			CAST(l_receiptdate AS DATE) - CAST(l_shipdate AS DATE) AS lag
			FROM lineitem WHERE l_orderkey <= 10 ORDER BY l_orderkey, l_linenumber`},
		// The cast's result as a GROUP BY key: the groups have to be dates,
		// and they have to be the SAME dates DuckDB groups by.
		duckdbCase{name: "DateCastGroupKey", sql: `SELECT CAST(l_shipdate AS DATE) AS k, COUNT(*) AS c
			FROM lineitem WHERE l_shipdate < '1992-01-20'
			GROUP BY CAST(l_shipdate AS DATE) ORDER BY k`},
		// Compared against another date, and carried through MIN/MAX — the
		// two places a date that is secretly a number stops ordering the way
		// a calendar does.
		duckdbCase{name: "DateCastComparison", sql: `SELECT COUNT(*) AS c FROM lineitem
			WHERE CAST(l_shipdate AS DATE) < DATE '1994-01-01'`},
		duckdbCase{name: "DateCastMinMax", sql: `SELECT MIN(CAST(l_shipdate AS DATE)) AS mn,
			MAX(CAST(l_shipdate AS DATE)) AS mx FROM lineitem`},
		// CAST(x AS TIMESTAMP) is the other half of the pair: leaving it
		// inert while DATE worked would be a new asymmetry. It is compared
		// through shapes both engines render identically — a cast back down
		// to a day, a date part, and a comparison against a typed literal —
		// because Wadjet boxes a TIMESTAMP as epoch milliseconds everywhere
		// and renders it at the renderers, exactly as a TIMESTAMP column does.
		duckdbCase{name: "TimestampCastThroughDate", sql: `SELECT o_orderkey,
			CAST(CAST(o_orderdate AS TIMESTAMP) AS DATE) AS d,
			YEAR(CAST(o_orderdate AS TIMESTAMP)) AS y
			FROM orders WHERE o_orderkey <= 10 ORDER BY o_orderkey`},
		duckdbCase{name: "TimestampCastComparison", sql: `SELECT COUNT(*) AS c FROM lineitem
			WHERE CAST(l_shipdate AS TIMESTAMP) < TIMESTAMP '1994-01-01 00:00:00'`},
		// Correlated subqueries whose outer column appears NOWHERE else in
		// the outer query — not in its SELECT list, not in its WHERE. Column
		// pruning dropped it (the pruning walk had no case for a subquery
		// node), the per-row evaluator found no such column in the batch and
		// substituted NULL, and every comparison went UNKNOWN: the scalar and
		// EXISTS forms answered 0 and the NOT EXISTS form answered the whole
		// table. All three carry a plausible-looking single number, which is
		// why nothing caught it (#347).
		//
		// The correlation must be NON-EQUI. An `=` correlation is
		// decorrelated into a join before pruning runs and never executes
		// per row, so an equality version of any of these passes with the bug
		// present.
		//
		// Arm B is pinned on all five, for a SEPARATE and pre-existing
		// defect: the stage DAG cannot run a per-row correlated subquery at
		// all. A worker compiles its fragment's filter without a
		// SubqueryRunner, so an EXISTS fails the task outright ("EXISTS
		// subquery requires a SubqueryRunner") and a correlated scalar
		// answers 0 — including for CorrelatedScalarProjectedOuterCol, which
		// has nothing to do with pruning and which arm A has always answered
		// correctly. Both arms are fully gated since #359: the DAG refuses
		// the shape and the coordinator routes it onto the local pipeline,
		// so the answers below hold everywhere.
		duckdbCase{name: "CorrelatedScalarUnprojectedOuterCol",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey)`,
		},
		// The control: the identical correlation with the outer column
		// forced into a projection, so pruning could never drop it. It was
		// correct before the fix and must stay correct after — the pair is
		// what localizes the defect to pruning.
		duckdbCase{name: "CorrelatedScalarProjectedOuterCol",
			sql: `SELECT COUNT(*) AS n FROM (SELECT c_nationkey, c_acctbal FROM customer) c1
				WHERE c1.c_acctbal > (SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey)`,
		},
		duckdbCase{name: "CorrelatedExistsUnprojectedOuterCol",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE EXISTS (SELECT 1 FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`,
		},
		duckdbCase{name: "CorrelatedNotExistsUnprojectedOuterCol",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE NOT EXISTS (SELECT 1 FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey AND c2.c_acctbal > 9000)`,
		},
		// Two subqueries deep: c1 is bound by the OUTERMOST query and read
		// inside the inner-inner SELECT, so the pruning walk, the correlation
		// analysis and the value substitution all have to recurse.
		duckdbCase{name: "CorrelatedNestedTwoDeep",
			sql: `SELECT COUNT(*) AS n FROM customer c1
				WHERE c1.c_acctbal > (SELECT AVG(c2.c_acctbal) FROM customer c2
					WHERE c2.c_acctbal > (SELECT AVG(c3.c_acctbal) FROM customer c3
						WHERE c3.c_nationkey < c1.c_nationkey))`,
		},
		// #375: an unqualified WHERE comparing columns of DIFFERENT types
		// (FLOAT64 o_totalprice <> INT32 r_regionkey) across a five-table
		// join chain ending in a LEFT JOIN. The col-col filter kernel
		// resolved from the left column's type only and panicked indexing
		// the right vector's empty typed slice; the qualified spelling never
		// saw the kernel, which is what made the unqualified form
		// load-bearing.
		duckdbCase{name: "MixedTypeCrossTableFilterJoinChain",
			sql: `SELECT t4.o_orderkey AS c8
				FROM customer t0
				JOIN nation   t1 ON t0.c_nationkey = t1.n_nationkey
				JOIN region   t2 ON t1.n_regionkey = t2.r_regionkey
				JOIN supplier t3 ON t1.n_nationkey = t3.s_nationkey
				LEFT JOIN orders t4 ON t0.c_custkey = t4.o_custkey
				WHERE o_totalprice <> r_regionkey`,
		},
		// #378: ORDER BY an aliased join column that is also the projected
		// column. The parallel Sort's MergeSink dropped the clones' schema
		// when the primary consumed nothing itself, so the sorted result
		// came back as rows with no columns at all — right row count,
		// scheduling-dependent. Ordered content compare against DuckDB pins
		// both the values and the sequence.
		duckdbCase{name: "OrderByAliasedJoinColumnAlsoProjected",
			sql: `SELECT t1.ps_suppkey AS c6
				FROM supplier t0 JOIN partsupp t1 ON t0.s_suppkey = t1.ps_suppkey
				WHERE t1.ps_partkey > 500
				ORDER BY t1.ps_suppkey`,
		},
		// BOOL_AND/BOOL_OR answered false whatever their input (#371): the
		// boolean predicate projected for the aggregate carried no declared
		// type, fell back to Float64, and dropped every write. Both arms
		// were wrong the same way, which is exactly what DuckDB truth is
		// for.
		duckdbCase{name: "BoolAggregates",
			sql: `SELECT BOOL_AND(n_nationkey >= 0) AS all_nonneg,
				BOOL_OR(n_nationkey > 3) AS any_big,
				BOOL_AND(n_nationkey > 3) AS all_big FROM nation`},
		duckdbCase{name: "BoolAggregatesGrouped",
			sql: `SELECT n_regionkey, BOOL_AND(n_nationkey > 2) AS all_late,
				BOOL_OR(n_nationkey > 20) AS any_tail
				FROM nation GROUP BY n_regionkey ORDER BY n_regionkey`},
		// MIN/MAX over a string CASE answered the integer 0 (#372); the
		// three control columns localize the trigger to the CASE alone.
		duckdbCase{name: "MinMaxOverStringCase",
			sql: `SELECT MIN(n_name) AS bare, MIN(LOWER(n_name)) AS fn,
				MIN(n_name || 'x') AS cat,
				MIN(CASE WHEN n_regionkey = 0 THEN n_name ELSE n_name END) AS case_expr
				FROM nation`},
		// Window MIN/MAX over a narrow INT column answered 0 on every row
		// (#361): the float64-declared output vector had no int32 arm in
		// Vector.SetValue. The window corpus uses a float column on
		// purpose; n_nationkey is INT32 and reaches the dropped write.
		duckdbCase{name: "WindowMinMaxNarrowInt",
			sql: `SELECT n_nationkey,
				MAX(n_nationkey) OVER (PARTITION BY n_regionkey ORDER BY n_nationkey) AS mx,
				MIN(n_name) OVER (ORDER BY n_nationkey) AS mn
				FROM nation ORDER BY n_nationkey`},
		// INTERSECT / EXCEPT (#346, second half): the stage DAG lowers them
		// as grouped counting (tag each arm, shuffle on the full row, SUM
		// tags, emit per the count rule). DuckDB truth is what pins the
		// COUNT RULES themselves — the ALL forms differ from the distinct
		// ones exactly when an arm holds duplicates, and nation carries
		// every region key five times against region's once, so min(5,1)
		// and max(0,5−1) each give an answer no other rule reproduces.
		duckdbCase{name: "IntersectDistinct",
			sql: `SELECT n_regionkey FROM nation INTERSECT SELECT r_regionkey FROM region`},
		duckdbCase{name: "IntersectAllMinCounts",
			sql: `SELECT n_regionkey FROM nation INTERSECT ALL SELECT r_regionkey FROM region`},
		duckdbCase{name: "ExceptDistinctFiltered",
			sql: `SELECT n_regionkey FROM nation EXCEPT
				SELECT r_regionkey FROM region WHERE r_regionkey < 2`},
		duckdbCase{name: "ExceptAllCountDiff",
			sql: `SELECT n_regionkey FROM nation EXCEPT ALL SELECT r_regionkey FROM region`},
		// NULL is a member and matches the other arm's NULL — GROUP BY
		// equality, not predicate equality. Both arms send region key 1 to
		// NULL; the intersection holds it once.
		duckdbCase{name: "IntersectNullsMatch",
			sql: `SELECT NULLIF(n_regionkey, 1) AS k FROM nation INTERSECT
				SELECT NULLIF(r_regionkey, 1) AS k FROM region`},
		// ORDER BY above the operation sorts the whole result (ordered
		// compare pins the sequence); the arms disagree on type (int + 100
		// vs float + 100.0), so the reconciling CAST is load-bearing on the
		// DAG — without it the two arms' rows can never compare equal.
		duckdbCase{name: "IntersectWidenedOrdered",
			sql: `SELECT n_regionkey + 100 AS k FROM nation INTERSECT
				SELECT r_regionkey + 100.0 AS k FROM region ORDER BY k`},
		// #359's SELECT-list arm: a correlated scalar PROJECTED rather than
		// filtered. Pre-fix the stage DAG failed the task at projection
		// compile ("subqueries require a SubqueryRunner"); it now rides the
		// refusal → coordinator-local route like the Correlated* family
		// above. The CAST pins the output as a number on both engines — the
		// single-process pipeline otherwise types a computed subquery column
		// as a string, which the cross-engine fingerprint reads verbatim.
		duckdbCase{name: "CorrelatedScalarInSelectList",
			sql: `SELECT c_custkey, CAST((SELECT AVG(c_acctbal) FROM customer c2
					WHERE c2.c_nationkey < c1.c_nationkey) AS DOUBLE) AS below_avg
				FROM customer c1 WHERE c1.c_custkey <= 25 ORDER BY c_custkey`},
		// #379: DISTINCT over an expression whose polymorphic type resolution
		// needs the CATALOG — COALESCE(float_col, 0) is Float64 because the
		// column is, and only the literal Int64 without it. The stage DAG's
		// pre-aggregate projection used to type the derived group key from
		// the expression text alone, truncating every float price into an
		// Int64 key vector: 28634 rows where DuckDB says 35921, on the DAG
		// only. The fix ships the planner's resolved type on the wire
		// (OpSpec.GroupByTypes). The corpus had no DISTINCT-over-expression
		// entry, which is why 175 gated entries never saw it.
		duckdbCase{name: "DistinctCoalesceLiteralScan",
			sql: `SELECT DISTINCT COALESCE(l_extendedprice, 0) AS c1 FROM lineitem`},
		// The shape the fuzzer found it in (seed 95): the same key above a
		// LEFT JOIN whose NULL-padded side is what the COALESCE is for.
		// Exercises the chain-terminal partial aggregate (join-fused) rather
		// than the fused scan-aggregate, so both dispatch paths stay pinned.
		duckdbCase{name: "DistinctCoalesceOverLeftJoin",
			sql: `SELECT DISTINCT COALESCE(t2.l_extendedprice, 0) AS c1
				FROM customer t0
				JOIN orders t1 ON t0.c_custkey = t1.o_custkey
				LEFT JOIN lineitem t2 ON t1.o_orderkey = t2.l_orderkey`},
		// --- #358: the outer-join ON residual, per join type ---
		//
		// The three formerly pinned entries above (OuterJoinOnResidual,
		// FullJoinOnConjunctBuildSide, OuterJoinExpressionKey) carry the
		// original defect shapes; these pin the semantics the fix must hold
		// on every disposition. The rule under test: the residual runs on
		// the COMBINED row before a match is accepted, a probe row whose
		// candidates all fail is UNMATCHED (LEFT/FULL still emit it padded,
		// never drop it), and a build row is matched only when some probe
		// row passed key AND residual — the bit the RIGHT/FULL unmatched
		// flush reads. On the DAG these also police the replicated-build
		// hazard: RIGHT/FULL never broadcast, so a fix that broke that gate
		// triples the unmatched rows on the three-worker arm B.
		//
		// VALUES, not counts: a count cannot tell a padded row from a
		// matched one.
		duckdbCase{name: "LeftJoinResidualCrossSideValues", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey
			ORDER BY n.n_name`},
		// A probe-side-only residual conjunct: extractJoinCondPredicates
		// must NOT push it into the probe scan (that deletes the rows a
		// LEFT join preserves) — it rides the residual instead, and the
		// failing rows come back padded.
		duckdbCase{name: "LeftJoinResidualProbeSideValues", sql: `SELECT n.n_name, r.r_name FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > 10
			ORDER BY n.n_name`},
		// RIGHT: the preserved side is the BUILD, so the unmatched flush is
		// what carries the residual-failed rows. COUNT(r.r_name) beside the
		// values separates "padded" from "matched" the #348 way.
		duckdbCase{name: "RightJoinResidualValues", sql: `SELECT n.n_name, r.r_name FROM region r
			RIGHT JOIN nation n ON r.r_regionkey = n.n_regionkey AND n.n_nationkey > r.r_regionkey
			ORDER BY n.n_name, r.r_name`},
		duckdbCase{name: "RightJoinResidualCount", sql: `SELECT COUNT(*) AS c, COUNT(r.r_name) AS matched
			FROM region r RIGHT JOIN nation n ON r.r_regionkey = n.n_regionkey AND n.n_nationkey > r.r_regionkey`},
		// FULL: both directions at once — probe rows whose candidates all
		// fail come back padded AND build rows nothing accepted flush
		// padded on the other side.
		duckdbCase{name: "FullJoinResidualBothSides", sql: `SELECT n.n_name, r.r_name FROM nation n
			FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey
			ORDER BY n.n_name, r.r_name`},
		// A residual that evaluates to NULL is a REJECTION (UNKNOWN is not
		// TRUE), not an error and not a match: the derived build column rk2
		// is NULL exactly for region 2, so every region-2 nation comes back
		// padded — while a residual that read NULL as "false-so-what" for
		// the OTHER regions, or errored on it, moves rows the fingerprint
		// pins. TPC-H holds no NULLs, so the NULL reaches the residual
		// through a subquery NULLIF, cross-side so it cannot be pushed into
		// the build scan. exec's unit grid covers the same rule over
		// hand-built NULLs.
		duckdbCase{name: "LeftJoinResidualNullEval", sql: `SELECT n.n_name, r.rk2 FROM nation n
			LEFT JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
			ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.rk2
			ORDER BY n.n_name`},
		// The keyless residual on the flush side: an expression-operand
		// equality leaves NO key conjunct, so every build row is a
		// candidate and the residual does all of the matching — RIGHT and
		// FULL exercise the unmatched flush over that degenerate chain
		// (LEFT is OuterJoinExpressionKey above).
		duckdbCase{name: "RightJoinExpressionKeyCount", sql: `SELECT COUNT(*) AS c, COUNT(r.r_name) AS matched
			FROM region r RIGHT JOIN nation n ON r.r_regionkey = n.n_nationkey - 20`},
		duckdbCase{name: "FullJoinExpressionKeyCount", sql: `SELECT COUNT(*) AS c,
			COUNT(n.n_name) AS n_rows, COUNT(r.r_name) AS r_rows FROM nation n
			FULL OUTER JOIN region r ON n.n_regionkey = r.r_regionkey + 3`},
		// Controls that must not move: the same residual on an INNER join
		// takes the #336 lift path and the two must agree with each other
		// through DuckDB; the anti-join IS NULL idiom over a plain
		// (residual-free) LEFT join keeps its answer.
		duckdbCase{name: "InnerJoinResidualControl", sql: `SELECT COUNT(*) AS c FROM nation n
			JOIN region r ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey`},
		duckdbCase{name: "PlainLeftJoinIsNullControl", sql: `SELECT COUNT(*) AS c FROM nation n
			LEFT JOIN region r ON n.n_regionkey = r.r_regionkey WHERE r.r_name IS NULL`},

		// --- #376: a comma cross join mixed with an ON join ---
		//
		// Each half always planned alone; only the MIXTURE failed — the
		// comma-joined relation contributes no ON clause, and the join
		// reorderer spelled the disconnected relation as an inner join with
		// an EMPTY condition, which key extraction refused ("could not
		// extract join keys from:" with nothing after the colon) and the
		// DAG emitted as a keyless hash_join the worker rejects. An absent
		// condition IS a cross join; the executor has had the Cartesian
		// path since #352.
		duckdbCase{name: "CommaJoinAfterOnJoin", sql: `SELECT COUNT(*) AS c
			FROM region t0 JOIN nation t1 ON t0.r_regionkey = t1.n_regionkey, supplier t2`},
		duckdbCase{name: "CommaJoinAfterOnJoinValues", sql: `SELECT t1.n_name, t2.s_suppkey AS c4
			FROM region t0 JOIN nation t1 ON t0.r_regionkey = t1.n_regionkey, supplier t2
			WHERE t2.s_suppkey <= 2 AND t1.n_nationkey <= 3 ORDER BY t1.n_name, c4`},
		// The fuzzer's second seed: two ON joins then the comma relation,
		// reduced to an aggregate so a wrong cross-product multiplicity is
		// unmissable (DuckDB: 13.22).
		duckdbCase{name: "CommaJoinAfterTwoOnJoins", sql: `SELECT AVG(t1.s_nationkey) AS c3
			FROM partsupp t0 JOIN supplier t1 ON t0.ps_suppkey = t1.s_suppkey
			JOIN part t2 ON t0.ps_partkey = t2.p_partkey, nation t3`},
		// The comma relation FIRST, and an explicit CROSS JOIN spelling of
		// the same mixture — the issue's "worth checking" pair.
		duckdbCase{name: "CommaJoinBeforeOnJoin", sql: `SELECT COUNT(*) AS c
			FROM region t0, nation t1 JOIN supplier t2 ON t1.n_nationkey = t2.s_nationkey`},
		duckdbCase{name: "CrossJoinMixedWithOnJoin", sql: `SELECT COUNT(*) AS c
			FROM region t0 JOIN nation t1 ON t0.r_regionkey = t1.n_regionkey CROSS JOIN supplier t2`},
		// A WHERE above the mixture supplying the cross-product filter: the
		// comma-lift pass turns it into a real join condition, which is the
		// path that must keep working once the reorderer spells the
		// leftover cross joins as such.
		duckdbCase{name: "CommaJoinMixtureWhereFilter", sql: `SELECT COUNT(*) AS c
			FROM region t0 JOIN nation t1 ON t0.r_regionkey = t1.n_regionkey, supplier t2
			WHERE t1.n_nationkey = t2.s_nationkey`},

		// --- #383: a computed subquery projection feeding a join input ---
		//
		// walkStages treats a subquery's Project as a passthrough, so a
		// COMPUTED column existed nowhere on the DAG: the scan's phantom
		// read of it fell back to full width, the build/probe files never
		// carried it, and every downstream read — an ON residual
		// (LeftJoinResidualNullEval above), the projected output, a sort
		// key — saw NULL or a missing column, silently. Renames were
		// already resolved-through per consumer (#355/#313); a computed
		// value has no source column to resolve TO, so it materializes at
		// the source instead (absorbComputedSubqueryProjection). One face
		// per entry, both join sides, plus the consumers above the join.
		duckdbCase{name: "JoinBuildComputedProjected", sql: `SELECT n.n_name, r.rk2
			FROM nation n JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
			ON n.n_regionkey = r.r_regionkey ORDER BY n.n_name`},
		duckdbCase{name: "JoinProbeComputedProjected", sql: `SELECT nx.n_name, nx.nk2, r.r_name
			FROM (SELECT n_name, n_regionkey, NULLIF(n_nationkey, 3) AS nk2 FROM nation) nx
			JOIN region r ON nx.n_regionkey = r.r_regionkey ORDER BY nx.n_name`},
		// Probe-side symmetry of the pinned build-side case: the residual
		// reads a computed PROBE column ("join residual column resolves on
		// neither side" was the pre-fix warning, and every LEFT row came
		// back padded).
		duckdbCase{name: "JoinProbeComputedResidual", sql: `SELECT nx.n_name, r.r_name
			FROM (SELECT n_name, n_regionkey, NULLIF(n_nationkey, 3) AS nk2 FROM nation) nx
			LEFT JOIN region r ON nx.n_regionkey = r.r_regionkey AND nx.nk2 > r.r_regionkey
			ORDER BY nx.n_name, r.r_name`},
		duckdbCase{name: "JoinBuildComputedOrderBy", sql: `SELECT n.n_name, r.rk2
			FROM nation n JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
			ON n.n_regionkey = r.r_regionkey ORDER BY r.rk2, n.n_name`},
		// The aggregate face rides #355's resolve-through (InputExpr) —
		// this pins that the two mechanisms compose over a join.
		duckdbCase{name: "JoinBuildComputedAggAbove", sql: `SELECT SUM(r.rk2) AS s
			FROM nation n JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
			ON n.n_regionkey = r.r_regionkey`},
		// No join at all: a sort keyed on the computed alias, which named
		// no column anywhere on the DAG (the scan emitted raw full width).
		duckdbCase{name: "SubqueryComputedOrderBy", sql: `SELECT rk2
			FROM (SELECT NULLIF(r_regionkey, 2) AS rk2 FROM region) t ORDER BY rk2`},
		// The WHERE faces are NOT the DAG passthrough: pushdownPredicates'
		// Filter-Project swap used to push the predicate below the Project
		// without substituting the computed alias, so the single-process
		// path errored and the DAG filtered everything out — both arms,
		// #384. Fixed by substituting the alias's defining expression into
		// the predicate at the swap (splitFilterForProjectPush); the
		// substituted predicate then also rides scan pushdown.
		duckdbCase{name: "SubqueryComputedWhere", sql: `SELECT rk2
			FROM (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) t
			WHERE rk2 > 1 ORDER BY rk2`},
		duckdbCase{name: "JoinBuildComputedWhereAbove", sql: `SELECT n.n_name, r.rk2
			FROM nation n JOIN (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) r
			ON n.n_regionkey = r.r_regionkey WHERE r.rk2 > 1 ORDER BY n.n_name`},
		// #384's sibling faces: the rename spelling (same substitution
		// check, alias of a plain column), a predicate mixing a computed
		// alias with a passthrough column, and a CASE-computed alias whose
		// NULLs must survive substitution (three-valued logic: WHERE on a
		// NULL-producing expression filters the NULL rows on both arms).
		duckdbCase{name: "SubqueryRenamedWhere", sql: `SELECT k
			FROM (SELECT r_regionkey AS k FROM region) t
			WHERE k > 1 ORDER BY k`},
		// #385: walkStages treats a rename-only subquery Project as a
		// passthrough, so no stage ever emits the alias — the gather's
		// OutputRenames sourced the alias name, resolved nothing, and fell
		// back to passing the full upstream width through under SOURCE
		// names. Fixed by chasing each rename's source through nested
		// Projects (resolveOutputRenameSource) and resolving a join's
		// NeededColumns the same way (resolveJoinNeededColumns). The faces
		// below cover the family: bare forward (full-width symptom),
		// multi-rename, rename through a join (either side), rename above
		// an aggregate subquery, chained renames, and an alias shadowing a
		// real column of the same table (wrong-VALUES symptom: the DAG
		// answered with the REAL r_name). Unordered entries stay unordered
		// so they hold regardless of #386 (the ORDER-BY face of the same
		// passthrough, pinned below).
		duckdbCase{name: "SubqueryRenamedBare", sql: `SELECT k
			FROM (SELECT r_regionkey AS k FROM region) t ORDER BY k`},
		duckdbCase{name: "SubqueryRenamedMulti", sql: `SELECT k1, k2
			FROM (SELECT r_regionkey AS k1, r_name AS k2 FROM region) t ORDER BY k1`},
		duckdbCase{name: "SubqueryRenamedJoinBuild", sql: `SELECT n_name, k
			FROM nation JOIN (SELECT r_regionkey AS k FROM region) t
			ON n_regionkey = k ORDER BY n_name`},
		duckdbCase{name: "SubqueryRenamedJoinProbe", sql: `SELECT k, r_name
			FROM (SELECT n_regionkey AS k, n_name FROM nation) t JOIN region
			ON k = r_regionkey ORDER BY r_name, k`},
		duckdbCase{name: "SubqueryRenamedAboveAgg", sql: `SELECT k
			FROM (SELECT n_regionkey AS k, COUNT(*) AS c FROM nation GROUP BY n_regionkey) t
			ORDER BY k`},
		duckdbCase{name: "SubqueryRenamedChained", sql: `SELECT a
			FROM (SELECT b AS a FROM (SELECT r_regionkey AS b FROM region) u) t`},
		duckdbCase{name: "SubqueryRenamedShadow", sql: `SELECT r_name
			FROM (SELECT r_comment AS r_name FROM region) t`},
		// #386's family: ORDER BY on a nested subquery rename. The DESC face
		// exposed the silent sort no-op; the ASC face sorts n_regionkey,
		// whose scan order (0,1,1,1,4,0,3,...) is NOT ascending, so it
		// asserts an order scan luck cannot supply (single column, so tied
		// rows are identical and the ordered digest stays deterministic);
		// the shadow face must sort by the ALIASED source (r_comment), not
		// the real r_name the stream also carries; plus chained rename,
		// LIMIT, and a rename forwarded through a join.
		duckdbCase{name: "SubqueryRenamedOrderDesc", sql: `SELECT k
			FROM (SELECT r_regionkey AS k FROM region) t ORDER BY k DESC`},
		duckdbCase{name: "SubqueryRenamedOrderAsc", sql: `SELECT k
			FROM (SELECT n_regionkey AS k FROM nation) t ORDER BY k`},
		duckdbCase{name: "SubqueryRenamedOuterAliasOrder", sql: `SELECT k AS j
			FROM (SELECT r_regionkey AS k FROM region) t ORDER BY j DESC`},
		duckdbCase{name: "SubqueryRenamedChainedOrder", sql: `SELECT a
			FROM (SELECT b AS a FROM (SELECT r_regionkey AS b FROM region) u) t ORDER BY a DESC`},
		duckdbCase{name: "SubqueryRenamedOrderLimit", sql: `SELECT k
			FROM (SELECT r_regionkey AS k FROM region) t ORDER BY k DESC LIMIT 3`},
		// Shadow with misaligned orders: the alias n_name carries
		// n_regionkey values (0,1,1,1,4,...) while the REAL n_name sorts in
		// scan order — keying the sort on the real column instead of the
		// aliased source produces a different sequence, so this face cannot
		// pass by luck (r_comment/r_name would sort identically).
		duckdbCase{name: "SubqueryRenamedShadowOrder", sql: `SELECT n_name
			FROM (SELECT n_regionkey AS n_name FROM nation) t ORDER BY n_name`},
		duckdbCase{name: "SubqueryRenamedJoinOrder", sql: `SELECT n_name, k
			FROM nation JOIN (SELECT r_regionkey AS k FROM region) t
			ON n_regionkey = k ORDER BY k DESC, n_name`},
		// #387's family: an outer SELECT expression over a nested subquery
		// rename. attachScanSelectProjections wrote the specs against the
		// subquery's OUTPUT schema, so the scan fragment compiled `k + 1`
		// against a schema carrying only r_regionkey and hard-errored;
		// fixed by substituting the references through the rename
		// (substituteNestedRenameRefs). Faces: with and without the sort,
		// WHERE on the renamed column and on the computed alias, ORDER BY
		// the computed alias, the expression over a CHAINED rename, a
		// function call, and a CASE.
		duckdbCase{name: "SubqueryRenamedComputedMix", sql: `SELECT k, k + 1 AS m
			FROM (SELECT r_regionkey AS k FROM region) t ORDER BY k`},
		duckdbCase{name: "SubqueryRenamedComputedNoSort", sql: `SELECT k, k + 1 AS m
			FROM (SELECT r_regionkey AS k FROM region) t`},
		duckdbCase{name: "SubqueryRenamedComputedWhere", sql: `SELECT k, k + 1 AS m
			FROM (SELECT r_regionkey AS k FROM region) t WHERE k > 1 ORDER BY k`},
		duckdbCase{name: "SubqueryRenamedComputedWhereExpr", sql: `SELECT k, k + 1 AS m
			FROM (SELECT r_regionkey AS k FROM region) t WHERE k + 1 > 2 ORDER BY k`},
		duckdbCase{name: "SubqueryRenamedComputedOrderByAlias", sql: `SELECT k, k + 1 AS m
			FROM (SELECT r_regionkey AS k FROM region) t ORDER BY m DESC`},
		duckdbCase{name: "SubqueryRenamedComputedChained", sql: `SELECT a + 1 AS m
			FROM (SELECT b AS a FROM (SELECT r_regionkey AS b FROM region) u) t ORDER BY m DESC`},
		duckdbCase{name: "SubqueryRenamedComputedFunc", sql: `SELECT UPPER(nm) AS u
			FROM (SELECT n_name AS nm FROM nation) t ORDER BY u DESC`},
		duckdbCase{name: "SubqueryRenamedComputedCase", sql: `SELECT CASE WHEN k > 2 THEN k ELSE 0 END AS b
			FROM (SELECT r_regionkey AS k FROM region) t ORDER BY b DESC`},
		duckdbCase{name: "SubqueryRenamedComputedOrderByHidden", sql: `SELECT UPPER(nm) AS u
			FROM (SELECT n_name AS nm FROM nation) t ORDER BY nm DESC`},
		duckdbCase{name: "SubqueryRenamedComputedJoin", sql: `SELECT n_name, k + 1 AS m
			FROM nation JOIN (SELECT r_regionkey AS k FROM region) t
			ON n_regionkey = k ORDER BY m DESC, n_name`},
		duckdbCase{name: "SubqueryRenamedComputedConcatShadow", sql: `SELECT n_name || '!' AS x
			FROM (SELECT n_comment AS n_name FROM nation) t ORDER BY x`},
		duckdbCase{name: "SubqueryComputedWhereMixed", sql: `SELECT r_regionkey, rk2
			FROM (SELECT r_regionkey, NULLIF(r_regionkey, 2) AS rk2 FROM region) t
			WHERE rk2 >= r_regionkey ORDER BY r_regionkey`},
		duckdbCase{name: "SubqueryCaseComputedWhere", sql: `SELECT n_name, bucket
			FROM (SELECT n_name, CASE WHEN n_regionkey < 2 THEN NULL ELSE n_regionkey END AS bucket FROM nation) t
			WHERE bucket > 2 ORDER BY n_name`},

		// #584 — an UNQUALIFIED outer WHERE conjunct is attributed to the
		// SUBQUERY's relation and pushed onto the inner scan, when a
		// correlated EXISTS is decorrelated and the outer relation carries an
		// alias. The outer rows are never filtered by it and the membership
		// set is filtered by a predicate that was never meant for it.
		//
		// The entry IS the number that proves it. This query answers 6 on
		// PostgreSQL 17 and DuckDB (the orders under 1000, each of which
		// trivially has a matching clerk) and 104 on wadjet. 104 is not a
		// corruption: it is the EXACT answer to
		// `EXISTS (... AND sub.o_totalprice < 1000)`, the predicate written
		// deliberately onto the subquery — verified equal on both engines.
		// Dropping the predicate entirely would answer 15000, every order;
		// mis-attributing it answers 104; the correct answer is 6. The engine
		// answers 104, so it is mis-attributed, not dropped.
		duckdbCase{name: "OuterPredicateBesideExists",
			sql: `SELECT COUNT(*) AS n FROM orders t0
				WHERE o_totalprice < 1000
				  AND EXISTS (SELECT 1 FROM orders sub WHERE sub.o_clerk = t0.o_clerk)`,
			knownBugArm: armBoth,
			knownBug: "an unqualified outer conjunct beside a decorrelated EXISTS is pushed onto the " +
				"subquery's scan: 104 rows where PostgreSQL 17 and DuckDB both say 6 (#584)"},
		// The control, and the whole localization: the SAME query with the
		// conjunct QUALIFIED answers 6 on both arms. Fully gated — if this one
		// ever starts diverging, the defect has grown past #584.
		duckdbCase{name: "OuterPredicateBesideExistsQualified",
			sql: `SELECT COUNT(*) AS n FROM orders t0
				WHERE t0.o_totalprice < 1000
				  AND EXISTS (SELECT 1 FROM orders sub WHERE sub.o_clerk = t0.o_clerk)`},
		// #584's second control — no alias on the outer relation, unqualified
		// conjunct, attribution correct again — is deliberately NOT an entry
		// here. Correlating by TABLE NAME rather than by alias is not
		// recognized as a correlation at all, so the EXISTS survives as a
		// per-row predicate and the stage DAG refuses it outright ("EXISTS
		// subquery requires a SubqueryRunner", the #535 family). Pinning arm
		// B for that would make this entry about a different defect than the
		// one above it. The control's numbers are in #584.
	)

	// --- BYTES / BLOB (#570) --------------------------------------------
	//
	// `bytea`/`BLOB` appeared nowhere in benchmarks/ before this: the TPC-H
	// fixture has no BYTES column, so this arm had never compared one
	// either. It runs over the same bytea_probe rows the PostgreSQL arm
	// loads.
	//
	// The BLOB VALUE is never projected. DuckDB's CSV output escapes a
	// non-printable byte as \xNN, so a raw-bytes comparison would be
	// comparing wadjet's bytes against DuckDB's DISPLAY of them and would
	// report a divergence about neither engine — the same reason the
	// DECIMAL entries project d_key. What is compared instead is the KEY a
	// predicate selects, the octet count, and the hex TEXT, which is where
	// a wrong byte actually shows.
	//
	// duckdbSQL carries the dialect differences (ADR-0012 rule 3: configure
	// the oracle, never exempt the entry): DuckDB needs `::BLOB` on a
	// literal that meets a BLOB column, and spells `bytea::text` as
	// '\x' || lower(hex(...)).
	const duckBlobHex = `CASE WHEN b_val IS NULL THEN NULL ELSE '\x' || lower(hex(b_val)) END`
	out = append(out,
		duckdbCase{name: "BlobEqLiteral",
			sql:       `SELECT b_key FROM bytea_probe WHERE b_val = 'hi' ORDER BY b_key`,
			duckdbSQL: `SELECT b_key FROM bytea_probe WHERE b_val = 'hi'::BLOB ORDER BY b_key`},
		duckdbCase{name: "BlobEqEmpty",
			sql:       `SELECT b_key FROM bytea_probe WHERE b_val = '' ORDER BY b_key`,
			duckdbSQL: `SELECT b_key FROM bytea_probe WHERE b_val = ''::BLOB ORDER BY b_key`},
		duckdbCase{name: "BlobGtLiteral",
			sql:       `SELECT b_key FROM bytea_probe WHERE b_val > 'hi' ORDER BY b_key`,
			duckdbSQL: `SELECT b_key FROM bytea_probe WHERE b_val > 'hi'::BLOB ORDER BY b_key`},
		// A PREFIX against the longer value it starts: the one ordering a
		// length-first comparison gets wrong.
		duckdbCase{name: "BlobOrderByKeys",
			sql: `SELECT b_key FROM bytea_probe ORDER BY b_val, b_key`},
		duckdbCase{name: "BlobOrderByKeysDesc",
			sql: `SELECT b_key FROM bytea_probe ORDER BY b_val DESC, b_key`},
		duckdbCase{name: "BlobColColEq",
			sql: `SELECT b_key FROM bytea_probe WHERE b_val = b_other ORDER BY b_key`},
		duckdbCase{name: "BlobColColLt",
			sql: `SELECT b_key FROM bytea_probe WHERE b_val < b_other ORDER BY b_key`},
		duckdbCase{name: "BlobIsNull",
			sql: `SELECT b_key FROM bytea_probe WHERE b_val IS NULL ORDER BY b_key`},
		duckdbCase{name: "BlobOctetLength",
			sql: `SELECT b_key, OCTET_LENGTH(b_val) AS n FROM bytea_probe ORDER BY b_key`},
		duckdbCase{name: "BlobCountDistinct",
			sql: `SELECT COUNT(DISTINCT b_val) AS n FROM bytea_probe`},
		// The rendering #570 changed, against the second oracle.
		duckdbCase{name: "BlobCastText",
			sql:       `SELECT b_key, CAST(b_val AS text) AS s FROM bytea_probe ORDER BY b_key`,
			duckdbSQL: `SELECT b_key, ` + duckBlobHex + ` AS s FROM bytea_probe ORDER BY b_key`},
		duckdbCase{name: "BlobCastTextOrder",
			sql:       `SELECT CAST(b_val AS text) AS s FROM bytea_probe ORDER BY s, b_key`,
			duckdbSQL: `SELECT ` + duckBlobHex + ` AS s FROM bytea_probe ORDER BY s, b_key`},
		duckdbCase{name: "BlobMinMaxOverCast",
			sql:       `SELECT MIN(CAST(b_val AS text)) AS lo, MAX(CAST(b_val AS text)) AS hi FROM bytea_probe`,
			duckdbSQL: `SELECT MIN(` + duckBlobHex + `) AS lo, MAX(` + duckBlobHex + `) AS hi FROM bytea_probe`},
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
		dRows, dCols, err := runDuckDB(setup, c.duckSQL())
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
	// The BYTES fixture has no committed parquet: it is generated in Go and
	// spelled for DuckDB from the SAME rows both engines are loaded with
	// (#570), so the two cannot drift.
	sb.WriteString(pgBytesDuckDBSetup())
	return sb.String()
}

// fixtureSchemas is the schema for every table in data: AllTables, plus the
// probe tables that exist only for an oracle and are deliberately kept out
// of AllTables (which drives the harness, the seeders and both data tiers).
// A caller whose data holds nothing but AllTables gets exactly AllTables.
func fixtureSchemas(data map[string][]map[string]any) map[string]parquet.Schema {
	out := make(map[string]parquet.Schema, len(AllTables)+1)
	for name, schema := range AllTables {
		out[name] = schema
	}
	probes := oracleTables()
	for name := range data {
		if _, ok := out[name]; ok {
			continue
		}
		schema, ok := probes[name]
		if !ok {
			continue
		}
		out[name] = schema
	}
	return out
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
	for tableName, schema := range fixtureSchemas(data) {
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
	// PostgreSQL null placement, which is what wadjet implements:
	// NULLS LAST for ASC, NULLS FIRST for DESC. DuckDB defaults to
	// NULLS LAST in both directions, and that is a SEMANTIC difference
	// rather than a defect in either engine — SQL leaves the default
	// implementation-defined. Configuring the oracle keeps every row
	// compared; exempting the entries would blind the gate to real
	// ordering bugs in the same queries.
	setup = "SET default_null_order='nulls_last_on_asc_first_on_desc';\n" + setup
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

// reportArmError handles an arm that could not answer at all. Failing to
// answer is a divergence like any other — the strongest kind — so it fails the
// subtest unless the arm is pinned by knownBugArm, and checkArm still fails the
// pinned arm the day it starts answering correctly. Never silently tolerated:
// a pin that stopped being about an ERROR and became about a wrong ANSWER is
// still gated by checkArm on the next run.
func reportArmError(t *testing.T, arm string, c duckdbCase, err error) {
	t.Helper()
	if c.knownBugArm == arm || c.knownBugArm == armBoth {
		t.Logf("known divergence, NOT gated on arm %s: the arm cannot answer this query: %v\n  %s", arm, err, c.knownBug)
		return
	}
	t.Errorf("arm %s failed: %v\n  SQL: %s", arm, err, c.sql)
}
