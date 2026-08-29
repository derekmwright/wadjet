package tpch

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/optswitch"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/shapegen"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// This file is the generated-query differential: shapegen emits SQL over the
// TPC-H schema and each arm compares Wadjet against a reference. Three axes,
// in descending order of what they can catch:
//
//	1. Wadjet vs DuckDB          — the only arm that catches a defect BOTH
//	                               Wadjet paths share. Needs the DuckDB binary.
//	2. fast path vs stage DAG    — routing-dependent answers.
//	3. optimizations on vs off   — optimizer-introduced divergence.
//
// Every arm also runs oracle.CheckOrder, an ABSOLUTE check that the rows came
// back in the order the query asked for. That one needs no reference at all,
// so it catches an ORDER BY that every arm drops the same way.
//
// Bounded by default so CI pays little; WADJET_FUZZ_SEED_COUNT extends it for
// local hunting, WADJET_FUZZ_SEED_START moves the window, WADJET_FUZZ_TRACE=1
// names each query on stderr before it runs (the only way to attribute a
// process-killing runtime fatal error to a seed).
//
//	go test -run TestFuzzDuckDBDifferential ./benchmarks/tpch/          # CI arm
//	WADJET_FUZZ_SEED_COUNT=5000 go test -run TestFuzzDuckDBDifferential \
//	    -timeout 60m ./benchmarks/tpch/                                 # hunting
//
// STATE: the default seed windows are green — every OPEN defect a default
// window reaches is either kept out of the generator or matched by
// fuzzKnownDivergence and skipped (see KNOWN DIVERGENCES). The window arm
// (internal/oracle/shapegen, #569) re-weighted the shape roll so the default
// window now draws different queries, which surfaced the comma-join-mixed
// shape at seeds 24 and 51 (#593/#594) — now FIXED, so it is generated and
// gated live rather than skipped. Extending the window is what
// hunting means, and past the default it still reports OPEN defects — roughly
// one per 60 seeds on the DuckDB arm (alias-vs-column collisions,
// star-with-IN-subquery, cross joins, a group key misaligned from its values)
// and one per 30 on the two-path arm (OFFSET ignored on the DAG, ORDER BY
// dropped after DISTINCT, a broadcast probe panic). Those are engine bugs to
// fix, not harness noise: rerun the failing seed and read the reduced query
// it prints.
//
// KNOWN DIVERGENCES: constructs already proven to disagree are kept out of the
// generator, or recognised by fuzzKnownDivergence and skipped — a harness that
// keeps re-finding a filed defect drowns the new ones.

const (
	fuzzDuckDBBin = "/tmp/duckdb"
	// fuzzMaxRows caps how large a reference result the harness materializes.
	fuzzMaxRows = 200000
)

// errTooLarge marks a reference result past fuzzMaxRows: not a defect, just a
// query this harness declines to compare.
var errTooLarge = errors.New("reference result exceeds the harness row cap")

// Constructs this differential already proved divergent, kept OUT of the
// generator so a filed defect does not drown the new ones. Each has a minimal
// repro in the report; none are generated:
//
//	`a || b`                     → NULL (0 for two literals), not concatenation
//	NULLIF(<text col>, 'x')      → 0, not the text value
//	CAST(d AS DATE) - CAST(...)  → 0 (CAST-to-DATE is a no-op)
//	STDDEV/VARIANCE over >1 batch→ wrong by ~0.3-0.9%
//	<set-op> ... ORDER BY        → ORDER BY dropped; UNION across differently
//	                               named columns also loses one arm's values
//	ORDER BY x DESC NULLS LAST   → NULLs come back first
//
// DATE_PART('year', d) used to head this list, answering NULL because the name
// was registered nowhere and an unresolvable function evaluated to NULL rather
// than erroring. #341 made the missing name a plan-time error and registered
// date_part as EXTRACT's spelling, so it is gone from here on both counts: it
// answers correctly now, and a name that does not answer says so.
//
// The two below CANNOT be kept out of the generator without deleting the
// shapes they live in, so they are recognised structurally and skipped. Delete
// the matcher when the fix lands — that is the whole of "the fix landed".
//
// left-join-where-dropped (#335) and on-clause-column-conjunct-dropped (#336)
// used to live here and are gone: the first covered 63 of 400 generated
// queries, the second 8, and between them they were most of what this harness
// declined to look at. What replaced the second is bugOuterOnResidual, which
// is the part of #336 the planner cannot fix on its own — see below.
const (
	// An ON conjunct spanning both sides of an OUTER join is still dropped.
	// #336 fixed the inner-join half by lifting the residual into a filter
	// above the join, which is exact only because ON and WHERE mean the same
	// thing for an inner join. An outer join evaluates its ON BEFORE the
	// NULL-padding, so the same move would delete the unmatched rows the join
	// exists to preserve, and the executor has no residual predicate on an
	// outer join's probe to carry it instead (HashJoin applies SemiAntiFilter
	// on the semi/anti path only). Gated as a corpus entry —
	// OuterJoinOnResidual in duckdb_compare_test.go — so the pin cannot
	// outlive the bug.
	bugOuterOnResidual = "outer-join-on-residual-dropped"
	bugHiddenSortKey   = "hidden-sort-key-with-filter"
)

// fuzzKnownDivergence reports which filed defect a query is guaranteed to hit,
// or "" when the query is fair game.
func fuzzKnownDivergence(s *shapegen.Schema, q *shapegen.Query) string {
	// A conjunct comparing two COLUMNS in an OUTER join's ON clause is
	// dropped, so the join matches rows it should not and their partners
	// arrive populated where they owe NULLs:
	//   SELECT COUNT(*) FROM nation n LEFT JOIN region r
	//     ON n.n_regionkey = r.r_regionkey AND n.n_nationkey > r.r_regionkey
	//     WHERE r.r_name IS NULL
	//   → Wadjet 0, DuckDB 3.
	// The inner-join form of the same ON clause is fixed and generated.
	for _, f := range q.From {
		if !strings.Contains(f.On, " AND ") {
			continue
		}
		switch j := strings.ToUpper(f.Join); {
		case strings.Contains(j, "LEFT"), strings.Contains(j, "RIGHT"), strings.Contains(j, "FULL"):
			return bugOuterOnResidual
		}
	}
	// A WHERE filter combined with an ORDER BY over a column the SELECT list
	// does not project fails INTERMITTENTLY (1-3 runs in 8 on the same query):
	//   SELECT t0.l_shipmode AS c2 FROM lineitem t0 WHERE t0.l_returnflag < 'N'
	//     ORDER BY t0.l_comment, t0.l_orderkey, t0.l_linenumber LIMIT 29
	//   → executing query: operator execute: column "c2" does not exist in the
	//     input schema
	// Dropping either the WHERE or the hidden sort key makes it pass 8/8, so
	// both halves are needed. Intermittency makes this the shape most likely to
	// flake a suite, which is why it is matched rather than left to chance.
	if len(q.Where) > 0 {
		for _, o := range q.Order {
			if o.Key == "" {
				return bugHiddenSortKey
			}
		}
	}
	// #593/#594 — an explicit JOIN ... ON mixed with a comma-join whose
	// equi-predicate is in WHERE — USED to be matched and skipped here: #593's
	// instance materialized a 120M-row cross product and OOM-killed the
	// process, which no pin survives. The fix lifts the comma-join equality
	// into a hash join, so the shape is now generated and gated live (seeds
	// 11/81/84/94 are pinned in fuzzRegressionSeeds), and the CommaJoin corpus
	// in duckdb_compare_test.go covers it on both paths.

	// #615 — a cross-type numeric equi-join key (int column = float/decimal
	// column) in a 3+-relation FROM — USED to be matched and skipped here:
	// it panicked the executor's int-key probe, because the fast path was
	// enabled from the BUILD column's storage while the probe loop indexed
	// the PROBE column's nil typed slice. The key is now built at the pair's
	// resolved common type and the fast paths are gated on it, so the shape
	// is generated and compared like any other.
	return ""
}

func fuzzEnvInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// fuzzRegressionSeeds are seeds a defect was found at, kept in EVERY window
// regardless of its size or start. A corpus entry says "this query is right";
// a regression seed says "this generated SHAPE is never allowed to regress",
// which is the stronger statement when the shape — not the query — was the
// bug. Add one here whenever a seed finds a defect, in the same commit as the
// fix, with the issue in the comment.
//
// The comma-join seeds below are not decoration: genFrom emits a comma at a
// 12% per-step chance, so whether a 60-seed window contains the shape at all
// is luck, and whether it contains the shape MIXED with an explicit JOIN ...
// ON — the one #593/#594 turn on — is luckier still. Pinning them makes the
// default window cover it by construction.
var fuzzRegressionSeeds = []int64{
	11, // #594 — comma item FIRST, then JOIN ... ON, equality in WHERE
	81, // #593 — a four-JOIN chain, then a comma item with its equality in WHERE
	84, // #593 — TWO joined FROM items separated by a comma (JoinInfo.FromItem)
	94, // #594 — a comma item beside a LEFT JOIN
}

func fuzzSeeds(defaultCount int) []int64 {
	start := int64(fuzzEnvInt("WADJET_FUZZ_SEED_START", 1))
	count := fuzzEnvInt("WADJET_FUZZ_SEED_COUNT", defaultCount)
	out := make([]int64, 0, count+len(fuzzRegressionSeeds))
	in := make(map[int64]bool, count+len(fuzzRegressionSeeds))
	for i := 0; i < count; i++ {
		seed := start + int64(i)
		out, in[seed] = append(out, seed), true
	}
	for _, seed := range fuzzRegressionSeeds {
		if !in[seed] {
			out, in[seed] = append(out, seed), true
		}
	}
	return out
}

// TestFuzzWindowCoversCommaJoinMixture is the guard on the guard: it asserts
// the DEFAULT window actually generates a comma-joined FROM item mixed with an
// explicit JOIN ... ON. Without it, retuning shapegen's weights or adding a
// draw anywhere upstream renumbers every seed (one sequential stream per
// seed), and this coverage could vanish with every suite still green.
//
// It runs without DuckDB, so it gates in CI where the differential arm skips.
func TestFuzzWindowCoversCommaJoinMixture(t *testing.T) {
	schema := shapegen.TPCH()
	comma, mixed := 0, 0
	for _, seed := range fuzzSeeds(60) {
		q := shapegen.New(seed, schema).Query()
		commas, ons := 0, 0
		for _, f := range q.From {
			switch f.Join {
			case ",":
				commas++
			case "":
				// the leading FROM item carries no join keyword
			default:
				ons++
			}
		}
		if commas > 0 {
			comma++
		}
		if commas > 0 && ons > 0 {
			mixed++
		}
	}
	if comma == 0 {
		t.Errorf("the default fuzz window generates no comma-joined FROM list at all")
	}
	if mixed == 0 {
		t.Errorf("the default fuzz window generates no comma FROM item MIXED with an explicit JOIN ... ON — "+
			"the shape #593/#594 turn on. Pin a seed that does in fuzzRegressionSeeds (%v covered it when "+
			"this gate was written).", fuzzRegressionSeeds)
	}
}

// fuzzTrace names the query about to run, on stderr, unbuffered. Some defects
// take the whole process down — a runtime fatal error (concurrent map write,
// for one) is not recoverable and no t.Log survives it — so the seed has to be
// on the wire BEFORE the query runs or the crash is unattributable.
// WADJET_FUZZ_TRACE=1 turns it on.
var fuzzTracing = os.Getenv("WADJET_FUZZ_TRACE") == "1"

func fuzzTrace(seed int64, sql string) {
	if fuzzTracing {
		fmt.Fprintf(os.Stderr, "[fuzz seed %d] %s\n", seed, sql)
	}
}

// fuzzResult is one arm's answer, or the failure that replaced it.
type fuzzResult struct {
	res   *oracle.Result
	err   error
	panic string
}

func (r fuzzResult) failed() bool { return r.err != nil || r.panic != "" }

func (r fuzzResult) why() string {
	switch {
	case r.panic != "":
		return "PANIC: " + r.panic
	case r.err != nil:
		return r.err.Error()
	}
	return ""
}

// fuzzQueryWadjet runs sql on the embedded engine, converting a panic into a
// reportable failure instead of taking the test process down with it.
func fuzzQueryWadjet(ctx context.Context, db *wadjet.DB, sql string) (out fuzzResult) {
	defer func() {
		if r := recover(); r != nil {
			out = fuzzResult{panic: fmt.Sprint(r)}
		}
	}()
	res, err := db.Query(ctx, sql)
	if err != nil {
		return fuzzResult{err: err}
	}
	return fuzzResult{res: &oracle.Result{Columns: res.Columns, Rows: res.Rows}}
}

// fuzzDuckDB runs sql through the DuckDB CLI over the same parquet fixture and
// returns rows as column→string maps.
//
// Deliberately a private copy of the CSV plumbing rather than a call into
// duckdb_compare_test.go: that file is the cross-engine CHECKSUM gate with its
// own baseline, and the two should be free to evolve independently.
// The CSV is read incrementally and abandoned past fuzzMaxRows. Shrinking a
// cartesian-join failure legitimately reduces to an unconstrained cross join,
// and materializing DuckDB's 17.8M-row answer to one took the whole test
// process down with the OOM killer — a result that size is not a repro anyone
// wants reported anyway.

// fuzzOracleTimeout bounds ONE oracle query. A generated query can ask for an
// astronomically large cartesian product even over the SF0.01 fixture, and the
// fuzzer treats an oracle error as "skip" — so a bounded oracle loses only
// queries nobody could answer, and keeps the sweep moving instead of wedging
// it at one seed.
const fuzzOracleTimeout = 90 * time.Second

func fuzzDuckDB(setup, sql string) ([]map[string]string, []string, error) {
	// PostgreSQL null placement, which is what wadjet implements:
	// NULLS LAST for ASC, NULLS FIRST for DESC. DuckDB defaults to
	// NULLS LAST in both directions, and that is a SEMANTIC difference
	// rather than a defect in either engine — SQL leaves the default
	// implementation-defined. Configuring the oracle keeps every row
	// compared; exempting the entries would blind the gate to real
	// ordering bugs in the same queries.
	// Bound the oracle. A generated query can ask for a cartesian product
	// that is astronomically large even over the SF0.01 fixture: on
	// 2026-08-19 one reached 30 GB RSS and DuckDB invoked the OOM killer,
	// which killed DuckDB and capped two 1500-seed sweeps at the same seed.
	// A differential harness must not be able to take the machine down —
	// an oracle that ERRORS on an unreasonable query is still a usable
	// oracle, and the fuzzer already treats an oracle error as "skip".
	setup = "SET memory_limit='4GB';\nSET default_null_order='nulls_last_on_asc_first_on_desc';\n" + setup
	script := setup + "\n.mode csv\n.headers on\n.nullvalue <NULL>\n" + sql + ";\n"
	// Bound the oracle in WALL TIME, not just memory. `SET memory_limit`
	// governs DuckDB's buffer manager and did NOT stop a generated
	// cartesian product reaching 28.9 GB RSS: on 2026-08-19 that first
	// OOM-killed the machine, and with the memory limit in place it simply
	// ran for 36+ minutes instead, wedging the sweep at the same seed both
	// times. A killed child also has to release the parent — cmd.Output()
	// waits on the stdout pipe, so killing the process by hand did not
	// unblock the harness. CommandContext closes both paths.
	ctx, cancel := context.WithTimeout(context.Background(), fuzzOracleTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, fuzzDuckDBBin)
	cmd.WaitDelay = 5 * time.Second
	cmd.Stdin = strings.NewReader(script)
	// DuckDB spills to a .tmp directory under its working directory; without
	// this it litters the repo. The view definitions use absolute paths, so
	// the working directory is otherwise irrelevant.
	cmd.Dir = os.TempDir()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, nil, err
	}
	defer func() {
		// Drain and reap: the reader stops early on an over-cap result, and a
		// child left blocked on a full pipe would wedge the run.
		io.Copy(io.Discard, stdout)
		cmd.Wait()
	}()

	r := csv.NewReader(stdout)
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	var cols []string
	var rows []map[string]string
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, fmt.Errorf("parsing duckdb csv: %w", err)
		}
		if cols == nil {
			cols = rec
			continue
		}
		if len(rows) >= fuzzMaxRows {
			return nil, nil, errTooLarge
		}
		row := make(map[string]string, len(cols))
		for i, col := range cols {
			if i < len(rec) {
				row[col] = rec[i]
			}
		}
		rows = append(rows, row)
	}
	if cols == nil {
		// No header at all: DuckDB rejected the statement.
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = "no output"
		}
		return nil, nil, fmt.Errorf("duckdb: %s", msg)
	}
	return rows, cols, nil
}

// fuzzTypeDuckDB converts DuckDB's all-string rows into the Go types the
// Wadjet result uses for the same column, so canonical rendering compares like
// with like rather than "5" against 5.
func fuzzTypeDuckDB(dRows []map[string]string, dCols []string, wadjetRes *oracle.Result) *oracle.Result {
	ref := make(map[string]any, len(dCols))
	if wadjetRes != nil {
		for _, row := range wadjetRes.Rows {
			for col, v := range row {
				if v != nil && ref[col] == nil {
					ref[col] = v
				}
			}
		}
	}
	rows := make([]map[string]any, len(dRows))
	for i, dr := range dRows {
		row := make(map[string]any, len(dr))
		for col, s := range dr {
			row[col] = oracle.ParseCell(s, ref[col])
		}
		rows[i] = row
	}
	return &oracle.Result{Columns: dCols, Rows: rows}
}

// fuzzRenderRows renders up to n rows for a failure report.
func fuzzRenderRows(res *oracle.Result, n int) string {
	if res == nil {
		return "<no result>"
	}
	if len(res.Rows) == 0 {
		return "<0 rows>"
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "%d rows, columns %v\n", len(res.Rows), res.Columns)
	for i, row := range res.Rows {
		if i >= n {
			fmt.Fprintf(&sb, "      ... %d more\n", len(res.Rows)-n)
			break
		}
		sb.WriteString("      ")
		for j, c := range res.Columns {
			if j > 0 {
				sb.WriteString(" | ")
			}
			fmt.Fprintf(&sb, "%s=%v", c, row[c])
		}
		sb.WriteString("\n")
	}
	return strings.TrimRight(sb.String(), "\n")
}

// fuzzSetupEmbedded opens a Wadjet DB over the committed DuckDB-written
// parquet fixture — the same bytes DuckDB reads, so the two engines answer
// over identical data.
func fuzzSetupEmbedded(tb testing.TB) *wadjet.DB {
	tb.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "tpch"})
	if err != nil {
		tb.Fatalf("open: %v", err)
	}
	tb.Cleanup(func() { db.Close() })

	names := make([]string, 0, len(AllTables))
	for name := range AllTables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		schema := AllTables[name]
		if err := db.CreateTable(ctx, name, schema, nil); err != nil {
			tb.Fatalf("create table %s: %v", name, err)
		}
		rows, err := fuzzLoadParquet(filepath.Join(".", duckdbDataDir, name+".parquet"), schema)
		if err != nil {
			tb.Fatalf("load %s: %v", name, err)
		}
		// Several row groups per table so a per-batch defect cannot hide
		// behind a single-group scan.
		ing := db.NewIngester(name, schema, nil, ingest.Config{
			MaxBufferRows: len(rows) + 1,
			RowGroupSize:  max(100, len(rows)/4),
		})
		if err := ing.Ingest(ctx, rows); err != nil {
			tb.Fatalf("ingest %s: %v", name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			tb.Fatalf("flush %s: %v", name, err)
		}
	}
	return db
}

func fuzzLoadParquet(path string, schema parquet.Schema) ([]map[string]any, error) {
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

func fuzzDuckDBViews(tb testing.TB) string {
	tb.Helper()
	absDir, err := filepath.Abs(filepath.Join(".", duckdbDataDir))
	if err != nil {
		tb.Fatalf("abs %s: %v", duckdbDataDir, err)
	}
	var sb strings.Builder
	for _, name := range []string{"region", "nation", "supplier", "part", "partsupp", "customer", "orders", "lineitem"} {
		fmt.Fprintf(&sb, "CREATE VIEW %s AS SELECT * FROM read_parquet('%s/%s.parquet');\n", name, absDir, name)
	}
	return sb.String()
}

// TestFuzzDuckDBDifferential is axis 1: generated SQL run against Wadjet and
// against DuckDB over the identical parquet fixture. This is the only arm that
// catches a defect both Wadjet execution paths share.
func TestFuzzDuckDBDifferential(t *testing.T) {
	if _, err := os.Stat(fuzzDuckDBBin); err != nil {
		t.Skipf("no DuckDB binary at %s — this arm needs ground truth; "+
			"install with: wget -q https://github.com/duckdb/duckdb/releases/download/v1.1.3/duckdb_cli-linux-amd64.zip -O /tmp/duckdb.zip && unzip -d /tmp /tmp/duckdb.zip",
			fuzzDuckDBBin)
	}
	ctx := context.Background()
	db := fuzzSetupEmbedded(t)
	setup := fuzzDuckDBViews(t)
	schema := shapegen.TPCH()
	seeds := fuzzSeeds(60)

	// evaluate runs one query on both engines and returns why they disagree
	// ("" when they agree). valid is false when DuckDB itself rejects the
	// query, which during shrinking means the candidate is malformed rather
	// than a smaller repro.
	evaluate := func(q *shapegen.Query) (reason string, valid bool) {
		sql := q.SQL()
		dRows, dCols, dErr := fuzzDuckDB(setup, sql)
		if dErr != nil {
			return "", false
		}
		w := fuzzQueryWadjet(ctx, db, sql)
		if w.failed() {
			return fmt.Sprintf("Wadjet failed on a query DuckDB answered (%d rows): %s",
				len(dRows), w.why()), true
		}
		want := fuzzTypeDuckDB(dRows, dCols, w.res)
		spec := q.CompareSpec()
		if diff := oracle.Compare(want, w.res, spec); diff != "" {
			return diff, true
		}
		// Absolute check: DuckDB first, so a checker bug shows up as a DuckDB
		// "failure" rather than being blamed on Wadjet.
		keys := q.OrderKeys()
		if bad := oracle.CheckOrder(want, keys); bad != "" {
			return "", false // harness could not decide the ordering; not a defect claim
		}
		if bad := oracle.CheckOrder(w.res, keys); bad != "" {
			return "Wadjet result is not ordered as the query asked:\n" + bad, true
		}
		return "", true
	}

	var stats fuzzStats
	for _, seed := range seeds {
		q := shapegen.New(seed, schema).Query()
		q.Seed = seed
		if bug := fuzzKnownDivergence(schema, q); bug != "" {
			stats.skip(bug)
			continue
		}
		fuzzTrace(seed, q.SQL())
		reason, valid := evaluate(q)
		stats.observe(t, ctx, db, setup, q, reason, valid)
		if reason == "" || !valid {
			continue
		}
		min := shapegen.Shrink(schema, q, func(c *shapegen.Query) bool {
			if fuzzKnownDivergence(schema, c) != "" {
				return false
			}
			r, ok := evaluate(c)
			return ok && r != ""
		})
		minReason, _ := evaluate(min)
		if minReason == "" {
			minReason = reason
			min = q
		}
		sql := min.SQL()
		dRows, dCols, _ := fuzzDuckDB(setup, sql)
		w := fuzzQueryWadjet(ctx, db, sql)
		t.Errorf("DIVERGENCE seed %d (shape %s, compare %s)\n  SQL: %s\n  %s\n  Wadjet: %s\n  DuckDB: %s",
			seed, q.Shape, min.CompareSpec().Mode, sql, minReason,
			fuzzRenderRows(w.res, 5), fuzzRenderRows(fuzzTypeDuckDB(dRows, dCols, w.res), 5))
	}
	stats.report(t, "Wadjet vs DuckDB", len(seeds))
}

// TestFuzzTwoPathDifferential is axis 2: the same generated SQL answered by
// the local fast path and by the stage DAG, over one cluster.
func TestFuzzTwoPathDifferential(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	fast, dag := setupTwoPathCluster(t, ctx)
	schema := shapegen.TPCH()
	seeds := fuzzSeeds(12)

	evaluate := func(q *shapegen.Query) (reason string, valid bool) {
		sql := q.SQL()
		aRows, aCols, aErr := runArm(t, ctx, fast, sql)
		bRows, bCols, bErr := runArm(t, ctx, dag, sql)
		switch {
		case aErr != nil && bErr != nil:
			return "", false // the query is not supported at all; not a divergence
		case aErr != nil:
			return fmt.Sprintf("fast path failed while the stage DAG returned %d rows: %v", len(bRows), aErr), true
		case bErr != nil:
			return fmt.Sprintf("stage DAG failed while the fast path returned %d rows: %v", len(bRows), bErr), true
		}
		a := &oracle.Result{Columns: aCols, Rows: aRows}
		b := &oracle.Result{Columns: bCols, Rows: bRows}
		if diff := oracle.Compare(a, b, q.CompareSpec()); diff != "" {
			return diff, true
		}
		keys := q.OrderKeys()
		if bad := oracle.CheckOrder(a, keys); bad != "" {
			return "fast-path result is not ordered as the query asked:\n" + bad, true
		}
		if bad := oracle.CheckOrder(b, keys); bad != "" {
			return "stage-DAG result is not ordered as the query asked:\n" + bad, true
		}
		return "", true
	}

	var stats fuzzStats
	for _, seed := range seeds {
		q := shapegen.New(seed, schema).Query()
		if bug := fuzzKnownDivergence(schema, q); bug != "" {
			stats.skip(bug)
			continue
		}
		fuzzTrace(seed, q.SQL())
		reason, valid := evaluate(q)
		stats.observeSimple(q, reason, valid)
		if reason == "" || !valid {
			continue
		}
		min := shapegen.Shrink(schema, q, func(c *shapegen.Query) bool {
			if fuzzKnownDivergence(schema, c) != "" {
				return false
			}
			r, ok := evaluate(c)
			return ok && r != ""
		})
		minReason, _ := evaluate(min)
		if minReason == "" {
			minReason, min = reason, q
		}
		sql := min.SQL()
		aRows, aCols, _ := runArm(t, ctx, fast, sql)
		bRows, bCols, _ := runArm(t, ctx, dag, sql)
		t.Errorf("DIVERGENCE seed %d (shape %s, compare %s)\n  SQL: %s\n  %s\n  fast path: %s\n  stage DAG: %s",
			seed, q.Shape, min.CompareSpec().Mode, sql, minReason,
			fuzzRenderRows(&oracle.Result{Columns: aCols, Rows: aRows}, 5),
			fuzzRenderRows(&oracle.Result{Columns: bCols, Rows: bRows}, 5))
	}
	stats.report(t, "fast path vs stage DAG", len(seeds))
}

// TestFuzzOptimizationInvariance is axis 3: every generated query run with all
// optimizations on, then once per kill switch with that one off. Same contract
// as TestTPCHOptimizationInvariance, over generated shapes instead of the fixed
// corpus — and with this file's ordering semantics, so a toggle that drops an
// ORDER BY is caught rather than sorted away.
func TestFuzzOptimizationInvariance(t *testing.T) {
	ctx := context.Background()
	db := fuzzSetupEmbedded(t)
	schema := shapegen.TPCH()
	seeds := fuzzSeeds(15)

	toggles := optswitch.All()
	if len(toggles) == 0 {
		t.Fatal("no toggles registered — optswitch packages not linked?")
	}
	prev := make(map[string]bool, len(toggles))
	for _, tg := range toggles {
		prev[tg.Name] = tg.Set(true)
	}
	t.Cleanup(func() {
		for _, tg := range toggles {
			tg.Set(prev[tg.Name])
		}
	})

	type entry struct {
		q    *shapegen.Query
		seed int64
		base *oracle.Result
	}
	var corpus []entry
	for _, seed := range seeds {
		q := shapegen.New(seed, schema).Query()
		fuzzTrace(seed, q.SQL())
		w := fuzzQueryWadjet(ctx, db, q.SQL())
		if w.failed() {
			continue // unsupported shapes are not this arm's business
		}
		corpus = append(corpus, entry{q: q, seed: seed, base: w.res})
	}
	if len(corpus) == 0 {
		t.Fatal("no generated query produced a baseline result")
	}
	t.Logf("optimization invariance: %d generated queries x %d configurations",
		len(corpus), len(toggles)+1)

	run := func(label string, off []*optswitch.Toggle) {
		t.Run(label, func(t *testing.T) {
			for _, tg := range off {
				tg.Set(false)
			}
			defer func() {
				for _, tg := range off {
					tg.Set(true)
				}
			}()
			for _, e := range corpus {
				sql := e.q.SQL()
				w := fuzzQueryWadjet(ctx, db, sql)
				if w.failed() {
					t.Errorf("seed %d failed with %s disabled: %s\n  SQL: %s", e.seed, label, w.why(), sql)
					continue
				}
				if diff := oracle.Compare(e.base, w.res, e.q.CompareSpec()); diff != "" {
					t.Errorf("seed %d (shape %s) diverges with %s disabled:\n  SQL: %s\n  %s\n  baseline: %s\n  got: %s",
						e.seed, e.q.Shape, label, sql, diff,
						fuzzRenderRows(e.base, 5), fuzzRenderRows(w.res, 5))
				}
			}
		})
	}
	for _, tg := range toggles {
		run(tg.Name, []*optswitch.Toggle{tg})
	}
	run("all-off", toggles)
}

// fuzzStats tracks what the run actually exercised. A differential over
// queries that all return zero rows proves nothing, so the shape and
// non-empty counts are part of the result.
type fuzzStats struct {
	shapes   map[string]int
	known    map[string]int
	nonEmpty int
	empty    int
	rejected int
	diverged int
}

// skip records a query the generator produced that is guaranteed to hit an
// already-filed defect. Counting them keeps the suppression visible: a run
// whose queries are mostly skipped is not the wide sweep it looks like.
func (s *fuzzStats) skip(bug string) {
	if s.known == nil {
		s.known = map[string]int{}
	}
	s.known[bug]++
}

func (s *fuzzStats) observeSimple(q *shapegen.Query, reason string, valid bool) {
	if s.shapes == nil {
		s.shapes = map[string]int{}
	}
	s.shapes[q.Shape]++
	if !valid {
		s.rejected++
		return
	}
	if reason != "" {
		s.diverged++
	}
}

func (s *fuzzStats) observe(t *testing.T, ctx context.Context, db *wadjet.DB, setup string, q *shapegen.Query, reason string, valid bool) {
	s.observeSimple(q, reason, valid)
	if !valid {
		return
	}
	w := fuzzQueryWadjet(ctx, db, q.SQL())
	if w.res != nil && len(w.res.Rows) > 0 {
		s.nonEmpty++
	} else {
		s.empty++
	}
}

func (s *fuzzStats) report(t *testing.T, arm string, seeds int) {
	t.Helper()
	names := make([]string, 0, len(s.shapes))
	for k := range s.shapes {
		names = append(names, k)
	}
	sort.Strings(names)
	var parts []string
	for _, n := range names {
		parts = append(parts, fmt.Sprintf("%s=%d", n, s.shapes[n]))
	}
	kn := make([]string, 0, len(s.known))
	for k := range s.known {
		kn = append(kn, k)
	}
	sort.Strings(kn)
	var kparts []string
	total := 0
	for _, k := range kn {
		kparts = append(kparts, fmt.Sprintf("%s=%d", k, s.known[k]))
		total += s.known[k]
	}
	known := "none"
	if total > 0 {
		known = strings.Join(kparts, " ")
	}
	rows := ""
	if s.nonEmpty+s.empty > 0 {
		// Only the arms that can cheaply re-run a query track this. A run
		// whose queries nearly all return zero rows compares nothing, so the
		// number is part of the result, not decoration.
		rows = fmt.Sprintf(", %d returned rows, %d empty", s.nonEmpty, s.empty)
	}
	t.Logf("%s: %d seeds, %d diverged, %d skipped on filed defects, %d unsupported/rejected%s\n  shapes: %s\n  skipped: %s",
		arm, seeds, s.diverged, total, s.rejected, rows, strings.Join(parts, " "), known)
}
