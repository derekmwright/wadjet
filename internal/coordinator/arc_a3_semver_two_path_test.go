package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/semvergen"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// THE SEMVER FAMILY ON EVERY ARM (#967).
//
// A scalar function over a text column looks like the last thing that needs
// five arms. `semver_sort_key` is not: its whole reason to exist is that a
// distributed ORDER BY, a MIN/MAX and a spilled external merge order versions
// by ordering its BYTES, and those three run in a different process, over a
// plan the coordinator built and a fragment a worker rebuilt. The question
// these cells answer is whether the key's order really is the specification's
// order once 5000 versions are split across four files, hash-partitioned,
// spilled to disk and merged back — and whether an unrecognized range literal
// reaches the client from inside a worker rather than becoming zero rows.
//
// THE HEADLINE CELL NEEDS NO EXTERNAL ORACLE AND CANNOT PASS VACUOUSLY. The
// arm returns the whole corpus `ORDER BY semver_sort_key(v)` and the test
// walks it with `semver_cmp` — the registry's own function, read through
// `expr.DefaultRegistry`, not a second comparison written here — asserting
// every adjacent pair is non-decreasing. Its CONTROL is the same query ordered
// by the raw TEXT, whose inversion count is a measured, nonzero number: if
// that ever reached zero the corpus would have stopped containing the pairs
// this family exists for and the first cell would prove nothing.
//
// The VALUE cells' expectations are PostgreSQL 17.11's, measured on the shared
// oracle server over the five pure-core versions of the edge fixture
// (1.0.0, 1.2.3, 1.2.10, 1.10.0, 2.0.0) with the pure-SQL spelling ADR-0012
// records as the value oracle:
//
//	string_agg(v,',' ORDER BY string_to_array(v,'.')::int[])
//	                                     1.0.0,1.2.3,1.2.10,1.10.0,2.0.0
//	string_agg(v,',' ORDER BY v COLLATE "C")
//	                                     1.0.0,1.10.0,1.2.10,1.2.3,2.0.0  <- the trap
//	sum((string_to_array(v,'.')::int[])[1])                   6  (bigint)
//	sum(...[2])                                              14
//	sum(...[3])                                              13
//	count(*) where core >= ARRAY[1,0,0] and core < ARRAY[2,0,0]      4
//	sum(three-way compare against ARRAY[1,0,0])                      4
func TestTheSemverFamilyOrdersByTheSpecificationOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	spilled := na2Standalone(t, ctx, 512*1024)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	// The pressured arm runs with the DRAIN FORCED and the run floor lowered,
	// and the reference arms disarmed — ADR-0027 §6's protocol. Arming both
	// sides would cancel a defect that lives in the drain (#790).
	budgeted := func(name, sql string) (*oracle.Result, error) {
		beforeDrain := exec.ForcedAggDrains.Load()
		beforeRaw := exec.RawRowSpillFiles.Load()
		beforeSort := exec.SortRunsWritten.Load()
		beforeWin := exec.WindowRunsWritten.Load()
		restoreDrain := exec.ForceAggDrainEvery(1)
		restoreRuns := exec.ForceSmallSpillRuns(512)
		res, err := tmdRunSingle(ctx, spilled, sql)
		restoreRuns()
		exec.ForceAggDrainEvery(restoreDrain)
		engaged := exec.ForcedAggDrains.Load() > beforeDrain ||
			exec.RawRowSpillFiles.Load() > beforeRaw ||
			exec.SortRunsWritten.Load() > beforeSort ||
			exec.WindowRunsWritten.Load() > beforeWin
		a3sEngaged.Store(name, engaged)
		return res, err
	}

	arms := []struct {
		name string
		// coord is the coordinator whose LOCAL-ROUTING counters this arm's
		// dispositions are read from, and nil on the single-process arms.
		// Rows alone cannot tell "executed on the DAG" from "refused and
		// routed local".
		coord *Coordinator
		run   func(name, sql string) (*oracle.Result, error)
	}{
		{"single", nil, func(_, sql string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, sql) }},
		{"single+budget+forced-drain", nil, budgeted},
		{"dag", coord, func(_, sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, sql) }},
		{"dag-shuffled", coordB, func(_, sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordB, sql) }},
		{"dag-morsel4", coordM, func(_, sql string) (*oracle.Result, error) { return tmdRunDAG(ctx, coordM, sql) }},
	}

	// ---- THE HEADLINE, and its control.
	t.Run("the_sort_keys_order_is_the_specifications_order", func(t *testing.T) {
		const bySemver = `SELECT v FROM semverpkg WHERE semver_valid(v) ORDER BY semver_sort_key(v)`
		const byText = `SELECT v FROM semverpkg WHERE semver_valid(v) ORDER BY v`
		for _, arm := range arms {
			before := a2fReadRoutes(arm.coord)
			res, err := arm.run("order/"+arm.name, bySemver)
			if err != nil {
				t.Fatalf("%s arm: %v", arm.name, err)
			}
			a2fCheckRoutes(t, arm.name, arm.coord, before, bySemver)
			got := a3sColumn(t, res, "v")
			if len(got) != svValidRows {
				t.Fatalf("%s arm returned %d versions, want %d", arm.name, len(got), svValidRows)
			}
			if bad := a3sInversions(t, got); bad != 0 {
				t.Errorf("%s arm: ORDER BY semver_sort_key(v) left %d adjacent pairs out of "+
					"specification order", arm.name, bad)
			}

			before = a2fReadRoutes(arm.coord)
			res, err = arm.run("text/"+arm.name, byText)
			if err != nil {
				t.Fatalf("%s arm: %v", arm.name, err)
			}
			a2fCheckRoutes(t, arm.name, arm.coord, before, byText)
			text := a3sColumn(t, res, "v")
			if n := a3sInversions(t, text); n != svTextInversions {
				t.Errorf("%s arm: a TEXT sort of the same corpus left %d pairs out of "+
					"specification order, want %d — if this reaches 0 the corpus no longer "+
					"holds the pairs this family exists for and the cell above is vacuous",
					arm.name, n, svTextInversions)
			}
		}
	})

	// ---- the value cells.
	for _, tc := range []struct {
		name, sql string
		ordered   bool
		want      []string
	}{
		// The pure-SQL value oracle, over the five pure-core versions.
		{name: "the_core_order_is_postgres_array_order", ordered: true,
			sql: `SELECT semver_normalize(v) AS nv FROM semverpkg
			       WHERE v IN ('1.0.0','1.2.3','1.2.10','1.10.0','2.0.0+build.7')
			       GROUP BY 1 ORDER BY MIN(semver_sort_key(v))`,
			want: []string{"nv=1.0.0", "nv=1.2.3", "nv=1.2.10", "nv=1.10.0", "nv=2.0.0+build.7"}},
		{name: "sum_major_over_the_pure_core_rows",
			sql: `SELECT SUM(semver_major(v)) AS s FROM semverpkg
			       WHERE v IN ('1.0.0','1.2.3','1.2.10','1.10.0','2.0.0+build.7')`,
			want: []string{"s=" + a3sSum(6)}},
		{name: "sum_minor_over_the_pure_core_rows",
			sql: `SELECT SUM(semver_minor(v)) AS s FROM semverpkg
			       WHERE v IN ('1.0.0','1.2.3','1.2.10','1.10.0','2.0.0+build.7')`,
			want: []string{"s=" + a3sSum(14)}},
		{name: "sum_patch_over_the_pure_core_rows",
			sql: `SELECT SUM(semver_patch(v)) AS s FROM semverpkg
			       WHERE v IN ('1.0.0','1.2.3','1.2.10','1.10.0','2.0.0+build.7')`,
			want: []string{"s=" + a3sSum(13)}},
		{name: "the_range_over_the_pure_core_rows",
			sql: `SELECT COUNT(*) AS n FROM semverpkg
			       WHERE v IN ('1.0.0','1.2.3','1.2.10','1.10.0','2.0.0+build.7')
			         AND semver_satisfies(v, '>=1.0.0 <2.0.0')`,
			want: []string{"n=int64:4"}},
		{name: "sum_cmp_against_one_zero_zero",
			sql: `SELECT SUM(semver_cmp(v,'1.0.0')) AS s FROM semverpkg
			       WHERE v IN ('1.0.0','1.2.3','1.2.10','1.10.0','2.0.0+build.7')`,
			want: []string{"s=int64:4"}},

		// The specification's own §11.4 chain, end to end.
		{name: "the_specification_chain_in_order", ordered: true,
			sql: `SELECT v FROM semverpkg
			       WHERE v IN ('1.0.0-alpha','1.0.0-alpha.1','1.0.0-alpha.beta','1.0.0-beta',
			                   '1.0.0-beta.2','1.0.0-beta.11','1.0.0-rc.1','1.0.0')
			       GROUP BY v ORDER BY MIN(semver_sort_key(v))`,
			want: []string{
				"v=1.0.0-alpha", "v=1.0.0-alpha.1", "v=1.0.0-alpha.beta",
				"v=1.0.0-beta", "v=1.0.0-beta.2", "v=1.0.0-beta.11",
				"v=1.0.0-rc.1", "v=1.0.0",
			}},
		// The pair a '.'-joined key inverts: `alpha.1` is below `alpha-x`.
		{name: "the_identifier_separator_pair",
			sql:  `SELECT semver_cmp('1.0.0-alpha.1','1.0.0-alpha-x') AS c FROM semverpkg WHERE id = 1`,
			want: []string{"c=int32:-1"}},

		// The lenient half: junk and NULL are NULL, not an error.
		{name: "junk_is_null_not_an_error",
			sql: `SELECT COUNT(*) AS valid, COUNT(semver_sort_key(v)) AS keyed FROM semverpkg
			       WHERE v IN ('01.2.3','not-a-version','1.2.3')`,
			want: []string{"valid=int64:3|keyed=int64:1"}},
		{name: "a_null_version_is_null",
			sql:  `SELECT COUNT(*) AS n FROM semverpkg WHERE semver_valid(v) IS NULL`,
			want: []string{"n=int64:1"}},
		{name: "the_v_prefix_normalizes_onto_the_same_version",
			sql:  `SELECT COUNT(*) AS n FROM semverpkg WHERE semver_normalize(v) = '1.2.3'`,
			want: []string{"n=int64:2"}},
		{name: "build_metadata_has_no_precedence",
			sql:  `SELECT semver_cmp('1.0.0+a','1.0.0+b') AS c FROM semverpkg WHERE id = 1`,
			want: []string{"c=int32:0"}},

		// A range from a COLUMN, which no literal fold can see.
		{name: "a_range_from_a_column",
			sql: `SELECT COUNT(*) AS n FROM semverpkg
			       WHERE id <> 4 AND semver_satisfies(v, rng)`,
			want: []string{"n=int64:" + fmt.Sprint(svColumnRangeMatches)}},

		// THE BOUND AT THE TOP OF THE DOMAIN (#967, round-1 review B1). A
		// component is accepted up to int64's maximum and the generated corpus
		// draws components from that value, so a desugaring that closes a band
		// by raising a component by one is handed one with nowhere to go on
		// roughly one row in ten. Wrapped, the upper half answers FALSE for
		// every row and the lower half answers TRUE for every row. The
		// expectations are computed from the fixture's STRINGS, not from the
		// range machinery these cells check.
		{name: "a_band_whose_major_is_the_acceptance_bound",
			sql: `SELECT COUNT(*) AS n FROM semverpkg
			       WHERE semver_satisfies(v, '^9223372036854775807.0.0')`,
			want: []string{"n=int64:" + fmt.Sprint(svBandReleases(t, "9223372036854775807"))}},
		{name: "nothing_is_above_the_acceptance_bound",
			sql: `SELECT COUNT(*) AS n FROM semverpkg
			       WHERE semver_satisfies(v, '>9223372036854775807.x')`,
			want: []string{"n=int64:0"}},
		{name: "a_band_whose_minor_is_the_acceptance_bound_still_bounds",
			sql: `SELECT COUNT(*) AS n FROM semverpkg
			       WHERE semver_satisfies(v, '1.9223372036854775807.x')`,
			want: []string{"n=int64:" + fmt.Sprint(svBandReleases(t, "1", "9223372036854775807"))}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := a2fReadRoutes(arm.coord)
				res, err := arm.run(tc.name+"/"+arm.name, tc.sql)
				if err != nil {
					t.Fatalf("%s arm: %v", arm.name, err)
				}
				a2fCheckRoutes(t, arm.name, arm.coord, before, tc.sql)
				got := a3sRender(res, tc.ordered)
				if len(got) != len(tc.want) {
					t.Fatalf("%s arm: %d rows, want %d\n  got  %v\n  want %v",
						arm.name, len(got), len(tc.want), got, tc.want)
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm row %d: %s, want %s", arm.name, i, got[i], tc.want[i])
					}
				}
			}
		})
	}

	// ---- THE REFUSALS, which must reach the client from every arm and must
	// not depend on whether a row was reached.
	t.Run("an_unknown_range_literal_is_refused_with_no_rows_at_all", func(t *testing.T) {
		for _, sql := range []string{
			`SELECT COUNT(*) AS n FROM semverpkg WHERE id < 0 AND semver_satisfies(v,'^^1.0')`,
			`SELECT semver_satisfies(v,'^^1.0') AS s FROM semverpkg WHERE id < 0`,
			`SELECT id FROM semverpkg WHERE id < 0 GROUP BY id HAVING semver_satisfies(MIN(v),'^^1.0')`,
			`SELECT id FROM semverpkg WHERE id < 0 ORDER BY semver_satisfies(v,'^^1.0')`,
			`SELECT s FROM (SELECT semver_satisfies(v,'^^1.0') AS s FROM semverpkg WHERE id < 0) d`,
		} {
			for _, arm := range arms {
				_, err := arm.run("refusal/"+arm.name, sql)
				if err == nil {
					t.Errorf("%s arm answered rows for a range that names no range\n  SQL: %s",
						arm.name, sql)
					continue
				}
				if !a3sIsRangeRefusal(err) {
					t.Errorf("%s arm: %v (SQLSTATE %q), want 22023 naming the range\n  SQL: %s",
						arm.name, err, sqlerr.StateOf(err), sql)
				}
			}
		}
	})

	// A range from a COLUMN that names no range is refused too — the one path
	// no plan-time fold can cover, since the range does not exist until a row
	// does. On the DAG arms the 22023 is relayed out of a worker.
	t.Run("a_range_from_a_column_that_names_no_range_is_refused_on_every_arm", func(t *testing.T) {
		const sql = `SELECT COUNT(*) AS n FROM semverpkg WHERE semver_satisfies(v, rng)`
		for _, arm := range arms {
			_, err := arm.run("columnrefusal/"+arm.name, sql)
			if err == nil {
				t.Errorf("%s arm counted rows over a column holding a range that names no range",
					arm.name)
				continue
			}
			if !a3sIsRangeRefusal(err) {
				t.Errorf("%s arm: %v (SQLSTATE %q), want 22023 naming the range",
					arm.name, err, sqlerr.StateOf(err))
			}
			if !strings.Contains(err.Error(), svBadRange) {
				t.Errorf("%s arm: the refusal does not name the range %q: %v",
					arm.name, svBadRange, err)
			}
		}
	})

	// The loud twin raises on a reached row, on every arm.
	t.Run("the_strict_normalizer_raises_on_a_row_that_is_not_a_version", func(t *testing.T) {
		const sql = `SELECT semver_normalize_strict(v) AS n FROM semverpkg WHERE v = 'not-a-version'`
		for _, arm := range arms {
			_, err := arm.run("strict/"+arm.name, sql)
			if err == nil {
				t.Errorf("%s arm answered rows where the strict form must raise", arm.name)
				continue
			}
			if !strings.Contains(err.Error(), "not-a-version") {
				t.Errorf("%s arm: the refusal does not name the string: %v", arm.name, err)
			}
		}
	})

	a3sCheckSpillEngagement(t)
}

// a3sIsRangeRefusal reports whether an error is the range refusal, whichever
// layer raised it: the SQLSTATE, and nothing else.
//
// It used to accept the SENTENCE as a substitute — a message containing
// "22023" or "is not a version range" — which meant a relay that lost the
// class entirely would still pass the cell, and the class is half of what
// these cells are for. A worker's failure carries the SQLSTATE as a TYPED
// FIELD across the DAG (#649), so there is nothing to fall back to: measured
// on all five arms, `sqlerr.StateOf` is "22023" on every one of them.
func a3sIsRangeRefusal(err error) bool {
	return sqlerr.StateOf(err) == "22023"
}

// a3sRender renders a result the way na2Run does — the Go TYPE beside every
// non-string box, because "the right number under the wrong Go type" is what
// the numeric arcs are about — but keeps the ROW ORDER when the cell has an
// ORDER BY, which na2Run's sort would destroy.
func a3sRender(res *oracle.Result, ordered bool) []string {
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			switch t := r[c].(type) {
			case nil:
				parts = append(parts, c+"=NULL")
			case string:
				parts = append(parts, c+"="+t)
			default:
				parts = append(parts, fmt.Sprintf("%s=%T:%v", c, t, t))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if !ordered {
		sort.Strings(out)
	}
	return out
}

// a3sColumn reads one column out of a result IN ROW ORDER.
func a3sColumn(t *testing.T, res *oracle.Result, col string) []string {
	t.Helper()
	out := make([]string, 0, len(res.Rows))
	for i, r := range res.Rows {
		s, ok := r[col].(string)
		if !ok {
			t.Fatalf("row %d column %q is %T, not a string", i, col, r[col])
		}
		out = append(out, s)
	}
	return out
}

// a3sInversions counts adjacent pairs that are OUT of specification order,
// asked of `semver_cmp` through the registry — the engine's own function, so
// the gate cannot drift from it by holding a second comparison of its own.
func a3sInversions(t *testing.T, vs []string) int {
	t.Helper()
	cmp := expr.DefaultRegistry.Lookup("semver_cmp")
	if cmp == nil {
		t.Fatal("semver_cmp is not registered; this gate would pass vacuously")
	}
	bad := 0
	for i := 1; i < len(vs); i++ {
		c, ok := cmp([]any{vs[i-1], vs[i]}).(int32)
		if !ok {
			t.Fatalf("semver_cmp(%q,%q) did not answer an int32", vs[i-1], vs[i])
		}
		if c > 0 {
			if bad < 3 {
				t.Logf("out of order at %d: %q then %q", i, vs[i-1], vs[i])
			}
			bad++
		}
	}
	return bad
}

// a3sSum renders the SUM of an int8-declared function the way the arm hands it
// back. That SUM is NUMERIC — PostgreSQL's own `sum(int8)` is, and
// `expr.PGIntegerResultWidth` puts the three component functions on the int8
// row — so the value arrives as a DECIMAL, whose box here is a decimal STRING
// rather than an int64. That is the whole point of those three rows, and it is
// why the `sum_*` cells below read `s=6` while `sum_cmp_*`, whose function is
// int4-declared and whose SUM is therefore bigint, reads `s=int64:4`.
func a3sSum(n int64) string { return fmt.Sprint(n) }

// svValidRows is how many of the fixture's rows are versions: the 5000
// generated ones plus fourteen of the sixteen edge strings (`01.2.3` has a
// leading zero and `not-a-version` is text), with the NULL row excluded by
// WHERE semver_valid(v).
const svValidRows = svRows + 14

// svTextInversions is how many adjacent pairs a BYTE sort of the same corpus
// leaves out of specification order. It is the control's expectation and it is
// measured, not chosen: a 0 here would mean the corpus has stopped containing
// the pairs this family exists for, which would make the headline cell vacuous.
const svTextInversions = 1538

// svColumnRangeMatches is `semver_satisfies(v, rng)` over the whole fixture,
// with the range coming from a COLUMN — the path no literal fold can see.
// Measured on the single-process arm; every other arm must agree. The one row
// whose range names no range is excluded by id, since asking it is the
// REFUSAL cell rather than a counting one.
const svColumnRangeMatches = 279

// svBadRangeID is the one fixture row whose `rng` names NO range, and
// svBadRange is what it holds.
//
// The per-row refusal is the one path a plan-time literal fold cannot cover by
// construction — the range is a COLUMN, so nothing can be decided before a row
// exists — and until this row arrived the census asked that path only VALID
// ranges, which left the relay of a per-row 22023 out of a worker unmeasured.
// The id is an edge row's, so a cell can name it, and the value it displaces
// was the cycle's NULL, which matched nothing: the counting cell beside it
// counts exactly what it counted before.
const (
	svBadRangeID = 4
	svBadRange   = "^^1.0"
)

// --- the fixture, which rides along in tmdTables() ---

const svTable = "semverpkg"

func svSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
		{Name: "rng", Type: parquet.TypeString, Nullable: true},
	}}
}

// svRows is the generated part's size. It is chosen so the fixture spans the
// four files tmdWriteTables writes and really merges across them on the DAG
// arms — a fixture small enough to fit one file cannot tell a correct merge
// from a correct single-file sort.
const svRows = 5000

// svData is the sixteen edge versions, one NULL, and svRows generated ones.
// The edge rows come FIRST and keep fixed ids, so a cell can name one, and the
// generator excludes every edge string so a predicate naming one matches
// exactly the rows this function put there.
//
// `rng` cycles four ranges so `semver_satisfies(v, rng)` exercises the per-row
// path no literal fold can see. A NULL range rides in the cycle because a NULL
// operand answering NULL is part of the contract, and ONE row carries a range
// that names no range at all — see svBadRangeID.
func svData() []map[string]any {
	edge := semvergen.Edge()
	corpus := semvergen.Corpus(967, svRows)
	ranges := []any{"^1.0.0", ">=1.0.0 <2.0.0", "*", nil}
	rows := make([]map[string]any, 0, len(edge)+len(corpus)+1)
	id := int32(1)
	add := func(v any) {
		rng := ranges[int(id-1)%len(ranges)]
		if id == svBadRangeID {
			rng = svBadRange
		}
		rows = append(rows, map[string]any{"id": id, "v": v, "rng": rng})
		id++
	}
	for _, v := range edge {
		add(v)
	}
	add(nil)
	for _, v := range corpus {
		add(v)
	}
	return rows
}

// svBandReleases counts the fixture's versions whose core begins with the
// given fields and which carry NO pre-release, by reading the STRINGS.
//
// It is deliberately not a second call into the range machinery the cells
// above check: an expectation computed by the code under test is not an
// expectation. The pre-release exclusion is the published rule — an
// intersection whose comparators name no pre-release admits no pre-release
// version — and every band cell here is spelled with release bounds only.
func svBandReleases(t *testing.T, want ...string) int {
	t.Helper()
	if len(want) == 0 || len(want) > 3 {
		t.Fatalf("svBandReleases takes one to three leading core fields, got %d", len(want))
	}
	rows := svData()
	n, cores := 0, 0
	for _, row := range rows {
		s, ok := row["v"].(string)
		if !ok {
			continue
		}
		core, prerelease := s, false
		if i := strings.IndexAny(s, "-+"); i >= 0 {
			prerelease = s[i] == '-'
			core = s[:i]
		}
		fields := strings.Split(core, ".")
		if len(fields) != 3 {
			continue // not a version at all: `not-a-version` is in the fixture on purpose
		}
		cores++
		if prerelease {
			// A band spelled with RELEASE bounds admits no pre-release —
			// the published rule — so these rows are counted as versions and
			// excluded from the band.
			continue
		}
		match := true
		for i, w := range want {
			if fields[i] != w {
				match = false
			}
		}
		if match {
			n++
		}
	}
	// Both silent-zero paths are loud instead: an expectation of 0 computed
	// because the fixture stopped parsing, or because the band names nothing,
	// would make the cell that uses it pass vacuously.
	if cores < len(rows)/2 {
		t.Fatalf("only %d of %d fixture rows split into three core fields; this expectation "+
			"would be 0 because the fixture stopped parsing, not because the band is empty",
			cores, len(rows))
	}
	if n == 0 {
		t.Fatalf("no fixture row is a release in the band %v, so the cell using this "+
			"expectation would pass vacuously", want)
	}
	return n
}

// --- the spill-engagement ledger, ADR-0027 §6's protocol ---

var a3sEngaged sync.Map // cell name -> bool

// a3sCheckSpillEngagement reports what the budgeted arm actually did. A
// budgeted arm that never spilled is a second copy of `single` wearing a spill
// label, and saying so is what keeps that from being invisible.
func a3sCheckSpillEngagement(t *testing.T) {
	t.Helper()
	var engaged, total int
	var names []string
	a3sEngaged.Range(func(k, v any) bool {
		total++
		if v.(bool) {
			engaged++
			names = append(names, k.(string))
		}
		return true
	})
	sort.Strings(names)
	t.Logf("budgeted arm: %d of %d cells engaged a spill or a forced drain: %v",
		engaged, total, names)
	if total > 0 && engaged == 0 {
		t.Errorf("the budgeted arm engaged NO spill on any cell, so it is a second copy of " +
			"the single-process arm; ADR-0027 §6")
	}
	// PER CELL for the one that matters. A 512 KiB budget is a coin toss on a
	// small shape, and the ADR's rule is that a gate whose trigger is a
	// CONDITION asserts the condition fired where the claim lives — here the
	// 5014-row ORDER BY, which is the whole point of a byte-ordered key.
	const headline = "order/single+budget+forced-drain"
	if v, ok := a3sEngaged.Load(headline); !ok || !v.(bool) {
		t.Errorf("the 5014-row ORDER BY did not spill on the budgeted arm, so this gate "+
			"never compared a MERGED order against an in-memory one (ADR-0027 §6); "+
			"engaged: %v", names)
	}
}
