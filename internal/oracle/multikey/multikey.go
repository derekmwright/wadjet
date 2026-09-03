// Package multikey is the fixture and query corpus for correlated subqueries
// that correlate on MORE THAN ONE column.
//
// Why it exists. Every correlated-subquery entry in every other corpus here
// correlates on exactly ONE equality. #562 is what that blind spot cost: a
// two-column correlated EXISTS answered ZERO rows, its NOT EXISTS twin
// answered EVERY row, and neither the type matrix, the TPC-H corpus, the
// DuckDB fingerprint corpus, the PostgreSQL oracle nor the shape fuzzer
// contained a single query that could show it. The defect was in the build
// side's NDV narrowing (dedupSemiAntiBuildSide): it read the join keys out of
// the condition TEXT with a split on " and " while a decorrelation renders
// " AND ", so it kept the FIRST conjunct's key and projected the build side
// down to that one column — deleting the column the second conjunct compares.
//
// A two-column correlated existence check is what a BI client emits for a
// compound-key lookup, so the shape is ordinary and the failure was total and
// silent.
//
// This package holds no assertions and no expected answers beyond the ones
// PostgreSQL gave: three gates consume it.
//
//	wadjet.TestMultiKeyCorrelatedSubqueries      — the embedded engine
//	coordinator.TestMultiKeyCorrelatedTwoPath    — stage DAG vs single process
//	(PostgresSetup renders the same fixture for the container that decided
//	every Want below.)
package multikey

import (
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The fixture tables. Outer is the probe relation; Inner is SMALLER than it
// and Wide is more than three times LARGER, which is the estimator's threshold
// for swapping a semi/anti join onto its other side (exec's RightSemiJoin /
// RightAntiJoin). Both sides of that decision have to answer the same, so the
// corpus runs the same shape against each.
const (
	Outer = "mk_outer"
	Inner = "mk_inner"
	Wide  = "mk_wide"
	Dim   = "mk_dim"
)

// Row counts. Wide > 3 × Outer is the swap gate; Inner < Outer is its control.
const (
	OuterRows = 40
	InnerRows = 24
	WideRows  = 260
)

// uuids are the four UUID values the fixture cycles through. Fixed text, so
// the same bytes reach both engines.
var uuids = []string{
	"1b4e28ba-2fa1-11d2-883f-0016d3cca427",
	"2c5f39cb-3fb2-11d2-883f-0016d3cca427",
	"3d6a4adc-4fc3-11d2-883f-0016d3cca427",
	"4e7b5bed-5fd4-11d2-883f-0016d3cca427",
}

// Schema is the shape of Outer, Inner and Wide: an id, three PAIRS of columns
// that a correlated subquery can key on together, and a low-cardinality g for
// the third key and the dimension join.
//
// The pairs are chosen so a multi-column key reaches three different key
// encodings in the hash join: STRING+INT64 mixes a serialized column with an
// integer one, DECIMAL+DATE is two columns neither of which is a plain
// integer, and CIDR+UUID is two network-native types that only this fixture
// and the type matrix carry at all.
//
// Each pair's two columns cycle on COPRIME periods (6 and 5, 7 and 5, 9 and
// 4), so the PAIR is far more selective than either column alone and Inner
// covers only part of the product. That is what makes a dropped key visible
// as a wrong non-zero answer and not only as the zero #562 produced: the
// two-key answer sits strictly between the one-key answers, which the
// exists_one_key control pins from the other side.
func Schema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
		{Name: "n", Type: parquet.TypeInt64, Nullable: true},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		{Name: "dt", Type: parquet.TypeDate, Nullable: true},
		{Name: "c", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "u", Type: parquet.TypeUUID, Nullable: true},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
	}}
}

// DimSchema is the join partner that turns the subquery's inner into a JOIN,
// which is the shape whose key spelling only reorderJoins settles (ADR-0021).
func DimSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt32},
		{Name: "label", Type: parquet.TypeString, Nullable: true},
	}}
}

// Data builds one table's rows. Every value is derived from the row index, so
// the fixture is identical in every process — and in PostgreSQL, which
// PostgresSetup renders from this same function.
//
// NULLs are placed so that for each keyed PAIR, one side nulls the FIRST
// column and the other side nulls the SECOND. PostgreSQL's rule is that a NULL
// key never matches anything, including another NULL, and a multi-column key
// gives an implementation two chances to get that wrong — once per column, and
// once more in whatever composite it builds out of them.
func Data(table string) []map[string]any {
	n := rowsOf(table)
	out := make([]map[string]any, n)
	for i := range out {
		r := map[string]any{
			"id": int64(i),
			"s":  fmt.Sprintf("s%d", i%6),
			"n":  int64(i % 5),
			"d":  float64(i%7) * 1.25,
			"dt": fmt.Sprintf("2024-01-%02d", 1+i%5),
			"c":  fmt.Sprintf("10.0.%d.0/24", i%9),
			"u":  uuids[i%4],
			"g":  int32(i % 4),
		}
		switch table {
		case Outer:
			// The FIRST column of each pair, on the probe side.
			nullIf(r, "s", i%11 == 4)
			nullIf(r, "dt", i%13 == 6)
			nullIf(r, "u", i%17 == 9)
		case Inner:
			// The SECOND column of each pair, on the build side.
			nullIf(r, "n", i%7 == 3)
			nullIf(r, "d", i%9 == 5)
			nullIf(r, "c", i%5 == 2)
		case Wide:
			// Both, on strides of their own, so the swapped shape is not a
			// re-run of the same null layout.
			nullIf(r, "s", i%23 == 8)
			nullIf(r, "n", i%29 == 11)
			nullIf(r, "c", i%31 == 13)
		}
		out[i] = r
	}
	return out
}

// DimData is five groups, one more than g's four, so the dimension join is not
// a no-op filter and one dim row matches nothing.
func DimData() []map[string]any {
	return []map[string]any{
		{"k": int32(0), "label": "zero"},
		{"k": int32(1), "label": "one"},
		{"k": int32(2), "label": "two"},
		{"k": int32(3), "label": "three"},
		{"k": int32(4), "label": "spare"},
	}
}

func rowsOf(table string) int {
	switch table {
	case Outer:
		return OuterRows
	case Inner:
		return InnerRows
	case Wide:
		return WideRows
	}
	return 0
}

func nullIf(r map[string]any, col string, cond bool) {
	if cond {
		r[col] = nil
	}
}

// FixtureTable is one loadable table: the four shared-schema relations and
// the four distinct-name ones. Every consumer loads this list, so an arm
// cannot be added to the corpus and forgotten in one of the four gates.
type FixtureTable struct {
	Name   string
	Schema parquet.Schema
	Rows   []map[string]any
}

// Tables is the whole fixture.
func Tables() []FixtureTable {
	out := []FixtureTable{
		{Outer, Schema(), Data(Outer)},
		{Inner, Schema(), Data(Inner)},
		{Wide, Schema(), Data(Wide)},
		{Dim, DimSchema(), DimData()},
	}
	for _, t := range []string{DNOuter, DNInner, DNWide, DNDim} {
		out = append(out, FixtureTable{t, DNSchema(t), DNData(t)})
	}
	return out
}

// Case is one corpus entry: a query and the number of rows PostgreSQL 17
// answers it with over this fixture.
//
// Want is an ABSOLUTE answer, not an agreement between two wadjet paths. The
// #562 defect took a semi join to zero and its anti twin to everything, and
// both of those are answers two agreeing arms will happily produce together —
// the stage DAG and the single-process pipeline share the logical optimizer,
// so a planner defect hits them identically. Only an outside authority can
// see it, which is why every Want here came from the container.
type Case struct {
	Name string
	SQL  string
	Want int64
	// Keys is how many equality conjuncts the decorrelation should produce,
	// for the report when an entry fails. Not asserted — the plan shape is
	// asserted in internal/planner/logical.
	Keys int
	// KnownBug pins a divergence from Want that is NOT this corpus's subject
	// and is tracked in Issue. The comparison still RUNS and Want stays
	// exactly as PostgreSQL wrote it: a pinned entry that starts AGREEING
	// fails, so deleting the pin is the whole of "the fix landed"
	// (ADR-0013 §Pins). Empty for every other entry.
	KnownBug string
	// Issue is the tracker reference for a KnownBug.
	Issue string
	// LoudLike and LoudLikeDAG are the substrings the entry's ERROR must
	// carry, per ARM, where the pinned divergence is a REFUSAL rather than a
	// wrong number. Empty means the entry is pinned on its VALUE and an error
	// from that arm is a failure, as it was before any entry became loud.
	//
	// They exist so that "this entry is pinned" cannot mean "this entry may
	// fail in any way at all". Four entries went from a wrong number to a
	// refusal when #734/#679/#535's consumer half landed, and a harness that
	// simply stopped asserting on error for every pinned entry would have
	// swallowed a future regression of any class in any of them.
	//
	// TWO fields because the two engines refuse these for two DIFFERENT
	// reasons, both pre-existing: the single-process path fails inside the
	// per-row re-run (an unparseable rebuild, or a reference it refuses to
	// resolve standalone), while the DAG never gets that far — its worker has
	// no SubqueryRunner and the filter compile refuses first. One substring
	// for both would have had to be short enough to match neither precisely.
	LoudLike    string
	LoudLikeDAG string
}

// pins are the entries whose Want a defect OTHER than #562 keeps wadjet from
// answering. Each is a live PostgreSQL answer that some other part of the
// engine gets wrong; none of them is a multi-key-key-list defect, and all of
// them reproduce with ONE key too, which is how they were told apart.
var pins = map[string]struct {
	issue, reason string
	// loudLike / loudLikeDAG are the substrings the entry's ERROR must carry,
	// per arm, where the pinned divergence is a REFUSAL rather than a wrong
	// number (see Case.LoudLike). Empty for an entry pinned on its VALUE,
	// where an error is still a failure.
	loudLike, loudLikeDAG string
}{
	// The three notin_* entries were pinned here under #578 and are gone: a
	// correlated NOT IN is no longer lowered to an anti join, which answers
	// the two-valued question, so the predicate stays a subquery and
	// expr.CorrelatedInSubquery.EvalBoolNull carries the three-valued rule
	// per outer row. They are gated outright now.
	//
	// LOUD SINCE 2026-09-02 (#734/#679/#535, the consumer half). These four
	// used to answer a WRONG NUMBER; they now FAIL the query. The re-run
	// their per-row predicate produced could not be executed — the dropped
	// column-alias list makes the rebuilt SQL unparseable, and the
	// un-decorrelated CTE reference dangles — and the evaluator that ate that
	// failure and returned a boolean constant fails instead. The divergence
	// from PostgreSQL is unchanged and the pins stand; what changed is that
	// the user is told. Both harnesses ask about the DISPOSITION before the
	// rows, so a pinned entry that errors is a logged divergence and one that
	// starts ANSWERING PostgreSQL's number still fails the pin.
	// derived_exists_colalias and derived_in_colalias were pinned here under
	// #613 and are gone: a derived table's column-alias list `(…) AS b(kk,nn)`
	// is applied on both arms now, so both answer PostgreSQL's 23 and 36. The
	// corpus entries stay, because they are the only ones that reach the list
	// through a CORRELATED subquery — the site it was lost at last.
	//
	// cte_probe_base_build and cte_referenced_twice were pinned here under
	// #535 and are gone: a CTE reference is a named SCOPE and the four
	// outer-scope collectors read it off the subtree root now, so a
	// correlated EXISTS over one is decorrelated like any other. Both are
	// gated outright.
}

// Corpus is the query set. Every entry counts rows, because a count is what
// #562 destroyed and because it is comparable against PostgreSQL without
// agreeing on how either engine renders a CIDR or a DECIMAL.
// oneKeyWant is PostgreSQL's answer for each single-key EXISTS control.
var oneKeyWant = map[string]int64{"s": 36, "n": 40, "d": 40, "dt": 37, "c": 40, "u": 38}

// Corpus is both arms. The shared-schema arm is where #562 was found and
// where the pass DECLINES (every conjunct reads `s = s`); the distinct-name
// arm (distinct_names.go) is where it FIRES. A gate that runs only the first
// would prove the decline and never touch the narrowing.
func Corpus() []Case {
	return append(sharedNameCorpus(), DistinctNameCorpus()...)
}

// sharedNameCorpus is the arm whose relations carry one schema.
func sharedNameCorpus() []Case {
	var out []Case
	add := func(name, sql string, want int64, keys int) {
		c := Case{Name: name, SQL: sql, Want: want, Keys: keys}
		if p, ok := pins[name]; ok {
			c.Issue, c.KnownBug = p.issue, p.reason
			c.LoudLike, c.LoudLikeDAG = p.loudLike, p.loudLikeDAG
		}
		out = append(out, c)
	}

	// --- two correlated keys, per type pair ------------------------------
	//
	// The #562 shape itself. Each pair reaches a different key encoding in
	// the hash join, and the composite key the build side is narrowed to is
	// built per encoding.
	add("exists_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.s = a.s AND b.n = a.n)`,
		Outer, Inner), 27, 2)
	// The same two conjuncts written the other way round. #562 reproduced in
	// both orders, so a fix that only reads the first conjunct passes one of
	// these and fails the other.
	add("exists_str_i64_reordered", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.n = a.n AND b.s = a.s)`,
		Outer, Inner), 27, 2)
	add("notexists_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b WHERE b.s = a.s AND b.n = a.n)`,
		Outer, Inner), 13, 2)
	add("exists_dec_date", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.d = a.d AND b.dt = a.dt)`,
		Outer, Inner), 24, 2)
	add("notexists_dec_date", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b WHERE b.d = a.d AND b.dt = a.dt)`,
		Outer, Inner), 16, 2)
	add("exists_cidr_uuid", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.c = a.c AND b.u = a.u)`,
		Outer, Inner), 21, 2)
	add("notexists_cidr_uuid", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b WHERE b.c = a.c AND b.u = a.u)`,
		Outer, Inner), 19, 2)

	// --- IN and NOT IN reach the same lowering by another route ----------
	//
	// A correlated IN contributes the IN key AND the correlation, so these
	// are two-key semi/anti joins built by tryDecorrelateInSubquery rather
	// than by tryDecorrelateExists.
	add("in_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM %s b WHERE b.n = a.n)`,
		Outer, Inner), 27, 2)
	add("notin_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s NOT IN (SELECT b.s FROM %s b WHERE b.n = a.n)`,
		Outer, Inner), 9, 2)
	add("in_dec_date", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.d IN (SELECT b.d FROM %s b WHERE b.dt = a.dt)`,
		Outer, Inner), 24, 2)
	add("in_cidr_uuid", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.c IN (SELECT b.c FROM %s b WHERE b.u = a.u)`,
		Outer, Inner), 21, 2)

	// --- three correlated keys -------------------------------------------
	add("exists_three_keys", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.s = a.s AND b.n = a.n AND b.g = a.g)`, Outer, Inner), 19, 3)
	add("notexists_three_keys", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.s = a.s AND b.n = a.n AND b.g = a.g)`, Outer, Inner), 21, 3)
	add("in_three_keys", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM %s b `+
			`WHERE b.n = a.n AND b.g = a.g)`, Outer, Inner), 19, 3)
	add("exists_three_mixed_types", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.d = a.d AND b.dt = a.dt AND b.c = a.c)`, Outer, Inner), 14, 3)

	// --- an inner-only predicate on either side of the correlations ------
	//
	// The decorrelation classifies each WHERE conjunct in write order and
	// pushes the inner-only ones onto the inner scan. Where the predicate sits
	// relative to the correlations decides nothing about the answer, and that
	// is the claim.
	add("exists_pred_before", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.id < 12 AND b.s = a.s AND b.n = a.n)`, Outer, Inner), 17, 2)
	add("exists_pred_after", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.s = a.s AND b.n = a.n AND b.id < 12)`, Outer, Inner), 17, 2)
	add("exists_pred_between", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.s = a.s AND b.id < 12 AND b.n = a.n)`, Outer, Inner), 17, 2)
	add("exists_outer_and_inner_pred", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.id %% 3 = 0 AND EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.s = a.s AND b.n = a.n AND b.id < 12)`, Outer, Inner), 6, 2)

	// --- NULLs in one key, on either side --------------------------------
	//
	// PostgreSQL's rule: a NULL key matches nothing, itself included. These
	// isolate it — the first two restrict the probe to rows whose FIRST key
	// is NULL, which no build row can match however the second key compares.
	add("exists_null_probe_key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IS NULL AND EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.s = a.s AND b.n = a.n)`, Outer, Inner), 0, 2)
	add("notexists_null_probe_key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IS NULL AND NOT EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.s = a.s AND b.n = a.n)`, Outer, Inner), 4, 2)
	// The build side's SECOND key is the NULL-carrying one here, so a build
	// row with a NULL n must not match a probe row with a NULL n either.
	add("exists_null_build_key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b `+
			`WHERE b.n = a.n AND b.d = a.d)`, Outer, Inner), 22, 2)
	// NOT IN's three-valued rule over a multi-column correlation: the list
	// carries NULLs in the IN key itself.
	add("notin_null_build_key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.n NOT IN (SELECT b.n FROM %s b WHERE b.s = a.s)`,
		Outer, Inner), 6, 2)
	add("notin_null_probe_key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s NOT IN (SELECT b.s FROM %s b `+
			`WHERE b.n = a.n AND b.s IS NOT NULL)`, Outer, Inner), 9, 2)

	// --- the estimator's swap --------------------------------------------
	//
	// Wide is more than three times Outer, which turns the semi join into a
	// RightSemiJoin and the anti into a RightAntiJoin — a different probe
	// loop, a different place for a build-side narrowing to go wrong. The
	// _noswap control runs the same shape with the sides' sizes reversed.
	add("exists_swap_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.s = a.s AND b.n = a.n)`,
		Outer, Wide), 36, 2)
	add("notexists_swap_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b WHERE b.s = a.s AND b.n = a.n)`,
		Outer, Wide), 4, 2)
	add("in_swap_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM %s b WHERE b.n = a.n)`,
		Outer, Wide), 36, 2)
	add("exists_noswap_str_i64", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.s = a.s AND b.n = a.n)`,
		Wide, Inner), 173, 2)

	// --- the subquery's inner is itself a JOIN ---------------------------
	//
	// Which of the inner's relations the join emits bare is reorderJoins'
	// decision, made long after the decorrelation named the key (ADR-0021).
	// Both write orders, because the two are spelled alike only by accident.
	add("exists_joined_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b JOIN %s k ON k.k = b.g `+
			`WHERE b.s = a.s AND b.n = a.n)`, Outer, Inner, Dim), 27, 2)
	add("exists_joined_inner_nonlead", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s k JOIN %s b ON k.k = b.g `+
			`WHERE b.s = a.s AND b.n = a.n)`, Outer, Dim, Inner), 27, 2)
	add("notexists_joined_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b JOIN %s k ON k.k = b.g `+
			`WHERE b.s = a.s AND b.n = a.n)`, Outer, Inner, Dim), 13, 2)
	add("in_joined_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM %s b JOIN %s k ON k.k = b.g `+
			`WHERE b.n = a.n)`, Outer, Inner, Dim), 27, 2)

	// --- the subquery's inner is a DERIVED TABLE -------------------------
	//
	// Gated, not pinned: #550/#571 (fix(planner,expr): decline decorrelation
	// over a derived-table inner) made all three correct on both paths, so
	// the #577 pins these carried were deleted — the entries now assert the
	// live PostgreSQL answer outright. The HARDER derived shapes #577 still
	// tracks (CTE, renamed columns, nested, aggregate) are fix-577's, not
	// these.
	add("exists_derived_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM `+
			`(SELECT s, n, g FROM %s WHERE id < 20) b WHERE b.s = a.s AND b.n = a.n)`,
		Outer, Inner), 23, 2)
	add("notexists_derived_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM `+
			`(SELECT s, n, g FROM %s WHERE id < 20) b WHERE b.s = a.s AND b.n = a.n)`,
		Outer, Inner), 17, 2)
	add("in_derived_inner", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM `+
			`(SELECT s, n FROM %s WHERE id < 20) b WHERE b.n = a.n)`, Outer, Inner), 23, 2)

	// --- HARDER derived/CTE shapes, and RECURSIVE CTEs (#577 remainder) ---
	//
	// The simple derived shapes above (exists/notexists/in_derived_inner) are
	// answered by innerRelationsAreScannable declining the derived table and
	// the materialize/local routes running it as written (#550). These are the
	// shapes that remained: a renamed or computed derived column, a two-level
	// nest, and — the class the decline did NOT reach until the enclosing WITH
	// was threaded to it — a CTE feeding the subquery, and a RECURSIVE CTE.
	//
	// All are GATED (unpinned): a CTE reference now declines exactly as a
	// derived table does, and buildSubqueryPipeline resolves it (it merges the
	// enclosing WITH); a recursive CTE is declined too and the materializer
	// REFUSES it (physical/in_subquery_set.go) rather than reading its
	// cacheless set as empty. Every Want is live PostgreSQL 17's.
	add("derived_exists_renamed", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM `+
			`(SELECT s AS kk, n AS nn FROM %s WHERE id < 20) b WHERE b.kk = a.s AND b.nn = a.n)`,
		Outer, Inner), 23, 2)
	add("derived_in_computed", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.n IN (SELECT b.m FROM `+
			`(SELECT n + 1 AS m FROM %s) b)`, Outer, Inner), 32, 1)
	add("derived_in_nested", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM `+
			`(SELECT s FROM (SELECT s FROM %s) c) b)`, Outer, Inner), 36, 1)
	add("derived_exists_nested", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM `+
			`(SELECT s, n FROM (SELECT s, n, id FROM %s) c WHERE c.id < 20) b `+
			`WHERE b.s = a.s AND b.n = a.n)`, Outer, Inner), 23, 2)

	// A CTE feeding the subquery — declined and resolved like a derived table
	// now that the WITH reaches innerRelationsAreScannable (#535/#581 build side).
	add("cte_in_2key", fmt.Sprintf(
		`WITH src AS (SELECT s, n FROM %s WHERE id < 20) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM src b WHERE b.n = a.n)`,
		Inner, Outer), 23, 2)
	add("cte_notin_2key", fmt.Sprintf(
		`WITH src AS (SELECT s, n FROM %s WHERE id < 20) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE a.s NOT IN (SELECT b.s FROM src b WHERE b.n = a.n)`,
		Inner, Outer), 13, 2)
	add("cte_exists_2key", fmt.Sprintf(
		`WITH src AS (SELECT s, n FROM %s WHERE id < 20) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM src b WHERE b.s = a.s AND b.n = a.n)`,
		Inner, Outer), 23, 2)
	add("cte_notexists_2key", fmt.Sprintf(
		`WITH src AS (SELECT s, n FROM %s WHERE id < 20) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM src b WHERE b.s = a.s AND b.n = a.n)`,
		Inner, Outer), 17, 2)
	add("cte_in_renamed", fmt.Sprintf(
		`WITH src AS (SELECT s AS kk FROM %s WHERE id < 12) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.kk FROM src b)`,
		Inner, Outer), 36, 1)
	add("cte_in_aggregate", fmt.Sprintf(
		`WITH src AS (SELECT n, COUNT(*) AS c FROM %s WHERE n IS NOT NULL GROUP BY n HAVING COUNT(*) > 4) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE a.n IN (SELECT b.n FROM src b)`,
		Inner, Outer), 8, 1)
	add("cte_exists_chained", fmt.Sprintf(
		`WITH s1 AS (SELECT s, n, id FROM %s), s2 AS (SELECT s, n FROM s1 WHERE id < 20) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM s2 b WHERE b.s = a.s AND b.n = a.n)`,
		Inner, Outer), 23, 2)
	add("cte_in_joined", fmt.Sprintf(
		`WITH src AS (SELECT s, n, g FROM %s WHERE id < 20) `+
			`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.s FROM src b `+
			`JOIN %s k ON k.k = b.g WHERE b.n = a.n)`, Inner, Outer, Dim), 23, 2)

	// A RECURSIVE CTE feeding IN/NOT IN — declined, and the materializer
	// refuses it (no fixed-point cache in the set producer) rather than
	// reading an empty set: IN was 0 and NOT IN every row on the DAG (#F1).
	rec := "WITH RECURSIVE r(x) AS (SELECT 0 UNION ALL SELECT x + 1 FROM r WHERE x < 3) "
	add("rec_in_direct", rec+fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.n IN (SELECT r.x FROM r)`, Outer), 32, 1)
	add("rec_notin_direct", rec+fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.n NOT IN (SELECT r.x FROM r)`, Outer), 8, 1)
	add("rec_in_derived", rec+fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.n IN (SELECT y.x FROM (SELECT x FROM r) y)`, Outer), 32, 1)
	add("rec_notin_derived", rec+fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.n NOT IN (SELECT y.x FROM (SELECT x FROM r) y)`, Outer), 8, 1)

	// --- pinned, tracked elsewhere ---------------------------------------
	//
	// A derived table's column-alias LIST, fixed in #613 and no longer pinned:
	// `AS b(kk, nn)` renames positionally, so these answer PostgreSQL's 23
	// and 36 on every arm. Kept in the corpus because they are the only
	// entries that reach the list through a CORRELATED subquery, which is
	// where it was lost last (RebuildSQL dropped it from the FROM clause it
	// rebuilds per row).
	add("derived_exists_colalias", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM `+
			`(SELECT s, n FROM %s WHERE id < 20) AS b(kk, nn) WHERE b.kk = a.s AND b.nn = a.n)`,
		Outer, Inner), 23, 2)
	add("derived_in_colalias", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE a.s IN (SELECT b.kk FROM `+
			`(SELECT s FROM %s WHERE id < 12) AS b(kk))`, Outer, Inner), 36, 1)
	// A CTE on the PROBE side is not decorrelated (#535); the build side being
	// a base table rules out the CTE-build path as the cause. Pinned.
	add("cte_probe_base_build", fmt.Sprintf(
		`WITH src AS (SELECT s, n FROM %s WHERE id < 30) `+
			`SELECT COUNT(*) AS n FROM src a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.s = a.s AND b.n = a.n)`,
		Outer, Inner), 19, 2)
	add("cte_referenced_twice", fmt.Sprintf(
		`WITH src AS (SELECT s, n FROM %s WHERE id < 30) `+
			`SELECT COUNT(*) AS n FROM src a WHERE EXISTS (SELECT 1 FROM src b WHERE b.s = a.s AND b.n = a.n)`,
		Outer), 27, 2)

	// --- single-key controls ---------------------------------------------
	//
	// These were correct before #562 and must stay correct: a fix that
	// disables the build-side narrowing wholesale would pass every entry
	// above while changing what these cost.
	// They are also the BRACKETS. A two-key answer that drifts up to one of
	// these is a key that stopped being compared, which is the non-zero half
	// of #562's failure mode and the half a "did it come back empty" check
	// cannot see.
	for _, k := range []struct{ name, col string }{
		{"s", "s"}, {"n", "n"}, {"d", "d"}, {"dt", "dt"}, {"c", "c"}, {"u", "u"},
	} {
		add("exists_one_key_"+k.name, fmt.Sprintf(
			`SELECT COUNT(*) AS n FROM %s a WHERE EXISTS (SELECT 1 FROM %s b WHERE b.%s = a.%s)`,
			Outer, Inner, k.col, k.col), oneKeyWant[k.name], 1)
	}
	add("notexists_one_key", fmt.Sprintf(
		`SELECT COUNT(*) AS n FROM %s a WHERE NOT EXISTS (SELECT 1 FROM %s b WHERE b.n = a.n)`,
		Outer, Inner), 0, 1)

	return out
}

// PostgresSetup renders the fixture as PostgreSQL DDL and INSERTs, from the
// same Data() the engines load. It is how every Want above was decided, and it
// is here so that deciding them again is a re-run rather than an archaeology
// exercise: TestCorpusAnswersComeFromPostgres (multikey_test.go) loads this
// script into a postgres:17-alpine container, runs every Corpus() query
// against it, and asserts each Want is what PostgreSQL says. It SKIPS when no
// container is reachable, exactly like the TPC-H oracle.
//
// The text columns are COLLATE "C" for the same reason the TPC-H oracle's are:
// wadjet compares strings by bytes.
func PostgresSetup() string {
	var b strings.Builder
	for _, tbl := range []string{Outer, Inner, Wide} {
		fmt.Fprintf(&b, "DROP TABLE IF EXISTS %s;\n", tbl)
		fmt.Fprintf(&b, "CREATE TABLE %s (id bigint, s text COLLATE \"C\", n bigint, "+
			"d numeric(18,4), dt date, c cidr, u uuid, g integer);\n", tbl)
		for _, r := range Data(tbl) {
			fmt.Fprintf(&b, "INSERT INTO %s VALUES (%d, %s, %s, %s, %s, %s, %s, %s);\n",
				tbl, r["id"].(int64),
				pgLit(r["s"]), pgLit(r["n"]), pgLit(r["d"]),
				pgLit(r["dt"]), pgLit(r["c"]), pgLit(r["u"]), pgLit(r["g"]))
		}
	}
	dnPostgresSetup(&b)
	fmt.Fprintf(&b, "DROP TABLE IF EXISTS %s;\n", Dim)
	fmt.Fprintf(&b, "CREATE TABLE %s (k integer, label text COLLATE \"C\");\n", Dim)
	for _, r := range DimData() {
		fmt.Fprintf(&b, "INSERT INTO %s VALUES (%s, %s);\n", Dim, pgLit(r["k"]), pgLit(r["label"]))
	}
	return b.String()
}

func pgLit(v any) string {
	switch x := v.(type) {
	case nil:
		return "NULL"
	case string:
		return "'" + strings.ReplaceAll(x, "'", "''") + "'"
	case int64:
		return fmt.Sprintf("%d", x)
	case int32:
		return fmt.Sprintf("%d", x)
	case float64:
		return fmt.Sprintf("%.4f", x)
	}
	return fmt.Sprint(v)
}
