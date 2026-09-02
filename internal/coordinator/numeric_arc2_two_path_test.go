package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// The numeric/decimal arc-2 census: every shape on FIVE arms, each answer
// anchored to live PostgreSQL 17 rather than to the other arm.
//
// The arms are the five an answer can differ between (ADR-0018 §3, ADR-0027):
//
//	single                      the embedded engine, no memory budget
//	single+budget+forced-drain  the same engine at 512 KiB with
//	                            exec.ForceAggDrainEvery(1) armed, the reference
//	                            arm disarmed (ADR-0027 §6) — a spill is a
//	                            CONDITION, not a shape, and a budget that does
//	                            not bite proves nothing (§5)
//	dag                         the stage DAG over an embedded NATS cluster
//	dag+broadcast               the DAG with BroadcastBytesOverride = 1, which
//	                            forces the shuffle rather than the broadcast join
//	dag+morsel4                 the DAG with worker.MorselWorkers = 4, so each
//	                            fragment's breaker runs morsel-parallel and
//	                            CloneSink's SECOND call site is exercised
//
// `want` is what `psql` printed on the oracle container (127.0.0.1:55432,
// --locale=C, PostgreSQL 17) for the SAME rows, recorded before any of these
// fixes was written. A DECIMAL is compared DIGIT FOR DIGIT: the whole point of
// these issues is a value that is right to six digits and wrong after them.
//
// Issues: #727 (a CTE's TEXT column re-typed from its first row's VALUE),
// #728 (an aggregate's output declared FLOAT64 through a rename), #786
// (a derived GROUP BY key typed against a scope that stops at a Project),
// #749 (an exact operator's scale reduced to buy integer digits), #703
// (DISTINCT dropped for every aggregate but COUNT, and then double-counted
// across morsel-parallel clones), #704 (an integer column compared against a
// non-integral literal by truncating the literal), #784 (SUM/AVG over integers
// answering in float64) and #696 (a scalar subquery's value substituted
// without its declaration on one path and without its SCALE on the other).
func TestNumericArc2ShapesMatchPostgres(t *testing.T) {
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
	// A DAG whose workers run their breakers MORSEL-PARALLEL. It is a fifth
	// arm, not a variant of the fourth: CloneSink has two call sites and the
	// worker's is the one only this width reaches. With it unguarded,
	// `SUM(DISTINCT a)` answered 64.96 for 16.24 — four clones, four times the
	// value — deterministically at a fixed width and nondeterministically
	// under the auto one (#703, review round 2 B1). Four workers because the
	// multiplier IS the clone count, so a wrong answer is unmistakable.
	infraM := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraM, nil)
	coordM := tmdCoordinatorWithWorkers(t, ctx, infraM,
		func(w *worker.Config) { w.MorselWorkers = 4 })

	for _, tc := range []struct {
		issue, name, sql string
		want             []string
	}{
		// ------------------------------------------------------------------
		// #727 — a CTE materialization used to read the FIRST ROW of every
		// STRING column and, when that one value parsed as a number, re-type
		// the whole column. decpair.s is TEXT holding "1.50","1.5","abc",
		// "1.500": the column came back double precision, `s = '1.50'`
		// compared NUMBERS, and "abc" became NULL and then a hard
		// `invalid input syntax for type double precision`.
		{"#727", "cte_text_column_stays_text",
			`WITH c AS (SELECT s, id FROM decpair) SELECT COUNT(*) AS n FROM c WHERE s = '1.50'`,
			[]string{"n=int64:1"}},
		{"#727", "derived_table_twin_unchanged",
			`SELECT COUNT(*) AS n FROM (SELECT s, id FROM decpair) c WHERE s = '1.50'`,
			[]string{"n=int64:1"}},
		{"#727", "cte_text_in_a_case_arm",
			`WITH c AS (SELECT s, a*2 AS v FROM decpair) SELECT SUM(CASE WHEN s='abc' THEN v ELSE 0 END) AS sm FROM c`,
			[]string{"sm=25.50"}},
		{"#727", "cte_text_column_reads_back_whole",
			`WITH c AS (SELECT s, id FROM decpair) SELECT s AS v FROM c WHERE id IN (1,3,6)`,
			[]string{"v=1.50", "v=1.500", "v=abc"}},
		// The type used to depend on the DATA: the same CTE body restricted
		// to the row holding "abc" kept the string, and without the
		// restriction did not. Both spellings must now answer as TEXT.
		{"#727", "cte_text_type_does_not_depend_on_the_first_row",
			`WITH c AS (SELECT s, id FROM decpair WHERE id = 3) SELECT s AS v FROM c`,
			[]string{"v=abc"}},

		// ------------------------------------------------------------------
		// #728 — SUM/AVG/MIN/MAX resolved a bare column argument by searching
		// the SCANS below for that NAME, which cannot see a rename. Over
		// `(SELECT dw AS v FROM decwin) x` nothing carries `v`, so the
		// aggregate's OUTPUT declared FLOAT64 and the projection above it
		// computed in float: two spellings of one question, two numbers.
		{"#728", "sum_times_two_over_a_rename",
			`SELECT SUM(v*2) AS s FROM (SELECT dw AS v FROM decwin) x`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "sum_times_two_over_the_base_column",
			`SELECT SUM(dw*2) AS s FROM decwin`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "sum_times_two_through_a_cte",
			`WITH c AS (SELECT dw AS v FROM decwin) SELECT SUM(v*2) AS s FROM c`,
			[]string{"s=7489777778620377.6246619782"}},
		{"#728", "max_times_two_over_a_rename",
			`SELECT MAX(v*2) AS m FROM (SELECT dw AS v FROM decwin) x`,
			[]string{"m=1994666666891066.6258890908"}},
		// The SHADOW shapes: a derived table that binds a name to one column
		// while another column OF THAT NAME exists below it. #728's first cut
		// preferred the emitted walk unconditionally, so the second question
		// the DAG asks — "what is `c_dec`", the DISPATCH spelling — was
		// answered INT32 by the derived table where the worker reads the
		// scan's DECIMAL. One partial then emitted its SUM as an integer while
		// its siblings emitted DECIMAL and ADR-0010's shuffle
		// type-consistency guard refused the read: eight shapes that answered
		// on the DAG at base became hard failures (review F2). The scan walk
		// is asked first now, and these are the fixture self-flag 5 said did
		// not exist.
		{"#728", "shadowed_rename_sum",
			`SELECT SUM(v) AS s FROM (SELECT c_dec AS v, c_i32 AS c_dec FROM typemx WHERE id < 8) x`,
			[]string{"s=28.0028"}},
		{"#728", "shadowed_rename_sum_times_two",
			`SELECT SUM(v * 2) AS s FROM (SELECT c_dec AS v, c_i32 AS c_dec FROM typemx WHERE id < 8) x`,
			[]string{"s=56.0056"}},
		{"#728", "shadowed_rename_max_times_two",
			`SELECT MAX(v * 2) AS m FROM (SELECT c_dec AS v, c_i32 AS c_dec FROM typemx WHERE id < 8) x`,
			[]string{"m=14.0014"}},
		{"#728", "shadowed_rename_through_a_cte",
			`WITH c AS (SELECT c_dec AS v, c_i32 AS c_dec FROM typemx WHERE id < 8) ` +
				`SELECT SUM(v * 2) AS s FROM c`,
			[]string{"s=56.0056"}},
		{"#728", "shadowed_rename_over_an_int64_column",
			`SELECT SUM(v) AS s FROM (SELECT c_dec AS v, c_i64 AS c_dec FROM typemx WHERE id < 8) x`,
			[]string{"s=28.0028"}},
		{"#728", "shadowed_rename_over_a_text_column",
			`SELECT SUM(v) AS s FROM (SELECT c_dec AS v, c_str AS c_dec FROM typemx WHERE id < 8) x`,
			[]string{"s=28.0028"}},

		// ------------------------------------------------------------------
		// #786 — a derived GROUP BY key was typed against inputColDecls,
		// which stops at a Project, and the arithmetic above the unresolved
		// column still answered expr.Decided FLOAT64. The key therefore
		// declared FLOAT64 over a derived table and died at the #361 store
		// guard. The delimited sibling column in #786's own spelling is not
		// the trigger: a parsed BinaryOp asks for `c_dec`, never for the name
		// "c_dec + 1" (ADR-0026 §2c).
		//
		// #781 is NOT here and was never fixed by this work, though an
		// earlier version of this header claimed it: #781 is a stage-SPELLING
		// problem, `window_key_group_two_path_test.go`'s `pin781/*` entries
		// still stand, and their ratchet agrees they still diverge.
		{"#786", "derived_decimal_key_over_a_derived_table",
			`SELECT c_dec + 1 AS k FROM (SELECT c_dec FROM typemx WHERE id < 4) s GROUP BY c_dec + 1 ORDER BY k`,
			[]string{"k=1.0000", "k=2.0001", "k=3.0002", "k=4.0003"}},
		{"#786", "derived_decimal_key_beside_a_delimited_column_of_that_text",
			`SELECT c_dec + 1 AS k, MAX("c_dec + 1") AS m FROM ` +
				`(SELECT c_dec, c_i32 AS "c_dec + 1" FROM typemx WHERE id < 4) s GROUP BY c_dec + 1 ORDER BY k`,
			[]string{"k=1.0000|m=int64:0", "k=2.0001|m=int64:3", "k=3.0002|m=int64:6", "k=4.0003|m=int64:9"}},

		// ------------------------------------------------------------------
		// #749 — item 3's p>38 reduction spent an EXACT operator's fraction
		// digits to buy integer digits: over DECIMAL(38,10), `dw + 1` came
		// back at scale 9 and `dw * 2` at scale 8, correctly rounded and so
		// indistinguishable from exact. PostgreSQL keeps all ten.
		{"#749", "wide_decimal_plus_one_keeps_its_scale",
			`SELECT dw + 1 AS p FROM decwin WHERE id = 199`,
			[]string{"p=997333333445534.3129445454"}},
		{"#749", "wide_decimal_times_two_keeps_its_scale",
			`SELECT dw * 2 AS t FROM decwin WHERE id = 199`,
			[]string{"t=1994666666891066.6258890908"}},
		{"#749", "wide_decimal_minus_a_fraction_keeps_the_wider_scale",
			`SELECT dw - 0.5 AS d FROM decwin WHERE id = 199`,
			[]string{"d=997333333445532.8129445454"}},

		// ------------------------------------------------------------------
		// #703 — DISTINCT was mapped onto COUNT's own AggFunc and dropped for
		// every other aggregate, so SUM(DISTINCT a) was a plain SUM wearing
		// the DISTINCT spelling.
		{"#703", "distinct_over_a_decimal_column",
			`SELECT SUM(DISTINCT a) AS sd, AVG(DISTINCT a) AS ad, MIN(DISTINCT a) AS md, ` +
				`MAX(DISTINCT a) AS xd, COUNT(DISTINCT a) AS cd FROM decpair`,
			[]string{"sd=14.74|ad=3.685000|md=-0.01|xd=12.75|cd=int64:4"}},
		{"#703", "distinct_over_the_wider_decimal_column",
			`SELECT SUM(DISTINCT b) AS sb FROM decpair`,
			[]string{"sb=49.2400"}},
		// UNGROUPED and ALONE is the shape the first cut missed: an ungrouped
		// aggregate whose every function has a BATCH kernel takes the scalar
		// fast path, which folds a whole vector at a time and never consults
		// the group's set. resolveBatchAggKernel answers by FUNC, so it
		// declined COUNT(DISTINCT) — its own AggFunc — and returned SUM's for
		// SUM(DISTINCT). Each of these must therefore stand ALONE: putting a
		// COUNT(DISTINCT) beside them declines the fast path for its own
		// reason and hides the defect.
		{"#703", "distinct_sum_alone_is_ungrouped",
			`SELECT SUM(DISTINCT a) AS sd FROM decpair`, []string{"sd=14.74"}},
		{"#703", "distinct_avg_alone_is_ungrouped",
			`SELECT AVG(DISTINCT a) AS ad FROM decpair`, []string{"ad=3.685000"}},
		{"#703", "distinct_min_alone_is_ungrouped",
			`SELECT MIN(DISTINCT a) AS md FROM decpair`, []string{"md=-0.01"}},
		{"#703", "distinct_sum_over_integers_alone",
			`SELECT SUM(DISTINCT c_i32) AS s FROM typemx`, []string{"s=int64:36198630"}},
		// The DUPLICATES-ACROSS-CLONES cells. Every cell above is single-batch
		// or duplicate-free, so none of them can reach the morsel-parallel
		// clone merge: `Pipeline.runParallel` returns serially when the source
		// is exhausted after its warm-up batch, and `SUM(DISTINCT c_i32)` over
		// typemx equals `SUM(c_i32)` because that column has no duplicates.
		// revdup is 7 500 rows — four batches — with every value in every
		// batch, so the split really happens and a clone merge that ADDS two
		// accumulators holding the same value shows up as a multiple of the
		// right answer (#703, review F1: 97.44 / 129.92 / 194.88 for 16.24
		// across four runs of one binary).
		{"#703", "distinct_across_clones_ungrouped",
			`SELECT SUM(DISTINCT a) AS sd, AVG(DISTINCT a) AS ad, MIN(DISTINCT a) AS md, ` +
				`COUNT(DISTINCT a) AS cd, SUM(DISTINCT i) AS si, SUM(DISTINCT f) AS sf FROM revdup`,
			[]string{"sd=16.24|ad=5.413333|md=-0.01|cd=int64:3|si=33|sf=float:4.5"}},
		{"#703", "distinct_across_clones_grouped",
			`SELECT g AS k, SUM(DISTINCT a) AS sd, AVG(DISTINCT a) AS ad, COUNT(DISTINCT a) AS cd ` +
				`FROM revdup GROUP BY g ORDER BY k`,
			[]string{
				"k=int32:0|sd=16.24|ad=5.413333|cd=int64:3",
				"k=int32:1|sd=16.24|ad=5.413333|cd=int64:3",
			}},
		// A DISTINCT aggregate beside a plain one and a COUNT(*), which is the
		// shape a clone merge can get half right: the plain SUM must keep
		// summing every row while the distinct one dedupes.
		{"#703", "distinct_beside_a_plain_aggregate",
			`SELECT SUM(DISTINCT a) AS sd, COUNT(*) AS n, SUM(i) AS si FROM revdup`,
			[]string{"sd=16.24|n=int64:7500|si=82500"}},
		{"#703", "distinct_grouped_over_integers",
			`SELECT g AS k, SUM(DISTINCT c_i32) AS s, COUNT(DISTINCT c_i32) AS c ` +
				`FROM typemx WHERE id < 40 GROUP BY g ORDER BY k`,
			// Sorted as TEXT by na2Run, so the NULL key leads.
			[]string{
				"k=NULL|s=int64:225|c=int64:3",
				"k=int32:0|s=int64:231|c=int64:5",
				"k=int32:1|s=int64:333|c=int64:6",
				"k=int32:2|s=int64:351|c=int64:6",
				"k=int32:3|s=int64:255|c=int64:5",
				"k=int32:4|s=int64:312|c=int64:5",
				"k=int32:5|s=int64:249|c=int64:4",
				"k=int32:6|s=int64:300|c=int64:5",
			}},
		{"#703", "distinct_over_a_float_column",
			`SELECT SUM(DISTINCT f) AS sf, AVG(DISTINCT f) AS af FROM decpair`,
			[]string{"sf=float:138.75|af=float:17.3438"}},
		{"#703", "distinct_over_text",
			`SELECT COUNT(DISTINCT s) AS cs, MIN(DISTINCT s) AS ms, MAX(DISTINCT s) AS xs FROM decpair`,
			[]string{"cs=int64:8|ms=-1|xs=abc"}},
		// PostgreSQL SORTS a DISTINCT string_agg — it has to, because the
		// dedup is a sort — while a plain one keeps arrival order, which is
		// unspecified (ADR-0013 class 1). Emitting first-seen order for the
		// DISTINCT form was a value divergence with a definite PostgreSQL
		// answer.
		{"#703", "distinct_string_agg_is_sorted",
			`SELECT STRING_AGG(DISTINCT s, ',') AS sa FROM decpair`,
			[]string{"sa=-1,0,1.5,1.50,1.500,10,9,abc"}},

		// ------------------------------------------------------------------
		// #784 — SUM/AVG over integers answer PostgreSQL's TYPES, taken from
		// the live server: pg_typeof(sum(int4)) is bigint, and
		// pg_typeof(sum(int8)) / pg_typeof(avg(int*)) are numeric. The bigsum
		// fixture is what makes the rules distinguishable — every value is
		// past 2^53, so a float64 accumulator drops integer digits, and the
		// total is EXACTLY 2^64, so an int64 one wraps to zero.
		{"#784", "sum_of_int32_is_bigint",
			`SELECT SUM(c_i32) AS s FROM typemx`, []string{"s=int64:36198630"}},
		{"#784", "sum_of_int64_is_numeric",
			`SELECT SUM(c_i64) AS s FROM typemx`, []string{"s=12093426280170"}},
		// AVG's SCALE is batch.AvgScale(0) = 4 where PostgreSQL's own
		// division scale renders 16 digits here and 8 for the wider column —
		// magnitude-dependent, which ADR-0024 declined to adopt. Both are
		// exact to the digits they keep and agree to min(scale): ADR-0012
		// item 9's class.
		{"#784", "avg_of_int32_is_numeric",
			`SELECT AVG(c_i32) AS a FROM typemx`, []string{"a=7497.6450"}},
		{"#784", "avg_of_int64_is_numeric",
			`SELECT AVG(c_i64) AS a FROM typemx`, []string{"a=2499158148.4129"}},
		{"#784", "sum_past_int64_is_exact",
			`SELECT SUM(b) AS s, COUNT(b) AS c FROM bigsum`,
			[]string{"s=18446744073709551616|c=int64:7"}},
		{"#784", "avg_past_2_53_is_exact",
			`SELECT AVG(b) AS a FROM bigsum`, []string{"a=2635249153387078802.2857"}},
		{"#784", "sum_and_avg_grouped_past_int64",
			`SELECT g AS k, SUM(b) AS s, AVG(b) AS a, COUNT(b) AS c FROM bigsum GROUP BY g ORDER BY k`,
			[]string{
				"k=int32:0|s=9232379236109516801|a=3077459745369838933.6667|c=int64:3",
				"k=int32:1|s=9214364837600034815|a=2303591209400008703.7500|c=int64:4",
			}},
		// EVERY SPELLING of the same sum, because the carrier is not the same
		// for all of them and the divergence was SILENT (review round 2 F4).
		//
		// `SUM(b)` reads a BARE int8 column, which #784 sums into Int128 and
		// answers exactly as PostgreSQL's numeric. `SUM(b + 0)` and
		// `SUM(b * 1)` are constant-folded back to that bare column before the
		// aggregate is built, so they are the same question and the same
		// answer — they are here as the CONTROL that the two below are not
		// about arithmetic in the argument.
		//
		// `SUM(-b)` and the CASE spelling stay COMPUTED, and a computed
		// integer argument is declared bigint on purpose so that
		// `SUM(CASE WHEN … THEN 1 ELSE 0 END)` — TPC-H Q12's shape — keeps
		// PostgreSQL's int8 OID (physical.aggOutputFromInputDecl). PostgreSQL
		// answers -18446744073709551616 for both; wadjet's int64 carrier
		// cannot hold it. It used to answer 0, which is the same digits'
		// worth of wrong as any other wrong number: three spellings of one
		// question were right and two were zero. Now the carrier is CHECKED
		// and the query fails at 22003 (ADR-0012 item 9 — a wrapped sum is a
		// different number wearing the right type).
		//
		// This is a REFUSAL, not an answer, and it is deliberately not
		// symmetric with the three above: the alternative — routing every
		// computed integer argument through the exact carrier — makes Q12's
		// sum of ones numeric where PostgreSQL says bigint, trading a loud
		// failure on a shape no data reaches for a wrong OID on the shape
		// every BI tool sends.
		{"#784", "sum_of_a_folded_zero_add_is_the_bare_column",
			`SELECT SUM(b + 0) AS s FROM bigsum`, []string{"s=18446744073709551616"}},
		{"#784", "sum_of_a_folded_one_multiply_is_the_bare_column",
			`SELECT SUM(b * 1) AS s FROM bigsum`, []string{"s=18446744073709551616"}},
		{"#784", "sum_of_a_bare_column_through_a_projection",
			`SELECT SUM(b) + 0 AS s FROM bigsum`, []string{"s=18446744073709551616"}},

		// The rules #784 does NOT change, on the same fixture: SUM over the
		// int32 class is bigint, and MIN/MAX keep the input's own type.
		{"#784", "sum_of_int32_beside_min_max",
			`SELECT SUM(g) AS sg, MIN(b) AS mn, MAX(b) AS mx FROM bigsum`,
			[]string{"sg=int64:4|mn=int64:-9007199254740993|mx=int64:9223372036854775807"}},
		// SUM over an INT32 column that leaves INT32 — the other half of the
		// same rule, and the half the batch kernel got wrong (review round 3,
		// F1). `sumSlice` was generic over the COLUMN's width, so the INT32
		// arm summed each batch in int32 and widened afterwards: four rows of
		// 2 000 000 000 answered -589934592 for PostgreSQL's 8000000000. The
		// GROUPED form was right the whole time (the flat scatter has always
		// used an int64 array) and so was the row path, so the same query had
		// two answers depending on the shape it took.
		{"#784", "sum_of_int32_past_int32_is_bigint",
			`SELECT SUM(w) AS s FROM i32wide`, []string{"s=int64:6000000000"}},
		{"#784", "sum_of_int32_past_int32_filtered",
			`SELECT SUM(w) AS s FROM i32wide WHERE g < 2`, []string{"s=int64:8000000000"}},
		{"#784", "sum_of_int32_past_int32_grouped",
			`SELECT g AS k, SUM(w) AS s, COUNT(w) AS c FROM i32wide GROUP BY g ORDER BY k`,
			[]string{
				"k=int32:0|s=int64:4000000000|c=int64:2",
				"k=int32:1|s=int64:4000000000|c=int64:2",
				"k=int32:2|s=int64:-2000000000|c=int64:1",
			}},
		{"#784", "avg_of_int32_past_int32_is_numeric",
			`SELECT AVG(w) AS a, COUNT(w) AS c FROM i32wide`,
			[]string{"a=1200000000.0000|c=int64:5"}},
		{"#784", "sum_of_a_negated_int32_past_int32",
			`SELECT SUM(-w) AS s FROM i32wide`, []string{"s=int64:-6000000000"}},
		{"#784", "sum_of_an_int32_case_arm_past_int32",
			`SELECT SUM(CASE WHEN w IS NULL THEN 0 ELSE w END) AS s FROM i32wide`,
			[]string{"s=int64:6000000000"}},
		// MIN/MAX keep the VALUE PostgreSQL gives; the BOX is int64 where
		// PostgreSQL keeps int4, on all five arms alike. That is ADR-0024's
		// recorded widening — wadjet declares every integer INT64 — and not a
		// two-path defect, which is why it is asserted here rather than left
		// unstated: the cell fails the day one arm disagrees with the others.
		{"#784", "min_max_of_int32_keep_their_value_in_the_wider_box",
			`SELECT MIN(w) AS mn, MAX(w) AS mx FROM i32wide`,
			[]string{"mn=int64:-2000000000|mx=int64:2000000000"}},

		// EMPTY SCAN TASKS. typemx is written as four chunks, so a selective
		// predicate leaves some scan task with no rows — and a task that saw no
		// batch declares its partial from the SPEC while its siblings declare
		// from the vector they observed. When those two disagree, ADR-0010's
		// shuffle type guard refuses the read, which is what `AVG(c_i32) WHERE
		// id < 3` did on both DAG arms while the single-process path answered
		// (review round 2 B2). The carrier is decided at plan time from the
		// declaration now, so both agree.
		{"#784", "avg_int32_with_empty_scan_tasks",
			`SELECT AVG(c_i32) AS a FROM typemx WHERE id < 3`, []string{"a=3.0000"}},
		{"#784", "avg_int32_with_empty_scan_tasks_wider",
			`SELECT AVG(c_i32) AS a FROM typemx WHERE id < 100`, []string{"a=147.8041"}},
		{"#784", "avg_int32_beside_count_with_empty_scan_tasks",
			`SELECT AVG(c_i32) AS a, COUNT(*) AS n FROM typemx WHERE id < 3`,
			[]string{"a=3.0000|n=int64:3"}},
		{"#784", "avg_int64_with_empty_scan_tasks",
			`SELECT AVG(c_i64) AS a FROM typemx WHERE id < 3`, []string{"a=1000003.0000"}},
		{"#784", "sum_int32_with_empty_scan_tasks",
			`SELECT SUM(c_i32) AS s FROM typemx WHERE id < 3`, []string{"s=int64:9"}},
		{"#784", "avg_int32_grouped_with_empty_scan_tasks",
			`SELECT g AS k, AVG(c_i32) AS a FROM typemx WHERE id < 3 GROUP BY g ORDER BY k`,
			[]string{"k=int32:0|a=0.0000", "k=int32:1|a=3.0000", "k=int32:2|a=6.0000"}},
		// The same family reached through a RENAME, with and without a shadow.
		{"#784", "avg_int32_through_a_rename",
			`SELECT AVG(v) AS a FROM (SELECT c_i32 AS v, c_dec AS other FROM typemx WHERE id < 8) x`,
			[]string{"a=10.5000"}},
		{"#784", "avg_int32_through_a_shadowing_rename",
			`SELECT AVG(c_dec) AS a FROM (SELECT c_i32 AS c_dec, c_dec AS other FROM typemx WHERE id < 8) x`,
			[]string{"a=10.5000"}},
		{"#784", "avg_int32_through_an_int64_shadow",
			`SELECT AVG(c_i64) AS a FROM (SELECT c_i32 AS c_i64, c_i64 AS other FROM typemx WHERE id < 8) x`,
			[]string{"a=10.5000"}},
		// A COMPUTED and a RENAMED argument carry the declaration to the DAG
		// too: the VALUE was right on both paths and the Go BOX — the wire OID a
		// client reads — was int64 on one and float on the other (F3).
		{"#784", "sum_of_a_computed_int32_is_bigint",
			`SELECT SUM(c_i32 * 2) AS s FROM typemx`, []string{"s=int64:72397260"}},
		{"#784", "sum_of_a_computed_int32_through_a_rename",
			`SELECT SUM(v*2) AS s FROM (SELECT c_i32 AS v FROM typemx WHERE id < 8) x`,
			[]string{"s=int64:168"}},
		{"#784", "sum_over_no_non_null_rows_is_null",
			`SELECT SUM(b) AS s, AVG(b) AS a FROM bigsum WHERE b IS NULL`,
			[]string{"s=NULL|a=NULL"}},

		// ------------------------------------------------------------------
		// #696 — a scalar subquery's value in an outer comparison, and it was
		// TWO mechanisms. The single-process path had no DECLARATION for the
		// subquery operand, so a DECIMAL column compared against it by the
		// BYTES of its rendered text and `"12.75" > "7.570000"` was false; the
		// stage DAG substituted the value's UNSCALED Int128 (7570000 for
		// 7.570000), so the threshold was 10^scale out. Equality survived on
		// the first only where the two scales rendered identically.
		{"#696", "decimal_column_gt_scalar_avg",
			`SELECT COUNT(*) AS n FROM decpair WHERE a > (SELECT AVG(a) FROM decpair)`,
			[]string{"n=int64:4"}},
		{"#696", "decimal_column_gt_scalar_min",
			`SELECT COUNT(*) AS n FROM decpair WHERE a > (SELECT MIN(a) FROM decpair)`,
			[]string{"n=int64:6"}},
		{"#696", "decimal_column_eq_scalar_max",
			`SELECT COUNT(*) AS n FROM decpair WHERE a = (SELECT MAX(a) FROM decpair)`,
			[]string{"n=int64:4"}},
		{"#696", "decimal_column_lt_scalar_avg",
			`SELECT COUNT(*) AS n FROM decpair WHERE a < (SELECT AVG(a) FROM decpair)`,
			[]string{"n=int64:3"}},
		// BETWEEN two scalar subqueries and `> (SELECT 9.56)` are NOT here,
		// and the reason is recorded rather than left as a gap: both are
		// PRE-EXISTING stage-DAG refusals unrelated to #696 — the worker's
		// filter compiler has no SubqueryRunner ("subqueries require a
		// SubqueryRunner") and a FROM-less scalar produces a `dual` stage with
		// no dependencies and no ScanFiles. Both reproduce on 74705d11. The
		// single-process arm answers 3 and 4, which is PostgreSQL's.
		{"#696", "wider_decimal_column_gt_its_own_avg",
			`SELECT COUNT(*) AS n FROM decpair WHERE b > (SELECT AVG(b) FROM decpair)`,
			[]string{"n=int64:4"}},
		// The value itself, projected. PostgreSQL renders 7.5700000000000000
		// at its own division scale; wadjet's AVG scale is s+4, so this is the
		// same number to the digits both keep (ADR-0012 item 9).
		{"#696", "scalar_subquery_projected",
			`SELECT (SELECT AVG(a) FROM decpair) AS av FROM decpair WHERE id = 1`,
			[]string{"av=7.570000"}},
		// The regression #784 would otherwise have introduced: SUM over an
		// INT64 column is numeric now, so a HAVING comparing it against a
		// scalar takes the same boxed path a DECIMAL column does. On the base
		// commit this answered 8 for PostgreSQL's 0.
		// The same question in a PROJECTION rather than in a WHERE. The
		// declaration seam reached the filter compile and not the aggregate
		// ARGUMENT compile, so `SUM(CASE WHEN a > (SELECT AVG(a)) …)` answered 0
		// on every arm while the WHERE spelling answered 4 — two spellings of one
		// question, two numbers, which is the defect shape this arc is about
		// (review round 2 F6). Both spellings take the option now.
		{"#696", "scalar_subquery_in_an_aggregate_argument",
			`SELECT SUM(CASE WHEN a > (SELECT AVG(a) FROM decpair) THEN 1 ELSE 0 END) AS n FROM decpair`,
			[]string{"n=int64:4"}},
		// The CORRELATED twin (#666): the subquery re-runs per row and its
		// declared TYPE does not change with the row, so it takes the same
		// declaration. It answered 0 for PostgreSQL's 4 on every arm, on base and
		// through round 2, for the same lexicographic reason.
		{"#696", "correlated_scalar_subquery_against_a_decimal_column",
			`SELECT COUNT(*) AS n FROM decpair d WHERE d.a > ` +
				`(SELECT AVG(x.a) FROM decpair x WHERE x.id <> d.id)`,
			[]string{"n=int64:4"}},
		{"#696", "integer_sum_in_having_against_a_scalar",
			`SELECT COUNT(*) AS n FROM (SELECT g, SUM(id) AS s FROM typemx WHERE id < 100 ` +
				`GROUP BY g HAVING SUM(id) > (SELECT SUM(id) * 0.4 FROM typemx WHERE id < 100)) x`,
			[]string{"n=int64:0"}},

		// ------------------------------------------------------------------
		// #775 — a COMPUTED aggregate argument over a DECIMAL WINDOW output.
		// The single-process half closed with #728; the DAG half was a hard
		// failure at the #361 store guard on both arms, and the review round-2
		// measurement narrowed it to exactly this: any arithmetic (`w*2`, `w+1`)
		// over any DECIMAL window aggregate (SUM/AVG/MAX OVER), with the FLOAT
		// twin passing — so it is the (p,s) of a window output used as an
		// aggregate INPUT, not the slot and not the multiplication. The bare
		// argument (`SUM(w)`) always answered and is kept here as the control
		// that says which half moved.
		// The FLOAT twin, which passed throughout: it is the control that makes
		// the three above a statement about the (p,s) and not about windows.
		{"#775", "sum_over_a_float_window_output",
			`SELECT SUM(w*2) AS s FROM (SELECT id, SUM(f) OVER () AS w FROM decpair) x`,
			[]string{"s=float:2497.5"}},
		{"#775", "sum_of_a_bare_decimal_window_output",
			`SELECT SUM(w) AS s FROM (SELECT id, SUM(a) OVER () AS w FROM decpair) x`,
			[]string{"s=476.91"}},

		// ------------------------------------------------------------------
		// #704 — an integer column against a NON-INTEGRAL numeric literal.
		// The filter kernels read the constant with `int64(float)`, which
		// TRUNCATES toward zero, so `= 3.5` matched the row holding 3 and
		// `= -0.5` matched the row holding 0. The typemx measurement in the
		// arc brief read 0 for the INT64 column only because no row of that
		// column holds 3; `c_i64 = 1000003.5` matched one.
		{"#704", "int32_eq_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 = 3.5`, []string{"n=int64:0"}},
		{"#704", "int32_ne_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 <> 3.5`, []string{"n=int64:4828"}},
		{"#704", "int32_ne_a_negative_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 <> -0.5`, []string{"n=int64:4828"}},
		{"#704", "int64_not_in_a_fraction_list",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 NOT IN (3.5, 99.5)`, []string{"n=int64:4839"}},
		{"#704", "int32_in_a_fraction_list",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 IN (3.5, 99.5)`, []string{"n=int64:0"}},
		{"#704", "int32_eq_a_negative_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 = -0.5`, []string{"n=int64:0"}},
		{"#704", "int32_gt_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 > 3.5`, []string{"n=int64:4826"}},
		{"#704", "int32_le_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 <= 3.5`, []string{"n=int64:2"}},
		{"#704", "int32_lt_a_negative_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 < -0.5`, []string{"n=int64:0"}},
		{"#704", "int64_eq_a_fraction_a_row_would_truncate_onto",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 = 1000003.5`, []string{"n=int64:0"}},
		{"#704", "int64_ge_a_fraction",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i64 >= 3.5`, []string{"n=int64:4838"}},
		{"#704", "int32_eq_an_integral_literal_still_matches",
			`SELECT COUNT(*) AS n FROM typemx WHERE c_i32 = 3.0`, []string{"n=int64:1"}},
	} {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			// The pressured arm runs with the DRAIN FORCED and the reference
			// arm disarmed, which is ADR-0027 §6's protocol: a 512 KiB budget
			// alone moved no engagement counter on ANY of these shapes (they
			// are 9 to 12 500 rows), so without the knob the arm was a second
			// copy of `single`. Arming both sides would cancel a defect that
			// lives in the drain (#790), so only this one is armed.
			drained := func(sql string) ([]string, error) {
				beforeDrain := exec.ForcedAggDrains.Load()
				beforeRaw := exec.RawRowSpillFiles.Load()
				restore := exec.ForceAggDrainEvery(1)
				// The RUN FLOOR comes down with the drain knob, because the
				// class that cannot take the partial-state drain takes the
				// LEGACY raw-row spill instead and a 12 500-row fixture never
				// crosses its default target. Both are on the pressured arm
				// only; the reference stays disarmed (ADR-0027 §6).
				restoreRuns := exec.ForceSmallSpillRuns(512)
				out, err := na2Run(tmdRunSingle(ctx, spilled, sql))
				restoreRuns()
				exec.ForceAggDrainEvery(restore)
				fired := exec.ForcedAggDrains.Load() > beforeDrain
				if fired {
					na2Drains.Add(1)
				}
				na2Engaged.Store(tc.name, na2Cell{
					sql:     tc.sql,
					drained: fired,
					rawRows: exec.RawRowSpillFiles.Load() > beforeRaw,
				})
				return out, err
			}
			for _, arm := range []struct {
				name string
				run  func(string) ([]string, error)
			}{
				{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
				{"single+budget+forced-drain", drained},
				{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
				{"dag+broadcast", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
				{"dag+morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
			} {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  PostgreSQL 17: %v", arm.name, err, tc.sql, tc.want)
					continue
				}
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v (live PostgreSQL 17)\n  SQL: %s",
						arm.name, len(got), len(tc.want), got, tc.want, tc.sql)
					continue
				}
				for i := range got {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm: row %d\n  got  %s\n  want %s (live PostgreSQL 17)\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
						break
					}
				}
			}
		})
	}
	na2CheckEngagement(t)
}

// na2Engaged is the PER-CELL spill ledger: cell name → the cell's SQL and
// whether the forced aggregate drain fired on its pressured arm.
//
// ADR-0027 §5 asks for more than a non-zero count — "cells that cannot spill
// are named with their reason" — and the count was hiding a real number here.
// The forced drain fires on FIVE of this census's cells; the rest compared two
// in-memory runs, which is worth knowing and was invisible behind "the forced
// drain fired on 5 cells" (review round 2 F8).
var na2Engaged sync.Map

type na2Cell struct {
	sql     string
	drained bool
	rawRows bool
}

// na2NoDrainReason says why a shape cannot engage the forced aggregate drain,
// or "" when it must engage. Two structural classes cover every quiet cell in
// this census, and naming the CLASS rather than listing seventy-odd cell names
// is what keeps the ledger a ratchet: a cell in neither class that stops
// draining is an unnamed silent in-memory comparison and fails.
//
//   - UNGROUPED. An ungrouped aggregate's whole state is one row of
//     accumulators plus extra state, so there is nothing to drain and no
//     partial-state run to write — ADR-0027 decision 4, which removed the
//     input buffer these shapes used to keep. Most of this census is scalar
//     aggregates and COUNT(*) filters, so most of it is here.
//   - GROUPED with a DISTINCT aggregate. The per-group value sets are extra
//     state the partial-state run format does not carry, so
//     exec.canUseExternalMerge declines and the operator takes the LEGACY
//     raw-row spill instead. That path does spill — it is simply not the drain
//     exec.ForcedAggDrains counts, so this knob cannot make it fire. This class
//     is PROVEN rather than excused: na2CheckEngagement requires those cells to
//     move exec.RawRowSpillFiles, so "it spills elsewhere" is an assertion and
//     not a claim in a comment (review round 3, F5).
//
// A cell that is grouped, carries no DISTINCT, and still does not drain has
// lost its pipeline breaker, and naming that is the point of returning "".
func na2NoDrainReason(sql string) string {
	u := strings.ToUpper(sql)
	if !strings.Contains(u, "GROUP BY") {
		return "ungrouped: one row of accumulators, nothing to drain (ADR-0027 decision 4)"
	}
	if strings.Contains(u, "(DISTINCT ") {
		return "grouped DISTINCT: extra state the partial-state run cannot carry, so the " +
			"operator takes the legacy raw-row spill, which this knob does not reach"
	}
	return ""
}

func na2CheckEngagement(t *testing.T) {
	t.Helper()
	if na2Drains.Load() == 0 {
		t.Error("the pressured arm drained on NO cell, so it compared in-memory runs and " +
			"proves nothing (ADR-0027 §5). Either the forcing knob stopped reaching the " +
			"aggregate or every shape lost its pipeline breaker.")
		return
	}
	var unnamed, unproven []string
	total, named, proven := 0, 0, 0
	na2Engaged.Range(func(k, v any) bool {
		name, cell := k.(string), v.(na2Cell)
		total++
		why := na2NoDrainReason(cell.sql)
		switch {
		case cell.drained && why != "":
			t.Errorf("cell %q DID drain and na2NoDrainReason says it cannot (%s). "+
				"The classification is stale (ADR-0027 §5).", name, why)
		case !cell.drained && why == "":
			unnamed = append(unnamed, name)
		case !cell.drained:
			named++
			// Class 2 says the shape spills SOMEWHERE ELSE. That is a claim
			// about behaviour, so it is asserted: the cell must have written a
			// raw-row spill file.
			if strings.HasPrefix(why, "grouped DISTINCT") {
				if cell.rawRows {
					proven++
				} else {
					unproven = append(unproven, name)
				}
			}
		}
		return true
	})
	if len(unproven) > 0 {
		sort.Strings(unproven)
		t.Errorf("%d grouped-DISTINCT cells are excused as taking the legacy raw-row "+
			"spill and wrote NO raw-row file: %v\nEither they spill nowhere — in which "+
			"case that arm compared two in-memory runs — or the class is wrong "+
			"(ADR-0027 §5).", len(unproven), unproven)
	}
	if len(unnamed) > 0 {
		sort.Strings(unnamed)
		t.Errorf("the pressured arm did NOT drain on %d cells and nothing says why: %v\n"+
			"A grouped shape with no DISTINCT that does not drain has lost its pipeline "+
			"breaker, and that arm then compared two in-memory runs (ADR-0027 §5).",
			len(unnamed), unnamed)
	}
	t.Logf("pressured arm: the forced drain fired on %d of %d cells; %d cannot drain, "+
		"by class, of which %d are PROVEN to spill on the raw-row path",
		na2Drains.Load(), total, named, proven)
}

// na2Drains counts the census cells whose pressured arm actually drained. The
// test fails if it is zero: ADR-0027 §5 says a spill gate proves it spilled,
// and the first cut of this file did not — a 512 KiB budget moved no
// engagement counter on any of its shapes, so the arm was a second copy of
// `single` wearing a spill label.
var na2Drains atomic.Int64

// na2Standalone is tmdStandalone with a per-query memory budget. The budget
// alone is not what makes the arm pressured — the census's cells are 9 to
// 12 500 rows and none of them crosses it — so the arm ARMS
// exec.ForceAggDrainEvery(1) around each run and asserts, once for the whole
// gate, that the drain really fired (ADR-0027 §§5-6).
func na2Standalone(t *testing.T, ctx context.Context, budget int64) *wadjet.DB {
	t.Helper()
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: objstore.NewMemStore(), Bucket: "test",
		MemoryBudget: budget, SpillDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("open budgeted standalone: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	for _, tbl := range tmdTables() {
		if tmdStoresAReservedName(tbl) {
			continue // the reserved-name fixtures need the catalog door; no shape here reads them
		}
		if err := db.CreateTable(ctx, tbl.name, tbl.schema, nil); err != nil {
			t.Fatalf("create %s: %v", tbl.name, err)
		}
		ing := db.NewIngester(tbl.name, tbl.schema, nil, ingest.Config{
			MaxBufferRows: len(tbl.rows) + 1, RowGroupSize: typematrix.RowGroup,
		})
		if err := ing.Ingest(ctx, tbl.rows); err != nil {
			t.Fatalf("ingest %s: %v", tbl.name, err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatalf("flush %s: %v", tbl.name, err)
		}
	}
	return db
}

// na2Run renders a result to comparable text.
//
// A DECIMAL is rendered VERBATIM — it arrives as its own text and every issue
// here is about a digit past the sixth, so rounding it would hide the defect.
// A FLOAT is rendered to six significant digits, which is ADR-0013's
// nondeterminism class 9: a float sum's last digits move with the order three
// workers hand batches to the aggregate. The Go TYPE is printed for every
// non-string box, because a float64 holding an exact integer and an int64
// holding it print identically under %v — and "the right number under the
// wrong Go type" is exactly what #728 and #784 are.
func na2Run(res *oracle.Result, err error) ([]string, error) {
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(res.Rows))
	for _, r := range res.Rows {
		parts := make([]string, 0, len(res.Columns))
		for _, c := range res.Columns {
			v := r[c]
			switch t := v.(type) {
			case nil:
				parts = append(parts, c+"=NULL")
			case string:
				parts = append(parts, c+"="+t)
			case float64:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, t))
			case float32:
				parts = append(parts, fmt.Sprintf("%s=float:%.6g", c, float64(t)))
			default:
				parts = append(parts, fmt.Sprintf("%s=%T:%v", c, v, v))
			}
		}
		out = append(out, strings.Join(parts, "|"))
	}
	// Every shape above either carries an ORDER BY or returns one row, so the
	// sort only makes an unordered multiset comparison total.
	sort.Strings(out)
	return out, nil
}
