package coordinator

import (
	"context"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// THE TCP FLAG FAMILY ON EVERY ARM (#966).
//
// A scalar predicate looks like the last thing that needs five arms. It is not:
// the family's whole reason to exist is that it is PUSHED INTO THE SCAN, and
// the scan runs in a different process on the DAG arms, under a plan the
// coordinator built and a fragment the worker rebuilt. The question these
// answer is whether the pushed predicate and the residual one agree once they
// are on opposite sides of a task boundary — and whether a per-row REFUSAL
// (an unknown flag name) reaches the client from inside a worker rather than
// stalling the query.
//
// The `bitwise_and_*` cells reproduce this arc's Round-0 finding on every arm:
// BITWISE_AND carried its operands through a float64, so (2^62|18) & 18
// answered 0 where PostgreSQL answers 18. They fail on every arm at base.
//
// Every expectation is PostgreSQL 17.11's answer for the equivalent bit
// spelling over the same rows, measured on the shared oracle server.
func TestTheTCPFlagFamilyAnswersPostgresBitArithmetic(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	// The pushdown is on for every arm here; the kill-switch half of the
	// contract is wadjet.TestAFlagPredicateReachesTheScanAndTheAnswerDoesNot
	// DependOnIt, which measures the counters a distributed arm cannot read.
	prevFlag := scan.FlagDictPushdown.Set(true)
	t.Cleanup(func() { scan.FlagDictPushdown.Set(prevFlag) })

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
	// and the reference arms disarmed — ADR-0027 §6's protocol. A 512 KiB
	// budget alone moves NO engagement counter on any shape here (the fixture
	// is 60 rows), so without the knob this arm was a second copy of `single`
	// wearing a spill label: #966 round 2 P1 measured zero spill files across
	// eighteen projections. Arming both sides would cancel a defect that lives
	// in the drain (#790), so only this one is armed.
	budgeted := func(name, sql string) ([]string, error) {
		beforeDrain := exec.ForcedAggDrains.Load()
		beforeRaw := exec.RawRowSpillFiles.Load()
		beforeSort := exec.SortRunsWritten.Load()
		beforeWin := exec.WindowRunsWritten.Load()
		restoreDrain := exec.ForceAggDrainEvery(1)
		restoreRuns := exec.ForceSmallSpillRuns(512)
		out, err := na2Run(tmdRunSingle(ctx, spilled, sql))
		restoreRuns()
		exec.ForceAggDrainEvery(restoreDrain)
		engaged := exec.ForcedAggDrains.Load() > beforeDrain ||
			exec.RawRowSpillFiles.Load() > beforeRaw ||
			exec.SortRunsWritten.Load() > beforeSort ||
			exec.WindowRunsWritten.Load() > beforeWin
		if engaged {
			a2fSpills.Add(1)
		}
		a2fEngaged.Store(name, engaged)
		return out, err
	}

	arms := []struct {
		name string
		// coord is the coordinator whose LOCAL-ROUTING counters this arm's
		// dispositions are read from, and nil on the single-process arms.
		// Rows alone cannot tell "executed on the DAG" from "refused and
		// routed local" — #966 round 2 P1: the durable census never read a
		// routing counter, so every DAG claim in it was unpoliced.
		coord *Coordinator
		run   func(name, sql string) ([]string, error)
	}{
		{"single", nil, func(_, sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget+forced-drain", nil, budgeted},
		{"dag", coord, func(_, sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", coordB, func(_, sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag-morsel4", coordM, func(_, sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
	}

	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		// ---- the three predicates, both widths. PostgreSQL over tcpflow's
		// 60 rows (four repeats of the fourteen values, plus a NULL),
		// measured on the shared oracle server over `a2_tcpflow2`:
		//   (f & 18) = 18  -> 20
		//   (f & 18) <> 0  -> 40
		//   (f & 18) = 0   -> 16
		// Both columns answer the same three numbers, which is what carrying
		// the wide value and both extremes at each width is for.
		{"has_all_int64", `SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_all(f8,'SYN','ACK')`,
			[]string{"n=int64:20"}},
		{"has_all_int32", `SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_all(f4,'SYN','ACK')`,
			[]string{"n=int64:20"}},
		{"has_any_int64", `SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_any(f8,'SYN','ACK')`,
			[]string{"n=int64:40"}},
		{"has_any_int32", `SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_any(f4,'SYN','ACK')`,
			[]string{"n=int64:40"}},
		{"has_none_int64", `SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_none(f8,'SYN','ACK')`,
			[]string{"n=int64:16"}},
		{"has_none_int32", `SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_none(f4,'SYN','ACK')`,
			[]string{"n=int64:16"}},

		// ---- the BITWISE_AND spelling, which is the oracle and the second
		// thing the pushdown recognizes. `(2^62|18) & 18 = 18` is `t` on
		// PostgreSQL; a float64 carrier answers 0 and drops four rows.
		{"bitwise_and_all", `SELECT COUNT(*) AS n FROM tcpflow WHERE BITWISE_AND(f8,18) = 18`,
			[]string{"n=int64:20"}},
		{"bitwise_and_any", `SELECT COUNT(*) AS n FROM tcpflow WHERE BITWISE_AND(f8,18) <> 0`,
			[]string{"n=int64:40"}},
		{"bitwise_and_none", `SELECT COUNT(*) AS n FROM tcpflow WHERE BITWISE_AND(f8,18) = 0`,
			[]string{"n=int64:16"}},
		// The projected value past 2^53, which is the Round-0 finding itself:
		// PostgreSQL answers 18 and a double answers 0.
		{"bitwise_and_wide_value", `SELECT BITWISE_AND(f8,18) AS v FROM tcpflow WHERE id = 10`,
			[]string{"v=int64:18"}},
		{"bitwise_or_wide_value", `SELECT BITWISE_OR(f8,1) AS v FROM tcpflow WHERE id = 10`,
			[]string{"v=int64:4611686018427387923"}},

		// ---- the renderers, which cross a task boundary as TEXT.
		{"flags_text", `SELECT tcp_flags_text(f8) AS s FROM tcpflow WHERE id = 3`,
			[]string{"s=SYN|ACK"}},
		{"flags_text_grouped",
			`SELECT tcp_flags_text(f8) AS s, COUNT(*) AS n FROM tcpflow
			 WHERE f8 IS NOT NULL AND tcp_flags_has_any(f8,'SYN','ACK') GROUP BY 1 ORDER BY 1`,
			// na2Run renders the columns in SELECT order and joins with "|",
			// which is also this function's own separator — so a row reads
			// `s=SYN|ACK|n=int64:8`. Left as it renders rather than tidied:
			// the six groups and their counts are the assertion, and the
			// grouping is over the rendered TEXT, which is what has to
			// survive a shuffle.
			[]string{
				"s=ACK|n=int64:4",
				"s=FIN|SYN|RST|PSH|ACK|URG|ECE|CWR|AE|n=int64:12",
				"s=PSH|ACK|n=int64:4",
				"s=RST|ACK|n=int64:4",
				"s=SYN|ACK|n=int64:8",
				"s=SYN|RST|PSH|URG|ECE|CWR|AE|n=int64:4",
				"s=SYN|n=int64:4",
			}},
		{"flag_mask", `SELECT tcp_flag_mask('SYN','ACK') AS m FROM tcpflow WHERE id = 1`,
			[]string{"m=int32:18"}},
		{"legacy_to_string_keeps_the_ninth_bit",
			`SELECT tcp_flags_to_string(f8) AS s FROM tcpflow WHERE id = 9`,
			[]string{"s=AE"}},

		// ---- NULL is NULL on every arm, and matches none of the three.
		{"null_flags_project", `SELECT tcp_flags_has_all(f8,'SYN') AS b FROM tcpflow WHERE id = 15`,
			[]string{"b=NULL"}},
		{"null_flags_has_none", `SELECT COUNT(*) AS n FROM tcpflow WHERE f8 IS NULL AND tcp_flags_has_none(f8,'ACK')`,
			[]string{"n=int64:0"}},
		// The other side of P4: a NULL *name* is a NULL mask operand, and
		// PostgreSQL's `NULL & NULL` is NULL — so this ANSWERS rather than
		// refusing, on the same NULL row the refusal above fires on.
		{"null_flag_name_is_null_not_a_refusal",
			`SELECT has_tcp_flag(f8, CAST(NULL AS VARCHAR)) AS b FROM tcpflow WHERE id = 15`,
			[]string{"b=NULL"}},

		// ---- the predicate BESIDE a range on another column, so the flag
		// conjunct rides in the same pushed set as a prunable one.
		{"flag_beside_a_range",
			`SELECT COUNT(*) AS n FROM tcpflow WHERE id >= 16 AND tcp_flags_has_any(f8,'SYN','ACK')`,
			[]string{"n=int64:30"}},

		// ---- NEGATIVE and EXTREME values, which is the property a magnitude
		// reading of a flags field breaks and which nothing here carried
		// before round 2 (#966 P3). Every expectation is PostgreSQL's for the
		// same row: `(-1) & 18 = 18`, int64 min has only its sign bit, and
		// int64 max has every bit but that one.
		{"all_bits_set_matches_everything",
			`SELECT tcp_flags_has_all(f8,'SYN','ACK') AS b FROM tcpflow WHERE id = 11`,
			[]string{"b=bool:true"}},
		{"all_bits_set_renders_every_name",
			`SELECT tcp_flags_text(f4) AS s FROM tcpflow WHERE id = 11`,
			[]string{"s=FIN|SYN|RST|PSH|ACK|URG|ECE|CWR|AE"}},
		{"int64_minimum_has_no_flag_bit",
			`SELECT tcp_flags_has_any(f8,'FIN','SYN','RST','PSH','ACK','URG','ECE','CWR','AE') AS b
			 FROM tcpflow WHERE id = 13`,
			[]string{"b=bool:false"}},
		{"int64_maximum_matches_syn_ack",
			`SELECT tcp_flags_has_all(f8,'SYN','ACK') AS b FROM tcpflow WHERE id = 14`,
			[]string{"b=bool:true"}},
		// The rest of the bitwise family over the same rows (#966 round 2 B1):
		// every one of these read its argument through a float64 at round 1.
		{"bitwise_not_of_the_int64_minimum",
			`SELECT BITWISE_NOT(f8) AS v FROM tcpflow WHERE id = 13`,
			[]string{"v=int64:9223372036854775807"}},
		{"arithmetic_shift_of_the_int64_minimum",
			`SELECT BITWISE_ARITHMETIC_SHIFT_RIGHT(f8, 1) AS v FROM tcpflow WHERE id = 13`,
			[]string{"v=int64:-4611686018427387904"}},
		{"shift_by_zero_is_the_identity",
			`SELECT BITWISE_RIGHT_SHIFT(f8, 0) AS v FROM tcpflow WHERE id = 10`,
			[]string{"v=int64:4611686018427387922"}},
		{"left_shift_of_the_wide_value",
			`SELECT BITWISE_LEFT_SHIFT(f8, 1) AS v FROM tcpflow WHERE id = 10`,
			[]string{"v=int64:-9223372036854775772"}},
		{"to_hex_of_the_int64_minimum",
			`SELECT TO_HEX(f8) AS v FROM tcpflow WHERE id = 13`,
			[]string{"v=8000000000000000"}},
		{"to_hex_of_the_wide_value",
			`SELECT TO_HEX(f8) AS v FROM tcpflow WHERE id = 10`,
			[]string{"v=4000000000000012"}},
		// TO_HEX over an INT32 COLUMN renders SIXTEEN digits, where
		// PostgreSQL's `to_hex((-1)::int4)` renders eight. Measured here on
		// all five arms rather than assumed: an INT32 column's value reaches a
		// scalar function as an int64 box, so the width the renderer can see
		// is 64 whatever the column declares. The NUMBER is the same on both
		// engines — it is the same two's complement, sign-extended — and a
		// non-negative argument renders identically. Recorded in ADR-0012;
		// this cell is what would notice if the boxing ever changed.
		{"to_hex_of_an_int32_column_is_the_sign_extended_word",
			`SELECT TO_HEX(f4) AS v FROM tcpflow WHERE id = 13`,
			[]string{"v=ffffffff80000000"}},
		{"to_hex_of_an_int32_column_negative_one",
			`SELECT TO_HEX(f4) AS v FROM tcpflow WHERE id = 11`,
			[]string{"v=ffffffffffffffff"}},
		{"to_hex_of_an_int32_column_positive_is_identical_to_postgres",
			`SELECT TO_HEX(f4) AS v FROM tcpflow WHERE id = 6`,
			[]string{"v=1ff"}},
		{"bit_count_of_all_bits_set",
			`SELECT BIT_COUNT(f8) AS v FROM tcpflow WHERE id = 11`,
			[]string{"v=int64:64"}},
		{"bit_count_of_the_int64_maximum",
			`SELECT BIT_COUNT(f8) AS v FROM tcpflow WHERE id = 14`,
			[]string{"v=int64:63"}},

		// ---- SUM OVER AN INTEGER-DECLARED FUNCTION (#966 round 2 B1).
		//
		// PostgreSQL's `f8 & k` is BIGINT and `SUM(bigint)` is NUMERIC, so
		// two rows of 2^62 add up to 9223372036854775808 and not to an
		// overflow. Here BITWISE_AND had just been declared int8 while the
		// aggregate-width walk still read an ordinary function as int4, so
		// SUM took the BIGINT accumulator and every one of these three
		// answered 22003 on every arm — a right value (the base answered
		// 2^63 as a float64) turned into a refusal.
		//
		// The grouped and the windowed spellings share the walk, so both are
		// here. The value is rendered VERBATIM by na2Run because it is a
		// DECIMAL: the digits past the fifteenth are the whole point.
		{"sum_of_a_wide_and_is_numeric",
			`SELECT SUM(BITWISE_AND(f8,4611686018427387904)) AS v FROM tcpflow
			 WHERE id = 10 OR id = 25`,
			[]string{"v=9223372036854775808"}},
		{"sum_of_a_wide_or_is_numeric",
			`SELECT SUM(BITWISE_OR(f8,1)) AS v FROM tcpflow WHERE id = 10 OR id = 25`,
			[]string{"v=9223372036854775846"}},
		{"windowed_sum_of_a_wide_or_is_numeric",
			`SELECT SUM(BITWISE_OR(f8,1)) OVER () AS v FROM tcpflow WHERE id = 10 OR id = 25`,
			[]string{"v=9223372036854775846", "v=9223372036854775846"}},
		// The GROUPED spelling, over the whole fixture, where the accumulator
		// is exercised per group rather than once. PostgreSQL's own
		// `SELECT (f8&511), SUM(f8&18), COUNT(*) … GROUP BY 1 ORDER BY 1`
		// over the same 56 non-NULL rows. This is also the census's only
		// shape with a pipeline breaker the forced drain can reach, so it is
		// what makes the budgeted arm a spilled arm.
		{"grouped_sum_of_a_mask",
			`SELECT BITWISE_AND(f8,511) AS k, SUM(BITWISE_AND(f8,18)) AS v, COUNT(*) AS n
			 FROM tcpflow WHERE f8 IS NOT NULL GROUP BY 1 ORDER BY 1`,
			// na2Run sorts the RENDERED rows, so this list is in string
			// order, not the numeric order the SQL asks for: "k=int64:20"
			// sorts before "k=int64:2|" because '0' < '|'. The comparison is
			// of the multiset of rows; the ORDER BY is there so the five arms
			// produce one, not so this list asserts it.
			[]string{
				"k=int64:0|v=0|n=int64:8",
				"k=int64:16|v=64|n=int64:4",
				"k=int64:18|v=144|n=int64:8",
				"k=int64:20|v=64|n=int64:4",
				"k=int64:24|v=64|n=int64:4",
				"k=int64:256|v=0|n=int64:4",
				"k=int64:2|v=8|n=int64:4",
				"k=int64:494|v=8|n=int64:4",
				"k=int64:4|v=0|n=int64:4",
				"k=int64:511|v=216|n=int64:12",
			}},
		// A SUM over an INT4 OPERAND, where the only thing that can be wrong
		// is the TYPE. 568 is PostgreSQL's total for `SUM(f4 & 18)` over the
		// fixture and `pg_typeof` of it is BIGINT, because `int4 & int4` is
		// integer there. The bitwise family is arithmetic for the width
		// question — it is as wide as its OPERANDS — so this engine says
		// bigint too, and na2Run renders a bigint as `int64:`.
		//
		// Round 3 read the function's RetInt64 CARRIER instead and declared
		// this numeric; the cell rendered `v=568` then, which is the same
		// digits in a DECIMAL box and a divergence from PostgreSQL's OID.
		{"sum_of_a_narrow_mask_is_bigint_like_postgres",
			`SELECT SUM(BITWISE_AND(f4,18)) AS v FROM tcpflow`,
			[]string{"v=int64:568"}},
		// id 3 and id 4 carry f4 = 18 and 16, so `18 & 18` + `16 & 18` is 34 —
		// PostgreSQL's `sum(f4 & 18)` over the same two rows, bigint.
		{"windowed_sum_of_a_narrow_mask_is_bigint",
			`SELECT SUM(BITWISE_AND(f4,18)) OVER () AS v FROM tcpflow WHERE id = 3 OR id = 4`,
			[]string{"v=int64:34", "v=int64:34"}},
		// ---- the same rule read from the FUNCTION side (#966 round 3 review
		// B1). These are functions whose PostgreSQL RESULT is `integer`, so
		// PostgreSQL's SUM of them is BIGINT — measured on 17.11 over two rows:
		//
		//   sum(regexp_count('abab','a'))    4   bigint
		//   sum(masklen('10.0.0.0/24'))     48   bigint
		//   sum(octet_length('abc'))         6   bigint
		//
		// Every one of them declares RetInt64 here, because every integer in
		// this engine computes in an int64; reading that declaration as a
		// WIDTH made all of them numeric. The width is
		// `expr.PGIntegerResultWidth`'s now, and it is PostgreSQL's.
		{"sum_of_an_int4_result_function_is_bigint",
			`SELECT SUM(REGEXP_COUNT('abab','a')) AS v FROM tcpflow WHERE id <= 2`,
			[]string{"v=int64:4"}},
		{"windowed_sum_of_an_int4_result_function_is_bigint",
			`SELECT SUM(REGEXP_COUNT('abab','a')) OVER () AS v FROM tcpflow WHERE id <= 2`,
			[]string{"v=int64:4", "v=int64:4"}},
		{"sum_of_a_prefix_length_is_bigint",
			`SELECT SUM(PREFIX_LENGTH('10.0.0.0/24')) AS v FROM tcpflow WHERE id <= 2`,
			[]string{"v=int64:48"}},
		{"sum_of_a_payload_length_is_bigint",
			`SELECT SUM(PAYLOAD_LENGTH('abc')) AS v FROM tcpflow WHERE id <= 2`,
			[]string{"v=int64:6"}},
		// The control the width rule needs from the other side: LENGTH is
		// declared INT32 and its PostgreSQL result is `integer`, so its SUM
		// keeps the BIGINT accumulator, exactly as PostgreSQL's
		// `SUM(length(text))` is bigint.
		{"sum_over_an_int4_declared_function_stays_bigint",
			`SELECT SUM(LENGTH(TO_HEX(f8))) AS v FROM tcpflow WHERE id <= 4`,
			[]string{"v=int64:6"}},
		// And an int8-RESULT function stays numeric: BIT_COUNT is PostgreSQL's
		// own and PostgreSQL declares it BIGINT, so `sum(bit_count(…))` is
		// numeric there — which is why this cell renders without an `int64:`
		// prefix while the four above do. PostgreSQL over two rows:
		// `sum(bit_count('\x4000000000000012'::bytea))` is 6, numeric.
		{"sum_of_an_int8_result_function_is_numeric",
			`SELECT SUM(BIT_COUNT(4611686018427387922)) AS v FROM tcpflow WHERE id <= 2`,
			[]string{"v=6"}},

		// ---- THE WIDTH SURVIVES MATERIALIZATION (#1018 round 5, B1).
		//
		// PostgreSQL's integer width is a property of a column's
		// DECLARATION, and it rides every derived table, CTE and
		// set-operation arm the way a DECIMAL's (p,s) does. Here every
		// integer expression materializes as an INT64 carrier (ADR-0024's
		// recorded widening), so before this the DIRECT call declared bigint
		// and the SAME call one level down declared numeric — the same
		// number in two boxes, on all five arms.
		//
		// The RENDERING is the claim: `v=int64:36` is PostgreSQL's bigint and
		// a bare `v=36` is its numeric. Every number below is live
		// PostgreSQL 17.11's over `(0,2,18,16)` at each width.
		{"derived_narrow_mask_sum_is_bigint",
			`SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(f4,18) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:36"}},
		{"derived_wide_mask_sum_is_numeric",
			`SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(f8,18) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=36"}},
		{"derived_int4_result_function_sum_is_bigint",
			`SELECT SUM(v) AS v FROM (SELECT REGEXP_COUNT('abab','a') AS v FROM tcpflow WHERE id <= 2) s`,
			[]string{"v=int64:4"}},
		{"derived_int8_result_function_sum_is_numeric",
			`SELECT SUM(v) AS v FROM (SELECT BIT_COUNT(4611686018427387922) AS v FROM tcpflow WHERE id <= 2) s`,
			[]string{"v=6"}},
		{"cte_over_a_derived_narrow_mask_is_bigint",
			`WITH c AS (SELECT v FROM (SELECT BITWISE_AND(f4,18) AS v FROM tcpflow WHERE id <= 4) s)
			 SELECT SUM(v) AS v FROM c`,
			[]string{"v=int64:36"}},
		{"a_union_all_of_two_narrow_arms_is_bigint",
			`SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(f4,18) AS v FROM tcpflow WHERE id <= 4
			  UNION ALL SELECT BITWISE_AND(f4,3) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:40"}},
		{"a_union_all_with_one_wide_arm_is_numeric",
			`SELECT SUM(v) AS v FROM (SELECT BITWISE_AND(f4,18) AS v FROM tcpflow WHERE id <= 4
			  UNION ALL SELECT BITWISE_AND(f8,18) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=72"}},
		{"windowed_sum_over_a_derived_narrow_mask_is_bigint",
			`SELECT SUM(v) OVER () AS v FROM (SELECT BITWISE_AND(f4,18) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:36", "v=int64:36", "v=int64:36", "v=int64:36"}},
		{"windowed_sum_over_a_derived_wide_mask_is_numeric",
			`SELECT SUM(v) OVER () AS v FROM (SELECT BITWISE_AND(f8,18) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=36", "v=36", "v=36", "v=36"}},

		// ---- MIN/MAX OVER A COMPUTED ARGUMENT KEEP THE ARGUMENT'S WIDTH
		// (#1018 round 5 review, P1). MIN copies a value the input HELD, so
		// its type is the INPUT's — and for a computed int4 argument the
		// declaration exists. `aggArgIntWidth` declined anything but a bare
		// column and the caller then recorded the INT64 CARRIER, so these
		// rendered `v=0` (numeric) where PostgreSQL 17.11 renders a bigint,
		// identically on all five arms. The WINDOW spelling was worse: its
		// slot declared float8, which is not in the integer family at all, so
		// no width could be recorded for it.
		//
		// PostgreSQL over the same four rows (f4/f8 = 0, 2, 18, 16):
		//   sum(min(f&18))                    bigint 0  / numeric 0
		//   sum(max(f&18))                    bigint 18
		//   sum(min(f&18)) GROUP BY id        bigint 36 / numeric 36
		//   sum(min(f&18) OVER ())            bigint 0  / numeric 0
		{"min_over_a_computed_narrow_argument_is_bigint",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(f4,18)) AS m FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:0"}},
		{"max_over_a_computed_narrow_argument_is_bigint",
			`SELECT SUM(m) AS v FROM (SELECT MAX(BITWISE_AND(f4,18)) AS m FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:18"}},
		{"min_over_a_computed_wide_argument_is_numeric",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(f8,18)) AS m FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=0"}},
		{"grouped_min_over_a_computed_narrow_argument_is_bigint",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(f4,18)) AS m FROM tcpflow WHERE id <= 4 GROUP BY id) s`,
			[]string{"v=int64:36"}},
		{"grouped_min_over_a_computed_wide_argument_is_numeric",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(f8,18)) AS m FROM tcpflow WHERE id <= 4 GROUP BY id) s`,
			[]string{"v=36"}},
		{"windowed_min_over_a_computed_narrow_argument_is_bigint",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(f4,18)) OVER () AS m FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:0"}},
		{"windowed_min_over_a_computed_wide_argument_is_numeric",
			`SELECT SUM(m) AS v FROM (SELECT MIN(BITWISE_AND(f8,18)) OVER () AS m FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=0"}},

		// ---- THE VALID NAMES STILL ANSWER OVER AN EMPTY INPUT (#1018 round
		// 5, B2's other side). A plan-time refusal that fired on a spelling
		// the family DOES know would be the false positive the binder's
		// standing contract forbids.
		{"valid_names_over_an_empty_input_answer_no_rows",
			`SELECT tcp_flags_has_all(f8,'SYN','ACK') AS b FROM tcpflow WHERE id < 0`,
			nil},
		{"a_column_supplied_name_over_an_empty_input_answers_no_rows",
			`SELECT tcp_flags_has_all(f8, TO_HEX(f4)) AS b FROM tcpflow WHERE id < 0`,
			nil},

		// ---- a shape the pushdown DECLINES (computed argument): the residual
		// exec filter must answer what the pushed spelling answers.
		//
		// COALESCE rather than ABS: the fixture now holds the int64 MINIMUM,
		// whose absolute value does not exist, and `abs(-9223372036854775808)`
		// is `22003 bigint out of range` on PostgreSQL and on every arm here —
		// asserted as a refusal below rather than smuggled into a count.
		{"computed_argument_is_the_same_answer",
			`SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_any(COALESCE(f8,0),'SYN','ACK')`,
			[]string{"n=int64:40"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a2fSQL[tc.name] = tc.sql
			for _, arm := range arms {
				before := a2fReadRoutes(arm.coord)
				got, err := arm.run(tc.name+"/"+arm.name, tc.sql)
				a2fCheckRoutes(t, arm.name, arm.coord, before, tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows, want %d\n  got  %v\n  want %v\n  SQL: %s",
						arm.name, len(got), len(tc.want), got, tc.want, tc.sql)
					continue
				}
				for i := range tc.want {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm row %d: %q, PostgreSQL says %q\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
					}
				}
			}
		})
	}

	// A per-row REFUSAL raised inside a WORKER. The failure mode is not a
	// wrong SQLSTATE, it is a query that never ends: the refusal has to travel
	// out of the evaluator, out of the task, and back to the caller.
	for _, tc := range []struct{ name, sql, msg string }{
		{"unknown_name_in_a_predicate",
			`SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_all(f8,'SYN','ACKK')`,
			`TCP flag name "ACKK" not recognized`},
		{"unknown_name_in_a_projection",
			`SELECT tcp_flag_mask('SYNN') AS m FROM tcpflow WHERE id < 5`,
			`TCP flag name "SYNN" not recognized`},
		{"empty_name_list",
			`SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_any(f8)`,
			"tcp_flags_has_any requires at least one TCP flag name"},
		// THE NAME OUTRANKS A NULL FLAGS ARGUMENT (#966 round 2, P4). id 15 is
		// the fixture's NULL row, so the only value the evaluator sees is
		// NULL and the refusal still has to fire — on every arm, including
		// from inside a worker. PostgreSQL raises for the operator
		// equivalent (`NULL::bigint & 'x'::bigint` is 22P02) whatever the rows
		// are.
		{"unknown_name_on_a_null_row",
			`SELECT tcp_flags_has_all(f8,'BOGUS') AS b FROM tcpflow WHERE id = 15`,
			`TCP flag name "BOGUS" not recognized`},
		// AN INVALID LITERAL NAME IS REFUSED WITH NO ROWS AT ALL (#1018
		// round 5, B2). A flag name is a MASK OPERAND and its spelling is a
		// property of the QUERY: PostgreSQL raises 22P02 for `'x'::int`
		// under `WHERE false`, because the coercion happens at parse
		// analysis and does not wait for data. These six answered ZERO ROWS
		// AND NO ERROR on all five arms and both wire formats, while the
		// same typo over a reached row was 22023 — whether a typo is an
		// error depended on the data.
		{"unknown_name_with_no_rows_at_all",
			`SELECT tcp_flags_has_all(f8,'BOGUS') AS b FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"unknown_name_with_no_rows_at_all_any",
			`SELECT tcp_flags_has_any(f8,'BOGUS') AS b FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"unknown_name_with_no_rows_at_all_none",
			`SELECT tcp_flags_has_none(f8,'BOGUS') AS b FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"unknown_name_with_no_rows_at_all_legacy",
			`SELECT has_tcp_flag(f8,'BOGUS') AS b FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"unknown_name_with_no_rows_at_all_mask",
			`SELECT tcp_flag_mask('BOGUS') AS m FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"unknown_name_with_no_rows_at_all_from_string",
			`SELECT tcp_flags_from_string('SYN,BOGUS') AS m FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		// The same thing in a PREDICATE, where the conjunct that empties the
		// input sits beside the one that is misspelled.
		{"unknown_name_in_a_predicate_with_no_rows_at_all",
			`SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_all(f8,'BOGUS') AND id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		// And the EMPTY LIST, which is the same fold answering the same way.
		{"empty_name_list_with_no_rows_at_all",
			`SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_any(f8) AND id < 0`,
			"tcp_flags_has_any requires at least one TCP flag name"},
		{"unknown_name_on_a_null_row_legacy_spelling",
			`SELECT has_tcp_flag(f8,'BOGUS') AS b FROM tcpflow WHERE id = 15`,
			`TCP flag name "BOGUS" not recognized`},

		// EVERY EXPRESSION POSITION, ON BOTH PLANNING PATHS (#1018 round 6,
		// B1). Round 5 folded the constant name at COMPILATION, which the
		// single-process path reaches while it PLANS and a DAG stage's
		// fragment reaches only when a TASK RUNS. So a position whose stage
		// received no rows was never folded: these four raised 22023 on
		// `single` and `single+budget` and answered ZERO ROWS AND NO ERROR on
		// `dag`, `dag-shuffled` and `dag-morsel4`, with every routing counter
		// flat — the query really did run as a DAG. Their `_reached` twins,
		// over the same table with the emptying predicate removed, were 22023
		// on every arm: whether a typo was an error depended on the data AND
		// on the plan shape.
		//
		// The refusal is now the BINDER's (physical.refuseUnknownFlagNames),
		// which Plan and PlanDistributed both run before any stage exists.
		// The pair — empty and reached — is the claim; either alone is not.
		{"position_having_empty",
			`SELECT id AS n FROM tcpflow WHERE id < 0 GROUP BY id HAVING COUNT(*) > 0 AND tcp_flags_has_all(MIN(f8),'BOGUS')`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_having_reached",
			`SELECT id AS n FROM tcpflow GROUP BY id HAVING COUNT(*) > 0 AND tcp_flags_has_all(MIN(f8),'BOGUS')`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_order_by_empty",
			`SELECT id AS n FROM tcpflow WHERE id < 0 ORDER BY tcp_flag_mask('BOGUS')`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_order_by_reached",
			`SELECT id AS n FROM tcpflow ORDER BY tcp_flag_mask('BOGUS') LIMIT 1`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_union_arm_empty",
			`SELECT tcp_flag_mask('SYN') AS n FROM tcpflow WHERE id < 0 UNION ALL SELECT tcp_flag_mask('BOGUS') AS n FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_union_arm_reached",
			`SELECT tcp_flag_mask('SYN') AS n FROM tcpflow UNION ALL SELECT tcp_flag_mask('BOGUS') AS n FROM tcpflow`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_projection_above_group_by_empty",
			`SELECT id AS g, tcp_flag_mask('BOGUS') AS n FROM tcpflow WHERE id < 0 GROUP BY id`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_projection_above_group_by_reached",
			`SELECT id AS g, tcp_flag_mask('BOGUS') AS n FROM tcpflow GROUP BY id`,
			`TCP flag name "BOGUS" not recognized`},
		// A SUBQUERY BODY is the same gap one position over: on the DAG the
		// EXISTS shape did not even answer, it handed the client the
		// coordinator's own "EXISTS subquery requires a SubqueryRunner"
		// (#1018 round 5 review, P3). The family's 22023 is the answer.
		{"position_exists_subquery_empty",
			`SELECT COUNT(*) AS n FROM tcpflow t WHERE EXISTS (SELECT 1 FROM tcpflow u WHERE u.id < 0 AND tcp_flags_has_all(u.f8,'BOGUS'))`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_in_subquery_empty",
			`SELECT COUNT(*) AS n FROM tcpflow t WHERE t.id IN (SELECT u.id FROM tcpflow u WHERE u.id < 0 AND tcp_flags_has_all(u.f8,'BOGUS'))`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_scalar_subquery_empty",
			`SELECT (SELECT MAX(tcp_flag_mask('BOGUS')) FROM tcpflow u2 WHERE u2.id < 0) AS v FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_window_argument_empty",
			`SELECT SUM(tcp_flag_mask('BOGUS')) OVER () AS w FROM tcpflow WHERE id < 0`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_derived_body_empty",
			`SELECT COUNT(*) AS n FROM (SELECT tcp_flag_mask('BOGUS') AS m FROM tcpflow WHERE id < 0) s`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_cte_body_empty",
			`WITH c AS (SELECT tcp_flag_mask('BOGUS') AS m FROM tcpflow WHERE id < 0) SELECT COUNT(*) AS n FROM c`,
			`TCP flag name "BOGUS" not recognized`},
		{"position_join_on_empty",
			`SELECT COUNT(*) AS n FROM tcpflow a JOIN tcpflow b ON a.id = b.id AND tcp_flags_has_all(b.f8,'BOGUS') WHERE a.id < 0`,
			`TCP flag name "BOGUS" not recognized`},
	} {
		t.Run("refusal/"+tc.name, func(t *testing.T) {
			a2fSQL["refusal/"+tc.name] = tc.sql
			for _, arm := range arms {
				before := a2fReadRoutes(arm.coord)
				got, err := arm.run("refusal/"+tc.name+"/"+arm.name, tc.sql)
				a2fCheckRoutes(t, arm.name, arm.coord, before, tc.sql)
				if err == nil {
					t.Errorf("%s arm ANSWERED %v; 22023 is due", arm.name, got)
					continue
				}
				if state := sqlerr.StateOf(err); state != "22023" {
					t.Errorf("%s arm raised SQLSTATE %s, want 22023\n  err: %v",
						arm.name, state, err)
				}
				if !strings.Contains(err.Error(), tc.msg) {
					t.Errorf("%s arm: %q does not carry %q", arm.name, err, tc.msg)
				}
			}
		})
	}

	// A SCALAR SUBQUERY'S COLUMN DECLARES WHAT THE SUBQUERY DECLARES (#1018
	// round 5 review, P2). A subquery is a whole second query whose type lives
	// in the CATALOG, and the declaration walks hold no Planner — so a
	// scalar-subquery column MATERIALIZED by a derived table or a CTE was
	// declared STRING and every reader above it fell to float8. These rendered
	// `v=float:72` on all five arms where PostgreSQL renders a bigint or a
	// numeric. The subquery reads id=3, whose f4/f8 is 18, over the four rows
	// id<=4: PostgreSQL 17.11 says bigint 72 / numeric 72 / bigint 72 /
	// numeric 72, and numeric for the COUNT(*) form (60 rows, four times).
	//
	// THE ROUTE IS ASSERTED, NOT ASSUMED. A scalar-subquery projection is a
	// shape the DAG refuses to stage, so every one of these runs in-process
	// and moves ScalarProjectionLocalRoutes. Rows alone cannot tell "executed
	// on the DAG" from "refused and routed local", and a cell that claimed the
	// former here would be claiming something false.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"derived_scalar_subquery_narrow_is_bigint",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT BITWISE_AND(f4,18) FROM tcpflow u2 WHERE u2.id = 3) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:72"}},
		{"derived_scalar_subquery_wide_is_numeric",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT BITWISE_AND(f8,18) FROM tcpflow u2 WHERE u2.id = 3) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=72"}},
		{"derived_scalar_subquery_bare_int4_column_is_bigint",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT f4 FROM tcpflow u2 WHERE u2.id = 3) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:72"}},
		{"derived_scalar_subquery_bare_int8_column_is_numeric",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT f8 FROM tcpflow u2 WHERE u2.id = 3) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=72"}},
		{"derived_scalar_subquery_count_is_numeric",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT COUNT(*) FROM tcpflow u2) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=240"}},
		{"cte_over_a_scalar_subquery_is_bigint",
			`WITH c AS (SELECT (SELECT BITWISE_AND(f4,18) FROM tcpflow u2 WHERE u2.id = 3) AS v FROM tcpflow WHERE id <= 4)
			 SELECT SUM(v) AS v FROM c`,
			[]string{"v=int64:72"}},

		// A SCALAR SUBQUERY THAT ITSELF HOLDS ONE, planned on the DAG arms
		// (#1018 round 6 review, B2). Resolving the outer declaration plans
		// the outer subquery, whose own annotation pass resolves the inner
		// one — through a CHILD planner. Before the ownership rule was
		// stated, that child wrote the PARENT's memo, and the same runner is
		// reached from every parallel pipeline goroutine.
		{"nested_scalar_subquery_sum_is_bigint",
			`SELECT SUM(v) AS v FROM (SELECT (SELECT (SELECT BITWISE_AND(f4,18) FROM tcpflow u3 WHERE u3.id = 3) FROM tcpflow u2 WHERE u2.id = 3) AS v FROM tcpflow WHERE id <= 4) s`,
			[]string{"v=int64:72"}},
	} {
		t.Run("scalar_subquery/"+tc.name, func(t *testing.T) {
			a2fSQL["scalar_subquery/"+tc.name] = tc.sql
			for _, arm := range arms {
				before := a2fReadRoutes(arm.coord)
				got, err := arm.run("scalar_subquery/"+tc.name+"/"+arm.name, tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				a2fCheckScalarRoute(t, arm.name, arm.coord, before, tc.sql)
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows %v, want %d %v\n  SQL: %s",
						arm.name, len(got), got, len(tc.want), tc.want, tc.sql)
					continue
				}
				for i := range tc.want {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm row %d: %q, PostgreSQL says %q\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
					}
				}
			}
		})
	}

	// THE BOUNDARY OF THE POSITION REFUSAL, FROM THE OTHER SIDE. The same
	// positions with a name the family KNOWS answer over the same empty input
	// on every arm: a plan-time fold that fired on one of these would be the
	// false positive the binder's standing contract forbids. A name supplied
	// by a COLUMN is not knowable before rows and keeps the per-row refusal,
	// so it answers here too.
	for _, tc := range []struct {
		name, sql string
		want      []string
	}{
		{"position_having_valid", `SELECT id AS n FROM tcpflow WHERE id < 0 GROUP BY id HAVING COUNT(*) > 0 AND tcp_flags_has_all(MIN(f8),'SYN')`, nil},
		{"position_order_by_valid", `SELECT id AS n FROM tcpflow WHERE id < 0 ORDER BY tcp_flag_mask('SYN')`, nil},
		{"position_union_arm_valid", `SELECT tcp_flag_mask('SYN') AS n FROM tcpflow WHERE id < 0 UNION ALL SELECT tcp_flag_mask('ACK') AS n FROM tcpflow WHERE id < 0`, nil},
		{"position_projection_above_group_by_valid", `SELECT id AS g, tcp_flag_mask('SYN') AS n FROM tcpflow WHERE id < 0 GROUP BY id`, nil},
		{"position_exists_subquery_valid", `SELECT COUNT(*) AS n FROM tcpflow t WHERE EXISTS (SELECT 1 FROM tcpflow u WHERE u.id < 0 AND tcp_flags_has_all(u.f8,'SYN'))`, []string{"n=int64:0"}},
		{"position_window_argument_valid", `SELECT SUM(tcp_flag_mask('SYN')) OVER () AS w FROM tcpflow WHERE id < 0`, nil},
		// A name that is NOT a constant is not knowable before rows, so it
		// keeps the per-row refusal and this ANSWERS over an empty input —
		// even though the name it would compute is the same misspelling.
		{"position_having_computed_name_stays_per_row", `SELECT id AS n FROM tcpflow WHERE id < 0 GROUP BY id HAVING tcp_flags_has_all(MIN(f8), UPPER('bogus'))`, nil},
	} {
		t.Run("control/"+tc.name, func(t *testing.T) {
			for _, arm := range arms {
				before := a2fReadRoutes(arm.coord)
				got, err := arm.run("control/"+tc.name+"/"+arm.name, tc.sql)
				a2fCheckRoutes(t, arm.name, arm.coord, before, tc.sql)
				if err != nil {
					t.Errorf("%s arm REFUSED a query it must answer: %v\n  SQL: %s",
						arm.name, err, tc.sql)
					continue
				}
				if len(got) != len(tc.want) {
					t.Errorf("%s arm: %d rows %v, want %d %v\n  SQL: %s",
						arm.name, len(got), got, len(tc.want), tc.want, tc.sql)
					continue
				}
				for i := range tc.want {
					if got[i] != tc.want[i] {
						t.Errorf("%s arm row %d: %q, want %q\n  SQL: %s",
							arm.name, i, got[i], tc.want[i], tc.sql)
					}
				}
			}
		})
	}

	// The int64 MINIMUM the fixture now carries has no absolute value, and
	// PostgreSQL 17.11 answers `ERROR: bigint out of range` for
	// `abs((-9223372036854775808)::int8)`. Every arm here refuses it too, which
	// is why the computed-argument cell above spells its wrapper COALESCE.
	t.Run("refusal/abs_of_the_int64_minimum", func(t *testing.T) {
		const sql = `SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_any(ABS(f8),'SYN')`
		a2fSQL["refusal/abs_of_the_int64_minimum"] = sql
		for _, arm := range arms {
			before := a2fReadRoutes(arm.coord)
			got, err := arm.run("refusal/abs_of_the_int64_minimum/"+arm.name, sql)
			a2fCheckRoutes(t, arm.name, arm.coord, before, sql)
			if err == nil {
				t.Errorf("%s arm ANSWERED %v; PostgreSQL raises bigint out of range",
					arm.name, got)
				continue
			}
			if !strings.Contains(err.Error(), "bigint out of range") {
				t.Errorf("%s arm: %q does not carry PostgreSQL's message", arm.name, err)
			}
		}
	})

	a2fCheckSpillEngagement(t)
}

// --- the fixture, which rides along in tmdTables() ---

const tcpfTable = "tcpflow"

func tcpfSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "f4", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f8", Type: parquet.TypeInt64, Nullable: true},
	}}
}

// tcpfData is four repeats of fifteen rows: the fourteen flag values
// PostgreSQL was measured over, at each column's own width, plus a NULL.
// id is 1..60, so within the first repeat
//
//	id  3  18                    (SYN|ACK)
//	id  9  256                   (AE, the ninth bit)
//	id 10  2^62 | 18             (the value a float carrier truncates)
//	id 11  -1                    (every bit set)
//	id 12  -18
//	id 13  int64 / int32 MINIMUM (only the sign bit)
//	id 14  int64 / int32 MAXIMUM
//	id 15  NULL
//
// The last four are the round-2 addition (#966 P3). ROUND0's transcript makes
// "a negative value is a bit pattern, not a magnitude" one of three
// load-bearing properties, and the first draft of this fixture derived the
// INT32 column as `v & 0x1FF`, which turned every negative into a positive
// before any arm saw it — so the property was asserted only in a unit test and
// never across a parquet write or a shuffle.
func tcpfData() []map[string]any {
	vals8 := []int64{0, 2, 18, 16, 4, 511, 24, 20, 256, 1<<62 | 18,
		-1, -18, -9223372036854775808, 9223372036854775807}
	vals4 := []int32{0, 2, 18, 16, 4, 511, 24, 20, 256, 18,
		-1, -18, -2147483648, 2147483647}
	rows := make([]map[string]any, 0, 60)
	for rep := 0; rep < 4; rep++ {
		for i := range vals8 {
			rows = append(rows, map[string]any{
				"id": int32(rep*15 + i + 1),
				"f4": vals4[i],
				"f8": vals8[i],
			})
		}
		rows = append(rows, map[string]any{"id": int32(rep*15 + 15), "f4": nil, "f8": nil})
	}
	return rows
}

// ---------------------------------------------------------------------------
// The two things rows cannot say (#966 round 2 P1).

// a2fRoutes is every local-routing counter the coordinator publishes.
//
// A DAG arm that ANSWERS is not thereby a DAG arm that EXECUTED: the
// coordinator refuses plans it cannot stage and runs them in-process, and the
// rows come back identical. The round-2 review had to bring its own matrix to
// establish that none of these cells was quietly taking that path, because
// this file — the DURABLE census — read no counter at all. Every counter is
// read, not the handful a shape is expected to touch: a refusal that moves to
// a different counter is still a refusal.
type a2fRoutes struct {
	names  []string
	values []int64
}

func a2fReadRoutes(c *Coordinator) a2fRoutes {
	if c == nil {
		return a2fRoutes{}
	}
	return a2fRoutes{
		names: []string{
			"Correlated", "Distinct", "GroupKey", "GroupingSets", "InSubquery",
			"LateralProjection", "NullAwareAnti", "ScalarProjection",
			"TableLess", "UnbuildableStage", "UnreachableOutput",
		},
		values: []int64{
			c.CorrelatedLocalRoutes(), c.DistinctLocalRoutes(), c.GroupKeyLocalRoutes(),
			c.GroupingSetsLocalRoutes(), c.InSubqueryLocalRoutes(),
			c.LateralProjectionLocalRoutes(), c.NullAwareAntiLocalRoutes(),
			c.ScalarProjectionLocalRoutes(), c.TableLessLocalRoutes(),
			c.UnbuildableStageLocalRoutes(), c.UnreachableOutputLocalRoutes(),
		},
	}
}

// a2fCheckRoutes asserts that no counter moved. Every shape in this file is
// one the DAG can stage; a nonzero delta means the arm answered from the local
// fallback and the cell proved nothing about distributed execution.
func a2fCheckRoutes(t *testing.T, arm string, c *Coordinator, before a2fRoutes, sql string) {
	t.Helper()
	after := a2fReadRoutes(c)
	for i, name := range after.names {
		if d := after.values[i] - before.values[i]; d != 0 {
			t.Errorf("%s arm: %sLocalRoutes moved by %d — the query was REFUSED and run "+
				"in-process, so its rows say nothing about the DAG\n  SQL: %s",
				arm, name, d, sql)
		}
	}
}

// a2fCheckScalarRoute is a2fCheckRoutes for a shape the DAG deliberately
// REFUSES to stage: the SCALAR-SUBQUERY projection, which the coordinator runs
// in-process instead. The claim is the route, so it is asserted — exactly one
// counter moves, and it is that one. Every other counter must stay flat, which
// is what tells "refused for the reason we think" from "refused for another".
func a2fCheckScalarRoute(t *testing.T, arm string, c *Coordinator, before a2fRoutes, sql string) {
	t.Helper()
	if c == nil {
		return // a single-process arm has no coordinator to route
	}
	after := a2fReadRoutes(c)
	moved := 0
	for i, name := range after.names {
		d := after.values[i] - before.values[i]
		if d == 0 {
			continue
		}
		moved++
		if name != "ScalarProjection" {
			t.Errorf("%s arm: %sLocalRoutes moved by %d; this shape routes local "+
				"through ScalarProjection and nothing else\n  SQL: %s", arm, name, d, sql)
		}
	}
	if moved == 0 {
		t.Errorf("%s arm: no local-routing counter moved, so this cell's rows do not "+
			"come from the route it claims. If the DAG can now stage a scalar-subquery "+
			"projection, this gate is the place that says so.\n  SQL: %s", arm, sql)
	}
}

// a2fSpills counts cells whose budgeted arm actually wrote a spill artifact,
// and a2fEngaged records the per-cell answer. ADR-0027 §5: a spill gate proves
// it spilled. Round 2 measured ZERO spill files across eighteen projections at
// a 512 KiB budget, so the arm was a second in-memory run.
var (
	a2fSpills  atomic.Int64
	a2fEngaged sync.Map
)

// a2fNonSpilling explains, per shape class, why a cell CANNOT engage — so a
// cell that stops spilling is a failure rather than a shrug. The knob forces a
// HashAggregate drain; a shape with no pipeline breaker has nothing to drain,
// and an UNGROUPED aggregate holds one row of accumulators (ADR-0027
// decision 4).
func a2fNonSpilling(sql string) string {
	u := strings.ToUpper(sql)
	switch {
	case strings.Contains(u, "ID < 0") || strings.Contains(u, "ID<0"):
		// The boundary controls empty the input on purpose. An aggregate that
		// receives no rows has no accumulator state, so the forced drain has
		// nothing to drain — which is the point of the cell, not a lapse.
		return "an input the predicate empties: no rows reach the aggregate"
	case strings.Contains(u, "GROUP BY"):
		return "" // must engage
	case strings.Contains(u, " OVER ("):
		return "window over the whole input: one partition of two rows, and the " +
			"window run floor is not what this knob lowers for it"
	case strings.Contains(u, "COUNT(") || strings.Contains(u, "SUM("):
		return "ungrouped aggregate: one row of accumulators, nothing to drain " +
			"(ADR-0027 decision 4)"
	default:
		return "projection only: no pipeline breaker in the plan"
	}
}

func a2fCheckSpillEngagement(t *testing.T) {
	t.Helper()
	if a2fSpills.Load() == 0 {
		t.Error("the budgeted arm spilled on NO cell, so every one of its comparisons " +
			"was between two in-memory runs and proves nothing (ADR-0027 §5). Either " +
			"the forcing knob stopped reaching the aggregate or every shape lost its " +
			"pipeline breaker.")
	}
	var unexplained []string
	total, engaged := 0, 0
	a2fEngaged.Range(func(k, v any) bool {
		name := k.(string)
		if !strings.HasSuffix(name, "/single+budget+forced-drain") {
			return true
		}
		// A REFUSED query builds no pipeline at all, so there is nothing for
		// the forced drain to reach. Its claim is the SQLSTATE, not a spill.
		if strings.HasPrefix(name, "refusal/") {
			return true
		}
		total++
		if v.(bool) {
			engaged++
			return true
		}
		if why := a2fNonSpilling(a2fSQL[strings.TrimSuffix(name, "/single+budget+forced-drain")]); why == "" {
			unexplained = append(unexplained, name)
		}
		return true
	})
	sort.Strings(unexplained)
	if len(unexplained) > 0 {
		t.Errorf("these cells have a GROUP BY and did NOT spill under the forced drain, "+
			"so their budgeted arm is a second in-memory run:\n  %s",
			strings.Join(unexplained, "\n  "))
	}
	t.Logf("SPILL ENGAGEMENT: %d of %d budgeted cells wrote a spill artifact; the rest "+
		"are named non-spilling shapes (no pipeline breaker, or an ungrouped "+
		"accumulator)", engaged, total)
}

// a2fSQL maps a cell name to its SQL so the engagement check can classify a
// shape it did not run itself.
var a2fSQL = map[string]string{}
