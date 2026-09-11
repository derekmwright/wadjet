package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

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

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, spilled, sql)) }},
		{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag-morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
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
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
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
	} {
		t.Run("refusal/"+tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
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

	// The int64 MINIMUM the fixture now carries has no absolute value, and
	// PostgreSQL 17.11 answers `ERROR: bigint out of range` for
	// `abs((-9223372036854775808)::int8)`. Every arm here refuses it too, which
	// is why the computed-argument cell above spells its wrapper COALESCE.
	t.Run("refusal/abs_of_the_int64_minimum", func(t *testing.T) {
		for _, arm := range arms {
			got, err := arm.run(
				`SELECT COUNT(*) AS n FROM tcpflow WHERE tcp_flags_has_any(ABS(f8),'SYN')`)
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
