package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/worker"
)

// An integer SUM that WRAPS fails; it does not answer.
//
// #784 gave a BARE integer column an exact Int128 carrier, so `SUM(b)` over
// the bigsum fixture answers PostgreSQL's 18446744073709551616. A COMPUTED
// integer argument keeps the INT64 carrier on purpose — that is what makes
// `SUM(CASE WHEN … THEN 1 ELSE 0 END)`, TPC-H Q12's shape and a BI staple,
// come back under PostgreSQL's int8 OID instead of numeric
// (physical.aggOutputFromInputDecl). The residual that narrowing names is a
// computed integer sum past 2^63, and until review round 2 F4 that residual
// was SILENT: `SUM(-b)` answered exactly 0 for PostgreSQL's
// -18446744073709551616, beside three spellings of the same question that
// answered exactly.
//
// So the two spellings that wrap must be LOUD, on every arm, and the three
// that do not must still answer. Both halves are the gate: an implementation
// that failed everything would pass a one-sided version of this test.
//
// The five arms are the census's own (numeric_arc2_two_path_test.go): the
// single-process pipeline, that pipeline under a memory budget with the
// aggregate drain FORCED (ADR-0027 §6 — the reference arm stays disarmed),
// the stage DAG, the DAG with every join broadcast, and the DAG with
// morsel-parallel breakers in the worker. The carrier's flag has to survive
// the drain, the partial-state merge and the clone; each arm is where one of
// those lives.
func TestIntegerSumOverflowIsLoudOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
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

	drained := func(sql string) ([]string, error) {
		restore := exec.ForceAggDrainEvery(1)
		defer exec.ForceAggDrainEvery(restore)
		return na2Run(tmdRunSingle(ctx, spilled, sql))
	}
	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget+forced-drain", drained},
		{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
	}

	for _, tc := range []struct {
		name, sql string
		// want is the expected single row, or "" when the query must FAIL.
		want string
	}{
		// The three that answer. b + 0 and b * 1 fold back to the bare column
		// before the aggregate is built, so they take #784's exact carrier;
		// they are the control that says the two refusals below are about the
		// CARRIER and not about arithmetic appearing in an argument at all.
		{"bare_column_is_exact", `SELECT SUM(b) AS s FROM bigsum`, "s=18446744073709551616"},
		{"folded_zero_add_is_exact", `SELECT SUM(b + 0) AS s FROM bigsum`, "s=18446744073709551616"},
		{"folded_one_multiply_is_exact", `SELECT SUM(b * 1) AS s FROM bigsum`, "s=18446744073709551616"},

		// The two that wrap. PostgreSQL 17 answers -18446744073709551616 for
		// the first and 18446744073709551616 for the second; wadjet's int64
		// carrier holds neither, and used to say 0 for both.
		{"negated_column_refuses", `SELECT SUM(-b) AS s FROM bigsum`, ""},
		{"case_arm_refuses",
			`SELECT SUM(CASE WHEN b IS NULL THEN 0 ELSE b END) AS s FROM bigsum`, ""},

		// GROUPED, where the running sums live in the flat SoA arrays and not
		// in an accumulator at all. Group 0's negated total is
		// -9232379236109516801, which is past the int64 floor by 9 006 661 665
		// 254 993; group 1's -9214364837600034815 fits. One wrapping group
		// fails the query, exactly as one wrapping group fails a DECIMAL sum.
		{"grouped_negation_refuses",
			`SELECT g AS k, SUM(-b) AS s FROM bigsum GROUP BY g ORDER BY k`, ""},

		// The sum of ones that must NOT be caught up in any of this: it is the
		// shape the bigint narrowing exists for, and no data makes it wrap.
		// Five of the eight rows hold a positive b; the NULL row takes the
		// ELSE arm because `NULL > 0` is not true.
		{"case_of_ones_still_answers",
			`SELECT SUM(CASE WHEN b > 0 THEN 1 ELSE 0 END) AS s FROM bigsum`, "s=int64:5"},

		// A computed sum that stays INSIDE the range answers, so the check is
		// not "any computed integer argument fails".
		{"negated_small_column_answers", `SELECT SUM(-g) AS s FROM bigsum`, "s=int64:-4"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if tc.want == "" {
					if err == nil {
						t.Errorf("%s arm: answered %v, want a 22003 refusal\n  SQL: %s\n  "+
							"PostgreSQL 17 answers exactly; wadjet's int64 carrier cannot, "+
							"so the only readings are the exact number or a failure — never a "+
							"wrapped one (ADR-0012 item 9)", arm.name, got, tc.sql)
						continue
					}
					if !strings.Contains(err.Error(), "overflowed the 64-bit accumulator") {
						t.Errorf("%s arm: failed with %v, want the integer-carrier overflow\n  SQL: %s",
							arm.name, err, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s\n  want %s", arm.name, err, tc.sql, tc.want)
					continue
				}
				if len(got) != 1 || got[0] != tc.want {
					t.Errorf("%s arm:\n  got  %v\n  want [%s]\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
