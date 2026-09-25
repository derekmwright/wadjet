// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/worker"
)

// Round 8, #1252: a text-input function's constant NUMERIC argument must
// survive the DAG's per-stage recompile. formatDecimalLitArgs (b5a29d58)
// rewrote EVERY argument of a stringInputFuncs entry through decimalLitText
// without asking whether THAT POSITION is itself declared text — SUBSTR's
// start/length, LEFT/RIGHT's count, SPLIT_PART's field, LPAD/RPAD's width
// and OVERLAY's start/count are numbers by the function's OWN signature
// (typedArgPositions), never text, no matter that the function as a whole
// "wants text" for its first argument. `SUBSTR('abcdef', 2)` lost the
// integer 2 to the string "2", and fnSubstr's own #1169 rule — a two-argument
// call's second operand is a REGEX PATTERN when it is a string, a character
// position when it is a number — read "2" as a pattern that does not match
// "abcdef" and answered NULL. Only the DAG arms showed it: the single-process
// pipeline folds this constant projection through the vectorized kernel,
// which formatDecimalLitArgs never touches; the DAG recompiles the stage's
// projection through the per-row Eval path, which does.
//
// This is the same table shape as TestArcEXAnswersTheSameOnEveryArm (five
// arms, one SQL statement, one answer) over every stringInputFuncs entry that
// has a typedArgPositions entry — the exact family the seam covers — crossed
// with the four argument SHAPES the seam has to tell apart: a bare INTEGER
// literal (the regression itself), a bare DECIMAL literal (decimalLitText's
// own target — the same rewrite, wrongly applied to a typed position), a
// COLUMN (never reached decimalLitText: isConstNumericLit is false for a
// ColRef, so this shape was never broken — a control), and an EXPRESSION
// (same control, over a BinOp). The DECIMAL-literal shape is PostgreSQL's own
// refusal (`substr(text, numeric)` does not exist, 42883 measured directly
// against 17.11 on every one of these functions) where this engine is an
// established superset (truncates toward the integer, same as the INTEGER
// shape) — ADR-0012's divergence list, not a defect this round changes —
// so that row asserts five-arm AGREEMENT rather than a PostgreSQL value.
func TestTypedNumericParameterSurvivesEveryArm(t *testing.T) {
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

	arms := func() []struct {
		name string
		run  func(string) ([]string, error)
	} {
		return []struct {
			name string
			run  func(string) ([]string, error)
		}{
			{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, single, sql)) }},
			{"single+budget", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, spilled, sql)) }},
			{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
			{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
			{"dag+morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
		}
	}

	// typemx id=1: c_str = "s-000001", c_i32 = 3 (int(i)*3 with i=1).

	// THE ANSWERS. Every cell measured directly against PostgreSQL 17.11
	// except the "decimal" shape (PostgreSQL refuses it on every one of
	// these functions with 42883 — the engine's established superset).
	for _, tc := range []struct {
		fn, shape, sql string
		want           []string
	}{
		// substr's POSITION argument (2-arg form) — the regression itself.
		{"substr", "constant_int",
			`SELECT SUBSTR('abcdef', 2) AS v FROM typemx WHERE id = 1`, []string{"v=bcdef"}},
		{"substr", "constant_decimal",
			`SELECT SUBSTR('abcdef', 2.6) AS v FROM typemx WHERE id = 1`, []string{"v=bcdef"}},
		{"substr", "column",
			`SELECT SUBSTR(c_str, c_i32) AS v FROM typemx WHERE id = 1`, []string{"v=000001"}},
		{"substr", "expression",
			`SELECT SUBSTR(c_str, c_i32 + 1) AS v FROM typemx WHERE id = 1`, []string{"v=00001"}},
		// substr's LENGTH argument (3-arg form; position held at the
		// constant 2 so only the length shape varies).
		{"substr_len", "constant_int",
			`SELECT SUBSTR(c_str, 2, 3) AS v FROM typemx WHERE id = 1`, []string{"v=-00"}},
		{"substr_len", "constant_decimal",
			`SELECT SUBSTR(c_str, 2, 3.6) AS v FROM typemx WHERE id = 1`, []string{"v=-00"}},
		{"substr_len", "column",
			`SELECT SUBSTR(c_str, 2, c_i32) AS v FROM typemx WHERE id = 1`, []string{"v=-00"}},
		{"substr_len", "expression",
			`SELECT SUBSTR(c_str, 2, c_i32 - 1) AS v FROM typemx WHERE id = 1`, []string{"v=-0"}},
		// substring: the synonym reaches the identical fnSubstr — one
		// control cell confirms the fix is not substr-name-specific.
		{"substring", "constant_int",
			`SELECT SUBSTRING('abcdef', 2) AS v FROM typemx WHERE id = 1`, []string{"v=bcdef"}},
		// left's COUNT argument.
		{"left", "constant_int",
			`SELECT LEFT('abcdef', 3) AS v FROM typemx WHERE id = 1`, []string{"v=abc"}},
		{"left", "constant_decimal",
			`SELECT LEFT('abcdef', 2.6) AS v FROM typemx WHERE id = 1`, []string{"v=ab"}},
		{"left", "column",
			`SELECT LEFT(c_str, c_i32) AS v FROM typemx WHERE id = 1`, []string{"v=s-0"}},
		{"left", "expression",
			`SELECT LEFT(c_str, c_i32 + 1) AS v FROM typemx WHERE id = 1`, []string{"v=s-00"}},
		// right's COUNT argument.
		{"right", "constant_int",
			`SELECT RIGHT('abcdef', 3) AS v FROM typemx WHERE id = 1`, []string{"v=def"}},
		{"right", "constant_decimal",
			`SELECT RIGHT('abcdef', 2.6) AS v FROM typemx WHERE id = 1`, []string{"v=ef"}},
		{"right", "column",
			`SELECT RIGHT(c_str, c_i32) AS v FROM typemx WHERE id = 1`, []string{"v=001"}},
		{"right", "expression",
			`SELECT RIGHT(c_str, c_i32 + 1) AS v FROM typemx WHERE id = 1`, []string{"v=0001"}},
		// split_part's FIELD argument.
		{"split_part", "constant_int",
			`SELECT SPLIT_PART('a,b,c', ',', 2) AS v FROM typemx WHERE id = 1`, []string{"v=b"}},
		{"split_part", "constant_decimal",
			`SELECT SPLIT_PART('a,b,c', ',', 2.6) AS v FROM typemx WHERE id = 1`, []string{"v=b"}},
		{"split_part", "column",
			`SELECT SPLIT_PART('a,b,c', ',', c_i32) AS v FROM typemx WHERE id = 1`, []string{"v=c"}},
		{"split_part", "expression",
			`SELECT SPLIT_PART('a,b,c', ',', c_i32 - 1) AS v FROM typemx WHERE id = 1`, []string{"v=b"}},
		// lpad's WIDTH argument.
		{"lpad", "constant_int",
			`SELECT LPAD('ab', 5, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=xxxab"}},
		{"lpad", "constant_decimal",
			`SELECT LPAD('ab', 5.6, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=xxxab"}},
		{"lpad", "column",
			`SELECT LPAD('ab', c_i32, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=xab"}},
		{"lpad", "expression",
			`SELECT LPAD('ab', c_i32 + 2, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=xxxab"}},
		// rpad's WIDTH argument.
		{"rpad", "constant_int",
			`SELECT RPAD('ab', 5, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=abxxx"}},
		{"rpad", "constant_decimal",
			`SELECT RPAD('ab', 5.6, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=abxxx"}},
		{"rpad", "column",
			`SELECT RPAD('ab', c_i32, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=abx"}},
		{"rpad", "expression",
			`SELECT RPAD('ab', c_i32 + 2, 'x') AS v FROM typemx WHERE id = 1`, []string{"v=abxxx"}},
		// overlay's START argument.
		{"overlay", "constant_int",
			`SELECT OVERLAY('abcdef' PLACING 'XY' FROM 2) AS v FROM typemx WHERE id = 1`, []string{"v=aXYdef"}},
		{"overlay", "constant_decimal",
			`SELECT OVERLAY('abcdef' PLACING 'XY' FROM 2.6) AS v FROM typemx WHERE id = 1`, []string{"v=aXYdef"}},
		{"overlay", "column",
			`SELECT OVERLAY(c_str PLACING 'XY' FROM c_i32) AS v FROM typemx WHERE id = 1`, []string{"v=s-XY0001"}},
		{"overlay", "expression",
			`SELECT OVERLAY(c_str PLACING 'XY' FROM c_i32 + 1) AS v FROM typemx WHERE id = 1`, []string{"v=s-0XY001"}},
	} {
		t.Run(tc.fn+"/"+tc.shape, func(t *testing.T) {
			for _, arm := range arms() {
				got, err := arm.run(tc.sql)
				if err != nil {
					t.Errorf("%s arm: %v — this shape answers on every other arm\n  SQL: %s",
						arm.name, err, tc.sql)
					continue
				}
				if strings.Join(got, "|") != strings.Join(tc.want, "|") {
					t.Errorf("%s arm: %v, want %v\n  SQL: %s", arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
