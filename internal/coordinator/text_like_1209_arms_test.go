// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// #1209 on FIVE ARMS: text LIKE takes the backslash as its default escape on
// single / single+budget / dag / dag-shuffled / dag+morsel4. A stored column
// against a literal pattern is pushed into the scan (scan.compileLike) on the
// single arms and compiled again inside a worker fragment on the DAG arms; a
// CASE-wrapped LIKE and a HAVING are not pushed and reach the expression
// evaluator and the exec LIKE filter. A pattern ending in a lone backslash is
// never pushed, so the 22025 is raised wherever the matcher reaches it.
//
// c_str is `s-%06d` and NULL on a stride; the counts come from the fixture.
// Every expectation was checked on PostgreSQL 17.11 against the literal values
// ('s-000001' LIKE 's\-%' t, 's-000012' LIKE 's\-0000\1%' t, 's-000021'
// LIKE 's\-0000\1%' f, 'a%b' LIKE 'a\%b' t, 's-000001' LIKE 's-%\' 22025).
func TestTextLike1209AnswersTheSameOnEveryArm(t *testing.T) {
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

	arms := []struct {
		name string
		run  func(string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return ptArmRun(tmdRunSingle(ctx, single, sql)) }},
		{"single+budget", func(sql string) ([]string, error) { return ptArmRun(tmdRunSingle(ctx, spilled, sql)) }},
		{"dag", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag+morsel4", func(sql string) ([]string, error) { return ptArmRun(tmdRunDAG(ctx, coordM, sql)) }},
	}

	nonNull, prefix00001 := 0, 0
	for _, r := range typematrix.Data(typematrix.Rows) {
		s, ok := r["c_str"].(string)
		if !ok {
			continue
		}
		nonNull++
		if strings.HasPrefix(s, "s-00001") {
			prefix00001++
		}
	}

	for _, tc := range []struct {
		name, sql string
		want      []string
		state     string
	}{
		// Pushed into the scan: `\-` is a literal hyphen, so every non-NULL
		// c_str matches (b0e0f116: 0, the prefix `s\-` matched nothing).
		{name: "pushed_escaped_prefix",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE c_str LIKE 's\-%'`,
			want: []string{fmt.Sprintf("n=int64:%d", nonNull)}},
		// Not pushed (CASE-wrapped): `\1` is a literal 1.
		{name: "case_wrapped_escaped_general",
			sql:  `SELECT COUNT(*) AS n FROM typemx WHERE CASE WHEN c_str LIKE 's\-0000\1%' THEN true ELSE false END`,
			want: []string{fmt.Sprintf("n=int64:%d", prefix00001)}},
		// HAVING over an aggregate: the group survives.
		{name: "having_escaped_prefix",
			sql: `SELECT g, COUNT(*) AS n FROM typemx WHERE g = 0 GROUP BY g ` +
				`HAVING MIN(c_str) LIKE 's\-%'`,
			want: []string{fmt.Sprintf("g=int32:0|n=int64:%d", typematrixGroupCount(t, 0))}},
		// A projected constant: the documented cell.
		{name: "projected_escaped_percent",
			sql:  `SELECT ('a%b' LIKE 'a\%b') AS v FROM typemx WHERE id = 1`,
			want: []string{"v=bool:true"}},
		// A pattern ending in a lone backslash, reached on the first row: 22025
		// on every arm (b0e0f116: 0 rows, the backslash was ordinary).
		{name: "trailing_escape_refuses",
			sql:   `SELECT COUNT(*) AS n FROM typemx WHERE c_str LIKE 's-%\'`,
			state: "22025"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if tc.state != "" {
					if err == nil {
						t.Errorf("%s arm ANSWERED %v; PostgreSQL 17.11 refuses with %s\n  SQL: %s",
							arm.name, got, tc.state, tc.sql)
					} else if st := sqlerr.StateOf(err); st != tc.state {
						t.Errorf("%s arm raised %s, want %s: %v\n  SQL: %s", arm.name, st, tc.state, err, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  SQL: %s", arm.name, err, tc.sql)
					continue
				}
				if strings.Join(got, ";") != strings.Join(tc.want, ";") {
					t.Errorf("%s arm answered\n  got  %v\n  want %v (PostgreSQL 17.11)\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
