// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A VOLATILE CTE READ MORE THAN ONCE IS EVALUATED ONCE ON THE SINGLE-PROCESS
// PATH, AND THE DISTRIBUTED PATH IS PINNED AS IT IS (#1531, ADR-0021 §2d).
//
// PostgreSQL 17.11 never inlines a CTE whose body is volatile and materializes
// any CTE referenced more than once, so every reference to
// `WITH s AS (SELECT id, random() AS r …)` reads the same rows. At c67ebf5b this
// engine evaluated the body once per reference wherever the reference was
// not a tag in the root's plan tree: from an expression subquery's text (the
// issue's `(SELECT r FROM s) <> (SELECT r FROM s)`, 3 for PostgreSQL's 0), from
// a nested block's WITH, from a set operation at the statement root, and from
// INSERT … WITH … UNION ALL. The single-process planner now serves every
// reference to a volatile WITH item from one evaluation (physical/once_cte.go).
//
// Three cells pin a spelling the parser refuses on every arm where PostgreSQL
// answers: `AS MATERIALIZED`, `AS NOT MATERIALIZED` (42601) and `WITH … INSERT`
// (other.md r25, r26).
//
// The table is the round-1 enumeration (reference count × position × body) plus
// the round-2 consumer-filter cells and the UNBEGUN ones — a reference under a
// constant-false filter or a LIMIT 0, which PostgreSQL never begins and so
// never evaluates the body for (`… TABLESAMPLE BERNOULLI (101) … WHERE false`
// is 0 there; read, it is 2202H), on seven arms; cmCells says what each arm
// asserts. The two embedded arms assert PostgreSQL's answer on every cell, eight
// times. The stage-DAG arms are NOT changed by this rule: a cell PostgreSQL
// answers and the DAG does not is pinned NOT (docs/adr/0012-divergences/other.md
// r43–r45: M4 a shared stage that is the scan, M5 a subquery run eagerly on the
// coordinator, M6 the asynchronous door), and a pin FAILS when the arm starts
// agreeing. A cell the DAG routes to the coordinator-local pipeline (a
// table-less SELECT, a SELECT-list subquery, the small-query fast path) runs
// this rule and asserts PostgreSQL's answer.
func TestArcCMVolatileCTEReadTwiceIsEvaluatedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: three DAG arms stand up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	arms := cmArms(t, ctx)
	for _, c := range cmCells {
		t.Run(c.name, func(t *testing.T) {
			for i, arm := range arms {
				want := c.expect[i]
				// Eight times on the two embedded arms; a distributed arm's
				// answer is asserted twice (PG, NOT) or once (ERR), and a
				// nondeterministic one (ANY) is measured in the notes, not run.
				reps := map[string]int{"PG": 2, "NOT": 2, "ERR": 1, "ANY": 0}[want]
				if strings.HasPrefix(want, "ERR ") {
					reps = 1
				}
				if i < 2 {
					reps = 8
				}
				for rep := 0; rep < reps; rep++ {
					got := arm.answer(c.sql)
					if !cmMatches(got, want, c.pg) {
						t.Errorf("%s rep %d: %s\n  got  %s\n  want %s (PostgreSQL 17.11: %s)",
							arm.name, rep, c.sql, got, want, c.pg)
						break
					}
				}
			}
		})
	}
}

type cmCell struct {
	name, sql, pg string
	expect        [7]string
}

func cmMatches(got, want, pg string) bool {
	isErr := strings.HasPrefix(got, "ERR") || strings.HasPrefix(got, "PANIC")
	if strings.HasPrefix(want, "ERR ") {
		// A coded refusal pinned on every arm (a spelling the parser does
		// not take): the pin FAILS when the spelling starts to answer.
		return strings.HasPrefix(got, want)
	}
	switch want {
	case "PG":
		if strings.HasPrefix(pg, "ERR ") {
			return strings.HasPrefix(got, pg)
		}
		return got == pg
	case "NOT":
		return !isErr && got != pg
	case "ERR":
		return isErr
	}
	return true // ANY
}

type cmArm struct {
	name string
	run  func(string) string
	exec func(string) string
}

// answer runs a cell: a SELECT's rendered answer, or for a `DML:` cell the
// store-and-read-back count of ids that carry two values (PostgreSQL 0).
func (a cmArm) answer(sql string) string {
	if !strings.HasPrefix(sql, "DML:") {
		return a.run(sql)
	}
	a.exec("DELETE FROM cm_t")
	if r := a.exec(strings.TrimPrefix(sql, "DML:")); strings.HasPrefix(r, "ERR") {
		return "ERR"
	}
	return a.run("SELECT count(*) FROM (SELECT id FROM cm_t GROUP BY id HAVING count(DISTINCT r) <> 1) g")
}

func cmArms(t *testing.T, ctx context.Context) []cmArm {
	t.Helper()
	tables := append(tbTables(), tmdTable{"cm_t", parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64}, {Name: "r", Type: parquet.TypeString, Nullable: true}}}, nil})
	open := func(budget int64) *wadjet.DB {
		db := tbStandalone(t, ctx, budget)
		tb := tables[len(tables)-1]
		if err := db.CreateTable(ctx, tb.name, tb.schema, nil); err != nil {
			t.Fatalf("create cm_t: %v", err)
		}
		return db
	}
	single := open(0)
	spilled := open(512 * 1024)
	stand := func(wcfg func(*worker.Config), opts ...func(*Config)) *Coordinator {
		infra := tmdInfra(t, ctx)
		tmdWriteTableList(t, ctx, infra, nil, tables)
		return tmdCoordinatorWithWorkers(t, ctx, infra, wcfg, opts...)
	}
	coord := stand(nil)
	coordB := stand(nil, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := stand(func(w *worker.Config) { w.MorselWorkers = 4 })
	coordS := stand(nil, func(c *Config) { c.LocalFastPathBytes = 64 << 10 })
	execDB := func(db *wadjet.DB) func(string) string {
		return func(s string) string {
			if _, err := db.Execute(ctx, s); err != nil {
				return tbErr(err)
			}
			return "OK"
		}
	}
	execC := func(c *Coordinator) func(string) string {
		return func(s string) string { return tbRunDAG(ctx, c, s) }
	}
	return []cmArm{
		{"single", func(s string) string { return tbRunSingle(ctx, single, s) }, execDB(single)},
		{"spilled512k", func(s string) string { return tbRunSingle(ctx, spilled, s) }, execDB(spilled)},
		{"dag", func(s string) string { return tbRunDAG(ctx, coord, s) }, execC(coord)},
		{"dag-shuffled", func(s string) string { return tbRunDAG(ctx, coordB, s) }, execC(coordB)},
		{"dag-morsel4", func(s string) string { return tbRunDAG(ctx, coordM, s) }, execC(coordM)},
		{"dag-fastpath64k", func(s string) string { return tbRunDAG(ctx, coordS, s) }, execC(coordS)},
		{"async", func(s string) string { return tbdAsync(ctx, coord, s) }, execC(coord)},
	}
}
