// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/worker"
)

// ARC TF ON FIVE ARMS — a table function in FROM is a relation.
//
// The wadjet package asserts what each spelling ANSWERS against live
// PostgreSQL 17.11 and the planner packages where the refusal is MADE. This
// file asserts what neither can see: that the answer, the SQLSTATE and the
// CARRIER agree on single / single+budget / dag / dag-shuffled / dag+morsel4.
//
// It is not a formality here. The declaration this arc gives a table
// function's column — `integer` for a call whose arguments fit int4, which is
// the overload PostgreSQL resolves — is read by the aggregate result-type
// rules, and those run once in the single process and again inside a worker
// fragment. A column that declared int4 in one and int8 in the other would
// answer bigint on one arm and numeric on another for the same SUM.
//
// THE PIN. A table function as the ONLY FROM item is not a DAG stage on this
// engine: `stage scan-0 has no dependencies and no ScanFiles`. That is
// PRE-EXISTING and `distributed` (arc PT measured it with a control cell), so
// every cell whose statement ANSWERS carries it and it is not chased here. A
// cell REFUSED AT PLAN TIME carries no pin, because the refusal is made before
// any stage is emitted — itself the property worth asserting.
func TestArcTFATableFunctionIsARelationOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "tf.json")
	if err := os.WriteFile(jsonPath,
		[]byte("{\"a\":1,\"b\":\"x\"}\n{\"a\":2,\"b\":\"y\"}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

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

	// The DAG arms answer since arc PC (see arc_fr_file_reader_arms_test.go).
	const seriesPin = ""

	for _, tc := range []struct {
		issue, name, sql string
		want             []string
		state            string
		dagPin           string
		pg               string
	}{
		// ---- #1210: an unknown column is 42703 on every arm -------------
		// No pin: a function whose SIGNATURE declares its columns is a closed
		// source in the binder, so the refusal is made before a stage exists.
		{issue: "#1210", name: "an_unknown_column_over_a_series",
			sql: `SELECT zz FROM generate_series(1,2)`, state: "42703",
			pg: `42703 column "zz" does not exist`},
		{issue: "#1210", name: "an_unknown_column_over_unnest",
			sql: `SELECT zz FROM unnest(1,2)`, state: "42703", pg: "42703"},
		{issue: "#1210", name: "the_renamed_away_name_is_unknown",
			sql: `SELECT generate_series FROM generate_series(1,2) AS g(x)`, state: "42703",
			pg: `42703 — AS g(x) publishes x and nothing called generate_series`},
		{issue: "#1210", name: "an_unknown_column_in_a_where",
			sql: `SELECT x FROM generate_series(1,2) AS g(x) WHERE zz = 1`, state: "42703",
			pg: "42703"},
		{issue: "#1210", name: "an_unknown_column_in_an_order_by",
			sql: `SELECT x FROM generate_series(1,2) AS g(x) ORDER BY zz`, state: "42703",
			pg: "42703"},
		{issue: "#1210", name: "an_unknown_column_in_an_aggregate",
			sql: `SELECT COUNT(zz) AS n FROM generate_series(1,2)`, state: "42703", pg: "42703"},
		// A LOCAL FILE READER's refusal is made at PLAN time since arc FR
		// (#1230): the table-function capability is authorized before the
		// statement binds, so the planner reads the file's columns and the
		// binder closes the scope. The pin this cell carried — arc PT's "a
		// table function is not a DAG stage" — started agreeing on all three
		// DAG arms, and deleting it is the proof (ADR-0039 §3).
		{issue: "#1210", name: "an_unknown_column_over_a_reader",
			sql:   `SELECT zz FROM read_json('` + jsonPath + `')`,
			state: "42703", pg: "42703"},
		{issue: "#1184", name: "an_over_long_alias_list_refuses_on_every_arm",
			sql: `SELECT * FROM generate_series(1,3) AS gs(x, y)`, state: "42P10",
			pg: `42P10 table "gs" has 1 columns available but 2 columns specified`},

		// ---- #1211: the column's declaration reaches its consumers ------
		{issue: "#1211", name: "the_series_column_declares_integer",
			sql:    `SELECT x AS v FROM generate_series(1,1) AS g(x)`,
			want:   []string{"v=int32:1"},
			dagPin: seriesPin, pg: "1, declared integer"},
		{issue: "#1211", name: "an_int8_series_column_declares_bigint",
			sql:    `SELECT x AS v FROM generate_series(3000000000,3000000000) AS g(x)`,
			want:   []string{"v=int64:3000000000"},
			dagPin: seriesPin, pg: "3000000000, declared bigint"},
		{issue: "#1211", name: "sum_over_a_series_is_bigint",
			sql:    `SELECT SUM(x) AS v FROM generate_series(1,3) AS g(x)`,
			want:   []string{"v=int64:6"},
			dagPin: seriesPin, pg: "6, declared bigint"},
		{issue: "#1211", name: "min_and_max_keep_the_input_width",
			sql:    `SELECT MIN(x) AS lo, MAX(x) AS hi FROM generate_series(1,3) AS g(x)`,
			want:   []string{"lo=int32:1|hi=int32:3"},
			dagPin: seriesPin, pg: "1|3, both declared integer"},
		{issue: "#1211", name: "count_over_a_series_is_bigint",
			sql:    `SELECT COUNT(x) AS v FROM generate_series(1,3) AS g(x)`,
			want:   []string{"v=int64:3"},
			dagPin: seriesPin, pg: "3, declared bigint"},
		{issue: "#1211", name: "sum_over_unnest_is_bigint",
			sql:    `SELECT SUM(v) AS s FROM unnest(1,2,3) AS u(v)`,
			want:   []string{"s=int64:6"},
			dagPin: seriesPin, pg: "6, declared bigint"},

		// ---- generate_series is PostgreSQL's series ---------------------
		{issue: "#1210", name: "an_empty_series_publishes_its_column",
			sql:    `SELECT x AS v FROM generate_series(3,1) AS g(x)`,
			want:   nil,
			dagPin: seriesPin, pg: "zero rows, one column"},
		{issue: "#1210", name: "a_descending_series_needs_its_negative_step",
			sql:    `SELECT x AS v FROM generate_series(3,1,-1) AS g(x)`,
			want:   []string{"v=int32:3", "v=int32:2", "v=int32:1"},
			dagPin: seriesPin, pg: "3;2;1"},
		// The 22023 is raised when the SOURCE is built, which is after stage
		// emission — so the three DAG arms meet the pin first, exactly as
		// they do for a series that answers.
		{issue: "#1210", name: "a_zero_step_refuses_on_every_arm",
			sql: `SELECT * FROM generate_series(1,3,0)`, state: "22023",
			dagPin: seriesPin, pg: "22023 step size cannot equal zero"},

		// ---- #1203: correlation binds through a table function ----------
		// The OUTER relation is a base table, so these reach stage emission
		// on the DAG arms the way any scalar-subquery query does.
		{issue: "#1203", name: "a_correlated_count_over_a_series",
			sql: `SELECT g, (SELECT COUNT(*) FROM generate_series(1,2) AS s(x) ` +
				`WHERE x <= g) AS c FROM typemx WHERE id <= 2 ORDER BY id`,
			want: tfCorrCountByG(t, 2, 2),
			pg:   "the count of 1..2 that are <= this row's g"},
		{issue: "#1203", name: "a_correlated_count_that_varies_with_the_outer_row",
			sql: `SELECT id, (SELECT COUNT(*) FROM generate_series(1,3) AS s(x) ` +
				`WHERE x <= id) AS c FROM typemx WHERE id <= 3 ORDER BY id`,
			want: tfCorrCountByID(t, 3, 3),
			pg:   "the count of 1..3 that are <= this row's id"},
		{issue: "#1203", name: "a_correlated_max_over_a_series",
			sql: `SELECT id, (SELECT MAX(x) FROM generate_series(1,3) AS s(x) ` +
				`WHERE x <= id) AS c FROM typemx WHERE id <= 2 ORDER BY id`,
			want: tfCorrMaxByID(t, 2, 3),
			pg:   "the largest of 1..3 that is <= this row's id, NULL where none is"},
	} {
		t.Run(tc.issue+"/"+tc.name, func(t *testing.T) {
			pinnedArms := 0
			for _, arm := range arms {
				got, err := arm.run(tc.sql)
				if tc.dagPin != "" && strings.HasPrefix(arm.name, "dag") {
					if err == nil {
						t.Errorf("%s arm ANSWERED %v: the pinned distributed refusal (%q) is "+
							"gone, so delete the pin — that is its proof\n  SQL: %s",
							arm.name, got, tc.dagPin, tc.sql)
						continue
					}
					if !strings.Contains(err.Error(), tc.dagPin) {
						t.Errorf("%s arm refused with something other than the pinned "+
							"mechanism %q: %v\n  SQL: %s", arm.name, tc.dagPin, err, tc.sql)
						continue
					}
					pinnedArms++
					continue
				}
				if tc.state != "" {
					if err == nil {
						t.Errorf("%s arm ANSWERED %v; PostgreSQL 17.11 refuses this with %s\n  SQL: %s",
							arm.name, got, tc.state, tc.sql)
						continue
					}
					if state := sqlerr.StateOf(err); state != tc.state {
						t.Errorf("%s arm raised SQLSTATE %s, want %s\n  err: %v\n  SQL: %s",
							arm.name, state, tc.state, err, tc.sql)
					}
					continue
				}
				if err != nil {
					t.Errorf("%s arm: %v\n  PostgreSQL 17.11: %s\n  SQL: %s",
						arm.name, err, tc.pg, tc.sql)
					continue
				}
				if strings.Join(got, ";") != strings.Join(tc.want, ";") {
					t.Errorf("%s arm answered\n  got  %v\n  want %v (PostgreSQL 17.11: %s)\n  SQL: %s",
						arm.name, got, tc.want, tc.pg, tc.sql)
				}
			}
			if tc.dagPin != "" && pinnedArms == 0 {
				t.Errorf("the pinned distributed refusal %q was produced by NO arm; delete the pin",
					tc.dagPin)
			}
		})
	}
}

// The three correlated cells' expectations are derived from the FIXTURE and
// from PostgreSQL 17.11's semantics for the same shape — `0|0 1|1 2|2 3|3` for
// the count and `NULL 1 2` for the MAX over ids 0..3, measured live against a
// table holding the same ids — never copied from a run of this engine.
//
// The MAX keeps the series column's INTEGER width, which is the half of these
// cells that belongs to #1211: a correlated scalar subquery's declared output
// is resolved from its own plan, so a series column that declares int4 must
// declare int4 there too.

func tfCorrIDsUpTo(t *testing.T, maxID int64) []int64 {
	t.Helper()
	var ids []int64
	for _, r := range typematrix.Data(typematrix.Rows) {
		id, ok := r["id"].(int64)
		if !ok || id > maxID {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

func tfCorrCountByID(t *testing.T, maxID, seriesHi int64) []string {
	t.Helper()
	var out []string
	for _, id := range tfCorrIDsUpTo(t, maxID) {
		n := int64(0)
		for x := int64(1); x <= seriesHi; x++ {
			if x <= id {
				n++
			}
		}
		out = append(out, fmt.Sprintf("id=int64:%d|c=int64:%d", id, n))
	}
	return out
}

func tfCorrCountByG(t *testing.T, maxID, seriesHi int64) []string {
	t.Helper()
	byID := map[int64]int32{}
	for _, r := range typematrix.Data(typematrix.Rows) {
		id, ok := r["id"].(int64)
		if !ok {
			continue
		}
		if g, ok := r["g"].(int32); ok {
			byID[id] = g
		}
	}
	var out []string
	for _, id := range tfCorrIDsUpTo(t, maxID) {
		g := byID[id]
		n := int64(0)
		for x := int64(1); x <= seriesHi; x++ {
			if x <= int64(g) {
				n++
			}
		}
		out = append(out, fmt.Sprintf("g=int32:%d|c=int64:%d", g, n))
	}
	return out
}

func tfCorrMaxByID(t *testing.T, maxID, seriesHi int64) []string {
	t.Helper()
	var out []string
	for _, id := range tfCorrIDsUpTo(t, maxID) {
		best := int64(0)
		for x := int64(1); x <= seriesHi; x++ {
			if x <= id && x > best {
				best = x
			}
		}
		if best == 0 {
			out = append(out, fmt.Sprintf("id=int64:%d|c=NULL", id))
			continue
		}
		out = append(out, fmt.Sprintf("id=int64:%d|c=int32:%d", id, best))
	}
	return out
}
