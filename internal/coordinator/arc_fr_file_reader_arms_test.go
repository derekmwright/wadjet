// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/worker"
)

// ARC FR ON FIVE ARMS — a file reader is a relation with a schema.
//
// The wadjet package asserts what each spelling ANSWERS against PostgreSQL
// 17.11's behaviour for an ordinary relation of the same schema. This file
// asserts what it cannot see: that the answer and the SQLSTATE agree on
// single / single+budget / dag / dag-shuffled / dag+morsel4.
//
// THE PIN, carried from arc PT and arc TF: a table function as the ONLY
// source of a stage is not a DAG stage on this engine — `stage scan-0 has no
// dependencies and no ScanFiles`. It is PRE-EXISTING and `distributed`, so a
// cell whose statement has no catalog relation in it carries the pin on the
// three DAG arms rather than chasing it.
func TestArcFRAFileReaderIsARelationOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	t.Setenv("WADJET_TEST_NO_READER_SCHEMA", "1")
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)

	dir := t.TempDir()
	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	j1 := write("r1.json", "{\"a\":1,\"b\":\"p\"}\n{\"a\":2,\"b\":\"q\"}\n"+
		"{\"a\":3,\"b\":\"r\"}\n{\"a\":4,\"b\":\"s\"}\n")
	j2 := write("r2.json", "{\"c\":2,\"d\":\"x\"}\n{\"c\":3,\"d\":\"y\"}\n")
	c1 := write("r1.csv", "a,b\n1,p\n2,q\n3,r\n4,s\n")
	c2 := write("r2.csv", "c,d\n2,x\n3,y\n")

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

	const readerPin = "no dependencies and no ScanFiles"

	rj1, rj2 := "read_json('"+j1+"')", "read_json('"+j2+"')"
	rc1, rc2 := "read_csv('"+c1+"')", "read_csv('"+c2+"')"

	for _, tc := range []struct {
		issue, name, sql string
		want             []string
		dagPin           string
		pg               string
	}{
		// ---- #1229: the ON keys whichever way round it is written -------
		{issue: "#1229", name: "two_readers_right_arm_first",
			sql:    `SELECT COUNT(*) AS n FROM ` + rj1 + ` r1 JOIN ` + rj2 + ` r2 ON r2.c = r1.a`,
			want:   []string{"n=int64:2"},
			dagPin: readerPin, pg: "2"},
		{issue: "#1229", name: "two_readers_left_arm_first",
			sql:    `SELECT COUNT(*) AS n FROM ` + rj1 + ` r1 JOIN ` + rj2 + ` r2 ON r1.a = r2.c`,
			want:   []string{"n=int64:2"},
			dagPin: readerPin, pg: "2"},
		{issue: "#1229", name: "two_csv_readers_right_arm_first",
			sql:    `SELECT COUNT(*) AS n FROM ` + rc1 + ` r1 JOIN ` + rc2 + ` r2 ON r2.c = r1.a`,
			want:   []string{"n=int64:2"},
			dagPin: readerPin, pg: "2"},
		{issue: "#1229", name: "two_readers_left_join_right_arm_first",
			sql:    `SELECT COUNT(*) AS n FROM ` + rj1 + ` r1 LEFT JOIN ` + rj2 + ` r2 ON r2.c = r1.a`,
			want:   []string{"n=int64:4"},
			dagPin: readerPin, pg: "4"},
		{issue: "#1229", name: "two_readers_comma_and_where_right_arm_first",
			sql:    `SELECT COUNT(*) AS n FROM ` + rj1 + ` r1, ` + rj2 + ` r2 WHERE r2.c = r1.a`,
			want:   []string{"n=int64:2"},
			dagPin: readerPin, pg: "2"},
		{issue: "#1229", name: "a_cross_join_of_two_readers_is_still_the_product",
			sql:    `SELECT COUNT(*) AS n FROM ` + rj1 + ` r1 CROSS JOIN ` + rj2 + ` r2`,
			want:   []string{"n=int64:8"},
			dagPin: readerPin, pg: "8"},
		// A reader joined to a CATALOG relation meets the SAME pin: the
		// reader arm is its own stage and that stage has no ScanFiles. The
		// cell is kept because it is the one that would come back first if
		// the pin were ever closed, and because the single arms measure the
		// rule over a MIXED join rather than two readers.
		{issue: "#1229", name: "a_reader_joined_to_a_catalog_table_right_arm_first",
			sql:    `SELECT COUNT(*) AS n FROM typemx t JOIN ` + rj1 + ` r1 ON r1.a = t.id`,
			want:   []string{"n=int64:4"},
			dagPin: readerPin, pg: "4 — ids 1..4 of the type matrix meet a = 1..4"},
		{issue: "#1229", name: "a_reader_joined_to_a_catalog_table_left_arm_first",
			sql:    `SELECT COUNT(*) AS n FROM typemx t JOIN ` + rj1 + ` r1 ON t.id = r1.a`,
			want:   []string{"n=int64:4"},
			dagPin: readerPin, pg: "4"},
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
