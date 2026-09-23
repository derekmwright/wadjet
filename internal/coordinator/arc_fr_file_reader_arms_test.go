// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
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
	// #1266: a reader's TIMESTAMP in the engine's unit (epoch ms). At
	// 260fc569 both readers stored microseconds and these read 56425-08-29.
	tsc := write("ts.csv", "k,ts\n1,2024-06-15 12:30:45.5\n2,1969-07-20 20:17:40.123\n")
	tsj := write("ts.json", "{\"k\":1,\"ts\":\"2024-06-15T12:30:45.5Z\"}\n"+
		"{\"k\":2,\"ts\":\"1969-07-20T20:17:40.123Z\"}\n")

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

	// The DAG arms answer since arc PC routes a table-function scan to the
	// coordinator-local pipeline; the pin that recorded their refusal is gone.
	const readerPin = ""

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
		// ---- #1266: the reader's TIMESTAMP is the engine's unit ---------
		{issue: "#1266", name: "read_csv_timestamp_is_epoch_millis",
			sql: `SELECT k, CAST(ts AS VARCHAR) AS s, ts = TIMESTAMP '2024-06-15 12:30:45.5' AS eq FROM read_csv('` +
				tsc + `') ORDER BY ts`,
			want:   []string{"k=int64:2|s=1969-07-20 20:17:40.123|eq=bool:false", "k=int64:1|s=2024-06-15 12:30:45.5|eq=bool:true"},
			dagPin: readerPin, pg: "2 | 1969-07-20 20:17:40.123 | f, 1 | 2024-06-15 12:30:45.5 | t"},
		{issue: "#1266", name: "read_json_timestamp_is_epoch_millis",
			sql: `SELECT k, CAST(ts AS VARCHAR) AS s, ts = TIMESTAMP '2024-06-15 12:30:45.5' AS eq FROM read_json('` +
				tsj + `') ORDER BY ts`,
			want:   []string{"k=int64:2|s=1969-07-20 20:17:40.123|eq=bool:false", "k=int64:1|s=2024-06-15 12:30:45.5|eq=bool:true"},
			dagPin: readerPin, pg: "2 | 1969-07-20 20:17:40.123 | f, 1 | 2024-06-15 12:30:45.5 | t"},
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

// ARC FR — THE COORDINATOR DOOR AUTHORIZES BEFORE IT READS.
//
// `internal/server`'s own gate holds the embedded, pgwire, HTTP and gRPC
// doors to it. This is the same property for the two doors that live here:
// `ExecuteSQL` (the small-query fast path AND the stage DAG) and the async /
// EXPLAIN entry, both of which plan a statement of their own and both of
// which call `auth.AuthorizeTableFunctions` before the binder.
//
// `physical.ReaderSchemaReads` counts the times the planner OPENED a reader's
// input. The counter is the discriminator, because the 42501 alone is not
// one: a plan that read the file and THEN refused answers 42501 too.
func TestArcFRTheCoordinatorDoorAuthorizesBeforeItReads(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	dir := t.TempDir()
	csvPath := filepath.Join(dir, "frcoord.csv")
	if err := os.WriteFile(csvPath, []byte("secret\nserver-local-value\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	// `reader` holds every RELATION and no table-function capability; `ops`
	// holds `admin`, which is what the legacy-role path requires.
	authn, authz := auth.New(auth.Config{
		Enabled: true,
		APIKeys: []auth.APIKeyDef{
			{Key: "fr-reader", Name: "reader", Role: "reader"},
			{Key: "fr-ops", Name: "ops", Role: "ops"},
		},
		Roles: []auth.RoleConfig{
			{Name: "reader", Tables: []string{"*"}, Allow: []string{"read"}},
			{Name: "ops", Tables: []string{"*"}, Allow: []string{"read", "write", "admin"}},
		},
	})
	provider := auth.NewProvider(authn, authz, nil, nil)
	readerCtx := auth.ContextWithIdentity(ctx, &auth.Identity{
		Name: "reader", Role: "reader", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read"}})
	opsCtx := auth.ContextWithIdentity(ctx, &auth.Identity{
		Name: "ops", Role: "ops", Method: "apikey",
		Tables: []string{"*"}, Perms: []string{"read", "write", "admin"}})

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	fast := tmdCoordinator(t, ctx, infra, func(c *Config) { c.LocalFastPathBytes = DefaultLocalFastPathBytes })
	fast.SetAuthProvider(provider)
	dag := tmdCoordinator(t, ctx, infra)
	dag.SetAuthProvider(provider)

	doors := []struct {
		name string
		run  func(context.Context, string) error
	}{
		{"coordinator/fastpath", func(c context.Context, sql string) error {
			res, err := fast.ExecuteSQL(c, sql)
			if err != nil {
				return err
			}
			if res != nil && res.Error != "" {
				return fmt.Errorf("%s", res.Error)
			}
			return nil
		}},
		{"coordinator/dag", func(c context.Context, sql string) error {
			res, err := dag.ExecuteSQL(c, sql)
			if err != nil {
				return err
			}
			if res != nil && res.Error != "" {
				return fmt.Errorf("%s", res.Error)
			}
			return nil
		}},
	}

	// An input that can be read ONCE. The resolver declines it even for an
	// authorized identity, but deciding that expands and stats the path,
	// which moves `physical.ReaderSchemaReads` — so a ZERO here is the guard
	// running before the path is touched at all, not the resolver declining.
	deniedFifo := filepath.Join(dir, "frcoord_denied.fifo")
	if err := syscall.Mkfifo(deniedFifo, 0o600); err != nil {
		t.Skipf("this platform has no FIFO: %v", err)
	}

	cells := []struct{ name, sql string }{
		{"direct", `SELECT * FROM read_csv('` + csvPath + `')`},
		// Nested on purpose: a reader in the statement's own FROM list is
		// refused by the early pass before the planner is reached, so it
		// cannot tell whether the resolver would have stat'd the path. A
		// nested one is SQL text then and reaches the resolver, where the
		// context guard is the only thing between the identity and the stat.
		{"a_read_once_input_in_a_cte",
			`WITH c AS (SELECT * FROM read_csv('` + deniedFifo + `')) SELECT * FROM c`},
		{"a_glob_holding_a_read_once_input_in_a_derived_table",
			`SELECT * FROM (SELECT * FROM read_csv('` +
				filepath.Join(dir, "frcoord*.fifo") + `')) d`},
		{"star_qualified", `SELECT f.* FROM read_csv('` + csvPath + `') AS f`},
		{"cte", `WITH c AS (SELECT * FROM read_csv('` + csvPath + `')) SELECT * FROM c`},
		{"derived_table", `SELECT * FROM (SELECT * FROM read_csv('` + csvPath + `')) d`},
		{"scalar_subquery", `SELECT (SELECT COUNT(*) FROM read_csv('` + csvPath + `')) AS c`},
		{"join_arm", `SELECT t.id FROM typemx t JOIN read_csv('` + csvPath +
			`') f ON t.c_str = f.secret`},
	}

	for _, d := range doors {
		d := d
		t.Run("refused-identity-opens-nothing/"+d.name, func(t *testing.T) {
			for _, c := range cells {
				before := physical.ReaderSchemaReads.Load()
				err := d.run(readerCtx, c.sql)
				if err == nil {
					t.Errorf("%s/%s: answered where the policy grants no table-function capability",
						d.name, c.name)
				} else if state := sqlerr.StateOf(err); state != "42501" {
					t.Errorf("%s/%s: SQLSTATE %q, want 42501: %v", d.name, c.name, state, err)
				}
				if after := physical.ReaderSchemaReads.Load(); after != before {
					t.Errorf("%s/%s: the planner opened the reader's input %d time(s) for an "+
						"identity the policy refuses — the capability must be decided first",
						d.name, c.name, after-before)
				}
			}
		})
		t.Run("authorized-identity-reads/"+d.name, func(t *testing.T) {
			before := physical.ReaderSchemaReads.Load()
			if err := d.run(opsCtx, `SELECT f.* FROM read_csv('`+csvPath+`') AS f`); err != nil &&
				!strings.Contains(err.Error(), "no dependencies and no ScanFiles") {
				t.Fatalf("%s: an admin identity must read the file: %v", d.name, err)
			}
			if physical.ReaderSchemaReads.Load() == before {
				t.Errorf("%s: the planner read NO schema for an authorized identity, so the "+
					"zero above proves nothing — the counter is not wired to this door", d.name)
			}
		})
	}
}
