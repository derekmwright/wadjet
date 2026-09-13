package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/worker"
	"github.com/derekmwright/wadjet/wadjet"
)

// A table a QUERY wrote reads back identically on every execution arm (#1024).
//
// The write itself is single-process — every write in this engine is, and the
// coordinator refuses them by name (see the second test below, and the arc's
// notes for the mechanism that would lift it). What this gate is about is the
// other side: the FILE the write produced is an ordinary table file, and the
// five arms must agree about what is in it. A writer that produced something
// only the arm that wrote it can read would be silent data loss for every
// distributed reader, and no gate over the fixture tables can see it, because
// those files are written by the test harness rather than by a statement.
//
// Both fixture tables are copied: `typemx` is flat and takes the native
// columnar decoder, `typemx_nested` carries the four container types and routes
// to the row reader.
func TestATableAQueryWroteReadsTheSameOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Minute)
	t.Cleanup(cancel)

	// ONE catalog and store behind every arm, so the table the write creates
	// is the table each arm reads.
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)

	open := func(budget int64) *wadjet.DB {
		t.Helper()
		cfg := wadjet.Config{MetaKV: infra.kv, Store: infra.store, Bucket: "test", MemoryBudget: budget}
		if budget > 0 {
			cfg.SpillDir = t.TempDir()
		}
		db, err := wadjet.Open(ctx, cfg)
		if err != nil {
			t.Fatalf("open over the shared catalog: %v", err)
		}
		t.Cleanup(func() { db.Close() })
		return db
	}
	writer := open(0)
	spilled := open(512 * 1024)
	coord := tmdCoordinator(t, ctx, infra)
	coordB := tmdCoordinator(t, ctx, infra, func(c *Config) { c.BroadcastBytesOverride = 1 })
	coordM := tmdCoordinatorWithWorkers(t, ctx, infra, func(w *worker.Config) { w.MorselWorkers = 4 })

	arms := []struct {
		name string
		run  func(sql string) ([]string, error)
	}{
		{"single", func(sql string) ([]string, error) { return na2Run(tmdRunSingle(ctx, writer, sql)) }},
		{"single+budget", func(sql string) ([]string, error) {
			restoreDrain := exec.ForceAggDrainEvery(1)
			restoreRuns := exec.ForceSmallSpillRuns(512)
			out, err := na2Run(tmdRunSingle(ctx, spilled, sql))
			restoreRuns()
			exec.ForceAggDrainEvery(restoreDrain)
			return out, err
		}},
		{"dag", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coord, sql)) }},
		{"dag-shuffled", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordB, sql)) }},
		{"dag-morsel4", func(sql string) ([]string, error) { return na2Run(tmdRunDAG(ctx, coordM, sql)) }},
	}

	for _, src := range []string{typematrix.Table, typematrix.Nested} {
		t.Run(src, func(t *testing.T) {
			dst := src + "_written"
			res, err := writer.Execute(ctx, fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", dst, src))
			if err != nil {
				t.Fatalf("the write: %v", err)
			}
			if res.Tag() != fmt.Sprintf("SELECT %d", typematrix.Rows) {
				t.Errorf("command tag %q, want SELECT %d", res.Tag(), typematrix.Rows)
			}

			want, err := na2Run(tmdRunSingle(ctx, writer, "SELECT * FROM "+src+" ORDER BY id"))
			if err != nil {
				t.Fatalf("reading the source: %v", err)
			}
			for _, arm := range arms {
				got, err := arm.run("SELECT * FROM " + dst + " ORDER BY id")
				if err != nil {
					t.Errorf("%s: %v", arm.name, err)
					continue
				}
				if len(got) != len(want) {
					t.Errorf("%s: %d rows, want %d", arm.name, len(got), len(want))
					continue
				}
				for i := range want {
					if got[i] != want[i] {
						t.Errorf("%s: row %d differs\n  written %s\n  source  %s",
							arm.name, i, got[i], want[i])
						break
					}
				}
			}
		})
	}
}

// The coordinator runs no writes, and says so by name.
//
// This is the arc's recorded BOUNDARY and it is a claim, so it has a fixture
// from both sides: the query-sourced write is refused 0A000 naming the
// statement, nothing is dispatched, and the identical bare SELECT still
// answers on the same coordinator.
func TestTheCoordinatorRefusesAQuerySourcedWriteByName(t *testing.T) {
	if testing.Short() {
		t.Skip("-short")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)

	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)

	cases := []struct {
		sql  string
		says string
	}{
		{fmt.Sprintf("CREATE TABLE refused_a AS SELECT id FROM %s", typematrix.Table), "CREATE TABLE AS SELECT"},
		{fmt.Sprintf("INSERT INTO %s SELECT id FROM %s", typematrix.Table, typematrix.Table), "INSERT"},
	}
	for _, tc := range cases {
		t.Run(tc.says, func(t *testing.T) {
			_, err := coord.ExecuteSQL(ctx, tc.sql)
			if err == nil {
				t.Fatal("the coordinator ran a write")
			}
			if got := sqlerr.StateOf(err); got != "0A000" {
				t.Errorf("SQLSTATE %q, want 0A000: %v", got, err)
			}
			if !strings.Contains(err.Error(), tc.says) {
				t.Errorf("the refusal does not name the statement: %v", err)
			}
			// Nothing was dispatched: no stage output under queries/.
			objs, lerr := infra.store.List(ctx, "test", objstore.ListOptions{Prefix: "queries/"})
			if lerr != nil {
				t.Fatal(lerr)
			}
			if len(objs) != 0 {
				t.Errorf("the refusal dispatched %d stage outputs; it must precede dispatch", len(objs))
			}
		})
	}

	// The boundary from the other side: the same coordinator answers the bare
	// query.
	if _, err := na2Run(tmdRunDAG(ctx, coord,
		fmt.Sprintf("SELECT id FROM %s ORDER BY id LIMIT 3", typematrix.Table))); err != nil {
		t.Errorf("the coordinator refused the bare query too: %v", err)
	}
}
