package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/wadjet"
)

// #483 on the stage DAG, in the exact wiring standalone runs.
//
// cmd/wadjet's runStandalone builds ONE NATS KV and hands it to two catalog
// owners: the coordinator (SELECT, via pgSrv.SetCoordinator) and a
// wadjet.Open DB (INSERT/UPDATE/DELETE and DDL). This test reproduces that —
// a DAG-only coordinator over tmdInfra's catalog, plus a DB over the same KV
// and store — so a write through the DB has to be visible to the coordinator's
// very next query, across three worker processes' own per-task catalogs.
//
// The DAG arm matters on its own: a scan task builds a fresh
// catalog.Catalog per pipeline task, so it never held a stale manifest —
// which means the DAG's staleness came entirely from what the COORDINATOR
// planned with. Both are covered here, since the answer only comes back
// correct when both are fresh.
func mfdSetup(t *testing.T, ctx context.Context) (*Coordinator, *wadjet.DB) {
	t.Helper()
	infra := tmdInfra(t, ctx)
	coord := tmdCoordinator(t, ctx, infra)
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger,
	})
	if err != nil {
		t.Fatalf("open DB over the coordinator's KV: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return coord, db
}

// mfdWrite sends DDL through Query and DML through Execute, the same split
// pgwire makes for statements it does not route to the coordinator.
func mfdWrite(t *testing.T, db *wadjet.DB, sql string) {
	t.Helper()
	ctx := context.Background()
	switch strings.ToUpper(strings.Fields(strings.TrimSpace(sql))[0]) {
	case "INSERT", "UPDATE", "DELETE":
		if _, err := db.Execute(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	default:
		if _, err := db.Query(ctx, sql); err != nil {
			t.Fatalf("%s: %v", sql, err)
		}
	}
}

// mfdSelect runs a query through the coordinator's stage DAG and returns one
// column, sorted so the comparison does not depend on task ordering.
func mfdSelect(t *testing.T, ctx context.Context, coord *Coordinator, sql, col string) []string {
	t.Helper()
	res, err := tmdRunDAG(ctx, coord, sql)
	if err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
	out := make([]string, 0, len(res.Rows))
	for _, row := range res.Rows {
		out = append(out, fmt.Sprint(row[col]))
	}
	sort.Strings(out)
	return out
}

func mfdWant(t *testing.T, got, want []string, what string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %v, want %v", what, got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("%s: %v, want %v", what, got, want)
		}
	}
}

func TestDistributedSelectAfterWriteSeesTheWrite(t *testing.T) {
	ctx := context.Background()
	coord, db := mfdSetup(t, ctx)

	mfdWrite(t, db, "CREATE TABLE dtp6 (c0 BIGINT, c1 TEXT)")
	mfdWrite(t, db, "INSERT INTO dtp6 VALUES (1, 'a')")
	mfdWant(t, mfdSelect(t, ctx, coord, "SELECT c0, c1 FROM dtp6", "c0"), []string{"1"}, "first read")

	mfdWrite(t, db, "INSERT INTO dtp6 VALUES (2, 'b')")
	mfdWant(t, mfdSelect(t, ctx, coord, "SELECT c0, c1 FROM dtp6", "c0"), []string{"1", "2"},
		"after an INSERT through the other catalog owner")

	// A never-before-run shape against the same table, to rule out a text- or
	// plan-keyed result cache as the thing under test.
	mfdWrite(t, db, "INSERT INTO dtp6 VALUES (3, 'c')")
	mfdWant(t, mfdSelect(t, ctx, coord, "SELECT c1, c0 FROM dtp6 WHERE c0 > 0", "c0"),
		[]string{"1", "2", "3"}, "after a third INSERT, new query shape")

	// DELETE was the documented gap on this arm until #491: the DAG's scan
	// source is built from the task's file list, and nothing on the wire
	// said which rows a merge-on-read DELETE had removed, so every DAG scan
	// answered with them still in it while the single-process path (which
	// reads the manifest at scan Init) did not. Now asserted, on the same
	// three-worker DAG as the rest of this test.
	mfdWrite(t, db, "DELETE FROM dtp6 WHERE c0 = 2")
	mfdWant(t, mfdSelect(t, ctx, coord, "SELECT c0, c1 FROM dtp6", "c0"), []string{"1", "3"},
		"after a DELETE through the other catalog owner")
	mfdWant(t, mfdSelect(t, ctx, coord, "SELECT COUNT(*) AS n FROM dtp6", "n"), []string{"2"},
		"COUNT(*) after the DELETE")
}

func TestDistributedDropAndRecreateDoesNotServeThePreviousIncarnation(t *testing.T) {
	ctx := context.Background()
	coord, db := mfdSetup(t, ctx)

	mfdWrite(t, db, "CREATE TABLE drepro (c0 BIGINT, c1 TEXT)")
	mfdWrite(t, db, "INSERT INTO drepro VALUES (1, 'hello')")
	mfdWant(t, mfdSelect(t, ctx, coord, "SELECT * FROM drepro", "c1"), []string{"hello"}, "first incarnation")

	mfdWrite(t, db, "DROP TABLE drepro")
	mfdWrite(t, db, "CREATE TABLE drepro (c0 BIGINT, c1 BIGINT)")
	mfdWrite(t, db, "INSERT INTO drepro VALUES (2, 999)")

	// The dropped incarnation's file stores c1 as STRING. DROP TABLE removes
	// the catalog entries immediately; the underlying object is not deleted
	// synchronously (#494's catalog.Catalog.FlushDroppedTableFiles reclaims
	// it later, behind a grace period and a live-manifest guard, and only
	// where a process opts in — see compaction.BackgroundConfig.
	// ReclaimDroppedTables), so the file is still physically present here.
	// It must not reach this scan under the new schema, on any worker,
	// regardless of whether or when it is eventually reclaimed.
	mfdWant(t, mfdSelect(t, ctx, coord, "SELECT * FROM drepro", "c1"), []string{"999"},
		"recreated table with a new column type")
}
