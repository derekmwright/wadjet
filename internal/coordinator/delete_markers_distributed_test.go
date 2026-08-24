package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/storage/compaction"
	"github.com/derekmwright/wadjet/wadjet"
)

// #491: merge-on-read DELETE on the stage DAG.
//
// A DELETE marks file-absolute row indices in the manifest instead of
// rewriting parquet. The single-process engine reads those markers at scan
// Init; the DAG's workers read a file list handed to them by the
// coordinator, and until this fix nothing on the wire told them which rows
// to skip — so every DAG scan of a table with deletes returned them.
//
// The arms here are the two paths over ONE catalog and ONE object store,
// which is what makes a divergence a defect rather than a fixture
// difference: dmdRun executes each query through the coordinator's stage
// DAG (LocalFastPathBytes=0, three real worker processes) and through the
// embedded single-process engine, and compares.
//
// The shapes are chosen for where a marker can go missing, not for SQL
// variety: a bare projection (the gather task reads base parquet itself,
// with no scan task in between), COUNT(*) (the metadata fast paths have
// their own marker check), GROUP BY (fused scan-aggregate + shuffle), and a
// JOIN (the build side reads its own files under its own alias — the
// carrier #423 needed a third field for).

// dmdCheck runs one SQL statement on both paths and fails on any difference.
func dmdCheck(t *testing.T, ctx context.Context, coord *Coordinator, db *wadjet.DB, sql string) {
	t.Helper()
	want, err := tmdRunSingle(ctx, db, sql)
	if err != nil {
		t.Fatalf("single-process %s: %v", sql, err)
	}
	got, err := tmdRunDAG(ctx, coord, sql)
	if err != nil {
		t.Fatalf("DAG %s: %v", sql, err)
	}
	if diff := oracle.Compare(want, got, oracle.CompareSpec{Mode: oracle.CmpUnordered}); diff != "" {
		t.Errorf("two paths disagree on %q: %s\n  single-process: %v\n  DAG:            %v",
			sql, diff, want.Rows, got.Rows)
	}
}

// dmdRows returns the DAG arm's single named column, sorted.
func dmdRows(t *testing.T, ctx context.Context, coord *Coordinator, sql, col string) []string {
	t.Helper()
	return mfdSelect(t, ctx, coord, sql, col)
}

// A DELETE must be invisible to every DAG shape, and the two paths must
// agree on all of them.
func TestDistributedScanHonorsDeleteMarkers(t *testing.T) {
	ctx := context.Background()
	coord, db := mfdSetup(t, ctx)

	mfdWrite(t, db, "CREATE TABLE dmd_a (k BIGINT, g TEXT, v BIGINT)")
	// One INSERT per statement, so the table is several files and the
	// markers land in only SOME of them — the shape where a per-file map
	// keyed by path has to be consulted per file rather than applied
	// wholesale.
	for i := 1; i <= 6; i++ {
		mfdWrite(t, db, fmt.Sprintf("INSERT INTO dmd_a VALUES (%d, '%s', %d)", i, []string{"x", "y"}[i%2], i*10))
	}
	mfdWrite(t, db, "CREATE TABLE dmd_b (k BIGINT, label TEXT)")
	for i := 1; i <= 6; i++ {
		mfdWrite(t, db, fmt.Sprintf("INSERT INTO dmd_b VALUES (%d, 'L%d')", i, i))
	}

	shapes := []string{
		"SELECT k, g, v FROM dmd_a",
		"SELECT COUNT(*) AS n FROM dmd_a",
		"SELECT SUM(v) AS s FROM dmd_a",
		"SELECT g, COUNT(*) AS n, SUM(v) AS s FROM dmd_a GROUP BY g",
		"SELECT k, v FROM dmd_a WHERE v > 20",
		"SELECT a.k, a.v, b.label FROM dmd_a a JOIN dmd_b b ON a.k = b.k",
		"SELECT COUNT(*) AS n FROM dmd_a a JOIN dmd_b b ON a.k = b.k",
		"SELECT MIN(v) AS lo, MAX(v) AS hi FROM dmd_a",
		"SELECT DISTINCT g FROM dmd_a",
	}

	for _, sql := range shapes {
		dmdCheck(t, ctx, coord, db, sql)
	}

	// Now delete, and re-run every shape. Deleting from BOTH tables also
	// covers the join's build side, which reads its own files under its own
	// alias.
	mfdWrite(t, db, "DELETE FROM dmd_a WHERE k = 3")
	mfdWrite(t, db, "DELETE FROM dmd_b WHERE k = 5")
	for _, sql := range shapes {
		dmdCheck(t, ctx, coord, db, sql)
	}

	// Absolute assertions, so the test still means something if BOTH arms
	// were to regress the same way.
	mfdWant(t, dmdRows(t, ctx, coord, "SELECT k FROM dmd_a", "k"),
		[]string{"1", "2", "4", "5", "6"}, "deleted row absent from the DAG scan")
	mfdWant(t, dmdRows(t, ctx, coord, "SELECT COUNT(*) AS n FROM dmd_a", "n"),
		[]string{"5"}, "COUNT(*) excludes the deleted row")
	mfdWant(t, dmdRows(t, ctx, coord, "SELECT SUM(v) AS s FROM dmd_a", "s"),
		[]string{"180"}, "SUM excludes the deleted row's value")
	mfdWant(t, dmdRows(t, ctx, coord, "SELECT COUNT(*) AS n FROM dmd_a a JOIN dmd_b b ON a.k = b.k", "n"),
		[]string{"4"}, "the join drops rows deleted on either side")

	// Deleting every row of one file, then of the whole table: the
	// all-rows-deleted batch must be dropped, not shipped with an empty
	// selection.
	mfdWrite(t, db, "DELETE FROM dmd_a WHERE k > 0")
	dmdCheck(t, ctx, coord, db, "SELECT k, g, v FROM dmd_a")
	dmdCheck(t, ctx, coord, db, "SELECT COUNT(*) AS n FROM dmd_a")
	mfdWant(t, dmdRows(t, ctx, coord, "SELECT COUNT(*) AS n FROM dmd_a", "n"),
		[]string{"0"}, "a fully-deleted table counts zero on the DAG")
}

// UPDATE is a DELETE plus an INSERT: the old rows get markers and the new
// ones land in a fresh file. The DAG has to see exactly one version of each
// updated row — the failure mode without markers is DOUBLE counting, not
// under-counting, which no amount of "the answer looks plausible" catches.
func TestDistributedScanHonorsUpdateDeleteMarkers(t *testing.T) {
	ctx := context.Background()
	coord, db := mfdSetup(t, ctx)

	mfdWrite(t, db, "CREATE TABLE dmd_u (k BIGINT, v BIGINT)")
	for i := 1; i <= 4; i++ {
		mfdWrite(t, db, fmt.Sprintf("INSERT INTO dmd_u VALUES (%d, %d)", i, i))
	}
	mfdWrite(t, db, "UPDATE dmd_u SET v = 100 WHERE k = 2")

	dmdCheck(t, ctx, coord, db, "SELECT k, v FROM dmd_u")
	dmdCheck(t, ctx, coord, db, "SELECT COUNT(*) AS n FROM dmd_u")
	dmdCheck(t, ctx, coord, db, "SELECT SUM(v) AS s FROM dmd_u")

	mfdWant(t, dmdRows(t, ctx, coord, "SELECT COUNT(*) AS n FROM dmd_u", "n"),
		[]string{"4"}, "an UPDATE must not duplicate the updated row on the DAG")
	mfdWant(t, dmdRows(t, ctx, coord, "SELECT SUM(v) AS s FROM dmd_u", "s"),
		[]string{"108"}, "the pre-update value must not survive on the DAG")
}

// Compaction FOLDS markers in: it rewrites the marked files without the
// deleted rows and drops the markers. The DAG must answer identically before
// and after — the markers being gone is not a licence for the rows to come
// back, and a stale plan-time marker set against a rewritten file must not
// delete the wrong rows either (it cannot: markers are keyed by the OLD
// path, which the manifest no longer lists).
func TestDistributedScanAgreesAcrossCompactionOfDeletedRows(t *testing.T) {
	ctx := context.Background()
	infra := tmdInfra(t, ctx)
	coord := tmdCoordinator(t, ctx, infra)
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger,
	})
	if err != nil {
		t.Fatalf("open DB over the coordinator's KV: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	mfdWrite(t, db, "CREATE TABLE dmd_c (k BIGINT, v BIGINT)")
	for i := 1; i <= 8; i++ {
		mfdWrite(t, db, fmt.Sprintf("INSERT INTO dmd_c VALUES (%d, %d)", i, i*5))
	}
	mfdWrite(t, db, "DELETE FROM dmd_c WHERE k IN (2, 7)")

	const shape = "SELECT k, v FROM dmd_c"
	const total = "SELECT COUNT(*) AS n, SUM(v) AS s FROM dmd_c"
	before := dmdRows(t, ctx, coord, shape, "k")
	dmdCheck(t, ctx, coord, db, shape)
	dmdCheck(t, ctx, coord, db, total)
	mfdWant(t, before, []string{"1", "3", "4", "5", "6", "8"}, "pre-compaction DAG answer")

	// RewriteTable folds every marker in and clears them, which is what the
	// background GC eventually does on its own.
	comp := compaction.New(infra.cat, slog.Default(), compaction.DefaultConfig())
	if _, err := comp.RewriteTable(ctx, "dmd_c"); err != nil {
		t.Fatalf("compacting dmd_c: %v", err)
	}
	manifest, err := infra.cat.GetManifest(ctx, "dmd_c")
	if err != nil {
		t.Fatalf("manifest after compaction: %v", err)
	}
	if len(manifest.DeleteMarkers) != 0 {
		t.Fatalf("compaction left %d markers behind; the fold-in did not happen",
			len(manifest.DeleteMarkers))
	}

	after := dmdRows(t, ctx, coord, shape, "k")
	mfdWant(t, after, before, "the DAG answer must be identical after compaction folds the markers in")
	dmdCheck(t, ctx, coord, db, shape)
	dmdCheck(t, ctx, coord, db, total)
}
