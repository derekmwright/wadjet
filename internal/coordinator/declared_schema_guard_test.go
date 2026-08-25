package coordinator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// The DAG must refuse a file whose stored type contradicts the catalog's
// declared type, the way the single-process reader already does.
//
// This is the #483 review's preserved probe, turned into a gate. The
// asymmetry it found (#503): parquet.Reader.SchemaAs(nil) short-circuits to
// the FILE's own schema, so where no declaration reached the worker the
// catalog never entered the comparison at all — `coerce` came out false and
// the STRING decoded as a STRING under a column the catalog calls BIGINT.
// Single-process refused by name ("schema declares INT64 but the file stores
// STRING"); the DAG returned 'hello' and reported success.
//
// The fixture is the shape a chunk-name collision (#494) produces on its own:
// a FRESH manifest for a re-created table, pointed at a file a previous
// incarnation wrote under the same path. Nothing here is corrupt — every
// file is a valid parquet, and the catalog is internally consistent. That is
// what makes the silent answer possible.
func TestDAGRefusesAFileThatContradictsTheDeclaredSchema(t *testing.T) {
	ctx := context.Background()
	infra := tmdInfra(t, ctx)
	coord := tmdCoordinator(t, ctx, infra)
	db, err := wadjet.Open(ctx, wadjet.Config{
		Store: infra.store, Bucket: "test", MetaKV: infra.kv, Logger: infra.logger,
	})
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	if _, err := db.Query(ctx, "CREATE TABLE drift (c0 BIGINT, c1 TEXT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Execute(ctx, "INSERT INTO drift VALUES (1, 'hello')"); err != nil {
		t.Fatal(err)
	}
	m, err := infra.cat.GetManifest(ctx, "drift")
	if err != nil {
		t.Fatal(err)
	}
	var oldFile catalog.FileEntry
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			oldFile = f
		}
	}
	if oldFile.Path == "" {
		t.Fatal("fixture wrote no file")
	}

	// Re-create the table with c1 as a BIGINT, then point the NEW manifest
	// at the OLD incarnation's file — where c1 holds the string 'hello'.
	if _, err := db.Query(ctx, "DROP TABLE drift"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Query(ctx, "CREATE TABLE drift (c0 BIGINT, c1 BIGINT)"); err != nil {
		t.Fatal(err)
	}
	if err := infra.cat.AddFiles(ctx, "drift", nil, "tables/drift/", []catalog.FileEntry{oldFile}); err != nil {
		t.Fatal(err)
	}

	const sql = "SELECT c0, c1 FROM drift"

	// Arm A: the single-process engine, which reads the catalog and refuses.
	// Asserted first, because it is the behaviour arm B has to match — if
	// this ever stops refusing, the comparison below means nothing.
	singleRes, singleErr := db.Query(ctx, sql)
	if singleErr == nil {
		t.Fatalf("single-process ANSWERED a type-drifted file instead of refusing: %v", singleRes.Rows)
	}

	// Arm B: the stage DAG. Same state, same query, and it must fail too.
	// What it must NOT do is come back with 'hello' for a BIGINT column.
	dagRes, dagErr := tmdRunDAG(ctx, coord, sql)
	if dagErr == nil {
		t.Fatalf("the stage DAG returned rows for a type-drifted file instead of refusing: %v\n"+
			"  single-process refused with: %v", dagRes.Rows, singleErr)
	}
	// Both arms must say the read was refused over a type disagreement, not
	// fail for some unrelated reason that happens to look like a pass.
	for _, c := range []struct{ arm, msg string }{
		{"single-process", singleErr.Error()},
		{"stage DAG", dagErr.Error()},
	} {
		low := strings.ToLower(c.msg)
		if !strings.Contains(low, "refus") {
			t.Errorf("%s failed, but not with a refusal: %s", c.arm, c.msg)
		}
	}
	t.Logf("single-process refused: %v", singleErr)
	t.Logf("stage DAG refused: %v", dagErr)
}

// A materialization failure must cost the query its consolidation, not its
// answer.
//
// dispatchReplicateStage consolidates a multi-file broadcast build into one
// WSHF cache, and falls back to handing the raw upstream files to every
// downstream reader when that task fails — "workers can still read the raw
// upstream files; we just lose the consolidation benefit."
//
// That fallback used to drop the upstream's scan annotations, and once #503's
// guard landed the sentence stopped being true. The branch it falls back FROM
// runs only over an upstream the bypass declined, which by construction is
// multi-file base parquet: exactly the input applyDeclaredScanSchema refuses
// to type from its own footers. With ScanTable and ScanSchema dropped,
// stageInputScanSchemas has nothing to key on, the broadcast build's
// BuildColumnTypes comes out empty, and the worker refuses — so an unhealthy
// S3 or a dead worker turned a slower query into a failed one.
//
// The failure is injected rather than provoked because a healthy cluster
// cannot reach this branch; what is asserted is everything downstream of it.
func TestReplicateFallbackKeepsTheQueryAnswerable(t *testing.T) {
	prevMin := replicateMaterializeMinFiles
	replicateMaterializeMinFiles = 2
	t.Cleanup(func() { replicateMaterializeMinFiles = prevMin })

	var materializeCalls int
	prevMat := replicateMaterialize
	replicateMaterialize = func(c *Coordinator, ctx context.Context, queryID string,
		stage physical.Stage, upstreamID string, files []string, upstream StageOutput,
	) ([]string, int64, error) {
		materializeCalls++
		return nil, 0, errors.New("injected: the consolidation task failed")
	}
	t.Cleanup(func() { replicateMaterialize = prevMat })

	_, coord, store := setupDistributed(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cat := coord.catalog

	// The build side is a multi-chunk BASE table, which is what puts raw
	// parquet — not WSHF — on the far side of the replicate. The type is
	// IPv4 on purpose: it is one of the nine a parquet footer cannot express,
	// so a build side that ends up typing itself from the file answers
	// 167772161 instead of 10.0.0.1 (#423) rather than merely refusing.
	dimSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "addr", Type: parquet.TypeIPv4},
	}
	ingestMultiFile(t, ctx, store, cat, "dim_repl", dimSchema, [][]map[string]any{
		{{"id": int64(1), "addr": "10.0.0.1"}},
		{{"id": int64(2), "addr": "10.0.0.2"}},
		{{"id": int64(3), "addr": "10.0.0.3"}},
	})

	factSchema := []parquet.Column{
		{Name: "fk", Type: parquet.TypeInt64},
		{Name: "amt", Type: parquet.TypeInt64},
	}
	ingestTestData(t, ctx, store, cat, "facts_repl", factSchema, []map[string]any{
		{"fk": int64(1), "amt": int64(10)},
		{"fk": int64(2), "amt": int64(20)},
		{"fk": int64(2), "amt": int64(25)},
		{"fk": int64(3), "amt": int64(30)},
	})

	const sql = `SELECT d.addr AS addr, COUNT(*) AS n
		FROM facts_repl f JOIN dim_repl d ON f.fk = d.id GROUP BY d.addr`

	res, err := coord.ExecuteSQL(ctx, sql)
	if err != nil {
		t.Fatalf("the query FAILED after a materialization fallback, which must only cost "+
			"the consolidation: %v", err)
	}
	if res.Error != "" {
		t.Fatalf("the query FAILED after a materialization fallback, which must only cost "+
			"the consolidation: %s", res.Error)
	}
	if materializeCalls == 0 {
		t.Fatal("materialization was never attempted — the fixture did not reach the branch " +
			"this test exists for, so it proves nothing")
	}

	// The answer, and the TYPES in it: a build side reading its own footers
	// would answer the IPv4 column as an integer, which is the silent half of
	// what the annotations carry.
	got := map[string]int64{}
	for _, r := range mustRows(t, res) {
		addr, ok := r["addr"].(string)
		if !ok {
			t.Fatalf("addr came back as %#v (%T), not an IPv4 string — the build side typed "+
				"itself from the file", r["addr"], r["addr"])
		}
		got[addr] = toInt64(r["n"])
	}
	want := map[string]int64{"10.0.0.1": 1, "10.0.0.2": 2, "10.0.0.3": 1}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for k, w := range want {
		if got[k] != w {
			t.Errorf("addr=%s: got %d, want %d (full result %v)", k, got[k], w, got)
		}
	}
}
