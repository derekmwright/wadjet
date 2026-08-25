package coordinator

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
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
