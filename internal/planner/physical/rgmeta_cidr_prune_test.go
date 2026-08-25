package physical

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// cidrRGMetaSchema is a two-column table whose CIDR column is what the
// predicate below prunes on.
var cidrRGMetaSchema = parquet.Schema{Columns: []parquet.Column{
	{Name: "id", Type: parquet.TypeInt64},
	{Name: "c_cidr", Type: parquet.TypeCIDR},
}}

// cidrRGMetaFixture builds a MemStore-backed catalog holding two files of
// three row groups each, with the two files on opposite sides of
// 192.168.0.0/16 in PostgreSQL's inet order: file 0 is all 10.x, file 1 all
// 192.168.x. Every row group's bounds are therefore disjoint and the prune
// decision for the predicate below is deterministic — three of the six units
// survive it.
func cidrRGMetaFixture(t *testing.T) (*catalog.Catalog, []catalog.FileEntry) {
	t.Helper()
	ctx := context.Background()

	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatalf("catalog init: %v", err)
	}
	if err := cat.CreateTable(ctx, "nets", cidrRGMetaSchema, nil); err != nil {
		t.Fatalf("create table: %v", err)
	}

	rows := func(prefix string, group, base int) []map[string]any {
		out := make([]map[string]any, 100)
		for i := range out {
			out[i] = map[string]any{
				"id":     int64(base + i),
				"c_cidr": fmt.Sprintf("%s.%d.%d/32", prefix, group, i),
			}
		}
		return out
	}

	var entries []catalog.FileEntry
	for fi, prefix := range []string{"10.0", "192.168"} {
		base := fi * 1000
		data := writeTestParquetMultiRG(t, cidrRGMetaSchema,
			rows(prefix, 0, base), rows(prefix, 1, base+100), rows(prefix, 2, base+200))
		path := fmt.Sprintf("tables/nets/chunk_%04d.parquet", fi)
		if _, err := store.Put(ctx, cat.Bucket(), path, bytes.NewReader(data),
			int64(len(data)), "application/octet-stream"); err != nil {
			t.Fatalf("put file: %v", err)
		}
		entry := catalog.FileEntry{Path: path, SizeBytes: int64(len(data)), NumRows: 300, CreatedAt: time.Now()}
		if err := cat.AddFiles(ctx, "nets", map[string]string{}, "tables/nets/",
			[]catalog.FileEntry{entry}); err != nil {
			t.Fatalf("add files: %v", err)
		}
		entries = append(entries, entry)
	}
	return cat, entries
}

// TestBuildRGUnitsPrunesCidrFromTheRGMetaBlob is the coordinator half of
// #523's follow-up: the persisted RG-metadata blob must carry a CIDR bound
// in a form the prune layer can still USE, not merely in a form that decodes
// without error.
//
// The blob's writeValue switches on the value's Go type, and a confirmed
// CIDR bound is a parquet.CidrInetBound, not a string — so before the
// rgMetaTagCidrInet arm existed it hit the "not a type statsToNative
// produces" default and was stored as NIL. Every CIDR bound in the blob came
// back nil, CanPruneRowGroup declined on a nil bound, and after ANALYZE the
// coordinator's rgmeta path — the one that exists so planning does not read
// 600 footers — silently stopped pruning CIDR at all. Storing it as a plain
// STRING instead is no better: scan.compareValuesOK refuses to compare an
// unboxed bound against the boxed literal kernel.StatsDomainValue produces,
// so the prune still declines. Neither failure is a wrong answer, which is
// exactly why nothing else catches it.
//
// The assertion is therefore in two halves — the blob path must prune the
// SAME units the footer path prunes, AND that must be fewer than all of them
// (a prune that disengaged agrees with a prune that disengaged).
func TestBuildRGUnitsPrunesCidrFromTheRGMetaBlob(t *testing.T) {
	ctx := context.Background()
	cat, entries := cidrRGMetaFixture(t)

	// The literal reaches the prune layer already converted, exactly as
	// plan.go's structuredConjuncts hands it over.
	lit, ok := kernel.StatsDomainValue(parquet.TypeCIDR, 0, "192.168.0.0/16")
	if !ok {
		t.Fatal("kernel.StatsDomainValue withheld a valid CIDR literal — pruning is off for the type")
	}
	preds := []scanPredicate{{Column: "c_cidr", Op: ">=", Value: lit}}

	build := func(preds []scanPredicate) *scanSourceInner {
		inner := &scanSourceInner{
			cat:       cat,
			tableName: "nets",
			files:     entries,
			schema:    cidrRGMetaSchema.Columns,
			scanPreds: preds,
		}
		inner.buildRGUnits(ctx)
		if inner.failedFiles > 0 {
			t.Fatalf("buildRGUnits failed files: %d (%v)", inner.failedFiles, inner.firstFileErr)
		}
		return inner
	}

	if total := len(collectUnits(build(nil))); total != 6 {
		t.Fatalf("unpredicated units = %d, want 6 (2 files × 3 row groups)", total)
	}

	// Footer path: no blob exists yet.
	footer := collectUnits(build(preds))
	if len(footer) != 3 {
		t.Fatalf("footer path: %d units survive `c_cidr >= '192.168.0.0/16'`, want 3 — "+
			"the CIDR prune is not engaged even before the blob is involved", len(footer))
	}

	// ANALYZE persists the RG-metadata blob, and deleting the parquet
	// objects proves the units below came from it and not from a footer.
	if _, err := cat.AnalyzeTable(ctx, "nets"); err != nil {
		t.Fatalf("AnalyzeTable: %v", err)
	}
	if m, err := cat.TableRGMeta(ctx, "nets"); err != nil || len(m) != 2 {
		t.Fatalf("TableRGMeta: %d files (err %v), want 2", len(m), err)
	}
	for _, e := range entries {
		if err := cat.Store().Delete(ctx, cat.Bucket(), e.Path); err != nil {
			t.Fatal(err)
		}
	}

	blob := collectUnits(build(preds))
	if len(blob) != len(footer) {
		t.Fatalf("blob path: %d units survive the CIDR predicate, footer path kept %d — "+
			"the persisted bound lost whatever the prune needed", len(blob), len(footer))
	}
	for i, u := range blob {
		if u != footer[i] {
			t.Errorf("blob path unit %d: %+v != footer %+v", i, u, footer[i])
		}
	}
}
