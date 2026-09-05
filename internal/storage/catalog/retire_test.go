package catalog

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// manifestFilePaths lists every file path a table's manifest names.
func manifestFilePaths(t *testing.T, cat *Catalog, ctx context.Context, table string) []string {
	t.Helper()
	m, err := cat.GetManifest(ctx, table)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, p := range m.Partitions {
		for _, f := range p.Files {
			out = append(out, f.Path)
		}
	}
	return out
}

// TestRetireObjectsPreservesWhatALiveManifestNames is #896's rule at the
// catalog door.
func TestRetireObjectsPreservesWhatALiveManifestNames(t *testing.T) {
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "events", testSchema(), nil); err != nil {
		t.Fatal(err)
	}
	if err := cat.CreateTable(ctx, "archive", testSchema(), nil); err != nil {
		t.Fatal(err)
	}
	shared := "tables/events/chunk_shared.parquet"
	lonely := "tables/events/chunk_lonely.parquet"
	for _, p := range []string{shared, lonely} {
		if _, err := cat.Store().Put(ctx, cat.Bucket(), p, bytes.NewReader([]byte("data")), 4, "application/octet-stream"); err != nil {
			t.Fatal(err)
		}
	}
	// events no longer names either; archive names the shared one.
	if err := cat.AddFiles(ctx, "archive", nil, "archive", []FileEntry{
		{Path: shared, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatal(err)
	}

	out := cat.RetireObjects(ctx, []RetireRequest{{Path: shared}, {Path: lonely}})
	if out[shared] != RetireReferenced {
		t.Errorf("a path a live manifest names must be %v, got %v", RetireReferenced, out[shared])
	}
	if out[lonely] != Retired {
		t.Errorf("an unreferenced path must be %v, got %v", Retired, out[lonely])
	}
	if _, err := cat.Store().Head(ctx, cat.Bucket(), shared); err != nil {
		t.Errorf("archive's referenced object was deleted: %v", err)
	}
	if _, err := cat.Store().Head(ctx, cat.Bucket(), lonely); err == nil {
		t.Error("the unreferenced object should have been retired")
	}
}

// TestRegistrationIsRefusedWhileAPathIsRetiring is the interlock's other half:
// a reference CHECK is a read, and cannot exclude a registration that lands
// after it. The mark can.
func TestRegistrationIsRefusedWhileAPathIsRetiring(t *testing.T) {
	cat, ctx := setupCatalog(t)
	if err := cat.CreateTable(ctx, "archive", testSchema(), nil); err != nil {
		t.Fatal(err)
	}
	path := "tables/events/chunk_x.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("data")), 4, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}

	marked, busy := cat.markRetiring([]string{path})
	if len(marked) != 1 || len(busy) != 0 {
		t.Fatalf("expected the path to be markable, got marked=%v busy=%v", marked, busy)
	}
	err := cat.AddFiles(ctx, "archive", nil, "archive", []FileEntry{
		{Path: path, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	})
	if !errors.Is(err, ErrPathRetiring) {
		t.Fatalf("expected ErrPathRetiring, got %v", err)
	}
	if got := manifestFilePaths(t, cat, ctx, "archive"); len(got) != 0 {
		t.Errorf("a refused registration must write nothing, got %v", got)
	}

	// Released, the same registration goes through.
	cat.unmarkRetiring(marked)
	if err := cat.AddFiles(ctx, "archive", nil, "archive", []FileEntry{
		{Path: path, SizeBytes: 4, NumRows: 1, CreatedAt: time.Now().UTC()},
	}); err != nil {
		t.Fatalf("after the mark is released the registration must succeed: %v", err)
	}
}

// TestAnInFlightRegistrationIsNotARetirementCandidate is the mirror: a
// retirement sweep that cannot see whether a registration's manifest write has
// landed declines to decide, rather than deleting on a maybe.
func TestAnInFlightRegistrationIsNotARetirementCandidate(t *testing.T) {
	cat, ctx := setupCatalog(t)
	path := "tables/events/chunk_y.parquet"
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("data")), 4, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	if err := cat.beginRegistration([]string{path}); err != nil {
		t.Fatal(err)
	}
	out := cat.RetireObjects(ctx, []RetireRequest{{Path: path}})
	if out[path] != RetireUnproven {
		t.Fatalf("a path with a registration in flight must be %v, got %v", RetireUnproven, out[path])
	}
	if _, err := cat.Store().Head(ctx, cat.Bucket(), path); err != nil {
		t.Errorf("doubt must preserve the bytes: %v", err)
	}
	cat.endRegistration([]string{path})
}

// TestRetireObjectsSkipsARecreatedObject keeps the guard the queue already
// had: bytes rewritten at the path since the retirement was scheduled are not
// the bytes that were scheduled.
func TestRetireObjectsSkipsARecreatedObject(t *testing.T) {
	cat, ctx := setupCatalog(t)
	path := "tables/events/chunk_z.parquet"
	scheduled := time.Now().Add(-time.Hour)
	if _, err := cat.Store().Put(ctx, cat.Bucket(), path, bytes.NewReader([]byte("new")), 3, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	out := cat.RetireObjects(ctx, []RetireRequest{{Path: path, NotModifiedAfter: scheduled}})
	if out[path] != RetireReferenced {
		t.Fatalf("a recreated object must not be retired, got %v", out[path])
	}
	if _, err := cat.Store().Head(ctx, cat.Bucket(), path); err != nil {
		t.Errorf("the recreated object was deleted: %v", err)
	}
}
