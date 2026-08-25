package physical

import (
	"testing"
)

// TestFKReferencedRowCountManifestReadsArePinned pins
// dynamic_filter.go's estimateFKReferencedRowCount, which reads a manifest
// for a table name it GUESSED from a column name ("s_nationkey" ->
// "nation", then "nations"). The #502 manifest-snapshot fold-in pinned the
// read count this function pays, but nothing exercised THIS call site
// directly until now.
//
// Both a HIT (the guessed table exists) and a MISS (it does not) are
// pinned: the miss is the interesting one, because the snapshot caches
// failures too, and a naive read-through cache would keep re-trying a
// speculative guess that already missed once this statement.
func TestFKReferencedRowCountManifestReadsArePinned(t *testing.T) {
	cat, kv, ctx := setupSelfJoinCatalog(t) // table "t"
	snap := NewManifestSnapshot()
	ctx = WithManifestSnapshot(ctx, snap)
	p := NewPlannerForContext(ctx, cat)

	// "x_tkey" -> stem "t" -> candidates "t" (HIT) and "ts" (never tried,
	// since "t" already answers with rows).
	if got := p.estimateFKReferencedRowCount(ctx, "x_tkey"); got != 200 {
		t.Fatalf("estimateFKReferencedRowCount(x_tkey) = %d, want 200 (table t has 2 files x 100 rows) "+
			"— the trigger did not fire, so this test proves nothing", got)
	}
	afterFirst := kv.manifestGets()
	for i := 0; i < 5; i++ {
		p.estimateFKReferencedRowCount(ctx, "x_tkey")
	}
	if got := kv.manifestGets(); got != afterFirst {
		t.Errorf("HIT path: %d manifest reads after 5 more calls, want it to stay %d — unpinned",
			got, afterFirst)
	}
	t.Logf("HIT path pinned: %d read(s) for 6 calls", afterFirst)

	// The MISS path: a stem naming no table at all. The snapshot caches the
	// FAILURE, so the speculative read happens once per candidate per
	// statement, not once per CALL.
	before := kv.manifestGets()
	for i := 0; i < 5; i++ {
		if got := p.estimateFKReferencedRowCount(ctx, "x_zzznotatablekey"); got != 0 {
			t.Fatalf("a stem naming no table returned %d, want 0", got)
		}
	}
	missReads := kv.manifestGets() - before
	if missReads > 2 {
		t.Errorf("MISS path: %d manifest reads for 5 calls over 2 candidates, want at most 2 "+
			"(one per candidate, the failure cached) — the speculative reads are unpinned", missReads)
	}
	t.Logf("MISS path pinned: %d read(s) for 5 calls x 2 candidates", missReads)

	// Unpinned contrast, so a green assertion above cannot be something
	// else having started caching.
	cat2, kv2, ctx2 := setupSelfJoinCatalog(t)
	p2 := NewPlanner(cat2)
	p2.ManifestSnapshot = nil // a bare planner: the documented fallback
	for i := 0; i < 6; i++ {
		p2.estimateFKReferencedRowCount(ctx2, "x_tkey")
	}
	if got := kv2.manifestGets(); got <= 1 {
		t.Errorf("unpinned reads = %d, want >1 — the pin above is not doing work", got)
	} else {
		t.Logf("unpinned contrast: %d read(s) for 6 calls", got)
	}
}
