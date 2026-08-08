package distributed

import (
	"sort"
	"testing"
)

func TestAffinityOwnerDeterministicAndOrderInsensitive(t *testing.T) {
	workers := []string{"worker-c", "worker-a", "worker-b"}
	files := []string{
		"tables/lineitem/chunk_1.parquet",
		"tables/lineitem/chunk_2.parquet",
		"tables/orders/chunk_1.parquet",
		"tables/nation/chunk_1.parquet",
	}
	for _, f := range files {
		owner := AffinityOwner(f, workers)
		if owner == "" {
			t.Fatalf("AffinityOwner(%q) returned no owner", f)
		}
		shuffled := []string{"worker-b", "worker-c", "worker-a"}
		if got := AffinityOwner(f, shuffled); got != owner {
			t.Fatalf("owner of %q depends on domain order: %q vs %q", f, owner, got)
		}
	}
	if AffinityOwner("tables/x.parquet", nil) != "" {
		t.Fatal("empty domain must yield no owner")
	}
}

func TestAffinityOwnerMinimalRemap(t *testing.T) {
	// Removing one worker must only remap the files it owned.
	workers := []string{"w1", "w2", "w3"}
	files := make([]string, 0, 100)
	for i := 0; i < 100; i++ {
		files = append(files, "tables/t/chunk_"+string(rune('a'+i%26))+string(rune('a'+i/26))+".parquet")
	}
	sort.Strings(files)
	before := make(map[string]string, len(files))
	for _, f := range files {
		before[f] = AffinityOwner(f, workers)
	}
	after := []string{"w1", "w3"}
	for _, f := range files {
		got := AffinityOwner(f, after)
		if before[f] != "w2" && got != before[f] {
			t.Fatalf("file %q moved from %q to %q though its owner never left", f, before[f], got)
		}
	}
}

func TestBaseTablePeerKeyRoundTrip(t *testing.T) {
	pk := BaseTablePeerKey("wadjet-bench", "tables/lineitem/chunk_1.parquet")
	bucket, key, ok := CutBaseTablePeerKey(pk)
	if !ok || bucket != "wadjet-bench" || key != "tables/lineitem/chunk_1.parquet" {
		t.Fatalf("round trip = (%q, %q, %v)", bucket, key, ok)
	}
	for _, bad := range []string{
		"queries/abc/stage-1/part-0.wshf", // scratch key
		"basetable:",                      // no bucket/key
		"basetable:bucketonly",            // no separator
		"basetable:/key",                  // empty bucket
		"basetable:bucket/",               // empty key
		"tables/lineitem/chunk_1.parquet", // bare object key
	} {
		if _, _, ok := CutBaseTablePeerKey(bad); ok {
			t.Fatalf("CutBaseTablePeerKey(%q) must not parse", bad)
		}
	}
}
