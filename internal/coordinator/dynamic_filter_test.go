package coordinator

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func putPartial(t *testing.T, store objstore.Store, bucket, key string, keys []int64) distributed.DynamicFilterPartialRef {
	t.Helper()
	bloom := make([]uint64, 16)
	mask := uint64(len(bloom) - 1)
	art := &distributed.DynamicFilterArtifact{
		KeyType: "int64", RowCount: int64(len(keys)),
		Bloom: bloom, BloomMask: mask,
	}
	for _, k := range keys {
		h := hashForTest(k)
		bloom[h&mask] |= 1 << (h & 63)
		bloom[(h>>17)&mask] |= 1 << ((h >> 6) & 63)
	}
	var buf bytes.Buffer
	if err := distributed.EncodeDynamicFilterArtifact(&buf, art); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bucket, key,
		bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	return distributed.DynamicFilterPartialRef{FilterID: "f1", Bucket: bucket, Key: key}
}

func hashForTest(k int64) uint64 {
	x := uint64(k)
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	return x
}

// mergeCompleteBuildStats must WITHHOLD a filter whose partial coverage is
// short of the task count: a bloom missing any task's keys falsely rejects
// rows at the consume side. Full coverage attaches; partial coverage drops.
func TestMergeCompleteBuildStats_CompletenessGate(t *testing.T) {
	store := objstore.NewMemStore()
	cat := catalog.NewWithStore(store, "test")
	if err := cat.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	c := &Coordinator{catalog: cat, logger: slog.Default(), config: Config{ResultBucket: "test"}}

	var refs []distributed.DynamicFilterPartialRef
	for i := 0; i < 3; i++ {
		refs = append(refs, putPartial(t, store, "test",
			fmt.Sprintf("queries/q/dynfilter/s/task%d-f1.wdf", i), []int64{int64(i * 10)}))
	}

	// Full coverage: 3 partials, 3 tasks → attach.
	merged := c.mergeCompleteBuildStats(context.Background(), "q", "s", refs, 3)
	if merged["f1"] == nil {
		t.Fatal("full coverage must attach the filter")
	}

	// Short coverage: 2 of 3 tasks → withhold.
	merged = c.mergeCompleteBuildStats(context.Background(), "q", "s", refs[:2], 3)
	if merged["f1"] != nil {
		t.Fatal("incomplete coverage must withhold the filter (missing keys = false rejections)")
	}

	// Duplicate refs from a task retry (same key) must not fake coverage.
	dup := []distributed.DynamicFilterPartialRef{refs[0], refs[0], refs[1]}
	merged = c.mergeCompleteBuildStats(context.Background(), "q", "s", dup, 3)
	if merged["f1"] != nil {
		t.Fatal("duplicate refs must dedupe by key; 2 distinct partials != 3 tasks")
	}
}
