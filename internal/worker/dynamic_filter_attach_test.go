package worker

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
)

func testBloomOf(keys ...int64) ([]uint64, uint64) {
	bloom, mask := exec.NewBloomSized(len(keys))
	for _, k := range keys {
		h := exec.BloomHashInt(k)
		bloom[h&mask] |= 1 << (h & 63)
		bloom[(h>>17)&mask] |= 1 << ((h >> 6) & 63)
	}
	return bloom, mask
}

func drainKeys(t *testing.T, src exec.Source) []int64 {
	t.Helper()
	var got []int64
	for {
		b, err := src.Next(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if b == nil {
			return got
		}
		col := b.Columns[0]
		if b.Sel != nil {
			for _, idx := range b.Sel {
				got = append(got, col.Int64Data[idx])
			}
		} else {
			for i := 0; i < b.Len; i++ {
				got = append(got, col.Int64Data[i])
			}
		}
	}
}

// Attach-on-arrival core semantics: batches read before the bloom resolves
// pass through UNFILTERED (drop-only correctness — the downstream join
// re-verifies); every batch from the resolution onward is filtered.
func TestBloomFilteredSourceLateAttach(t *testing.T) {
	bloom, mask := testBloomOf(10, 20, 30)
	pb := &pendingBloom{done: make(chan struct{})}
	src := &bloomFilteredSource{
		inner: &sliceSource{batches: []*batch.RecordBatch{
			intBatch(t, "k", []int64{10, 999, 20}), // pre-attach: unfiltered
			intBatch(t, "k", []int64{998, 30, 40}), // post-attach: filtered
			intBatch(t, "k", []int64{500, 501}),    // post-attach: all rejected
		}},
		pending: []*deferredBloomFilter{{pb: pb, filterID: "f1", column: "k"}},
		logger:  slog.Default(),
	}
	b1, err := src.Next(context.Background())
	if err != nil || b1 == nil {
		t.Fatalf("batch 1: %v %v", b1, err)
	}
	if b1.Sel != nil {
		t.Fatal("pre-attach batch must pass through unfiltered")
	}
	// Resolve the bloom between batches.
	pb.bloom, pb.mask, pb.ok = bloom, mask, true
	close(pb.done)

	got := drainKeys(t, src)
	for _, k := range []int64{30} {
		found := false
		for _, g := range got {
			if g == k {
				found = true
			}
		}
		if !found {
			t.Fatalf("in-bloom key %d dropped post-attach", k)
		}
	}
	for _, g := range got {
		if g >= 500 {
			t.Fatalf("key %d survived the attached bloom", g)
		}
	}
	if len(src.pending) != 0 || len(src.ops) != 1 {
		t.Fatalf("promotion failed: pending=%d ops=%d", len(src.pending), len(src.ops))
	}
}

// A poll that finishes without a bloom (withheld filter / deadline) must
// drop the pending entry and leave the scan permanently unfiltered.
func TestBloomFilteredSourceLateAttachNeverArrives(t *testing.T) {
	pb := &pendingBloom{done: make(chan struct{})}
	close(pb.done) // finished, ok=false
	src := &bloomFilteredSource{
		inner: &sliceSource{batches: []*batch.RecordBatch{
			intBatch(t, "k", []int64{1, 2, 3}),
			intBatch(t, "k", []int64{4, 5}),
		}},
		pending: []*deferredBloomFilter{{pb: pb, filterID: "f1", column: "k"}},
		logger:  slog.Default(),
	}
	got := drainKeys(t, src)
	if len(got) != 5 {
		t.Fatalf("unfiltered scan must pass every row, got %d", len(got))
	}
	if len(src.pending) != 0 || len(src.ops) != 0 {
		t.Fatalf("finished-empty poll must drop pending without installing: pending=%d ops=%d",
			len(src.pending), len(src.ops))
	}
}

func stageArtifact(t *testing.T, store objstore.Store, bucket, key string, keys ...int64) {
	t.Helper()
	bloom, mask := testBloomOf(keys...)
	art := &distributed.DynamicFilterArtifact{KeyType: "int64", Bloom: bloom, BloomMask: mask, RowCount: int64(len(keys))}
	var buf bytes.Buffer
	if err := distributed.EncodeDynamicFilterArtifact(&buf, art); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bucket, key,
		bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
}

// pollDeferredBloom: singleflight per key; resolves when the artifact
// lands mid-poll; a request after resolution starts a fresh poll that
// succeeds immediately.
func TestPollDeferredBloomResolvesOnArrival(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey: "queries/q/dynfilter-merged/s/f1.wdf", Deferred: true,
	}
	p1 := e.pollDeferredBloom(spec)
	p2 := e.pollDeferredBloom(spec)
	if p1 != p2 {
		t.Fatal("same key must singleflight to one pending slot")
	}
	select {
	case <-p1.done:
		t.Fatal("resolved before the artifact exists")
	case <-time.After(50 * time.Millisecond):
	}
	stageArtifact(t, store, "b", spec.BloomKey, 10, 20)
	select {
	case <-p1.done:
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not resolve after artifact landed")
	}
	if bloom, _, ok := p1.resolved(); !ok || len(bloom) == 0 {
		t.Fatal("resolved slot must carry the bloom")
	}
	// Map entry cleaned up: a fresh request re-fetches and resolves fast.
	p3 := e.pollDeferredBloom(spec)
	select {
	case <-p3.done:
	case <-time.After(5 * time.Second):
		t.Fatal("post-resolution request must resolve immediately")
	}
	if bloom, _, ok := p3.resolved(); !ok || len(bloom) == 0 {
		t.Fatal("fresh poll after staging must find the artifact")
	}
}
