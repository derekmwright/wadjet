package worker

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
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

// parsePartialCount: key-shape parsing for count-in-key partials.
func TestParsePartialCount(t *testing.T) {
	cases := []struct {
		key, filterID string
		wantN         int
		wantOK        bool
	}{
		{"queries/st-s-q/dynfilter/s/abc123-f1.of3.wdf", "f1", 3, true},
		{"abc123-f1.of12.wdf", "f1", 12, true},
		{"queries/st-s-q/dynfilter/s/abc123-f1.wdf", "f1", 0, false},     // legacy, no stamp
		{"queries/st-s-q/dynfilter/s/abc123-f2.of3.wdf", "f1", 0, false}, // other filter
		{"queries/st-s-q/dynfilter/s/abc123-f1.of0.wdf", "f1", 0, false}, // bad count
		{"queries/st-s-q/dynfilter/s/abc123-f1.ofx.wdf", "f1", 0, false},
		{"queries/st-s-q/dynfilter/s/abc123-f1.of3.txt", "f1", 0, false},
		// FilterID containing a dash still suffix-matches.
		{"queries/st-s-q/dynfilter/s/abc123-df-scan-7-c0.of24.wdf", "df-scan-7-c0", 24, true},
	}
	for _, c := range cases {
		n, ok := parsePartialCount(c.key, c.filterID)
		if n != c.wantN || ok != c.wantOK {
			t.Errorf("parsePartialCount(%q, %q) = (%d, %v), want (%d, %v)",
				c.key, c.filterID, n, ok, c.wantN, c.wantOK)
		}
	}
}

// Incremental partial publication: the poll resolves via the consumer-side
// union the moment the LAST ".of<N>" partial lands — no merged key needed —
// and never before full coverage (an incomplete union falsely rejects rows).
func TestPollDeferredBloomIncrementalPartials(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey:      "queries/q/dynfilter-merged/scan-1/f1.wdf",
		PartialPrefix: "queries/st-scan-1-q/dynfilter/scan-1/",
		Deferred:      true,
	}
	// Two of three partials present: must NOT resolve.
	stageArtifact(t, store, "b", spec.PartialPrefix+"t1-f1.of3.wdf", 10)
	stageArtifact(t, store, "b", spec.PartialPrefix+"t2-f1.of3.wdf", 20)
	// A different filter's partial in the same prefix must be ignored.
	stageArtifact(t, store, "b", spec.PartialPrefix+"t1-f2.of1.wdf", 999)
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
		t.Fatal("resolved with incomplete partial coverage")
	case <-time.After(600 * time.Millisecond):
	}
	stageArtifact(t, store, "b", spec.PartialPrefix+"t3-f1.of3.wdf", 30)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("did not resolve after the last partial landed")
	}
	bloom, mask, ok := p.resolved()
	if !ok || p.source != "partials" || p.partials != 3 {
		t.Fatalf("want resolution via 3 partials, got ok=%v source=%q partials=%d", ok, p.source, p.partials)
	}
	// The union must equal the bitwise OR of the three partials.
	b1, m1 := testBloomOf(10)
	b2, _ := testBloomOf(20)
	b3, _ := testBloomOf(30)
	if mask != m1 || len(bloom) != len(b1) {
		t.Fatalf("union shape mismatch: mask=%d want %d, words=%d want %d", mask, m1, len(bloom), len(b1))
	}
	for i := range bloom {
		if bloom[i] != b1[i]|b2[i]|b3[i] {
			t.Fatalf("union word %d = %x, want OR of partials %x", i, bloom[i], b1[i]|b2[i]|b3[i])
		}
	}
}

// The coordinator-staged merged key is authoritative and supersedes the
// partial path: when it exists, the poll resolves from it regardless of
// partial coverage.
func TestPollDeferredBloomMergedSupersedesPartials(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey:      "queries/q/dynfilter-merged/scan-1/f1.wdf",
		PartialPrefix: "queries/st-scan-1-q/dynfilter/scan-1/",
		Deferred:      true,
	}
	stageArtifact(t, store, "b", spec.PartialPrefix+"t1-f1.of3.wdf", 10)
	stageArtifact(t, store, "b", spec.BloomKey, 10, 20, 30)
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("merged key present but poll did not resolve")
	}
	if _, _, ok := p.resolved(); !ok || p.source != "merged" {
		t.Fatalf("want resolution via merged key, got ok=%v source=%q", ok, p.source)
	}
}

// A partial whose bloom size disagrees with the accumulated union (planner
// bug guard) must never count toward completeness — the filter degrades to
// never-activating, mirroring the coordinator's withhold.
func TestPollDeferredBloomPartialSizeMismatchNeverActivates(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey:      "queries/q/dynfilter-merged/scan-1/f1.wdf",
		PartialPrefix: "queries/st-scan-1-q/dynfilter/scan-1/",
		Deferred:      true,
	}
	stageArtifact(t, store, "b", spec.PartialPrefix+"t1-f1.of2.wdf", 10)
	// Larger key set -> NewBloomSized picks a bigger bloom -> size mismatch.
	stageArtifact(t, store, "b", spec.PartialPrefix+"t2-f1.of2.wdf",
		1, 2, 3, 4, 5, 6, 7, 8, 9, 11, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22,
		23, 24, 25, 26, 27, 28, 29, 31, 32, 33, 34, 35, 36, 37, 38, 39, 41, 42,
		43, 44, 45, 46, 47, 48, 49, 51, 52, 53, 54, 55, 56, 57, 58, 59, 61, 62,
		63, 64, 65, 66, 67, 68, 69, 71, 72, 73, 74, 75, 76, 77, 78, 79, 81, 82)
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
		t.Fatal("size-mismatched partial must not complete the union")
	case <-time.After(900 * time.Millisecond):
	}
}

// Emit-side count-in-key: a stamped StagePartials + PartialPrefix names the
// partial "<PartialPrefix><taskID>-<filterID>.of<N>.wdf" — the prefix is
// used VERBATIM (the coordinator stamps the same value into consumer specs,
// so emit and consume agree by construction) — and the shape round-trips
// through parsePartialCount.
func TestFinalizeDynamicFilterEmitsCountInKey(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	op := exec.NewDynamicFilterEmitOp("f1", "k", "int64", 256)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := op.Execute(context.Background(), intBatch(t, "k", []int64{10, 20})); err != nil {
		t.Fatal(err)
	}
	task := distributed.Task{
		ID: "abc123", QueryID: "st-scan-1-qid", StageID: "scan-1", ResultBucket: "b",
	}
	specs := []distributed.DynamicFilterEmit{
		{FilterID: "f1", KeyColumn: "k", KeyType: "int64", BloomBits: 256,
			StagePartials: 3, PartialPrefix: "queries/st-scan-1-qid/dynfilter/scan-1/"},
	}
	var result distributed.ResultNotification
	if err := e.finalizeDynamicFilterEmits(context.Background(), task,
		[]*exec.DynamicFilterEmitOp{op}, specs, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.DynamicFilterPartials) != 1 {
		t.Fatalf("want 1 partial ref, got %d", len(result.DynamicFilterPartials))
	}
	key := result.DynamicFilterPartials[0].Key
	want := "queries/st-scan-1-qid/dynfilter/scan-1/abc123-f1.of3.wdf"
	if key != want {
		t.Fatalf("partial key layout drifted:\n got %q\nwant %q", key, want)
	}
	if n, ok := parsePartialCount(key, "f1"); !ok || n != 3 {
		t.Fatalf("emit key must round-trip through parsePartialCount: n=%d ok=%v", n, ok)
	}
	if _, _, err := store.Get(context.Background(), "b", key); err != nil {
		t.Fatalf("partial not uploaded at its ref key: %v", err)
	}
}

// Un-stamped specs (StagePartials zero — legacy coordinator) keep the
// original key shape so the coordinator merge path is byte-compatible.
func TestFinalizeDynamicFilterEmitsLegacyKeyWithoutStamp(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	op := exec.NewDynamicFilterEmitOp("f1", "k", "int64", 256)
	if err := op.Init(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := op.Execute(context.Background(), intBatch(t, "k", []int64{10})); err != nil {
		t.Fatal(err)
	}
	task := distributed.Task{ID: "abc123", QueryID: "q", StageID: "s", ResultBucket: "b"}
	specs := []distributed.DynamicFilterEmit{{FilterID: "f1", KeyColumn: "k", KeyType: "int64", BloomBits: 256}}
	var result distributed.ResultNotification
	if err := e.finalizeDynamicFilterEmits(context.Background(), task,
		[]*exec.DynamicFilterEmitOp{op}, specs, &result); err != nil {
		t.Fatal(err)
	}
	if len(result.DynamicFilterPartials) != 1 ||
		result.DynamicFilterPartials[0].Key != "queries/q/dynfilter/s/abc123-f1.wdf" {
		t.Fatalf("legacy key shape changed: %+v", result.DynamicFilterPartials)
	}
}

// A zero-byte tombstone at the merged key (coordinator withheld the
// filter) must terminate the poll promptly with ok=false — consumers stay
// unfiltered and guarded re-emits flush unguarded, instead of waiting out
// the 10-minute poll deadline (the ead0976 SF100 hang class).
func TestPollDeferredBloomTombstoneTerminates(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey: "queries/q/dynfilter-merged/s/f1.wdf", Deferred: true,
	}
	if _, err := store.Put(context.Background(), "b", spec.BloomKey,
		bytes.NewReader(nil), 0, "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("tombstone must terminate the poll promptly")
	}
	if _, _, ok := p.resolved(); ok {
		t.Fatal("tombstone termination must not deliver a bloom")
	}
}

// Blob router: priority and priority_deep flags classify tasks onto the
// leaf vs deep lanes (disjoint slot pools — the deadlock prevention).
func TestTaskBlobPriorityClass(t *testing.T) {
	cases := []struct {
		blob      string
		pri, deep bool
	}{
		{`{"priority":true}`, true, false},
		{`{"priority":true,"priority_deep":true}`, true, true},
		{`{"priority_deep":true}`, false, true}, // never emitted, but parse faithfully
		{`{}`, false, false},
		{`not-json`, false, false},
	}
	for _, c := range cases {
		pri, deep := taskBlobPriority([]byte(c.blob))
		if pri != c.pri || deep != c.deep {
			t.Errorf("taskBlobPriority(%s) = (%v,%v), want (%v,%v)", c.blob, pri, deep, c.pri, c.deep)
		}
	}
}

// Consumer-side mirror of the empty-partial rule: an empty ".of<N>" partial
// counts toward completeness without adopting shape, so a zero-row emitter
// task can neither poison nor stall the incremental union.
func TestPollDeferredBloomIncrementalEmptyPartialCounts(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey:      "queries/q/dynfilter-merged/scan-1/f1.wdf",
		PartialPrefix: "queries/st-scan-1-q/dynfilter/scan-1/",
		Deferred:      true,
	}
	// Empty partial FIRST (zero-row task finishes first), real one second.
	emptyArt := &distributed.DynamicFilterArtifact{KeyType: "int64"}
	var buf bytes.Buffer
	if err := distributed.EncodeDynamicFilterArtifact(&buf, emptyArt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), "b", spec.PartialPrefix+"t1-f1.of2.wdf",
		bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
	stageArtifact(t, store, "b", spec.PartialPrefix+"t2-f1.of2.wdf", 10)
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("union with an empty partial must complete")
	}
	bloom, mask, ok := p.resolved()
	if !ok || p.partials != 2 {
		t.Fatalf("want resolution counting both partials, ok=%v partials=%d", ok, p.partials)
	}
	want, wantMask := testBloomOf(10)
	if mask != wantMask || len(bloom) != len(want) {
		t.Fatal("union must adopt the real partial's shape")
	}
}

// fakeDFAppender records iterator-level filter deliveries.
type fakeDFAppender struct {
	ranges []exec.DynamicRange
	blooms []*exec.BloomScanFilter
	calls  int
}

func (f *fakeDFAppender) AddDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	f.ranges = append(f.ranges, ranges...)
	f.blooms = append(f.blooms, blooms...)
	f.calls++
}

// A resolved deferred filter must reach the ITERATOR layer too (row-group
// pruning + prune-aware advises), not just the row-level op — with the
// artifact's build-key range when it carried one. This is the delivery the
// EC2 SF100 arms showed missing (docs/design/rowgroup-touch-ahead.md,
// third arm: advise totals byte-identical because iterator filters never
// attached on the deferred path).
func TestBloomFilteredSourceLateAttach_GroupLevel(t *testing.T) {
	bloom, mask := testBloomOf(10, 20, 30)
	pb := &pendingBloom{done: make(chan struct{})}
	sink := &fakeDFAppender{}
	src := &bloomFilteredSource{
		inner: &sliceSource{batches: []*batch.RecordBatch{
			intBatch(t, "k", []int64{10, 999}),
			intBatch(t, "k", []int64{998, 30}),
		}},
		pending:   []*deferredBloomFilter{{pb: pb, filterID: "f1", column: "k", keyType: "int64"}},
		groupSink: sink,
		logger:    slog.Default(),
	}
	if _, err := src.Next(context.Background()); err != nil {
		t.Fatal(err)
	}
	pb.bloom, pb.mask, pb.ok = bloom, mask, true
	pb.hasRange, pb.min, pb.max = true, 10, 30
	close(pb.done)
	drainKeys(t, src)

	if sink.calls != 1 {
		t.Fatalf("groupSink calls = %d, want 1", sink.calls)
	}
	if len(sink.blooms) != 1 || len(sink.blooms[0].Bloom) != len(bloom) ||
		sink.blooms[0].Column != "k" || !sink.blooms[0].UseIntKey {
		t.Fatalf("iterator-level bloom not delivered: %+v", sink.blooms)
	}
	if len(sink.ranges) != 1 {
		t.Fatalf("iterator-level range not delivered: %+v", sink.ranges)
	}
	if minV, ok := sink.ranges[0].MinValue.(int64); !ok || minV != 10 {
		t.Errorf("range MinValue = %v, want int64(10)", sink.ranges[0].MinValue)
	}
	if maxV, ok := sink.ranges[0].MaxValue.(int64); !ok || maxV != 30 {
		t.Errorf("range MaxValue = %v, want int64(30)", sink.ranges[0].MaxValue)
	}
}

// A rangeless resolution (bloom-only artifact) must deliver the bloom with
// no range — never a zero-valued range that would falsely prune.
func TestBloomFilteredSourceLateAttach_NoRange(t *testing.T) {
	bloom, mask := testBloomOf(7)
	pb := &pendingBloom{done: make(chan struct{})}
	pb.bloom, pb.mask, pb.ok = bloom, mask, true
	close(pb.done)
	sink := &fakeDFAppender{}
	src := &bloomFilteredSource{
		inner:     &sliceSource{batches: []*batch.RecordBatch{intBatch(t, "k", []int64{7, 8})}},
		pending:   []*deferredBloomFilter{{pb: pb, filterID: "f1", column: "k", keyType: "int64"}},
		groupSink: sink,
		logger:    slog.Default(),
	}
	drainKeys(t, src)
	if len(sink.blooms) != 1 || len(sink.ranges) != 0 {
		t.Fatalf("want bloom without range, got blooms=%d ranges=%d", len(sink.blooms), len(sink.ranges))
	}
}

// fakeGroupIter records SetDynamicFilters deliveries on the iterator face.
type fakeGroupIter struct {
	ranges []exec.DynamicRange
	blooms []*exec.BloomScanFilter
	sets   int
}

func (f *fakeGroupIter) Next() (*batch.RecordBatch, error) { return nil, nil }
func (f *fakeGroupIter) SetDynamicFilters(ranges []exec.DynamicRange, blooms []*exec.BloomScanFilter) {
	f.ranges, f.blooms = ranges, blooms
	f.sets++
}
func (f *fakeGroupIter) Close() error { return nil }

// AddDynamicFilters must forward the accumulated union to the currently
// open parquet iterator AND a pre-opened next file, and keep it for files
// opened later (source fields).
func TestCachedFileStreamSourceAddDynamicFilters(t *testing.T) {
	cur, next := &fakeGroupIter{}, &fakeGroupIter{}
	s := &cachedFileStreamSource{parquetIter: cur, nextParquet: &pendingParquet{iter: next}}
	bloomA, maskA := testBloomOf(1)
	s.AddDynamicFilters(nil, []*exec.BloomScanFilter{{Bloom: bloomA, BloomMask: maskA, Column: "k", UseIntKey: true}})
	bloomB, maskB := testBloomOf(2)
	s.AddDynamicFilters(
		[]exec.DynamicRange{{Column: "k", MinValue: int64(1), MaxValue: int64(2)}},
		[]*exec.BloomScanFilter{{Bloom: bloomB, BloomMask: maskB, Column: "k", UseIntKey: true}})

	for _, it := range []*fakeGroupIter{cur, next} {
		if it.sets != 2 {
			t.Fatalf("iterator saw %d SetDynamicFilters calls, want 2", it.sets)
		}
		if len(it.blooms) != 2 || len(it.ranges) != 1 {
			t.Fatalf("iterator filter union: blooms=%d ranges=%d, want 2/1", len(it.blooms), len(it.ranges))
		}
	}
	if len(s.bloomFilters) != 2 || len(s.dynamicRanges) != 1 {
		t.Fatalf("source union: blooms=%d ranges=%d, want 2/1", len(s.bloomFilters), len(s.dynamicRanges))
	}
}

// stageRangedArtifact stages a partial/merged artifact carrying a build-key
// range beside its bloom.
func stageRangedArtifact(t *testing.T, store objstore.Store, bucket, key string, min, max int64, keys ...int64) {
	t.Helper()
	bloom, mask := testBloomOf(keys...)
	art := &distributed.DynamicFilterArtifact{
		KeyType: "int64", HasRange: true, Min: min, Max: max,
		Bloom: bloom, BloomMask: mask, RowCount: int64(len(keys)),
	}
	var buf bytes.Buffer
	if err := distributed.EncodeDynamicFilterArtifact(&buf, art); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(context.Background(), bucket, key,
		bytes.NewReader(buf.Bytes()), int64(buf.Len()), "application/octet-stream"); err != nil {
		t.Fatal(err)
	}
}

// The partial union folds each partial's range (min of mins / max of maxes);
// the resolved slot carries it for iterator-level range pruning.
func TestPollDeferredBloomPartialsRangeUnion(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey:      "queries/q/dynfilter-merged/scan-1/f1.wdf",
		PartialPrefix: "queries/st-scan-1-q/dynfilter/scan-1/",
		Deferred:      true,
	}
	stageRangedArtifact(t, store, "b", spec.PartialPrefix+"t1-f1.of2.wdf", 100, 200, 150)
	stageRangedArtifact(t, store, "b", spec.PartialPrefix+"t2-f1.of2.wdf", 50, 120, 60)
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("partials complete but poll did not resolve")
	}
	if _, _, ok := p.resolved(); !ok {
		t.Fatal("poll terminated without a bloom")
	}
	if !p.hasRange || p.min != 50 || p.max != 200 {
		t.Fatalf("range union = (hasRange=%v, %d, %d), want (true, 50, 200)", p.hasRange, p.min, p.max)
	}
}

// A non-empty partial WITHOUT a range breaks the union: an incomplete
// range would falsely prune groups holding the rangeless partial's keys.
func TestPollDeferredBloomPartialsRangeBroken(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey:      "queries/q/dynfilter-merged/scan-1/f1.wdf",
		PartialPrefix: "queries/st-scan-1-q/dynfilter/scan-1/",
		Deferred:      true,
	}
	stageRangedArtifact(t, store, "b", spec.PartialPrefix+"t1-f1.of2.wdf", 100, 200, 150)
	stageArtifact(t, store, "b", spec.PartialPrefix+"t2-f1.of2.wdf", 60) // rangeless
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("partials complete but poll did not resolve")
	}
	if _, _, ok := p.resolved(); !ok {
		t.Fatal("poll terminated without a bloom")
	}
	if p.hasRange {
		t.Fatalf("range must not survive a rangeless partial: (%d, %d)", p.min, p.max)
	}
}

// The merged (coordinator-staged) artifact's range rides the resolution.
func TestPollDeferredBloomMergedCarriesRange(t *testing.T) {
	store := objstore.NewMemStore()
	if err := store.MakeBucket(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	e := &Executor{store: store, logger: slog.Default()}
	spec := distributed.DynamicFilterSpec{
		FilterID: "f1", BloomBucket: "b",
		BloomKey: "queries/q/dynfilter-merged/scan-1/f1.wdf",
		Deferred: true,
	}
	stageRangedArtifact(t, store, "b", spec.BloomKey, 7, 9, 8)
	p := e.pollDeferredBloom(spec)
	select {
	case <-p.done:
	case <-time.After(5 * time.Second):
		t.Fatal("merged key present but poll did not resolve")
	}
	if !p.hasRange || p.min != 7 || p.max != 9 {
		t.Fatalf("merged range = (hasRange=%v, %d, %d), want (true, 7, 9)", p.hasRange, p.min, p.max)
	}
}
