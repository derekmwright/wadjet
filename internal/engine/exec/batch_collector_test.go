package exec

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/memory"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// Tests for SpillableBatchCollector (sweep finding #12): the join bridges'
// probe-side collection must be tracker-charged, spill past pressure, and
// shed memory and scratch progressively during replay.

var collectorSchema = []parquet.Column{
	{Name: "k", Type: parquet.TypeInt64},
	{Name: "s", Type: parquet.TypeString},
}

func collectorBatch(tb testing.TB, start, n int) *batch.RecordBatch {
	tb.Helper()
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = map[string]any{"k": int64(start + i), "s": string(rune('a' + (start+i)%26))}
	}
	return batch.FromRows(collectorSchema, rows)
}

func replayAllKeys(tb testing.TB, c *SpillableBatchCollector) []int64 {
	tb.Helper()
	var keys []int64
	for {
		b, err := c.NextReplay(context.Background())
		if err != nil {
			tb.Fatal(err)
		}
		if b == nil {
			return keys
		}
		for _, r := range b.ToRows() {
			keys = append(keys, r["k"].(int64))
		}
	}
}

func TestSpillableBatchCollector_InMemoryReplay(t *testing.T) {
	c := &SpillableBatchCollector{}
	ctx := context.Background()
	if err := c.Consume(ctx, collectorBatch(t, 0, 5)); err != nil {
		t.Fatal(err)
	}
	// Selection vectors must be honored and snapshotted.
	scratch := []uint32{1, 3} // a filter operator's reusable outSel buffer
	sel := collectorBatch(t, 5, 4)
	sel.Sel = scratch // keys 6, 8
	if err := c.Consume(ctx, sel); err != nil {
		t.Fatal(err)
	}
	scratch[0] = 0 // operator clobbers its scratch — the snapshot must hold

	if c.Rows() != 7 {
		t.Fatalf("Rows() = %d, want 7", c.Rows())
	}
	keys := replayAllKeys(t, c)
	want := []int64{0, 1, 2, 3, 4, 6, 8}
	if len(keys) != len(want) {
		t.Fatalf("replayed %d rows, want %d (%v)", len(keys), len(want), keys)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("keys[%d] = %d, want %d", i, keys[i], want[i])
		}
	}
	c.Release()
}

func TestSpillableBatchCollector_SpillRoundTrip(t *testing.T) {
	forceTinyRuns(t)
	tracker := memory.NewTracker("collector-test", 1) // 1-byte budget: every Consume is over pressure
	dir := t.TempDir()
	sm, err := memory.NewSpillManager(dir, tracker)
	if err != nil {
		t.Fatal(err)
	}
	c := &SpillableBatchCollector{Spill: sm}
	ctx := context.Background()

	const total = 10_000
	for start := 0; start < total; start += 500 {
		b := collectorBatch(t, start, 500)
		if err := c.Consume(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	spillGlob := filepath.Join(sm.SpillDir(), "bridge-collect-*.bin")
	files, _ := filepath.Glob(spillGlob)
	if len(files) == 0 {
		t.Fatal("expected spill files under pressure, found none")
	}

	// Iterate (the bloom pre-scan) must see every row without consuming.
	seen := 0
	if err := c.Iterate(func(b *batch.RecordBatch) error {
		seen += b.ActiveLen()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if seen != total {
		t.Fatalf("Iterate saw %d rows, want %d", seen, total)
	}

	keys := replayAllKeys(t, c)
	if len(keys) != total {
		t.Fatalf("replayed %d rows, want %d", len(keys), total)
	}
	for i, k := range keys {
		if k != int64(i) {
			t.Fatalf("keys[%d] = %d; order or content corrupted", i, k)
		}
	}

	// Consumed scratch is deleted during replay; nothing should remain.
	files, _ = filepath.Glob(spillGlob)
	if len(files) != 0 {
		t.Errorf("replay left spill scratch behind: %v", files)
	}
	c.Release()
	if got := tracker.Used(); got != 0 {
		t.Errorf("tracker still charged %d bytes after Release", got)
	}
}

func TestSpillableBatchCollector_ReleaseMidReplay(t *testing.T) {
	forceTinyRuns(t)
	tracker := memory.NewTracker("collector-test", 1)
	sm, err := memory.NewSpillManager(t.TempDir(), tracker)
	if err != nil {
		t.Fatal(err)
	}
	c := &SpillableBatchCollector{Spill: sm}
	ctx := context.Background()
	for start := 0; start < 4000; start += 500 {
		if err := c.Consume(ctx, collectorBatch(t, start, 500)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.NextReplay(ctx); err != nil { // start replay, then abandon
		t.Fatal(err)
	}
	c.Release()
	c.Release() // idempotent

	files, _ := filepath.Glob(filepath.Join(sm.SpillDir(), "bridge-collect-*.bin"))
	if len(files) != 0 {
		t.Errorf("Release left spill scratch behind: %v", files)
	}
	if got := tracker.Used(); got != 0 {
		t.Errorf("tracker still charged %d bytes after Release", got)
	}
	if b, err := c.NextReplay(ctx); err != nil || b != nil {
		t.Errorf("NextReplay after Release = (%v, %v), want (nil, nil)", b, err)
	}
}

// The incremental bloom build (NewBloomSized + BloomAddBatch over a
// collector Iterate) must produce a bloom identical to the one-shot
// BuildBloomFromBatches it replaces in reverseBloomBridge.
func TestBloomAddBatch_MatchesOneShot(t *testing.T) {
	batches := []*batch.RecordBatch{
		collectorBatch(t, 0, 100),
		collectorBatch(t, 100, 57),
	}
	batches[1].Sel = []uint32{0, 5, 10}

	wantBloom, wantMask := BuildBloomFromBatches(batches, "k")

	rows := 0
	for _, b := range batches {
		rows += b.ActiveLen()
	}
	gotBloom, gotMask := NewBloomSized(rows)
	for _, b := range batches {
		BloomAddBatch(gotBloom, gotMask, b, "k")
	}

	if gotMask != wantMask || len(gotBloom) != len(wantBloom) {
		t.Fatalf("shape mismatch: mask %d vs %d, slots %d vs %d", gotMask, wantMask, len(gotBloom), len(wantBloom))
	}
	for i := range wantBloom {
		if gotBloom[i] != wantBloom[i] {
			t.Fatalf("bloom word %d differs", i)
		}
	}
}
