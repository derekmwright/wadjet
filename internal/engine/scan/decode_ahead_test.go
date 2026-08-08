package scan

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/engine/exec"
	pqt "github.com/citc-tech/wadjet/internal/storage/parquet"
)

// manyRowGroupFile builds an in-memory parquet file with numGroups row
// groups of rowsPerGroup rows each, ids tagged g*1000+i (fourRowGroupFile
// shape, parameterized so decode-ahead tests can outnumber the workers).
func manyRowGroupFile(t *testing.T, numGroups, rowsPerGroup int) []byte {
	t.Helper()
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}
	cfg := pqt.DefaultWriterConfig()
	cfg.RowGroupSize = rowsPerGroup

	var buf bytes.Buffer
	w, err := pqt.NewWriter(&buf, pqt.Schema{Columns: schema}, cfg)
	if err != nil {
		t.Fatal(err)
	}
	for g := 0; g < numGroups; g++ {
		rows := make([]map[string]any, rowsPerGroup)
		for i := range rows {
			rows[i] = map[string]any{"id": int64(g*1000 + i)}
		}
		if err := w.WriteRows(rows); err != nil {
			t.Fatalf("write group %d: %v", g, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func openFileReader(t *testing.T, data []byte) *pqt.Reader {
	t.Helper()
	reader, err := pqt.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return reader
}

// drainGroupIter collects every batch an iterator yields, stopping on
// error. Returns batches and the terminal error (nil on clean exhaust).
type groupIter interface {
	Next() (*batch.RecordBatch, error)
}

func drainGroupIter(t *testing.T, it groupIter) ([]*batch.RecordBatch, error) {
	t.Helper()
	var out []*batch.RecordBatch
	for {
		b, err := it.Next()
		if err != nil {
			return out, err
		}
		if b == nil {
			return out, nil
		}
		out = append(out, b)
	}
}

// requireSameBatches asserts batch-by-batch identity (count, length, and
// every id value in order) between serial and decode-ahead output.
func requireSameBatches(t *testing.T, want, got []*batch.RecordBatch) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("batch count: got %d, want %d", len(got), len(want))
	}
	for bi := range want {
		if want[bi].Len != got[bi].Len {
			t.Fatalf("batch %d: len %d, want %d", bi, got[bi].Len, want[bi].Len)
		}
		for i := 0; i < want[bi].Len; i++ {
			w, g := want[bi].Columns[0].Int64Data[i], got[bi].Columns[0].Int64Data[i]
			if w != g {
				t.Fatalf("batch %d row %d: id %d, want %d", bi, i, g, w)
			}
		}
	}
}

// TestDecodeAheadIter_MatchesSerial: the consumer-facing contract is
// byte-identical to RowGroupIter — same batches, same order — across
// worker counts, including k exceeding the group count.
func TestDecodeAheadIter_MatchesSerial(t *testing.T) {
	data := manyRowGroupFile(t, 16, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	want, err := drainGroupIter(t, serial)
	if err != nil {
		t.Fatalf("serial drain: %v", err)
	}
	if len(want) != 16 {
		t.Fatalf("fixture: serial yielded %d batches, want 16", len(want))
	}

	for _, workers := range []int{1, 2, 4, 32} {
		it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
			DecodeAheadOpts{Workers: workers})
		if err != nil {
			t.Fatalf("workers=%d: %v", workers, err)
		}
		got, err := drainGroupIter(t, it)
		if err != nil {
			t.Fatalf("workers=%d drain: %v", workers, err)
		}
		requireSameBatches(t, want, got)
		if groups, _, _, _, _ := it.Stats(); groups != 16 {
			t.Errorf("workers=%d: groups read = %d, want 16", workers, groups)
		}
		// Post-exhaustion Next returns (nil, nil), like serial.
		if b, err := it.Next(); err != nil || b != nil {
			t.Errorf("workers=%d: Next after exhaustion = (%v, %v), want (nil, nil)", workers, b, err)
		}
		it.Close()
	}
}

// TestDecodeAheadIter_TinyWindowStallsButDelivers: a window smaller than
// any single group forces full serialization through the always-admit-
// the-delivery-cursor rule — output stays identical, no deadlock, and
// the stall counter proves the window actually gated.
func TestDecodeAheadIter_TinyWindowStallsButDelivers(t *testing.T) {
	data := manyRowGroupFile(t, 8, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	want, _ := drainGroupIter(t, serial)

	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, WindowBytes: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	got, err := drainGroupIter(t, it)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	requireSameBatches(t, want, got)
	if _, stalls, _, _, _ := it.Stats(); stalls == 0 {
		t.Error("WindowBytes=1 with 4 workers never stalled — window is not gating admission")
	}
	if wfNs, _, _, _ := it.StallDurations(); wfNs <= 0 {
		t.Error("window-full stalls counted but zero blocked duration recorded")
	}
}

// TestDecodeAheadIter_PressureDegradesToSerial: with the pressure hook
// permanently asserted, only the delivery-cursor group may be in flight
// — output stays identical (no deadlock, no loss) and the stall counter
// proves the hook gated admission.
func TestDecodeAheadIter_PressureDegradesToSerial(t *testing.T) {
	data := manyRowGroupFile(t, 8, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	want, _ := drainGroupIter(t, serial)

	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, Pressure: func() bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	got, err := drainGroupIter(t, it)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	requireSameBatches(t, want, got)
	if _, _, stalls, _, _ := it.Stats(); stalls == 0 {
		t.Error("permanent pressure with 4 workers never stalled — hook is not gating admission")
	}
	if _, pNs, _, _ := it.StallDurations(); pNs <= 0 {
		t.Error("pressure stalls counted but zero blocked duration recorded")
	}
}

// countingTokenPool counts acquire/release and enforces a capacity, so
// tests can prove per-group hold semantics: peak holds bounded, zero
// residual after drain, and progress with capacity 0.
type countingTokenPool struct {
	mu       sync.Mutex
	capacity int
	inUse    int
	peak     int
	acquires int
}

func (p *countingTokenPool) TryAcquire(n int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	avail := p.capacity - p.inUse
	if avail <= 0 {
		return 0
	}
	if n > avail {
		n = avail
	}
	p.inUse += n
	p.acquires += n
	if p.inUse > p.peak {
		p.peak = p.inUse
	}
	return n
}

func (p *countingTokenPool) Release(n int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inUse -= n
}

// TestDecodeAheadIter_PerGroupTokens: tokens are held per decode, not
// per worker lifetime — capacity 0 still drains the file (cursor group
// is token-exempt) with stalls counted; an ample pool ends with zero
// residual holds and per-group acquires.
func TestDecodeAheadIter_PerGroupTokens(t *testing.T) {
	data := manyRowGroupFile(t, 12, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	want, _ := drainGroupIter(t, serial)

	// Capacity 0: every non-cursor admission stalls, yet the iterator
	// must still deliver everything (serial via the cursor exemption).
	starved := &countingTokenPool{capacity: 0}
	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, Tokens: starved})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	got, err := drainGroupIter(t, it)
	if err != nil {
		t.Fatalf("starved drain: %v", err)
	}
	requireSameBatches(t, want, got)
	if _, _, _, stalls, _ := it.Stats(); stalls == 0 {
		t.Error("capacity-0 pool never produced a token stall")
	}
	if starved.acquires != 0 {
		t.Errorf("capacity-0 pool recorded %d acquires, want 0", starved.acquires)
	}

	// Ample pool: acquires happen per group, peak bounded by workers-ish,
	// and nothing is held after the drain (no hoarding).
	ample := &countingTokenPool{capacity: 16}
	it2, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, Tokens: ample})
	if err != nil {
		t.Fatal(err)
	}
	defer it2.Close()
	got2, err := drainGroupIter(t, it2)
	if err != nil {
		t.Fatalf("ample drain: %v", err)
	}
	requireSameBatches(t, want, got2)
	ample.mu.Lock()
	inUse, peak, acquires := ample.inUse, ample.peak, ample.acquires
	ample.mu.Unlock()
	if inUse != 0 {
		t.Errorf("tokens still held after drain: %d, want 0 (hoarding)", inUse)
	}
	if peak > 4 {
		t.Errorf("peak concurrent holds = %d, want <= workers (4)", peak)
	}
	if acquires == 0 {
		t.Error("ample pool saw no acquires — tokens not engaged")
	}
}

// TestDecodeAheadIter_RespectsShard mirrors the serial shard test: the
// union of all shards covers every row exactly once.
func TestDecodeAheadIter_RespectsShard(t *testing.T) {
	data := manyRowGroupFile(t, 4, 100)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	const shardCount = 4
	seen := make(map[int64]bool)
	totalRows := 0
	for shardIdx := 0; shardIdx < shardCount; shardIdx++ {
		it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, shardIdx, shardCount,
			DecodeAheadOpts{Workers: 2})
		if err != nil {
			t.Fatalf("shard %d: %v", shardIdx, err)
		}
		batches, err := drainGroupIter(t, it)
		if err != nil {
			t.Fatalf("shard %d drain: %v", shardIdx, err)
		}
		for _, b := range batches {
			for i := 0; i < b.Len; i++ {
				id := b.Columns[0].Int64Data[i]
				if seen[id] {
					t.Errorf("shard %d: duplicate id %d", shardIdx, id)
				}
				seen[id] = true
				totalRows++
			}
		}
		it.Close()
	}
	if totalRows != 4*100 {
		t.Errorf("union of shards: %d rows, want %d", totalRows, 4*100)
	}
}

// TestDecodeAheadIter_OutOfRangeShardYieldsNothing mirrors the serial
// empty-shard sentinel.
func TestDecodeAheadIter_OutOfRangeShardYieldsNothing(t *testing.T) {
	data := manyRowGroupFile(t, 4, 100)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}
	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 5, 4, DecodeAheadOpts{})
	if err != nil {
		t.Fatalf("expected nil err for out-of-range shard, got %v", err)
	}
	defer it.Close()
	if b, err := it.Next(); err != nil || b != nil {
		t.Errorf("Next on empty shard = (%v, %v), want (nil, nil)", b, err)
	}
}

// TestDecodeAheadIter_ErrorAtSerialPosition: corrupting row group 2's
// first data page makes the serial iterator deliver groups 0 and 1 and
// then error; decode-ahead must surface the same error at the same
// position even though later groups may have decoded fine already.
func TestDecodeAheadIter_ErrorAtSerialPosition(t *testing.T) {
	data := manyRowGroupFile(t, 6, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	// Locate group 2's first data page and stomp it.
	fr := openFileReader(t, data).FileReader()
	rg := fr.RowGroupMeta(2)
	if rg == nil || rg.Columns[0].MetaData == nil {
		t.Fatal("fixture: no metadata for row group 2")
	}
	off := rg.Columns[0].MetaData.DataPageOffset
	if dict := rg.Columns[0].MetaData.DictionaryPageOffset; dict > 0 && dict < off {
		off = dict
	}
	corrupted := append([]byte(nil), data...)
	for i := int64(0); i < 16 && off+i < int64(len(corrupted)); i++ {
		corrupted[off+i] ^= 0xFF
	}

	serial, err := OpenRowGroupIter(openFileReader(t, corrupted), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	serialBatches, serialErr := drainGroupIter(t, serial)
	if serialErr == nil {
		t.Fatal("fixture: corruption did not produce a serial decode error")
	}
	if len(serialBatches) != 2 {
		t.Fatalf("fixture: serial delivered %d batches before erroring, want 2", len(serialBatches))
	}

	it, err := OpenDecodeAheadIter(openFileReader(t, corrupted), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	gotBatches, gotErr := drainGroupIter(t, it)
	if gotErr == nil {
		t.Fatal("decode-ahead did not surface the decode error")
	}
	if len(gotBatches) != 2 {
		t.Fatalf("decode-ahead delivered %d batches before erroring, want 2 (serial parity)", len(gotBatches))
	}
	requireSameBatches(t, serialBatches, gotBatches)
	// Dead after error, like a failed task expects.
	if b, err := it.Next(); err != nil || b != nil {
		t.Errorf("Next after error = (%v, %v), want (nil, nil)", b, err)
	}
}

// TestDecodeAheadIter_PruneParity: dynamic-filter row-group pruning must
// skip the same groups as serial and report the same counters.
func TestDecodeAheadIter_PruneParity(t *testing.T) {
	data := manyRowGroupFile(t, 8, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}
	// Groups tag ids as g*1000+i — a range of [3000, 4049] covers exactly
	// groups 3 and 4; the other six groups prune.
	ranges := []exec.DynamicRange{{Column: "id", MinValue: int64(3000), MaxValue: int64(4049)}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	serial.SetDynamicFilters(ranges, nil)
	want, err := drainGroupIter(t, serial)
	if err != nil {
		t.Fatal(err)
	}
	_, wantPrunedRange, wantRead := serial.PruneStats()

	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer it.Close()
	it.SetDynamicFilters(ranges, nil)
	got, err := drainGroupIter(t, it)
	if err != nil {
		t.Fatal(err)
	}
	requireSameBatches(t, want, got)
	_, gotPrunedRange, gotRead := it.PruneStats()
	if gotPrunedRange != wantPrunedRange || gotRead != wantRead {
		t.Errorf("prune stats: (range=%d, read=%d), want (range=%d, read=%d)",
			gotPrunedRange, gotRead, wantPrunedRange, wantRead)
	}
	if wantRead != 2 {
		t.Errorf("fixture: serial read %d groups, want 2", wantRead)
	}
}

// TestDecodeAheadIter_CloseMidStreamJoinsWorkers: Close after a partial
// read must stop delivery and return only after in-flight decodes are
// joined — the caller munmaps the underlying bytes immediately after.
// The -race build is the real assertion here.
func TestDecodeAheadIter_CloseMidStreamJoinsWorkers(t *testing.T) {
	data := manyRowGroupFile(t, 16, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}
	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := it.Next(); err != nil {
		t.Fatal(err)
	}
	if err := it.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if b, err := it.Next(); err != nil || b != nil {
		t.Errorf("Next after Close = (%v, %v), want (nil, nil)", b, err)
	}
	// Close before first Next (workers never started) must not hang.
	it2, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1, DecodeAheadOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if err := it2.Close(); err != nil {
		t.Fatalf("Close before Next: %v", err)
	}
}

// countingLedger enforces a byte capacity and counts charge traffic so
// tests can prove the memo-§9 ledger semantics: denial-driven collapse
// to the cursor-only serial floor, forced cursor charges, and a zero
// balance once every group is delivered or dropped.
type countingLedger struct {
	mu       sync.Mutex
	capacity int64 // < 0 = unlimited
	balance  int64
	peak     int64
	reserves int64 // successful Reserve calls
	forced   int64 // ForceReserve calls
}

func (l *countingLedger) Reserve(n int64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.capacity >= 0 && l.balance+n > l.capacity {
		return fmt.Errorf("countingLedger: %d over capacity", n)
	}
	l.balance += n
	l.reserves++
	if l.balance > l.peak {
		l.peak = l.balance
	}
	return nil
}

func (l *countingLedger) ForceReserve(n int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.balance += n
	l.forced++
	if l.balance > l.peak {
		l.peak = l.balance
	}
}

func (l *countingLedger) Release(n int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.balance -= n
}

func (l *countingLedger) snapshot() (balance, peak, reserves, forced int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.balance, l.peak, l.reserves, l.forced
}

// TestDecodeAheadIter_LedgerDeniedCollapsesToSerial: a ledger with zero
// capacity denies every non-cursor admission, yet the iterator still
// delivers everything through the cursor exemption — whose bytes are
// force-charged, not skipped — and the ledger balance returns to zero.
func TestDecodeAheadIter_LedgerDeniedCollapsesToSerial(t *testing.T) {
	data := manyRowGroupFile(t, 8, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	want, _ := drainGroupIter(t, serial)

	ledger := &countingLedger{capacity: 0}
	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, Window: NewDecodeWindowWithLedger(0, ledger)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := drainGroupIter(t, it)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	requireSameBatches(t, want, got)
	if _, _, _, _, stalls := it.Stats(); stalls == 0 {
		t.Error("capacity-0 ledger never produced a ledger stall")
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	balance, _, reserves, forced := ledger.snapshot()
	if balance != 0 {
		t.Errorf("ledger balance after close = %d, want 0", balance)
	}
	if reserves != 0 {
		t.Errorf("capacity-0 ledger recorded %d successful reserves, want 0", reserves)
	}
	if forced == 0 {
		t.Error("cursor groups were never force-charged — real bytes invisible to the ledger")
	}
}

// TestDecodeAheadIter_LedgerBalancedOnAllPaths: with an unlimited ledger
// the balance returns to zero after a full drain, after a token-denial
// rollback storm (capacity-0 token pool releases every ledger reserve it
// cannot use), and after a mid-stream Close that drops parked slots.
func TestDecodeAheadIter_LedgerBalancedOnAllPaths(t *testing.T) {
	data := manyRowGroupFile(t, 12, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	want, _ := drainGroupIter(t, serial)

	// Full drain.
	ledger := &countingLedger{capacity: -1}
	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, Window: NewDecodeWindowWithLedger(0, ledger)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := drainGroupIter(t, it)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	requireSameBatches(t, want, got)
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	balance, peak, reserves, _ := ledger.snapshot()
	if balance != 0 {
		t.Errorf("ledger balance after full drain = %d, want 0", balance)
	}
	if reserves == 0 || peak == 0 {
		t.Errorf("unlimited ledger saw reserves=%d peak=%d — ledger not engaged", reserves, peak)
	}

	// Token denial must roll the ledger reserve back.
	rollback := &countingLedger{capacity: -1}
	it2, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, Tokens: &countingTokenPool{capacity: 0},
			Window: NewDecodeWindowWithLedger(0, rollback)})
	if err != nil {
		t.Fatal(err)
	}
	got2, err := drainGroupIter(t, it2)
	if err != nil {
		t.Fatalf("rollback drain: %v", err)
	}
	requireSameBatches(t, want, got2)
	if err := it2.Close(); err != nil {
		t.Fatal(err)
	}
	if balance, _, _, _ := rollback.snapshot(); balance != 0 {
		t.Errorf("ledger balance after token-rollback drain = %d, want 0", balance)
	}

	// Mid-stream Close drops undelivered slots and credits their charges.
	mid := &countingLedger{capacity: -1}
	it3, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4, Window: NewDecodeWindowWithLedger(0, mid)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := it3.Next(); err != nil {
		t.Fatal(err)
	}
	if err := it3.Close(); err != nil {
		t.Fatal(err)
	}
	if balance, _, _, _ := mid.snapshot(); balance != 0 {
		t.Errorf("ledger balance after mid-stream close = %d, want 0", balance)
	}
}

// TestDecodeAheadIter_PressureOccupancyFloor: memo §9.5 — under
// permanent pressure the default (non-strict) mode still pipelines
// 2-deep (cursor + one ahead): output identical to serial, the ahead
// count peaks at exactly 1, stalls fire once the floor is held, and
// the count returns to zero on drain and on mid-stream Close. Strict
// mode admits nothing ahead (peak 0), preserving the edge behavior.
func TestDecodeAheadIter_PressureOccupancyFloor(t *testing.T) {
	data := manyRowGroupFile(t, 12, 50)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	serial, err := OpenRowGroupIter(openFileReader(t, data), schema, nil, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	want, _ := drainGroupIter(t, serial)

	run := func(strict bool) (got []*batch.RecordBatch, stalls int64, aheadPeak int, it *DecodeAheadIter) {
		t.Helper()
		it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
			DecodeAheadOpts{Workers: 4, PressureStrict: strict, Pressure: func() bool { return true }})
		if err != nil {
			t.Fatal(err)
		}
		// Sample the shared ahead count between deliveries; the mutex
		// serializes reads with admission so peaks cannot be missed
		// entirely (a worker may admit between samples, but the floor
		// guarantee is that ahead never EXCEEDS the bound — asserting
		// the sampled max <= bound is what matters).
		for {
			b, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			it.win.mu.Lock()
			if it.win.ahead > aheadPeak {
				aheadPeak = it.win.ahead
			}
			it.win.mu.Unlock()
			if b == nil {
				break
			}
			got = append(got, b)
		}
		_, _, stalls, _, _ = it.Stats()
		return got, stalls, aheadPeak, it
	}

	// Default (floored): 2-deep, correct, stalled, drained ahead == 0.
	got, stalls, peak, it := run(false)
	requireSameBatches(t, want, got)
	if peak > 1 {
		t.Errorf("floored mode ahead peak = %d, want <= 1", peak)
	}
	if stalls == 0 {
		t.Error("floored mode never stalled — floor not gating past 1 ahead")
	}
	it.win.mu.Lock()
	if it.win.ahead != 0 {
		t.Errorf("ahead after drain = %d, want 0", it.win.ahead)
	}
	it.win.mu.Unlock()
	it.Close()

	// Strict: nothing ahead, ever.
	got2, stalls2, peak2, it2 := run(true)
	requireSameBatches(t, want, got2)
	if peak2 != 0 {
		t.Errorf("strict mode ahead peak = %d, want 0", peak2)
	}
	if stalls2 == 0 {
		t.Error("strict mode never stalled")
	}
	it2.Close()

	// Mid-stream Close drops parked ahead slots and zeroes the count.
	it3, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 4})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := it3.Next(); err != nil {
		t.Fatal(err)
	}
	if err := it3.Close(); err != nil {
		t.Fatal(err)
	}
	it3.win.mu.Lock()
	if it3.win.ahead != 0 {
		t.Errorf("ahead after mid-stream close = %d, want 0", it3.win.ahead)
	}
	it3.win.mu.Unlock()
}

// TestDecodeAheadIter_AdviseCoversAllGroups: with an Advise hook wired,
// every row group's projected column chunks are announced exactly once,
// with sane file-relative ranges, and output is unchanged.
func TestDecodeAheadIter_AdviseCoversAllGroups(t *testing.T) {
	const groups = 12
	data := manyRowGroupFile(t, groups, 40)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	var mu sync.Mutex
	type span struct{ off, n int64 }
	var advised []span
	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 0, 1,
		DecodeAheadOpts{Workers: 3, Advise: func(off, n int64) {
			mu.Lock()
			advised = append(advised, span{off, n})
			mu.Unlock()
		}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := drainGroupIter(t, it)
	it.Close()
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if len(got) != groups {
		t.Fatalf("batches = %d, want %d", len(got), groups)
	}
	mu.Lock()
	defer mu.Unlock()
	// One projected leaf per group → exactly one span per group.
	if len(advised) != groups {
		t.Fatalf("advised spans = %d, want %d (one per group, no repeats)", len(advised), groups)
	}
	seen := make(map[int64]bool)
	for _, s := range advised {
		if s.off < 0 || s.n <= 0 || s.off+s.n > int64(len(data)) {
			t.Fatalf("advised span [%d,+%d) outside file of %d bytes", s.off, s.n, len(data))
		}
		if seen[s.off] {
			t.Fatalf("span at offset %d advised twice", s.off)
		}
		seen[s.off] = true
	}
}

// TestDecodeAheadIter_AdviseRespectsShardRange: a sharded iterator must
// only advise the row groups in its own shard slice.
func TestDecodeAheadIter_AdviseRespectsShardRange(t *testing.T) {
	const groups = 12
	data := manyRowGroupFile(t, groups, 40)
	schema := []pqt.Column{{Name: "id", Type: pqt.TypeInt64}}

	var mu sync.Mutex
	var spans int
	it, err := OpenDecodeAheadIter(openFileReader(t, data), schema, nil, 1, 3,
		DecodeAheadOpts{Workers: 2, Advise: func(off, n int64) {
			mu.Lock()
			spans++
			mu.Unlock()
		}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := drainGroupIter(t, it)
	it.Close()
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if spans != len(got) {
		t.Fatalf("advised %d spans for a shard that decoded %d groups", spans, len(got))
	}
	if spans == 0 || spans >= groups {
		t.Fatalf("shard advised %d of %d groups; want a proper subset", spans, groups)
	}
}
