package coordinator

import (
	"bufio"
	"context"
	"encoding/binary"
	"os"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// wshfInt64Payload hand-builds a one-chunk WSHF payload with a single int64
// column of n rows (format documented in internal/worker/shuffle_format.go).
// Values are base, base+1, ...
func wshfInt64Payload(tb testing.TB, n int, base int64) []byte {
	tb.Helper()
	var buf []byte
	buf = append(buf, 'W', 'S', 'H', 'F')
	buf = binary.LittleEndian.AppendUint32(buf, 1) // 1 chunk
	buf = binary.LittleEndian.AppendUint16(buf, 1) // 1 col
	buf = binary.LittleEndian.AppendUint16(buf, 1) // nameLen
	buf = append(buf, 'x')
	buf = append(buf, byte(parquet.TypeInt64))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(n)) // numRows
	words := (n + 63) / 64
	buf = binary.LittleEndian.AppendUint32(buf, uint32(words))
	for w := 0; w < words; w++ {
		buf = binary.LittleEndian.AppendUint64(buf, ^uint64(0)) // all valid
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(n*8))
	for i := 0; i < n; i++ {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(base+int64(i)))
	}
	return buf
}

// drainReplay drains a stream, returning every int64 value seen in column 0
// and asserting per-batch sanity.
func drainReplay(tb testing.TB, s BatchStream) []int64 {
	tb.Helper()
	var vals []int64
	for {
		b, err := s.Next(context.Background())
		if err != nil {
			tb.Fatalf("replay Next: %v", err)
		}
		if b == nil {
			return vals
		}
		for i := 0; i < b.ActiveLen(); i++ {
			row := i
			if b.Sel != nil {
				row = int(b.Sel[i])
			}
			vals = append(vals, b.Columns[0].Int64Data[row])
		}
	}
}

// Regression test for the streaming-SQLResult refactor: past the budget the
// receiver must DEGRADE — keep the decoded in-memory prefix, append the
// remaining raw frames to local scratch, and replay them lazily — instead
// of failing the query (PR #142's interim behavior) or OOMing the process
// (the original sweep finding #14).
func TestGatherReceiver_BudgetSpillsAndReplays(t *testing.T) {
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            32 * 1024, // 32 KiB — a few 2048-row int64 payloads
		done:              make(chan struct{}, 1),
	}

	const rowsPerMsg = 2048
	for i := 0; i < 5; i++ {
		payload := wshfInt64Payload(t, rowsPerMsg, int64(i*rowsPerMsg))
		r.handleParsed(&distributed.GatherBatchMsg{Payload: payload, RowCount: rowsPerMsg, WorkerID: "w"})
	}
	r.handleParsed(&distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})

	r.mu.Lock()
	if r.workerErr != "" {
		r.mu.Unlock()
		t.Fatalf("over-budget receiver must not fail the query, got %q", r.workerErr)
	}
	if r.spillPath == "" || r.spillBytes == 0 {
		r.mu.Unlock()
		t.Fatal("receiver did not spill past the 32KiB budget")
	}
	if len(r.batches) == 0 {
		r.mu.Unlock()
		t.Fatal("in-memory prefix dropped — prefix under budget must be kept")
	}
	if r.accBytes > r.budget+rowsPerMsg*16 {
		r.mu.Unlock()
		t.Fatalf("in-memory bytes %d far exceed budget %d", r.accBytes, r.budget)
	}
	r.mu.Unlock()

	gr, err := r.wait(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if gr.totalRows != 5*rowsPerMsg {
		t.Fatalf("totalRows = %d, want %d", gr.totalRows, 5*rowsPerMsg)
	}
	if gr.columns == nil || gr.columns[0] != "x" {
		t.Fatalf("columns = %v, want [x]", gr.columns)
	}
	if _, statErr := os.Stat(gr.spillPath); statErr != nil {
		t.Fatalf("scratch file missing after claim: %v", statErr)
	}

	// Replay must yield every row exactly once, in arrival order
	// (prefix first, then spilled frames — which IS arrival order).
	s := newGatherReplayStream(gr.batches, gr.spillPath, gr.renamer)
	vals := drainReplay(t, s)
	if len(vals) != 5*rowsPerMsg {
		t.Fatalf("replayed %d rows, want %d", len(vals), 5*rowsPerMsg)
	}
	for i, v := range vals {
		if v != int64(i) {
			t.Fatalf("row %d = %d, want %d", i, v, i)
		}
	}
	// Exhaustion deletes the scratch file.
	if _, statErr := os.Stat(gr.spillPath); !os.IsNotExist(statErr) {
		t.Fatalf("scratch file not removed after exhausted replay: %v", statErr)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close after exhaustion: %v", err)
	}
}

// Close before exhaustion must remove the scratch file (the alerts path
// stops at its row limit; pgwire clients can disconnect mid-stream).
func TestGatherReplayStream_CloseRemovesScratch(t *testing.T) {
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            1, // decode exactly one payload, spill the rest
		done:              make(chan struct{}, 1),
	}
	for i := 0; i < 3; i++ {
		r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 64, 0), RowCount: 64, WorkerID: "w"})
	}
	r.handleParsed(&distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})
	gr, err := r.wait(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	s := newGatherReplayStream(gr.batches, gr.spillPath, nil)
	if b, err := s.Next(context.Background()); err != nil || b == nil {
		t.Fatalf("first Next = %v, %v", b, err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, statErr := os.Stat(gr.spillPath); !os.IsNotExist(statErr) {
		t.Fatalf("scratch not removed by Close: %v", statErr)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// discard() without a claiming wait() must remove scratch (dispatch error
// paths) — and a late frame after discard must not recreate it.
func TestGatherReceiver_DiscardRemovesScratch(t *testing.T) {
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            1,
		done:              make(chan struct{}, 1),
	}
	for i := 0; i < 2; i++ {
		r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 64, 0), RowCount: 64, WorkerID: "w"})
	}
	r.mu.Lock()
	path := r.spillPath
	r.mu.Unlock()
	if path == "" {
		t.Fatal("no spill before discard")
	}
	r.discard()
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("scratch not removed by discard: %v", statErr)
	}
	// Late frame after discard: must not recreate scratch.
	r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 64, 0), RowCount: 64, WorkerID: "w"})
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.spillPath != "" || r.spillFile != nil {
		t.Fatal("late frame recreated scratch after discard")
	}
}

// A scratch write failure degrades to the clean per-query fail — the
// process must not die, and the accumulated result is dropped.
func TestGatherReceiver_SpillWriteFailureFailsQueryCleanly(t *testing.T) {
	scratch := t.TempDir() + "/ro-scratch"
	if err := os.WriteFile(scratch, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(scratch) // read-only handle: writes fail
	if err != nil {
		t.Fatal(err)
	}
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            1,
		done:              make(chan struct{}, 1),
	}
	// First payload decodes (fills the prefix past budget=1).
	r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 64, 0), RowCount: 64, WorkerID: "w"})
	// Force the spill file to the read-only handle, with a tiny buffer so
	// the write surfaces immediately (payload > 16 bytes forces a flush).
	r.mu.Lock()
	r.spillFile = f
	r.spillPath = scratch
	r.spillW = bufio.NewWriterSize(f, 16)
	r.mu.Unlock()
	r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 64, 0), RowCount: 64, WorkerID: "w"})
	r.handleParsed(&distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})

	r.mu.Lock()
	if !r.spillFailed {
		r.mu.Unlock()
		t.Fatal("spill write failure not recorded")
	}
	if r.workerErr == "" {
		r.mu.Unlock()
		t.Fatal("no error recorded on spill failure")
	}
	if r.batches != nil {
		r.mu.Unlock()
		t.Fatal("accumulated batches not freed on spill failure")
	}
	r.mu.Unlock()

	if _, err := r.wait(context.Background(), time.Second); err == nil {
		t.Fatal("wait must surface the spill failure as a query error")
	}
}

func TestGatherReceiver_UnderBudgetUnaffected(t *testing.T) {
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            1 << 20,
		done:              make(chan struct{}, 1),
	}
	r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 100, 0), RowCount: 100, WorkerID: "w"})
	r.handleParsed(&distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.spillPath != "" || r.workerErr != "" {
		t.Fatalf("under-budget receiver flagged: spill=%q err=%q", r.spillPath, r.workerErr)
	}
	if r.totalRows != 100 || len(r.batches) != 1 {
		t.Fatalf("rows=%d batches=%d, want 100/1", r.totalRows, len(r.batches))
	}
	if r.columns == nil || r.columns[0] != "x" {
		t.Fatalf("columns = %v, want [x]", r.columns)
	}
}

// Renames recorded on the gather result must apply to REPLAYED batches the
// same as to the in-memory prefix — schema names diverging mid-stream would
// be silent wrong results for any over-budget query with SELECT aliases.
func TestGatherReplayStream_AppliesRenames(t *testing.T) {
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            1,
		done:              make(chan struct{}, 1),
	}
	for i := 0; i < 2; i++ {
		r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 8, 0), RowCount: 8, WorkerID: "w"})
	}
	r.handleParsed(&distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})
	gr, err := r.wait(context.Background(), time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	applyOutputRenames(gr, []physical.OutputRename{{From: "x", To: "alias_x"}})
	if gr.columns[0] != "alias_x" {
		t.Fatalf("columns after rename = %v", gr.columns)
	}
	if gr.renamer == nil {
		t.Fatal("renamer not recorded on spilled gather result")
	}
	s := newGatherReplayStream(gr.batches, gr.spillPath, gr.renamer)
	defer s.Close()
	n := 0
	for {
		b, err := s.Next(context.Background())
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if b == nil {
			break
		}
		n++
		if b.Schema[0].Name != "alias_x" {
			t.Fatalf("batch %d schema name = %q, want alias_x", n, b.Schema[0].Name)
		}
	}
	if n != 2 {
		t.Fatalf("replayed %d batches, want 2", n)
	}
}

func TestGatherResultBudget_Resolution(t *testing.T) {
	c := &Coordinator{config: Config{GatherResultBudget: 123}}
	if got := c.gatherResultBudget(); got != 123 {
		t.Errorf("explicit budget: got %d, want 123", got)
	}
	c = &Coordinator{config: Config{GatherResultBudget: -1}}
	if got := c.gatherResultBudget(); got != 0 {
		t.Errorf("negative (uncapped): got %d, want 0", got)
	}
	c = &Coordinator{}
	if got := c.gatherResultBudget(); got <= 0 {
		t.Errorf("default budget: got %d, want > 0", got)
	}
}
