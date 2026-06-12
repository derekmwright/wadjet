package coordinator

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// wshfInt64Payload hand-builds a one-chunk WSHF payload with a single int64
// column of n rows (format documented in internal/worker/shuffle_format.go).
func wshfInt64Payload(tb testing.TB, n int) []byte {
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
		buf = binary.LittleEndian.AppendUint64(buf, uint64(i))
	}
	return buf
}

// Regression test for sweep finding #14: the gather receiver accumulated
// every decoded batch with no cap or charge — a no-LIMIT distributed result
// landed fully in coordinator heap and OOM-killed the process (all queries
// die). Over budget, the receiver must fail THIS query cleanly: drop the
// accumulated batches, surface a clear error from wait(), and keep counting
// terminals so wait() returns promptly.
func TestGatherReceiver_BudgetCap(t *testing.T) {
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            32 * 1024, // 32 KiB — a few 2048-row int64 payloads
		done:              make(chan struct{}, 1),
	}

	payload := wshfInt64Payload(t, 2048) // ~16 KiB decoded
	for i := 0; i < 5; i++ {
		r.handleParsed(&distributed.GatherBatchMsg{Payload: payload, RowCount: 2048, WorkerID: "w"})
	}

	r.mu.Lock()
	if !r.overBudget {
		r.mu.Unlock()
		t.Fatal("receiver not over budget after 5×16KiB against a 32KiB cap")
	}
	if r.batches != nil {
		r.mu.Unlock()
		t.Fatal("accumulated batches not freed on over-budget")
	}
	if r.workerErr == "" {
		r.mu.Unlock()
		t.Fatal("no error recorded on over-budget")
	}
	r.mu.Unlock()

	// Terminal must still complete the receiver.
	r.handleParsed(&distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})

	// wait() needs a subscription only for its deferred Unsubscribe; give
	// it none by checking the done/err state directly through the public
	// pieces we can reach: done fired and workerErr set.
	select {
	case <-r.done:
	case <-time.After(time.Second):
		t.Fatal("done not signaled after terminal")
	}
}

func TestGatherReceiver_UnderBudgetUnaffected(t *testing.T) {
	r := &gatherReceiver{
		expectedTerminals: 1,
		budget:            1 << 20,
		done:              make(chan struct{}, 1),
	}
	r.handleParsed(&distributed.GatherBatchMsg{Payload: wshfInt64Payload(t, 100), RowCount: 100, WorkerID: "w"})
	r.handleParsed(&distributed.GatherBatchMsg{Terminal: true, WorkerID: "w"})

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.overBudget || r.workerErr != "" {
		t.Fatalf("under-budget receiver flagged: overBudget=%v err=%q", r.overBudget, r.workerErr)
	}
	if r.totalRows != 100 || len(r.batches) != 1 {
		t.Fatalf("rows=%d batches=%d, want 100/1", r.totalRows, len(r.batches))
	}
	if r.columns == nil || r.columns[0] != "x" {
		t.Fatalf("columns = %v, want [x]", r.columns)
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
