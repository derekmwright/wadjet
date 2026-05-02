package worker

import (
	"context"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// TestGatherReplySink publishes two batches and a terminal to a NATS reply
// subject, then asserts the subscriber receives the WSHF-encoded payloads
// and can decode them back to the original rows.
func TestGatherReplySink(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	const subject = "test.gather.reply"
	var (
		mu       sync.Mutex
		received []distributed.GatherBatchMsg
		done     = make(chan struct{})
	)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var msg distributed.GatherBatchMsg
		if err := distributed.Unmarshal(m.Data, &msg); err != nil {
			t.Logf("unmarshal: %v", err)
			return
		}
		mu.Lock()
		received = append(received, msg)
		terminal := msg.Terminal
		mu.Unlock()
		if terminal {
			close(done)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Two batches: one with 3 rows, one with 2 rows.
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "name", Type: parquet.TypeString, Nullable: true},
	}
	b1 := batch.NewRecordBatch(schema, 3)
	b1.Columns[0].Int64Data[0], b1.Columns[0].Int64Data[1], b1.Columns[0].Int64Data[2] = 1, 2, 3
	b1.Columns[1].BytesData.Set(0, []byte("alice"))
	b1.Columns[1].BytesData.Set(1, []byte("bob"))
	b1.Columns[1].BytesData.Set(2, []byte("carol"))

	b2 := batch.NewRecordBatch(schema, 2)
	b2.Columns[0].Int64Data[0], b2.Columns[0].Int64Data[1] = 4, 5
	b2.Columns[1].BytesData.Set(0, []byte("dave"))
	b2.Columns[1].BytesData.Set(1, []byte("eve"))

	sink := newGatherReplySink(nc, subject, schema)
	ctx := context.Background()
	if err := sink.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := sink.Consume(ctx, b1); err != nil {
		t.Fatalf("Consume 1: %v", err)
	}
	if err := sink.Consume(ctx, b2); err != nil {
		t.Fatalf("Consume 2: %v", err)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for terminal")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(received) != 3 {
		t.Fatalf("want 3 messages (2 batches + terminal), got %d", len(received))
	}
	if received[2].Terminal != true || received[2].Err != "" {
		t.Errorf("terminal: got terminal=%v err=%q", received[2].Terminal, received[2].Err)
	}
	if received[0].RowCount != 3 || received[1].RowCount != 2 {
		t.Errorf("row counts: got %d,%d want 3,2", received[0].RowCount, received[1].RowCount)
	}

	// Decode payloads via shuffleChunkReader and verify round-trip.
	wantIDs := []int64{1, 2, 3, 4, 5}
	wantNames := []string{"alice", "bob", "carol", "dave", "eve"}
	var gotIDs []int64
	var gotNames []string
	for i, msg := range received[:2] {
		rdr, err := newShuffleChunkReader(msg.Payload)
		if err != nil {
			t.Fatalf("msg %d: reader: %v", i, err)
		}
		for {
			rb, err := rdr.Next()
			if err != nil {
				t.Fatalf("msg %d: Next: %v", i, err)
			}
			if rb == nil {
				break
			}
			for r := 0; r < rb.Len; r++ {
				gotIDs = append(gotIDs, rb.Columns[0].Int64Data[r])
				gotNames = append(gotNames, string(rb.Columns[1].BytesData.Value(r)))
			}
		}
	}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("decoded rows: got %d, want %d", len(gotIDs), len(wantIDs))
	}
	for i := range wantIDs {
		if gotIDs[i] != wantIDs[i] || gotNames[i] != wantNames[i] {
			t.Errorf("row %d: got (%d,%s) want (%d,%s)", i, gotIDs[i], gotNames[i], wantIDs[i], wantNames[i])
		}
	}
}

// TestGatherReplySinkChunksOversizedBatch is a regression test for the
// grace-hash-join hang on the harness's micro_grace_hash_join: when the
// upstream join's 1:N output produced a single batch larger than 8 MB,
// the previous gather sink tried to publish it as one NATS message and
// hit "nats: maximum payload exceeded". The task failed without
// publishing a terminal marker; the coord then waited on the reply
// subject until query timeout (~2 minutes) instead of failing fast.
//
// The fix: cap each NATS message at gatherMaxRowsPerMessage rows and
// split larger batches into multiple messages.
func TestGatherReplySinkChunksOversizedBatch(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	en, err := distributed.NewEmbeddedNATS(cfg, logger)
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)

	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	const subject = "test.gather.chunks"
	var (
		mu       sync.Mutex
		received []distributed.GatherBatchMsg
		done     = make(chan struct{})
	)
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		var msg distributed.GatherBatchMsg
		if err := distributed.Unmarshal(m.Data, &msg); err != nil {
			return
		}
		mu.Lock()
		received = append(received, msg)
		terminal := msg.Terminal
		mu.Unlock()
		if terminal {
			close(done)
		}
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	// Build a batch large enough that publishing it as a single NATS
	// message would have failed previously. 4 × DefaultBatchSize rows
	// guarantees splitting into at least 4 chunks.
	const totalRows = 4 * batch.DefaultBatchSize
	schema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64, Nullable: true},
		{Name: "val", Type: parquet.TypeInt64, Nullable: true},
	}
	b := batch.NewRecordBatch(schema, totalRows)
	wantSum := int64(0)
	for i := 0; i < totalRows; i++ {
		b.Columns[0].Int64Data[i] = int64(i)
		b.Columns[1].Int64Data[i] = int64(i + 1)
		wantSum += int64(i + 1)
	}

	sink := newGatherReplySink(nc, subject, schema)
	ctx := context.Background()
	if err := sink.Init(ctx); err != nil {
		t.Fatalf("Init: %v", err)
	}
	if err := sink.Consume(ctx, b); err != nil {
		t.Fatalf("Consume: %v", err)
	}
	if err := sink.Finalize(ctx); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for terminal")
	}

	mu.Lock()
	defer mu.Unlock()
	// Expect at least totalRows / DefaultBatchSize batch messages plus 1
	// terminal. Allow exactly that — the chunker is row-count-bounded so
	// the count is deterministic for this row width.
	wantBatchMsgs := totalRows / batch.DefaultBatchSize
	if len(received) != wantBatchMsgs+1 {
		t.Fatalf("messages: got %d, want %d (= %d batch chunks + 1 terminal)",
			len(received), wantBatchMsgs+1, wantBatchMsgs)
	}
	if !received[len(received)-1].Terminal {
		t.Errorf("last message should be terminal, got %v", received[len(received)-1])
	}
	// Round-trip every chunk and verify the union covers all input rows.
	totalDecoded := 0
	gotSum := int64(0)
	for i, msg := range received[:wantBatchMsgs] {
		rdr, err := newShuffleChunkReader(msg.Payload)
		if err != nil {
			t.Fatalf("msg %d reader: %v", i, err)
		}
		for {
			rb, err := rdr.Next()
			if err != nil {
				t.Fatalf("msg %d Next: %v", i, err)
			}
			if rb == nil {
				break
			}
			for r := 0; r < rb.Len; r++ {
				gotSum += rb.Columns[1].Int64Data[r]
			}
			totalDecoded += rb.Len
		}
	}
	if totalDecoded != totalRows {
		t.Errorf("decoded rows: got %d want %d", totalDecoded, totalRows)
	}
	if gotSum != wantSum {
		t.Errorf("sum: got %d want %d", gotSum, wantSum)
	}
}

// TestGatherReplySinkConsumeError verifies that a failure inside Consume
// surfaces in the terminal message's Err field, and that subsequent Consume
// calls are no-ops.
func TestGatherReplySinkConsumeError(t *testing.T) {
	cfg := distributed.DefaultNATSConfig()
	cfg.Port = -1
	cfg.StoreDir = t.TempDir()
	en, err := distributed.NewEmbeddedNATS(cfg, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	if err != nil {
		t.Fatalf("embed NATS: %v", err)
	}
	t.Cleanup(en.Shutdown)
	nc, err := distributed.ConnectInProcess(en.Server())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	// Schema with an unsupported shuffle type (Array) forces writeChunk to fail.
	schema := []parquet.Column{
		{Name: "a", Type: parquet.TypeArray, Nullable: true, ElementType: &parquet.Column{Type: parquet.TypeInt64}},
	}
	b := batch.NewRecordBatch(schema, 1)

	var terminal distributed.GatherBatchMsg
	done := make(chan struct{})
	sub, err := nc.Subscribe("gather.err.test", func(m *nats.Msg) {
		var msg distributed.GatherBatchMsg
		_ = distributed.Unmarshal(m.Data, &msg)
		if msg.Terminal {
			terminal = msg
			close(done)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	sink := newGatherReplySink(nc, "gather.err.test", schema)
	if err := sink.Consume(context.Background(), b); err == nil {
		t.Fatal("expected Consume to fail on unsupported type")
	}
	// Second Consume should return the cached error, not re-enter writeChunk.
	if err := sink.Consume(context.Background(), b); err == nil {
		t.Fatal("expected second Consume to return cached error")
	}
	if err := sink.Finalize(context.Background()); err != nil {
		t.Fatalf("Finalize: %v", err)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	if !terminal.Terminal {
		t.Fatal("terminal flag")
	}
	if terminal.Err == "" {
		t.Fatal("expected non-empty Err on terminal message")
	}
}
