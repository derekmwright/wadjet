package server

import (
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// fakeQueryStream records every response Send'd by the streaming path.
// Only Send is exercised; the embedded ServerStream is never touched.
type fakeQueryStream struct {
	grpc.ServerStream
	sent []*wadjetv1.QueryStreamResponse
}

func (f *fakeQueryStream) Send(resp *wadjetv1.QueryStreamResponse) error {
	f.sent = append(f.sent, resp)
	return nil
}

func makeIntBatch(tb testing.TB, vals ...int64) *batch.RecordBatch {
	tb.Helper()
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}
	rows := make([]map[string]any, len(vals))
	for i, v := range vals {
		rows[i] = map[string]any{"n": v}
	}
	return batch.FromRows(schema, rows)
}

// Regression test for sweep finding #20: QueryStream materialized the full
// result via Rows() before chunking. The streaming path must consume
// result.Batches incrementally — dropping each batch reference as it goes —
// and preserve the wire contract: columns only on the first response, stats
// and IsLast only on the last.
func TestStreamResultBatches_PerBatch(t *testing.T) {
	result := &coordinator.SQLResult{
		Columns: []string{"n"},
		Batches: []*batch.RecordBatch{
			makeIntBatch(t, 1, 2, 3),
			makeIntBatch(t, 4, 5),
			makeIntBatch(t), // trailing empty batch must not produce a stray chunk
		},
		TotalRows: 5,
	}
	stats := &wadjetv1.QueryStats{TotalRows: 5, Elapsed: durationpb.New(0)}

	fs := &fakeQueryStream{}
	cs := &chunkStreamer{stream: fs, columns: result.Columns, stats: stats}
	if err := streamResultBatches(cs, result); err != nil {
		t.Fatal(err)
	}

	if result.Batches != nil {
		t.Error("result.Batches not released after streaming")
	}
	if len(fs.sent) != 2 {
		t.Fatalf("got %d responses, want 2 (one per non-empty batch)", len(fs.sent))
	}

	var gotRows []*wadjetv1.Row
	for i, resp := range fs.sent {
		isFirst, isLast := i == 0, i == len(fs.sent)-1
		if (resp.Columns != nil) != isFirst {
			t.Errorf("response %d: columns presence = %v, want first-only", i, resp.Columns != nil)
		}
		if resp.IsLast != isLast {
			t.Errorf("response %d: IsLast = %v, want %v", i, resp.IsLast, isLast)
		}
		if (resp.Stats != nil) != isLast {
			t.Errorf("response %d: stats presence = %v, want last-only", i, resp.Stats != nil)
		}
		gotRows = append(gotRows, resp.Rows...)
	}
	if len(gotRows) != 5 {
		t.Fatalf("got %d total rows, want 5", len(gotRows))
	}
}

func TestStreamResultBatches_Empty(t *testing.T) {
	result := &coordinator.SQLResult{Columns: []string{"n"}}
	stats := &wadjetv1.QueryStats{TotalRows: 0}

	fs := &fakeQueryStream{}
	cs := &chunkStreamer{stream: fs, columns: result.Columns, stats: stats}
	if err := streamResultBatches(cs, result); err != nil {
		t.Fatal(err)
	}

	if len(fs.sent) != 1 {
		t.Fatalf("got %d responses, want 1", len(fs.sent))
	}
	resp := fs.sent[0]
	if len(resp.Rows) != 0 || !resp.IsLast || resp.Stats == nil || resp.Columns == nil {
		t.Fatalf("empty result response = %+v; want no rows, IsLast, stats, columns", resp)
	}
}

// A batch larger than streamBatchSize must be split into multiple chunks.
func TestChunkStreamer_SplitsLargeBatch(t *testing.T) {
	vals := make([]int64, streamBatchSize+10)
	for i := range vals {
		vals[i] = int64(i)
	}
	result := &coordinator.SQLResult{
		Columns:   []string{"n"},
		Batches:   []*batch.RecordBatch{makeIntBatch(t, vals...)},
		TotalRows: int64(len(vals)),
	}

	fs := &fakeQueryStream{}
	cs := &chunkStreamer{stream: fs, columns: result.Columns, stats: &wadjetv1.QueryStats{}}
	if err := streamResultBatches(cs, result); err != nil {
		t.Fatal(err)
	}

	if len(fs.sent) != 2 {
		t.Fatalf("got %d responses, want 2", len(fs.sent))
	}
	if n := len(fs.sent[0].Rows); n != streamBatchSize {
		t.Errorf("first chunk = %d rows, want %d", n, streamBatchSize)
	}
	if n := len(fs.sent[1].Rows); n != 10 {
		t.Errorf("last chunk = %d rows, want 10", n)
	}
	if !fs.sent[1].IsLast || fs.sent[0].IsLast {
		t.Error("IsLast must be set on the final chunk only")
	}
}
