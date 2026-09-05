package server

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/internal/worker"
)

// The gRPC streaming RPC's COORDINATOR arm, over MORE THAN ONE CHUNK.
//
// `QueryStream` has two arms and they box their rows in different places: the
// embedded one hands `chunkStreamer.pushRows` the whole `QueryResult`, and the
// distributed one goes through `streamResultBatches`, which boxes each
// `RecordBatch` itself — `b.ToRows()` for the map form and, when
// `batchNeedsPositionalRows(b)` says the schema publishes a name twice,
// `b.ToRowValues()` for the positional one. The round-3 door gate reaches only
// the embedded arm (`GRPCConfig{DB: …}`), so reverting the coordinator arm's
// `b.ToRowValues()` failed nothing (round-3 review P1). This cell is that arm.
//
// It drives 1200 rows on purpose: `streamBatchSize` is 1000, so the result
// crosses a chunk boundary and the positional slice `values[i:vend]` has to
// stay index-aligned with `rows[i:end]` across it. The FIRST row of the second
// chunk is asserted by name for that reason.
//
// The unique-name control is the other half of the claim: `batchNeedsPositional-
// Rows` must answer FALSE there, so no `values` is sent at all and an existing
// client's bytes are unchanged. Without it, "send the positional form whenever
// we can" would pass the first half and quietly double every response.
func TestCoordinatorStreamCarriesDuplicateNamesPositionally(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster and a worker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	logger := slog.Default()

	natsCfg := distributed.DefaultNATSConfig()
	natsCfg.Port = -1
	natsCfg.StoreDir = t.TempDir()
	embedded, err := distributed.NewEmbeddedNATS(natsCfg, logger)
	if err != nil {
		t.Fatalf("embedded nats: %v", err)
	}
	t.Cleanup(embedded.Shutdown)
	nc, err := distributed.ConnectInProcess(embedded.Server())
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	js, err := distributed.NewJetStream(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err := distributed.SetupStreams(ctx, js); err != nil {
		t.Fatal(err)
	}
	store := objstore.NewMemStore()
	if err := store.MakeBucket(ctx, "test"); err != nil {
		t.Fatal(err)
	}
	kv, err := catalog.NewNATSKV(js)
	if err != nil {
		t.Fatal(err)
	}
	cat := catalog.New(kv, store, "test")
	if err := cat.Init(ctx); err != nil {
		t.Fatal(err)
	}

	// 1200 rows — over streamBatchSize, so the stream really chunks.
	const rows = 1200
	sch := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString},
	}}
	if err := cat.CreateTable(ctx, "dupstream", sch, nil); err != nil {
		t.Fatal(err)
	}
	ing := ingest.New(cat, "dupstream", sch, nil,
		ingest.Config{MaxBufferRows: rows + 1, RowGroupSize: 512})
	data := make([]map[string]any, rows)
	for i := range data {
		data[i] = map[string]any{"id": int64(i), "s": fmt.Sprintf("s%04d", i)}
	}
	if err := ing.Ingest(ctx, data); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	const workerID = "f4-dupstream-worker"
	w := worker.New(worker.Config{
		WorkerID: workerID, NATSUrl: embedded.ClientURL(),
		MaxConcurrent: 4, CacheBytes: 64 << 20, SpillDir: t.TempDir(),
	}, store, nc, js, logger)
	wctx, wcancel := context.WithCancel(context.Background())
	t.Cleanup(wcancel)
	if err := w.Start(wctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	// LocalFastPathBytes 0 so the coordinator plans the DAG rather than
	// answering on its own local pipeline: `streamResultBatches` is the arm
	// under test and a routed query would not reach it.
	coord := coordinator.New(coordinator.Config{
		NATSUrl: embedded.ClientURL(), ResultBucket: "test", LocalFastPathBytes: 0,
	}, cat, nc, js, logger)

	deadline := time.Now().Add(30 * time.Second)
	for coord.Workers().Count() < 1 {
		hb, err := distributed.Marshal(distributed.WorkerHeartbeat{
			WorkerID: workerID, MaxConcurrent: 4, Timestamp: time.Now(),
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := nc.Publish(distributed.SubjectHeartbeat, hb); err != nil {
			t.Fatal(err)
		}
		nc.Flush()
		if time.Now().After(deadline) {
			t.Fatalf("worker did not register: %d", coord.Workers().Count())
		}
		time.Sleep(50 * time.Millisecond)
	}

	g := NewGRPCServer(GRPCConfig{Coord: coord}, logger)

	stream := func(sql string) ([]string, []*wadjetv1.Row) {
		t.Helper()
		fs := &dupNameStream{}
		if err := g.QueryStream(&wadjetv1.QueryRequest{Sql: sql}, fs); err != nil {
			t.Fatalf("QueryStream(%s): %v", sql, err)
		}
		var cols []string
		var out []*wadjetv1.Row
		for _, r := range fs.sent {
			if len(r.Columns) > 0 {
				cols = r.Columns
			}
			out = append(out, r.Rows...)
		}
		return cols, out
	}

	t.Run("three-unnamed-columns-across-chunks", func(t *testing.T) {
		cols, got := stream("SELECT id + 1, id + 2, id + 3 FROM dupstream ORDER BY 1")
		if len(cols) != 3 {
			t.Fatalf("columns = %v, want three `?column?`", cols)
		}
		for i, c := range cols {
			if c != "?column?" {
				t.Errorf("column %d = %q, want %q", i, c, "?column?")
			}
		}
		if len(got) != rows {
			t.Fatalf("streamed %d rows, want %d", len(got), rows)
		}
		bad := 0
		for i, r := range got {
			if len(r.Values) != 3 {
				bad++
				continue
			}
			want := []float64{float64(i + 1), float64(i + 2), float64(i + 3)}
			for j := range want {
				if r.Values[j].GetNumberValue() != want[j] {
					bad++
					break
				}
			}
		}
		if bad != 0 {
			t.Fatalf("%d of %d rows carry no positional values or the wrong ones — a "+
				"protobuf map holds ONE key for three columns of one name, so those "+
				"values exist nowhere else", bad, len(got))
		}
		// The chunk boundary explicitly: streamBatchSize is 1000, and the
		// positional slice must be cut with the same index as the map rows.
		for _, at := range []int{0, streamBatchSize - 1, streamBatchSize, rows - 1} {
			r := got[at]
			if len(r.Values) != 3 || r.Values[0].GetNumberValue() != float64(at+1) {
				t.Fatalf("row %d = %v, want its own [%d %d %d]: the positional slice is "+
					"misaligned with the map rows across a chunk boundary",
					at, r.Values, at+1, at+2, at+3)
			}
		}
	})

	// The CONTROL: unique names send no positional form at all, so a client
	// that never asked for one reads exactly the bytes it read before.
	t.Run("ctl-unique-names-send-no-values", func(t *testing.T) {
		cols, got := stream("SELECT id, s FROM dupstream ORDER BY id")
		if len(cols) != 2 || cols[0] != "id" || cols[1] != "s" {
			t.Fatalf("columns = %v, want [id s]", cols)
		}
		if len(got) != rows {
			t.Fatalf("streamed %d rows, want %d", len(got), rows)
		}
		withValues := 0
		for _, r := range got {
			if len(r.Values) > 0 {
				withValues++
			}
		}
		if withValues != 0 {
			t.Fatalf("%d of %d rows carry a positional form for a result whose names are "+
				"UNIQUE: the second boxed copy is paid for nothing and this path's peak "+
				"residency is deliberately one batch", withValues, len(got))
		}
	})
}
