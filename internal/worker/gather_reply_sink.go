package worker

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/dataplane"
	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// gatherReplySink is an exec.Sink that streams batches to a NATS reply
// subject instead of materializing to S3. Used by Gather stage tasks so
// the coordinator can stream results as they are produced.
//
// Each Consume call publishes one or more GatherBatchMsg, each carrying a
// self-contained WSHF-encoded single-chunk batch capped at
// gatherMaxRowsPerMessage so the message stays under NATS's payload limit
// (8 MB by default — see internal/distributed/nats_setup.go::DefaultNATSConfig).
// Finalize publishes a terminal message. On any error the sink remembers
// it and surfaces it in the terminal message; subsequent Consume calls are
// no-ops.
type gatherReplySink struct {
	nc        *nats.Conn
	dpClient  *dataplane.Client // optional gRPC delivery; nil = NATS only
	subject   string
	queryID   string // derived from subject; used for gRPC routing
	workerID  string // stamped on each GatherBatchMsg for coord-side liveness
	schema    []parquet.Column
	err       error
	finalized bool
}

// gatherMaxRowsPerMessage caps the number of rows packed into a single
// NATS message. With ~120-byte rows (5 columns of int64 + a short string)
// 2048 rows ≈ 250 KB, comfortably under NATS's 8 MB payload limit. Wide
// 1:N hash-join outputs (a 2048-row probe batch fanning out 10× from
// 10 build matches per probe key) used to arrive here as a single 12 MB
// batch and the publish failed silently — coord stayed blocked on the
// reply subject until query timeout.
const gatherMaxRowsPerMessage = batch.DefaultBatchSize

func newGatherReplySink(nc *nats.Conn, subject, workerID string, schema []parquet.Column) *gatherReplySink {
	return &gatherReplySink{
		nc:       nc,
		subject:  subject,
		queryID:  strings.TrimPrefix(subject, "wadjet.gather."),
		workerID: workerID,
		schema:   schema,
	}
}

// withDataPlane attaches an optional gRPC client. When the client is
// Connected at send time, batches go via SendResultBatch; otherwise
// the sink falls back to NATS publish. Both transports route to the
// same coord-side gatherReceiver.handleParsed so cross-transport
// interleaving is safe.
func (s *gatherReplySink) withDataPlane(c *dataplane.Client) *gatherReplySink {
	s.dpClient = c
	return s
}

func (s *gatherReplySink) Init(_ context.Context) error { return nil }

func (s *gatherReplySink) Consume(_ context.Context, b *batch.RecordBatch) error {
	if s.err != nil {
		return s.err
	}
	active := b.ActiveLen()
	if active == 0 {
		return nil
	}
	if s.schema == nil {
		s.schema = b.Schema
	}

	// Resolve the source selection vector. When b.Sel is nil we synthesize
	// 0..active-1 so the chunk-slicing loop below can use a single uniform
	// path for both selected and dense batches.
	srcSel := b.Sel
	var fullSel []uint32
	if srcSel == nil {
		fullSel = make([]uint32, active)
		for i := range fullSel {
			fullSel[i] = uint32(i)
		}
		srcSel = fullSel
	}

	for off := 0; off < active; off += gatherMaxRowsPerMessage {
		end := off + gatherMaxRowsPerMessage
		if end > active {
			end = active
		}
		window := srcSel[off:end]
		windowLen := end - off

		var buf bytes.Buffer
		sw := newShuffleWriter(&buf, s.schema)
		if err := sw.writeHeader(); err != nil {
			s.err = fmt.Errorf("gather: writing header: %w", err)
			return s.err
		}
		if err := sw.writeChunk(b.Columns, window, windowLen); err != nil {
			s.err = fmt.Errorf("gather: writing chunk: %w", err)
			return s.err
		}
		// Patch chunk count (header wrote 0 as placeholder).
		payload := buf.Bytes()
		// Magic (4) + NumChunks (4 LE) + ...
		payload[4] = 1
		payload[5] = 0
		payload[6] = 0
		payload[7] = 0

		msg := distributed.GatherBatchMsg{
			RowCount: int32(windowLen),
			Payload:  payload,
			WorkerID: s.workerID,
		}
		if err := s.sendBatch(msg); err != nil {
			s.err = err
			return s.err
		}
	}
	return nil
}

// sendBatch prefers the gRPC data-plane stream when the client is set
// and currently connected; otherwise publishes to the NATS reply
// subject. Both transports flow into the same gatherReceiver on coord,
// so per-message switching across a worker's lifetime is safe.
func (s *gatherReplySink) sendBatch(msg distributed.GatherBatchMsg) error {
	if s.dpClient != nil && s.dpClient.Connected() {
		err := s.dpClient.SendResultBatch(dataplane.ResultBatch{
			QueryID:  s.queryID,
			WorkerID: msg.WorkerID,
			Terminal: msg.Terminal,
			RowCount: msg.RowCount,
			Payload:  msg.Payload,
			Err:      msg.Err,
		})
		if err == nil {
			return nil
		}
		// Stream disconnected mid-publish (rare). Fall through to NATS
		// so the batch still reaches coord rather than failing the task.
	}
	data, err := distributed.Marshal(msg)
	if err != nil {
		return fmt.Errorf("gather: marshal: %w", err)
	}
	if err := s.nc.Publish(s.subject, data); err != nil {
		return fmt.Errorf("gather: publish: %w", err)
	}
	return nil
}

// Finalize publishes a terminal message carrying any recorded error.
// Idempotent so callers can defer it as a safety net AND still call it
// explicitly to surface any publish error from the terminal message.
// Without idempotency a deferred Finalize-on-error would double-publish
// the terminal marker and risk a duplicate-result race on the coord side.
func (s *gatherReplySink) Finalize(_ context.Context) error {
	if s.finalized {
		return nil
	}
	s.finalized = true
	msg := distributed.GatherBatchMsg{Terminal: true, WorkerID: s.workerID}
	if s.err != nil {
		msg.Err = s.err.Error()
	}
	if err := s.sendBatch(msg); err != nil {
		return err
	}
	// Ensure the terminal message leaves this process before we return.
	// gRPC send is synchronous (returns when the server has accepted),
	// so this only matters for the NATS fallback path.
	if s.dpClient == nil || !s.dpClient.Connected() {
		return s.nc.Flush()
	}
	return nil
}

func (s *gatherReplySink) Close() error { return nil }
