package worker

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/nats-io/nats.go"

	"github.com/derekmwright/wadjet/internal/dataplane"
	"github.com/derekmwright/wadjet/internal/distributed"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
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

// sendBatch routes the result over whichever transport the sink was
// configured for: when dpClient is set, gRPC is used exclusively (no
// NATS fallback — coord redispatches the task on TCP disconnect via
// task_id idempotency, see Phase D design); when nil, the legacy NATS
// reply-subject path is used.
//
// The either/or choice is per-cluster — `--data-plane=grpc` wires the
// dpClient at startup. We deliberately don't mix transports per call:
// a single query writing to both NATS and gRPC would force the receiver
// to subscribe to both forever.
func (s *gatherReplySink) sendBatch(msg distributed.GatherBatchMsg) error {
	if s.dpClient != nil {
		if err := s.dpClient.SendResultBatch(dataplane.ResultBatch{
			QueryID:  s.queryID,
			WorkerID: msg.WorkerID,
			Terminal: msg.Terminal,
			RowCount: msg.RowCount,
			Payload:  msg.Payload,
			Err:      msg.Err,
		}); err != nil {
			return fmt.Errorf("gather: dataplane send: %w", err)
		}
		return nil
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
	// so the flush is only meaningful on the NATS legacy path.
	if s.dpClient == nil {
		return s.nc.Flush()
	}
	return nil
}

// recordErr makes err the terminal message's Err, unless the sink already
// recorded one of its own.
//
// A gather task that FAILS still publishes its terminal marker — the
// coordinator's receiver blocks on it, and withholding it turned every
// failure into a query-timeout hang. But the marker is also the only thing
// the coordinator waits on for this stage: it never reads the task's result
// notification. So a marker published without the failure on it reported
// SUCCESS with zero rows, which is how a worker that correctly REFUSED to
// decode a type-drifted file (#503) still answered the client with an empty
// result set.
func (s *gatherReplySink) recordErr(err error) {
	if err == nil || s.err != nil {
		return
	}
	s.err = err
}

func (s *gatherReplySink) Close() error { return nil }
