package worker

import (
	"bytes"
	"context"
	"fmt"

	"github.com/nats-io/nats.go"

	"github.com/citc-tech/wadjet/internal/distributed"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// gatherReplySink is an exec.Sink that streams batches to a NATS reply
// subject instead of materializing to S3. Used by Gather stage tasks so
// the coordinator can stream results as they are produced.
//
// Each Consume call publishes one GatherBatchMsg carrying a self-contained
// WSHF-encoded single-chunk batch. Finalize publishes a terminal message.
// On Consume error, the sink remembers the error and surfaces it in the
// terminal message; Consume is a no-op once an error has been recorded.
type gatherReplySink struct {
	nc      *nats.Conn
	subject string
	schema  []parquet.Column
	err     error
}

func newGatherReplySink(nc *nats.Conn, subject string, schema []parquet.Column) *gatherReplySink {
	return &gatherReplySink{nc: nc, subject: subject, schema: schema}
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

	var buf bytes.Buffer
	sw := newShuffleWriter(&buf, s.schema)
	if err := sw.writeHeader(); err != nil {
		s.err = fmt.Errorf("gather: writing header: %w", err)
		return s.err
	}
	if err := sw.writeChunk(b.Columns, b.Sel, active); err != nil {
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
		RowCount: int32(active),
		Payload:  payload,
	}
	data, err := distributed.Marshal(msg)
	if err != nil {
		s.err = fmt.Errorf("gather: marshal: %w", err)
		return s.err
	}
	if err := s.nc.Publish(s.subject, data); err != nil {
		s.err = fmt.Errorf("gather: publish: %w", err)
		return s.err
	}
	return nil
}

func (s *gatherReplySink) Finalize(_ context.Context) error {
	msg := distributed.GatherBatchMsg{Terminal: true}
	if s.err != nil {
		msg.Err = s.err.Error()
	}
	data, err := distributed.Marshal(msg)
	if err != nil {
		return fmt.Errorf("gather: marshal terminal: %w", err)
	}
	if err := s.nc.Publish(s.subject, data); err != nil {
		return fmt.Errorf("gather: publish terminal: %w", err)
	}
	// Ensure the terminal message leaves this process before we return.
	return s.nc.Flush()
}

func (s *gatherReplySink) Close() error { return nil }
