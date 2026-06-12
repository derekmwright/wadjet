package pgwire

import (
	"bytes"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// recordConn is a net.Conn that captures everything written to it.
type recordConn struct {
	net.Conn
	buf bytes.Buffer
}

func (r *recordConn) Write(p []byte) (int, error)      { return r.buf.Write(p) }
func (r *recordConn) SetWriteDeadline(time.Time) error { return nil }

// countMsgs walks the captured wire bytes and counts messages of the given type.
func countMsgs(data []byte, typ byte) int {
	n := 0
	for len(data) >= 5 {
		msgLen := int(binary.BigEndian.Uint32(data[1:5]))
		if data[0] == typ {
			n++
		}
		data = data[1+msgLen:]
	}
	return n
}

// Regression test for sweep finding #21: the coord-routed pgwire path boxed
// the full row set (batchesToRows) before writing any DataRow — ~10x the
// columnar footprint held on top of the already-held Batches. sendResultRows
// must emit DataRows batch-by-batch, dropping each batch reference as it
// goes, and report the exact row count for CommandComplete.
func TestSendResultRows_PerBatch(t *testing.T) {
	schema := []parquet.Column{{Name: "n", Type: parquet.TypeInt64}}
	mk := func(vals ...int64) *batch.RecordBatch {
		rows := make([]map[string]any, len(vals))
		for i, v := range vals {
			rows[i] = map[string]any{"n": v}
		}
		return batch.FromRows(schema, rows)
	}

	rc := &recordConn{}
	c := &pgConn{conn: rc}

	batches := []*batch.RecordBatch{mk(1, 2, 3), mk(4, 5)}
	sent := c.sendResultRows([]string{"n"}, batches, nil, nil)

	if sent != 5 {
		t.Errorf("sent = %d, want 5", sent)
	}
	if got := countMsgs(rc.buf.Bytes(), 'D'); got != 5 {
		t.Errorf("DataRow messages = %d, want 5", got)
	}
	for i, b := range batches {
		if b != nil {
			t.Errorf("batch %d reference not dropped after send", i)
		}
	}
}

func TestSendResultRows_LegacyRows(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}

	rows := []map[string]any{{"n": int64(1)}, {"n": int64(2)}}
	sent := c.sendResultRows([]string{"n"}, nil, rows, nil)

	if sent != 2 {
		t.Errorf("sent = %d, want 2", sent)
	}
	if got := countMsgs(rc.buf.Bytes(), 'D'); got != 2 {
		t.Errorf("DataRow messages = %d, want 2", got)
	}
}
