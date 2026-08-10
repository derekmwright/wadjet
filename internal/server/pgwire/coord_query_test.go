package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/citc-tech/wadjet/internal/coordinator"
	"github.com/citc-tech/wadjet/internal/engine/batch"
	"github.com/citc-tech/wadjet/internal/storage/objstore"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
	"github.com/citc-tech/wadjet/wadjet"
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
	sent, err := c.sendResultRows([]string{"n"}, coordinator.NewSliceStream(batches), nil, nil)
	if err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}

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

// Regression test (2026-08-10 disk-full sit): a Describe-time execution
// failure was swallowed (NoData) and Execute silently RE-EXECUTED the whole
// query — doubling the cost of every deterministic failure, and against a
// broken environment (ENOSPC) converting a 4s fast-fail into a hang to the
// query timeout. Execute must replay the cached Describe error instead.
func TestExecuteReplaysDescribeError(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}

	rc := &recordConn{}
	c := &pgConn{conn: rc, db: db, stmts: map[string]string{}}
	c.preparedSQL = "SELECT no_such_function(1) FROM (SELECT" // guaranteed parse failure

	c.describeSQL(c.preparedSQL)
	if c.describeErr == nil {
		t.Fatal("describeSQL on a failing query did not cache the failure")
	}
	if got := countMsgs(rc.buf.Bytes(), 'n'); got != 1 {
		t.Fatalf("NoData messages after failed Describe = %d, want 1", got)
	}

	// Overwrite the cached error with a sentinel: if Execute REPLAYS the
	// cache, the client sees the sentinel; if it re-executes the query, it
	// would see a fresh table-not-found message instead.
	c.describeErr = errors.New("sentinel: describe-time failure replayed")
	rc.buf.Reset()
	c.handleExecute(nil)

	wire := rc.buf.Bytes()
	if got := countMsgs(wire, 'E'); got != 1 {
		t.Fatalf("ErrorResponse messages = %d, want 1", got)
	}
	if !bytes.Contains(wire, []byte("sentinel: describe-time failure replayed")) {
		t.Fatalf("Execute did not replay the cached Describe error; wire = %q", wire)
	}
	if c.describeErr != nil {
		t.Fatal("describeErr not cleared after replay")
	}
}

func TestSendResultRows_LegacyRows(t *testing.T) {
	rc := &recordConn{}
	c := &pgConn{conn: rc}

	rows := []map[string]any{{"n": int64(1)}, {"n": int64(2)}}
	sent, err := c.sendResultRows([]string{"n"}, nil, rows, nil)
	if err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}

	if sent != 2 {
		t.Errorf("sent = %d, want 2", sent)
	}
	if got := countMsgs(rc.buf.Bytes(), 'D'); got != 2 {
		t.Errorf("DataRow messages = %d, want 2", got)
	}
}
