package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
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
	sent, err := c.sendResultRows(context.Background(), []string{"n"}, coordinator.NewSliceStream(batches), nil, nil, nil, nil)
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

	c.describeSQL(c.preparedSQL, nil)
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
	sent, err := c.sendResultRows(context.Background(), []string{"n"}, nil, rows, nil, nil, nil)
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

// TestSendResultRowsRenamedRowColumnUsesCoordPositionalFallback pins FIX 4
// (adversarial review of #464/#471): the #471 defect resurfacing under a
// RENAMED output column.
//
// queryViaCoord resolves nestedSchema from res.OutputSchema(), which names
// each column by its schema's own Name — not necessarily the name the
// column surfaces under in the result's own Columns list, which an alias or
// the gather's renamer can change. Before this fix, nestedColumnFor only
// ever looked a column up BY NAME, so a renamed ROW column's schema entry
// was simply never found: formatPgComposite fell back to its schema-less
// path (alphabetically sorted keys) instead of the declared field order,
// rendering "(Reston,VA,20190)" where PostgreSQL's declared order is
// "(Reston,20190,VA)".
//
// This drives the exact chain queryViaCoord feeds sendResultRows — a coord-
// style nestedSchema from nestedSchemaByName, through the SliceStream
// harness TestSendResultRows_PerBatch above already established for the
// coord path — without needing a live NATS-backed *coordinator.Coordinator
// (review N3): the schema-resolution and positional-fallback logic under
// test lives entirely in nestedSchemaByName/nestedColumnFor/sendResultRows,
// none of which reads through c.coord itself.
func TestSendResultRowsRenamedRowColumnUsesCoordPositionalFallback(t *testing.T) {
	// The query's declared output schema, exactly the shape
	// queryViaCoord/nestedSchemaByName would see from res.OutputSchema():
	// column 1 is a ROW named "addr" with a deliberately non-alphabetical
	// field order (sorted would be city, state, zip).
	outputSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "addr", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "city", Type: parquet.TypeString, Nullable: true},
			{Name: "zip", Type: parquet.TypeInt32, Nullable: true},
			{Name: "state", Type: parquet.TypeString, Nullable: true},
		}},
	}
	nestedSchema := nestedSchemaByName(outputSchema)

	// The row's own column list, as sendResultRows/sendDataRow key by: same
	// POSITION as outputSchema, but a DIFFERENT name for column 1 — "home",
	// not "addr" — the alias/renamer case nestedSchemaByName's byName map
	// alone cannot resolve.
	outCols := []string{"id", "home"}
	rowSchema := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "home", Type: parquet.TypeRow, Nullable: true, Fields: outputSchema[1].Fields},
	}
	b := batch.FromRows(rowSchema, []map[string]any{
		{"id": int32(1), "home": map[string]any{"city": "Reston", "zip": int32(20190), "state": "VA"}},
	})

	rc := &recordConn{}
	c := &pgConn{conn: rc}
	sent, err := c.sendResultRows(context.Background(), outCols,
		coordinator.NewSliceStream([]*batch.RecordBatch{b}), nil, nil, nil, nestedSchema)
	if err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	if sent != 1 {
		t.Fatalf("sent = %d, want 1", sent)
	}

	wire := rc.buf.Bytes()
	const wantDeclaredOrder = "(Reston,20190,VA)" // city, zip, state — the declared order
	const gotSortedOrder = "(Reston,VA,20190)"    // city, state, zip — alphabetically sorted keys
	if bytes.Contains(wire, []byte(gotSortedOrder)) {
		t.Errorf("wire contains the sorted-key fallback %q — nestedColumnFor did not use the "+
			"positional match for the renamed \"home\" column (#471 resurfacing)", gotSortedOrder)
	}
	if !bytes.Contains(wire, []byte(wantDeclaredOrder)) {
		t.Errorf("wire = %q, want it to contain the declared field order %q", wire, wantDeclaredOrder)
	}
}

// TestNestedColumnForLegacySchemaHasNoPositionalFallback pins the other half
// of FIX 4: nestedColumnSchemas' catalog-lookup schema (the legacy,
// non-coord query path) must NOT get a positional fallback. Its entries come
// from whichever catalog table columns happen to share a name with
// something in the SQL text — no relationship to output column position —
// so applying one there would attach a random table column's structure to
// an unrelated output column instead of correctly reporting "unresolved".
func TestNestedColumnForLegacySchemaHasNoPositionalFallback(t *testing.T) {
	legacy := &nestedFieldSchema{
		byName: map[string]parquet.Column{
			"addr": {Name: "addr", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "city", Type: parquet.TypeString},
			}},
		},
		// ordered intentionally nil: nestedColumnSchemas never sets it.
	}
	if col := nestedColumnFor(legacy, "addr", 1); col == nil {
		t.Error(`nestedColumnFor(legacy, "addr", 1) = nil, want the by-name match regardless of position`)
	}
	if col := nestedColumnFor(legacy, "home", 0); col != nil {
		t.Errorf(`nestedColumnFor(legacy, "home", 0) = %+v, want nil — no positional fallback for a `+
			`catalog-lookup schema even though position 0 exists`, col)
	}
}
