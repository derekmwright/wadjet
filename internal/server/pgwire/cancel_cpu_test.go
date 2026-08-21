package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// setupSelfJoinDB builds a table sized so that a keyless self-join is CPU-bound
// for seconds: n rows make n²/2 pairs, and the string concat per pair keeps the
// work in the executor, not the storage layer. The row count — not wall time —
// bounds the query, so the test is not flaky under parallel load: a slow
// machine only makes the uncancelled run longer, never the cancelled one.
func setupSelfJoinDB(t *testing.T, n int) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "o_orderkey", Type: parquet.TypeInt32},
			{Name: "o_comment", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "orders", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("orders", schema, nil, ingest.Config{MaxBufferRows: n, RowGroupSize: n})
	rows := make([]map[string]any, 0, n)
	for i := 0; i < n; i++ {
		rows = append(rows, map[string]any{
			"o_orderkey": int32(i),
			"o_comment":  fmt.Sprintf("requests along the furiously ironic package %08d", i),
		})
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// waitStatementRunning polls until the session identified by pid has a
// registered statement, and returns without cancelling anything.
func waitStatementRunning(t *testing.T, srv *Server, pid int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		srv.sessionsMu.Lock()
		c := srv.sessions[pid]
		srv.sessionsMu.Unlock()
		if c != nil {
			c.stmtMu.Lock()
			running := c.stmt != nil
			c.stmtMu.Unlock()
			if running {
				return
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the statement to register")
}

// TestCancelRequestStopsCPUBoundQuery is issue #368. The existing cancel tests
// gate the query INSIDE the storage layer, where a ctx-aware select is already
// waiting — they prove delivery, not observation. The wire probe
// (benchmarks/tpch/postgres_wire_test.go, WireProtocol/Cancellation) showed the
// gap: a keyless self-join that is pure executor CPU took the cancel without
// error and ran 11 more seconds to a normal completion, because nothing in the
// join's fan-out loop ever looked at the statement context.
//
// The test distinguishes the two failure hypotheses from the issue thread:
// cancelSession returning false would mean the (pid, secret) lookup missed the
// statement (delivery); cancelSession returning true followed by a long unwind
// means the running query never polls its cancelled context (observation).
func TestCancelRequestStopsCPUBoundQuery(t *testing.T) {
	// 15000 rows ~= the SF0.01 probe: ~112M pairs, tens of seconds of executor
	// work if the cancel is not observed, sub-second when it is.
	db := setupSelfJoinDB(t, 15000)
	srv := startTestServer(t, db)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	const slow = `SELECT COUNT(*) AS c FROM orders a, orders b
		WHERE a.o_orderkey < b.o_orderkey AND LENGTH(a.o_comment || b.o_comment) > 0`

	out := a.asyncSimpleQuery(slow)
	waitStatementRunning(t, srv, a.pid)
	// Land the cancel mid-execution, past parse/plan. The query runs for tens
	// of seconds; 500ms is safely inside it and safely past planning.
	time.Sleep(500 * time.Millisecond)

	// The real second-connection CancelRequest, exactly as psql ^C sends it.
	sendCancelRequest(t, srv.Addr(), a.pid, a.secret)
	// The registry must still hold the (now cancelled) statement: a false here
	// would mean the lookup missed — hypothesis (1) of #368 — rather than the
	// executor ignoring its context. Cancelling twice is what a nervous client
	// does anyway, and must be harmless.
	if !srv.cancelSession(a.pid, a.secret) {
		t.Fatal("cancelSession found no statement: the CancelRequest lookup missed a running query")
	}
	cancelledAt := time.Now()

	const unwindBound = 2 * time.Second
	select {
	case res := <-out:
		unwind := time.Since(cancelledAt)
		if res.err != nil {
			t.Fatalf("reading query response: %v", res.err)
		}
		if res.code != sqlstateQueryCanceled {
			t.Fatalf("SQLSTATE = %q (%q) after %s, want %s", res.code, res.msg, unwind, sqlstateQueryCanceled)
		}
		if res.msg != errCanceledByRequest.Error() {
			t.Errorf("message = %q, want %q", res.msg, errCanceledByRequest.Error())
		}
		if unwind > unwindBound {
			t.Errorf("statement took %s to unwind after the cancel; a query that polls its context between batches unwinds in milliseconds", unwind)
		}
	case <-time.After(60 * time.Second):
		t.Fatal("query never returned after cancellation")
	}

	// The session survives its cancelled statement.
	_, rows, tag := a.simpleQuery("SELECT COUNT(*) AS c FROM orders")
	if len(rows) != 1 || !strings.HasPrefix(tag, "SELECT") {
		t.Errorf("connection unusable after cancel: rows=%d tag=%q", len(rows), tag)
	}
	a.terminate()
}
