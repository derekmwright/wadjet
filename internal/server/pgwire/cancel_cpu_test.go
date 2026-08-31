package pgwire

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
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
//
// The deadline is generous on purpose: this is a HARNESS precondition, not the
// property under test, so a machine too loaded to schedule the backend
// goroutine must report itself as a harness timeout rather than as a
// cancellation defect (#756).
func waitStatementRunning(t *testing.T, srv *Server, pid int32) {
	t.Helper()
	const registerBound = 30 * time.Second
	deadline := time.Now().Add(registerBound)
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
	t.Fatalf("harness: statement never registered on session %d within %s", pid, registerBound)
}

// fanOutFrame is the frame a goroutine carries while it is driving batches
// through the operator chain — the join's fan-out resume loop included. It is
// the site #368 was about (exec.ChainDriver.push), so a stack carrying it is
// the statement doing the CPU-bound work this test cancels.
const fanOutFrame = "(*ChainDriver).push"

// waitExecutorInFanOut polls the process's goroutine stacks until one is in
// fanOutFrame.
//
// Why this and not a sleep, and not the cheaper "is the statement registered"
// or "has the scan opened the file": everything BEFORE the operator chain has
// its own, coarser cancellation points — scanner.Next tests ctx once per file
// and this fixture has exactly one — so a cancel that lands ahead of the
// fan-out is observed by those whether or not the fan-out ever looks at its
// context. That is #368 exactly. Measured on this tree: with the per-batch ctx
// checks in exec disabled, landing the cancel at registration passed 10/10 and
// landing it here fails. The poll is what keeps the gate able to fail.
//
// A rename of push would make this time out as a HARNESS failure naming the
// frame, which is the loud outcome; it cannot turn into a quiet pass.
func waitExecutorInFanOut(t *testing.T) {
	t.Helper()
	const fanOutBound = 60 * time.Second
	deadline := time.Now().Add(fanOutBound)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		for n == len(buf) { // truncated: the frame may lie past the end
			buf = make([]byte, 2*len(buf))
			n = runtime.Stack(buf, true)
		}
		if bytes.Contains(buf[:n], []byte(fanOutFrame)) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("harness: no goroutine reached %s within %s — the statement never got as far as the operator chain",
		fanOutFrame, fanOutBound)
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
	// Two preconditions, each polled on the thing itself rather than waited out
	// on a clock (#756 — the fixed 500 ms sleep this replaces):
	//
	//   1. the statement is REGISTERED, so the CancelRequest's (pid, secret)
	//      lookup has something to find;
	//   2. the statement is IN THE OPERATOR CHAIN, so the cancel lands on the
	//      quadratic fan-out rather than ahead of it, where coarser checks
	//      would observe it for free (see waitExecutorInFanOut).
	//
	// Uncancelled, this statement runs ~70s on this fixture, so both polls
	// resolve while there is nearly all of it left to stop.
	waitStatementRunning(t, srv, a.pid)
	waitExecutorInFanOut(t)

	// The real second-connection CancelRequest, exactly as psql ^C sends it.
	sendCancelRequest(t, srv.Addr(), a.pid, a.secret)
	cancelledAt := time.Now()

	// Cancelling twice is what a nervous client does anyway, and must be
	// harmless. The lookup is also the observation that distinguishes the two
	// #368 hypotheses — a miss means the (pid, secret) lookup did not find a
	// running statement (delivery) rather than the executor ignoring its
	// context (observation) — but it CANNOT be asserted on its own here:
	// sendCancelRequest waits for the server to close the cancel socket, a
	// full round trip, and a statement that observes its cancelled context
	// unwinds in milliseconds and unregisters itself on the way out
	// (beginStatement's CancelFunc). A healthy server therefore reports "no
	// statement" whenever that unwind wins the race — reproduced 1-in-20 with
	// `go test -count=20` while ./internal/coordinator/ ran beside it, which
	// is #756. A miss is the delivery hypothesis only when the statement ALSO
	// never reports the cancellation, so the verdict is deferred to the
	// outcome below.
	lookupHit := srv.cancelSession(a.pid, a.secret)
	const lookupMiss = "cancelSession found no statement: the CancelRequest lookup missed a running query"

	const unwindBound = 2 * time.Second
	select {
	case res := <-out:
		unwind := time.Since(cancelledAt)
		if res.err != nil {
			t.Fatalf("reading query response: %v", res.err)
		}
		if res.code != sqlstateQueryCanceled {
			if !lookupHit {
				t.Fatalf("%s — and the statement answered %q (%q) instead of %s",
					lookupMiss, res.code, res.msg, sqlstateQueryCanceled)
			}
			t.Fatalf("SQLSTATE = %q (%q) after %s, want %s", res.code, res.msg, unwind, sqlstateQueryCanceled)
		}
		if res.msg != errCanceledByRequest.Error() {
			t.Errorf("message = %q, want %q", res.msg, errCanceledByRequest.Error())
		}
		if unwind > unwindBound {
			t.Errorf("statement took %s to unwind after the cancel; a query that polls its context between batches unwinds in milliseconds", unwind)
		}
	case <-time.After(60 * time.Second):
		if !lookupHit {
			t.Fatalf("%s — and the statement never answered at all", lookupMiss)
		}
		t.Fatal("query never returned after cancellation")
	}

	// The session survives its cancelled statement.
	_, rows, tag := a.simpleQuery("SELECT COUNT(*) AS c FROM orders")
	if len(rows) != 1 || !strings.HasPrefix(tag, "SELECT") {
		t.Errorf("connection unusable after cancel: rows=%d tag=%q", len(rows), tag)
	}
	a.terminate()
}
