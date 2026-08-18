package pgwire

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// gateStore holds every read of a table data file until it is released, so a
// test can catch a query mid-scan and act on it. A held read returns as soon
// as its context is cancelled, which is what makes it a faithful stand-in for
// the long scan a client actually hits the stop button over: the query is
// blocked inside the storage layer on the statement's context.
type gateStore struct {
	objstore.Store
	armed   atomic.Bool
	held    atomic.Int32
	release chan struct{}
}

func newGateStore() *gateStore {
	return &gateStore{Store: objstore.NewMemStore(), release: make(chan struct{})}
}

func (g *gateStore) Get(ctx context.Context, bucket, key string) (io.ReadCloser, objstore.ObjectInfo, error) {
	if g.armed.Load() && strings.HasSuffix(key, ".parquet") {
		g.held.Add(1)
		defer g.held.Add(-1)
		select {
		case <-ctx.Done():
			return nil, objstore.ObjectInfo{}, ctx.Err()
		case <-g.release:
		}
	}
	return g.Store.Get(ctx, bucket, key)
}

// waitHeld blocks until n reads are parked in the gate.
func (g *gateStore) waitHeld(t *testing.T, n int32) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if g.held.Load() >= n {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d blocked reads (have %d)", n, g.held.Load())
}

func (g *gateStore) releaseAll() {
	select {
	case <-g.release:
	default:
		close(g.release)
	}
}

// setupGatedDB builds the standard test DB on a gated store. The gate is armed
// after ingest, so only queries block.
func setupGatedDB(t *testing.T) (*wadjet.DB, *gateStore) {
	t.Helper()
	ctx := context.Background()
	store := newGateStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt32},
			{Name: "name", Type: parquet.TypeString},
		},
	}
	if err := db.CreateTable(ctx, "users", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("users", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	rows := []map[string]any{
		{"id": int32(1), "name": "alice"},
		{"id": int32(2), "name": "bob"},
	}
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	store.armed.Store(true)
	return db, store
}

// setupGatedServer wires a gated DB to a pgwire server. The gate release is
// registered AFTER the server's Shutdown cleanup so it runs first (t.Cleanup
// is LIFO): Shutdown waits for connection goroutines, and a test that fails
// with a query still parked in the gate would otherwise hang until the
// package test timeout instead of reporting its failure.
func setupGatedServer(t *testing.T) (*Server, *gateStore) {
	t.Helper()
	db, gate := setupGatedDB(t)
	srv := startTestServer(t, db)
	t.Cleanup(gate.releaseAll)
	return srv, gate
}

// queryOutcome is what a client saw for one simple query.
type queryOutcome struct {
	code string // SQLSTATE from ErrorResponse, "" when the query succeeded
	msg  string
	tag  string
	rows int
	err  error // transport-level failure
}

// asyncSimpleQuery sends a simple query and reads its response on a
// background goroutine, so the test can act on another connection while this
// one is blocked — exactly the shape of a real cancellation.
func (c *pgClient) asyncSimpleQuery(sql string) <-chan queryOutcome {
	out := make(chan queryOutcome, 1)
	payload := append([]byte(sql), 0)
	c.writeMsg('Q', payload)
	go func() {
		var res queryOutcome
		for {
			c.conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			typ, data, err := c.readMsgNoDeadline()
			if err != nil {
				res.err = err
				out <- res
				return
			}
			switch typ {
			case 'D':
				res.rows++
			case 'C':
				res.tag = readCString(data)
			case 'I':
				res.tag = "EMPTY"
			case 'E':
				res.code, res.msg = parseErrorFields(data)
			case 'Z':
				out <- res
				return
			}
		}
	}()
	return out
}

// readMsgNoDeadline reads one message using whatever deadline the caller set.
func (c *pgClient) readMsgNoDeadline() (byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(c.conn, header[:]); err != nil {
		return 0, nil, err
	}
	msgType := header[0]
	msgLen := int(binary.BigEndian.Uint32(header[1:])) - 4
	if msgLen < 0 {
		return msgType, nil, nil
	}
	payload := make([]byte, msgLen)
	if _, err := io.ReadFull(c.conn, payload); err != nil {
		return 0, nil, err
	}
	return msgType, payload, nil
}

// parseErrorFields pulls the SQLSTATE and message out of an ErrorResponse.
func parseErrorFields(data []byte) (code, msg string) {
	for len(data) > 0 {
		fieldType := data[0]
		data = data[1:]
		if fieldType == 0 {
			break
		}
		val := readCString(data)
		data = data[len(val)+1:]
		switch fieldType {
		case 'C':
			code = val
		case 'M':
			msg = val
		}
	}
	return code, msg
}

// sendCancelRequest opens the second connection a real client uses to cancel:
// a CancelRequest carries the key material and gets no reply at all.
func sendCancelRequest(t *testing.T, addr string, pid, secret int32) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatalf("dialing for cancel: %v", err)
	}
	defer conn.Close()

	var payload []byte
	payload = binary.BigEndian.AppendUint32(payload, cancelRequestCode)
	payload = binary.BigEndian.AppendUint32(payload, uint32(pid))
	payload = binary.BigEndian.AppendUint32(payload, uint32(secret))
	msg := binary.BigEndian.AppendUint32(nil, uint32(len(payload)+4))
	msg = append(msg, payload...)
	if _, err := conn.Write(msg); err != nil {
		t.Fatalf("writing cancel request: %v", err)
	}

	// PostgreSQL answers a CancelRequest with nothing and closes: any byte
	// here would desynchronize clients that reuse the socket.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var buf [16]byte
	n, err := conn.Read(buf[:])
	if n > 0 {
		t.Fatalf("cancel connection got %d bytes of reply (%q); PostgreSQL sends none", n, buf[:n])
	}
	if err == nil {
		t.Fatal("expected the server to close the cancel connection")
	}
}

func expectCanceled(t *testing.T, out <-chan queryOutcome, wantMsg string) {
	t.Helper()
	select {
	case res := <-out:
		if res.err != nil {
			t.Fatalf("reading query response: %v", res.err)
		}
		if res.code != sqlstateQueryCanceled {
			t.Fatalf("SQLSTATE = %q (%q), want %s", res.code, res.msg, sqlstateQueryCanceled)
		}
		if wantMsg != "" && res.msg != wantMsg {
			t.Errorf("message = %q, want %q", res.msg, wantMsg)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query never returned after cancellation")
	}
}

// TestBackendKeyDataIsRealKeyMaterial: the pair the server advertises has to
// identify something. It used to be a hardcoded 0/0 for every session.
func TestBackendKeyDataIsRealKeyMaterial(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")
	b := newPGClient(t, srv.Addr())
	b.startup("testuser", "testdb")

	if a.pid == 0 || a.secret == 0 {
		t.Errorf("BackendKeyData is empty: pid=%d secret=%d", a.pid, a.secret)
	}
	if a.pid < 0 {
		t.Errorf("pid should be positive, got %d", a.pid)
	}
	if a.pid == b.pid {
		t.Errorf("two sessions share pid %d", a.pid)
	}
	if a.secret == b.secret {
		t.Errorf("two sessions share secret %d", a.secret)
	}

	// The session is unregistered when the connection goes away.
	pid := a.pid
	a.terminate()
	a.conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		srv.sessionsMu.Lock()
		_, live := srv.sessions[pid]
		srv.sessionsMu.Unlock()
		if !live {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("session left registered after disconnect")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestCancelRequestStopsRunningQuery is the issue itself: a long-running
// query on connection A, a CancelRequest on connection B, and A must come
// back with 57014 instead of running to completion.
func TestCancelRequestStopsRunningQuery(t *testing.T) {
	srv, gate := setupGatedServer(t)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	out := a.asyncSimpleQuery("SELECT * FROM users")
	gate.waitHeld(t, 1)

	sendCancelRequest(t, srv.Addr(), a.pid, a.secret)
	expectCanceled(t, out, errCanceledByRequest.Error())

	// The session survives its cancelled statement: PostgreSQL clients reuse
	// the connection immediately, and a cancel must not poison the next one.
	gate.releaseAll()
	_, rows, tag := a.simpleQuery("SELECT * FROM users")
	if len(rows) != 2 || !strings.HasPrefix(tag, "SELECT") {
		t.Errorf("connection unusable after cancel: rows=%d tag=%q", len(rows), tag)
	}
	a.terminate()
}

// TestCancelRequestWrongSecret: the secret is the only thing standing between
// an unauthenticated port and anyone killing another session's query.
func TestCancelRequestWrongSecret(t *testing.T) {
	srv, gate := setupGatedServer(t)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	out := a.asyncSimpleQuery("SELECT * FROM users")
	gate.waitHeld(t, 1)

	sendCancelRequest(t, srv.Addr(), a.pid, a.secret+1)

	select {
	case res := <-out:
		t.Fatalf("query ended on a wrong-secret cancel: code=%q msg=%q", res.code, res.msg)
	case <-time.After(300 * time.Millisecond):
	}

	// Still running, and still correct once the scan is allowed to proceed.
	gate.releaseAll()
	select {
	case res := <-out:
		if res.code != "" {
			t.Fatalf("query failed: %s %s", res.code, res.msg)
		}
		if res.rows != 2 {
			t.Errorf("rows = %d, want 2", res.rows)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query never completed")
	}
	a.terminate()
}

// TestCancelRequestUnknownPID: a cancel for a session that does not exist is
// a no-op, not a panic and not a crashed server.
func TestCancelRequestUnknownPID(t *testing.T) {
	srv, gate := setupGatedServer(t)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	out := a.asyncSimpleQuery("SELECT * FROM users")
	gate.waitHeld(t, 1)

	sendCancelRequest(t, srv.Addr(), a.pid+1, a.secret)
	sendCancelRequest(t, srv.Addr(), 0, 0) // the old hardcoded key
	sendCancelRequest(t, srv.Addr(), -1, 12345)

	select {
	case res := <-out:
		t.Fatalf("query ended on an unknown-pid cancel: code=%q msg=%q", res.code, res.msg)
	case <-time.After(300 * time.Millisecond):
	}

	gate.releaseAll()
	select {
	case res := <-out:
		if res.code != "" || res.rows != 2 {
			t.Errorf("query outcome: code=%q msg=%q rows=%d", res.code, res.msg, res.rows)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("query never completed")
	}
	a.terminate()
}

// TestCancelRequestAfterQueryFinished: a cancel that arrives late must not
// land on whatever the session runs next.
func TestCancelRequestAfterQueryFinished(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	_, rows, _ := a.simpleQuery("SELECT * FROM users")
	if len(rows) != 3 {
		t.Fatalf("setup query returned %d rows, want 3", len(rows))
	}

	sendCancelRequest(t, srv.Addr(), a.pid, a.secret)

	// Idle session: the cancel found no statement, so the next one runs.
	_, rows, tag := a.simpleQuery("SELECT * FROM users")
	if len(rows) != 3 {
		t.Errorf("query after a late cancel returned %d rows (tag %q), want 3", len(rows), tag)
	}
	if strings.HasPrefix(tag, "ERROR") {
		t.Errorf("query after a late cancel failed: %s", tag)
	}
	a.terminate()
}

// TestCancelRequestConcurrentSessions: cancelling one session must leave
// every other session running.
func TestCancelRequestConcurrentSessions(t *testing.T) {
	srv, gate := setupGatedServer(t)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")
	b := newPGClient(t, srv.Addr())
	b.startup("testuser", "testdb")

	outA := a.asyncSimpleQuery("SELECT * FROM users")
	outB := b.asyncSimpleQuery("SELECT * FROM users")
	gate.waitHeld(t, 2)

	sendCancelRequest(t, srv.Addr(), a.pid, a.secret)
	expectCanceled(t, outA, errCanceledByRequest.Error())

	select {
	case res := <-outB:
		t.Fatalf("cancelling session A ended session B's query: code=%q msg=%q", res.code, res.msg)
	case <-time.After(300 * time.Millisecond):
	}

	gate.releaseAll()
	select {
	case res := <-outB:
		if res.code != "" || res.rows != 2 {
			t.Errorf("session B outcome: code=%q msg=%q rows=%d", res.code, res.msg, res.rows)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("session B never completed")
	}
	a.terminate()
	b.terminate()
}

// TestStatementTimeoutReportsQueryCanceled: statement_timeout and
// CancelRequest share one mechanism, and both owe the client 57014 —
// a timed-out statement used to surface as a generic 42000 syntax-ish error.
func TestStatementTimeoutReportsQueryCanceled(t *testing.T) {
	srv, gate := setupGatedServer(t)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	if _, _, tag := a.simpleQuery("SET statement_timeout = 250"); tag != "SET" {
		t.Fatalf("SET statement_timeout: %q", tag)
	}

	start := time.Now()
	out := a.asyncSimpleQuery("SELECT * FROM users")
	gate.waitHeld(t, 1)
	expectCanceled(t, out, errStatementTimeout.Error())
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("statement_timeout took %s to fire", elapsed)
	}
}

// TestServerQueryTimeoutReportsQueryCanceled covers the server-level default
// (--query-timeout) on the same path.
func TestServerQueryTimeoutReportsQueryCanceled(t *testing.T) {
	db, gate := setupGatedDB(t)
	srv := NewServer(db, Config{QueryTimeout: 250 * time.Millisecond}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	t.Cleanup(gate.releaseAll) // LIFO: release before Shutdown waits (see setupGatedServer)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	out := a.asyncSimpleQuery("SELECT * FROM users")
	gate.waitHeld(t, 1)
	expectCanceled(t, out, errStatementTimeout.Error())
}

// TestCancelRequestExtendedProtocol: JDBC and pgx cancel through
// Parse/Bind/Execute, not the simple query path.
func TestCancelRequestExtendedProtocol(t *testing.T) {
	srv, gate := setupGatedServer(t)

	a := newPGClient(t, srv.Addr())
	a.startup("testuser", "testdb")

	sql := "SELECT * FROM users"
	a.writeMsg('P', append(append([]byte{0}, sql...), 0, 0, 0))
	a.writeMsg('B', []byte{0, 0, 0, 0, 0, 0, 0, 0})
	a.writeMsg('E', []byte{0, 0, 0, 0, 0})
	a.writeMsg('S', nil)

	out := make(chan queryOutcome, 1)
	go func() {
		var res queryOutcome
		for {
			a.conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			typ, data, err := a.readMsgNoDeadline()
			if err != nil {
				res.err = err
				out <- res
				return
			}
			switch typ {
			case 'D':
				res.rows++
			case 'C':
				res.tag = readCString(data)
			case 'E':
				res.code, res.msg = parseErrorFields(data)
			case 'Z':
				out <- res
				return
			}
		}
	}()

	gate.waitHeld(t, 1)
	sendCancelRequest(t, srv.Addr(), a.pid, a.secret)
	expectCanceled(t, out, errCanceledByRequest.Error())
}

// TestPgxCancelRequest drives the real thing: pgx builds the CancelRequest
// from the BackendKeyData it was handed at startup, which is what psql's
// Ctrl-C, DataGrip's stop button and JDBC's Statement.cancel() all do.
func TestPgxCancelRequest(t *testing.T) {
	srv, gate := setupGatedServer(t)
	ctx := context.Background()

	connStr := fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		srv.Addr()[len("127.0.0.1:"):])
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	errCh := make(chan error, 1)
	go func() {
		_, qErr := conn.Query(ctx, "SELECT * FROM users", pgx.QueryExecModeSimpleProtocol)
		errCh <- qErr
	}()

	gate.waitHeld(t, 1)
	if err := conn.PgConn().CancelRequest(ctx); err != nil {
		t.Fatalf("pgx CancelRequest: %v", err)
	}

	select {
	case qErr := <-errCh:
		var pgErr *pgconn.PgError
		if !errors.As(qErr, &pgErr) {
			t.Fatalf("query error = %v, want a *pgconn.PgError", qErr)
		}
		if pgErr.Code != sqlstateQueryCanceled {
			t.Fatalf("SQLSTATE = %s (%s), want %s", pgErr.Code, pgErr.Message, sqlstateQueryCanceled)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pgx query never returned after CancelRequest")
	}
}

// TestCancelSessionSecretMismatch exercises the registry directly, including
// the case a wire test cannot reach: a session that is registered but idle.
func TestCancelSessionSecretMismatch(t *testing.T) {
	s := &Server{}
	c := &pgConn{}
	if err := s.registerSession(c); err != nil {
		t.Fatal(err)
	}

	if s.cancelSession(c.cancelPID, c.cancelSecret) {
		t.Error("cancelled an idle session; there is no statement to cancel")
	}

	ctx, release := c.beginStatement(context.Background(), 0)
	if s.cancelSession(c.cancelPID, c.cancelSecret+1) {
		t.Error("wrong secret cancelled the statement")
	}
	if ctx.Err() != nil {
		t.Fatal("statement context cancelled by a wrong-secret request")
	}
	if !s.cancelSession(c.cancelPID, c.cancelSecret) {
		t.Fatal("correct key did not cancel the statement")
	}
	if !errors.Is(context.Cause(ctx), errCanceledByRequest) {
		t.Errorf("cause = %v, want %v", context.Cause(ctx), errCanceledByRequest)
	}
	if got := canceledMessage(ctx); got != errCanceledByRequest.Error() {
		t.Errorf("canceledMessage = %q", got)
	}
	release()

	// After release the statement is gone: a repeat cancel finds nothing.
	if s.cancelSession(c.cancelPID, c.cancelSecret) {
		t.Error("cancelled a finished statement")
	}
	s.unregisterSession(c)
	if s.cancelSession(c.cancelPID, c.cancelSecret) {
		t.Error("cancelled a session that had disconnected")
	}
}

// TestStatementTimeoutCause keeps the two 57014 conditions distinguishable.
func TestStatementTimeoutCause(t *testing.T) {
	c := &pgConn{}
	ctx, release := c.beginStatement(context.Background(), 10*time.Millisecond)
	defer release()
	<-ctx.Done()
	if got := canceledMessage(ctx); got != errStatementTimeout.Error() {
		t.Errorf("canceledMessage = %q, want %q", got, errStatementTimeout.Error())
	}

	// A statement that ends normally is not a cancelled one.
	ctx2, release2 := c.beginStatement(context.Background(), 0)
	if got := canceledMessage(ctx2); got != "" {
		t.Errorf("canceledMessage on a live statement = %q", got)
	}
	release2()
	if got := canceledMessage(ctx2); got != "" {
		t.Errorf("canceledMessage after normal release = %q, want \"\"", got)
	}
}
