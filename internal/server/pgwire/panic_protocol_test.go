package pgwire

import (
	"strings"
	"testing"
	"time"
)

// readUntilReady drains messages until ReadyForQuery, returning every message
// type it saw (Z included) and any ErrorResponse text.
func readUntilReady(t *testing.T, c *pgClient, budget time.Duration) (types string, errText string) {
	t.Helper()
	deadline := time.Now().Add(budget)
	for {
		if err := c.conn.SetReadDeadline(deadline); err != nil {
			t.Fatalf("SetReadDeadline: %v", err)
		}
		typ, data, err := c.readMsg()
		if err != nil {
			t.Fatalf("reading (saw %q so far): %v", types, err)
		}
		types += string(typ)
		if typ == 'E' {
			errText = c.parseError(data)
		}
		if typ == 'Z' {
			_ = c.conn.SetReadDeadline(time.Time{})
			return types, errText
		}
	}
}

// assertNothingPending is the assertion a ReadyForQuery COUNT cannot make. A
// client stops reading at the Z it was waiting for, so a second, stale Z is
// invisible to it right then — it sits in the socket and is consumed as the
// reply to whatever the client sends next. Catching the desync means looking
// for a message that should not be there yet.
func assertNothingPending(t *testing.T, c *pgClient, what string) {
	t.Helper()
	if err := c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	defer func() { _ = c.conn.SetReadDeadline(time.Time{}) }()
	typ, _, err := c.readMsg()
	if err == nil {
		t.Fatalf("%s: an unsolicited %q was buffered on the connection. A client consumes "+
			"it as the answer to its NEXT statement, so a perfectly good query then returns "+
			"whatever this message says — silently.", what, string(typ))
	}
}

// TestExtendedQueryPanicKeepsTheProtocolInStep is the protocol regression for
// the panic boundary in dispatch, driven over a raw socket.
//
// It pins two things at once.
//
// First: an empty Bind or Parse payload — four bytes any client can put on
// the wire — panics inside the handler on a slice bound. Before the boundary
// existed that killed the whole server.
//
// Second, and the reason the boundary alone is not enough: who owes a
// ReadyForQuery is NOT uniform. handleQuery ('Q') sends its own on every path
// it can take, while the extended-query handlers (P/B/D/E/C) send none —
// there, only Sync does. A boundary that answered every panic with
// ErrorResponse + ReadyForQuery emitted a SPURIOUS Z for the extended case,
// and the client, still waiting on its own Sync, consumed the stale one as
// the reply to its next statement. The failure mode is not an error, it is
// zero rows for a good query on a healthy connection.
//
// PostgreSQL's rule is what is asserted: report the error, discard until
// Sync, and let Sync emit the one ReadyForQuery.
func TestExtendedQueryPanicKeepsTheProtocolInStep(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  byte
	}{
		{"empty_bind", 'B'},
		{"empty_parse", 'P'},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			db := setupTestDB(t)
			srv := startTestServer(t, db)
			c := newPGClient(t, srv.Addr())
			c.startup("wadjet", "wadjet")

			c.writeMsg(tc.typ, nil) // the payload that panics the handler
			c.writeMsg('S', nil)    // Sync: the message that owes the Z

			types, errText := readUntilReady(t, c, 20*time.Second)
			if !strings.Contains(types, "E") {
				t.Fatalf("no ErrorResponse for a panicking message; saw %q", types)
			}
			if n := strings.Count(types, "Z"); n != 1 {
				t.Fatalf("saw %d ReadyForQuery messages (%q), want exactly 1", n, types)
			}
			if !strings.Contains(errText, "out of range") {
				t.Errorf("error text %q lost the panic value", errText)
			}
			// Sync's Z has been read. Anything still queued is the spurious
			// one, and this is the assertion that catches it.
			assertNothingPending(t, c, "after Sync following a panicking "+string(tc.typ))

			// The connection must be usable AND in step: with a stale Z
			// buffered, this query's real answer is read as the reply to
			// something else and the client sees nothing.
			cols, rows, _ := c.simpleQuery("SELECT 42 AS answer")
			if len(cols) != 1 || cols[0] != "answer" {
				t.Fatalf("next query returned columns %v — the stream is desynchronised", cols)
			}
			if len(rows) != 1 || rows[0][0] != "42" {
				t.Fatalf("next query returned rows %v, want [[42]]", rows)
			}
		})
	}
}

// TestPanicInExtendedFlowIsDiscardedUntilSync pins the other half of
// PostgreSQL's rule: once an extended-query message has errored, the messages
// between it and Sync are DISCARDED. Executing them instead runs work the
// client already knows failed and emits replies it is not expecting.
func TestPanicInExtendedFlowIsDiscardedUntilSync(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	c := newPGClient(t, srv.Addr())
	c.startup("wadjet", "wadjet")

	c.writeMsg('B', nil) // panics -> error state
	// Everything here must be swallowed, not answered.
	c.writeMsg('D', []byte{'P', 0})
	c.writeMsg('E', []byte{0, 0, 0, 0, 0})
	c.writeMsg('S', nil)

	types, _ := readUntilReady(t, c, 20*time.Second)
	if types != "EZ" {
		t.Fatalf("message sequence %q, want \"EZ\" — messages between the error and Sync "+
			"must be discarded, not executed", types)
	}
	assertNothingPending(t, c, "after Sync ended the error state")

	cols, rows, _ := c.simpleQuery("SELECT 42 AS answer")
	if len(cols) != 1 || len(rows) != 1 || rows[0][0] != "42" {
		t.Fatalf("connection unusable after the error state: cols=%v rows=%v", cols, rows)
	}
}

// TestUnsupportedMessageBetweenParseAndSyncKeepsTheProtocolInStep pins the
// same desync for the dispatch default arm (unsupported message type) that
// TestExtendedQueryPanicKeepsTheProtocolInStep pins for a recovered panic.
//
// An unsupported message type is never 'Q', so nothing stops a client (or a
// fuzzer, or a driver like the one 'F' FunctionCall realistically models)
// from sending one in the middle of an extended-query sequence, before its
// Sync. Answering it with an immediate ReadyForQuery — as if it were a
// self-contained request — emits a Z the client's own Sync hasn't earned
// yet: the client, still waiting for that Sync's reply, consumes the
// spurious one as the answer to whatever it sends next.
func TestUnsupportedMessageBetweenParseAndSyncKeepsTheProtocolInStep(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	c := newPGClient(t, srv.Addr())
	c.startup("wadjet", "wadjet")

	sql := "SELECT 1"
	c.writeMsg('P', append(append([]byte{0}, sql...), 0, 0, 0)) // unnamed statement, no params
	c.writeMsg('F', nil)                                        // unsupported mid-sequence
	c.writeMsg('S', nil)                                        // Sync: the message that owes the Z

	types, errText := readUntilReady(t, c, 20*time.Second)
	if !strings.Contains(types, "E") {
		t.Fatalf("no ErrorResponse for an unsupported message type; saw %q", types)
	}
	if n := strings.Count(types, "Z"); n != 1 {
		t.Fatalf("saw %d ReadyForQuery messages (%q), want exactly 1", n, types)
	}
	if !strings.Contains(errText, "unsupported") {
		t.Errorf("error text %q lost the unsupported-message report", errText)
	}
	// Sync's Z has been read. Anything still queued is the spurious one an
	// immediate Z on the default arm would have produced.
	assertNothingPending(t, c, "after Sync following an unsupported message mid-sequence")

	cols, rows, _ := c.simpleQuery("SELECT 42 AS answer")
	if len(cols) != 1 || cols[0] != "answer" {
		t.Fatalf("next query returned columns %v — the stream is desynchronised", cols)
	}
	if len(rows) != 1 || rows[0][0] != "42" {
		t.Fatalf("next query returned rows %v, want [[42]]", rows)
	}
}

// TestSimpleQueryPanicStillReadiesTheConnection is the counterpart: in the
// simple-query protocol handleQuery owes the Z itself, so when a panic kills
// it before it sends one the boundary must supply it — otherwise the client
// waits forever on a connection the server considers idle.
func TestSimpleQueryPanicStillReadiesTheConnection(t *testing.T) {
	db := setupTestDB(t)
	srv := startTestServer(t, db)
	c := newPGClient(t, srv.Addr())
	c.startup("wadjet", "wadjet")

	c.writeMsg('Q', nil) // unterminated: readCString runs off the end

	types, _ := readUntilReady(t, c, 20*time.Second)
	if n := strings.Count(types, "Z"); n != 1 {
		t.Fatalf("saw %d ReadyForQuery messages (%q), want exactly 1", n, types)
	}
	assertNothingPending(t, c, "after a panicking simple query")

	cols, rows, _ := c.simpleQuery("SELECT 42 AS answer")
	if len(cols) != 1 || len(rows) != 1 || rows[0][0] != "42" {
		t.Fatalf("connection unusable after a recovered panic: cols=%v rows=%v", cols, rows)
	}
}
