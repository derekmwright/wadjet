package pgwire

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// cancelRequestCode is the protocol version field of a CancelRequest
// (1234 << 16 | 5678). A client that wants to stop a running statement opens
// a SECOND connection and sends this instead of a StartupMessage, carrying
// the (pid, secret) pair the server handed out in BackendKeyData — the
// connection running the statement is blocked reading results, so it cannot
// carry the request itself.
const cancelRequestCode = 80877102

// sqlstateQueryCanceled is SQLSTATE 57014 (query_canceled), what PostgreSQL
// reports to the client whose statement was cancelled or timed out.
const sqlstateQueryCanceled = "57014"

// Causes attached to a statement context, so the error path can tell a
// cancelled statement from a timed-out one and from an ordinary failure.
// The strings are PostgreSQL's own messages for these two conditions.
var (
	errCanceledByRequest = errors.New("canceling statement due to user request")
	errStatementTimeout  = errors.New("canceling statement due to statement timeout")
)

// errCancelRequest ends the startup handshake of a cancel connection.
// PostgreSQL sends NO reply on that connection — not an error, not a
// ReadyForQuery — it simply closes it, and clients rely on that.
var errCancelRequest = errors.New("cancel request handled")

// runningStmt is the statement currently executing on a connection. It exists
// only to hand the cancel path (running on ANOTHER connection's goroutine) a
// stable handle on this statement's context: the pointer identity is what
// makes "cancel arrived after the statement finished" a no-op instead of a
// cancellation of whatever ran next.
type runningStmt struct {
	cancel context.CancelCauseFunc
}

// registerSession assigns the connection random (pid, secret) key material and
// publishes it in the server's session map so a CancelRequest carrying that
// pair can find this connection. The pid is positive and unique among live
// sessions; it is not an OS process id (Wadjet serves a session per goroutine)
// and clients never treat it as one — it is an opaque handle.
func (s *Server) registerSession(c *pgConn) error {
	s.sessionsMu.Lock()
	defer s.sessionsMu.Unlock()
	if s.sessions == nil {
		s.sessions = make(map[int32]*pgConn)
	}
	for attempt := 0; attempt < 64; attempt++ {
		pid, err := randInt32()
		if err != nil {
			return fmt.Errorf("generating cancel key: %w", err)
		}
		pid &= 0x7fffffff // keep it positive: clients print and re-encode it
		if pid == 0 {
			continue
		}
		if _, taken := s.sessions[pid]; taken {
			continue
		}
		secret, err := randInt32()
		if err != nil {
			return fmt.Errorf("generating cancel key: %w", err)
		}
		c.cancelPID = pid
		c.cancelSecret = secret
		s.sessions[pid] = c
		return nil
	}
	return errors.New("generating cancel key: no free session id")
}

// unregisterSession drops the connection's cancel key. Safe to call for a
// connection that was never registered (startup failed, cancel connection).
func (s *Server) unregisterSession(c *pgConn) {
	if c.cancelPID == 0 {
		return
	}
	s.sessionsMu.Lock()
	if s.sessions[c.cancelPID] == c {
		delete(s.sessions, c.cancelPID)
	}
	s.sessionsMu.Unlock()
}

// cancelSession cancels the statement running on the session identified by
// pid, if the secret matches. Reports whether a statement was actually
// cancelled — false covers every hostile or racy case (unknown pid, wrong
// secret, session idle between statements, statement already finished), all
// of which are no-ops by design.
//
// The secret is compared in constant time: a cancel connection is
// unauthenticated by protocol design, so a timing oracle here would let an
// attacker who can reach the port recover a session's secret byte by byte and
// kill other users' queries.
func (s *Server) cancelSession(pid, secret int32) bool {
	s.sessionsMu.Lock()
	c := s.sessions[pid]
	s.sessionsMu.Unlock()
	if c == nil {
		return false
	}
	var want, got [4]byte
	binary.BigEndian.PutUint32(want[:], uint32(c.cancelSecret))
	binary.BigEndian.PutUint32(got[:], uint32(secret))
	if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		return false
	}
	return c.cancelStatement()
}

// handleCancelRequest reads a CancelRequest payload (already positioned past
// the version field) and cancels the addressed statement.
func (s *Server) handleCancelRequest(payload []byte) {
	if len(payload) < 8 {
		return
	}
	pid := int32(binary.BigEndian.Uint32(payload[0:4]))
	secret := int32(binary.BigEndian.Uint32(payload[4:8]))
	cancelled := s.cancelSession(pid, secret)
	s.logger.Debug("pgwire cancel request", "pid", pid, "cancelled", cancelled)
}

// beginStatement builds the context for one statement and registers it as
// this connection's cancellable statement. The returned CancelFunc
// unregisters and releases it; callers defer it exactly as before.
//
// Both ends of the statement's life are covered: a cancel that arrives while
// no statement is registered finds nil and does nothing, and a cancel that
// races the end of a statement cancels a context nobody is waiting on.
func (c *pgConn) beginStatement(base context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancelCause := context.WithCancelCause(base)
	stop := context.CancelFunc(func() {})
	if timeout > 0 {
		ctx, stop = context.WithTimeoutCause(ctx, timeout, errStatementTimeout)
	}

	st := &runningStmt{cancel: cancelCause}
	c.stmtMu.Lock()
	c.stmt = st
	c.stmtMu.Unlock()

	return ctx, func() {
		c.stmtMu.Lock()
		if c.stmt == st {
			c.stmt = nil
		}
		c.stmtMu.Unlock()
		stop()
		cancelCause(context.Canceled)
	}
}

// cancelStatement cancels whatever statement this connection is running.
// Called from another connection's goroutine.
func (c *pgConn) cancelStatement() bool {
	c.stmtMu.Lock()
	st := c.stmt
	c.stmtMu.Unlock()
	if st == nil {
		return false
	}
	st.cancel(errCanceledByRequest)
	return true
}

// canceledMessage returns the PostgreSQL message for a statement context that
// ended in cancellation or statement timeout, or "" for any other outcome.
func canceledMessage(ctx context.Context) string {
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, errCanceledByRequest):
		return errCanceledByRequest.Error()
	case errors.Is(cause, errStatementTimeout):
		return errStatementTimeout.Error()
	}
	return ""
}

// sendQueryError surfaces a statement failure. A statement whose context was
// cancelled — by a CancelRequest or by statement_timeout — reports SQLSTATE
// 57014 (query_canceled) with PostgreSQL's message, which is what clients
// match on to distinguish "you stopped it" from "your SQL is wrong". An error
// that carries its own SQLSTATE (sqlerr.StateOf — plan-time rejections like
// 42P01/42702/42803, runtime data errors like 22012) reports that code;
// everything else keeps the generic code the caller chose (#366 tracks the
// remaining blanket 42000s).
func (c *pgConn) sendQueryError(ctx context.Context, code string, err error) {
	if msg := canceledMessage(ctx); msg != "" {
		c.sendError("ERROR", sqlstateQueryCanceled, msg)
		return
	}
	if s := sqlerr.StateOf(err); s != "" {
		code = s
	}
	c.sendError("ERROR", code, err.Error())
}

// copyIngestError reports a COPY row the ingester refused.
//
// A value with no DECIMAL at its column's (p, s) is 22003, and text that names
// no number is 22P02 — the same codes the same value earns through INSERT,
// because both go through parquet.DecimalValueFromBox (#647). The blanket
// XX000 this replaced told a client nothing it could branch on, and COPY is
// the bulk path where a client most needs to tell "this row is bad" from
// "the server broke".
func (c *pgConn) copyIngestError(err error) {
	code := "XX000"
	if s := sqlerr.StateOf(err); s != "" {
		code = s
	}
	c.sendError("ERROR", code, fmt.Sprintf("COPY ingest: %v", err))
}

func randInt32() (int32, error) {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(b[:])), nil
}
