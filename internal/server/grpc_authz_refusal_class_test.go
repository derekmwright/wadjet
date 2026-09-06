package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestGRPCQueryRefusalCarriesPermissionDenied holds the refusal CLASS on the
// two SQL RPCs.
//
// Both wrapped every engine error as codes.Internal, so a statement refused
// with SQLSTATE 42501 before a row was touched reached the client as "the
// server failed". A client cannot tell "you may not do that" from "we broke":
// it retries the second and gives up on the first, and an operator reading the
// code sees an outage where there is a working control. The other doors carry
// the class (HTTP 403, pgwire 42501).
//
// The cells are DML refusals, which are the ones that carry 42501 today:
// auth.EnforceDMLPolicies refuses an ActionWrite before anything is read or
// written. A table-level SELECT denial still crosses as Internal because
// internal/auth/plan_enforce.go returns it without a class on ANY door — that
// is recorded as a residual, not asserted here, and this mapping needs no edit
// when it is fixed.
func TestGRPCQueryRefusalCarriesPermissionDenied(t *testing.T) {
	rig := grpcAuthzUp(t, grpcAuthzABAC, grpcAuthzConfig())

	for _, sql := range []string{
		"DELETE FROM allowed WHERE id = 1",
		"INSERT INTO allowed (id, ssn) VALUES (1, 'x')",
		"UPDATE allowed SET ssn = 'y' WHERE id = 1",
	} {
		_, err := rig.client.Query(grpcAuthzCtx("reader-key"), &wadjetv1.QueryRequest{Sql: sql})
		if got := grpcAuthzCode(err); got != codes.PermissionDenied {
			t.Errorf("reader Query(%q): code %v, want PermissionDenied (err %v)", sql, got, err)
		}
		// The message is the refusal's own, not "query error: ..." — a
		// refusal is not an execution failure.
		if err != nil && strings.Contains(err.Error(), "query error:") {
			t.Errorf("reader Query(%q) message %v\n  want the refusal's own text", sql, err)
		}
		if err == nil || !strings.Contains(err.Error(), "permission denied for table") {
			t.Errorf("reader Query(%q) message %v\n  want the 42501 text", sql, err)
		}
	}

	// The same statements from an identity that may write are unaffected —
	// the mapping is on the refusal, not on the statement.
	if _, err := rig.client.Query(grpcAuthzCtx("writer-key"),
		&wadjetv1.QueryRequest{Sql: "DELETE FROM allowed WHERE id = 1"}); err != nil {
		t.Errorf("writer Query(DELETE): %v", err)
	}

	// An ordinary execution error keeps codes.Internal: this narrows the
	// Internal bucket, it does not empty it.
	_, err := rig.client.Query(grpcAuthzCtx("admin-key"),
		&wadjetv1.QueryRequest{Sql: "SELECT * FROM no_such_table"})
	if got := grpcAuthzCode(err); got != codes.Internal {
		t.Errorf("admin Query(missing table): code %v, want Internal (err %v)", got, err)
	}
}

// TestGRPCResultDrainRefusalCarriesItsClass covers the DRAIN paths — the
// streaming loop in streamResultBatches and its unary twin, result.Rows() —
// which raise a failure after the result handle already exists.
//
// Nothing produces a 42501 there today: the cost guard's ceilings are plan-time
// estimates, so a `query_limit` obligation refuses before the result is built,
// and a bufconn cell cannot drive one. That is exactly why the mapping is
// asserted at the function: the two drains were the last place in the two SQL
// RPCs where a refusal would have crossed as codes.Internal, and a class that
// holds on three paths out of four is not a class (round-1 review P4). The
// wiring itself is one line in each drain and visible in the diff.
func TestGRPCResultDrainRefusalCarriesItsClass(t *testing.T) {
	refusals := []error{
		sqlerr.New("42501", `permission denied for table "secret"`),
		fmt.Errorf("wrapped: %w", sqlerr.New("42501", `permission denied for table "secret"`)),
		fmt.Errorf("%w: %q permission required", auth.ErrUnauthorized, "write"),
	}
	for _, err := range refusals {
		got := grpcResultError(err, "reading result batches")
		if code := grpcAuthzCode(got); code != codes.PermissionDenied {
			t.Errorf("grpcResultError(%v) = %v, want PermissionDenied", err, code)
		}
		// The refusal keeps its own words on BOTH drains: an identity told
		// "permission denied" mid-stream and "reading result batches:
		// permission denied" up front is two answers to one question.
		if strings.Contains(got.Error(), "reading result batches") {
			t.Errorf("grpcResultError(%v) = %v\n  a refusal must not wear the drain's prefix", err, got)
		}
	}

	// A real drain failure keeps codes.Internal AND the caller's own prefix —
	// the message "reading result batches" clients and operators already see.
	got := grpcResultError(errors.New("scratch file vanished"), "reading result batches")
	if code := grpcAuthzCode(got); code != codes.Internal {
		t.Errorf("grpcResultError(drain failure) = %v, want Internal", code)
	}
	if !strings.Contains(got.Error(), "reading result batches: scratch file vanished") {
		t.Errorf("grpcResultError(drain failure) = %v\n  want the drain's own message preserved", got)
	}
	// And the query prefix is still the query prefix.
	if q := grpcQueryError(errors.New("boom")); !strings.Contains(q.Error(), "query error: boom") {
		t.Errorf("grpcQueryError = %v, want the query prefix preserved", q)
	}
}

// grpcFakeBatchStream is a result stream that fails on the first Next. It is
// what lets the streaming drain be driven without a coordinator.
type grpcFakeBatchStream struct {
	err    error
	closed bool
}

func (s *grpcFakeBatchStream) Next(context.Context) (*batch.RecordBatch, error) { return nil, s.err }
func (s *grpcFakeBatchStream) Close() error                                     { s.closed = true; return nil }

// TestStreamResultBatchesMapsTheDrainRefusal gates the WIRING, not the mapping.
//
// TestGRPCResultDrainRefusalCarriesItsClass asserts grpcResultError in
// isolation, so restoring this drain's call site to a bare
// `status.Errorf(codes.Internal, …)` passed the entire package: the fix's
// mapping half was gated and the half that is the actual point — that THIS loop
// routes through it — was not (round-1 review P1). Driving the real loop with a
// stream that refuses closes that.
func TestStreamResultBatchesMapsTheDrainRefusal(t *testing.T) {
	refused := &grpcFakeBatchStream{err: sqlerr.New("42501", `permission denied for table "secret"`)}
	err := streamResultBatches(&chunkStreamer{}, refused)
	if got := grpcAuthzCode(err); got != codes.PermissionDenied {
		t.Errorf("streamResultBatches over a refusing stream: code %v, want PermissionDenied (err %v)", got, err)
	}
	if err == nil || !strings.Contains(err.Error(), `permission denied for table "secret"`) {
		t.Errorf("streamResultBatches refusal = %v\n  want the refusal's own text", err)
	}
	if err != nil && strings.Contains(err.Error(), "reading result batches") {
		t.Errorf("streamResultBatches refusal = %v\n  a refusal must not wear the drain's prefix", err)
	}
	// The stream is closed on the error exit: an undrained spill-backed stream
	// pins its scratch file until Close runs, and a refusal is an early exit.
	if !refused.closed {
		t.Error("streamResultBatches did not close the stream on the refusal path")
	}

	// A genuine drain failure still carries Internal and the message operators
	// already see — this narrows that bucket, it does not empty it.
	broken := &grpcFakeBatchStream{err: errors.New("scratch file vanished")}
	err = streamResultBatches(&chunkStreamer{}, broken)
	if got := grpcAuthzCode(err); got != codes.Internal {
		t.Errorf("streamResultBatches over a broken stream: code %v, want Internal (err %v)", got, err)
	}
	if err == nil || !strings.Contains(err.Error(), "reading result batches: scratch file vanished") {
		t.Errorf("streamResultBatches failure = %v\n  want the drain's own message", err)
	}
	if !broken.closed {
		t.Error("streamResultBatches did not close the stream on the failure path")
	}
}
