package auth

import (
	"bytes"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestNewAuditLogger(t *testing.T) {
	// With nil logger should use slog.Default()
	al := NewAuditLogger(nil)
	if al == nil {
		t.Fatal("expected non-nil audit logger")
	}
	if al.logger == nil {
		t.Fatal("expected logger to be set")
	}

	// With provided logger
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	al2 := NewAuditLogger(logger)
	if al2 == nil {
		t.Fatal("expected non-nil audit logger")
	}
}

func TestAuditLoggerNilSafe(t *testing.T) {
	// All methods on nil AuditLogger should not panic
	var al *AuditLogger

	al.LogQuery(nil, "SELECT 1", nil, time.Second, nil)
	al.LogAccessDenied(nil, "table", "reason")
	al.LogRowFilterApplied(nil, "table", "filter")
	al.LogColumnPolicy(nil, "table", nil, nil)
	al.LogAuthFailure("1.2.3.4:8080", "/v1/query", "bad token")
}

func TestAuditLogQuery(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	al := NewAuditLogger(logger)

	// Successful query with identity
	id := &Identity{Name: "alice", Role: "admin", Method: "jwt"}
	al.LogQuery(id, "SELECT * FROM events", []string{"events"}, 100*time.Millisecond, nil)
	if !bytes.Contains(buf.Bytes(), []byte("query_executed")) {
		t.Error("expected query_executed in log output")
	}
	if !bytes.Contains(buf.Bytes(), []byte("alice")) {
		t.Error("expected identity name in log output")
	}

	// Query with error
	buf.Reset()
	al.LogQuery(id, "SELECT * FROM bad", nil, 50*time.Millisecond, errors.New("table not found"))
	if !bytes.Contains(buf.Bytes(), []byte("query_failed")) {
		t.Error("expected query_failed in log output")
	}
	if !bytes.Contains(buf.Bytes(), []byte("table not found")) {
		t.Error("expected error message in log output")
	}

	// Query with nil identity (anonymous)
	buf.Reset()
	al.LogQuery(nil, "SELECT 1", nil, time.Millisecond, nil)
	if !bytes.Contains(buf.Bytes(), []byte("anonymous")) {
		t.Error("expected anonymous identity in log output")
	}
}

func TestAuditLogAccessDenied(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	al := NewAuditLogger(logger)

	id := &Identity{Name: "bob", Role: "reader"}
	al.LogAccessDenied(id, "users", "insufficient permissions")
	if !bytes.Contains(buf.Bytes(), []byte("access_denied")) {
		t.Error("expected access_denied in log output")
	}

	// Nil identity
	buf.Reset()
	al.LogAccessDenied(nil, "secrets", "unauthorized")
	if !bytes.Contains(buf.Bytes(), []byte("access_denied")) {
		t.Error("expected access_denied for nil identity")
	}
}

func TestAuditLogRowFilterApplied(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	al := NewAuditLogger(logger)

	id := &Identity{Name: "alice", Role: "analyst"}
	al.LogRowFilterApplied(id, "events", "department = 'engineering'")
	if !bytes.Contains(buf.Bytes(), []byte("row_filter_applied")) {
		t.Error("expected row_filter_applied in log output")
	}
}

func TestAuditLogColumnPolicy(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	al := NewAuditLogger(logger)

	id := &Identity{Name: "alice", Role: "analyst"}

	// Empty masked/denied should not log
	al.LogColumnPolicy(id, "events", nil, nil)
	if buf.Len() > 0 {
		t.Error("expected no log output for empty masked/denied")
	}

	// With masked columns
	al.LogColumnPolicy(id, "users", []string{"email", "phone"}, nil)
	if !bytes.Contains(buf.Bytes(), []byte("column_policy_applied")) {
		t.Error("expected column_policy_applied in log output")
	}

	// With denied columns
	buf.Reset()
	al.LogColumnPolicy(id, "users", nil, []string{"ssn"})
	if !bytes.Contains(buf.Bytes(), []byte("column_policy_applied")) {
		t.Error("expected column_policy_applied in log output")
	}

	// Both masked and denied
	buf.Reset()
	al.LogColumnPolicy(id, "users", []string{"email"}, []string{"ssn"})
	if !bytes.Contains(buf.Bytes(), []byte("column_policy_applied")) {
		t.Error("expected column_policy_applied in log output")
	}
}

func TestAuditLogAuthFailure(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	al := NewAuditLogger(logger)

	al.LogAuthFailure("192.168.1.1:12345", "/v1/query", "invalid token")
	if !bytes.Contains(buf.Bytes(), []byte("auth_failure")) {
		t.Error("expected auth_failure in log output")
	}
}
