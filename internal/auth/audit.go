package auth

import (
	"log/slog"
	"time"
)

// AuditLogger records security-relevant events for compliance and forensics.
type AuditLogger struct {
	logger *slog.Logger
}

// NewAuditLogger creates an audit logger that writes structured security events.
func NewAuditLogger(logger *slog.Logger) *AuditLogger {
	if logger == nil {
		logger = slog.Default()
	}
	return &AuditLogger{logger: logger.With("component", "audit")}
}

// LogQuery records a query execution with identity context.
func (a *AuditLogger) LogQuery(identity *Identity, sql string, tables []string, elapsed time.Duration, err error) {
	if a == nil {
		return
	}
	attrs := []any{
		"sql", sql,
		"tables", tables,
		"elapsed", elapsed.String(),
	}
	if identity != nil {
		attrs = append(attrs, "identity", identity.Name, "role", identity.Role, "method", identity.Method)
	} else {
		attrs = append(attrs, "identity", "anonymous")
	}
	if err != nil {
		attrs = append(attrs, "error", err.Error())
		a.logger.Warn("query_failed", attrs...)
	} else {
		a.logger.Info("query_executed", attrs...)
	}
}

// LogAccessDenied records authorization failures.
func (a *AuditLogger) LogAccessDenied(identity *Identity, resource, reason string) {
	if a == nil {
		return
	}
	attrs := []any{
		"resource", resource,
		"reason", reason,
	}
	if identity != nil {
		attrs = append(attrs, "identity", identity.Name, "role", identity.Role)
	}
	a.logger.Warn("access_denied", attrs...)
}

// LogRowFilterApplied records when a row filter is injected into a query plan.
func (a *AuditLogger) LogRowFilterApplied(identity *Identity, table, filter string) {
	if a == nil {
		return
	}
	a.logger.Info("row_filter_applied",
		"identity", identity.Name,
		"role", identity.Role,
		"table", table,
		"filter", filter,
	)
}

// LogColumnPolicy records when columns are masked or denied.
func (a *AuditLogger) LogColumnPolicy(identity *Identity, table string, masked, denied []string) {
	if a == nil {
		return
	}
	if len(masked) == 0 && len(denied) == 0 {
		return
	}
	attrs := []any{
		"identity", identity.Name,
		"role", identity.Role,
		"table", table,
	}
	if len(masked) > 0 {
		attrs = append(attrs, "masked_columns", masked)
	}
	if len(denied) > 0 {
		attrs = append(attrs, "denied_columns", denied)
	}
	a.logger.Info("column_policy_applied", attrs...)
}

// LogAuthFailure records authentication failures.
func (a *AuditLogger) LogAuthFailure(remoteAddr, path, reason string) {
	if a == nil {
		return
	}
	a.logger.Warn("auth_failure",
		"remote_addr", remoteAddr,
		"path", path,
		"reason", reason,
	)
}
