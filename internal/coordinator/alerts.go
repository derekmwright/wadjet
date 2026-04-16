package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/citc-tech/wadjet/internal/alerts"
	"github.com/citc-tech/wadjet/internal/auth"
	sql "github.com/citc-tech/wadjet/internal/planner/sql"
	"github.com/citc-tech/wadjet/internal/storage/catalog"
)

// handleCreateAlertSQL parses a CREATE ALERT statement and persists the AlertMeta.
func (c *Coordinator) handleCreateAlertSQL(ctx context.Context, sqlText string) error {
	if !c.alertsEnabled {
		return fmt.Errorf("alerts are disabled on this cluster; set --enable-alerts or WADJET_ENABLE_ALERTS=1")
	}
	pq, err := sql.Parse(sqlText)
	if err != nil {
		return err
	}
	if pq.Type != sql.QueryCreateAlert || pq.CreateAlert == nil {
		return fmt.Errorf("not a CREATE ALERT statement")
	}
	info := pq.CreateAlert

	if _, err := sql.Parse(info.QueryText); err != nil {
		return fmt.Errorf("invalid alert query: %w", err)
	}
	if err := alerts.EnsureHistoryTable(ctx, c.catalog); err != nil {
		return fmt.Errorf("ensuring alert_history: %w", err)
	}

	// Validate InsertInto table exists (unless it's alert_history, which we just ensured).
	if info.InsertInto != "" && info.InsertInto != alerts.HistoryTableName {
		if _, err := c.catalog.GetTable(ctx, info.InsertInto); err != nil {
			return fmt.Errorf("CREATE ALERT: INSERT INTO table %q not found", info.InsertInto)
		}
	}

	m := catalog.AlertMeta{
		Name:            info.Name,
		QueryText:       info.QueryText,
		IntervalSeconds: int64(info.Interval / time.Second),
		WebhookURL:      info.WebhookURL,
		WebhookHeaders:  info.Headers,
		InsertIntoTable: info.InsertInto,
		Enabled:         true,
		CreatedAt:       time.Now().UTC(),
		CreatedBy:       identityFromCtx(ctx),
	}
	return c.catalog.CreateAlert(ctx, m)
}

// handleDropAlertSQL parses DROP ALERT [IF EXISTS] and removes the entry.
func (c *Coordinator) handleDropAlertSQL(ctx context.Context, sqlText string) error {
	if !c.alertsEnabled {
		return fmt.Errorf("alerts are disabled on this cluster; set --enable-alerts or WADJET_ENABLE_ALERTS=1")
	}
	pq, err := sql.Parse(sqlText)
	if err != nil {
		return err
	}
	if pq.Type != sql.QueryDropAlert || pq.DropAlert == nil {
		return fmt.Errorf("not a DROP ALERT statement")
	}
	if _, err := c.catalog.GetAlert(ctx, pq.DropAlert.Name); err != nil {
		if pq.DropAlert.IfExists {
			return nil
		}
		return err
	}
	return c.catalog.DropAlert(ctx, pq.DropAlert.Name)
}

// handleAlterAlertSQL parses ALTER ALERT and toggles Enabled.
func (c *Coordinator) handleAlterAlertSQL(ctx context.Context, sqlText string) error {
	if !c.alertsEnabled {
		return fmt.Errorf("alerts are disabled on this cluster; set --enable-alerts or WADJET_ENABLE_ALERTS=1")
	}
	pq, err := sql.Parse(sqlText)
	if err != nil {
		return err
	}
	if pq.Type != sql.QueryAlterAlert || pq.AlterAlert == nil {
		return fmt.Errorf("not an ALTER ALERT statement")
	}
	return c.catalog.SetAlertEnabled(ctx, pq.AlterAlert.Name, pq.AlterAlert.Enable)
}

// identityFromCtx returns a string identity from ctx using the auth package.
func identityFromCtx(ctx context.Context) string {
	if id := auth.IdentityFromContext(ctx); id != nil {
		return id.Name
	}
	return ""
}

// SetAlertsEnabled toggles the feature flag. When false, StartAlertScheduler
// is a no-op. DDL-level rejection is added in Task 13.
func (c *Coordinator) SetAlertsEnabled(on bool) {
	c.alertsEnabled = on
	if !on {
		c.StopAlertScheduler()
	}
}

// StartAlertScheduler begins scheduling alerts. Must only be called while this
// coordinator holds leadership. Safe to call multiple times; a running scheduler
// is stopped and replaced.
func (c *Coordinator) StartAlertScheduler(parent context.Context) {
	if !c.alertsEnabled {
		return
	}
	c.StopAlertScheduler()
	ctx, cancel := context.WithCancel(parent)
	c.alertSchedulerCancel = cancel
	c.alertScheduler = alerts.NewScheduler(c.catalog, c.asSQLExecutor(), c.alertSinkFactory)
	c.alertScheduler.Start(ctx)
}

// StopAlertScheduler cancels the running scheduler and waits for it to exit.
// Safe to call when no scheduler is running.
func (c *Coordinator) StopAlertScheduler() {
	if c.alertSchedulerCancel != nil {
		c.alertSchedulerCancel()
		c.alertSchedulerCancel = nil
	}
	if c.alertScheduler != nil {
		c.alertScheduler.Wait()
		c.alertScheduler = nil
	}
}

// alertSinkFactory returns the sinks configured for the alert.
// WebhookSink first (if URL set), TableSink last (reads prior results).
func (c *Coordinator) alertSinkFactory(m catalog.AlertMeta) []alerts.AlertSink {
	var sinks []alerts.AlertSink
	if m.WebhookURL != "" {
		sinks = append(sinks, alerts.NewWebhookSink(m.Name, m.WebhookURL, m.WebhookHeaders, 10*time.Second))
	}
	if m.InsertIntoTable != "" {
		sinks = append(sinks, &alerts.TableSink{Executor: c.asSQLExecutor()})
	}
	return sinks
}

// asSQLExecutor adapts the coordinator to alerts.SQLExecutor.
func (c *Coordinator) asSQLExecutor() alerts.SQLExecutor {
	return &coordinatorExecutor{c: c}
}

// coordinatorExecutor bridges alerts.SQLExecutor onto *Coordinator.
type coordinatorExecutor struct{ c *Coordinator }

func (e *coordinatorExecutor) Execute(ctx context.Context, sqlText string) error {
	_, err := e.c.ExecuteSQL(ctx, sqlText)
	return err
}

// Query runs a SELECT and returns rows as []map[string]any.
// SQLResult.Rows() materializes RecordBatch slices into row maps; we cap at
// limit here but count all rows for the total. This is adequate for the
// threshold-style queries alerts use (low cardinality).
func (e *coordinatorExecutor) Query(ctx context.Context, sqlText string, limit int) ([]map[string]any, []alerts.ColumnMeta, int64, bool, error) {
	rs, err := e.c.ExecuteSQL(ctx, sqlText)
	if err != nil {
		return nil, nil, 0, false, err
	}
	all := rs.Rows()
	total := rs.TotalRows
	// Build column schema from the Columns list. Type information is not
	// carried through SQLResult at this time, so Type is left empty.
	schema := make([]alerts.ColumnMeta, 0, len(rs.Columns))
	for _, name := range rs.Columns {
		schema = append(schema, alerts.ColumnMeta{Name: name})
	}
	truncated := false
	if limit > 0 && len(all) > limit {
		all = all[:limit]
		truncated = true
	}
	return all, schema, total, truncated, nil
}
