package coordinator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/alerts"
	"github.com/derekmwright/wadjet/internal/auth"
	sql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// handleCreateAlertSQL parses a CREATE ALERT statement and persists the AlertMeta.
func (c *Coordinator) handleCreateAlertSQL(ctx context.Context, sqlText string) error {
	if !c.alertsEnabled {
		return fmt.Errorf("alerts are disabled on this cluster; set --enable-alerts or WADJET_ENABLE_ALERTS=1")
	}
	// An alert is a persistent server-side job that runs arbitrary SQL on a
	// cadence under its creator's identity — a privileged management action.
	if err := auth.RequirePermission(c.authProvider, ctx, "admin"); err != nil {
		return err
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

	// Snapshot the creator's identity so the scheduler can run this alert
	// under it (definer's rights) on every tick — see alertEvalDecorator.
	snap := auth.SnapshotIdentity(ctx)
	m := catalog.AlertMeta{
		Name:            info.Name,
		QueryText:       info.QueryText,
		IntervalSeconds: int64(info.Interval / time.Second),
		WebhookURL:      info.WebhookURL,
		WebhookHeaders:  info.Headers,
		InsertIntoTable: info.InsertInto,
		Enabled:         true,
		CreatedAt:       time.Now().UTC(),
		CreatedBy:       snap.Name,
		CreatedByRole:   snap.Role,
		CreatedByMethod: snap.Method,
		CreatedByAttrs:  snap.Attributes,
	}
	return c.catalog.CreateAlert(ctx, m)
}

// handleDropAlertSQL parses DROP ALERT [IF EXISTS] and removes the entry.
func (c *Coordinator) handleDropAlertSQL(ctx context.Context, sqlText string) error {
	if !c.alertsEnabled {
		return fmt.Errorf("alerts are disabled on this cluster; set --enable-alerts or WADJET_ENABLE_ALERTS=1")
	}
	if err := auth.RequirePermission(c.authProvider, ctx, "admin"); err != nil {
		return err
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
	if err := auth.RequirePermission(c.authProvider, ctx, "admin"); err != nil {
		return err
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
	c.alertScheduler = alerts.NewScheduler(c.catalog, c.asSQLExecutor(), c.alertSinkFactory, c.alertEvalDecorator())
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

// alertEvalDecorator returns the scheduler's per-alert context decorator that
// runs each alert query under its creator's identity (definer's rights). When
// auth is disabled the context is untouched. Legacy alerts with no stored
// identity run fail-closed under ABAC (auth.StampDefiner) and are warned about
// once per process so operators know to recreate them.
func (c *Coordinator) alertEvalDecorator() alerts.EvalContextFunc {
	var warned sync.Map
	return func(ctx context.Context, m catalog.AlertMeta) context.Context {
		snap := auth.IdentitySnapshot{
			Name:       m.CreatedBy,
			Role:       m.CreatedByRole,
			Method:     m.CreatedByMethod,
			Attributes: m.CreatedByAttrs,
		}
		newCtx, attributed := auth.StampDefiner(ctx, c.authProvider, snap)
		if !attributed {
			if _, seen := warned.LoadOrStore(m.Name, true); !seen {
				c.logger.Warn("alert has no stored creator identity; running fail-closed under ABAC — recreate it to attribute a definer",
					"alert", m.Name)
			}
		}
		return newCtx
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
	rs, err := e.c.ExecuteSQL(ctx, sqlText)
	rs.Close() // result is discarded; release any spill-backed stream
	return err
}

// Query runs a SELECT and returns rows as []map[string]any.
// Rows are boxed batch-by-batch and only until limit is reached — alerts
// run unattended on every scheduled tick on a GOMEMLIMIT'd coordinator, so
// boxing the full result via rs.Rows() before truncating (the previous
// shape) charged every tick for the query's whole cardinality, firing or
// not. Truncation is detected from row counts, never by boxing the excess.
func (e *coordinatorExecutor) Query(ctx context.Context, sqlText string, limit int) ([]map[string]any, []alerts.ColumnMeta, int64, bool, error) {
	rs, err := e.c.ExecuteSQL(ctx, sqlText)
	if err != nil {
		rs.Close()
		return nil, nil, 0, false, err
	}
	boxed, truncated, err := boxRowsUpTo(ctx, rs.Stream(), limit)
	if err != nil {
		return nil, nil, 0, false, err
	}
	// Build column schema from the Columns list. Type information is not
	// carried through SQLResult at this time, so Type is left empty.
	schema := make([]alerts.ColumnMeta, 0, len(rs.Columns))
	for _, name := range rs.Columns {
		schema = append(schema, alerts.ColumnMeta{Name: name})
	}
	return boxed, schema, rs.TotalRows, truncated, nil
}

// boxRowsUpTo materializes stream rows into maps, stopping once limit rows
// are boxed (limit <= 0 means no limit). The excess is never boxed; the
// truncated flag reports whether any active rows were left behind. The
// stream is always closed before returning — once truncation is detected
// the remaining batches (and any spill scratch behind them) are released
// without being read.
func boxRowsUpTo(ctx context.Context, in BatchStream, limit int) ([]map[string]any, bool, error) {
	defer in.Close()
	var boxed []map[string]any
	for {
		b, err := in.Next(ctx)
		if err != nil {
			return nil, false, err
		}
		if b == nil {
			return boxed, false, nil
		}
		if limit > 0 && len(boxed) >= limit {
			if b.ActiveLen() > 0 {
				return boxed, true, nil
			}
			continue
		}
		rows := b.ToRows()
		if limit > 0 && len(boxed)+len(rows) > limit {
			boxed = append(boxed, rows[:limit-len(boxed)]...)
			// More active rows exist past the cap — truncated, and any
			// remaining batches need not be examined.
			return boxed, true, nil
		}
		boxed = append(boxed, rows...)
	}
}
