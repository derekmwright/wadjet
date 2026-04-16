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
