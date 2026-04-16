package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// alertSummary is the MCP-visible shape of an alert definition.
type alertSummary struct {
	Name            string `json:"name"`
	IntervalSeconds int64  `json:"interval_seconds"`
	Enabled         bool   `json:"enabled"`
	WebhookURL      string `json:"webhook_url,omitempty"`
	InsertIntoTable string `json:"insert_into_table,omitempty"`
	Query           string `json:"query"`
}

// handleListAlerts returns all CREATE ALERT definitions in the catalog.
func (s *Server) handleListAlerts(ctx context.Context) callToolResult {
	alerts, err := s.db.Catalog().ListAlerts(ctx)
	if err != nil {
		return errorResult("failed to list alerts: " + err.Error())
	}

	summaries := make([]alertSummary, len(alerts))
	for i, a := range alerts {
		summaries[i] = alertSummary{
			Name:            a.Name,
			IntervalSeconds: a.IntervalSeconds,
			Enabled:         a.Enabled,
			WebhookURL:      a.WebhookURL,
			InsertIntoTable: a.InsertIntoTable,
			Query:           a.QueryText,
		}
	}

	data, err := json.Marshal(map[string]any{"alerts": summaries})
	if err != nil {
		return errorResult("failed to marshal alerts: " + err.Error())
	}
	return textResult(string(data))
}

// handleDescribeAlert returns the full AlertMeta for a given alert plus its
// 10 most recent fires from alert_history (if the table exists).
func (s *Server) handleDescribeAlert(ctx context.Context, args map[string]any) callToolResult {
	name, _ := args["name"].(string)
	if name == "" {
		return errorResult("missing required argument: name")
	}

	meta, err := s.db.Catalog().GetAlert(ctx, name)
	if err != nil {
		return errorResult(fmt.Sprintf("alert %q: %s", name, err.Error()))
	}

	summary := alertSummary{
		Name:            meta.Name,
		IntervalSeconds: meta.IntervalSeconds,
		Enabled:         meta.Enabled,
		WebhookURL:      meta.WebhookURL,
		InsertIntoTable: meta.InsertIntoTable,
		Query:           meta.QueryText,
	}

	// Query recent fires from alert_history. Gracefully handle the table not
	// existing yet (errors map to an empty slice, not a tool error).
	var recentFires []map[string]any
	histSQL := fmt.Sprintf(
		"SELECT * FROM alert_history WHERE alert_name = '%s' ORDER BY fired_at DESC LIMIT 10",
		strings.ReplaceAll(name, "'", "''"),
	)
	result, queryErr := s.db.Query(ctx, histSQL)
	if queryErr == nil {
		recentFires = result.Rows
	}
	if recentFires == nil {
		recentFires = []map[string]any{}
	}

	out := map[string]any{
		"alert":  summary,
		"recent": recentFires,
	}
	data, err := json.Marshal(out)
	if err != nil {
		return errorResult("failed to marshal alert: " + err.Error())
	}
	return textResult(string(data))
}
