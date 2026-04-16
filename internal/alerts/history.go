package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/citc-tech/wadjet/internal/storage/catalog"
	"github.com/citc-tech/wadjet/internal/storage/parquet"
)

// HistoryTableName is the name of the system table holding alert fires.
const HistoryTableName = "alert_history"

// EnsureHistoryTable idempotently creates alert_history.
// Day-partitioned on partition_date (synthetic YYYY-MM-DD bucket).
func EnsureHistoryTable(ctx context.Context, cat *catalog.Catalog) error {
	if _, err := cat.GetTable(ctx, HistoryTableName); err == nil {
		return nil
	}
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "fired_at", Type: parquet.TypeTimestamp},
			{Name: "alert_name", Type: parquet.TypeString},
			{Name: "evaluated_at", Type: parquet.TypeTimestamp},
			{Name: "row_count", Type: parquet.TypeInt64},
			{Name: "truncated", Type: parquet.TypeBool},
			{Name: "match_snapshot", Type: parquet.TypeString},
			{Name: "delivery_status", Type: parquet.TypeString},
			{Name: "sink_results", Type: parquet.TypeString},
			{Name: "delivery_error", Type: parquet.TypeString},
			{Name: "partition_date", Type: parquet.TypeString},
		},
	}
	if err := cat.CreateTable(ctx, HistoryTableName, schema, []string{"partition_date"}); err != nil {
		return fmt.Errorf("creating %s: %w", HistoryTableName, err)
	}
	return nil
}

// SinkResult records per-sink delivery outcome.
type SinkResult struct {
	Sink  string `json:"sink"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// BuildHistoryInsertSQL constructs an INSERT INTO alert_history VALUES (...) statement.
// All string values are SQL-escaped (single-quote doubling) to handle embedded
// quotes in JSON payloads. Inputs are internal-only but escaping is defense-in-depth.
func BuildHistoryInsertSQL(fire AlertFire, results []SinkResult, now time.Time) (string, error) {
	snapshot, err := json.Marshal(fire.Rows)
	if err != nil {
		return "", err
	}
	sinkResultsJSON, err := json.Marshal(results)
	if err != nil {
		return "", err
	}
	status := "delivered"
	var firstErr string
	okCount := 0
	for _, r := range results {
		if r.OK {
			okCount++
		} else if firstErr == "" {
			firstErr = r.Error
		}
	}
	switch {
	case okCount == 0 && len(results) > 0:
		status = "failed"
	case okCount < len(results):
		status = "partial"
	}

	partitionDate := now.UTC().Format("2006-01-02")
	firedAt := now.UTC().Format(time.RFC3339Nano)
	evaluatedAt := fire.EvaluatedAt.UTC().Format(time.RFC3339Nano)

	return fmt.Sprintf(
		`INSERT INTO %s (fired_at, alert_name, evaluated_at, row_count, truncated, match_snapshot, delivery_status, sink_results, delivery_error, partition_date) VALUES ('%s', '%s', '%s', %d, %s, '%s', '%s', '%s', '%s', '%s')`,
		HistoryTableName,
		firedAt,
		sqlEscape(fire.AlertName),
		evaluatedAt,
		fire.RowCount,
		boolLit(fire.Truncated),
		sqlEscape(string(snapshot)),
		status,
		sqlEscape(string(sinkResultsJSON)),
		sqlEscape(firstErr),
		partitionDate,
	), nil
}

func sqlEscape(s string) string { return strings.ReplaceAll(s, "'", "''") }

func boolLit(b bool) string {
	if b {
		return "TRUE"
	}
	return "FALSE"
}
