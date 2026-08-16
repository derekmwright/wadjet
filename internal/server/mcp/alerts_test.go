package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// TestListAlertsTool seeds two alerts and verifies list_alerts returns them both.
func TestListAlertsTool(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	// Seed two alerts directly via the catalog.
	if err := db.Catalog().CreateAlert(ctx, catalog.AlertMeta{
		Name:            "high_latency",
		QueryText:       "SELECT * FROM flow_logs WHERE bytes_in > 1000",
		IntervalSeconds: 60,
		Enabled:         true,
	}); err != nil {
		t.Fatalf("seeding alert 1: %v", err)
	}
	if err := db.Catalog().CreateAlert(ctx, catalog.AlertMeta{
		Name:            "port_scan",
		QueryText:       "SELECT * FROM flow_logs WHERE dst_port < 1024",
		IntervalSeconds: 300,
		Enabled:         false,
		WebhookURL:      "https://example.com/hook",
	}); err != nil {
		t.Fatalf("seeding alert 2: %v", err)
	}

	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "list_alerts",
		"arguments": map[string]any{},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected RPC error: %v", resp.Error.Message)
	}

	// Unwrap callToolResult → text content → JSON
	data, _ := json.Marshal(resp.Result)
	var toolResult callToolResult
	if err := json.Unmarshal(data, &toolResult); err != nil {
		t.Fatalf("unmarshaling tool result: %v", err)
	}
	if toolResult.IsError {
		t.Fatalf("tool returned error: %v", toolResult.Content)
	}
	if len(toolResult.Content) == 0 {
		t.Fatal("no content blocks returned")
	}

	text := toolResult.Content[0].Text
	var payload struct {
		Alerts []alertSummary `json:"alerts"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		t.Fatalf("parsing list_alerts JSON: %v\nraw: %s", err, text)
	}

	if len(payload.Alerts) != 2 {
		t.Errorf("expected 2 alerts, got %d", len(payload.Alerts))
	}

	names := map[string]bool{}
	for _, a := range payload.Alerts {
		names[a.Name] = true
	}
	if !names["high_latency"] {
		t.Error("missing alert: high_latency")
	}
	if !names["port_scan"] {
		t.Error("missing alert: port_scan")
	}
}

// TestDescribeAlertTool seeds one alert and verifies describe_alert returns its name.
func TestDescribeAlertTool(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()

	if err := db.Catalog().CreateAlert(ctx, catalog.AlertMeta{
		Name:            "brute_force",
		QueryText:       "SELECT src_ip FROM flow_logs GROUP BY src_ip HAVING count(*) > 100",
		IntervalSeconds: 120,
		Enabled:         true,
		InsertIntoTable: "alert_history",
	}); err != nil {
		t.Fatalf("seeding alert: %v", err)
	}

	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "describe_alert",
		"arguments": map[string]any{"name": "brute_force"},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected RPC error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var toolResult callToolResult
	if err := json.Unmarshal(data, &toolResult); err != nil {
		t.Fatalf("unmarshaling tool result: %v", err)
	}
	if toolResult.IsError {
		t.Fatalf("tool returned error: %v", toolResult.Content)
	}
	if len(toolResult.Content) == 0 {
		t.Fatal("no content blocks returned")
	}

	text := toolResult.Content[0].Text
	if !strings.Contains(text, "brute_force") {
		t.Errorf("response JSON does not contain alert name 'brute_force': %s", text)
	}

	// Verify the response is well-formed JSON with the expected keys.
	var payload struct {
		Alert  alertSummary     `json:"alert"`
		Recent []map[string]any `json:"recent"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		t.Fatalf("parsing describe_alert JSON: %v\nraw: %s", err, text)
	}
	if payload.Alert.Name != "brute_force" {
		t.Errorf("alert name mismatch: %s", payload.Alert.Name)
	}
	if payload.Alert.InsertIntoTable != "alert_history" {
		t.Errorf("insert_into_table mismatch: %s", payload.Alert.InsertIntoTable)
	}
	// alert_history table doesn't exist in the test DB, so recent must be an empty slice.
	if payload.Recent == nil {
		t.Error("recent field should be an empty slice, not null")
	}
}

// TestResourcesListIncludesAlertsDocs verifies resources/list returns the alerts doc entry.
func TestResourcesListIncludesAlertsDocs(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "resources/list", nil, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected RPC error: %v", resp.Error.Message)
	}

	body, _ := json.Marshal(resp.Result)
	if !strings.Contains(string(body), "wadjet://docs/alerts") {
		t.Errorf("resources/list missing alerts doc: %s", body)
	}
}

// TestResourcesReadAlertsDoc verifies resources/read returns the embedded markdown.
func TestResourcesReadAlertsDoc(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "resources/read", map[string]string{"uri": "wadjet://docs/alerts"}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected RPC error: %v", resp.Error.Message)
	}

	body, _ := json.Marshal(resp.Result)
	if !strings.Contains(string(body), "CREATE ALERT") {
		t.Errorf("resources/read did not return alerts doc: %s", body)
	}
}

// TestResourcesReadUnknownURI verifies resources/read returns an error for unknown URIs.
func TestResourcesReadUnknownURI(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "resources/read", map[string]string{"uri": "wadjet://docs/nonexistent"}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error == nil {
		t.Fatal("expected error for unknown resource URI")
	}
	if resp.Error.Code != -32602 {
		t.Errorf("expected error code -32602, got %d", resp.Error.Code)
	}
}

// TestListFunctionsDDLCapabilities verifies list_functions returns the ddl_capabilities section.
func TestListFunctionsDDLCapabilities(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "tools/call", map[string]any{
		"name":      "list_functions",
		"arguments": map[string]any{},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected RPC error: %v", resp.Error.Message)
	}

	data, _ := json.Marshal(resp.Result)
	var toolResult callToolResult
	if err := json.Unmarshal(data, &toolResult); err != nil {
		t.Fatalf("unmarshaling tool result: %v", err)
	}
	if toolResult.IsError {
		t.Fatalf("tool returned error: %v", toolResult.Content)
	}
	if len(toolResult.Content) == 0 {
		t.Fatal("no content blocks returned")
	}

	text := toolResult.Content[0].Text
	var payload struct {
		DDLCapabilities []map[string]string `json:"ddl_capabilities"`
	}
	if err := json.Unmarshal([]byte(text), &payload); err != nil {
		t.Fatalf("parsing list_functions JSON: %v\nraw: %s", err, text)
	}
	if len(payload.DDLCapabilities) == 0 {
		t.Fatalf("ddl_capabilities missing from list_functions response: %s", text)
	}
	names := map[string]bool{}
	for _, cap := range payload.DDLCapabilities {
		names[cap["name"]] = true
	}
	if !names["CREATE ALERT"] {
		t.Errorf("ddl_capabilities missing CREATE ALERT entry")
	}
	if !names["DROP ALERT"] {
		t.Errorf("ddl_capabilities missing DROP ALERT entry")
	}
}

// TestInitializeAdvertisesAlertCapability verifies the initialize handshake
// includes the "wadjet.ddl.create_alert" capability key.
func TestInitializeAdvertisesAlertCapability(t *testing.T) {
	db := setupTestDB(t)
	srv := NewServer(db, nil)

	in := &bytes.Buffer{}
	sendRPC(t, in, "initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "test", "version": "1.0"},
	}, 1)

	out := runStdio(t, srv, in)
	resp := readResponse(t, out)
	if resp.Error != nil {
		t.Fatalf("unexpected error: %v", resp.Error.Message)
	}

	// Marshal the result to JSON and check for the capability key.
	resultJSON, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatalf("marshaling result: %v", err)
	}

	// The result JSON has shape: {"wadjet":{"ddl.create_alert":{...}}, ...}
	// We verify both the outer "wadjet" key and the inner "ddl.create_alert" key are present.
	if !strings.Contains(string(resultJSON), `"wadjet"`) {
		t.Errorf("initialize result missing 'wadjet' capability map:\n%s", string(resultJSON))
	}
	if !strings.Contains(string(resultJSON), `"ddl.create_alert"`) {
		t.Errorf("initialize result does not advertise 'ddl.create_alert' under wadjet:\n%s", string(resultJSON))
	}
}
