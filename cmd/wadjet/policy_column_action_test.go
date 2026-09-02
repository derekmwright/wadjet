package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/auth"
	"github.com/derekmwright/wadjet/internal/config"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// TestConfigWithUnknownColumnActionIsRefused is the load half of #802's gate.
//
// `columns:` maps a column to an ACTION. Before the fix the parser's default
// arm returned ColumnAllow, so the replacement-string spelling that every
// version of docs/security.md recommended — `src_ip: "***REDACTED***"` — was
// read as a full grant on the column an operator believed was masked. The
// config must now refuse to load, naming the column and the value.
func TestConfigWithUnknownColumnActionIsRefused(t *testing.T) {
	cfgPath := writeConfigFile(t, `
auth:
  enabled: true
  api_keys:
    - key: "reader-key"
      name: "reader"
      role: reader
  roles:
    - name: reader
      tables: ["*"]
      allow: [read]
  policies:
    - table: flow_logs
      role: reader
      columns:
        src_ip: "***REDACTED***"
`)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}

	provider, err := buildProviderFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatalf("a policy with an unrecognised column action must be refused; got provider %v", provider)
	}
	for _, want := range []string{"src_ip", "***REDACTED***", "flow_logs", "reader"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("refusal %q does not name %q", err, want)
		}
	}
}

// TestConfiguredColumnPoliciesMaskAndDenyTheValuesAClientReceives is the
// enforcement half of #802's gate: mask and deny, read from a YAML file
// through the shipped loader, asserted on the VALUES that come back from a
// query — not on the parsed policy struct, which is what the old unit test
// checked while the shipped ABAC path silently dropped the same actions.
func TestConfiguredColumnPoliciesMaskAndDenyTheValuesAClientReceives(t *testing.T) {
	ctx := context.Background()

	cfgPath := writeConfigFile(t, `
auth:
  enabled: true
  api_keys:
    - key: "reader-key"
      name: "reader"
      role: reader
    - key: "admin-key"
      name: "admin"
      role: admin
  roles:
    - name: reader
      tables: ["*"]
      allow: [read]
    - name: admin
      tables: ["*"]
      allow: [read, write, admin]
  policies:
    - table: flow_logs
      role: reader
      columns:
        src_ip: mask
        payload: deny
        note: DENY          # the vocabulary is case-insensitive on both paths
`)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("loading config: %v", err)
	}
	provider, err := buildProviderFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("building provider: %v", err)
	}

	db, err := wadjet.Open(ctx, wadjet.Config{
		Store:        objstore.NewMemStore(),
		Bucket:       "test",
		AuthProvider: provider,
	})
	if err != nil {
		t.Fatal(err)
	}

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "src_ip", Type: parquet.TypeString},
		{Name: "payload", Type: parquet.TypeString},
		{Name: "note", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "flow_logs", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int64(1), "src_ip": "10.1.2.3", "payload": "secret-one", "note": "note-one"},
		{"id": int64(2), "src_ip": "8.8.8.8", "payload": "secret-two", "note": "note-two"},
	}
	ing := db.NewIngester("flow_logs", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	query := func(t *testing.T, key, sql string) []map[string]any {
		t.Helper()
		id, err := provider.Authenticator().AuthenticateToken(key)
		if err != nil {
			t.Fatalf("authenticating %q: %v", key, err)
		}
		res, err := db.Query(auth.ContextWithIdentity(ctx, id), sql)
		if err != nil {
			t.Fatalf("query as %s: %v", id.Role, err)
		}
		return res.Rows
	}

	const star = "SELECT * FROM flow_logs ORDER BY id"
	const named = "SELECT id, src_ip, payload, note FROM flow_logs ORDER BY id"

	// The role with no policy sees the raw values — the control that proves
	// the fixture's values can tell a mask from a grant.
	for _, sql := range []string{star, named} {
		admin := query(t, "admin-key", sql)
		if len(admin) != 2 {
			t.Fatalf("admin %q: expected 2 rows, got %d", sql, len(admin))
		}
		if got := admin[0]["src_ip"]; got != "10.1.2.3" {
			t.Errorf("admin %q: src_ip = %v, want 10.1.2.3", sql, got)
		}
		if got := admin[0]["payload"]; got != "secret-one" {
			t.Errorf("admin %q: payload = %v, want secret-one", sql, got)
		}
		if got := admin[0]["note"]; got != "note-one" {
			t.Errorf("admin %q: note = %v, want note-one", sql, got)
		}
	}

	// SELECT *: the mask replaces the value and the denied column is gone.
	for i, row := range query(t, "reader-key", star) {
		if got, ok := row["src_ip"]; !ok || got != "***" {
			t.Errorf("reader row %d src_ip = %v (present=%v), want %q — `src_ip: mask` reached no query", i, got, ok, "***")
		}
		if got, ok := row["payload"]; ok {
			t.Errorf("reader row %d still carries payload = %v; `payload: deny` reached no query", i, got)
		}
		if got, ok := row["note"]; ok {
			t.Errorf("reader row %d still carries note = %v; `note: DENY` reached no query", i, got)
		}
	}

	// Naming the denied column explicitly must not hand back its value. The
	// security projection drops it, so the outer reference resolves to NULL
	// rather than being rejected — that disposition is pre-existing and not
	// what this issue is about, but the VALUE must never survive.
	for i, row := range query(t, "reader-key", named) {
		if got, ok := row["src_ip"]; !ok || got != "***" {
			t.Errorf("reader row %d src_ip = %v (present=%v), want %q — `src_ip: mask` reached no query", i, got, ok, "***")
		}
		if got := row["payload"]; got != nil {
			t.Errorf("reader row %d payload = %v, want no value; `payload: deny` reached no query", i, got)
		}
		if got := row["note"]; got != nil {
			t.Errorf("reader row %d note = %v, want no value; `note: DENY` reached no query", i, got)
		}
	}
}

// TestSecurityDocCellPolicyExampleLoads reads the cell-level policy YAML out
// of docs/security.md and puts it through the shipped loader. It gates the
// DOC: the example that shipped for twelve releases spelled the action as a
// replacement string, which now refuses to load, so an edit back to that
// spelling fails here rather than in an operator's deployment.
func TestSecurityDocCellPolicyExampleLoads(t *testing.T) {
	const docPath = "../../docs/security.md"
	doc, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("reading %s: %v", docPath, err)
	}
	block := yamlBlockAfter(t, string(doc), "## Cell-Level Policies")

	// The doc's fragment is a `policies:` block; a policy only reaches
	// enforcement alongside the roles it names, so the scaffolding is added
	// here and the POLICIES come verbatim from the doc.
	cfgPath := writeConfigFile(t, block+`
  enabled: true
  api_keys:
    - key: "reader-key"
      name: "reader"
      role: reader
  roles:
    - name: reader
      tables: ["*"]
      allow: [read]
    - name: netops
      tables: ["*"]
      allow: [read]
`)

	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatalf("the docs/security.md cell-policy example does not load: %v", err)
	}
	if len(cfg.Auth.Policies) == 0 {
		t.Fatal("extracted no policies from the doc block — the extractor missed the example")
	}
	if _, err := buildProviderFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatalf("the docs/security.md cell-policy example is refused by the loader: %v", err)
	}

	// And it must actually MASK: a doc example that loads but grants is the
	// defect this issue is about.
	ps, err := auth.ParsePolicies(buildPolicyConfigs(cfg.Auth.Policies))
	if err != nil {
		t.Fatalf("parsing the doc's policies: %v", err)
	}
	p := ps.Lookup("flow_logs", "reader")
	if p == nil {
		t.Fatal("the doc example no longer carries a flow_logs/reader policy")
	}
	if p.Columns["src_ip"] != auth.ColumnMask {
		t.Errorf("doc example: flow_logs.src_ip is %d, want ColumnMask (%d)", p.Columns["src_ip"], auth.ColumnMask)
	}
}

// yamlBlockAfter returns the first ```yaml fenced block following heading.
func yamlBlockAfter(t *testing.T, doc, heading string) string {
	t.Helper()
	idx := strings.Index(doc, heading)
	if idx < 0 {
		t.Fatalf("heading %q not found", heading)
	}
	rest := doc[idx:]
	open := strings.Index(rest, "```yaml\n")
	if open < 0 {
		t.Fatalf("no yaml block after %q", heading)
	}
	rest = rest[open+len("```yaml\n"):]
	end := strings.Index(rest, "```")
	if end < 0 {
		t.Fatalf("unterminated yaml block after %q", heading)
	}
	return strings.TrimRight(rest[:end], "\n")
}
