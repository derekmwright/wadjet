package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// `wadjet mcp` end to end, through the BUILT binary, because both halves of
// what it asserts are process-level.
//
// The command opened its DB with `wadjet.Open` and NO `MetaKV`, so its catalog
// was `catalog.NewWithStore`'s in-memory map: empty on every invocation, while
// `query`, `shell`, `create-table` and `drop-table` share one persisted
// catalog (#842). An agent therefore saw no table the operator had created.
//
// Since a policy set binds to the catalog it is attached to (#882), that had a
// second consequence: a config carrying ANY policy could not start this
// command at all — the policy named a relation, the catalog it bound against
// held nothing, and the bind refused. `docs/security.md` tells an operator to
// "create the relation first, then add the policy", and on this door that
// remedy could not be followed: the relation was never in the catalog this
// command bound against.
//
// So the gate is one run: create a table with one process, then start the MCP
// server with an auth config whose policy names that table, and ask it for the
// table. It must START (the bind resolved the name), and it must ENFORCE (the
// denied column is not in the answer).

// mcpRun runs one command like e2eRun but with stdin attached, and returns
// stdout and stderr separately — the MCP protocol is on stdout and the
// server's log lines are on stderr, so a combined capture cannot be parsed.
func mcpRun(t *testing.T, bin, root, stdin string, args ...string) (string, string, error) {
	t.Helper()
	full := append([]string{
		"--storage-type=file",
		"--data-dir=" + filepath.Join(root, "data"),
		"--bucket=wadjet",
	}, args...)
	cmd := exec.Command(bin, full...)
	home := filepath.Join(root, "home")
	if err := os.MkdirAll(home, 0o755); err != nil {
		t.Fatal(err)
	}
	cmd.Env = append(os.Environ(), "HOME="+home)
	cmd.Stdin = strings.NewReader(stdin)
	var out, errb strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errb
	err := cmd.Run()
	return out.String(), errb.String(), err
}

func TestMCPSharesTheCatalogAndEnforcesItsPolicy(t *testing.T) {
	bin := e2eBin(t)
	root := t.TempDir()

	// One process creates the relation the policy will name.
	if out, err := e2eRun(t, bin, root, "create-table",
		"CREATE TABLE mcp_flows (id BIGINT, src_ip VARCHAR, secret VARCHAR)"); err != nil {
		t.Fatalf("create-table: %v\n%s", err, out)
	}

	cfgPath := filepath.Join(root, "auth.yaml")
	if err := os.WriteFile(cfgPath, []byte(`
auth:
  enabled: true
  api_keys:
    - key: "agent-key"
      name: "agent"
      role: analyst
  roles:
    - name: analyst
      tables: ["*"]
      allow: [read]
  policies:
    - table: mcp_flows
      role: analyst
      columns:
        secret: deny
`), 0o600); err != nil {
		t.Fatal(err)
	}

	// One JSON-RPC line per request; the transport is line-delimited.
	req := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}` + "\n" +
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"query",` +
		`"arguments":{"sql":"SELECT * FROM mcp_flows"}}}` + "\n"

	stdout, stderr, err := mcpRun(t, bin, root, req,
		"mcp", "--config", cfgPath, "--api-key", "agent-key")
	if err != nil {
		t.Fatalf("`wadjet mcp` with a policy naming an EXISTING relation failed to run: %v\n"+
			"stdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	// The refusal this door used to hit: the policy named a relation its own
	// (empty, private) catalog did not hold.
	if strings.Contains(stderr, "names no relation in the catalog") ||
		strings.Contains(stdout, "names no relation in the catalog") {
		t.Fatalf("`wadjet mcp` refused to start against a catalog that HOLDS the relation "+
			"its policy names — it is not reading the shared catalog:\nstderr:\n%s", stderr)
	}

	// It answered, and it saw the table another process created. The tool's
	// payload is JSON inside a JSON string, so the quotes are escaped there:
	// match the bare words rather than a literal `"columns"`.
	if !strings.Contains(stdout, "columns") {
		t.Fatalf("the query tool returned no result:\nstdout:\n%s\nstderr:\n%s", stdout, stderr)
	}
	if strings.Contains(stdout, "does not exist") {
		t.Fatalf("`wadjet mcp` does not see a table `create-table` wrote (#842):\n%s", stdout)
	}
	// And the policy is enforced: the denied column is not in the answer.
	if strings.Contains(stdout, "secret") {
		t.Fatalf("the MCP door returned the DENIED column:\n%s", stdout)
	}
	if !strings.Contains(stdout, "src_ip") {
		t.Fatalf("the answer carries no columns at all, so the deny above proves nothing:\n%s",
			stdout)
	}
}
