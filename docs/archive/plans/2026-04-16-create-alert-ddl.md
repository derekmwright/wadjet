# CREATE ALERT DDL v1 — Implementation Plan


**Goal:** Ship SQL-native detection alerts — `CREATE ALERT name AS SELECT ... EVERY N MINUTES [WEBHOOK ...] [INSERT INTO ...]` — with stateless polling, webhook + table sinks, leader-only scheduler, feature-flagged, and discoverable via MCP and `information_schema`.

**Architecture:** New `internal/alerts/` package with `AlertSink` interface, `WebhookSink`, `TableSink`, and `Scheduler`. Parser + catalog extensions for the new DDL. Coordinator owns scheduler lifecycle via existing leader-election. MCP and pgwire surfaces extended for discoverability.

**Tech Stack:** Go stdlib (`net/http`, `net/http/httptest`), existing `internal/planner/sql/` hand-rolled parser, existing `internal/storage/catalog/` NATS KV (MemKV in tests), existing Prometheus metrics registry, existing MCP + pgwire servers.

**Spec:** `docs/archive/specs/2026-04-16-create-alert-ddl-design.md`

---

## File structure

**New files:**
- `internal/alerts/types.go` — `AlertSink`, `AlertFire`, `ColumnMeta`, `SQLExecutor` interface
- `internal/alerts/webhook_sink.go` + `_test.go`
- `internal/alerts/table_sink.go` + `_test.go`
- `internal/alerts/history.go` + `_test.go` — alert_history table auto-create + row encoder
- `internal/alerts/scheduler.go` + `_test.go`
- `internal/alerts/metrics.go`
- `internal/alerts/integration_test.go`
- `internal/storage/catalog/alerts.go` + `alerts_test.go`
- `internal/server/mcp/alerts.go` + `alerts_test.go`
- `testdata/alerts/golden.sql`

**Modified:**
- `internal/planner/sql/lexer.go` — new keywords
- `internal/planner/sql/parser.go` — new AST nodes + parser functions, dispatch wiring
- `internal/planner/sql/parser_test.go`
- `internal/coordinator/coordinator.go` — DDL handlers + scheduler lifecycle on leader change
- `internal/coordinator/leader_alerts_test.go` (new)
- `internal/server/mcp/server.go` — register tools + initialize capability + resource
- `internal/server/pgwire/server.go` — `information_schema.alerts` interception
- `cmd/wadjet/main.go` — `--enable-alerts` flag

---

### Task 1: Lexer keywords

**Files:**
- Modify: `internal/planner/sql/lexer.go`

- [ ] **Step 1: Add keyword token constants**

In `internal/planner/sql/lexer.go`, inside the `const (...)` block for keywords (add after `TokenKWMatched` at line 146, just before the `Raw capture` comment):

```go
	// Alert DDL keywords
	TokenKWAlert
	TokenKWEvery
	TokenKWWebhook
	TokenKWHeaders
	TokenKWEnable
	TokenKWDisable
	TokenKWSeconds
	TokenKWMinutes
	TokenKWHours
```

- [ ] **Step 2: Register keywords in the lookup map**

In `internal/planner/sql/lexer.go`, in the `keywords` map (near line 154), add the entries (keep alphabetical-ish grouping with the existing style):

```go
	"ALERT":    TokenKWAlert,
	"EVERY":    TokenKWEvery,
	"WEBHOOK":  TokenKWWebhook,
	"HEADERS":  TokenKWHeaders,
	"ENABLE":   TokenKWEnable,
	"DISABLE":  TokenKWDisable,
	"SECONDS":  TokenKWSeconds,
	"MINUTES":  TokenKWMinutes,
	"HOURS":    TokenKWHours,
```

- [ ] **Step 3: Verify build**

Run: `go build ./internal/planner/sql/`
Expected: compiles cleanly.

- [ ] **Step 4: Commit**

```bash
git add internal/planner/sql/lexer.go
git commit -m "feat(planner): add lexer keywords for CREATE ALERT DDL"
```

---

### Task 2: CREATE ALERT AST + parser

**Files:**
- Modify: `internal/planner/sql/parser.go`
- Modify: `internal/planner/sql/parser_test.go`

- [ ] **Step 1: Write failing tests**

Append to `internal/planner/sql/parser_test.go`:

```go
func TestParseCreateAlertMinimal(t *testing.T) {
	sql := `CREATE ALERT my_alert AS SELECT 1 FROM t EVERY 5 MINUTES WEBHOOK 'https://x.example'`
	pq, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if pq.Type != QueryCreateAlert {
		t.Fatalf("type: want QueryCreateAlert, got %v", pq.Type)
	}
	if pq.CreateAlert == nil {
		t.Fatal("CreateAlert is nil")
	}
	if pq.CreateAlert.Name != "my_alert" {
		t.Errorf("name: want my_alert, got %q", pq.CreateAlert.Name)
	}
	if pq.CreateAlert.Interval != 5*time.Minute {
		t.Errorf("interval: want 5m, got %v", pq.CreateAlert.Interval)
	}
	if pq.CreateAlert.WebhookURL != "https://x.example" {
		t.Errorf("url: want https://x.example, got %q", pq.CreateAlert.WebhookURL)
	}
	if !strings.Contains(pq.CreateAlert.QueryText, "SELECT 1 FROM t") {
		t.Errorf("queryText missing SELECT: %q", pq.CreateAlert.QueryText)
	}
}

func TestParseCreateAlertAllOptions(t *testing.T) {
	sql := `CREATE ALERT a AS SELECT x FROM t EVERY 30 SECONDS WEBHOOK 'http://y.example' HEADERS { 'Authorization' = 'Token abc' } INSERT INTO alert_history`
	pq, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if pq.CreateAlert.Interval != 30*time.Second {
		t.Errorf("interval: want 30s, got %v", pq.CreateAlert.Interval)
	}
	if pq.CreateAlert.Headers["Authorization"] != "Token abc" {
		t.Errorf("header: want Token abc, got %q", pq.CreateAlert.Headers["Authorization"])
	}
	if pq.CreateAlert.InsertInto != "alert_history" {
		t.Errorf("insert into: want alert_history, got %q", pq.CreateAlert.InsertInto)
	}
}

func TestParseCreateAlertInsertOnly(t *testing.T) {
	sql := `CREATE ALERT a AS SELECT 1 FROM t EVERY 1 HOURS INSERT INTO h`
	pq, err := Parse(sql)
	if err != nil {
		t.Fatal(err)
	}
	if pq.CreateAlert.WebhookURL != "" {
		t.Errorf("url should be empty, got %q", pq.CreateAlert.WebhookURL)
	}
	if pq.CreateAlert.InsertInto != "h" {
		t.Errorf("insert into: want h, got %q", pq.CreateAlert.InsertInto)
	}
	if pq.CreateAlert.Interval != time.Hour {
		t.Errorf("interval: want 1h, got %v", pq.CreateAlert.Interval)
	}
}
```

Also add `"time"` and `"strings"` to the imports of `parser_test.go` if not already present.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestParseCreateAlert -v ./internal/planner/sql/`
Expected: FAIL — `QueryCreateAlert` / `CreateAlert` field undefined.

- [ ] **Step 3: Add AST types and QueryType value**

In `internal/planner/sql/parser.go`, inside the `ParsedQuery` struct (line 11-31), add the `CreateAlert` field. Replace:

```go
	Insert         *InsertInfo
```

with:

```go
	Insert         *InsertInfo
	CreateAlert    *CreateAlertInfo
	DropAlert      *DropAlertInfo
	AlterAlert     *AlterAlertInfo
```

Add the import `"time"` to the import block at the top of `parser.go` if not already present.

Add the new types after `InsertInfo` (before the `QueryType` declaration at line 156):

```go
// CreateAlertInfo holds details for a CREATE ALERT statement.
type CreateAlertInfo struct {
	Name       string
	QueryText  string            // raw SELECT text, re-parsed at eval time
	Interval   time.Duration     // validated >= 10s at parse time
	WebhookURL string            // "" if no webhook sink
	Headers    map[string]string
	InsertInto string            // "" if no table sink; at least one sink required
}

// DropAlertInfo holds details for a DROP ALERT statement.
type DropAlertInfo struct {
	Name     string
	IfExists bool
}

// AlterAlertInfo holds details for ALTER ALERT ... ENABLE|DISABLE.
type AlterAlertInfo struct {
	Name   string
	Enable bool // true = ENABLE, false = DISABLE
}
```

In the `QueryType` const block (line 158-176), add new values before `QueryUnsupported`:

```go
	QueryCreateAlert
	QueryDropAlert
	QueryAlterAlert
	QueryUnsupported
```

- [ ] **Step 4: Wire dispatch in `lexParseCreate`**

In `internal/planner/sql/parser.go`, inside `lexParseCreate` (line 459), after the `VIEW` dispatch block (line 479-480), before the `FUNCTION` check, add:

```go
	if tok.typ == TokenKWAlert {
		return lexParseCreateAlert(sql, l)
	}
```

Also update the error message on line 483 to mention ALERT:

```go
		return nil, fmt.Errorf("expected TABLE, VIEW, FUNCTION, or ALERT after CREATE")
```

- [ ] **Step 5: Implement `lexParseCreateAlert`**

Append to `internal/planner/sql/parser.go` (after `lexParseCreateTable` ends, near the end of the file):

```go
// lexParseCreateAlert handles:
//   CREATE ALERT <name> AS <SELECT ...> EVERY <N> {SECONDS|MINUTES|HOURS}
//     [WEBHOOK '<url>' [HEADERS { 'K' = 'V', ... }]]
//     [INSERT INTO <table>]
// CREATE ALERT has already been consumed.
func lexParseCreateAlert(sql string, l *lexer) (*ParsedQuery, error) {
	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("CREATE ALERT: alert name is required")
	}
	name := nameTok.val

	asTok := l.nextToken()
	if asTok.typ != TokenKWAs {
		return nil, fmt.Errorf("CREATE ALERT: expected AS after alert name")
	}

	// Scan SELECT body up to the top-level EVERY keyword using the raw input
	// (preserves whitespace/punctuation which the token stream throws away).
	rest := l.rest()
	before, after, ok := splitAtTopLevelKeyword(rest, "EVERY")
	if !ok {
		return nil, fmt.Errorf("CREATE ALERT: expected EVERY after SELECT body; example: CREATE ALERT x AS SELECT ... EVERY 5 MINUTES WEBHOOK 'https://...'")
	}
	queryText := strings.TrimSpace(before)
	if queryText == "" {
		return nil, fmt.Errorf("CREATE ALERT: SELECT body is required")
	}
	// Rebuild the lexer starting at EVERY so the rest of this function can
	// consume tokens as usual.
	l = newLexer(after)
	everyTok := l.nextToken()
	if everyTok.typ != TokenKWEvery {
		return nil, fmt.Errorf("CREATE ALERT: expected EVERY, got %q", everyTok.val)
	}

	// EVERY consumed; now parse: <N> <unit>
	nTok := l.nextToken()
	if nTok.typ != TokenNumber {
		return nil, fmt.Errorf("CREATE ALERT: expected number after EVERY; example: EVERY 5 MINUTES")
	}
	n, err := strconv.ParseInt(nTok.val, 10, 64)
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("CREATE ALERT: interval must be a positive integer, got %q", nTok.val)
	}
	unitTok := l.nextToken()
	var interval time.Duration
	switch unitTok.typ {
	case TokenKWSeconds:
		interval = time.Duration(n) * time.Second
	case TokenKWMinutes:
		interval = time.Duration(n) * time.Minute
	case TokenKWHours:
		interval = time.Duration(n) * time.Hour
	default:
		return nil, fmt.Errorf("CREATE ALERT: expected SECONDS|MINUTES|HOURS, got %q", unitTok.val)
	}
	if interval < 10*time.Second {
		return nil, fmt.Errorf("CREATE ALERT: interval must be >= 10 seconds, got %v", interval)
	}

	info := &CreateAlertInfo{
		Name:      name,
		QueryText: queryText,
		Interval:  interval,
	}

	// Optional sinks: WEBHOOK, INSERT INTO. Order WEBHOOK-first allowed; also INSERT-first.
	for {
		peek := l.peekToken()
		switch peek.typ {
		case TokenKWWebhook:
			l.nextToken() // consume WEBHOOK
			urlTok := l.nextToken()
			if urlTok.typ != TokenString {
				return nil, fmt.Errorf("CREATE ALERT: WEBHOOK expects a string URL literal")
			}
			u, err := url.Parse(urlTok.val)
			if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
				return nil, fmt.Errorf("CREATE ALERT: WEBHOOK URL must be http:// or https://, got %q", urlTok.val)
			}
			info.WebhookURL = urlTok.val

			// Optional HEADERS { 'K' = 'V', ... }
			if l.peekToken().typ == TokenKWHeaders {
				l.nextToken() // consume HEADERS
				if l.nextToken().typ != TokenLBrace {
					return nil, fmt.Errorf("CREATE ALERT: expected '{' after HEADERS")
				}
				info.Headers = make(map[string]string)
				for {
					if l.peekToken().typ == TokenRBrace {
						l.nextToken()
						break
					}
					if len(info.Headers) > 0 {
						if l.nextToken().typ != TokenComma {
							return nil, fmt.Errorf("CREATE ALERT: expected ',' between headers")
						}
					}
					keyTok := l.nextToken()
					if keyTok.typ != TokenString {
						return nil, fmt.Errorf("CREATE ALERT: header key must be a string literal")
					}
					if l.nextToken().typ != TokenEq {
						return nil, fmt.Errorf("CREATE ALERT: expected '=' in header pair")
					}
					valTok := l.nextToken()
					if valTok.typ != TokenString {
						return nil, fmt.Errorf("CREATE ALERT: header value must be a string literal")
					}
					info.Headers[keyTok.val] = valTok.val
				}
			}
		case TokenKWInsert:
			l.nextToken() // consume INSERT
			if l.nextToken().typ != TokenKWInto {
				return nil, fmt.Errorf("CREATE ALERT: expected INTO after INSERT")
			}
			tTok := l.nextToken()
			if tTok.typ != TokenIdent {
				return nil, fmt.Errorf("CREATE ALERT: expected table name after INSERT INTO")
			}
			info.InsertInto = tTok.val
		case TokenEOF:
			goto doneSinks
		default:
			return nil, fmt.Errorf("CREATE ALERT: unexpected token %q, expected WEBHOOK or INSERT INTO", peek.val)
		}
	}
doneSinks:

	if info.WebhookURL == "" && info.InsertInto == "" {
		return nil, fmt.Errorf("CREATE ALERT: at least one sink (WEBHOOK or INSERT INTO) is required")
	}
	if !isValidIdent(name) {
		return nil, fmt.Errorf("CREATE ALERT: invalid alert name %q (must match [a-zA-Z_][a-zA-Z0-9_]*, len<=128)", name)
	}

	return &ParsedQuery{
		Type:        QueryCreateAlert,
		SQL:         sql,
		CreateAlert: info,
	}, nil
}

// isValidIdent reports whether s is a valid identifier (first char letter/_, rest alnum/_, len<=128).
func isValidIdent(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for i, r := range s {
		first := i == 0
		isLetter := (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || r == '_'
		isDigit := r >= '0' && r <= '9'
		if first && !isLetter {
			return false
		}
		if !first && !isLetter && !isDigit {
			return false
		}
	}
	return true
}

// splitAtTopLevelKeyword scans s left-to-right and returns the split point
// at the first occurrence of kw (case-insensitive) that is (a) at paren-depth
// zero, (b) outside any single-quoted string literal, and (c) bounded by
// non-identifier characters on both sides. Returns before (text preceding kw)
// and after (text starting at kw, kw inclusive). ok=false if not found.
func splitAtTopLevelKeyword(s, kw string) (before, after string, ok bool) {
	depth := 0
	inString := false
	kwLen := len(kw)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			if c == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++ // doubled quote, skip
					continue
				}
				inString = false
			}
			continue
		}
		switch c {
		case '\'':
			inString = true
			continue
		case '(':
			depth++
			continue
		case ')':
			depth--
			continue
		}
		if depth != 0 {
			continue
		}
		if i+kwLen > len(s) {
			break
		}
		if !strings.EqualFold(s[i:i+kwLen], kw) {
			continue
		}
		if !isKWBoundary(s, i, kwLen) {
			continue
		}
		return s[:i], s[i:], true
	}
	return "", "", false
}

// isKWBoundary reports whether s[start:start+n] is bounded on both sides
// by characters that cannot appear in an identifier (or is at the edge).
func isKWBoundary(s string, start, n int) bool {
	if start > 0 {
		p := s[start-1]
		if (p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z') || (p >= '0' && p <= '9') || p == '_' {
			return false
		}
	}
	end := start + n
	if end < len(s) {
		c := s[end]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			return false
		}
	}
	return true
}
```

Add imports to `parser.go` if missing: `"net/url"`. (`strconv`, `strings` already present.)

Also add `TokenLBrace` and `TokenRBrace` to `lexer.go` if not already there. Check with:

```bash
grep -n "TokenLBrace\|TokenRBrace" internal/planner/sql/lexer.go
```

If missing, add to the punctuation token constants (near line 30) and to the single-char case in the lexer's `nextToken` / `scanToken` (there's an existing block for `{` / `}` handling; look for `case '{'` in the file, add if absent):

```go
	TokenLBrace    // {
	TokenRBrace    // }
```

And in the lexer's character dispatch (look for the `case '['` / `case ']'` block):

```go
	case '{':
		l.advance()
		return token{typ: TokenLBrace, val: "{"}
	case '}':
		l.advance()
		return token{typ: TokenRBrace, val: "}"}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `go test -run TestParseCreateAlert -v ./internal/planner/sql/`
Expected: 3 tests PASS.

- [ ] **Step 7: Commit**

```bash
git add internal/planner/sql/
git commit -m "feat(planner): parse CREATE ALERT DDL with webhook and insert sinks"
```

---

### Task 3: DROP ALERT and ALTER ALERT parsing

**Files:**
- Modify: `internal/planner/sql/parser.go`
- Modify: `internal/planner/sql/parser_test.go`

- [ ] **Step 1: Write failing tests**

Append to `internal/planner/sql/parser_test.go`:

```go
func TestParseDropAlert(t *testing.T) {
	cases := []struct {
		sql      string
		name     string
		ifExists bool
	}{
		{"DROP ALERT a", "a", false},
		{"DROP ALERT IF EXISTS a", "a", true},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			pq, err := Parse(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if pq.Type != QueryDropAlert {
				t.Fatalf("type: want QueryDropAlert, got %v", pq.Type)
			}
			if pq.DropAlert.Name != tc.name {
				t.Errorf("name: want %q, got %q", tc.name, pq.DropAlert.Name)
			}
			if pq.DropAlert.IfExists != tc.ifExists {
				t.Errorf("ifExists: want %v, got %v", tc.ifExists, pq.DropAlert.IfExists)
			}
		})
	}
}

func TestParseAlterAlert(t *testing.T) {
	cases := []struct {
		sql    string
		name   string
		enable bool
	}{
		{"ALTER ALERT foo ENABLE", "foo", true},
		{"ALTER ALERT foo DISABLE", "foo", false},
	}
	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			pq, err := Parse(tc.sql)
			if err != nil {
				t.Fatal(err)
			}
			if pq.Type != QueryAlterAlert {
				t.Fatalf("type: want QueryAlterAlert, got %v", pq.Type)
			}
			if pq.AlterAlert.Name != tc.name {
				t.Errorf("name: want %q, got %q", tc.name, pq.AlterAlert.Name)
			}
			if pq.AlterAlert.Enable != tc.enable {
				t.Errorf("enable: want %v, got %v", tc.enable, pq.AlterAlert.Enable)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestParseDropAlert\|TestParseAlterAlert -v ./internal/planner/sql/`
Expected: FAIL.

- [ ] **Step 3: Wire DROP ALERT into `lexParseDrop`**

In `internal/planner/sql/parser.go`, in `lexParseDrop` (line 557), after the VIEW dispatch (line 564-566) and before the FUNCTION check, add:

```go
	if kindTok.typ == TokenKWAlert {
		return lexParseDropAlert(sql, l)
	}
```

Update the error message on line 569 to include ALERT:

```go
		return nil, fmt.Errorf("expected TABLE, VIEW, FUNCTION, or ALERT after DROP")
```

- [ ] **Step 4: Implement `lexParseDropAlert`**

Append to `parser.go`:

```go
// lexParseDropAlert handles: DROP ALERT [IF EXISTS] <name>
// DROP ALERT has already been consumed.
func lexParseDropAlert(sql string, l *lexer) (*ParsedQuery, error) {
	ifExists := false
	tok := l.nextToken()
	if tok.typ == TokenKWIf {
		existsTok := l.nextToken()
		if existsTok.typ != TokenKWExists {
			return nil, fmt.Errorf("expected EXISTS after IF")
		}
		ifExists = true
		tok = l.nextToken()
	}
	if tok.typ != TokenIdent {
		return nil, fmt.Errorf("DROP ALERT: alert name is required")
	}
	return &ParsedQuery{
		Type:      QueryDropAlert,
		SQL:       sql,
		DropAlert: &DropAlertInfo{Name: tok.val, IfExists: ifExists},
	}, nil
}
```

- [ ] **Step 5: Wire ALTER ALERT into `parseAlterTable`**

In `internal/planner/sql/parser.go`, find `parseAlterTable` (search for `func parseAlterTable`). ALTER has been consumed. Before the TABLE dispatch, add an ALERT branch. Look for the first `l.nextToken()` that consumes the kind after ALTER, and add:

```go
	// Peek at kind: TABLE or ALERT
	kindTok := l.peekToken()
	if kindTok.typ == TokenKWAlert {
		l.nextToken() // consume ALERT
		return lexParseAlterAlert(sql, l)
	}
```

(If `parseAlterTable` currently assumes the next token is `TABLE`, simply preserve its existing logic by consuming TABLE explicitly inside its body after this new early-return.)

- [ ] **Step 6: Implement `lexParseAlterAlert`**

Append to `parser.go`:

```go
// lexParseAlterAlert handles: ALTER ALERT <name> {ENABLE|DISABLE}
// ALTER ALERT has already been consumed.
func lexParseAlterAlert(sql string, l *lexer) (*ParsedQuery, error) {
	nameTok := l.nextToken()
	if nameTok.typ != TokenIdent {
		return nil, fmt.Errorf("ALTER ALERT: alert name is required")
	}
	actionTok := l.nextToken()
	var enable bool
	switch actionTok.typ {
	case TokenKWEnable:
		enable = true
	case TokenKWDisable:
		enable = false
	default:
		return nil, fmt.Errorf("ALTER ALERT: expected ENABLE or DISABLE, got %q", actionTok.val)
	}
	return &ParsedQuery{
		Type:       QueryAlterAlert,
		SQL:        sql,
		AlterAlert: &AlterAlertInfo{Name: nameTok.val, Enable: enable},
	}, nil
}
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `go test -run TestParseDropAlert\|TestParseAlterAlert -v ./internal/planner/sql/`
Expected: all PASS.

- [ ] **Step 8: Run full parser test suite to catch regressions**

Run: `go test ./internal/planner/sql/`
Expected: PASS (no parser regressions).

- [ ] **Step 9: Commit**

```bash
git add internal/planner/sql/parser.go internal/planner/sql/parser_test.go
git commit -m "feat(planner): parse DROP ALERT and ALTER ALERT ENABLE|DISABLE"
```

---

### Task 4: Parser validation error cases

**Files:**
- Modify: `internal/planner/sql/parser_test.go`

- [ ] **Step 1: Write failing tests**

Append to `internal/planner/sql/parser_test.go`:

```go
func TestParseCreateAlertInvalid(t *testing.T) {
	cases := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "no sink",
			sql:     `CREATE ALERT a AS SELECT 1 FROM t EVERY 5 MINUTES`,
			wantErr: "at least one sink",
		},
		{
			name:    "interval below floor",
			sql:     `CREATE ALERT a AS SELECT 1 FROM t EVERY 5 SECONDS WEBHOOK 'https://x'`,
			wantErr: "interval must be >= 10 seconds",
		},
		{
			name:    "bad URL scheme",
			sql:     `CREATE ALERT a AS SELECT 1 FROM t EVERY 10 SECONDS WEBHOOK 'ftp://x'`,
			wantErr: "WEBHOOK URL must be http",
		},
		{
			name:    "invalid name",
			sql:     `CREATE ALERT 1bad AS SELECT 1 FROM t EVERY 10 SECONDS WEBHOOK 'http://x'`,
			wantErr: "alert name is required",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse(tc.sql)
			if err == nil {
				t.Fatal("want error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("err: want substring %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to verify all pass**

Run: `go test -run TestParseCreateAlertInvalid -v ./internal/planner/sql/`
Expected: all 4 PASS (the impl from Task 2 already handles these).

If any fail, adjust the error-message substring in the tests to match the actual messages. Do not weaken validation — if a case doesn't produce the expected error, fix the parser.

- [ ] **Step 3: Commit**

```bash
git add internal/planner/sql/parser_test.go
git commit -m "test(planner): cover CREATE ALERT validation error cases"
```

---

### Task 5: Catalog `AlertMeta` + CRUD

**Files:**
- Create: `internal/storage/catalog/alerts.go`
- Create: `internal/storage/catalog/alerts_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/storage/catalog/alerts_test.go`:

```go
package catalog

import (
	"context"
	"testing"
	"time"
)

func newTestCatalog(t *testing.T) *Catalog {
	t.Helper()
	kv := NewMemKV()
	cat := &Catalog{kv: kv, clusterID: "test"}
	return cat
}

func TestCreateAndGetAlert(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{
		Name:            "a1",
		QueryText:       "SELECT 1",
		IntervalSeconds: 60,
		WebhookURL:      "https://x",
		Enabled:         true,
		CreatedAt:       time.Unix(1000, 0).UTC(),
	}
	if err := cat.CreateAlert(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	got, err := cat.GetAlert(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Name != "a1" || got.QueryText != "SELECT 1" || got.IntervalSeconds != 60 {
		t.Errorf("round-trip mismatch: %+v", got)
	}
	if got.Version != 1 {
		t.Errorf("version: want 1, got %d", got.Version)
	}
}

func TestCreateAlertDuplicate(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 60}
	if err := cat.CreateAlert(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	err := cat.CreateAlert(context.Background(), m)
	if err == nil {
		t.Fatal("want duplicate error, got nil")
	}
}

func TestDropAlert(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", IntervalSeconds: 60}
	_ = cat.CreateAlert(context.Background(), m)
	if err := cat.DropAlert(context.Background(), "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := cat.GetAlert(context.Background(), "a1"); err == nil {
		t.Error("alert still exists after drop")
	}
}

func TestSetAlertEnabled(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", IntervalSeconds: 60, Enabled: true}
	_ = cat.CreateAlert(context.Background(), m)
	if err := cat.SetAlertEnabled(context.Background(), "a1", false); err != nil {
		t.Fatal(err)
	}
	got, _ := cat.GetAlert(context.Background(), "a1")
	if got.Enabled {
		t.Error("want disabled, got enabled")
	}
	if got.Version != 2 {
		t.Errorf("version: want 2, got %d", got.Version)
	}
}

func TestTouchAlertEvaluated(t *testing.T) {
	cat := newTestCatalog(t)
	m := AlertMeta{Name: "a1", IntervalSeconds: 60}
	_ = cat.CreateAlert(context.Background(), m)
	when := time.Unix(5000, 0).UTC()
	if err := cat.TouchAlertEvaluated(context.Background(), "a1", when); err != nil {
		t.Fatal(err)
	}
	got, _ := cat.GetAlert(context.Background(), "a1")
	if !got.LastEvaluatedAt.Equal(when) {
		t.Errorf("last evaluated: want %v, got %v", when, got.LastEvaluatedAt)
	}
}

func TestListAlerts(t *testing.T) {
	cat := newTestCatalog(t)
	for _, name := range []string{"b", "a", "c"} {
		_ = cat.CreateAlert(context.Background(), AlertMeta{Name: name, IntervalSeconds: 60})
	}
	alerts, err := cat.ListAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(alerts) != 3 {
		t.Fatalf("want 3 alerts, got %d", len(alerts))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestCreateAndGetAlert\|TestCreateAlertDuplicate\|TestDropAlert\|TestSetAlertEnabled\|TestTouchAlertEvaluated\|TestListAlerts -v ./internal/storage/catalog/`
Expected: FAIL — `AlertMeta` / `CreateAlert` / etc. undefined.

- [ ] **Step 3: Implement catalog methods**

Create `internal/storage/catalog/alerts.go`:

```go
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// AlertMeta is the catalog entry for a CREATE ALERT definition.
// Stored at key "<clusterID>.alert.<name>" via MetaKV CAS.
type AlertMeta struct {
	Name             string            `json:"name"`
	QueryText        string            `json:"query"`
	IntervalSeconds  int64             `json:"interval_seconds"`
	WebhookURL       string            `json:"webhook_url,omitempty"`
	WebhookHeaders   map[string]string `json:"webhook_headers,omitempty"`
	InsertIntoTable  string            `json:"insert_into_table,omitempty"`
	Enabled          bool              `json:"enabled"`
	CreatedAt        time.Time         `json:"created_at"`
	CreatedBy        string            `json:"created_by,omitempty"`
	LastEvaluatedAt  time.Time         `json:"last_evaluated_at,omitempty"`
	Version          int64             `json:"version"`
}

const alertKeyPrefix = "alert."

// CreateAlert writes a new alert entry; fails if an alert with the same name exists.
func (c *Catalog) CreateAlert(_ context.Context, m AlertMeta) error {
	if m.Name == "" {
		return fmt.Errorf("alert name is required")
	}
	key := c.key(alertKeyPrefix + m.Name)
	if _, _, err := c.kv.Get(key); err == nil {
		return fmt.Errorf("alert %q already exists", m.Name)
	} else if err != ErrKeyNotFound {
		return err
	}
	m.Version = 1
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	_, err = c.kv.Put(key, data)
	return err
}

// GetAlert returns the AlertMeta for name; returns an error if missing.
func (c *Catalog) GetAlert(_ context.Context, name string) (*AlertMeta, error) {
	key := c.key(alertKeyPrefix + name)
	data, _, err := c.kv.Get(key)
	if err != nil {
		if err == ErrKeyNotFound {
			return nil, fmt.Errorf("alert %q not found", name)
		}
		return nil, err
	}
	var m AlertMeta
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// DropAlert removes the alert entry; no error if missing.
func (c *Catalog) DropAlert(_ context.Context, name string) error {
	return c.kv.Delete(c.key(alertKeyPrefix + name))
}

// SetAlertEnabled toggles the enabled flag via CAS. Retries on revision mismatch.
func (c *Catalog) SetAlertEnabled(ctx context.Context, name string, enabled bool) error {
	return c.mutateAlert(ctx, name, func(m *AlertMeta) {
		m.Enabled = enabled
	})
}

// TouchAlertEvaluated updates LastEvaluatedAt; retries on CAS conflict.
// Failure to update is non-fatal for the scheduler; callers log and move on.
func (c *Catalog) TouchAlertEvaluated(ctx context.Context, name string, at time.Time) error {
	return c.mutateAlert(ctx, name, func(m *AlertMeta) {
		m.LastEvaluatedAt = at.UTC()
	})
}

// ListAlerts returns all alert entries, sorted by name.
func (c *Catalog) ListAlerts(_ context.Context) ([]AlertMeta, error) {
	prefix := c.key(alertKeyPrefix)
	keys, err := c.kv.List(prefix)
	if err != nil {
		return nil, err
	}
	var alerts []AlertMeta
	for _, k := range keys {
		if !strings.HasPrefix(k, prefix) {
			continue
		}
		data, _, err := c.kv.Get(k)
		if err != nil {
			continue
		}
		var m AlertMeta
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		alerts = append(alerts, m)
	}
	sort.Slice(alerts, func(i, j int) bool { return alerts[i].Name < alerts[j].Name })
	return alerts, nil
}

// mutateAlert reads, mutates, and writes an alert with CAS retry.
func (c *Catalog) mutateAlert(_ context.Context, name string, fn func(*AlertMeta)) error {
	key := c.key(alertKeyPrefix + name)
	const maxRetries = 10
	for attempt := 0; attempt < maxRetries; attempt++ {
		data, rev, err := c.kv.Get(key)
		if err != nil {
			if err == ErrKeyNotFound {
				return fmt.Errorf("alert %q not found", name)
			}
			return err
		}
		var m AlertMeta
		if err := json.Unmarshal(data, &m); err != nil {
			return err
		}
		fn(&m)
		m.Version++
		out, err := json.Marshal(m)
		if err != nil {
			return err
		}
		if _, err := c.kv.Update(key, out, rev); err == nil {
			return nil
		} else if err != ErrRevisionMismatch {
			return err
		}
		casBackoff(attempt)
	}
	return fmt.Errorf("alert %q: exceeded CAS retries", name)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/storage/catalog/`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/storage/catalog/alerts.go internal/storage/catalog/alerts_test.go
git commit -m "feat(catalog): AlertMeta CRUD with CAS-safe enable and touch"
```

---

### Task 6: Alerts package — core types

**Files:**
- Create: `internal/alerts/doc.go`
- Create: `internal/alerts/types.go`

- [ ] **Step 1: Create package doc**

Create `internal/alerts/doc.go`:

```go
// Package alerts implements CREATE ALERT DDL runtime: scheduler, sinks
// (webhook, alert_history table), and Prometheus metrics. Alerts run
// exclusively on the leader coordinator; see internal/coordinator for
// lifecycle wiring. The DDL grammar is parsed in internal/planner/sql.
package alerts
```

- [ ] **Step 2: Create types file**

Create `internal/alerts/types.go`:

```go
package alerts

import (
	"context"
	"time"
)

// AlertFire is the payload delivered to each sink on a matching evaluation.
type AlertFire struct {
	AlertName   string           `json:"alert"`
	EvaluatedAt time.Time        `json:"evaluated_at"`
	RowCount    int64            `json:"row_count"`          // true count, pre-truncation
	Rows        []map[string]any `json:"rows"`               // capped at MaxRowsPerFire
	Truncated   bool             `json:"truncated"`
	Schema      []ColumnMeta     `json:"schema"`
}

// ColumnMeta describes one result column.
type ColumnMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// MaxRowsPerFire is the hard cap on rows included in AlertFire.Rows.
const MaxRowsPerFire = 1000

// AlertSink delivers an AlertFire to its destination.
// Implementations must be safe to call concurrently and should respect ctx cancellation.
type AlertSink interface {
	Name() string
	Deliver(ctx context.Context, fire AlertFire) error
}

// SQLExecutor is the narrow interface the TableSink and scheduler use to
// run SQL against the engine. Implemented by *coordinator.Coordinator so
// this package doesn't import coordinator (breaks a would-be import cycle).
type SQLExecutor interface {
	// Execute runs a mutation statement (INSERT INTO ...). Returns an error
	// on failure; no result is surfaced.
	Execute(ctx context.Context, sql string) error

	// Query runs a SELECT and returns rows as []map[string]any plus a schema.
	// Implementations should cap the number of rows returned to at most limit.
	// If the underlying result has more rows, truncated is true and total is
	// the true (pre-truncation) row count.
	Query(ctx context.Context, sql string, limit int) (rows []map[string]any, schema []ColumnMeta, total int64, truncated bool, err error)
}
```

- [ ] **Step 3: Verify build**

Run: `go build ./internal/alerts/`
Expected: compiles cleanly.

- [ ] **Step 4: Commit**

```bash
git add internal/alerts/doc.go internal/alerts/types.go
git commit -m "feat(alerts): introduce package with AlertSink, AlertFire, SQLExecutor"
```

---

### Task 7: Alerts package — `WebhookSink`

**Files:**
- Create: `internal/alerts/webhook_sink.go`
- Create: `internal/alerts/webhook_sink_test.go`

- [ ] **Step 1: Write failing tests**

Create `internal/alerts/webhook_sink_test.go`:

```go
package alerts

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebhookSinkSuccess(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		body, _ := io.ReadAll(r.Body)
		var f AlertFire
		if err := json.Unmarshal(body, &f); err != nil {
			t.Errorf("body not valid AlertFire: %v", err)
		}
		if r.Header.Get("X-Auth") != "secret" {
			t.Errorf("missing custom header")
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := NewWebhookSink(srv.URL, map[string]string{"X-Auth": "secret"}, 10*time.Second)
	err := s.Deliver(context.Background(), AlertFire{AlertName: "a", EvaluatedAt: time.Now(), RowCount: 1})
	if err != nil {
		t.Fatalf("Deliver: %v", err)
	}
	if atomic.LoadInt32(&calls) != 1 {
		t.Errorf("want 1 call, got %d", calls)
	}
}

func TestWebhookSinkRetriesOn5xx(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := &WebhookSink{
		URL:     srv.URL,
		Client:  &http.Client{Timeout: 2 * time.Second},
		retries: 3,
		backoff: 1 * time.Millisecond, // speed up test
	}
	err := s.Deliver(context.Background(), AlertFire{AlertName: "a"})
	if err == nil {
		t.Fatal("want error after retries, got nil")
	}
	if atomic.LoadInt32(&calls) != 4 { // initial + 3 retries
		t.Errorf("want 4 calls, got %d", calls)
	}
}

func TestWebhookSinkConnectionError(t *testing.T) {
	s := &WebhookSink{
		URL:     "http://127.0.0.1:1", // unreachable
		Client:  &http.Client{Timeout: 200 * time.Millisecond},
		retries: 1,
		backoff: 1 * time.Millisecond,
	}
	err := s.Deliver(context.Background(), AlertFire{AlertName: "a"})
	if err == nil {
		t.Fatal("want connection error, got nil")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -run TestWebhookSink -v ./internal/alerts/`
Expected: FAIL — `NewWebhookSink` / `WebhookSink` undefined.

- [ ] **Step 3: Implement `WebhookSink`**

Create `internal/alerts/webhook_sink.go`:

```go
package alerts

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"time"
)

// WebhookSink POSTs an AlertFire JSON body to a URL with configurable headers
// and jittered exponential-backoff retries.
type WebhookSink struct {
	URL     string
	Headers map[string]string
	Client  *http.Client

	// retries is the number of retries after the initial attempt (3 in prod).
	retries int
	// backoff is the base backoff; doubled each attempt with +/-50% jitter.
	backoff time.Duration
}

// NewWebhookSink constructs a WebhookSink with production defaults: 3 retries,
// 200ms base backoff (→ 200, 800, 3200 ms), and the supplied HTTP timeout.
func NewWebhookSink(url string, headers map[string]string, timeout time.Duration) *WebhookSink {
	return &WebhookSink{
		URL:     url,
		Headers: headers,
		Client:  &http.Client{Timeout: timeout},
		retries: 3,
		backoff: 200 * time.Millisecond,
	}
}

func (*WebhookSink) Name() string { return "webhook" }

// Deliver POSTs the fire as JSON, retrying on network errors and non-2xx
// responses. Returns the last error after all retries are exhausted.
func (s *WebhookSink) Deliver(ctx context.Context, fire AlertFire) error {
	body, err := json.Marshal(fire)
	if err != nil {
		return fmt.Errorf("marshal AlertFire: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= s.retries; attempt++ {
		if attempt > 0 {
			d := jitteredBackoff(s.backoff, attempt-1)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(d):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.URL, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range s.Headers {
			req.Header.Set(k, v)
		}
		resp, err := s.Client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return nil
		}
		lastErr = fmt.Errorf("webhook HTTP %d", resp.StatusCode)
	}
	return fmt.Errorf("webhook delivery failed after %d attempts: %w", s.retries+1, lastErr)
}

// jitteredBackoff returns base * 2^n +/- 50% jitter.
func jitteredBackoff(base time.Duration, n int) time.Duration {
	d := base << uint(n)
	jitter := time.Duration(rand.Int63n(int64(d)))
	return d/2 + jitter
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestWebhookSink -v ./internal/alerts/`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/alerts/webhook_sink.go internal/alerts/webhook_sink_test.go
git commit -m "feat(alerts): WebhookSink with 3-retry jittered backoff"
```

---

### Task 8: Alerts package — `alert_history` helper + `TableSink`

**Files:**
- Create: `internal/alerts/history.go`
- Create: `internal/alerts/history_test.go`
- Create: `internal/alerts/table_sink.go`
- Create: `internal/alerts/table_sink_test.go`

- [ ] **Step 1: Write failing test for `EnsureHistoryTable`**

Create `internal/alerts/history_test.go`:

```go
package alerts

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

func TestEnsureHistoryTableIdempotent(t *testing.T) {
	kv := catalog.NewMemKV()
	cat, err := catalog.NewWithCluster(kv, nil, "test-bucket", "test")
	if err != nil {
		t.Fatal(err)
	}
	if err := EnsureHistoryTable(context.Background(), cat); err != nil {
		t.Fatal(err)
	}
	if err := EnsureHistoryTable(context.Background(), cat); err != nil {
		t.Fatalf("second call should be a no-op, got %v", err)
	}
	tm, err := cat.GetTable(context.Background(), HistoryTableName)
	if err != nil {
		t.Fatalf("alert_history not found: %v", err)
	}
	// Assert key columns are present
	wantCols := []string{"fired_at", "alert_name", "row_count", "delivery_status"}
	have := make(map[string]bool)
	for _, c := range tm.Schema.Columns {
		have[c.Name] = true
	}
	for _, w := range wantCols {
		if !have[w] {
			t.Errorf("schema missing column %q", w)
		}
	}
}
```

Uses `catalog.NewWithCluster(kv, store, bucket, clusterID)` — the constructor that takes a cluster ID (defined in `internal/storage/catalog/catalog.go:122`).

- [ ] **Step 2: Run to verify it fails**

Run: `go test -run TestEnsureHistoryTableIdempotent -v ./internal/alerts/`
Expected: FAIL.

- [ ] **Step 3: Implement `EnsureHistoryTable`**

Create `internal/alerts/history.go`:

```go
package alerts

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// HistoryTableName is the name of the system table holding alert fires.
const HistoryTableName = "alert_history"

// EnsureHistoryTable idempotently creates alert_history.
// Day-partitioned on fired_at.
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
		},
	}
	// Day-granularity partitioning is materialized as a synthetic string key
	// "YYYY-MM-DD" derived from fired_at; partitioning by a computed column
	// isn't supported yet, so we store a pre-bucketed partition column.
	// For v1 use a single partition key column "partition_date" we populate on insert.
	schema.Columns = append(schema.Columns, parquet.Column{Name: "partition_date", Type: parquet.TypeString})
	if err := cat.CreateTable(ctx, HistoryTableName, schema, []string{"partition_date"}); err != nil {
		return fmt.Errorf("creating %s: %w", HistoryTableName, err)
	}
	return nil
}

// SinkResult records per-sink delivery outcome. Serialized into alert_history.sink_results.
type SinkResult struct {
	Sink  string `json:"sink"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// BuildHistoryInsertSQL constructs an INSERT INTO alert_history VALUES (...) statement.
// Strings are SQL-escaped with single-quote doubling. Inputs are internal only but
// escaping protects against embedded quotes in JSON payloads.
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
	case okCount == 0:
		status = "failed"
	case okCount < len(results):
		status = "partial"
	}

	partitionDate := now.UTC().Format("2006-01-02")

	firedAt := now.UTC().Format(time.RFC3339Nano)
	evaluatedAt := fire.EvaluatedAt.UTC().Format(time.RFC3339Nano)

	return fmt.Sprintf(
		`INSERT INTO %s (fired_at, alert_name, evaluated_at, row_count, truncated, match_snapshot, delivery_status, sink_results, delivery_error, partition_date) VALUES (TIMESTAMP '%s', '%s', TIMESTAMP '%s', %d, %s, '%s', '%s', '%s', '%s', '%s')`,
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -run TestEnsureHistoryTableIdempotent -v ./internal/alerts/`
Expected: PASS.

- [ ] **Step 5: Write failing test for `TableSink`**

Create `internal/alerts/table_sink_test.go`:

```go
package alerts

import (
	"context"
	"strings"
	"testing"
	"time"
)

type recordingExecutor struct {
	sqls []string
}

func (r *recordingExecutor) Execute(_ context.Context, sql string) error {
	r.sqls = append(r.sqls, sql)
	return nil
}
func (r *recordingExecutor) Query(context.Context, string, int) ([]map[string]any, []ColumnMeta, int64, bool, error) {
	return nil, nil, 0, false, nil
}

func TestTableSinkDeliver(t *testing.T) {
	ex := &recordingExecutor{}
	s := &TableSink{Executor: ex, Now: func() time.Time { return time.Unix(1700000000, 0).UTC() }}
	fire := AlertFire{
		AlertName:   "a",
		EvaluatedAt: time.Unix(1699999995, 0).UTC(),
		RowCount:    3,
		Rows:        []map[string]any{{"x": 1}},
		Truncated:   false,
	}
	if err := s.Deliver(context.Background(), fire); err != nil {
		t.Fatal(err)
	}
	if len(ex.sqls) != 1 {
		t.Fatalf("want 1 statement, got %d", len(ex.sqls))
	}
	got := ex.sqls[0]
	for _, want := range []string{"INSERT INTO alert_history", "'a'", "partition_date"} {
		if !strings.Contains(got, want) {
			t.Errorf("SQL missing %q: %s", want, got)
		}
	}
}
```

- [ ] **Step 6: Run test to verify it fails**

Run: `go test -run TestTableSinkDeliver -v ./internal/alerts/`
Expected: FAIL.

- [ ] **Step 7: Implement `TableSink`**

Create `internal/alerts/table_sink.go`:

```go
package alerts

import (
	"context"
	"time"
)

// TableSink inserts one row into the alert_history table per fire by
// running INSERT INTO via an injected SQLExecutor.
type TableSink struct {
	Executor SQLExecutor
	// Now is a clock injection seam for tests. Defaults to time.Now.
	Now func() time.Time
	// SinkResults captured from sibling sinks, embedded in the history row.
	// The scheduler sets this before calling Deliver.
	Results []SinkResult
}

func (*TableSink) Name() string { return "table" }

func (s *TableSink) Deliver(ctx context.Context, fire AlertFire) error {
	now := time.Now()
	if s.Now != nil {
		now = s.Now()
	}
	sql, err := BuildHistoryInsertSQL(fire, s.Results, now)
	if err != nil {
		return err
	}
	return s.Executor.Execute(ctx, sql)
}
```

- [ ] **Step 8: Run all alerts tests**

Run: `go test ./internal/alerts/`
Expected: all PASS.

- [ ] **Step 9: Commit**

```bash
git add internal/alerts/history.go internal/alerts/history_test.go internal/alerts/table_sink.go internal/alerts/table_sink_test.go
git commit -m "feat(alerts): alert_history schema helper and TableSink"
```

---

### Task 9: Alerts package — `Scheduler` and metrics

**Files:**
- Create: `internal/alerts/metrics.go`
- Create: `internal/alerts/scheduler.go`
- Create: `internal/alerts/scheduler_test.go`

Create `internal/alerts/metrics.go` FIRST so the scheduler can reference the metric vars. If the project does not already use `promauto`, check with `grep -rn promauto internal/` and switch to the project's established registration pattern. Do not introduce a new dependency.

```go
package alerts

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricEvaluations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "wadjet_alert_evaluations_total",
		Help: "Count of alert evaluations by alert and status.",
	}, []string{"alert", "status"})

	metricEvalDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "wadjet_alert_evaluation_duration_seconds",
		Help:    "Time to evaluate an alert (query + sink delivery).",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10),
	}, []string{"alert"})

	metricRowsMatched = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "wadjet_alert_rows_matched",
		Help: "Row count returned by the most recent evaluation of each alert.",
	}, []string{"alert"})

	metricListErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "wadjet_alert_scheduler_list_errors_total",
		Help: "Scheduler catalog.ListAlerts errors.",
	})
)
```

- [ ] **Step 1: Write failing tests**

Create `internal/alerts/scheduler_test.go`:

```go
package alerts

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

type stubSink struct {
	name  string
	calls int32
	mu    sync.Mutex
	fires []AlertFire
}

func (s *stubSink) Name() string { return s.name }
func (s *stubSink) Deliver(_ context.Context, f AlertFire) error {
	atomic.AddInt32(&s.calls, 1)
	s.mu.Lock()
	s.fires = append(s.fires, f)
	s.mu.Unlock()
	return nil
}

type stubExec struct {
	rows  []map[string]any
	total int64
	schema []ColumnMeta
}

func (*stubExec) Execute(context.Context, string) error { return nil }
func (e *stubExec) Query(context.Context, string, int) ([]map[string]any, []ColumnMeta, int64, bool, error) {
	return e.rows, e.schema, e.total, false, nil
}

func newSchedulerTest(t *testing.T) (*catalog.Catalog, *stubExec, *stubSink) {
	t.Helper()
	kv := catalog.NewMemKV()
	cat, err := catalog.NewWithCluster(kv, nil, "b", "c")
	if err != nil {
		t.Fatal(err)
	}
	ex := &stubExec{
		rows:  []map[string]any{{"n": int64(1)}},
		total: 1,
		schema: []ColumnMeta{{Name: "n", Type: "INT64"}},
	}
	sink := &stubSink{name: "webhook"}
	return cat, ex, sink
}

func TestSchedulerFiresDueAlert(t *testing.T) {
	cat, ex, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: true,
	})
	s := NewScheduler(cat, ex, SinkFactory(func(m catalog.AlertMeta) []AlertSink {
		return []AlertSink{sink}
	}))
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) < 1 {
		t.Error("want at least 1 fire, got 0")
	}
}

func TestSchedulerSkipsDisabled(t *testing.T) {
	cat, ex, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: false,
	})
	s := NewScheduler(cat, ex, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }))
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) != 0 {
		t.Errorf("disabled alert fired: %d calls", sink.calls)
	}
}

func TestSchedulerNoFireOnZeroRows(t *testing.T) {
	cat, _, sink := newSchedulerTest(t)
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", QueryText: "SELECT 1", IntervalSeconds: 1, Enabled: true,
	})
	emptyExec := &stubExec{rows: nil, total: 0}
	s := NewScheduler(cat, emptyExec, SinkFactory(func(catalog.AlertMeta) []AlertSink { return []AlertSink{sink} }))
	s.tickInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	s.Start(ctx)
	s.Wait()
	if atomic.LoadInt32(&sink.calls) != 0 {
		t.Errorf("want no fire on zero rows, got %d calls", sink.calls)
	}
}
```

- [ ] **Step 2: Run to verify they fail**

Run: `go test -run TestScheduler -v ./internal/alerts/`
Expected: FAIL.

- [ ] **Step 3: Implement Scheduler**

Create `internal/alerts/scheduler.go`:

```go
package alerts

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// SinkFactory returns the set of sinks for an alert. Injected so the scheduler
// doesn't know about WebhookSink/TableSink concretely and tests can stub.
type SinkFactory func(m catalog.AlertMeta) []AlertSink

// Scheduler runs alerts on their configured cadence. It owns one goroutine
// that ticks and dispatches per-alert evaluations as short-lived goroutines.
type Scheduler struct {
	cat          *catalog.Catalog
	exec         SQLExecutor
	sinks        SinkFactory
	tickInterval time.Duration
	logger       *slog.Logger

	// Concurrency guard: alert name → in-flight.
	inflightMu sync.Mutex
	inflight   map[string]bool

	wg     sync.WaitGroup
	doneCh chan struct{}
}

// NewScheduler constructs a scheduler with default 1s tick cadence.
func NewScheduler(cat *catalog.Catalog, exec SQLExecutor, sinks SinkFactory) *Scheduler {
	return &Scheduler{
		cat:          cat,
		exec:         exec,
		sinks:        sinks,
		tickInterval: 1 * time.Second,
		inflight:     make(map[string]bool),
		logger:       slog.Default(),
		doneCh:       make(chan struct{}),
	}
}

// Start begins the scheduler loop. Returns immediately. Call Wait to block
// until ctx.Done() and all in-flight evaluations complete.
func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer close(s.doneCh)
		t := time.NewTicker(s.tickInterval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				s.tick(ctx, now)
			}
		}
	}()
}

// Wait blocks until the scheduler goroutine exits and all in-flight
// evaluations have finished.
func (s *Scheduler) Wait() {
	s.wg.Wait()
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	alerts, err := s.cat.ListAlerts(ctx)
	if err != nil {
		s.logger.Warn("list alerts", "err", err)
		metricListErrors.Inc()
		return
	}
	for _, a := range alerts {
		if !a.Enabled {
			continue
		}
		if now.Sub(a.LastEvaluatedAt) < time.Duration(a.IntervalSeconds)*time.Second {
			continue
		}
		if !s.tryClaim(a.Name) {
			metricEvaluations.WithLabelValues(a.Name, "skipped_concurrent").Inc()
			continue
		}
		s.wg.Add(1)
		go func(a catalog.AlertMeta) {
			defer s.wg.Done()
			defer s.release(a.Name)
			s.evaluate(ctx, a, now)
		}(a)
	}
}

func (s *Scheduler) tryClaim(name string) bool {
	s.inflightMu.Lock()
	defer s.inflightMu.Unlock()
	if s.inflight[name] {
		return false
	}
	s.inflight[name] = true
	return true
}

func (s *Scheduler) release(name string) {
	s.inflightMu.Lock()
	delete(s.inflight, name)
	s.inflightMu.Unlock()
}

func (s *Scheduler) evaluate(ctx context.Context, a catalog.AlertMeta, now time.Time) {
	interval := time.Duration(a.IntervalSeconds) * time.Second
	timeout := interval
	if timeout > 60*time.Second {
		timeout = 60 * time.Second
	}
	evalCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	rows, schema, total, truncated, err := s.exec.Query(evalCtx, a.QueryText, MaxRowsPerFire)
	metricEvalDuration.WithLabelValues(a.Name).Observe(time.Since(start).Seconds())
	if err != nil {
		s.logger.Warn("alert query failed", "alert", a.Name, "err", err)
		metricEvaluations.WithLabelValues(a.Name, "error").Inc()
		_ = s.cat.TouchAlertEvaluated(ctx, a.Name, now)
		return
	}
	metricRowsMatched.WithLabelValues(a.Name).Set(float64(total))

	if total == 0 {
		_ = s.cat.TouchAlertEvaluated(ctx, a.Name, now)
		return
	}

	fire := AlertFire{
		AlertName:   a.Name,
		EvaluatedAt: now.UTC(),
		RowCount:    total,
		Rows:        rows,
		Truncated:   truncated,
		Schema:      schema,
	}

	sinks := s.sinks(a)
	var results []SinkResult
	okCount := 0
	for _, sink := range sinks {
		// TableSink, if present, runs last and is given prior sink results.
		if ts, ok := sink.(*TableSink); ok {
			ts.Results = results
		}
		derr := sink.Deliver(evalCtx, fire)
		if derr == nil {
			results = append(results, SinkResult{Sink: sink.Name(), OK: true})
			okCount++
		} else {
			results = append(results, SinkResult{Sink: sink.Name(), OK: false, Error: derr.Error()})
		}
	}
	switch {
	case okCount == 0:
		metricEvaluations.WithLabelValues(a.Name, "failed").Inc()
	case okCount < len(results):
		metricEvaluations.WithLabelValues(a.Name, "partial").Inc()
	default:
		metricEvaluations.WithLabelValues(a.Name, "delivered").Inc()
	}

	_ = s.cat.TouchAlertEvaluated(ctx, a.Name, now)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -run TestScheduler -v ./internal/alerts/`
Expected: 3 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/alerts/scheduler.go internal/alerts/scheduler_test.go
git commit -m "feat(alerts): leader-owned Scheduler with per-alert concurrency guard"
```

---

### Task 10: Verify alert metrics are scraped

**Files:**
- Modify: none (verification only) OR `cmd/wadjet/main.go` if the metrics endpoint uses a non-default registry

- [ ] **Step 1: Identify the Prometheus registry in use**

Run: `grep -rn "prometheus.NewRegistry\|promauto.With\|MustRegister" cmd/wadjet/ internal/` to see whether the project uses the default registry or a custom one.

- [ ] **Step 2: If a custom registry is used, register alert metrics explicitly**

If the grep in Step 1 shows a custom registry (e.g., `reg := prometheus.NewRegistry()`), `promauto.NewCounterVec` in `internal/alerts/metrics.go` registers against the default registry and will NOT show up on `/metrics`. Replace `promauto.New*` with explicit construction + `reg.MustRegister(...)` at coordinator startup. Example fix inside `internal/coordinator/coordinator.go` startup:

```go
reg.MustRegister(alerts.Metrics()...)
```

And add an exported accessor in `internal/alerts/metrics.go`:

```go
// Metrics returns the collector set for external registration.
func Metrics() []prometheus.Collector {
	return []prometheus.Collector{metricEvaluations, metricEvalDuration, metricRowsMatched, metricListErrors}
}
```

If the project uses the default registry, this task is a no-op — verify that a running coordinator exposes `wadjet_alert_evaluations_total` on `/metrics`:

```bash
curl -s http://localhost:8080/metrics | grep wadjet_alert_
```

- [ ] **Step 3: Commit (if any changes were needed)**

```bash
git add internal/alerts/metrics.go internal/coordinator/coordinator.go
git commit -m "feat(alerts): register scheduler metrics with coordinator registry"
```

---

### Task 11: Coordinator DDL handlers

**Files:**
- Modify: `internal/coordinator/coordinator.go`
- Create or modify: `internal/coordinator/alerts.go` (new, keeps coordinator.go small)
- Create: `internal/coordinator/alerts_test.go`

- [ ] **Step 1: Write failing test**

Create `internal/coordinator/alerts_test.go`:

```go
package coordinator

import (
	"context"
	"strings"
	"testing"
)

func TestCreateAlertHandler(t *testing.T) {
	c := newTestCoordinator(t) // assumed helper in existing tests; if missing, build a minimal coord with MemKV catalog
	sql := `CREATE ALERT a1 AS SELECT 1 FROM t EVERY 30 SECONDS WEBHOOK 'https://x'`
	if err := c.handleCreateAlertSQL(context.Background(), sql); err != nil {
		t.Fatalf("handleCreateAlertSQL: %v", err)
	}
	m, err := c.cat.GetAlert(context.Background(), "a1")
	if err != nil {
		t.Fatal(err)
	}
	if m.Name != "a1" || m.IntervalSeconds != 30 {
		t.Errorf("unexpected meta: %+v", m)
	}
}

func TestDropAlertIfExistsMissing(t *testing.T) {
	c := newTestCoordinator(t)
	err := c.handleDropAlertSQL(context.Background(), `DROP ALERT IF EXISTS nope`)
	if err != nil {
		t.Errorf("IF EXISTS should swallow missing: %v", err)
	}
	err = c.handleDropAlertSQL(context.Background(), `DROP ALERT nope`)
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("want not-found error, got %v", err)
	}
}

func TestAlterAlertToggles(t *testing.T) {
	c := newTestCoordinator(t)
	_ = c.handleCreateAlertSQL(context.Background(), `CREATE ALERT a AS SELECT 1 FROM t EVERY 10 SECONDS WEBHOOK 'https://x'`)
	if err := c.handleAlterAlertSQL(context.Background(), `ALTER ALERT a DISABLE`); err != nil {
		t.Fatal(err)
	}
	m, _ := c.cat.GetAlert(context.Background(), "a")
	if m.Enabled {
		t.Error("want disabled")
	}
}
```

If `newTestCoordinator` does not exist in coordinator tests, build it inline using `catalog.NewWithCluster(catalog.NewMemKV(), nil, "b", "c")` and a minimal Coordinator struct; do not introduce new public constructors.

- [ ] **Step 2: Run to verify failure**

Run: `go test -run TestCreateAlertHandler\|TestDropAlertIfExistsMissing\|TestAlterAlertToggles -v ./internal/coordinator/`
Expected: FAIL.

- [ ] **Step 3: Implement handlers**

Create `internal/coordinator/alerts.go`:

```go
package coordinator

import (
	"context"
	"fmt"
	"time"

	"github.com/derekmwright/wadjet/internal/alerts"
	"github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

// handleCreateAlertSQL parses a CREATE ALERT statement and persists the
// AlertMeta. Validates the referenced tables exist by parsing and building
// a logical plan for the SELECT. Ensures alert_history is present.
func (c *Coordinator) handleCreateAlertSQL(ctx context.Context, sqlText string) error {
	pq, err := sql.Parse(sqlText)
	if err != nil {
		return err
	}
	if pq.Type != sql.QueryCreateAlert || pq.CreateAlert == nil {
		return fmt.Errorf("not a CREATE ALERT statement")
	}
	info := pq.CreateAlert

	// Validate the SELECT by re-parsing; catches unknown tables / syntax.
	if _, err := sql.Parse(info.QueryText); err != nil {
		return fmt.Errorf("invalid alert query: %w", err)
	}

	if err := alerts.EnsureHistoryTable(ctx, c.cat); err != nil {
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
	return c.cat.CreateAlert(ctx, m)
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
	if _, err := c.cat.GetAlert(ctx, pq.DropAlert.Name); err != nil {
		if pq.DropAlert.IfExists {
			return nil
		}
		return err
	}
	return c.cat.DropAlert(ctx, pq.DropAlert.Name)
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
	return c.cat.SetAlertEnabled(ctx, pq.AlterAlert.Name, pq.AlterAlert.Enable)
}

// identityFromCtx returns a string identity from ctx (auth package). Falls back to "" if absent.
func identityFromCtx(ctx context.Context) string {
	// If the project has an auth ctx helper, use it here. For v1 fallback to "".
	_ = ctx
	return ""
}
```

- [ ] **Step 4: Wire the handlers into the SQL dispatch and confirm admin-role gating**

Find where `coordinator.ExecuteSQL` dispatches on `pq.Type` (look for the existing `case sql.QueryCreateTable:` branch). Add cases:

```go
	case sql.QueryCreateAlert:
		return nil, c.handleCreateAlertSQL(ctx, sqlText)
	case sql.QueryDropAlert:
		return nil, c.handleDropAlertSQL(ctx, sqlText)
	case sql.QueryAlterAlert:
		return nil, c.handleAlterAlertSQL(ctx, sqlText)
```

The exact return shape must match the existing DDL cases (e.g., some return a nil `ResultSet` with nil error on success). Mirror the `QueryCreateTable` case precisely.

**Permissions check:** Before/around the `case sql.QueryCreateTable` branch, there is an admin-role (ABAC/RBAC) gate. Confirm the new `QueryCreateAlert/DropAlert/AlterAlert` cases are placed inside that same gate so alert DDL inherits admin-only enforcement. Run `grep -n "admin\|ABAC\|RBAC" internal/coordinator/coordinator.go` to locate. If the existing gate uses a list of admitted QueryTypes, add the three new values; if it wraps the dispatch unconditionally, no change needed.

- [ ] **Step 5: Run tests**

Run: `go test -run TestCreateAlertHandler\|TestDropAlertIfExistsMissing\|TestAlterAlertToggles -v ./internal/coordinator/`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add internal/coordinator/alerts.go internal/coordinator/alerts_test.go internal/coordinator/coordinator.go
git commit -m "feat(coordinator): handle CREATE/DROP/ALTER ALERT DDL"
```

---

### Task 12: Coordinator — leader-bound scheduler lifecycle

**Files:**
- Modify: `internal/coordinator/coordinator.go`
- Modify: `internal/coordinator/leader.go` (if needed to expose `LeaderChanged`)
- Create: `internal/coordinator/leader_alerts_test.go`

- [ ] **Step 1: Add scheduler start/stop hooks**

In `internal/coordinator/coordinator.go`, add fields to the `Coordinator` struct:

```go
	alertScheduler       *alerts.Scheduler
	alertSchedulerCancel context.CancelFunc
	alertsEnabled        bool
```

Import `"github.com/derekmwright/wadjet/internal/alerts"`.

Add methods (in `alerts.go` of the coordinator package to keep it tidy):

```go
// StartAlertScheduler begins scheduling alerts. Must only be called while
// this coordinator holds leadership. Safe to call multiple times; a running
// scheduler is stopped and replaced.
func (c *Coordinator) StartAlertScheduler(parent context.Context) {
	if !c.alertsEnabled {
		return
	}
	c.StopAlertScheduler()
	ctx, cancel := context.WithCancel(parent)
	c.alertSchedulerCancel = cancel
	c.alertScheduler = alerts.NewScheduler(c.cat, c.asSQLExecutor(), c.alertSinkFactory)
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
		sinks = append(sinks, alerts.NewWebhookSink(m.WebhookURL, m.WebhookHeaders, 10*time.Second))
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

// coordinatorExecutor bridges the narrow interface alerts needs onto the coordinator.
type coordinatorExecutor struct{ c *Coordinator }

func (e *coordinatorExecutor) Execute(ctx context.Context, sqlText string) error {
	_, err := e.c.ExecuteSQL(ctx, sqlText)
	return err
}

func (e *coordinatorExecutor) Query(ctx context.Context, sqlText string, limit int) ([]map[string]any, []alerts.ColumnMeta, int64, bool, error) {
	rs, err := e.c.ExecuteSQL(ctx, sqlText)
	if err != nil {
		return nil, nil, 0, false, err
	}
	// Drain up to limit rows, keep counting for total.
	rows := make([]map[string]any, 0, limit)
	var schema []alerts.ColumnMeta
	for _, col := range rs.Columns() { // assumes Columns() []ColumnSpec on result set
		schema = append(schema, alerts.ColumnMeta{Name: col.Name, Type: col.Type})
	}
	var total int64
	truncated := false
	for rs.Next() {
		total++
		if int64(len(rows)) < int64(limit) {
			rows = append(rows, rs.CurrentRowMap())
		} else {
			truncated = true
		}
	}
	return rows, schema, total, truncated, rs.Err()
}
```

The adapter method signatures are illustrative; they MUST match the actual coordinator's ExecuteSQL result type. Before writing the adapter, inspect the real signature and the `ResultSet` / result row interfaces. Replace the illustrative calls accordingly. If there is no `CurrentRowMap`-equivalent, build `map[string]any` from `rs.Columns()` and `rs.Values()`.

- [ ] **Step 2: Hook into leader election**

Find the existing leader-change callback in `internal/coordinator/leader.go` (the `LeaderChanged()` channel or similar). In the coordinator's main loop that observes leader changes, add:

```go
	for ev := range c.leader.LeaderChanged() {
		if ev.BecameLeader {
			c.StartAlertScheduler(c.lifecycleCtx)
		} else {
			c.StopAlertScheduler()
		}
	}
```

If `LeaderChanged()` uses a different shape (e.g., sends booleans), adapt. Fallback pattern: poll `c.leader.IsLeader()` each second in a ticker that compares with the previous value.

Also ensure `StopAlertScheduler()` is called during `Coordinator.Close()` / shutdown paths to avoid goroutine leaks.

- [ ] **Step 3: Write a lifecycle unit test**

Create `internal/coordinator/leader_alerts_test.go`:

```go
package coordinator

import (
	"context"
	"testing"
	"time"
)

// TestStartStopAlertScheduler exercises the start/stop lifecycle directly
// (without real leader-election, which has its own harness). Asserts that
// Start+Stop completes cleanly and that calling Stop with no running
// scheduler is a no-op.
func TestStartStopAlertScheduler(t *testing.T) {
	c := newTestCoordinator(t)
	c.SetAlertsEnabled(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.StartAlertScheduler(ctx)
	if c.alertScheduler == nil {
		t.Fatal("scheduler not started")
	}

	// Give the goroutine a moment to begin its loop.
	time.Sleep(20 * time.Millisecond)

	c.StopAlertScheduler()
	if c.alertScheduler != nil {
		t.Error("scheduler not cleared after Stop")
	}
	// Calling Stop again must be safe.
	c.StopAlertScheduler()
}

// TestFlagGateBlocksScheduler asserts that Start is a no-op when the
// --enable-alerts flag is off.
func TestFlagGateBlocksScheduler(t *testing.T) {
	c := newTestCoordinator(t)
	c.SetAlertsEnabled(false)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c.StartAlertScheduler(ctx)
	if c.alertScheduler != nil {
		t.Error("scheduler started despite disabled flag")
	}
}
```

Multi-coordinator leader-election failover is covered manually for v1; add a real failover harness only when the project has a reusable two-coordinator test setup (check `grep -rn "newTestLeader\|TwoCoordinator" internal/coordinator/` first).

- [ ] **Step 4: Verify build and existing tests**

Run: `go build ./... && go test ./internal/coordinator/ ./internal/alerts/`
Expected: both compile and existing tests pass.

- [ ] **Step 5: Commit**

```bash
git add internal/coordinator/alerts.go internal/coordinator/coordinator.go internal/coordinator/leader_alerts_test.go
git commit -m "feat(coordinator): start/stop alert scheduler on leader change"
```

---

### Task 13: Feature flag `--enable-alerts`

**Files:**
- Modify: `cmd/wadjet/main.go`
- Modify: `internal/coordinator/alerts.go`

- [ ] **Step 1: Plumb the flag in main**

In `cmd/wadjet/main.go`, locate where coordinator-mode flags are defined (search for existing bool flags like `--enable-auth` or similar). Add:

```go
	enableAlerts := serveCmd.Flags().Bool("enable-alerts", false, "enable CREATE ALERT DDL and scheduler (default: disabled)")
```

Support env override — right after flag parsing in the serve command body:

```go
	if v := os.Getenv("WADJET_ENABLE_ALERTS"); v == "1" || strings.EqualFold(v, "true") {
		*enableAlerts = true
	}
```

Pass to the coordinator constructor or set after construction:

```go
	coord.SetAlertsEnabled(*enableAlerts)
```

- [ ] **Step 2: Add the setter on Coordinator**

In `internal/coordinator/alerts.go`, add:

```go
// SetAlertsEnabled flips the feature flag. If disabled, DDL is rejected
// and the scheduler will not start on leader acquisition.
func (c *Coordinator) SetAlertsEnabled(on bool) {
	c.alertsEnabled = on
	if !on {
		c.StopAlertScheduler()
	}
}
```

And in each of `handleCreateAlertSQL`, `handleDropAlertSQL`, `handleAlterAlertSQL`, add as the first check:

```go
	if !c.alertsEnabled {
		return fmt.Errorf("alerts are disabled on this cluster; set --enable-alerts or WADJET_ENABLE_ALERTS=1")
	}
```

- [ ] **Step 3: Update existing handler tests**

The tests from Task 11 will now fail because the flag defaults off. Update `newTestCoordinator` (or the test setup) to call `c.SetAlertsEnabled(true)` before invoking the handlers. If `newTestCoordinator` doesn't exist as a helper, update each test's inline construction.

- [ ] **Step 4: Add a disabled-path test**

Append to `internal/coordinator/alerts_test.go`:

```go
func TestCreateAlertRejectedWhenDisabled(t *testing.T) {
	c := newTestCoordinator(t)
	c.SetAlertsEnabled(false)
	err := c.handleCreateAlertSQL(context.Background(), `CREATE ALERT a AS SELECT 1 FROM t EVERY 10 SECONDS WEBHOOK 'http://x'`)
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Errorf("want disabled error, got %v", err)
	}
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/coordinator/`
Expected: all PASS.

- [ ] **Step 6: Build CLI**

Run: `go build ./cmd/wadjet`
Expected: compiles cleanly.

- [ ] **Step 7: Commit**

```bash
git add cmd/wadjet/main.go internal/coordinator/alerts.go internal/coordinator/alerts_test.go
git commit -m "feat(alerts): --enable-alerts feature flag (off by default)"
```

---

### Task 14: MCP tools — `list_alerts` and `describe_alert`

**Files:**
- Create: `internal/server/mcp/alerts.go`
- Create: `internal/server/mcp/alerts_test.go`
- Modify: `internal/server/mcp/server.go`

- [ ] **Step 1: Write failing tests**

Create `internal/server/mcp/alerts_test.go`:

```go
package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestListAlertsTool(t *testing.T) {
	s := newTestServerWithAlerts(t, []seedAlert{
		{name: "a", interval: 60, enabled: true, webhook: "https://x"},
		{name: "b", interval: 30, enabled: false},
	})
	resp := s.callTool(t, "list_alerts", map[string]any{})
	var payload struct {
		Alerts []struct {
			Name    string `json:"name"`
			Enabled bool   `json:"enabled"`
		} `json:"alerts"`
	}
	if err := json.Unmarshal([]byte(resp), &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.Alerts) != 2 {
		t.Fatalf("want 2 alerts, got %d", len(payload.Alerts))
	}
}

func TestDescribeAlertTool(t *testing.T) {
	s := newTestServerWithAlerts(t, []seedAlert{{name: "a", interval: 60, enabled: true, webhook: "https://x"}})
	resp := s.callTool(t, "describe_alert", map[string]any{"name": "a"})
	if !strings.Contains(resp, "\"a\"") {
		t.Errorf("response missing alert name: %s", resp)
	}
}

func TestInitializeAdvertisesAlertCapability(t *testing.T) {
	s := newTestServerWithAlerts(t, nil)
	init := s.handleInitialize(&jsonRPCRequest{ID: json.RawMessage("1")})
	body, _ := json.Marshal(init.Result)
	if !strings.Contains(string(body), "wadjet.ddl.create_alert") {
		t.Errorf("initialize missing CREATE ALERT capability: %s", body)
	}
}
```

**Pre-step: inspect `internal/server/mcp/server_test.go`** to find the existing MCP test harness. Look for a helper that constructs a `*Server` backed by a `*wadjet.DB`. Run:

```bash
grep -n "func newTest\|func setup\|wadjet.Open" internal/server/mcp/server_test.go
```

Use whatever pattern already exists to construct the server in the new tests. Then add helper code to `alerts_test.go`:

```go
type seedAlert struct {
	name     string
	interval int64
	enabled  bool
	webhook  string
}

// newTestServerWithAlerts opens an in-memory Wadjet DB, seeds the given
// alerts into the catalog, and returns an MCP Server bound to that DB.
// Follows the construction pattern in server_test.go.
func newTestServerWithAlerts(t *testing.T, seeds []seedAlert) *Server {
	t.Helper()
	db := openTestDB(t) // <-- call the equivalent helper you found in server_test.go
	for _, s := range seeds {
		m := catalog.AlertMeta{
			Name:            s.name,
			IntervalSeconds: s.interval,
			Enabled:         s.enabled,
			WebhookURL:      s.webhook,
			QueryText:       "SELECT 1",
		}
		if err := db.Catalog().CreateAlert(context.Background(), m); err != nil {
			t.Fatal(err)
		}
	}
	return NewServer(db, nil)
}

func (s *Server) callTool(t *testing.T, name string, args map[string]any) string {
	t.Helper()
	params := map[string]any{"name": name, "arguments": args}
	raw, _ := json.Marshal(params)
	resp := s.handleToolsCall(context.Background(), &jsonRPCRequest{
		ID:     json.RawMessage("1"),
		Method: "tools/call",
		Params: raw,
	})
	out, _ := json.Marshal(resp.Result)
	return string(out)
}
```

Replace `openTestDB(t)` with the real helper name from `server_test.go`. If no helper exists, add one (e.g., `func openTestDB(t *testing.T) *wadjet.DB` that calls `wadjet.Open` with `MemStore`) and use it in both test files.

- [ ] **Step 2: Implement tools**

Create `internal/server/mcp/alerts.go`:

```go
package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/derekmwright/wadjet/internal/storage/catalog"
)

type alertSummary struct {
	Name            string `json:"name"`
	IntervalSeconds int64  `json:"interval_seconds"`
	Enabled         bool   `json:"enabled"`
	WebhookURL      string `json:"webhook_url,omitempty"`
	InsertInto      string `json:"insert_into_table,omitempty"`
	Query           string `json:"query"`
}

// handleListAlerts returns all alert definitions known to the catalog.
func (s *Server) handleListAlerts(ctx context.Context, _ json.RawMessage) (any, error) {
	cat, err := s.catalog()
	if err != nil {
		return nil, err
	}
	alerts, err := cat.ListAlerts(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]alertSummary, 0, len(alerts))
	for _, a := range alerts {
		out = append(out, summarize(a))
	}
	return map[string]any{"alerts": out}, nil
}

// handleDescribeAlert returns a single alert plus its last 10 history rows.
func (s *Server) handleDescribeAlert(ctx context.Context, args json.RawMessage) (any, error) {
	var a struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(args, &a); err != nil || a.Name == "" {
		return nil, fmt.Errorf("describe_alert: 'name' argument is required")
	}
	cat, err := s.catalog()
	if err != nil {
		return nil, err
	}
	m, err := cat.GetAlert(ctx, a.Name)
	if err != nil {
		return nil, err
	}
	// Recent fires — optional; return empty if alert_history missing.
	history, _ := s.queryHistory(ctx, a.Name)
	return map[string]any{
		"alert":  summarize(*m),
		"recent": history,
	}, nil
}

func summarize(a catalog.AlertMeta) alertSummary {
	return alertSummary{
		Name:            a.Name,
		IntervalSeconds: a.IntervalSeconds,
		Enabled:         a.Enabled,
		WebhookURL:      a.WebhookURL,
		InsertInto:      a.InsertIntoTable,
		Query:           a.QueryText,
	}
}

// queryHistory returns up to 10 most recent fires for name. Uses the Wadjet DB
// exposed on the server. Returns an empty slice if alert_history doesn't exist.
func (s *Server) queryHistory(ctx context.Context, name string) ([]map[string]any, error) {
	q := fmt.Sprintf(
		"SELECT fired_at, row_count, delivery_status, delivery_error FROM alert_history WHERE alert_name = '%s' ORDER BY fired_at DESC LIMIT 10",
		sqlEscapeIdent(name),
	)
	result := s.db.Query(ctx, q)
	if result.Err() != nil {
		return nil, nil
	}
	var rows []map[string]any
	for result.Next() {
		rows = append(rows, result.Row())
	}
	return rows, nil
}

// sqlEscapeIdent escapes single quotes in a string intended for a SQL literal.
// Alert names are already validated by parser regex, but defense-in-depth.
func sqlEscapeIdent(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\'' {
			out = append(out, '\'', '\'')
		} else {
			out = append(out, s[i])
		}
	}
	return string(out)
}

// catalog returns the catalog from the DB. Exposed via a helper so tests can
// inject or fake. If the Wadjet DB doesn't expose catalog directly, add a
// small getter there rather than reaching through privates here.
func (s *Server) catalog() (*catalog.Catalog, error) {
	c := s.db.Catalog()
	if c == nil {
		return nil, fmt.Errorf("catalog not available on DB")
	}
	return c, nil
}
```

If `wadjet.DB` does not have `Catalog()` / `Query()`/`Result()` shapes as assumed, adapt to what's there. The important contract is: `list_alerts` reads the catalog, `describe_alert` reads the catalog + queries `alert_history`.

- [ ] **Step 3: Register tools and advertise capability**

In `internal/server/mcp/server.go`, find where `handleToolsList` returns the tool list and add entries for `list_alerts` and `describe_alert`:

```go
	{
		Name:        "list_alerts",
		Description: "List all CREATE ALERT definitions in this cluster.",
		InputSchema: map[string]any{"type": "object", "properties": map[string]any{}},
	},
	{
		Name:        "describe_alert",
		Description: "Return the full AlertMeta for a given alert plus its 10 most recent fires.",
		InputSchema: map[string]any{
			"type":       "object",
			"required":   []string{"name"},
			"properties": map[string]any{"name": map[string]any{"type": "string"}},
		},
	},
```

In `handleToolsCall`, add dispatch branches calling `handleListAlerts` / `handleDescribeAlert`.

In `handleInitialize` (line 239), extend the result to include alert capability. Find the existing `Result` map and add a `capabilities` / `wadjet` section as appropriate. If the existing response is a fixed struct, add a field:

```go
	// In the result map returned by handleInitialize:
	"wadjet": map[string]any{
		"ddl.create_alert": map[string]any{
			"description": "Schedule a SQL query to evaluate periodically and deliver matches to a webhook or history table.",
			"example":     "CREATE ALERT failed_logins AS SELECT ... EVERY 5 MINUTES WEBHOOK 'https://...' INSERT INTO alert_history;",
			"docs_uri":    "wadjet://docs/alerts",
		},
	},
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/server/mcp/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/server/mcp/alerts.go internal/server/mcp/alerts_test.go internal/server/mcp/server.go
git commit -m "feat(mcp): list_alerts/describe_alert tools and initialize capability"
```

---

### Task 15: MCP — `wadjet://docs/alerts` resource + `list_functions` DDL addendum

**Files:**
- Create: `internal/server/mcp/alerts_doc.go`
- Create: `testdata/mcp/alerts-doc.md`
- Modify: `internal/server/mcp/server.go`

- [ ] **Step 1: Create doc content**

Create `testdata/mcp/alerts-doc.md`:

```markdown
# Wadjet CREATE ALERT

Define a SQL-driven alert that evaluates a query periodically and delivers matching rows to a webhook, a history table, or both.

## Grammar

```
CREATE ALERT <name>
  AS <SELECT ...>
  EVERY <N> {SECONDS|MINUTES|HOURS}
  [ WEBHOOK '<url>' [HEADERS { 'K' = 'V', ... }] ]
  [ INSERT INTO <table> ]
  ;

DROP ALERT [IF EXISTS] <name> ;
ALTER ALERT <name> {ENABLE|DISABLE} ;
```

At least one of `WEBHOOK` or `INSERT INTO` is required. Minimum interval is 10 seconds.

## Example

```sql
CREATE ALERT failed_logins_spike AS
  SELECT user_id, COUNT(*) AS failures
  FROM auth_events
  WHERE event_type = 'login_failed' AND ts >= now() - INTERVAL '5 minutes'
  GROUP BY user_id HAVING COUNT(*) > 10
EVERY 5 MINUTES
WEBHOOK 'https://example/alert'
INSERT INTO alert_history;
```

## Semantics

- **Stateless polling.** The query re-runs every interval; a persistent condition will re-fire every tick.
- **One fire per evaluation.** The first 1000 matching rows are sent in the payload; `truncated=true` when `row_count > 1000`.
- **Leader-only execution.** Exactly one coordinator runs the scheduler at a time.
- **No delivery if zero rows match.**

## Limits

- Interval floor: 10 seconds.
- Row payload cap: 1000 rows per fire.
- Max alerts per cluster (soft): ~100.
```

- [ ] **Step 2: Embed the doc and register the resource**

Create `internal/server/mcp/alerts_doc.go`:

```go
package mcp

import _ "embed"

//go:embed ../../../testdata/mcp/alerts-doc.md
var alertsDocMD string

// alertsDocURI is the MCP resource URI that serves alerts-doc.md.
const alertsDocURI = "wadjet://docs/alerts"
```

In `server.go`, extend the `resources/list` and `resources/read` handlers (or add them if absent — grep for `resources/list` first; MCP SDK may not yet expose resources in this project). Minimum addition in `handleRequest`:

```go
	case "resources/list":
		return &jsonRPCResponse{ID: req.ID, Result: map[string]any{
			"resources": []map[string]any{{
				"uri":         alertsDocURI,
				"name":        "CREATE ALERT docs",
				"description": "Grammar, semantics, and limits for Wadjet alerts.",
				"mimeType":    "text/markdown",
			}},
		}}
	case "resources/read":
		var p struct{ URI string `json:"uri"` }
		_ = json.Unmarshal(req.Params, &p)
		if p.URI != alertsDocURI {
			return errorResponse(req.ID, -32602, "unknown resource")
		}
		return &jsonRPCResponse{ID: req.ID, Result: map[string]any{
			"contents": []map[string]any{{
				"uri":      alertsDocURI,
				"mimeType": "text/markdown",
				"text":     alertsDocMD,
			}},
		}}
```

If existing helpers exist (`errorResponse`, `jsonRPCResponse`), match their patterns; don't duplicate.

- [ ] **Step 3: `list_functions` DDL addendum**

Find `handleListFunctions` (search `grep -n list_functions internal/server/mcp/`). Extend the response payload with a `ddl_capabilities` field:

```go
	"ddl_capabilities": []map[string]string{
		{"name": "CREATE ALERT", "description": "Schedule a SQL query; deliver matches to a webhook and/or alert_history."},
		{"name": "DROP ALERT", "description": "Remove an alert definition."},
		{"name": "ALTER ALERT ENABLE|DISABLE", "description": "Toggle evaluation without deleting the alert."},
	},
```

- [ ] **Step 4: Verify build and tests**

Run: `go build ./internal/server/mcp/ && go test ./internal/server/mcp/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/server/mcp/alerts_doc.go internal/server/mcp/server.go testdata/mcp/alerts-doc.md
git commit -m "feat(mcp): wadjet://docs/alerts resource and list_functions DDL addendum"
```

---

### Task 16: pgwire — `information_schema.alerts`

**Files:**
- Modify: `internal/server/pgwire/server.go`
- Modify: `internal/server/pgwire/server_test.go`

- [ ] **Step 1: Write failing unit test against the handler directly**

Append to `internal/server/pgwire/server_test.go`:

```go
func TestHandleInfoSchemaAlerts(t *testing.T) {
	// Build a minimal pgwire.Server with a MemKV-backed catalog and one seeded alert.
	kv := catalog.NewMemKV()
	cat, err := catalog.NewWithCluster(kv, nil, "b", "c")
	if err != nil {
		t.Fatal(err)
	}
	_ = cat.CreateAlert(context.Background(), catalog.AlertMeta{
		Name: "a1", IntervalSeconds: 60, WebhookURL: "https://x", Enabled: true,
	})
	s := &Server{coord: newTestCoord(t, cat)} // adapt to the pgwire Server shape

	rows, err := s.handleInfoSchemaAlerts(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("want 1 row, got %d", len(rows))
	}
	if rows[0]["name"] != "a1" {
		t.Errorf("row[0].name: want a1, got %v", rows[0]["name"])
	}
	if rows[0]["enabled"] != true {
		t.Errorf("row[0].enabled: want true, got %v", rows[0]["enabled"])
	}
}
```

`newTestCoord` should be a minimal helper that returns whatever type `pgwire.Server.coord` expects. If the pgwire server couples tightly to a full coordinator, extract a narrow interface (e.g., `type catalogSource interface { Catalog() *catalog.Catalog }`) and test against that. Do NOT wire up a full pgwire network harness — the end-to-end path is covered by Task 17.

- [ ] **Step 2: Locate the information_schema interception point**

In `internal/server/pgwire/server.go`, find `handleInfoSchemaTables` (around line 1518). The pattern is: match specific SQL shapes, return synthesized rows. Add a similar branch.

Search for where the query is dispatched — look for a function that routes `information_schema.tables` / `.columns` (around line 1281-1287). Add a third routing case for `alerts`.

- [ ] **Step 3: Implement handler**

Add a new function in the same file:

```go
// handleInfoSchemaAlerts returns rows for information_schema.alerts.
func (s *Server) handleInfoSchemaAlerts(ctx context.Context) ([]map[string]any, error) {
	cat := s.coord.Catalog()
	alerts, err := cat.ListAlerts(ctx)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	for _, a := range alerts {
		rows = append(rows, map[string]any{
			"name":              a.Name,
			"interval_seconds":  a.IntervalSeconds,
			"enabled":           a.Enabled,
			"webhook_url":       a.WebhookURL,
			"insert_into_table": a.InsertIntoTable,
			"last_evaluated_at": a.LastEvaluatedAt,
		})
	}
	return rows, nil
}
```

Wire it into the routing near the existing `information_schema.tables` pattern match.

- [ ] **Step 4: Verify build**

Run: `go build ./internal/server/pgwire/`
Expected: compiles cleanly.

- [ ] **Step 5: Commit**

```bash
git add internal/server/pgwire/server.go internal/server/pgwire/server_test.go
git commit -m "feat(pgwire): information_schema.alerts virtual table"
```

---

### Task 17: End-to-end integration test

**Files:**
- Create: `internal/alerts/integration_test.go`

- [ ] **Step 1: Write the integration test**

Create `internal/alerts/integration_test.go`:

```go
package alerts_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/wadjet"
)

// TestCreateAlertEndToEnd boots an embedded Wadjet with alerts enabled,
// creates an alert that fires every second, and asserts webhook + history.
func TestCreateAlertEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("end-to-end alerts test skipped under -short")
	}

	var hits int32
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer fake.Close()

	db, err := wadjet.Open(wadjet.Options{
		StorageType:   "memory",
		EnableAlerts:  true,
		AlertInterval: 100 * time.Millisecond, // override for tests, see note below
	})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Seed a small table the alert can query.
	if err := db.Exec(context.Background(), `CREATE TABLE t (x INT64)`); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(context.Background(), `INSERT INTO t VALUES (1), (2), (3)`); err != nil {
		t.Fatal(err)
	}

	// Override interval floor for tests (see implementation note).
	sql := fmt.Sprintf(`CREATE ALERT t_rows AS SELECT * FROM t EVERY 1 SECONDS WEBHOOK '%s' INSERT INTO alert_history`, fake.URL)
	if err := db.Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}

	// Wait for at least 2 fires.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&hits) >= 2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if n := atomic.LoadInt32(&hits); n < 2 {
		t.Errorf("want ≥2 webhook hits, got %d", n)
	}

	// Assert alert_history has rows.
	rs := db.Query(context.Background(), `SELECT COUNT(*) FROM alert_history WHERE alert_name = 't_rows'`)
	if rs.Err() != nil {
		t.Fatal(rs.Err())
	}
	rs.Next()
	var count int64
	_ = rs.ScanInt64(&count) // adapt to actual API
	if count < 2 {
		t.Errorf("want ≥2 history rows, got %d", count)
	}

	// DROP ALERT stops future fires.
	if err := db.Exec(context.Background(), `DROP ALERT t_rows`); err != nil {
		t.Fatal(err)
	}
	hitsBefore := atomic.LoadInt32(&hits)
	time.Sleep(1500 * time.Millisecond)
	hitsAfter := atomic.LoadInt32(&hits)
	if hitsAfter-hitsBefore > 1 { // tolerate one in-flight
		t.Errorf("fires continued after DROP: before=%d after=%d", hitsBefore, hitsAfter)
	}
}
```

**Note on interval floor:** the 10s floor is in the parser. For this test to run at 1s cadence, expose an internal `alertIntervalFloor` package-level variable in `internal/planner/sql/parser.go` defaulting to `10 * time.Second`, and override it via a test helper that sets it to `1 * time.Second` for the duration of this test (restore in `t.Cleanup`). The production parser logic stays the same; only the floor is tunable via the package-scoped var.

Add in `parser.go`:

```go
// alertIntervalFloor is the minimum allowed interval for CREATE ALERT.
// Exposed as a var for integration tests that need sub-10s cadence.
var alertIntervalFloor = 10 * time.Second
```

And replace the literal `10*time.Second` check in `lexParseCreateAlert` with `alertIntervalFloor`.

Provide a test-only helper `SetAlertIntervalFloorForTest(d time.Duration) func()` in the sql package that returns a restore function.

Adjust the integration test above to call it (drop the unknown `AlertInterval: 100 * time.Millisecond` option — it doesn't exist):

```go
	restore := sql.SetAlertIntervalFloorForTest(1 * time.Second)
	defer restore()
```

- [ ] **Step 2: Adapt API shape to reality**

The test above assumes `wadjet.Open(Options{EnableAlerts: true})` and `db.Exec` / `db.Query` signatures. Before running, check `wadjet/wadjet.go` for the real Options struct and method names. Adapt the test to match — do not change the public API to fit the test.

If `wadjet.Options` lacks `EnableAlerts`, add it and plumb to `coord.SetAlertsEnabled(o.EnableAlerts)` during `wadjet.Open`. Small, scoped change.

- [ ] **Step 3: Run the test**

Run: `go test -v -run TestCreateAlertEndToEnd ./internal/alerts/`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add internal/alerts/integration_test.go internal/planner/sql/parser.go wadjet/wadjet.go
git commit -m "test(alerts): end-to-end CREATE ALERT with webhook and history"
```

---

### Task 18: Golden SQL fixtures and final verification

**Files:**
- Create: `testdata/alerts/golden.sql`
- Modify: `internal/planner/sql/parser_test.go`

- [ ] **Step 1: Create golden fixtures**

Create `testdata/alerts/golden.sql`:

```sql
-- Minimal webhook alert.
CREATE ALERT minimal_webhook AS
  SELECT 1 AS n FROM t
EVERY 10 SECONDS
WEBHOOK 'http://example.com/hook';

-- Full DDL: webhook with headers + history table.
CREATE ALERT full_alert AS
  SELECT user_id, COUNT(*) AS c FROM events GROUP BY user_id HAVING COUNT(*) > 10
EVERY 5 MINUTES
WEBHOOK 'https://api.example.com/v2/alerts'
  HEADERS { 'Authorization' = 'Token abc123', 'X-Env' = 'prod' }
INSERT INTO alert_history;

-- INSERT-only alert (no webhook).
CREATE ALERT log_only AS
  SELECT * FROM errors WHERE severity = 'fatal'
EVERY 1 HOURS
INSERT INTO alert_history;

-- Lifecycle.
ALTER ALERT minimal_webhook DISABLE;
ALTER ALERT minimal_webhook ENABLE;
DROP ALERT IF EXISTS log_only;
DROP ALERT minimal_webhook;
```

- [ ] **Step 2: Add regression test that parses each statement**

Append to `internal/planner/sql/parser_test.go`:

```go
func TestAlertGoldenFixturesParse(t *testing.T) {
	data, err := os.ReadFile("../../../testdata/alerts/golden.sql")
	if err != nil {
		t.Fatal(err)
	}
	stmts := strings.Split(string(data), ";")
	for i, s := range stmts {
		s = strings.TrimSpace(s)
		if s == "" || strings.HasPrefix(s, "--") {
			continue
		}
		if _, err := Parse(s); err != nil {
			t.Errorf("stmt %d: parse failed: %v\nSQL: %s", i, err, s)
		}
	}
}
```

Add `"os"` to the test file's imports if absent.

- [ ] **Step 3: Run all tests**

Run: `go test ./internal/planner/sql/ ./internal/storage/catalog/ ./internal/alerts/ ./internal/coordinator/ ./internal/server/mcp/ ./internal/server/pgwire/`
Expected: all PASS.

- [ ] **Step 4: Build all binaries**

Run: `go build ./cmd/wadjet && go build ./cmd/tpch-harness`
Expected: both compile cleanly. (Remove existing `wadjet` binary first if it conflicts: `rm -f wadjet wadjet_bin`.)

- [ ] **Step 5: Run go vet**

Run: `go vet ./internal/alerts/ ./internal/coordinator/ ./internal/planner/sql/ ./internal/storage/catalog/ ./internal/server/mcp/ ./internal/server/pgwire/`
Expected: no warnings.

- [ ] **Step 6: Commit**

```bash
git add testdata/alerts/golden.sql internal/planner/sql/parser_test.go
git commit -m "test(alerts): golden SQL fixtures and parse regression test"
```

- [ ] **Step 7: Final smoke test**

Run: `go test -count=1 -short ./...`
Expected: no new failures beyond pre-existing flakes.

---

## Out of scope (v2)

Not part of this plan. Listed here for plan readers so they don't expand scope mid-implementation:

- Watermark / dedup semantics (`WITH WATERMARK ts` or similar).
- Per-row fire mode.
- Automatic `alert_history` retention.
- Cron expressions and non-interval schedules.
- Additional sink kinds (Slack, NATS publish, statsd).
- `ALTER ALERT SET INTERVAL | SET QUERY | SET WEBHOOK`.
- Scheduler scaling beyond ~100 alerts (indexed ticks, heap ordering).
- Dead-letter queue for persistent webhook failures.
