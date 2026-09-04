> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# CREATE ALERT DDL — Design Spec

**Status:** Approved, ready for implementation plan.
**Date:** 2026-04-16
**Scope:** v1 — stateless polling alerts with webhook + table sinks.

---

## 1. Goal

Add SQL-native detection and alerting to Wadjet. Users define an alert as a SQL query plus a schedule and delivery targets; the cluster evaluates the query periodically and delivers matches to a webhook and/or a history table. Positions Wadjet as a lightweight detection engine for SIEM-adjacent use cases, a differentiator vs. Trino/ClickHouse.

## 2. Non-goals (v1)

- **Per-row fire semantics.** v1 fires once per evaluation with up to 1000 rows in the payload.
- **Deduplication / watermark semantics.** A condition that persists for 6 hours will re-fire every tick. Dedup is v2 work.
- **Cron expressions.** Only simple intervals (`EVERY N SECONDS|MINUTES|HOURS`).
- **ALTER ALERT ... SET ...** (other than `ENABLE|DISABLE`). Users `DROP` + `CREATE` to change query, interval, or sinks.
- **Slack / email / PagerDuty built-in integrations.** Users stand up a webhook relay in front.
- **Dead-letter queue for failed webhooks.** Failures logged to `alert_history`; ops surface retries from there.
- **Automatic retention of `alert_history`.** Users manage partitions manually.
- **Scaling beyond ~100 alerts per cluster.** v1 scheduler is a linear scan.

## 3. Architecture

```
DDL path (any coordinator can accept the statement)
  psql/pgwire → parser (CreateAlertInfo)
    → coordinator.handleCreateAlertSQL
        → catalog.CreateAlert  (NATS KV CAS, key = <cluster>.alert.<name>)
        → ensure alert_history table exists (one-time, on first CREATE)

Runtime path (leader coordinator only)
  AlertScheduler (one goroutine, started on IsLeader=true, stopped on IsLeader=false)
    ticker 1s → catalog.ListAlerts
      → for each alert where enabled && now - last_evaluated_at >= interval:
          spawn evaluate(alert)
             1. ExecuteSQL(alert.Query) with timeout = min(interval, 60s)
             2. build AlertFire{...} capped at 1000 rows, true count preserved
             3. for each sink in [webhook, table]: sink.Deliver(ctx, fire)
             4. append consolidated alert_history row
```

**Leader binding.** Scheduler is started/stopped by the existing leader-election loop (`internal/coordinator/leader.go`). Same pattern as `BackgroundCompactor`.

**Concurrency guard.** In-memory `map[string]bool` on the scheduler blocks a second evaluation of the same alert while the first is in flight. Skipped ticks are counted via `wadjet_alert_evaluations_total{status="skipped_concurrent"}`.

**Leader flip tolerance.** Evaluations are stateless. Worst case, a leader flip mid-evaluation triggers one duplicate fire on failover; webhook consumers are expected to be idempotent. `LastEvaluatedAt` is written best-effort on success and used by the new leader to avoid immediate re-fire, not to strictly dedup.

## 4. DDL Grammar

```sql
-- Create
CREATE ALERT <name>
  AS <SELECT ...>
  EVERY <N> { SECONDS | MINUTES | HOURS }
  [ WEBHOOK '<url>' [HEADERS { 'K' = 'V', ... }] ]
  [ INSERT INTO <table> ]
  ;
-- At least one of WEBHOOK / INSERT INTO required.

-- Drop
DROP ALERT <name> [IF EXISTS] ;

-- Toggle
ALTER ALERT <name> { ENABLE | DISABLE } ;
```

### 4.1 Parser additions (`internal/planner/sql/`)

New lexer keywords (add if absent): `ALERT`, `EVERY`, `WEBHOOK`, `HEADERS`, `ENABLE`, `DISABLE`.

New `QueryType` values: `QueryCreateAlert`, `QueryDropAlert`, `QueryAlterAlert`.

New AST nodes on `ParsedQuery`:

```go
type CreateAlertInfo struct {
    Name       string
    QueryText  string            // raw SELECT text, re-parsed at eval time
    Interval   time.Duration     // validated ≥ 10s
    WebhookURL string            // "" if no webhook sink
    Headers    map[string]string
    InsertInto string            // "" if no table sink; at least one sink required
}
type DropAlertInfo  struct { Name string; IfExists bool }
type AlterAlertInfo struct { Name string; Enable bool } // true=ENABLE, false=DISABLE
```

### 4.2 Parse-time validation

- At least one sink present (`WEBHOOK` or `INSERT INTO`).
- Interval ≥ 10 seconds.
- Webhook URL parses and scheme is `http://` or `https://`.
- Alert name matches `[a-zA-Z_][a-zA-Z0-9_]*`, length ≤ 128.

### 4.3 CREATE-time validation (coordinator handler)

- Parse + build logical plan of the SELECT. Reject unknown tables / syntax with a clear error.
- If `INSERT INTO <table>` names a non-existent table, reject (users must create it or omit).
- Auto-create `alert_history` if missing.

### 4.4 Error-message expectations

Parser errors include a concrete example on the same error line. An agent running into `syntax error near 'EVERY': expected number literal; example: EVERY 5 MINUTES` should recover without a round-trip to docs.

### 4.5 Example

```sql
CREATE ALERT failed_logins_spike AS
  SELECT user_id, COUNT(*) AS failures
  FROM auth_events
  WHERE event_type = 'login_failed'
    AND ts >= now() - INTERVAL '5 minutes'
  GROUP BY user_id
  HAVING COUNT(*) > 10
EVERY 5 MINUTES
WEBHOOK 'https://pagerduty.example/v2/enqueue'
  HEADERS { 'Authorization' = 'Token abc123' }
INSERT INTO alert_history;
```

## 5. AI Agent Discoverability

Alerts are a first-class concept in Wadjet's MCP surface.

### 5.1 New MCP tools (`internal/server/mcp/`)

- **`list_alerts`** → `[{name, interval_seconds, enabled, query, webhook_url, insert_into_table, last_fired_at, last_status}]`. Reads catalog + a cheap `GROUP BY alert_name` roll-up from `alert_history`.
- **`describe_alert <name>`** → full `AlertMeta` + last 10 rows from `alert_history`. Agents use this to investigate fires.

Both are read-only and share the same auth gate as the existing `query` tool.

### 5.2 Initialization advertisement

The MCP `initialize` response gains a `wadjet.ddl.create_alert` capability entry:

```json
{
  "description": "Schedule a SQL query to evaluate periodically and deliver matches to a webhook or history table.",
  "example": "CREATE ALERT failed_logins AS SELECT ... EVERY 5 MINUTES WEBHOOK 'https://...' INSERT INTO alert_history;",
  "docs_uri": "wadjet://docs/alerts"
}
```

Agents see the feature at handshake.

### 5.3 MCP resource

Register `wadjet://docs/alerts` via the `resources/read` primitive. The resource returns a short markdown doc: grammar summary, one full example, semantics notes (stateless polling, per-eval fire, 1000-row cap), limits. Kept under 2 KB so agents pulling it don't blow context.

### 5.4 `information_schema.alerts` view

Standard SQL discovery for non-MCP clients:

```sql
SELECT name, interval_seconds, enabled, webhook_url, insert_into_table, last_fired_at
FROM information_schema.alerts;
```

Implemented using the same pattern Wadjet already uses for `information_schema.tables` / `information_schema.columns` in `internal/server/pgwire/server.go` — pgwire intercepts the query and returns synthesized rows sourced from `catalog.ListAlerts()` joined against an aggregation of `alert_history`. Available to any pgwire client (psql, JDBC, Superset); not implemented as a true SQL-engine virtual table in v1.

### 5.5 `list_functions` addendum

The existing MCP `list_functions` tool gains a short "DDL Capabilities" section listing `CREATE ALERT`, `DROP ALERT`, `ALTER ALERT ENABLE|DISABLE` with one-line descriptions each. Same API call agents already make when orienting.

### 5.6 Deferred

- Dedicated `create_alert` MCP tool with structured args. Duplicates `query`; add only if agents struggle with grammar in practice.
- `prompts/list` templates (e.g., "failed-login spike"). Product decision, not v1 design decision.

## 6. Evaluator & Sink Interface (`internal/alerts/`)

### 6.1 Sink interface

```go
type AlertSink interface {
    Name() string                                   // "webhook" | "table"
    Deliver(ctx context.Context, fire AlertFire) error
}

type AlertFire struct {
    AlertName   string
    EvaluatedAt time.Time
    RowCount    int64                               // true count, pre-truncation
    Rows        []map[string]any                    // capped at 1000
    Truncated   bool
    Schema      []ColumnMeta                        // name + type per result column
}
```

### 6.2 `WebhookSink`

- `http.Client{Timeout: 10*time.Second}`.
- POST `application/json` with the `AlertFire` marshalled verbatim.
- 3 retries on network error or non-2xx, jittered exponential backoff: 200ms, 800ms, 3.2s.
- Custom headers applied per-request.
- Returns error if all retries exhausted.

### 6.3 `TableSink`

- Depends only on a narrow `SQLExecutor` interface defined in `internal/alerts/` to keep `internal/alerts/` from importing `internal/coordinator/`:
  ```go
  type SQLExecutor interface {
      Execute(ctx context.Context, sql string) error
  }
  ```
  Coordinator implements this and injects itself when constructing the scheduler.
- `Deliver` builds a single-row `INSERT INTO <table> VALUES (...)` statement. Column values come from internal state (not user input), but the builder MUST still use string-literal escaping for `match_snapshot` and `sink_results` to handle embedded quotes in payloads. No concatenation of unescaped strings.
- Row payload: see `alert_history` schema in §7.2.
- Returns error on execution failure.

### 6.4 Scheduler loop

```go
func (s *Scheduler) run(ctx context.Context) {
    t := time.NewTicker(1 * time.Second)
    defer t.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case now := <-t.C:
            alerts, err := s.cat.ListAlerts(ctx)
            if err != nil { s.metrics.listErrors.Inc(); continue }
            for _, a := range alerts {
                if !a.Enabled { continue }
                if now.Sub(a.LastEvaluatedAt) < time.Duration(a.IntervalSeconds)*time.Second { continue }
                if !s.tryClaim(a.Name) { s.metrics.skippedConcurrent.WithLabelValues(a.Name).Inc(); continue }
                go func(a catalog.AlertMeta) {
                    defer s.release(a.Name)
                    s.evaluate(ctx, a, now)
                }(a)
            }
        }
    }
}
```

### 6.5 `evaluate()`

1. Derive a timeout ctx: `min(interval, 60s)`.
2. Run `ExecuteSQL(ctx, alert.QueryText)`.
3. Collect rows, truncate payload at 1000, keep counting for `RowCount`.
4. If `RowCount == 0`: update `LastEvaluatedAt` and return. No history row written.
5. For each configured sink, call `Deliver`. Capture per-sink error without aborting others.
6. Write one consolidated `alert_history` row summarizing all sinks.
7. Update `LastEvaluatedAt` (best-effort; failure here is logged, not fatal — the tick still counts).

### 6.6 Metrics

Exposed on the coordinator's existing Prometheus endpoint:

| Metric | Type | Labels |
|---|---|---|
| `wadjet_alert_evaluations_total` | counter | `alert`, `status` ∈ {delivered, partial, failed, error, skipped_concurrent} |
| `wadjet_alert_evaluation_duration_seconds` | histogram | `alert` |
| `wadjet_alert_rows_matched` | gauge | `alert` |
| `wadjet_alert_webhook_retries_total` | counter | `alert` |

## 7. Catalog & `alert_history` Schema

### 7.1 `AlertMeta`

Persisted in NATS KV at `<cluster>.alert.<name>` via `MetaKV.Update` (CAS).

```go
type AlertMeta struct {
    Name             string            `json:"name"`
    QueryText        string            `json:"query"`
    IntervalSeconds  int64             `json:"interval_seconds"`
    WebhookURL       string            `json:"webhook_url,omitempty"`
    WebhookHeaders   map[string]string `json:"webhook_headers,omitempty"`
    InsertIntoTable  string            `json:"insert_into_table,omitempty"`
    Enabled          bool              `json:"enabled"`
    CreatedAt        time.Time         `json:"created_at"`
    CreatedBy        string            `json:"created_by"`          // identity from auth ctx
    LastEvaluatedAt  time.Time         `json:"last_evaluated_at"`   // scheduler bookkeeping
    Version          int64             `json:"version"`             // CAS
}
```

Catalog methods (new):

```go
func (c *Catalog) CreateAlert(ctx context.Context, m AlertMeta) error
func (c *Catalog) DropAlert(ctx context.Context, name string) error
func (c *Catalog) SetAlertEnabled(ctx context.Context, name string, enabled bool) error
func (c *Catalog) TouchAlertEvaluated(ctx context.Context, name string, at time.Time) error
func (c *Catalog) GetAlert(ctx context.Context, name string) (AlertMeta, error)
func (c *Catalog) ListAlerts(ctx context.Context) ([]AlertMeta, error)
```

Key layout (existing + new):

```
<cluster>.table.<name>      # unchanged
<cluster>.role.<name>       # unchanged
<cluster>.policy.<name>     # unchanged
<cluster>.alert.<name>      # new
```

### 7.2 `alert_history` table

Auto-created on first `CREATE ALERT`. Day-partitioned for efficient "last 24h" queries.

```sql
CREATE TABLE alert_history (
    fired_at        TIMESTAMP NOT NULL,
    alert_name      STRING    NOT NULL,
    evaluated_at    TIMESTAMP NOT NULL,
    row_count       INT64     NOT NULL,
    truncated       BOOL      NOT NULL,
    match_snapshot  STRING    NOT NULL,  -- JSON array of rows (≤1000)
    delivery_status STRING    NOT NULL,  -- 'delivered' | 'partial' | 'failed'
    sink_results    STRING    NOT NULL,  -- JSON array of {sink, ok, error}
    delivery_error  STRING                -- first non-empty error, convenience
)
PARTITION BY date_trunc('day', fired_at);
```

JSON stored as `STRING` rather than a native nested type; users extract with `json_extract`. This deliberately keeps the schema stable across future payload evolution.

## 8. Feature Flag & Permissions

### 8.1 Feature flag

Alerts are off by default.

- Config: `WADJET_ENABLE_ALERTS=1` env var, or `--enable-alerts` CLI flag on coordinator.
- When off: `CREATE ALERT` / `DROP ALERT` / `ALTER ALERT` return `feature disabled: set WADJET_ENABLE_ALERTS=1 to enable`. Scheduler does not start even on the leader.
- Protects clusters that don't want the feature from accidental webhook spam.

### 8.2 Permissions

- `CREATE ALERT` / `DROP ALERT` / `ALTER ALERT` require the existing admin role gate (same as `CREATE TABLE`). No new ACL primitive in v1.
- `AlertMeta.CreatedBy` records the identity from the auth context; surfaced via `information_schema.alerts` and `describe_alert`.

## 9. Testing Strategy

### 9.1 Unit

- **Parser** (`internal/planner/sql/parser_test.go`): table-driven valid + invalid cases for all three statements. Cover missing sink, interval floor, malformed URL, HEADERS syntax variants.
- **Catalog** (`internal/storage/catalog/alerts_test.go`): CRUD against `MemKV`. Duplicate-name CAS conflict, missing-name drop, concurrent `SetAlertEnabled`.
- **Scheduler** (`internal/alerts/scheduler_test.go`): fake clock, fake catalog, stub sinks. Due alert fires, non-due doesn't, disabled doesn't, long-running next tick records `skipped_concurrent`.
- **WebhookSink** (`internal/alerts/webhook_sink_test.go`): `httptest.Server`. Success, 500 with 3-retry-then-fail, connection refused, 10s timeout, custom headers present.
- **TableSink** (`internal/alerts/table_sink_test.go`): `wadjet.Open(MemStore)`, assert `alert_history` row after `Deliver`.

### 9.2 Integration

- **End-to-end** (`internal/alerts/integration_test.go`): embedded coordinator + MemStore. `CREATE ALERT` with 1-second interval (test-only override of the 10s floor). Fake webhook server counts calls. Assert ≥ 2 fires over 3 seconds; assert `alert_history` matches. `DROP ALERT` stops fires. `ALTER ALERT DISABLE` stops fires, keeps metadata.
- **Leader failover** (`internal/coordinator/leader_alerts_test.go`): two coordinators sharing NATS KV. Scheduler runs only on leader. Kill leader; successor resumes within election TTL; ≤ 1 duplicate fire on flip.
- **MCP surface** (`internal/server/mcp/alerts_test.go`): `list_alerts`, `describe_alert`, and `initialize` capability advertisement payloads match the documented contract.

### 9.3 Fixtures

- `testdata/alerts/` — golden SQL examples that must continue to parse. The grammar is effectively public API once agents lean on it; breaking changes are high-cost.

### 9.4 Out of scope for v1 tests

- Load/stress tests at 10K alerts.
- End-to-end delivery against real webhook providers (Slack, PagerDuty) — users' adapters aren't our problem.

### 9.5 Performance budget

- Scheduler tick: ≤ 1 ms for ≤ 100 alerts (catalog list + per-alert compare). Above that, indexed scheduling is v2 work.
- Webhook `Deliver`: ≤ 30 s worst case (10 s timeout + 3 retries with backoff).
- No per-row allocation on the evaluator hot path beyond the result slice itself.

## 10. Open Questions

None — all clarifying questions resolved during brainstorming.

## 11. Future Work (explicit v2 candidates)

1. **Watermark / dedup semantics.** `ON CHANGE` or `WITH WATERMARK ts` clauses. Enables per-new-match fires without re-firing on persistent conditions.
2. **Per-row fire mode.** `ON FIRE PER ROW` — one delivery per matching row.
3. **Automatic retention on `alert_history`.** Policy-driven partition drop.
4. **Additional sink kinds.** `NATS PUBLISH '<subject>'`, `EMIT STATSD '<key>'`, `SLACK` adapter.
5. **`ALTER ALERT ... SET INTERVAL / SET WEBHOOK / SET QUERY`.** Replace the current drop+create pattern.
6. **Cron expressions.** `CRON '*/5 * * * *'` for the remaining 5% of scheduling needs.
7. **Scaling.** Indexed scheduling / heap-based tick ordering for clusters with thousands of alerts.
8. **Dead-letter queue** for persistent webhook failures.
