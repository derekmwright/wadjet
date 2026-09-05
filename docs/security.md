# Security

Wadjet provides layered security: authentication, authorization (RBAC and ABAC), and cell-level access policies. Authentication, role and policy configuration is hot-reloadable; the mTLS listener's certificates and CA are read once at startup and require a restart to rotate.

## Authentication

Three authentication methods are supported. Configure one in the YAML config file.

### API Keys

Simple bearer token authentication. Best for service-to-service communication. Each key maps to a name (identity label), a role, and optional ABAC attributes.

```yaml
auth:
  enabled: true
  api_keys:
    - key: "wadjet-ingest-key-abc123"
      name: "ingest-pipeline"
      role: writer
    - key: "wadjet-dashboard-key-xyz789"
      name: "grafana-dashboard"
      role: reader
    - key: "wadjet-admin-key-000000"
      name: "admin-operator"
      role: admin
```

Usage:

```bash
curl -H "Authorization: Bearer wadjet-ingest-key-abc123" http://localhost:8080/v1/queries ...
```

### JWT

JSON Web Token authentication. Best for user-facing applications where an identity provider issues tokens.

**HMAC-SHA256:**

```yaml
auth:
  enabled: true
  jwt:
    enabled: true
    secret: "your-256-bit-secret-here"
    role_claim: role          # JWT claim containing the role name
    issuer: "my-idp"         # Expected issuer (optional, validates iss claim)
```

**RSA:**

```yaml
auth:
  enabled: true
  jwt:
    enabled: true
    public_key_file: /etc/wadjet/jwt-public.pem
    role_claim: role
    issuer: "my-idp"
```

The JWT payload must include a role claim (matching `role_claim` config) for authorization:

```json
{
  "sub": "jdoe@example.com",
  "role": "reader",
  "iss": "my-idp",
  "iat": 1773792000,
  "exp": 1773795600
}
```

Usage:

```bash
curl -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..." http://localhost:8080/v1/queries ...
```

### mTLS

Mutual TLS authentication. Best for zero-trust environments and government/high-security deployments.

```yaml
auth:
  enabled: true
  mtls:
    enabled: true
    ca_file: /etc/wadjet/ca.pem
    cert_file: /etc/wadjet/server-cert.pem
    key_file: /etc/wadjet/server-key.pem
    role_map:
      "CN=ingest-pipeline": writer
      "CN=grafana-dashboard": reader
      "CN=admin-operator": admin
    default_role: reader    # Fallback if CN not in role_map
```

The server validates client certificates against the configured CA. The certificate's Common Name is mapped to a role via `role_map`. If the CN isn't found, `default_role` is used.

Usage:

```bash
curl --cert client.pem --key client-key.pem --cacert ca.pem https://wadjet.internal:8080/v1/queries ...
```

### Identity Enrichment

All authentication methods enrich the `Identity` with attributes for ABAC policy evaluation:

- **API keys**: none beyond the automatic `role`, `name` and `method`. An api_keys entry carries only `key`, `name` and `role`; there is no `attributes` field.
- **JWT**: All non-standard claims from the token payload (e.g., `department`, `clearance`, `team`)
- **mTLS**: Certificate fields — `cn`, `org`, `ou`, `san_dns`, `san_email`, `issuer`

These attributes are available as `subject.*` conditions in ABAC policies. The `role`, `name`, and `method` attributes are always populated automatically.

### Multi-Protocol Authentication

Authentication is enforced across all three client protocols:

| Protocol | Mechanism | Details |
|----------|-----------|---------|
| **HTTP** | `Authorization: Bearer <token>` header | API key, JWT, or mTLS from TLS state |
| **PostgreSQL wire** (pgwire) | Cleartext password flow | Server sends `AuthenticationCleartextPassword`, client sends API key or JWT as password |
| **gRPC** | `authorization` metadata header | Bearer token extracted from gRPC metadata |

When auth is disabled (no provider configured), all protocols accept connections without credentials.

**pgwire example** (psql):
```bash
# API key as password
psql -h localhost -p 5433 -U analyst -W
Password: wadjet-dashboard-key-xyz789

# Or via connection string
psql "host=localhost port=5433 user=analyst password=wadjet-dashboard-key-xyz789"
```

**gRPC example** (grpcurl):
```bash
grpcurl -H "authorization: Bearer wadjet-key-abc123" \
  -d '{"sql": "SELECT * FROM flow_logs LIMIT 10"}' \
  localhost:9090 wadjet.v1.WadjetService/Query
```

## Authorization

Wadjet supports two authorization models: RBAC (role-based) and ABAC (attribute-based). RBAC is simpler and sufficient for most deployments. ABAC provides fine-grained control for government, clearance-level, and multi-tenant environments.

When both are configured, ABAC takes precedence. When only RBAC roles are defined, they are automatically migrated to equivalent ABAC policies at startup.

### RBAC

Role-based access control (RBAC) maps authenticated identities to permissions on tables.

### Role Definitions

Roles are defined as a flat list. Each role has a name, a list of tables it can access (or `"*"` for all), and a list of allowed permissions:

```yaml
auth:
  roles:
    # Read-only access to all tables
    - name: reader
      tables: ["*"]
      allow: [read]

    # Read/write access to specific tables
    - name: writer
      tables: [flow_logs, syslog]
      allow: [read, write]

    # Network operations — read everything, write device tables
    - name: netops
      tables: [flow_logs, syslog, snmp_traps, device_inventory]
      allow: [read, write]

    # Full administrative access
    - name: admin
      tables: ["*"]
      allow: [read, write, admin]
```

### Permission Types

| Permission | Allows |
|-----------|-------|
| `read` | Execute SELECT queries against the table |
| `write` | Ingest data into the table (via API or embedded) |
| `admin` | Create/drop the table, modify its schema |

### Permission Resolution

1. Look up the user's role from their authentication credentials
2. Find the matching role definition
3. For each table referenced in the query, check if the role's `tables` list includes it (or `"*"`)
4. Check if the role's `allow` list includes the required permission
5. If the role does not grant access, the request is denied with 403 Forbidden

### ABAC (Attribute-Based Access Control)

ABAC policies evaluate subject attributes (who), resource attributes (what), action (how), and environment conditions (when/where) to make access decisions. This enables policies like "users with clearance=SECRET can read classified tables during business hours, but SSN columns are masked."

#### Policy Structure

```yaml
auth:
  abac_policies:
    - name: classified-data-access
      description: "Control access to classified tables by clearance level"
      priority: 10           # lower = evaluated first
      rules:
        - effect: allow
          conditions:
            - attribute: subject.clearance
              operator: in
              value: "TOP_SECRET,SECRET"
            - attribute: resource.name
              operator: eq
              value: "classified_events"
            - attribute: subject.role
              operator: neq
              value: "contractor"
          obligations:
            - type: row_filter
              target: classified_events
              value: "classification_level <= 2"

        - effect: allow
          conditions:
            - attribute: subject.clearance
              operator: eq
              value: "SECRET"
            - attribute: resource.name
              operator: eq
              value: "classified_events"
          obligations:
            - type: mask_column
              target: ssn
              value: "'***REDACTED***'"   # a SQL expression — quote string literals
            - type: deny_column
              target: raw_payload

    - name: deny-contractors-classified
      description: "Contractors cannot access classified tables"
      priority: 5            # higher priority (lower number) than allow rules
      rules:
        - effect: deny
          conditions:
            - attribute: subject.role
              operator: eq
              value: "contractor"
            - attribute: resource.name
              operator: eq
              value: "classified_events"
```

#### Condition Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `eq` | Equals | `subject.role eq admin` |
| `neq` | Not equals | `subject.method neq apikey` |
| `in` | Value in comma-separated list | `subject.clearance in TOP_SECRET,SECRET` |
| `not_in` | Value not in list | `subject.department not_in hr,legal` |
| `gt`, `lt`, `gte`, `lte` | Numeric comparison | `env.hour gte 9` |
| `contains` | String contains | `subject.name contains @example.com` |
| `regex` | Regular expression match | `resource.name regex ^classified_.*` |
| `exists` | Attribute is present | `subject.clearance exists` |
| `not_exists` | Attribute is absent | `subject.clearance not_exists` |

#### Obligation Types

Obligations are side-effects attached to **allow** rules. They constrain how data is returned even when access is granted.

| Type | Target | Value | Description |
|------|--------|-------|-------------|
| `row_filter` | table name | SQL predicate | Filter node injected directly above the scan; the predicate reads the row as STORED |
| `mask_column` | column name | SQL expression, e.g. `"'REDACTED'"` | Column replaced by the expression at the scan, for every consumer above it. Empty `value` means a placeholder chosen from the column's declared type |
| `deny_column` | column name | — | The column does not exist for this identity: absent from `SELECT *`, and naming it is 42703 |
| `query_limit` | one of `max_scan_rows` (the default when empty), `max_scan_bytes`, `max_scan_files` | a positive integer | A cost ceiling for this identity on this relation, merged into the same guard `query_limits:` uses. A policy can only NARROW: the tighter of the two applies, and a statement reading two policed relations is held to the tighter of theirs. An obligation whose value is not a positive integer, or whose target is not one of the three, refuses at config load. |

#### Deny-Overrides Combining

ABAC uses a **deny-overrides** combining algorithm:

1. All matching policies are evaluated (by priority order)
2. If **any** matching rule has `effect: deny`, the result is **Deny** — regardless of any allow rules
3. If at least one `effect: allow` matches and no deny rules match, the result is **Allow**
4. Obligations from all matching allow rules are merged (row filters are AND'd)
5. If no rules match at all, the default is **Deny** (closed-world assumption)

This ensures that a narrow deny rule always overrides a broad allow — critical for clearance-level data where over-granting is unacceptable.

#### RBAC Auto-Migration

When `abac_policies` is not configured but `roles` are defined, Wadjet automatically converts RBAC roles to equivalent ABAC policies at startup:

- Each role becomes an ABAC policy with `subject.role eq <name>` conditions
- Table lists become `resource.name in <tables>` conditions
- Permission lists become action conditions
- Cell-level `policies` become obligations (row_filter, mask_column, deny_column)

This means existing RBAC configurations work unchanged — they get ABAC evaluation semantics (deny-overrides, attribute matching) automatically.

## Cell-Level Policies

Cell-level policies provide fine-grained data access control. Both halves are
enforced **at plan time, at the scan**: a row filter becomes a Filter node
directly above the scan, and a column policy becomes a *security projection*
above that filter, which replaces every masked column with its mask expression
and omits every denied one. Nothing downstream of the scan — a WHERE clause, a
GROUP BY key, an aggregate, `COUNT(DISTINCT)`, a join key, a window partition,
a derived table, a CTE, a `UNION` arm, `SELECT *` — can see the raw value,
because the raw value never leaves the scan. This holds identically on the
embedded API, the PostgreSQL wire protocol and the HTTP API, and on
single-process, spilled and distributed execution (ADR-0033).

Each policy names a table/role pair. **`columns` maps a column name to an
ACTION — `allow`, `mask` or `deny` — not to a replacement string.** The action
is matched case-insensitively; any other value is a configuration error and the
server **refuses to start**, naming the table, role, column and the value it
could not read. A cell policy never degrades to a grant, so a config written
with a replacement string fails loudly instead of silently returning the column
in full. On hot reload the same refusal keeps the previous configuration in
place rather than installing a weaker one.

```yaml
auth:
  policies:
    # Mask source IPs and filter to internal traffic for readers
    - table: flow_logs
      role: reader
      columns:
        src_ip: mask                  # action: allow | mask | deny
      row_filter: "src_ip LIKE '10.%' OR src_ip LIKE '172.16.%' OR src_ip LIKE '192.168.%'"

    # Mask raw messages for readers viewing syslog
    - table: syslog
      role: reader
      columns:
        message: mask

    # NetOps can only see production devices
    - table: device_inventory
      role: netops
      row_filter: "environment = 'production'"
```

**Column masking** (`columns: {<column>: mask}`): the column is replaced, at
the scan, by a placeholder chosen from its **declared type** — `0` for
numerics, `false` for booleans, `'***'` for strings and every other type. This
type-derived placeholder is specific to THIS legacy form; an ABAC
`mask_column` obligation must say what it means with `value:` (see below). The
replacement is **not configurable** through this section; use an ABAC
obligation with a quoted SQL expression (`value: "'REDACTED'"`) when you need a
specific value.

The masked column declares the **mask expression's** type on the wire, which
is what PostgreSQL declares for any expression in a SELECT list. `'***'` over
a text column and `0` over a numeric one keep the column's own declaration; a
mask of a different type moves it, so a `TIMESTAMP` masked with `'***'`
arrives as text and `WHERE ts > '2000-01-01'` returns no rows. Write a mask of
the column's own type (`value: "TIMESTAMP '1970-01-01'"`) when a client
depends on the declaration.

**Column denial** (`columns: {<column>: deny}`): for this identity the column
**does not exist**. It is absent from `SELECT *` and from the result schema,
and naming it anywhere in a statement — the SELECT list, a WHERE clause, an
aggregate, a derived table, a CTE, a subquery — is
`42703 unknown column "<name>"`, the same error the engine gives for a column
the table really does not have. It is not returned as NULL, and a predicate on
it is not quietly answered from the stored value.

**Row filtering** (`row_filter`): A SQL predicate automatically injected into
the query, directly above the scan and **below** the security projection. That
ordering is PostgreSQL's row-level-security semantics: the *policy's* predicate
reads the row as stored, so a `row_filter` written against a masked column
compares the true value, while a predicate the *user* writes sits above the
projection and compares the mask (`WHERE src_ip = '10.1.2.3'` returns nothing,
`WHERE src_ip = '***'` returns every visible row). The ordering holds on the
distributed path too — the scan fragment carries a filter slot on each side of
the projection.

A `row_filter` naming a column the table does not have is **refused** (42703):
it would restrict no rows, which is the same silent failure a mask that cannot
be applied used to have. It may freely name a column the same policy denies —
that is what a policy predicate is for.

Both can be combined in a single policy. A policy with only `columns` applies masking without row filtering, and vice versa.

If a column policy cannot be applied to a plan — a scan whose columns cannot be
resolved, or a table with every column denied — the query is **refused**. A
security control never degrades to a grant.

A policy binds to a **relation**, never to an alias: `FROM other AS employees`
is a scan of `other` and no policy on `employees` touches it.

The projection covers **every** read of the relation in the plan that executes,
not only the ones the statement's FROM clause names: a table referenced inside
an `IN`/`EXISTS` subquery is policed too, so a predicate written inside that
subquery reads the mask like any other.

Where the engine cannot show that a plan keeps its predicates above the
projection, the query is **refused** (`0A000`) rather than answered, loud on
every door and every execution path. What decides it is the **inner plan**,
not which tables the outer statement names:

- a subquery the planner folds into the outer plan is one plan, and it answers
  — `… IN (SELECT col FROM policed WHERE …)`, `EXISTS (…)`, `NOT IN`, a
  derived table or a CTE in the `FROM` clause, a derived table inside an `IN`
  list, a non-correlated scalar subquery;
- a subquery that keeps a plan of its own is refused: a **set operation**
  (`UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`) written inside an `IN`/`EXISTS`
  list, a **correlated scalar** subquery, and `LATERAL`. This holds whether or
  not the outer statement reads the same relation — `SELECT id FROM t WHERE id
  IN (SELECT id FROM t WHERE c > 300 UNION ALL SELECT id FROM t WHERE c > 500)`
  is refused exactly like the same subquery under an unpoliced outer.

A refusal is the answer to "this shape cannot be ordered safely", so it does
not vary with the data or with the identity's row filter: the same statement is
refused for every identity the policy covers.

### A mask is a SQL expression, and it must say what it means

An ABAC `mask_column` obligation is refused **at config load and at hot
reload** — keeping the previous policy set in place — when it:

- gives neither a `value` nor a `mask_func`. The type-derived placeholder is
  the legacy `columns: {col: mask}` form's rule, not this one;
- gives a `value` the SQL parser does not read as written. `value:
  "***REDACTED***"` parses as the expression `* * *` — the word is dropped and
  every masked column answers `0` — and `value: "[MASKED]"` does not parse at
  all. A literal string needs its quotes: `value: "'REDACTED'"`;
- gives a `value` that reads a column the same rule masks or denies. **A mask
  expression is evaluated against the row AS STORED**, below the security
  projection, so `value: "ssn"` on a masked `ssn` would publish exactly the
  value the rule hides. An expression over an unrestricted column
  (`value: "'redacted-' || dept"`) is allowed, and sees the stored row.

### DML under a policy

An `INSERT`, `UPDATE`, `DELETE` or `MERGE` is a **write**: an identity whose
policies grant it no write on the table is refused with `42501 permission
denied for table "…"` before any row is read or written.

When the write is allowed, the statement's own reads see what a `SELECT` would
see. A **denied** column does not exist inside the statement — naming it in a
predicate, as a `SET` or `INSERT` target, or inside a `SET` expression is
`42703` — and a **masked** column reads as its mask, so
`WHERE ssn = '<stored value>'` matches nothing and `SET dept = ssn` writes the
mask rather than copying the stored value into a column the identity may read.
`MERGE` against a table carrying a column policy is refused (`0A000`): its
`WHEN` clauses carry raw text the rewriter does not decompose.

### Policy Evaluation Order

1. Column references are bound against the schema **this identity** can see, so
   a denied column resolves to nothing (42703) before anything is planned
2. Table access is decided for every base table the plan reads — including the
   tables behind derived tables, CTE references and set-operation arms
3. The security projection is injected above each policed scan: masked columns
   become their mask expression, denied columns are gone
4. Row filter predicates are injected as Filter nodes between the scan and that
   projection
5. The optimizer pushes filters down to scan operators and through partition
   pruning; the security projection is a barrier the optimizer preserves
6. In distributed mode the projection is absorbed into the scan stage
   (`SecurityProjectExprs`) and applied on the worker before anything else
   consumes rows; row filters propagate via the physical plan's `FilterExprs`
7. Query executes — restricted values never leave the scan

An expression subquery (`(SELECT MAX(col) FROM t)`, an `IN` set, an `EXISTS`)
is planned under the same policies as its enclosing statement.

Admin roles are typically exempt from all policies (they see the raw data). An
identity with no matching column obligations is unaffected, and an in-process
embedded caller with no identity at all sees the raw table.

### Distributed Enforcement

In distributed mode, identity context (name, role, and attributes) is propagated from the HTTP/pgwire/gRPC server to the coordinator and stamped on every worker task. Row filters are injected at plan time and flow naturally through distributed execution — workers never see rows outside the filter predicate.

A column policy travels with the plan, not as a second decision the worker
re-derives: the security projection is absorbed into the scan stage
(`SecurityProjectExprs`) and the worker's scan fragment applies it as

    OpScan → OpFilter(the policy's row filter) → SecurityProject → OpFilter(the client's predicates)

so the projection sits between the scan and everything that consumes rows —
the aggregate, the join, the exchange. The two filter slots are the same
ordering the single-process pipeline uses and the same one § Policy Evaluation
Order describes: a policy's own `row_filter` reads the row as stored, and a
predicate the client wrote reads the mask. The coordinator refuses the query
(`0A000`) rather than dispatch it if any predicate below the projection names
a column the projection hides.

`worker/executor.go`'s `enforcePolicyDecision` re-checks the task against the
serialized decision as defense in depth, rejecting a task whose requested
columns include a denied one. It reads `PolicyDecisionJSON`, which the
coordinator does not currently populate, so today it is a guard waiting for a
producer rather than an active check — the enforcement that runs is the
plan-time one above.

**A task that carries a statement's TEXT is refused under a policy.** A worker
re-plans such a task's SQL where no policy is in reach, so the coordinator
refuses to dispatch one (`0A000`) whenever a row or column policy shaped the
query — the check sits at the single point every dispatcher publishes through.
In practice this is `POST /v1/queries/async`, which answers **403** with
SQLSTATE `0A000` for a policed identity and names `POST /v1/queries` as the
alternative; an identity with no obligations is unaffected. See ADR-0033's
not-settled list.

## Security Configuration Example

Complete security configuration for a network monitoring deployment:

```yaml
# wadjet-security.yaml

auth:
  enabled: true

  jwt:
    enabled: true
    public_key_file: /etc/wadjet/idp-public.pem
    role_claim: role
    issuer: "corporate-idp"

  api_keys:
    - key: "wadjet-ingest-key-secret"
      name: "bento-pipeline"
      role: ingest-service

  roles:
    # SOC analysts — read all data
    - name: soc-analyst
      tables: ["*"]
      allow: [read]

    # Network engineers — read network data, manage device inventory
    - name: network-engineer
      tables: [flow_logs, syslog, snmp_traps, device_inventory]
      allow: [read, write, admin]

    # Ingestion services — write only
    - name: ingest-service
      tables: [flow_logs, syslog, snmp_traps]
      allow: [write]

    # Platform admin — full access
    - name: platform-admin
      tables: ["*"]
      allow: [read, write, admin]

  policies:
    # Mask source IPs for SOC analysts
    - table: flow_logs
      role: soc-analyst
      columns:
        src_ip: mask

    # Network engineers can only see their region's devices
    - table: device_inventory
      role: network-engineer
      row_filter: "region = 'us-east-1'"
```

## Hot Reload

All security configuration changes take effect without restarting the server:

1. Edit the YAML config file
2. The file watcher detects the change
3. New config is parsed and validated
4. Auth providers are swapped atomically
5. In-flight requests continue with their existing credentials
6. New requests use the updated configuration

This enables operations like:
- Rotating API keys without downtime
- Adding/removing roles dynamically
- Updating cell-level policies in response to incidents
- Revoking access immediately

## Audit Logging

Wadjet logs security-relevant events as structured slog entries with the `component=audit` attribute. These events are emitted automatically and can be filtered and forwarded to your SIEM or log aggregation system.

### Audit Events

| Event | Level | Description |
|-------|-------|-------------|
| `query_executed` | INFO | Query completed successfully, with identity, SQL, tables, and elapsed time |
| `query_failed` | WARN | Query failed, with identity and error details |
| `access_denied` | WARN | Authorization denied access to a table or permission |
| `row_filter_applied` | INFO | Row-level security filter injected into query plan |
| `column_policy_applied` | INFO | Column masking or denial applied, listing affected columns |
| `auth_failure` | WARN | Authentication failed (bad credentials, expired token) |

### Example Log Output

```
level=INFO component=audit msg=row_filter_applied identity=analyst@corp.com role=soc-analyst table=flow_logs filter="src_ip LIKE '10.%'"
level=INFO component=audit msg=column_policy_applied identity=analyst@corp.com role=soc-analyst table=flow_logs masked_columns=[src_ip]
level=INFO component=audit msg=query_executed identity=analyst@corp.com role=soc-analyst sql="SELECT * FROM flow_logs" tables=[flow_logs] elapsed=1.23s
```

### Forwarding Audit Logs

Since audit events use standard Go slog, configure your log aggregation pipeline to filter on `component=audit`:

```bash
# journalctl filter
journalctl -u wadjet | grep 'component=audit'

# Vector/Fluentd: filter on structured field component=audit
```

## Query Cost Estimation and Guards

Wadjet estimates query cost at plan time using manifest metadata (file sizes, row counts) before any I/O occurs. Cost-based guards can reject expensive queries before they execute.

An ABAC `query_limit` obligation narrows these same ceilings for one
identity on one relation; see the obligation table above. The two meet at one
enforcement point — the guard below — so a policy ceiling applies on every door
and on the distributed path, and the tighter of the two always wins.

`query_limits:` in the config file is enforced on **every** plan a served query
can meet. Each planner-construction site installs the limits in its
constructor, so an entry point added later carries the guard by construction:

| Site | Reached by |
|---|---|
| `Coordinator` — the distributed planner, the in-process small-query fast path, the pipeline the DAG's refusals route to, and the async `SubmitSQL` planner | HTTP, gRPC, and the pgwire statements the coordinator answers |
| `wadjet.DB` — the embedded planner | the embedded Go API, and every pgwire statement the coordinator does **not** answer |
| `server.Server` — the HTTP server's own planner | `POST /v1/queries` and `EXPLAIN` on a deployment with no coordinator |

That second row is load-bearing. pgwire routes a statement to the coordinator
only when it begins with `SELECT` or `WITH ` and the connection's auth state
allows it, so a leading comment (`/* … */ SELECT …`, which JDBC, DataGrip and
dbt emit routinely), `TABLE t`, `VALUES (…)`, and — when an `auth:` block is
present but `enabled: false` — *every* statement fall back to the embedded
planner. Before the limits reached `wadjet.DB` those were unbounded, so the
guard had a hole exactly where the BI clients sit.

Per-role limits resolve from the identity on the request or connection at every site.

> **Environment variables do not set these limits.** `WADJET_QUERY_MAX_SCAN_BYTES`,
> `_ROWS` and `_FILES` are parsed by `config.applyEnvOverrides`, which nothing
> on the `serve` path calls — the same is true of every other `WADJET_*` config
> override. Use the config file.

### Configuration

```yaml
# Global query limits (apply to all users)
query_limits:
  max_scan_bytes: 107374182400       # 100 GB — reject queries scanning more
  max_scan_rows: 1000000000          # 1 billion rows
  max_scan_files: 10000              # 10,000 files
  require_filter_above_bytes: 10737418240  # Require WHERE clause for scans > 10 GB
  require_limit_above_rows: 100000000      # Require LIMIT for scans > 100M rows

auth:
  roles:
    - name: admin
      tables: ["*"]
      allow: [read, write, admin]
      # No query_limits → unlimited (overrides global)

    - name: analyst
      tables: ["*"]
      allow: [read]
      query_limits:
        max_scan_bytes: 10737418240    # 10 GB
        max_scan_rows: 100000000       # 100M rows

    - name: viewer
      tables: ["*"]
      allow: [read]
      query_limits:
        max_scan_bytes: 1073741824     # 1 GB
        max_scan_rows: 10000000        # 10M rows
        require_filter_above_bytes: 0  # Always require filter
```

### How It Works

1. The planner resolves file lists from the catalog manifest
2. Each file's `size_bytes` and `num_rows` metadata (recorded at ingest time) is summed
3. The estimated cost is checked against the applicable limits (per-role if defined, else global)
4. If any limit is exceeded, the query is rejected with a descriptive error **before any I/O**

### Per-Role Limits

Per-role `query_limits` override the global limits entirely. If a role defines `query_limits`, only those limits apply — the global limits are ignored for that role. To grant unlimited access, define a role with no `query_limits` (or set all values to 0).

### Example Error

A rejection carries SQLSTATE **`53400`** (`configuration_limit_exceeded`), so a
PostgreSQL client can tell an administrator's cap apart from a syntax or type
error and stop retrying.

```json
{
  "error": "physical plan error: query would scan 234.5GB (251804272557 bytes) across 2847 files, exceeding limit of 100.0GB \u2014 add a WHERE clause or partition filter"
}
```

## Best Practices

1. **Use mTLS for government/clearance-level deployments** — certificate-based identity is strongest
2. **Use JWT for user-facing applications** — integrate with your identity provider (Keycloak, Okta, etc.)
3. **Use API keys for service-to-service** — simpler, adequate for internal services behind a network boundary
4. **Apply least-privilege roles** — don't give `admin` to services that only need `read`
5. **Use ABAC for clearance-level or multi-tenant workloads** — attribute conditions and deny-overrides prevent over-granting
6. **Use row filters for multi-tenancy** — ensure each tenant only sees their own data
7. **Use column masking for PII** — comply with data handling policies without separate data copies
8. **Use deny rules for hard boundaries** — deny-overrides ensure narrow denials always win over broad allows
9. **Rotate credentials regularly** — hot reload makes this zero-downtime
10. **Audit access** — filter server logs on `component=audit` (no Prometheus metric carries identity) and server logs to track who queries what
