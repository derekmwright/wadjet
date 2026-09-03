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
| `row_filter` | table name | SQL predicate | Injected as WHERE clause before execution |
| `mask_column` | column name | replacement string | Column values replaced with mask value |
| `deny_column` | column name | — | Column excluded from results entirely |
| `query_limit` | — | row count | **Not enforced** — the policy evaluator skips this obligation. |

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

Cell-level policies provide fine-grained data access control. Row filters are injected into the query plan **before** execution (pushed down to scan operators for distributed enforcement). Column masking is applied **after** execution but **before** results are returned.

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

**Column masking** (`columns: {<column>: mask}`): the value is replaced by a
type-appropriate placeholder — `"***"` for strings, `0` for numbers, `false`
for booleans. The replacement is **not configurable** through this section; use
an ABAC `mask_column` obligation with a quoted SQL expression
(`value: "'REDACTED'"`) when you need a specific value.

**Column denial** (`columns: {<column>: deny}`): the column is dropped from results.

**Row filtering** (`row_filter`): A SQL predicate automatically injected into the query. Only rows matching the filter are returned to the role.

Both can be combined in a single policy. A policy with only `columns` applies masking without row filtering, and vice versa.

### Policy Evaluation Order

1. Row filter predicates are injected into the logical query plan as additional Filter nodes above scan operators
2. The optimizer pushes these filters down to scan operators and through partition pruning
3. In distributed mode, row filters propagate to worker tasks via the physical plan's `FilterExprs`
4. Query executes — workers only scan rows matching the filter
5. Column mask policies are applied to the results before returning to the client

Admin roles are typically exempt from all policies (they see the raw data).

### Distributed Enforcement

In distributed mode, identity context (name, role, and attributes) is propagated from the HTTP/pgwire/gRPC server to the coordinator and stamped on every worker task. Row filters are injected at plan time and flow naturally through distributed execution — workers never see rows outside the filter predicate.

For ABAC, pre-evaluated policy decisions are serialized as JSON into the distributed task's `PolicyDecisionJSON` field. Workers enforce column denial, column masking, and row filters from these decisions without needing access to the full policy set.

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

`query_limits:` in the config file is enforced on **every** plan a served query
can meet. There are two planner-construction sites in the process and both
carry the limits:

| Site | Reached by |
|---|---|
| `Coordinator` — the distributed planner, the in-process small-query fast path, the pipeline the DAG's refusals route to, and the async `SubmitSQL` planner | HTTP, gRPC, and the pgwire statements the coordinator answers |
| `wadjet.DB` — the embedded planner | the embedded Go API, and every pgwire statement the coordinator does **not** answer |

That second row is load-bearing. pgwire routes a statement to the coordinator
only when it begins with `SELECT` or `WITH ` and the connection's auth state
allows it, so a leading comment (`/* … */ SELECT …`, which JDBC, DataGrip and
dbt emit routinely), `TABLE t`, `VALUES (…)`, and — when an `auth:` block is
present but `enabled: false` — *every* statement fall back to the embedded
planner. Before the limits reached `wadjet.DB` those were unbounded, so the
guard had a hole exactly where the BI clients sit.

Per-role limits resolve from the identity on the connection on both sites.

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
