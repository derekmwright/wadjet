# Security

Wadjet provides layered security: authentication, role-based authorization, and cell-level access policies. All security configuration is hot-reloadable.

## Authentication

Three authentication methods are supported. Configure one in the YAML config file.

### API Keys

Simple bearer token authentication. Best for service-to-service communication. Each key maps to a name (identity label) and a single role.

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
curl --cert client.pem --key client-key.pem --cacert ca.pem https://wadjet.internal:8443/v1/queries ...
```

## Authorization

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

## Cell-Level Policies

Cell-level policies provide fine-grained data access control. Row filters are injected into the query plan **before** execution (pushed down to scan operators for distributed enforcement). Column masking is applied **after** execution but **before** results are returned.

Policies combine column masking and row filtering in a single definition per table/role pair:

```yaml
auth:
  policies:
    # Mask source IPs and filter to internal traffic for readers
    - table: flow_logs
      role: reader
      columns:
        src_ip: "***REDACTED***"      # Column name -> mask replacement value
      row_filter: "src_ip LIKE '10.%' OR src_ip LIKE '172.16.%' OR src_ip LIKE '192.168.%'"

    # Mask raw messages for readers viewing syslog
    - table: syslog
      role: reader
      columns:
        message: "[MASKED]"

    # NetOps can only see production devices
    - table: device_inventory
      role: netops
      row_filter: "environment = 'production'"
```

**Column masking** (`columns`): A map of column name to replacement value. When the specified role queries this table, the column values are replaced with the mask string in the response.

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

In distributed mode, identity context (name and role) is propagated from the HTTP server to the coordinator and stamped on every worker task. Row filters are injected at plan time and flow naturally through distributed execution — workers never see rows outside the filter predicate.

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
        src_ip: "EXTERNAL"

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
level=INFO component=audit msg=column_policy_applied identity=analyst@corp.com role=soc-analyst table=flow_logs masked_columns=[src_ip] denied_columns=[]
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

### Environment Variable Overrides

Global query limits can also be set via environment variables (useful for container deployments):

| Variable | Description |
|----------|-------------|
| `WADJET_QUERY_MAX_SCAN_BYTES` | Max bytes to scan per query |
| `WADJET_QUERY_MAX_SCAN_ROWS` | Max rows to scan per query |
| `WADJET_QUERY_MAX_SCAN_FILES` | Max files to scan per query |

### Example Error

```json
{
  "error": "query exceeds scan size limit: estimated 234.5 GB across 2,847 files, limit is 100.0 GB. Add a WHERE clause to narrow the scan."
}
```

## Best Practices

1. **Use mTLS for government/clearance-level deployments** — certificate-based identity is strongest
2. **Use JWT for user-facing applications** — integrate with your identity provider (Keycloak, Okta, etc.)
3. **Use API keys for service-to-service** — simpler, adequate for internal services behind a network boundary
4. **Apply least-privilege roles** — don't give `admin` to services that only need `read`
5. **Use row filters for multi-tenancy** — ensure each tenant only sees their own data
6. **Use column masking for PII** — comply with data handling policies without separate data copies
7. **Rotate credentials regularly** — hot reload makes this zero-downtime
8. **Audit access** — use Prometheus metrics and server logs to track who queries what
