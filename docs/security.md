# Security

Caelum provides layered security: authentication, role-based authorization, and cell-level access policies. All security configuration is hot-reloadable.

## Authentication

Three authentication methods are supported. Configure one in the YAML config file.

### API Keys

Simple bearer token authentication. Best for service-to-service communication.

```yaml
auth:
  method: apikey
  api_keys:
    - key: "caelum-ingest-key-abc123"
      identity: "ingest-pipeline"
      roles:
        - writer
    - key: "caelum-dashboard-key-xyz789"
      identity: "grafana-dashboard"
      roles:
        - reader
    - key: "caelum-admin-key-000000"
      identity: "admin-operator"
      roles:
        - admin
```

Usage:

```bash
curl -H "Authorization: Bearer caelum-ingest-key-abc123" http://localhost:8080/v1/queries ...
```

### JWT

JSON Web Token authentication. Best for user-facing applications where an identity provider issues tokens.

**HMAC-SHA256:**

```yaml
auth:
  method: jwt
  jwt:
    signing_method: hmac
    secret: "your-256-bit-secret-here"
    subject_claim: sub  # JWT claim containing user identity
```

**RSA:**

```yaml
auth:
  method: jwt
  jwt:
    signing_method: rsa
    public_key: /etc/caelum/jwt-public.pem
    subject_claim: sub
```

The JWT payload must include a `roles` claim (array of role names) for authorization:

```json
{
  "sub": "jdoe@example.com",
  "roles": ["reader"],
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
  method: mtls
  mtls:
    ca_cert: /etc/caelum/ca.pem
    identity_field: common_name  # or "serial", "dns_san"
```

The server validates client certificates against the configured CA. Identity is extracted from the certificate field specified by `identity_field`.

Usage:

```bash
curl --cert client.pem --key client-key.pem --cacert ca.pem https://caelum.internal:8443/v1/queries ...
```

## Authorization

Role-based access control (RBAC) maps authenticated identities to permissions on tables.

### Role Definitions

```yaml
roles:
  # Read-only access to all tables
  reader:
    tables:
      "*":
        permissions:
          - read

  # Read/write access to specific tables
  writer:
    tables:
      flow_logs:
        permissions:
          - read
          - write
      syslog:
        permissions:
          - read
          - write

  # Network operations — read everything, write device tables
  netops:
    tables:
      "*":
        permissions:
          - read
      device_inventory:
        permissions:
          - read
          - write

  # Full administrative access
  admin:
    tables:
      "*":
        permissions:
          - read
          - write
          - admin
```

### Permission Types

| Permission | Allows |
|-----------|-------|
| `read` | Execute SELECT queries against the table |
| `write` | Ingest data into the table (via API or embedded) |
| `admin` | Create/drop the table, modify its schema |

### Permission Resolution

1. Look up the user's roles from their authentication credentials
2. For each table referenced in the query, check if any role grants the required permission
3. Wildcard (`"*"`) matches all tables
4. Specific table entries take precedence over wildcards
5. If no role grants the required permission, the request is denied with 403 Forbidden

## Cell-Level Policies

Cell-level policies provide fine-grained data access control, applied **after** query execution but **before** results are returned to the client.

### Column Masking

Replace column values with a masked value for specific roles:

```yaml
policies:
  - name: mask-source-ips
    description: "Mask source IPs for non-admin users"
    table: flow_logs
    type: column_mask
    column: src_ip
    mask_value: "***REDACTED***"
    applies_to:
      roles:
        - reader

  - name: mask-messages
    description: "Truncate syslog messages for reader role"
    table: syslog
    type: column_mask
    column: message
    mask_value: "[MASKED]"
    applies_to:
      roles:
        - reader
```

When a user with the `reader` role queries `flow_logs`, the `src_ip` column values are replaced with `"***REDACTED***"` in the response.

### Row Filtering

Automatically inject WHERE clause conditions to restrict which rows a role can see:

```yaml
policies:
  - name: internal-traffic-only
    description: "Readers can only see internal network traffic"
    table: flow_logs
    type: row_filter
    filter: "src_ip LIKE '10.%' OR src_ip LIKE '172.16.%' OR src_ip LIKE '192.168.%'"
    applies_to:
      roles:
        - reader

  - name: production-devices-only
    description: "NetOps can only see production device data"
    table: device_inventory
    type: row_filter
    filter: "environment = 'production'"
    applies_to:
      roles:
        - netops
```

When a `reader` queries `flow_logs`, the row filter is silently applied — they only see rows where `src_ip` matches internal RFC 1918 ranges.

### Policy Evaluation Order

1. Query executes normally against full data
2. Row filter policies are applied (rows not matching the filter are removed)
3. Column mask policies are applied (column values are replaced)
4. Filtered and masked results are returned to the client

Admin roles are typically exempt from all policies (they see the raw data).

## Security Configuration Example

Complete security configuration for a network monitoring deployment:

```yaml
# caelum-security.yaml

auth:
  method: jwt
  jwt:
    signing_method: rsa
    public_key: /etc/caelum/idp-public.pem
    subject_claim: sub

roles:
  # SOC analysts — read all data, see everything
  soc-analyst:
    tables:
      "*":
        permissions:
          - read

  # Network engineers — read network data, manage device inventory
  network-engineer:
    tables:
      flow_logs:
        permissions:
          - read
      syslog:
        permissions:
          - read
      snmp_traps:
        permissions:
          - read
      device_inventory:
        permissions:
          - read
          - write
          - admin

  # Ingestion services — write only
  ingest-service:
    tables:
      flow_logs:
        permissions:
          - write
      syslog:
        permissions:
          - write
      snmp_traps:
        permissions:
          - write

  # Platform admin — full access
  platform-admin:
    tables:
      "*":
        permissions:
          - read
          - write
          - admin

policies:
  # Mask source IPs for SOC analysts reviewing external traffic
  - name: mask-external-sources
    table: flow_logs
    type: column_mask
    column: src_ip
    mask_value: "EXTERNAL"
    applies_to:
      roles:
        - soc-analyst
    # Only apply when the source is external (custom logic would need code support)

  # Network engineers can only see their region's devices
  - name: region-filter
    table: device_inventory
    type: row_filter
    filter: "region = 'us-east-1'"
    applies_to:
      roles:
        - network-engineer
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

## Best Practices

1. **Use mTLS for government/clearance-level deployments** — certificate-based identity is strongest
2. **Use JWT for user-facing applications** — integrate with your identity provider (Keycloak, Okta, etc.)
3. **Use API keys for service-to-service** — simpler, adequate for internal services behind a network boundary
4. **Apply least-privilege roles** — don't give `admin` to services that only need `read`
5. **Use row filters for multi-tenancy** — ensure each tenant only sees their own data
6. **Use column masking for PII** — comply with data handling policies without separate data copies
7. **Rotate credentials regularly** — hot reload makes this zero-downtime
8. **Audit access** — use Prometheus metrics and server logs to track who queries what
