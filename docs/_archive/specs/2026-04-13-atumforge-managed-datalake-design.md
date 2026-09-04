> **ARCHIVED — superseded design note.** Kept for design lineage only; it does not describe the current code. Current positions: `docs/adr/` (decisions), `docs/internals/` (code maps), `docs/design/` (active memos). Search skips `docs/_archive/` by default (`.ignore`); use `rg --no-ignore` to include it.

# AtumForge Managed Datalake Analytics — Design Spec

> **Status:** Future vision — not for immediate implementation. Wadjet engine must reach production maturity first.
>
> **Date:** 2026-04-13
>
> **Context:** Brainstorming session exploring how to offer datalake analytics as a managed service powered by Wadjet, targeting MSSPs (Managed Security Service Providers).

---

## Product Concept

AtumForge offers managed datalake analytics to MSSPs. The MSP brings their existing datalake (S3 buckets with security telemetry); AtumForge provides the query engine, optional ingestion pipelines, and the operational expertise to run it all.

### Positioning

AtumForge is not a Splunk replacement. It complements existing SIEMs by providing a cost-effective analytics tier for historical data:

- **Splunk/Sentinel** handles real-time alerting on the hot tier (last 24-48h)
- **AtumForge** handles everything older — threat hunting, compliance queries, forensic investigations, trend analysis across months or years of retained data

The pitch: *"Keep 7 days in Splunk for alerting. Send everything to AtumForge, retain 2 years for the same price. Your analysts still use SPL."*

### Why MSSPs

MSSPs monitor dozens to hundreds of end-customers. They have massive data volumes, high retention requirements (compliance, forensics), and are acutely sensitive to per-GB ingestion costs from tools like Splunk. AtumForge's parquet-on-S3 storage model is 10-50x cheaper than Splunk indexers for the same data volume, and Wadjet's columnar engine is faster than Splunk for historical analytical queries.

### Priority Ordering

1. **Security** — data isolation between MSPs is non-negotiable
2. **Cost efficiency** — must be cheap enough to operate profitably at the per-MSP level
3. **Operational simplicity** — easy to deploy, upgrade, and monitor across many MSP accounts
4. **Noisy neighbor prevention** — one analyst's heavy query shouldn't tank others (important but lowest priority)

---

## Deployment Architecture

### Per-MSP Account Isolation

Each MSP gets a dedicated AWS account for Wadjet compute. This provides the strongest possible isolation boundary — data can never leak between MSPs because they're in entirely separate AWS accounts.

```
+----------------------------------+     +-------------------------------+
|  AtumForge Management Account    |     |  MSP's Existing AWS Account   |
|                                  |     |                               |
|  - Fleet control plane           |     |  - S3: netflow, syslog,       |
|    (v1: Terraform + CI/CD)       |     |    parquet datalake            |
|    (vision: real service)        |     |  - IAM role: wadjet-reader    |
|  - Cross-account monitoring      |     |    (read-only on data buckets) |
|  - Billing aggregation           |     |                               |
+----------------+-----------------+     +---------------+---------------+
                 |                                       |
                 | manages                               | assume-role
                 v                                       v
+----------------------------------------------------------------+
|  MSP "Acme Security" — Wadjet Compute Account                  |
|                                                                |
|  VPC (private subnets)                                         |
|  +-------------+  +-----------+  +-----------+                 |
|  | Coordinator |  | Worker(1) |  | Worker(N) |                 |
|  | (standalone)|  |           |  |           |                 |
|  | + pgwire    |  |           |  |           |                 |
|  | + NATS      |  |           |  |           |                 |
|  +-------------+  +-----------+  +-----------+                 |
|                                                                |
|  S3: wadjet-internal (spill, catalog snapshots, result cache)  |
|  IAM role: wadjet-compute (assumes MSP's wadjet-reader role)   |
+----------------------------------------------------------------+
```

**Three accounts per MSP:**

1. **AtumForge management account** — runs the fleet control plane, monitoring, billing
2. **MSP's existing account** — their data stays here, AtumForge never moves it
3. **Wadjet compute account** — dedicated per MSP, AtumForge owns and manages it

The compute account stores no customer data at rest. Data is read from the MSP's S3 via cross-account assume-role, results are returned over pgwire, spill files are ephemeral. If the compute account is compromised, the blast radius is limited — the MSP's IAM role is read-only and scoped to specific buckets.

### Cross-Account Data Access

Wadjet connects to the MSP's S3 buckets via IAM assume-role with an external ID to prevent confused deputy attacks.

**MSP-side setup:**
- Create IAM role `wadjet-reader` with read-only S3 access to their data buckets
- Trust policy allows the Wadjet compute account to assume it
- External ID shared out-of-band during onboarding

**Wadjet-side config:**
```yaml
storage:
  type: s3
  region: us-east-2
  bucket: acme-security-datalake
  assume_role_arn: arn:aws:iam::123456789:role/wadjet-reader
  external_id: atumforge-acme-2026
```

**What changes in Wadjet:** The object store layer (`MinIOStore`) needs an assume-role credential provider using AWS STS. The `Store` interface does not change — this is a credential concern, not an API concern. The AWS SDK credential chain handles assume-role natively.

### Fleet Control Plane

**v1 (Approach A — start here):** Terraform/OpenTofu modules + CI/CD pipeline. Each MSP gets a `tfvars` file with their config (instance types, data bucket ARN, assume-role ARN). Updates are applied via pipeline. Monitoring via CloudWatch cross-account or Grafana.

**Vision (Approach C — evolve toward):** A real management service that provisions MSP accounts, pushes Wadjet updates, auto-scales clusters, aggregates billing, and provides a management console. This becomes necessary when MSP count exceeds what manual Terraform management can handle (roughly 20-50 MSPs).

---

## SPL and KQL Translation Layers

### Motivation

The single biggest adoption barrier for MSSPs is analyst retraining. SOC analysts know SPL (Splunk) or KQL (Microsoft Sentinel). Asking them to learn SQL is friction that kills deals. Translation layers eliminate this: analysts paste existing queries and they work.

### Architecture

Translation happens entirely in the parser/planner front-end. The engine does not change.

```
SPL text  --> SPL Parser  --> Wadjet SQL AST --> logical plan --> physical plan --> execution
KQL text  --> KQL Parser  --> Wadjet SQL AST --> logical plan --> physical plan --> execution
SQL text  --> SQL Parser  --> Wadjet SQL AST --> logical plan --> physical plan --> execution
```

The pgwire connection or HTTP API detects the query language (explicit flag, or heuristic: pipes = SPL, `|` at start = KQL, otherwise SQL) and routes to the appropriate parser.

### SPL Translation

**Core commands to support (covers ~90% of real analyst queries):**

| SPL Command | SQL Equivalent |
|---|---|
| `search index=X` | `FROM <resolved tables>` |
| `where <expr>` | `WHERE <expr>` |
| `stats count/sum/avg by field` | `GROUP BY field` + aggregates |
| `eval field = expr` | `SELECT expr AS field` |
| `sort -field` | `ORDER BY field DESC` |
| `head N` / `tail N` | `LIMIT N` |
| `table field1, field2` | `SELECT field1, field2` |
| `rename old AS new` | `SELECT old AS new` |
| `dedup field` | `DISTINCT ON (field)` or window function |
| `timechart span=1h count` | `GROUP BY time_bucket('1 hour', _time)` |
| `top N field` | `GROUP BY + ORDER BY + LIMIT` |
| `rare field` | `GROUP BY + ORDER BY ASC + LIMIT` |
| `lookup table field` | `JOIN` |
| `rex field=_raw "(?<name>pattern)"` | `regexp_extract()` function |
| `earliest=-24h latest=now` | `WHERE _time >= NOW() - INTERVAL '24 hours'` |

**Deferred (diminishing returns):**

- `transaction` — complex sessionization, would need a custom window function or operator
- `tstats` — Splunk-specific acceleration structure, no equivalent
- `subsearch` — translates to subqueries, but implicit behavior is subtle
- `macros` — would need a macro expansion layer
- `datamodel` — Splunk's CIM; AtumForge would define its own equivalent

**Parser approach:** Pike-style lexer with state functions (same pattern as Wadjet's existing SQL parser). SPL's pipe-forward syntax is regular and straightforward to tokenize. Each pipe stage maps to a SQL clause or subquery.

### KQL Translation

KQL is cleaner than SPL and maps more directly to SQL. Would be easier to implement first.

| KQL Operator | SQL Equivalent |
|---|---|
| `TableName \| where expr` | `FROM TableName WHERE expr` |
| `summarize count() by field` | `GROUP BY field` + aggregates |
| `project field1, field2` | `SELECT field1, field2` |
| `extend new_field = expr` | `SELECT *, expr AS new_field` |
| `sort by field desc` | `ORDER BY field DESC` |
| `top N by field` | `ORDER BY + LIMIT` |
| `join kind=inner (T2) on key` | `JOIN T2 ON key` |
| `render timechart` | Output format hint (no SQL change) |
| `ago(24h)` | `NOW() - INTERVAL '24 hours'` |

**Implementation order:** KQL first (cleaner, growing Sentinel market share), then SPL core subset.

---

## Index Registry and Sourcetype Abstraction

### Problem

SPL's `index=firewall` is not just `FROM firewall`. It implies:
- Routing to multiple sourcetype tables within that index
- Implicit time-range scoping via `_time`
- Common metadata fields (`host`, `source`, `sourcetype`)
- Field extraction rules per sourcetype

Wadjet needs a metadata layer that models these concepts so the SPL translator can resolve index references into real table queries.

### Design

An **Index Registry** in the catalog — a metadata structure that maps Splunk-style index names to sets of sourcetype tables:

```
Index "firewall" {
    sourcetypes:
        "pan:traffic"    -> table firewall.pan_traffic
        "cisco:asa"      -> table firewall.cisco_asa
        "fortinet:utm"   -> table firewall.fortinet_utm
    time_column: _time
    partition_by: day(_time)
    common_columns: [_time, host, source, sourcetype]
}
```

**Resolution rules:**

- `index=firewall` → `UNION ALL` across all sourcetype tables in the index
- `index=firewall sourcetype=pan:traffic` → single table `firewall.pan_traffic`
- `index=firewall earliest=-24h` → add `WHERE _time >= NOW() - INTERVAL '24 hours'` with partition pruning
- `index=*` → all indexes (all tables)

### Schema-on-Write vs Schema-on-Read

Splunk uses schema-on-read: stores raw text, extracts fields at search time via regex. This is flexible but slow.

Wadjet uses schema-on-write: fields are parsed and typed at ingest time, stored as typed parquet columns. This is 10-100x faster for queries.

**AtumForge's approach:**
- Schema-on-write by default — ingestion pipelines parse raw data into typed columns based on pre-built sourcetype definitions
- AtumForge ships with sourcetype definitions for common security data formats (Palo Alto, Fortinet, CrowdStrike, Cisco ASA, Windows Event Logs, netflow v5/v9/IPFIX, syslog RFC 3164/5424)
- Fallback `_raw` TEXT column for unrecognized formats, with `rex()` for search-time extraction when needed

### Catalog Changes

The existing Wadjet catalog stores tables with parquet schemas. The Index Registry adds a layer on top:

```go
type IndexDef struct {
    Name           string                      // "firewall"
    Sourcetypes    map[string]string           // "pan:traffic" -> "firewall.pan_traffic"
    TimeColumn     string                      // "_time"
    CommonColumns  []string                    // ["_time", "host", "source", "sourcetype"]
}
```

Stored in the NATS KV catalog alongside table metadata. The SPL translator queries the registry during parsing to resolve index references.

---

## Intra-Cluster Resource Isolation

Within a single MSP's Wadjet cluster, multiple analysts query concurrently. A heavy threat-hunting query across 6 months of netflow should not starve a dashboard refresh.

Given the priority ordering (noisy neighbor is #4), the approach is lightweight — use what exists plus a query kill switch, not a full resource governor.

### What Already Exists

- Per-role query cost limits (`MaxScanBytes`, `MaxScanRows`, `MaxScanFiles`)
- Per-task memory budgets with spill-to-disk
- Query concurrency semaphore (default 64)
- Per-identity rate limiting (token bucket)
- Query timeouts (per-connection `statement_timeout`)
- Slow query logging

### What to Add (Minimal)

1. **Query scan-byte kill switch** — today, `MaxScanBytes` is enforced at planning time (estimated). Add runtime enforcement: if a query's actual scan bytes exceed the limit, kill it mid-execution. This is the single most important noisy-neighbor protection for a datalake workload.

2. **Per-role concurrency pools** — instead of one global semaphore, split it: e.g., "analyst" role gets 8 concurrent queries, "dashboard" role gets 16, "admin" gets 4. Prevents one role from monopolizing the cluster.

3. **Usage metering** — track per-identity query count, scan bytes, wall time, and peak memory. Required for billing and for MSPs to understand their own usage patterns. Emit as structured logs or Prometheus metrics.

### What Not to Build (Yet)

- Per-tenant memory quotas — overkill when each MSP has their own cluster
- Priority queues with preemption — complex, noisy neighbor is priority #4
- Worker affinity per end-customer — unnecessary at MSP cluster scale
- Tenant-scoped spill directories — single-tenant cluster, not needed

---

## What Changes in Wadjet vs. Infrastructure

### Wadjet Engine Changes (Required)

| Change | Scope | Notes |
|---|---|---|
| Assume-role S3 credential provider | `internal/storage/objstore/` | AWS STS assume-role with external ID |
| SPL parser + translator | `internal/planner/spl/` (new) | Pike-style lexer, translates to SQL AST |
| KQL parser + translator | `internal/planner/kql/` (new) | Cleaner, implement first |
| Index Registry in catalog | `internal/storage/catalog/` | IndexDef metadata, resolution API |
| Runtime scan-byte enforcement | `internal/engine/scan/` | Kill query if actual scan exceeds budget |
| Per-role concurrency pools | `internal/coordinator/` | Split the query semaphore by role |
| Usage metering | `internal/coordinator/` | Per-identity metrics emission |

### Infrastructure / Ops Work (Required)

| Work | Scope | Notes |
|---|---|---|
| Terraform module for MSP account | `deploy/` | VPC, EC2, IAM, S3, security groups |
| CI/CD pipeline for fleet updates | AtumForge repo | Apply Terraform + deploy Wadjet binary |
| Cross-account monitoring | AtumForge repo | CloudWatch or Grafana for fleet health |
| Sourcetype definition library | AtumForge repo | Pre-built schemas for common security data |
| Ingestion pipeline framework | `internal/storage/ingest/` or separate | Netflow, syslog, raw log parsers |

### Not Required (Account Isolation Handles It)

- Per-tenant catalogs / database namespaces
- Tenant-aware NATS subject routing
- Worker pool partitioning by tenant
- Cross-tenant query prevention in the engine

---

## Open Questions

1. **Pricing model** — per-TB-scanned (like Athena), per-GB-retained, flat monthly tier, or usage-based blend? Impacts metering requirements.

2. **Ingestion scope** — does AtumForge handle ingestion from day one, or is it query-only initially with ingestion as an upsell? Ingestion adds significant engineering scope (parsers for each data format, delivery guarantees, backpressure).

3. **Auto-scaling** — can workers scale to zero when idle to minimize cost? What's the cold-start latency if a coordinator needs to warm up? Does the pgwire endpoint need to be always-on?

4. **Existing data formats** — will MSPs have data in formats other than parquet (CSV, JSON, Avro)? Wadjet reads parquet natively; other formats would need conversion at ingest or a shim reader.

5. **Compliance / certifications** — will AtumForge need SOC 2, FedRAMP, or other certifications to sell to MSSPs handling government data? This impacts infrastructure choices (GovCloud, dedicated tenancy, encryption requirements).

6. **SPL/KQL fidelity bar** — what level of compatibility is "good enough"? If 90% of queries work, is the 10% failure rate acceptable or a deal-breaker? Error messages for unsupported commands need to be clear and actionable.

7. **Multi-region** — will MSPs have data in different AWS regions? Cross-region S3 reads have latency and egress cost implications. Wadjet compute should be co-located with the data.

---

## Summary

AtumForge positions Wadjet as the analytical engine underneath a managed datalake service for MSSPs. Per-MSP AWS account isolation provides security without complex multi-tenancy engineering. SPL/KQL translation layers eliminate adoption friction by letting analysts use the query languages they already know. The Index Registry and sourcetype abstraction bridge the gap between Splunk's data model and Wadjet's relational catalog.

The approach is deliberately incremental:

1. **Now:** Prove Wadjet in production (engine maturity, operational hardening)
2. **Next:** Add assume-role S3 access + basic fleet management (Terraform modules)
3. **Then:** KQL translator (easier, growing market)
4. **Then:** SPL translator (core subset, ~30 commands)
5. **Then:** Index Registry + sourcetype library
6. **Later:** Control plane service (Approach C), auto-scaling, ingestion pipelines
