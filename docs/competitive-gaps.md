# SMB Competitive Gap Analysis

> **Context**: Wadjet powers [Warden](https://citc.tech), a security analytics product. This analysis identifies gaps relative to competitors in the SMB security analytics and embedded analytics engine markets.

## Competitive Landscape

Wadjet competes at two levels:

1. **As an embedded engine** — against DuckDB, ClickHouse, Apache Datafusion, QuestDB
2. **As the engine behind Warden** — against Elasticsearch/OpenSearch, Splunk, CrowdStrike LogScale (Humio), Gravwell, Graylog, Sumo Logic

### Wadjet's Unique Strengths

| Strength | Competitor Comparison |
|----------|----------------------|
| 21 native types including IPv4, IPv6, CIDR, MAC, Port, Protocol | No competitor has first-class network types; all require string parsing |
| 80+ network functions (JA3, DNS, TLS, HTTP, TCP flags, GeoIP) | Splunk has similar via SPL; no SQL engine matches this |
| Single Go binary, 512 MB viable | DuckDB is single-binary but C++; ClickHouse needs 4+ GB |
| Distributed over NATS + S3 with zero-coordinator-bottleneck | DuckDB is single-node; ClickHouse needs ZooKeeper/Keeper |
| PostgreSQL wire protocol (psql, JDBC, Superset, DBeaver) | ClickHouse has its own protocol; DuckDB has no server mode |
| RBAC + ABAC with row filtering and column masking | Rare in embedded engines; common only in enterprise SIEM |
| MCP server for AI agent integration | No competitor offers this |
| Merge-on-read DML (INSERT, UPDATE, DELETE) | DuckDB has DML; ClickHouse UPDATE/DELETE are async mutations |

---

## Critical Gaps (High Priority)

### Gap 1: No Managed Cloud Offering

**Impact**: Blocker for SMB adoption. SMBs don't run infrastructure.

**Competitors**:
- ClickHouse Cloud: fully managed, usage-based pricing, $0.14/GB compressed storage
- Elasticsearch Cloud: managed on AWS/GCP/Azure
- Splunk Cloud: SaaS, no infra management
- Sumo Logic: cloud-native from day one
- CrowdStrike LogScale: SaaS with per-GB/day pricing

**What's needed**: A hosted Warden offering where customers provide data sources and get dashboards. Wadjet's standalone mode + S3 + NATS architecture is already cloud-ready, but packaging, billing, tenant isolation, and onboarding UX are missing.

**Recommendation**: This is a Warden-level concern, not a Wadjet engine concern. Wadjet's multi-tenant ABAC + row filtering provides the foundation. Priority: **business decision, not engineering blocker**.

---

### Gap 2: No Built-in Alerting or Detection Rules Engine

**Impact**: Critical for security analytics. Every SIEM competitor has this.

**Competitors**:
- Splunk: Correlation searches, adaptive response actions, SOAR integration
- Elasticsearch: Watcher + detection rules with MITRE ATT&CK mapping
- CrowdStrike LogScale: Scheduled searches with alerts, webhook/email/PagerDuty
- Gravwell: Scheduled searches with notification targets
- Graylog: Alert conditions, event definitions, notification routing

**What's needed**:
- Scheduled query execution (cron-based)
- Threshold/anomaly alert conditions
- Notification targets (email, webhook, Slack, PagerDuty)
- Alert state management (active, acknowledged, resolved)
- Detection rule library (sigma rule compatibility would be ideal)

**Recommendation**: Add a `CREATE ALERT` DDL or a scheduled query system. This can be implemented at the Warden layer using Wadjet's query API, but some engine support (materialized views, incremental queries) would make it far more efficient.

---

### Gap 3: No Materialized Views or Continuous Aggregation

**Impact**: High. Dashboard workloads and alerting both need pre-computed rollups.

**Competitors**:
- ClickHouse: Materialized views with automatic incremental updates on INSERT
- QuestDB: Continuous aggregation for time-series
- Elasticsearch: Transforms (continuous aggregation)
- TimescaleDB: Continuous aggregates with automatic refresh

**What's needed**:
- `CREATE MATERIALIZED VIEW ... AS SELECT ...` with periodic or on-ingest refresh
- Time-bucketed rollups (1m, 5m, 1h, 1d) for dashboards
- Retention policies (keep detailed data 7d, rollups 90d, summaries 1y)

**Recommendation**: High priority for engine development. Materialized views make dashboards fast and alerting efficient. Start with periodic refresh (cron-based re-execution), then add incremental refresh for append-only tables.

---

### Gap 4: No Data Retention Policies

**Impact**: High for compliance and cost management. SMBs need automated lifecycle management.

**Competitors**:
- Elasticsearch: Index Lifecycle Management (ILM) with hot/warm/cold/frozen tiers
- Splunk: Retention policies per index
- ClickHouse: TTL on tables and columns with automatic deletion or tiering
- CrowdStrike LogScale: Per-repository retention periods

**What's needed**:
- `ALTER TABLE ... SET RETENTION 90 DAYS`
- Automatic deletion of expired partitions
- Optional tiering (move old data to cheaper S3 storage class)
- Per-table or per-partition retention rules

**Recommendation**: Implement at the catalog/storage layer. Partition-based retention is straightforward since Wadjet already supports time-based partitioning. A background goroutine that scans partition metadata and deletes expired files from S3 would cover 90% of use cases.

---

### Gap 5: No Streaming Ingestion from Message Queues

**Impact**: Medium-high. Many SMB security deployments use syslog→Kafka or cloud pub/sub.

**Competitors**:
- ClickHouse: Native Kafka engine, RabbitMQ engine, NATS engine
- Elasticsearch: Logstash (Kafka/Beats/syslog input), Elastic Agent
- Splunk: Universal Forwarder, HEC (HTTP Event Collector), modular inputs
- CrowdStrike LogScale: Log shippers, Kafka ingest, syslog

**Current state**: Wadjet has micro-batch ingestion via Go API, HTTP REST, and gRPC. External tools like Bento handle Kafka/syslog/NetFlow → Parquet → S3. This works but adds operational complexity.

**What's needed**:
- Built-in syslog receiver (UDP/TCP, RFC5424)
- Built-in Kafka consumer (at minimum)
- HTTP Event Collector endpoint (HEC-compatible for Splunk migration)

**Recommendation**: The Bento-based approach is architecturally sound and avoids bloating the engine. However, a built-in syslog receiver would eliminate the most common "extra component" for SMB deployments. Consider a lightweight syslog input as part of the ingest package, and keep Kafka/complex pipelines as Bento's domain.

---

### Gap 6: Limited Connector Ecosystem for Data Sources

**Impact**: Medium. Limits "query anything" use cases.

**Competitors**:
- DuckDB: 50+ extensions (Postgres, MySQL, SQLite, HTTP, Delta Lake, Iceberg, S3, GCS, Azure)
- ClickHouse: 30+ table engines and integrations
- Trino/Presto: 40+ connectors

**Current state**: Wadjet has `postgres_scan`, `mysql_scan`, `read_json`, `read_csv`, `read_parquet`, and Iceberg (read-only). This covers the basics but lacks:
- SQLite connector
- Delta Lake support
- Azure Blob / GCS native support (only S3-compatible)
- LDAP/Active Directory lookup (valuable for security enrichment)

**Recommendation**: Medium priority. The current connectors cover the primary use cases for security analytics (Parquet on S3, external Postgres/MySQL for enrichment). Add connectors based on Warden customer demand rather than trying to match DuckDB's breadth.

---

## Moderate Gaps (Medium Priority)

### Gap 7: No Web UI or Query Explorer

**Impact**: Medium for engine; high for product. Warden likely provides its own UI.

**Competitors**:
- ClickHouse: Built-in Play UI, plus Grafana/Superset
- Elasticsearch: Kibana (full analytics UI)
- Splunk: Full web UI with dashboards, search, reports
- Gravwell: Built-in web UI with query builder

**Current state**: Wadjet has pgwire compatibility (works with Superset, DBeaver, psql) and MCP for AI agents. No built-in web UI.

**Recommendation**: This is a Warden concern, not a Wadjet engine concern. Wadjet's pgwire compatibility means any BI tool works. The Superset integration in `deploy/superset/` already provides a zero-cost dashboard layer. Engine work should focus on making pgwire faster and more compatible, not building a UI.

---

### Gap 8: No Full-Text Search

**Impact**: Medium-high for security analytics (log search is a core SIEM use case).

**Competitors**:
- Elasticsearch: World-class full-text search with BM25, fuzzy, phrase, regexp
- Splunk: Field extraction + search with wildcards, regex, subsearch
- CrowdStrike LogScale: Regex search across raw events
- Gravwell: Full raw data search

**Current state**: Wadjet has `LIKE`, `ILIKE`, and regex via scalar functions. No inverted index, no full-text search optimization.

**What's needed**:
- At minimum: optimized regex search with Parquet predicate pushdown on string columns
- Ideally: optional inverted index for high-cardinality string columns (log messages, URLs, user agents)

**Recommendation**: For security analytics, most "search" is structured (filter by IP, port, protocol, time range) where Wadjet excels. Unstructured log search is the gap. A pragmatic approach: optimize string predicate pushdown in the scan layer and add a `CONTAINS` function that can leverage Parquet bloom filters. Full inverted indexing is a large effort with unclear ROI given Wadjet's columnar strengths.

---

### Gap 9: No Backup/Restore or Point-in-Time Recovery

**Impact**: Medium. Required for compliance (SOC 2, HIPAA).

**Current state**: Data lives on S3 (inherently durable). Catalog metadata is in NATS KV (can be reconstructed from S3 manifests). No explicit backup/restore commands.

**What's needed**:
- `BACKUP DATABASE TO 's3://backup-bucket/'`
- `RESTORE DATABASE FROM 's3://backup-bucket/' AS OF '2024-01-15T00:00:00Z'`
- Or at minimum: documentation for S3 bucket replication + NATS KV snapshot

**Recommendation**: Low engine priority since S3 provides durability. Document the manual backup procedure (S3 cross-region replication + NATS KV export). Add explicit backup/restore commands as a convenience feature later.

---

### Gap 10: No Query Result Caching

**Impact**: Medium. Dashboard refresh performance.

**Competitors**:
- ClickHouse: Query cache with TTL
- Elasticsearch: Request cache, shard-level cache
- Superset: Built-in query result cache

**Current state**: Every query re-scans from S3/object store. The worker LRU cache helps with repeated file reads, but query results are not cached.

**Recommendation**: For dashboard workloads, materialized views (Gap 3) solve this better than result caching. If implemented, a simple TTL-based query result cache at the coordinator level would be straightforward. Medium priority.

---

## Lower Priority Gaps

### Gap 11: No ACID Transactions

Wadjet uses merge-on-read for DML. No multi-statement transactions, no isolation levels. This is acceptable for an analytics engine — ClickHouse and DuckDB (in server mode) have similar limitations. Not a competitive gap for the security analytics use case.

### Gap 12: No Lateral Joins or Recursive CTEs

Niche SQL features. Lateral joins would help with "top-N per group" patterns common in security analytics (e.g., top 5 destination IPs per source). Recursive CTEs are rarely needed. Low priority but lateral joins would be a quality-of-life improvement.

### Gap 13: No Schema Evolution / ALTER TABLE

Currently requires DROP + CREATE. Would be useful for adding columns to long-lived tables (e.g., adding a new enrichment field). Medium-long term improvement.

### Gap 14: No OpenTelemetry / Distributed Tracing

Prometheus metrics exist but no trace context propagation. Useful for debugging slow queries in distributed mode. Low priority for SMB.

---

## Strategic Recommendations

### For Wadjet (the engine)

| Priority | Gap | Effort | Impact |
|----------|-----|--------|--------|
| P0 | Materialized views / continuous aggregation | Large | Enables dashboards + alerting |
| P0 | Data retention policies (TTL) | Medium | Compliance + cost control |
| P1 | Built-in syslog receiver | Small | Eliminates Bento for simple deployments |
| P1 | Full-text search optimization | Medium | Log search use case |
| P2 | Query result caching | Small | Dashboard performance |
| P2 | Schema evolution (ALTER TABLE) | Medium | Operational convenience |
| P3 | Additional connectors | Varies | Customer-driven |

### For Warden (the product)

| Priority | Gap | Notes |
|----------|-----|-------|
| P0 | Alerting / detection rules engine | Core SIEM differentiator |
| P0 | Managed cloud offering | SMB adoption |
| P1 | Web UI with dashboards | Can start with Superset/Grafana embedding |
| P1 | Log shipper / agent | Simplified deployment for SMBs |
| P2 | Sigma rule compatibility | Security community adoption |
| P2 | SOAR integration | Automated response workflows |

### Positioning Strategy

Wadjet's network-native type system and 80+ network functions are **genuinely unmatched** in the market. No SQL engine — not ClickHouse, not DuckDB, not Elasticsearch — has first-class IPv4/IPv6/CIDR/MAC/Port/Protocol types with dedicated storage and vectorized operations.

**Recommended positioning**: "The only analytics engine built for network data" — lean into the network-native angle rather than competing as a general-purpose OLAP engine. For Warden: "Security analytics with SQL, not SPL" — attract teams frustrated with Splunk's proprietary query language and pricing.

**Key differentiators to emphasize**:
1. Network-native types with zero parsing overhead
2. PostgreSQL wire protocol (use any SQL tool, no vendor lock-in)
3. Single binary, 512 MB viable (runs on edge hardware)
4. RBAC + ABAC with cell-level policies (compliance-ready)
5. MCP server for AI-assisted threat hunting
6. GeoIP + JA3 + DNS/TLS/HTTP protocol analysis built-in
