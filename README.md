# Wadjet

[![Release](https://img.shields.io/github/v/release/derekmwright/wadjet?sort=semver)](https://github.com/derekmwright/wadjet/releases) [![CI](https://img.shields.io/github/actions/workflow/status/derekmwright/wadjet/ci.yml?branch=main&label=CI)](https://github.com/derekmwright/wadjet/actions/workflows/ci.yml) [![Go Version](https://img.shields.io/github/go-mod/go-version/derekmwright/wadjet)](https://github.com/derekmwright/wadjet/blob/main/go.mod) [![License](https://img.shields.io/github/license/derekmwright/wadjet)](https://github.com/derekmwright/wadjet/blob/main/LICENSE) [![Go Report Card](https://goreportcard.com/badge/github.com/derekmwright/wadjet)](https://goreportcard.com/report/github.com/derekmwright/wadjet) [![Issues](https://img.shields.io/github/issues/derekmwright/wadjet)](https://github.com/derekmwright/wadjet/issues)

A lightweight analytical query engine in pure Go. Columnar storage on Parquet, vectorized execution, full SQL, and optional distributed processing over NATS and S3-compatible object storage.

## Why Wadjet

- **No coordinator bottleneck** — the coordinator plans queries and schedules tasks but never touches data bytes. Workers read from and write results to object storage directly.
- **Lightweight workers** — viable at 512 MB RAM with spill-to-disk. Scale to zero, start in under 2 seconds.
- **Single binary** — run standalone for development or split into coordinator + workers for production.
- **Pure Go** — no JVM, no CGo, no external query engine dependencies. Custom recursive descent SQL parser, vectorized batch execution, typed kernel dispatch.
- **Network-native types** — first-class IPv4, IPv6, CIDR, MAC, Port, and Protocol column types with 80+ network functions covering CIDR math, deep packet inspection, ICMP analysis, IPv6 tunneling, JA3/JA3S TLS fingerprinting, payload search, and GeoIP/ASN enrichment (MaxMind).
- **Nested types** — ARRAY, ROW/STRUCT, and MAP column types with dot-notation field access, array functions, and full Parquet round-trip.
- **Table functions** — `read_json()`, `read_csv()`, `read_parquet()` query local files and HTTP URLs directly from SQL, with glob patterns and named parameters.
- **GeoIP enrichment** — optional MaxMind GeoLite2/GeoIP2 integration with 11 functions for IP geolocation (country, city, subdivision, coordinates, timezone, continent) and ASN lookup (AS number, organization).

## Quick Start

Query files on disk. No server, no object storage, no configuration:

```bash
# Build (-o must not be plain "wadjet" — that's the API package directory)
go build -o wadjet-bin ./cmd/wadjet

# Query a JSON log straight from disk
./wadjet-bin query "SELECT id_orig_h, SUM(orig_bytes) AS total FROM read_json('conn.log') GROUP BY 1 ORDER BY 2 DESC LIMIT 10"
```

`read_json()`, `read_csv()`, and `read_parquet()` take local paths, `~/` paths, glob patterns, and HTTP URLs. Add `--format table` for human-readable output.

### With object storage

Managed tables live in an S3-compatible store (MinIO, AWS S3, R2). That is the distributed and production path — see [Getting Started](docs/getting-started.md) for MinIO setup:

```bash
# Start standalone (embedded NATS + worker + coordinator)
./wadjet-bin serve --mode=standalone --endpoint=localhost:9000

# Run a query against a managed table
./wadjet-bin query --endpoint=localhost:9000 "SELECT src_ip, SUM(bytes_in) AS total FROM flow_logs GROUP BY src_ip ORDER BY total DESC LIMIT 10"

# Interactive shell
./wadjet-bin shell --endpoint=localhost:9000
```

## Native Functions

Wadjet's SQL surface leans hard into functions purpose-built for network
and security analytics — a dedicated IPv4/IPv6/CIDR/MAC/Port/Protocol type
system backed by a library of functions that collapse what's usually
string-parsing or a UDF elsewhere into one call. A few representative
examples, each run against a live server as part of writing this section.

**Is this flow internal-to-external?**

```sql
SELECT src_ip, dst_ip, bytes_out
FROM flow_logs
WHERE cidr_contains('10.0.0.0/8', src_ip)
  AND NOT cidr_contains('10.0.0.0/8', dst_ip)
ORDER BY bytes_out DESC
```

Elsewhere: general-purpose warehouses (Snowflake, BigQuery, Redshift,
Databricks) have no CIDR type at all, so this becomes string parsing or a
UDF. PostgreSQL does have real `inet`/`cidr` containment operators — but
it isn't a columnar engine built to scan billions of flow rows.

**Top-talking /24 subnets by egress**

```sql
SELECT mask_ip(src_ip, 1) AS subnet_24,
       SUM(bytes_out) AS egress,
       COUNT(*) AS flows
FROM flow_logs
GROUP BY mask_ip(src_ip, 1)
ORDER BY egress DESC
```

```
  subnet_24  |  egress  | flows
-------------+----------+-------
 10.0.1.0    |  1393540 |     4
 10.0.2.0    |  1200300 |     2
 203.0.113.0 |     1800 |     1
 192.168.1.0 |      500 |     1
```

Elsewhere: `split_part(ip,'.',1) || '.' || split_part(ip,'.',2) || '.' ||
split_part(ip,'.',3) || '.0'` — four string operations per row to do what
`mask_ip` does in one.

**Which NIC vendor prefixes are actually on the network**

```sql
SELECT mac_vendor_oui(src_mac) AS oui, COUNT(*) AS devices
FROM flow_logs
GROUP BY mac_vendor_oui(src_mac)
ORDER BY devices DESC
```

`mac_vendor_oui` pulls the 3-byte OUI prefix out of a MAC in one call
instead of hand-slicing the string; join the result against an IEEE OUI
table to resolve a manufacturer name, same as anywhere else.

**Human-readable service breakdown from raw flow tuples**

```sql
SELECT protocol_name(protocol)   AS protocol,
       port_name(dst_port)       AS service,
       port_class(dst_port)      AS port_class,
       COUNT(*)                  AS flows,
       SUM(bytes_in + bytes_out) AS total_bytes
FROM flow_logs
GROUP BY protocol_name(protocol), port_name(dst_port), port_class(dst_port)
ORDER BY total_bytes DESC
```

```
 protocol | service | port_class | flows | total_bytes
----------+---------+------------+-------+-------------
 tcp      | https   | well-known |     3 |     2602600
 tcp      | ssh     | well-known |     2 |       26400
 tcp      | rdp     | registered |     2 |        2200
 udp      | dns     | well-known |     1 |         230
```

Elsewhere: a `CASE WHEN dst_port = 443 THEN 'https' WHEN dst_port = 22
THEN 'ssh' ...` ladder maintained by hand, and a lookup table for IANA
protocol numbers.

**Near-duplicate alert triage**

```sql
SELECT b.alert_id, b.description,
       cosine_similarity(embed(a.description), embed(b.description)) AS score
FROM alerts a, alerts b
WHERE a.alert_id = 1 AND b.alert_id != 1
ORDER BY score DESC
```

`embed()` batches one API call per record batch — not per row — against
OpenAI, Voyage AI, or Ollama, with an LRU cache; `cosine_similarity`
scores the result inline, so a triage query that groups a flood of
near-duplicate alerts is one SELECT. Elsewhere this means standing up a
separate vector database (pgvector, Milvus, Pinecone) alongside the
analytics engine.

**Function families**

| Family | Count | Covers |
|---|---:|---|
| Network & protocol | 100+ | CIDR/subnet math, MAC, port/protocol semantics, TCP/DNS/TLS/HTTP deep inspection, ICMP, IPv6 tunneling, JA3/JA3S fingerprinting, payload search |
| GeoIP / ASN | 11 | MaxMind GeoLite2/GeoIP2 city + ASN lookup |
| Vector & embeddings | 5 + `embed()` | cosine_similarity, l2_distance, dot_product, vector_norm, vector_dims, embed()/embed_model()/embed_dim() |
| Date/time | 30+ | truncation, extraction, ISO 8601, Unix time, timezone conversion |
| String | 45+ | regex, padding, encoding, distance (Levenshtein/Soundex/Hamming) |
| Aggregate | 23 | approx_distinct, corr, covar, percentile_cont/disc, mode, median, min_by/max_by |

Full signatures for every function: [SQL Reference § Built-in Functions](docs/sql-reference.md#built-in-functions).

## Features

### SQL

Full analytical SQL via a custom recursive descent parser:

- SELECT, EXPLAIN, DESCRIBE, CREATE TABLE, DROP TABLE
- CTEs (`WITH ... AS`), UNION / INTERSECT / EXCEPT (with ALL variants)
- INNER, LEFT, RIGHT, FULL OUTER, CROSS JOINs
- Subqueries: scalar, IN, EXISTS, correlated subqueries
- Window functions with PARTITION BY, ORDER BY, NULLS FIRST/LAST, and ROWS/RANGE frame specs
- GROUP BY, GROUPING SETS, CUBE, ROLLUP, and ORDER BY with positional references
- CASE, CAST, LIKE, BETWEEN, IN, IS NULL/TRUE/FALSE
- Fixed-point DECIMAL(p,s) type with Int128 arithmetic (DuckDB-style scaled integers)
- Nested types: ARRAY, ROW/STRUCT, MAP with `person.name` dot-notation, `element_at()`, `map_keys()`
- Table functions: `read_json()`, `read_csv()`, `read_parquet()` with glob patterns and named parameters
- VECTOR(N) type for embedding storage with cosine_similarity, l2_distance, dot_product, vector_norm, vector_dims
- `embed()` SQL function — OpenAI, Voyage AI, and Ollama embedding providers with batched API calls (one call per record batch) and LRU cache
- 280+ built-in scalar functions (string, math, trig, date/time, network, UUID, conditional, regex, hash, encoding, bitwise, JSON, URL, deep packet inspection, ICMP, IPv6, JA3 fingerprinting, payload search, GeoIP/ASN, vector distance)
- 23 aggregate functions including approx_distinct, corr, covar, percentile_cont/disc, mode, median, min_by/max_by
- User-defined functions (CREATE FUNCTION)

```sql
-- Query JSON files directly from SQL
SELECT ip, count, geoip_country(ip) AS country
FROM read_json('https://example.com/traffic.json')
WHERE count > 100
ORDER BY count DESC

-- Query CSV with custom delimiter
SELECT * FROM read_csv('logs/*.csv', delimiter='|', header=true)

-- Window functions over CTEs
WITH hourly AS (
    SELECT DATE_TRUNC('hour', timestamp) AS hour, SUM(bytes_in) AS bytes
    FROM flow_logs WHERE date = '2026-03-15'
    GROUP BY 1
)
SELECT hour, bytes,
       SUM(bytes) OVER (ORDER BY hour ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative,
       RANK() OVER (ORDER BY bytes DESC) AS traffic_rank
FROM hourly
ORDER BY hour
```

```sql
-- Semantic search with embeddings
SELECT alert_id, description,
       cosine_similarity(embed(description), embed('credential theft')) AS score
FROM alerts
ORDER BY score DESC LIMIT 10

-- Store embeddings in VECTOR columns
CREATE TABLE doc_embeddings (doc_id INT64, embedding VECTOR(1536))
```

### Execution Engine

- **Vectorized** — operators process batches of 2048 rows, not row-at-a-time
- **Push-based pipelines** — Source → UnaryOp → Sink with selection vectors instead of data copying
- **Typed kernels** — type dispatch resolved once at query init, no per-row switches in hot loops
- **3-level pushdown** — partition pruning → row-group stats pruning → row-level filtering
- **Cost-based optimization** — DP join reordering with Selinger-style costing over column statistics (`ANALYZE TABLE`: HLL distinct counts, histograms)
- **Spill-to-disk everywhere** — hash join (grace partition-on-arrival), hash aggregate (partial-state k-way merge), sort and window (external sorted-run merge) all degrade gracefully past memory, governed by a byte-true memory ledger
- **Morsel-driven parallelism** (`--morsel-workers=0`) — intra-task parallel pipeline consumers with bounded, self-draining aggregate partials; opt-in, validated at SF100

### Table Functions

Query files directly from SQL without ingestion:

```sql
SELECT * FROM read_json('data.json')                              -- local file
SELECT * FROM read_json('https://api.example.com/events.json')    -- HTTP/HTTPS
SELECT * FROM read_json('logs/*.json')                            -- glob patterns
SELECT * FROM read_csv('data.csv', delimiter='|', header=false)   -- named parameters
SELECT * FROM read_parquet('warehouse/sales.parquet')             -- Parquet files
```

- **read_json** — JSONL and JSON array auto-detection, schema inference (IPv4, timestamp, bool, numeric), custom direct-to-columnar byte scanner (8x faster than `encoding/json`)
- **read_csv** — configurable delimiter, header detection, type inference
- **read_parquet** — column-at-a-time page reading with row-group stats pruning
- **HTTP filesystem** — connection pooling, Range requests, configurable auth headers
- **Glob patterns** — `read_json('data/*.json')` expands and concatenates matching files

### Storage

- **Apache Parquet** on any S3-compatible store (MinIO, AWS S3, R2, SeaweedFS)
- **Apache Iceberg** metadata reading — register external Iceberg tables and query them via the catalog
- **Hive-style partitioning** with automatic time-based partition keys
- **NATS KV catalog** with revision-based optimistic concurrency
- **Micro-batch ingestion** with configurable flush thresholds (size, row count, time)

### Distributed

- **Stage-DAG execution** — distributed queries run as multi-stage DAGs; every stage output is durable in object storage, giving Trino-style fault-tolerant execution with task retry and worker-death recovery
- **Streaming exchange** (default on) — consumers fetch stage outputs directly from producer workers' local disk over gRPC with asynchronous S3 upload; any failure falls back to the durable S3 path (SF100 suite −23% vs S3-only shuffle)
- **Small-query fast path** — queries under a post-pruning size threshold (default 64 MiB) execute in-process on the coordinator, skipping the DAG entirely
- **Broadcast + probe-split joins** — small builds replicate to all workers; the probe side's files split across workers with coordinator merge
- **Split control/data plane** — NATS for heartbeats, cancellation, and the KV catalog; one multiplexed gRPC stream per worker for task dispatch and results
- **Memory-aware scheduling** — per-task byte estimates bin-packed against live worker pool budgets, with admission gating under memory pressure
- **Graceful worker drain** — SIGTERM stops intake, finishes in-flight tasks, flushes uploads, then exits; Kubernetes-ready with `/healthz`, `/readyz`, and `POST /drain`
- **Catalog snapshots** — periodic S3 snapshots of the NATS KV catalog; a rebooted cluster discovers its tables in seconds
- **Federation** across clusters via NATS leaf nodes
- **Embedded NATS** — no external dependencies beyond object storage

### Security

- API key, JWT (HMAC/RSA), and mTLS authentication — enforced on HTTP, pgwire, and gRPC
- RBAC (role-based) and ABAC (attribute-based) access control with deny-overrides combining
- Cell-level policies: column masking, column denial, row filtering via ABAC obligations
- Identity enrichment from JWT claims, mTLS cert fields, and API key attributes
- Hot-reloadable configuration (including ABAC policies)

### Client Connectivity

- **PostgreSQL wire protocol** (pgwire) — connect with `psql`, JDBC, ODBC, or any PostgreSQL client
- **HTTP** REST API for queries, table management, health, and Prometheus metrics
- **gRPC** API with protobuf service definition — generate type-safe clients for Go, Python, Java, TypeScript, Rust, C#, and more
- **MCP** (Model Context Protocol) — AI agent integration for Claude Desktop, Claude Code, Cursor, and other MCP-compatible tools
- gRPC health checking protocol for load balancer integration

### Operations

- Prometheus metrics for queries, scans, workers, cache, and spill
- Kubernetes-compatible probes on every process (`/healthz`, `/readyz`) and graceful worker drain (`SIGTERM` / `POST /drain`)
- Catalog snapshot / restore for fast cluster recovery
- Output in table, JSON, or CSV format

## Benchmarks

All 22 TPC-H queries pass with row-count-validated results at SF0.01 (CI,
~5s), SF10, and SF100 (~600M lineitem rows, distributed with
spill-to-disk). Cross-engine result validation against DuckDB confirms
identical results over the same S3 Parquet data. ClickBench runs the full
43-query suite under the official methodology with cell-exact
cross-validation against DuckDB.

### TPC-H SF100, distributed (4 nodes)

Coordinator `c7g.2xlarge` + 3× `c7gd.4xlarge` workers (16 vCPU / 32 GB /
NVMe each), SF100 Parquet on S3 (us-east-2), NATS control plane, gRPC
streaming exchange with durable S3 fallback. Steady-state suite (mean of
runs 2-4 of 4; caches populated — cold run 1 of the same session was
2m47s). Row counts are validated per run and the answers are additionally
verifiable value-level against a committed DuckDB fingerprint ground
truth (`benchmarks/tpch/fingerprint-sf100.json`, captured in-region).
2026-08-23 at v0.18.0, SF100 window 6, `results/w6cand` run
`20260823-020311` (same-window baseline `results/w6base` run
`20260823-014839`, engine `9a8b564` / v0.17.0-clawback).

| Query | Time | | Query | Time |
|---|---:|---|---|---:|
| Q01 | 3.5s | | Q12 | 4.6s |
| Q02 | 4.2s | | Q13 | 5.3s |
| Q03 | 9.2s | | Q14 | 1.8s |
| Q04 | 5.1s | | Q15 | 1.8s |
| Q05 | 6.0s | | Q16 | 5.1s |
| Q06 | 0.8s | | Q17 | 5.6s |
| Q07 | 4.2s | | Q18 | 10.5s |
| Q08 | 15.0s | | Q19 | 3.8s |
| Q09 | 14.2s | | Q20 | 9.2s |
| Q10 | 11.1s | | Q21 | 9.0s |
| Q11 | 2.6s | | Q22 | 2.8s |

**Suite total: 2m15s steady (mean of runs 2-4) / 2m47s cold.** The best
single steady run was 2m14s (134.40s, run 4), beating the prior all-time
record of 2m33s (152.9s, 2026-08-22); the cold run also beat the prior
best cold of 2m59s (179.4s, 2026-08-22). Same-window baseline on engine
`9a8b564` (v0.17.0-clawback) was 2m25s steady / 3m24s cold (145.43s /
203.67s) — a 7.0% steady-state improvement, 18.1% on cold, 10.5% on
suite totals across all 4 runs (639.97s vs 572.47s). Row counts and
DuckDB fingerprint value signatures are identical across every arm and
run of this window. This is the arc that follows v0.17.0-clawback:
coordinator-side probe-split placement now groups a broadcast join's
probe files by rendezvous owner and dispatches one task per owner
instead of an even split placed by memory binpack
(`WADJET_PROBE_SPLIT_AFFINITY`) — base-table peer traffic falls 51.6 GB
→ 5.0 GB per suite and steady-run acquisition stragglers on the
Q08/Q09/Q17 probe-split stages go from 2 to 0; the scheduler's affinity
tier moved ahead of locality and the scan prefetcher skips its redundant
spill copy once a peer fetch already populated the cache
(`WADJET_AFFINITY_BEFORE_LOCALITY`, `WADJET_PREFETCH_CACHE_SKIP`, riders
on the same fix). The unbounded final aggregate's group-index layout is
now decided from its input-row bound at construction, so a sink whose
row count cannot amortize the flat→bucketed conversion is born flat and
never pays it (`WADJET_TWO_LEVEL_ROW_BOUND`) — Q18 −1.7s, −70 CPU-s per
suite off the int-aggregate probe+convert path. The coordinator's own
stage-output reads (gather-merge scalar substitution) now try the
producing worker's local copy before S3 (`WADJET_COORD_PEER_READS`) —
the 12 scalar-substitution reads across the 4-run suite drop from
4,635ms to 40ms of total wait. And the scan source's decoded row-group output is now
pooled and reused across row groups instead of minting a fresh
multi-hundred-MB arena per group (`WADJET_SCAN_BACKING_REUSE`) —
allocation −453 GiB/run (−24.6%), GC cycles −18.5%. Same-window arc
scoreboard (v0.17.0-clawback `9a8b564` → main `be5fcf1`): worker CPU
3,201.9 → 3,048.3 CPU-s/run (41.4% → 44.1% utilization), inter-node peer
bytes 137.87 → 96.03 GiB/run (−30%), heap allocation 1,878.4 → 1,392.3
GiB/run (−25.9%), GC cycles 1,483 → 1,159/run (−21.9%). Full attribution:
[probe-split-affinity-2026-08-22.md](docs/benchmarks/probe-split-affinity-2026-08-22.md),
[sf100-window4-analysis-2026-08-22.md](docs/benchmarks/sf100-window4-analysis-2026-08-22.md),
[sf100-window5-analysis-2026-08-23.md](docs/benchmarks/sf100-window5-analysis-2026-08-23.md),
and [sf100-window6-analysis-2026-08-23.md](docs/benchmarks/sf100-window6-analysis-2026-08-23.md).
On identical hardware in a same-day paired run (2026-08-14), Wadjet's
steady state beat Trino 470 FTE by 10% on suite wall and 19% on
per-query geomean, winning 12 of 22 queries
([full comparison](docs/benchmarks/trino-comparison-2026-08-14.md)).

### ClickBench, single node (official spec)

The full 43-query ClickBench suite on the official listing hardware —
`c6a.4xlarge` (16 vCPU / 32 GB), 500 GB gp2, querying the 100M-row
`hits` Parquet data in place (14.7 GB, no import step). Official
methodology: page-cache drop before each query, cold + 2 hot tries,
one process per query. Every query result is cell-exact against DuckDB
on the same data (`benchmarks/clickbench/`). 2026-08-22 at
v0.17.0-clawback, `benchmarks/clickbench/results-c6a-20260822-v0170.json`
— **not re-run for v0.18.0**; this arc targeted the distributed SF100
TPC-H path only (probe-split placement, aggregate layout, coordinator
stage reads, scan backing reuse — all worker/coordinator-side, out of
scope for a single-node suite), so the numbers below are carried
forward unchanged from v0.17.0-clawback.

| Query | Cold | Hot | Query | Cold | Hot |
|---|---:|---:|---|---:|---:|
| Q01 | 0.001s | 0.001s | Q23 | 21.5s | 4.37s |
| Q02 | 0.056s | 0.021s | Q24 | 12.3s | 3.11s |
| Q03 | 0.23s | 0.19s | Q25 | 2.64s | 1.12s |
| Q04 | 0.33s | 0.19s | Q26 | 0.99s | 0.92s |
| Q05 | 0.74s | 0.69s | Q27 | 2.64s | 1.13s |
| Q06 | 1.70s | 1.53s | Q28 | 9.75s | 3.96s |
| Q07 | 0.014s | 0.012s | Q29 | 12.3s | 11.7s |
| Q08 | 0.13s | 0.095s | Q30 | 0.15s | 0.11s |
| Q09 | 1.15s | 1.08s | Q31 | 2.07s | 0.96s |
| Q10 | 3.22s | 2.85s | Q32 | 5.70s | 1.39s |
| Q11 | 0.69s | 0.59s | Q33 | 5.88s | 5.25s |
| Q12 | 0.77s | 0.71s | Q34 | 10.7s | 4.70s |
| Q13 | 1.28s | 1.03s | Q35 | 14.7s | 8.41s |
| Q14 | 3.11s | 2.67s | Q36 | 4.29s | 3.81s |
| Q15 | 1.56s | 1.31s | Q37 | 0.31s | 0.19s |
| Q16 | 0.96s | 0.87s | Q38 | 0.20s | 0.11s |
| Q17 | 3.13s | 2.48s | Q39 | 0.15s | 0.067s |
| Q18 | 2.67s | 2.10s | Q40 | 0.55s | 0.41s |
| Q19 | 11.1s | 10.8s | Q41 | 0.071s | 0.042s |
| Q20 | 0.17s | 0.053s | Q42 | 0.071s | 0.046s |
| Q21 | 10.1s | 1.44s | Q43 | 0.19s | 0.15s |
| Q22 | 11.2s | 1.95s |  |  |  |

**Suite sums: 2m42s cold / 1m25s hot (43/43, no failures).** By the
official ClickBench formula (reproducible via
`benchmarks/clickbench/rank.py`) this places Wadjet at combined #41,
hot #66, and cold #17 of the 136 published `c6a.4xlarge` entries
(as of 2026-08-22) — ahead of the Trino, Presto, Impala, Spark,
Daft, GlareDB, and pg_duckdb Parquet entries on the same hardware.
The v0.17.0-clawback arc that produced these numbers targeted the
distributed TPC-H path; ClickBench (single-node) was flat within noise
against v0.16.0-correctness (cold 161.5s vs 162.3s, hot 84.6s vs 85.2s),
and the v0.18.0 arc above is single-node-out-of-scope in the same way.
The remaining hot spots (Q29, Q33, Q19, Q35 — regex-keyed grouping
and high-cardinality aggregation) are the active optimization arc.
Cold times for early large-read queries vary run-to-run with EBS gp2
burst-credit state (inherent to the official hardware spec); hot times
are stable.

```bash
# SF0.01 correctness (CI, ~5s)
go test -v -run TestTPCHQueries ./benchmarks/tpch/

# ClickBench correctness vs DuckDB (needs a hits part + /tmp/duckdb)
WADJET_HITS_PART=hits_0.parquet WADJET_CLICKBENCH_DUCKDB=1 \
  go test -run TestHitsCorrectness ./benchmarks/clickbench/

# Distributed smoke gate (~20s, spawns a local coordinator + workers)
go run ./cmd/tpch-harness --mode=local

# Full EC2 benchmark matrix (OpenTofu + SSM, no SSH required)
cd deploy/benchmark/terraform && tofu apply -var-file=sf100-distributed.tfvars
cd deploy/benchmark/terraform-clickbench && tofu apply   # official ClickBench run
```

## Deployment Modes

```
wadjet serve --mode=standalone     # All-in-one (dev / small workloads)
wadjet serve --mode=coordinator    # Plans queries, embeds NATS, touches zero data
wadjet serve --mode=worker         # Stateless task executor, scale horizontally
```

## AI Agent Integration (MCP)

Wadjet includes a native [Model Context Protocol](https://modelcontextprotocol.io/) server, enabling AI agents to discover tables, inspect schemas, and execute SQL queries.

The MCP server communicates over **stdio only** — there is no network listener.

```bash
# Local/dev: unauthenticated, direct-to-store (no ABAC enforced)
wadjet mcp

# Secured: enforce row/column ABAC under an authenticated identity
wadjet mcp --config /etc/wadjet/config.yaml --api-key "$WADJET_MCP_API_KEY"
```

**Security:** when `--config` supplies an auth block, MCP enforces the same
ABAC row filters, column masks, and table-access rules as the pgwire and gRPC
paths, under the identity resolved from `--api-key` (or `WADJET_MCP_API_KEY`).
If auth is configured but no valid credential is supplied, the server refuses
to start (fail closed). Without a config, MCP runs unauthenticated against a
direct-to-store DB — appropriate only where the operator already holds the
store credentials.

Configure in Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "wadjet": {
      "command": "wadjet",
      "args": ["mcp", "--config", "/etc/wadjet/config.yaml", "--api-key", "..."]
    }
  }
}
```

The MCP server exposes 5 tools:

| Tool | Description |
|------|-------------|
| `list_tables` | Discover all tables in the catalog |
| `describe_table` | Get schema with column types (including network-native types), nullability, and partition keys |
| `query` | Execute SQL with token-efficient compact JSON output (array-of-arrays, not array-of-objects) |
| `explain` | Show query execution plan without running |
| `list_functions` | List user-defined functions |

AI agents automatically understand network-typed columns (IPv4, CIDR, MAC, Port, Protocol) and receive hints about available network analysis functions.

## Embedding

Use Wadjet as a Go library:

```go
import "github.com/derekmwright/wadjet/wadjet"

db, _ := wadjet.Open(ctx, wadjet.Config{
    StorageEndpoint: "localhost:9000",
    Bucket:          "analytics",
})
defer db.Close()

result, _ := db.Query(ctx, "SELECT src_ip, COUNT(*) FROM flow_logs GROUP BY src_ip LIMIT 10")
```

## Documentation

| Guide | Description |
|-------|-------------|
| [Getting Started](docs/getting-started.md) | Installation, first table, first query |
| [Architecture](docs/architecture.md) | System internals, execution model, data flow |
| [SQL Reference](docs/sql-reference.md) | Full SQL syntax, functions, operators |
| [Data Types](docs/data-types.md) | Column types including network primitives |
| [HTTP API](docs/api-reference.md) | REST endpoints for queries, tables, health |
| [gRPC API](docs/grpc-api.md) | Protobuf service for multi-language client generation |
| [Configuration](docs/configuration.md) | YAML config, environment variables, CLI flags |
| [Ingestion](docs/ingestion.md) | Micro-batch accumulator, partitioning, Bento pipelines |
| [Embedding](docs/embedding.md) | Using Wadjet as a Go library |
| [Distributed Deployment](docs/distributed.md) | Multi-node setup, federation, cluster routing |
| [Security](docs/security.md) | API keys, JWT, mTLS, RBAC, ABAC, cell-level policies |
| [Performance Tuning](docs/tuning.md) | Memory budgets, spill tuning, environment profiles |
| [Runbook](docs/runbook.md) | Run scenarios, the full flag surface, Kubernetes lifecycle |
| [Operations](docs/operations.md) | Monitoring, Prometheus metrics, troubleshooting |
| [Network Analytics](docs/network-analytics.md) | End-to-end workflow: devices → Bento → Wadjet → app |
| [Disaster Recovery](docs/disaster-recovery.md) | Recovery scenarios, verification procedures, RTO/RPO |

## TPC-H Benchmark Queries

All 22 TPC-H queries pass with row-count validation at SF0.01 (CI), SF10, and SF100. See [Benchmarks](#benchmarks).

```bash
go test -v -run TestTPCHQueries ./benchmarks/tpch/                                    # SF0.01 correctness
TPCH_SCALE=10 go test -v -run TestTPCHQueriesLarge -timeout 120m ./benchmarks/tpch/   # SF10 performance
```

## License

Wadjet is free and open-source software licensed under the
[GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0).

If the AGPL doesn't fit your use case (e.g., embedding Wadjet in a
proprietary product), commercial licenses are available — contact
derekmwright@gmail.com.

Contributions are accepted under the [CLA](CLA.md); see
[CONTRIBUTING.md](CONTRIBUTING.md).
