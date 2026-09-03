# Proposed GitHub repository description and topics

**Status: proposal, awaiting the maintainer. Nothing here has been applied.**
The repository's About box and topic list are set by hand
(Settings → General, or `gh repo edit`); this file exists so the wording can be
reviewed against the code before it goes live, the same way an ADR is reviewed
before it is followed.

Read against the tip: `a9ec63b8` (v0.18.21), 2026-09-03.

## 1. Description (2 sentences, 344 characters)

Copy this verbatim into the About box:

```
Distributed SQL analytics engine in pure Go: a columnar OLAP query engine with vectorized execution and spill-to-disk over Parquet and Apache Iceberg on S3-compatible object storage — no JVM, no CGo, PostgreSQL wire protocol. Its niche is network telemetry: first-class IPv4/IPv6/CIDR/MAC/Port/Protocol column types with 100+ network functions.
```

Sentence 1 is the category — what a reader searching for a distributed SQL /
OLAP engine needs to recognise in one line. Sentence 2 is the niche, which is a
reason to choose Wadjet and not the definition of it.

Every clause is checkable on this tip:

| Clause | Where it is true |
|---|---|
| distributed SQL, columnar, OLAP, query engine | `internal/planner/`, `internal/engine/`, `internal/coordinator/`, `internal/worker/` |
| vectorized execution | 2048-row batches, selection vectors, typed kernels ([ADR-0002](../adr/0002-push-based-vectorized-execution.md)) |
| spill-to-disk | every pipeline breaker spills ([ADR-0027](../adr/0027-a-spill-gate-proves-it-spilled.md), [ADR-0006](../adr/0006-never-oom-memory-model.md)) |
| Parquet | `internal/storage/parquet/` (own reader/writer) |
| Apache Iceberg | `internal/iceberg/` — metadata read; registration is a Go API and writes are not supported |
| S3-compatible object storage | `internal/storage/objstore/` (MinIO/S3/R2, plus file, memory and HTTP stores) |
| pure Go, no JVM, no CGo | no `import "C"` in the tree and no cgo-using dependency; only stdlib packages (`net`, `os/user`) carry cgo files, and they build in pure-Go mode |
| PostgreSQL wire protocol | `internal/server/pgwire/`, and PostgreSQL is the semantics authority ([ADR-0012](../adr/0012-sql-semantics-authority.md)) |
| IPv4/IPv6/CIDR/MAC/Port/Protocol column types | dedicated vector storage per type, `docs/data-types.md` |
| 100+ network functions | ~109 network-family names in `expr.DefaultRegistry` (verified in the 2026-09-02 drift audit; the README's function-family table says 100+) |

### What this replaces, and why

The live description (read 2026-09-03) is:

> Distributed columnar SQL analytics engine in pure Go — vectorized execution
> over Parquet on S3, no JVM, no CGo. **Trino-class performance on a fraction
> of the footprint.** Network-native IPv4/CIDR/MAC types for telemetry and
> security workloads.

Two problems, both in the bolded sentence:

1. **"Trino-class performance"** is a comparison the repository's own front
   page no longer makes in those words. The measured position is specific and
   better than a vague class claim: same hardware, same day, SF100 TPC-H,
   Wadjet's steady suite 198.5 s against Trino 470's 221.2 s in fault-tolerant
   execution, per-query geomean −19 %, 12 of 22 queries won
   ([memo](../benchmarks/trino-comparison-2026-08-14.md)). A description cannot
   carry that, so it should carry no performance claim at all and let the
   README's "How Wadjet compares" do it.
2. **"on a fraction of the footprint"** has no measurement behind it. Nothing
   in the tree compares Wadjet's memory or node footprint to Trino's. The
   startup and idle-footprint numbers that *are* measured are in
   [the benchmarks index](../benchmarks/README.md#local-process-measurements-2026-09-03),
   and they are absolute, not comparative.

The proposal also adds Apache Iceberg (a search term the live text omits) and
the PostgreSQL wire protocol (the thing that makes it usable from psql, JDBC
and BI tools), and spells the full set of network types.

## 2. Topics (20 of GitHub's 20-topic cap)

Currently set (read 2026-09-03 via `gh repo view --json repositoryTopics`), 20:

```
analytics apache-iceberg big-data columnar database distributed-systems go
golang lakehouse model-context-protocol nats network-monitoring object-storage
observability olap parquet postgresql query-engine real-time-analytics sql
```

Proposed, 20 — copy verbatim:

```
distributed-sql sql olap query-engine analytical-database columnar-database
vectorized-execution database parquet apache-iceberg lakehouse object-storage
s3 distributed-systems golang go postgresql analytics big-data
network-monitoring
```

| Change | Topic | Reason |
|---|---|---|
| **add** | `distributed-sql` | the category phrase itself, and absent today |
| **add** | `analytical-database` | how the OLAP-engine audience searches |
| **add** | `columnar-database` | replaces the bare `columnar`, which is ambiguous outside this context |
| **add** | `vectorized-execution` | the execution model, and a term this project's peers (DuckDB, ClickHouse, DataFusion) are found under |
| **add** | `s3` | the storage layer people actually type; `object-storage` alone under-indexes |
| **drop** | `columnar` | superseded by `columnar-database` |
| **drop** | `nats` | an implementation detail of the control plane, not something a user searches for when choosing an engine |
| **drop** | `model-context-protocol` | a feature (`wadjet mcp`), not the product category; it stays documented in the README |
| **drop** | `observability` | a different audience (metrics/traces); the niche term that fits is `network-monitoring`, which stays |
| **drop** | `real-time-analytics` | Wadjet is a batch/interactive analytical engine over object storage; ingestion is micro-batch with a 60 s default flush, so this term promises a streaming posture the engine does not take |

Kept for the reasons they were set: `sql`, `olap`, `query-engine`, `database`,
`parquet`, `apache-iceberg`, `lakehouse`, `object-storage`,
`distributed-systems`, `golang`, `go`, `postgresql`, `analytics`, `big-data`,
`network-monitoring`.

If you would rather spend a slot differently, each of these is a one-for-one
swap at the cap. The two cheapest slots to reassign are `go` (the least
load-bearing keeper, since `golang` is the one people search) and
`analytical-database`, which is a thin topic — created 2025-10-20 and used by
6 repositories, against `distributed-sql` 33, `columnar-database` 21 and
`vectorized-execution` 14. `netflow`, `network-security` or `packet-analysis`
would deepen the niche if you want the telemetry audience weighted heavier than
the general Go audience.

## 3. Applying it

```bash
# Description
gh repo edit derekmwright/wadjet --description "<paste section 1 verbatim>"

# Topics: --add-topic / --remove-topic, one flag per topic
gh repo edit derekmwright/wadjet \
  --add-topic distributed-sql --add-topic analytical-database \
  --add-topic columnar-database --add-topic vectorized-execution \
  --add-topic s3 \
  --remove-topic columnar --remove-topic nats \
  --remove-topic model-context-protocol --remove-topic observability \
  --remove-topic real-time-analytics
```

GitHub caps a repository at 20 topics, so the removals have to go in with (or
before) the additions.
