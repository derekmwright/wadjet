# GitHub repository description and topics

**Status: APPLIED 2026-09-03.** The maintainer approved this wording and it was
applied with `gh repo edit`; `gh repo view --json description,repositoryTopics`
returns exactly the description and the twenty topics below. This page started
as the proposal — the About box and the topic list live outside the repository,
so they could not be reviewed the way a README claim is — and is now the record
of what was applied, what it replaced, and why.

Written and checked against the tip `a9ec63b8` (v0.18.21); the branch that
carries this file also rewrote the README's opening to match.

## 1. Description (348 characters)

The README's opening paragraph is the maintainer's own wording and is the LONG
form, at **388 characters** — over GitHub's 350-character limit for the About
box, so it cannot be pasted there:

> Wadjet is a distributed SQL analytics engine for Go. Embed it directly in
> your Go application, or deploy it as a fault-tolerant query cluster with
> durable S3 exchange. Vectorized columnar OLAP over Parquet and Apache
> Iceberg, with PostgreSQL wire protocol, no JVM, and no CGo. Built for network
> telemetry with first-class IPv4/IPv6/CIDR/MAC/Port/Protocol types and 100+
> network functions.

This is the same paragraph inside the limit, and it is what the About box now
carries (measured with `wc -c`: **348**):

```
Distributed SQL analytics engine for Go: embed it in a Go application, or deploy it as a fault-tolerant query cluster with durable S3 exchange. Vectorized columnar OLAP over Parquet and Apache Iceberg; PostgreSQL wire protocol; no JVM, no CGo. Built for network telemetry: first-class IPv4/IPv6/CIDR/MAC/Port/Protocol types, 100+ network functions.
```

It keeps the long form's order — category, the two deployments, the engine
properties, then the niche — and loses only connective words. If GitHub ever
tightens the limit further, "a Go application" → "Go apps" recovers 9
characters without touching a claim.

Every clause is checkable on this tip:

| Clause | Where it is true |
|---|---|
| distributed SQL, columnar, OLAP, query engine | `internal/planner/`, `internal/engine/`, `internal/coordinator/`, `internal/worker/` |
| vectorized | 2048-row batches, selection vectors, typed kernels ([ADR-0002](../adr/0002-push-based-vectorized-execution.md)) |
| embed it in a Go application | the `wadjet/` package (`wadjet.Open`, `DB.Query`), with the README's snippet compiled as an example in `wadjet/example_readme_test.go`. Caveat, stated in the README: `Config` is typed in terms of `internal/` packages, so out-of-tree embedding does not compile yet ([#805](https://github.com/derekmwright/wadjet/issues/805)) |
| fault-tolerant query cluster with durable S3 exchange | every stage's output materializes to the object store and task retries are idempotent overwrites; a producer lost before its durable copy lands costs a one-shot whole-QUERY re-execution ([ADR-0004](../adr/0004-stage-dag-with-streaming-exchange.md) §Decision item 2, §Consequences) |
| Parquet | `internal/storage/parquet/` (own reader/writer) |
| Apache Iceberg | `internal/iceberg/` — metadata read; registration is a Go API and writes are not supported |
| S3-compatible object storage | `internal/storage/objstore/` (MinIO/S3/R2, plus file, memory and HTTP stores) |
| pure Go, no JVM, no CGo | no `import "C"` in the tree and no cgo-using dependency; only stdlib packages (`net`, `os/user`) carry cgo files, and they build in pure-Go mode |
| PostgreSQL wire protocol | `internal/server/pgwire/`, and PostgreSQL is the semantics authority ([ADR-0012](../adr/0012-sql-semantics-authority.md)) |
| IPv4/IPv6/CIDR/MAC/Port/Protocol column types | dedicated vector storage per type, `docs/data-types.md` |
| 100+ network functions | ~109 network-family names in `expr.DefaultRegistry` (verified in the 2026-09-02 drift audit; the README's function-family table says 100+) |

### What it replaced, and why

The description in the About box until 2026-09-03 was:

> Distributed columnar SQL analytics engine in pure Go — vectorized execution
> over Parquet on S3, no JVM, no CGo. **Trino-class performance on a fraction
> of the footprint.** Network-native IPv4/CIDR/MAC types for telemetry and
> security workloads.

Two problems, both in the bolded sentence, and they are why it was replaced:

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

The replacement also adds Apache Iceberg (a search term the old text omitted)
and the PostgreSQL wire protocol (the thing that makes it usable from psql,
JDBC and BI tools), spells the full set of network types, and leads with the
two ways the engine is deployed.

## 2. Topics (20 of GitHub's 20-topic cap)

Before (until 2026-09-03), 20:

```
analytics apache-iceberg big-data columnar database distributed-systems go
golang lakehouse model-context-protocol nats network-monitoring object-storage
observability olap parquet postgresql query-engine real-time-analytics sql
```

Applied 2026-09-03, and what `gh repo view --json repositoryTopics` returns
today, 20:

```
distributed-sql sql olap query-engine analytical-database columnar-database
vectorized-execution database parquet apache-iceberg lakehouse object-storage
s3 distributed-systems golang go postgresql analytics big-data
network-monitoring
```

| Change | Topic | Reason it was made |
|---|---|---|
| **add** | `distributed-sql` | the category phrase itself, and absent before |
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

If a future slot is worth reassigning, each of these is a one-for-one swap at
the cap (the list is at 20, GitHub's maximum). The two cheapest slots to reassign are `go` (the least
load-bearing keeper, since `golang` is the one people search) and
`analytical-database`, which is a thin topic — created 2025-10-20 and used by
6 repositories, against `distributed-sql` 33, `columnar-database` 21 and
`vectorized-execution` 14. `netflow`, `network-security` or `packet-analysis`
would deepen the niche if you want the telemetry audience weighted heavier than
the general Go audience.

## 3. How it was applied, and how to re-check it

Applied 2026-09-03 on the maintainer's approval, with `gh repo edit` — the
description in one call, the topic changes in another. GitHub caps a repository
at 20 topics, so the five removals had to go in with (or before) the five
additions:

```bash
# Description
gh repo edit derekmwright/wadjet --description "<section 1, verbatim>"

# Topics: --add-topic / --remove-topic, one flag per topic
gh repo edit derekmwright/wadjet \
  --add-topic distributed-sql --add-topic analytical-database \
  --add-topic columnar-database --add-topic vectorized-execution \
  --add-topic s3 \
  --remove-topic columnar --remove-topic nats \
  --remove-topic model-context-protocol --remove-topic observability \
  --remove-topic real-time-analytics
```

To confirm the live state still matches this page:

```bash
gh repo view derekmwright/wadjet --json description,repositoryTopics
```

If it ever diverges, this page is the record of what was decided — update both
together, or the next reader inherits the drift this arc existed to remove.
