# SQLancer harness for wadjet's pgwire endpoint

Runbook for wadjet issue #289 ("evaluate SQLancer (TLP/NoREC) against the
pgwire endpoint") and the standing adversarial-testing practice it kicks
off (`feedback-external-adversarial-tooling`: run SQLancer in full, locally,
as a source of findings independent of wadjet's own oracles — every finding
becomes a permanent gate entry, not a one-off fix).

[SQLancer](https://github.com/sqlancer/sqlancer) is a Java, JDBC-based
metamorphic SQL fuzzer. It generates a random schema, populates it, then
runs one of several **test oracles** — TLP (Ternary Logic Partitioning:
`SELECT ... WHERE p` must equal the union of `WHERE p`, `WHERE NOT p`, and
`WHERE p IS NULL`), NoREC (an unoptimizable rewrite of a query must return
the same row count as the original), PQS (a query constructed to
provably return a specific pivot row must return it), and a few others —
none of which need a second reference engine. It has no PostgreSQL-wire
support of its own, but it ships a full PostgreSQL dialect implementation
(`sqlancer.postgres.*`) built against the real `postgresql` JDBC driver,
which is what makes pointing it at wadjet's pgwire endpoint possible at
all without writing a fuzzer from scratch.

## Prerequisites

SQLancer needs Java 11+ and Maven; wadjet needs its usual Go toolchain.
This repo's dev box had neither Java nor Maven, and had no `sudo`/`apt`
access. `sdk` (SDKMAN) was tried first and failed at its own preflight
check (`zip` not installed, and `zip` — not `unzip`, which was present —
needs `apt install zip`, also blocked). The prerequisites were installed
straight from upstream tarballs into user space instead, which needs
nothing beyond `curl`/`tar`:

```bash
mkdir -p ~/tools
curl -sL -o /tmp/jdk17.tar.gz \
  "https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.20.1%2B1/OpenJDK17U-jdk_x64_linux_hotspot_17.0.20.1_1.tar.gz"
curl -sL -o /tmp/maven.tar.gz \
  "https://dlcdn.apache.org/maven/maven-3/3.9.16/binaries/apache-maven-3.9.16-bin.tar.gz"
tar xzf /tmp/jdk17.tar.gz -C ~/tools
tar xzf /tmp/maven.tar.gz -C ~/tools

cat > ~/tools/sqlancer-env.sh << 'EOF'
export JAVA_HOME=$HOME/tools/jdk-17.0.20.1+1
export M2_HOME=$HOME/tools/apache-maven-3.9.16
export PATH="$JAVA_HOME/bin:$M2_HOME/bin:$PATH"
EOF
source ~/tools/sqlancer-env.sh
java -version && mvn -version
```

If this box's network/package policy changes and `apt`/`sudo` become
available, the equivalent one-liner is simpler:

```bash
sudo apt-get install -y zip openjdk-17-jdk maven   # ! run this, not the tarball dance above
```

Verify current pinned versions (Temurin release tags move) at
<https://api.adoptium.net/v3/assets/latest/17/hotspot?architecture=x64&image_type=jdk&os=linux&vendor=eclipse>
and <https://dlcdn.apache.org/maven/maven-3/>.

## Build

```bash
tools/sqlancer/build.sh [target-dir]     # default target-dir: /tmp/sqlancer-wadjet
```

This clones `sqlancer/sqlancer` (GPLv3 — deliberately never vendored into
this repo, see "Licensing" below), applies
`tools/sqlancer/patches/0001-wadjet-dialect-fixups.patch` (see "Patches
applied to the SQLancer clone"), copies in the three
`tools/sqlancer/adapter-src/sqlancer/wadjet/*.java` files, and runs
`mvn package -DskipTests`, producing `target/sqlancer-2.0.0.jar`. Rerunning
it against the same `target-dir` reuses the clone and re-applies the patch
idempotently (skips if already applied).

## The WadjetProvider adapter

`adapter-src/sqlancer/wadjet/` — `WadjetProvider`, `WadjetTableGenerator`,
`WadjetInsertGenerator` — is a minimal subclass of SQLancer's
`sqlancer.postgres.PostgresProvider`. Wadjet speaks enough of the pgwire
protocol and enough of information_schema/pg_catalog (verified directly:
the exact introspection queries `PostgresSchema` issues — the
information_schema/pg_class/pg_namespace join for table listing, per-table
information_schema.columns, pg_indexes, pg_statistic_ext, pg_collation,
pg_opclass, pg_operator, pg_am, pg_proc — all either return correct rows or
a valid empty result set, never a parse error) that SQLancer's Postgres
schema introspection, expression generation, and TLP/NoREC/PQS oracle
machinery all work against it unmodified. What doesn't carry over is the
DDL/action surface — see the Javadoc atop `WadjetProvider.java` for the
full list of what's replaced and why (short version: wadjet's CREATE TABLE
grammar has no constraints of any kind, no CREATE DATABASE, no CREATE
INDEX/VIEW, and none of Postgres's admin statements).

One deliberate simplification carries a correctness caveat: wadjet has one
catalog namespace (no CREATE DATABASE), so `WadjetProvider.createDatabase`
reuses the single database in `--connection-url` across every round,
dropping leftover tables from the previous round instead of a real
`DROP DATABASE`/`CREATE DATABASE`. Combined with wadjet#483 (below), a
table name recycled across rounds after being read once in a prior round
can surface a stale-manifest decode error — see "Known friction" below.
**Run with `--num-threads 1` always** — with more than one thread, multiple
threads race to CREATE/DROP the same recycled table names (`t0`..`t5`) in
the single shared namespace, which is a pure harness artifact (not a
wadjet defect) and was the single largest noise source before it was
diagnosed; see "Known friction".

## Patches applied to the SQLancer clone

`patches/0001-wadjet-dialect-fixups.patch`, applied by `build.sh`. Every
patch here is a **rendering-only** change to `sqlancer.postgres`'s shared
query-generation code — none change what any oracle expects a query to
return, only which of several syntactically-equivalent (in real
PostgreSQL) spellings gets emitted. Each was found by running the pilot
soak and seeing it dominate the "unexpected error" noise:

1. **`PostgresToStringVisitor.visit(PostgresFromTable)`** — never emit
   Postgres table-inheritance syntax (`ONLY table` / `table*`). Wadjet has
   no inheritance model and parse-errors on both forms; `isOnly()` has no
   effect on any oracle's expected-value computation (grep-confirmed), so
   always rendering the plain table name is semantically identical.
2. **`PostgresExpressionGenerator.getComparison` /
   `potentiallyWrapInCollate`** — the two places that wrap a TEXT
   expression/comparison in an explicit `COLLATE "C"` clause do so whenever
   `PostgresProvider.generateOnlyKnown` is set, which `WadjetProvider`
   always sets (it's also what restricts the generated type pool to
   wadjet's INT/BOOLEAN/TEXT — see `WadjetTableGenerator`'s Javadoc). Wadjet
   has no COLLATE syntax at all (parse error), and per wadjet's own
   `CLAUDE.md` it already always compares strings by bytes, so the clause
   changed nothing about the comparison it decorated. Patched to also
   require `--test-collations` (reusing that existing, already-plumbed
   option) so a wadjet harness can keep the type-pool restriction without
   the COLLATE emission.
3. **`PostgresToStringVisitor.visit(PostgresBetweenOperation)`** — same
   COLLATE issue, a separate hardcoded emission site not reachable through
   #2's fix (no `PostgresGlobalState` in scope in this stateless visitor);
   dropped unconditionally rather than threading global state through.
4. **`PostgresToStringVisitor`, the `SelectOption.ALL` case** — never emit
   the `ALL` keyword (`SELECT ALL ...`). Standard SQL's no-op opposite of
   `DISTINCT`; wadjet's parser doesn't accept it at all. This was, by a
   wide margin, the single largest source of "unexpected token" noise
   before it was patched — `SELECT ALL` is emitted by the base `SELECT`
   renderer that essentially every generated query goes through.
5. **`PostgresPostfixOperation.PostfixOperator.IS_NULL` /
   `IS_NOT_NULL`** — dropped the alternate `ISNULL`/`NOTNULL` postfix
   spellings, kept only the standard `IS NULL`/`IS NOT NULL` form,
   which wadjet's parser accepts (verified directly). TLPWhereOracle's
   required third comparison arm is literally `p IS NULL`/`p ISNULL` on
   every single query it generates, so the postfix form — previously
   picked at random — was the second-largest noise source.

**Follow-up, not yet patched** (lower volume per pilot soak, or needs more
care to patch safely — see the soak's failure-class inventory below):
`IS UNKNOWN`/`IS NOT UNKNOWN` (semantically identical to `IS NULL`/`IS NOT
NULL` restricted to BOOLEAN inputs per `PostgresPostfixOperation.apply()`,
so removing them from `PostfixOperator.getRandom()`'s candidate pool should
be exactly as safe as #5 above, but needs `getRandom()` itself touched
rather than just a per-constant string list).

## Running

Every command needs `source ~/tools/sqlancer-env.sh` first (or your own
`java`/`mvn` on PATH), and a wadjet standalone server up:

```bash
task build
mkdir -p /tmp/wadjet-sqlancer-data
dist/wadjet serve --mode=standalone --pg-addr=:15432 \
  --storage-type=file --data-dir=/tmp/wadjet-sqlancer-data
```

Then, from the SQLancer build directory:

```bash
java -jar target/sqlancer-2.0.0.jar \
  --num-threads 1 \
  --random-seed <N> \
  --num-queries <per-round budget> \
  --max-generated-databases <round budget> \
  --num-tries <stop-after-this-many-found-"errors", see below> \
  --username wadjet --password wadjet \
  wadjet --oracle <ORACLE> --test-collations=false \
  --connection-url postgresql://localhost:15432/wadjet
```

`--num-tries` (default 100) is SQLancer's own "stop after N found errors"
budget, and it counts *every* uncaught exception toward that total — not
just genuine oracle-detected wrong-result findings, but also every
"wadjet rejected this generated SQL with an error message Postgres's
ExpectedErrors list doesn't recognize" case (see "Known friction"). At the
current noise level that cap is reached well before any meaningful
`--num-queries` budget is exhausted, so **set it far above the expected
noise volume** (some multiple of 100 per planned soak duration) for
anything longer than a quick smoke test — a plain `--num-tries 100` run
looks like SQLancer stopped almost immediately, not that it found 100 real
bugs.

Per-oracle notes, ordered as recommended (matches wadjet#289's own
priority: TLP first, then NoREC, then PQS):

- **`--oracle QUERY_PARTITIONING`** — composite: TLP-WHERE + TLP-HAVING +
  TLP-Aggregate in one pass. Recommended default; this is what the pilot
  ran.
- **`--oracle WHERE`** / **`--oracle HAVING`** — the two halves of
  QUERY_PARTITIONING individually, useful when triaging a specific
  mismatch class.
- **`--oracle NOREC`** — smaller, faster queries (no OUTER JOIN chains to
  build), lower per-query noise; good for a first smoke test of a fresh
  build.
- **`--oracle PQS`** — untested in this pilot (ran out of time budget).
  `requiresAllTablesToContainRows()` is true for PQS, which the harness's
  INSERT-only, pure-append schema-generation phase satisfies fine; try it
  next.
- **`--oracle CERT`** — **do not use.** It parses `EXPLAIN` plan text for
  Postgres-specific cardinality-estimator row counts and plan-node names;
  wadjet's `EXPLAIN` output has neither, so every check would fail on
  format alone, not a real defect.
- **`--oracle FUZZER`** — pure crash-hunting, no correctness oracle. Cheap
  or free to add to a long soak alongside one of the above; not run in
  this pilot.

## Reproducing a finding from a seed

SQLancer writes one running log per database round to
`<target-dir>/logs/wadjet/database<N>-cur.log` (also mirrored to stdout as
it runs) — every DDL/DML/query statement issued for that round, in order,
with the seed value in the header. To reproduce:

1. Find the `-- seed value: N` line for the failing round in the log or in
   the captured stdout.
2. Rerun with `--random-seed N --max-generated-databases 1
   --num-threads 1` and the same `--oracle`/`--num-queries` — SQLancer's
   generation is seed-deterministic, so this regenerates the identical
   schema and query sequence.
3. For a **genuine oracle violation** (see "Triage protocol" below), copy
   the exact CREATE TABLE/INSERT/SELECT statements straight out of the
   database log into `psql -p 15432` against a scratch wadjet server —
   they're already valid, executable SQL in file order, no reconstruction
   needed.

## Triage protocol

For every SQLancer-reported failure, in order:

1. **Classify the failure shape first.** A genuine oracle-detected
   wrong-result finding's `AssertionError` message is oracle-specific and
   starts with a stable phrase — NoREC: `"the counts mismatch (%d and
   %d)!"`; the TLP family: `"... mismatch:"` — followed by the compared
   queries and their results. Everything else (an `AssertionError` whose
   message is just the raw query text, thrown by
   `SQLQueryAdapter.checkException`) is an *unexpected error*: wadjet
   rejected or failed on a generated statement with a message that isn't
   in `sqlancer.postgres`'s Postgres-shaped `ExpectedErrors` list. Both are
   worth triaging, but they mean different things — grep a soak log for
   `"counts mismatch"` / `"mismatch:"` to pull out just the former.
2. **Reduce to a minimal repro** (see above) — strip the reproduced SQL
   down by hand to the smallest table/query shape that still reproduces.
3. **Verify against live `postgres:17-alpine`** (`docker run -d -p
   <port>:5432 -e POSTGRES_PASSWORD=postgres postgres:17-alpine`) — adjust
   column types to real Postgres equivalents if wadjet's aren't 1:1
   (BIGINT/BOOL/TEXT all are). Confirms it's a real semantic divergence,
   not a misunderstanding of what the SQL should do.
4. **Check the open issue list and in-flight territories before
   classifying** — `gh issue list --state open`, plus (as of this pilot)
   the concurrently-worked territories: LIMIT-0 semantics (#481, #482),
   DECIMAL literal/compare (#462, #463, #465, #476, #477), pgwire binary
   numeric/ROW rendering (#464, #471), DECIMAL+NaN hash keys (#474, #459),
   derived-table aliasing (#467, #468), window DECIMAL (#475), derived
   LIMIT (#478), DAG loud failures (#480), DATE clamp (#451). A finding
   that matches one of these: comment `CONFIRMS-#N` on this issue's pilot
   summary, don't file a duplicate.
5. **File genuinely new findings** — `gh issue create --label bug,correctness`,
   body = minimal repro SQL + which oracle/seed found it + the live-Postgres
   comparison from step 3.
6. **Every fix lands as a permanent gate entry**, not a one-off patch —
   per `feedback-external-adversarial-tooling`: add the minimal repro to
   whichever existing gate fits (pg-oracle differential suite, two-path
   invariance corpus, shapegen fuzzer corpus) or a new arm if none fits. A
   SQLancer finding that gets fixed without a permanent regression test is
   incomplete per this repo's own testing requirements
   (`CLAUDE.md` "All bug fixes must include a regression test").

## Pilot soak results (2026-08-24)

`--oracle QUERY_PARTITIONING`, wadjet main @ `b570b5e879404c16e49f5d89f48164660363ad67`,
`--num-threads 1`, patches above applied. Two soaks after the patch set
landed, ~200s / up to 60-200 database rounds each, `--test-collations=false`:

- **0 genuine oracle violations** (`"counts mismatch"` / `"mismatch:"`) in
  either soak.
- **~100 "unexpected error" events per soak** (SQLancer's own `--num-tries`
  default cap — see "Running" above for why this stops a soak early rather
  than reflecting the real defect rate). Bucketed by underlying wadjet
  error text (one soak's breakdown, representative of both):

  | Count | Class | Verdict |
  |---|---|---|
  | 11 | `stage scan-N has no dependencies and no ScanFiles` (native DAG) | Unexamined — worth a follow-up look, not chased in this pilot's time budget |
  | 9 | `syntax error ... trailing input after the end of the statement` (SQLancer emits `... ISNULL;` style trailing tokens the parser doesn't consume) | SQL-surface gap in the *parser's* recovery, not wadjet wrong-answer; candidate follow-up patch or a wadjet parser polish, unfiled |
  | 6 | `join ON residual "..." not evaluable as a probe residual` / `cannot be represented as an equi-join key` | Wadjet's physical join executor loudly refusing a JOIN ON clause it can't push into the join (function calls, casts, non-equality) rather than silently mishandling it — by design per wadjet's loud-failure philosophy (CLAUDE.md), not a correctness defect |
  | 6 | `expected NULL, TRUE, FALSE, or DISTINCT FROM after IS [NOT]` (`IS UNKNOWN`) | The unpatched follow-up noted above |
  | 5 | `No results were returned by the query.` | Unexamined |
  | 4 | schema/physical-type decode mismatch (`schema declares X but the file stores Y`), both the single-process and native-DAG scan-filter variants | **CONFIRMS wadjet#483** (filed and substantially corrected during this pilot — see below) |
  | 3 | `parse: parsing SQL: expected )` | Unexamined parser edge case |

None of these landed on the in-flight territories listed in the triage
protocol (DECIMAL/pgwire-numeric/derived-alias/window-DECIMAL/etc.) — the
harness's type pool deliberately excludes DECIMAL/FLOAT for exactly this
reason (see `WadjetTableGenerator`'s Javadoc).

### wadjet#483 — filed and substantially corrected during this pilot

The most severe finding of the pilot, found by manual DML probing *before*
the SQLancer campaign proper (not by an oracle — it's a data-integrity bug,
not a query-logic one, and QUERY_PARTITIONING's read-only-after-generation
query pattern happens not to trigger it — see below):

**A table's manifest/file-list is cached at that table's first `SELECT`
and never invalidated** — not by further `INSERT`/`UPDATE`/`DELETE` to
that table, and not even by `DROP TABLE` + `CREATE TABLE` recreating that
name from scratch. Three manifestations, from mildest to most severe (all
in the issue, with exact repro SQL):

1. `UPDATE`/`DELETE` report success (correct row count in the command tag)
   but the mutation never becomes visible to any later read of that table.
2. Any further write (including plain `INSERT`) to a table that has been
   read once becomes invisible the same way — not specific to UPDATE/DELETE
   at all.
3. **The dangerous case**: `DROP TABLE foo; CREATE TABLE foo (...);` — an
   entirely ordinary schema-migration pattern — after `foo` was read once
   in its previous life, silently resurrects the *previous incarnation's*
   data instead of the new table's. When the two incarnations' schemas
   happen to be physically incompatible this surfaces loudly (a decode
   error, `schema declares X but the file stores Y` — the class this
   pilot's soak hit repeatedly, since `WadjetProvider` recycles table
   names `t0`..`t5` across every round); when they're compatible it is
   **completely silent** — the standalone reproduction in the issue
   (`repro4`) has a `SELECT` return `111` after an `INSERT 222` reported
   success, with zero indication anything is wrong.

The harness's own INSERT-only, generate-then-read-only-oracle-test
structure (no interleaved reads during schema generation — `WadjetProvider`
never issues a `SELECT` until the oracle phase starts) means this bug does
not corrupt this pilot's own oracle results, but it does churn database
rounds (as the decode-mismatch class in the table above) and is expected
to be the **dominant noise source in a longer soak**, since every round
after the first reuses the previous round's table names.

**Recommendation: this should be fixed, or worked around by widening the
harness's per-round table-name uniqueness, before the full pre-release
soak below is run for real.** A quick, low-risk harness-side mitigation
(not yet implemented) would be to suffix table names with a per-round
counter (`t0_r7` instead of `t0`) rather than reusing `t0`..`t5` — avoids
the name-reuse trigger entirely without needing #483 fixed first, at the
cost of losing the "table recreated with an incompatible schema" scenario
as a thing the soak itself exercises (which is exactly the scenario that
found #483, so there's a real tradeoff either way).

### Operational notes from the pilot

- **`--num-threads` must be 1.** With more than one thread, concurrent
  rounds race to `CREATE`/`DROP` the same recycled table names in wadjet's
  single shared namespace — a pure harness artifact of the no-CREATE-DATABASE
  workaround, not a wadjet defect, but it dominates the noise floor before
  it's diagnosed (85 of 100 `--num-tries` slots in one early single-thread-vs-4-thread
  comparison).
- **A soak needs `--timeout-seconds` set.** One soak stalled indefinitely
  (`timeout 200` had to kill it) on what was very likely one runaway
  multi-way `CROSS JOIN` with no `LIMIT` — the wadjet server itself stayed
  healthy and answered a fresh connection immediately, so this reads as an
  expensive-but-legal query with no client-side timeout, not a server hang.
  SQLancer's own `--timeout-seconds` defaults to disabled (`-1`); the pilot
  didn't hit this until late and didn't have budget left to pick a good
  value — start the full soak with one set.
- **`information_schema.tables.is_insertable_into` is always NULL** on
  wadjet, which the JDBC driver reads back as `false`, which made
  `PostgresTable::isInsertable` empty the candidate-table list for every
  INSERT (fixed harness-side: `WadjetInsertGenerator` picks from
  `getDatabaseTables()` directly instead of filtering on it — see its
  Javadoc). Minor catalog-completeness gap, not filed as its own issue
  (cosmetic; nothing in this repo's BI-compatibility test suite currently
  exercises that column).

## Recommendation for the full pre-release soak

1. **Land or work around wadjet#483 first** (see above) — it's the
   dominant noise source and, in its silent form, a severe enough bug on
   its own that a soak run before it's addressed will spend most of its
   `--num-tries` budget on symptoms of one already-known issue.
2. Widen the type pool once the in-flight DECIMAL/float correctness work
   (#462, #463, #465, #474, #475, #476, #477, #459) lands — `WadjetTableGenerator`
   and `WadjetInsertGenerator` currently hardcode `PostgresDataType`'s
   `generateOnlyKnown`-restricted INT/BOOLEAN/TEXT pool specifically to
   avoid re-discovering that work.
3. Patch the `IS UNKNOWN`/`IS NOT UNKNOWN` follow-up noted above — cheap,
   same shape as the `ISNULL` fix already applied, and likely a similar
   noise reduction.
4. Set `--timeout-seconds` to something reasonable (a few seconds — every
   query in this schema is small) and `--num-tries` to a large multiple of
   the planned duration's expected noise volume, not the 100 default.
5. Run all of `QUERY_PARTITIONING`, `NOREC`, and `PQS` (untested in this
   pilot — try it first) as a single long soak, `--num-threads 1`,
   `FUZZER` layered in if time allows.
6. Feed every finding through the triage protocol above; every fix gets a
   permanent gate entry, per `feedback-external-adversarial-tooling`.
7. Consider whether wadjet's native TLP arm in `internal/oracle/shapegen`
   (mentioned in that same feedback note) should grow a network-type
   (IPv4/IPv6/CIDR/MAC/PORT/PROTOCOL) corpus specifically — SQLancer's
   Postgres dialect has no vocabulary for those types at all, so this
   harness structurally cannot reach that part of wadjet's surface no
   matter how it's configured.

## Licensing

`sqlancer/sqlancer` is GPLv3-licensed. It is cloned fresh by `build.sh`
into a scratch directory and never vendored into or committed to this
(AGPL-3.0) repository — only this harness's own thin adapter
(`adapter-src/`) and the small dialect patch (`patches/`) are ours, kept
here as plain text against SQLancer's public source, same as any other
patch file.
