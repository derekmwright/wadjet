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
this repo, see "Licensing" below), applies every
`tools/sqlancer/patches/*.patch` in name order (see "Patches applied to the
SQLancer clone"), copies in the three
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

`build.sh` applies every `patches/*.patch` in name order. There are two:
`0002-tlp-violation-dump.patch` is diagnostics only and is described under
"Self-diagnosing violation dumps" above — it adds output on the path that
was already about to throw, and changes no oracle's verdict.

`patches/0001-wadjet-dialect-fixups.patch` is the dialect patch. Every
change in it is a **rendering-only** change to `sqlancer.postgres`'s shared
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

Then run it through `tools/sqlancer/run.sh` (or `task sqlancer:run`) —
**not** a raw `java -jar`. SQLancer writes `logs/wadjet/*.log` relative to
wherever it's launched from, and launching it from an arbitrary shell's
cwd instead of a scratch directory landed ~800 stray log files in this
repo's main checkout, twice, during the 2026-08-25 standing soak
(wadjet#289) — an ad hoc supervisor script skipped the "cd first" this
section used to just tell you to remember. `run.sh` always cds into a
scratch working directory before invoking java, so that mistake now takes
deliberately bypassing this script to repeat:

```bash
tools/sqlancer/run.sh \
  --num-threads 1 \
  --random-seed <N> \
  --num-queries <per-round budget> \
  --max-generated-databases <round budget> \
  --num-tries <stop-after-this-many-found-"errors", see below> \
  --username wadjet --password wadjet \
  wadjet --oracle <ORACLE> --test-collations=false \
  --connection-url postgresql://localhost:15432/wadjet
```

`run.sh` prints the scratch directory it's using to stderr (override with
`SQLANCER_RUN_DIR`) — that's where `logs/wadjet/database<N>-cur.log` lands
for the "Reproducing a finding from a seed" steps below.

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
  format alone, not a real defect. (It's still reachable —
  `WadjetProvider` extends `PostgresProvider` and doesn't remove `CERT`
  from `PostgresOracleFactory` — and `tools/sqlancer/triage` still
  recognizes its `"Inconsistent result for query:"` assertion text as its
  own category rather than filing it away as ordinary noise, in case this
  advisory is ever overridden or an old soak ran it anyway. A CERT finding
  needs the same false-positive-format-mismatch skepticism this advisory
  already gives it, though — expect to have to rule that out by hand.)
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
2. Rerun through `tools/sqlancer/run.sh` with `--random-seed N
   --max-generated-databases 1 --num-threads 1` and the same
   `--oracle`/`--num-queries` — SQLancer's generation is seed-deterministic,
   so this regenerates the identical schema and query sequence.
3. For a **genuine oracle violation** (see "Triage protocol" below), copy
   the exact CREATE TABLE/INSERT/SELECT statements straight out of the
   database log into `psql -p 15432` against a scratch wadjet server —
   they're already valid, executable SQL in file order, no reconstruction
   needed.

## Self-diagnosing violation dumps

`patches/0002-tlp-violation-dump.patch` makes every TLP violation write a
directory of everything a human needs to decide the finding offline, and
names that directory in the `AssertionError`. Read `rerun.txt` first.

Why it exists: three soaks (2026-08-24, -25, -28) each logged a batch of
TLP violations over outer/cross JOINs that **never reproduced** — the one
captured instance survived both a fresh-server reconstruction and a
byte-for-byte 622-statement replay (wadjet#626). The controlled class
experiment that closed out v0.18.3 ruled out the obvious explanation:
`ComparatorHelper.isEqualDouble`'s epsilon is `|a-b| < 0.001*max(|a|,|b|)
+ 0.001` — a **1e-3 relative** tolerance, ten orders of magnitude wider
than any float accumulation-order delta, and the harness's type pool is
INT/BOOLEAN/TEXT besides. **A TLP violation is never float accumulation
order; triage goes straight to the transient-state lead.** The one
retained signature was a 9.2x wrong join CARDINALITY, which is
count-visible and would equally break `COUNT(*)`.

The measurement that separates a transient state read from a stable wrong
answer only exists at the moment of the violation, and it costs nothing:
re-execute both arms immediately, on the same connection. That is what
this patch does.

```
<run-dir>/sqlancer-violations/<oracle>-seed<N>-<millis>-<seq>/
  summary.txt    oracle, seed, database, wall-clock instant, the assertion
  queries.sql    every partition's SQL in order, executable as-is
  rerun.txt      each arm RE-EXECUTED immediately on the same connection,
                 with instants either side, its FULL row set beside the full
                 row set the oracle read, and a STABLE/TRANSIENT verdict
  plans.txt      EXPLAIN of each arm
  schema.sql     CREATE TABLE per table, from information_schema
  fixtures.csv   every table's cardinality and contents, as RFC4180 CSV
```

`schema.sql` then `fixtures.csv` then `queries.sql` rebuilds the finding on
any server, without the round's log. Two details that are deliberate, both
learned from the first five dumps this produced:

- **The dump never issues `SELECT *`.** Wadjet's pgwire sends no
  RowDescription for a star projection returning zero rows (#846), so JDBC
  throws "No results were returned by the query" — which replaced three of
  those five dumps' table contents with a stack trace. The column list comes
  from `information_schema.columns` and is always explicit. Do not
  "simplify" it back.
- **Values are CSV-quoted, and NULL is unquoted.** The generator produces
  values containing `|`, `"` and newlines; an unquoted dump of those is not
  recoverable, and `NULL` has to be distinguishable from the string
  `"NULL"` and from `""`.

Reading `rerun.txt` — it states the verdict per arm, so this is what the
verdict MEANS rather than how to derive it:

- **STABLE** (re-run matched the first read). The answer is
  deterministically wrong. Replay the seed per "Reproducing a finding from
  a seed" and it will reproduce; reduce it and file it.
- **TRANSIENT** (re-run differed). The first read saw state that no longer
  exists — manifest visibility, background compaction, a cache. This is the
  class three soaks' worth of violations fell into, and the dump is the only
  place it is visible. Attach the whole directory to the issue; a seed
  replay will NOT reproduce it and its absence is not evidence.

Row sets are sorted before comparison, because an unordered query may
legitimately return its rows in any order — so a reordering never reads as
TRANSIENT. When a TLP oracle only ever held its three partition branches
concatenated, the concatenation carries the verdict and each branch is
dumped separately for its SQL, plan and re-run rows, marked `verdict: n/a`.

The directory root is `sqlancer-violations` under the run's working
directory (`run.sh` always sets one; it prints the path to stderr).
Override with `-Dwadjet.violationDir=/some/path` in `JAVA_TOOL_OPTIONS`.
Nothing in the dump can fail the run: every step is best-effort and a
dump that cannot be written leaves the finding's own message intact.

Covers the aggregate arm (`PostgresTLPAggregateOracle.aggregateCheck`) and
both result-set arms — cardinality and content — through
`ComparatorHelper.assumeResultSetsAreEqual`, which TLP-WHERE, TLP-HAVING
and `QUERY_PARTITIONING`'s composite all share.

## Triage protocol

For every SQLancer-reported failure, in order:

0. **A TLP violation wrote a dump — read it before anything else.** See
   "Self-diagnosing violation dumps" above. `rerun.txt` answers
   stable-vs-transient in one look, and the rest of this protocol only
   applies to a STABLE finding: a transient one cannot be reduced or
   replayed, and step 2 will waste the afternoon that three previous soaks
   already spent. Attach the directory to the issue either way.
1. **Classify the failure shape first — with the classifier, not a grep.**
   This section used to say to grep a soak log for `"counts mismatch"` /
   `"mismatch:"`. **That grep cannot match a real TLP violation and never
   could** — a defect found on 2026-08-25 (wadjet#289) after the standing
   soak's own `"0 genuine violations"` reading turned out to be an artifact
   of this exact grep, not a real absence of violations. Every genuine
   oracle-detected wrong-result finding's `AssertionError` message is
   oracle-specific:
   - NoREC (`NoRECOracle.check`): `"the counts mismatch (%d and %d)!"` —
     this is the one case the old grep actually matched.
   - TLP-WHERE / TLP-HAVING (`ComparatorHelper.assumeResultSetsAreEqual`,
     shared by both and by `QUERY_PARTITIONING`'s composite):
     `"The size of the result sets mismatch (%d and %d)!"` or `"The
     content of the result sets mismatch!"` — no colon after "mismatch",
     and not "counts". The old grep's `"mismatch:"` half was chasing text
     that does not exist anywhere in SQLancer's source.
   - TLP-Aggregate (`PostgresTLPAggregateOracle.aggregateCheck` and its
     per-dialect equivalents): `"the results mismatch!"`.
   - PQS (`PivotedQuerySynthesisBase.reportMissingPivotRow`): `throw new
     AssertionError(query)`, whose message is just the bare, unquoted SQL
     query string — **byte-for-byte the same shape** as the ordinary
     *unexpected error* below. The only way to tell a genuine PQS
     violation apart from routine noise is the **stack trace**: look for
     a frame naming `PivotedQuerySynthesisBase.reportMissingPivotRow`.
   - CERT (`CERTOracle.check`; see "do not use" above): `"Inconsistent
     result for query: " + query1 + "; --" + rowCount1 + "\n" + query2 +
     "; --" + rowCount2` — its own distinct text, at least, so unlike PQS
     it needs no stack-frame check to tell apart from an unexpected error.

   Everything else (an `AssertionError` whose message is just the raw
   query text, thrown by `SQLQueryAdapter.checkException`) is an
   *unexpected error*: wadjet rejected or failed on a generated statement
   with a message that isn't in `sqlancer.postgres`'s Postgres-shaped
   `ExpectedErrors` list. Both are worth triaging, but they mean different
   things.

   One more trap a grep (correct pattern or not) can't see around: SQLancer's
   own per-round reproduction log (`logs/wadjet/database<N>[-cur].log` —
   what "Reproducing a finding from a seed" above points you at) never
   prints the unprefixed exception form at all, only the `"--"`-commented
   echo `state.getState().getLocalState().log(...)` appends after a
   round's real DDL/DML. A tool (or a human grep) that dedupes a
   *session* log's doubled form by skipping every `"--"`-prefixed line
   would find nothing at all in a file like that and report a silent 0.
   `tools/sqlancer/triage` handles this (see its package doc and
   `classify_test.go`'s `roundLogEchoOnlyFixture`), but it's worth knowing
   about if you're ever tempted to grep one of these files directly.

   Use `tools/sqlancer/triage` (`task sqlancer:triage LOGS="<path>"`,
   or `go run ./cmd/sqlancer-triage <file-or-dir>...`) instead of grepping
   for these by hand — it classifies every failure shape above (plus a
   wadjet crash's echo on the SQLancer/JDBC side: connection
   refused/reset, an I/O error sending to the backend) and prints counts
   plus each genuine violation's best-effort minimized query. Its exit
   status is non-zero if any source was unreadable or only partially
   scanned, so a script driving it can tell a clean run from one whose
   counts are a floor, not a complete answer. It
   transparently reads gzip'd session logs and walks a directory
   recursively. See `tools/sqlancer/triage/classify_test.go` for the
   classifier's own fixtures (several are real snippets pulled from a
   past soak) if you need to see the exact shapes it recognizes.
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

> **Correction (2026-08-25, wadjet#289):** the "0 genuine oracle
> violations" reading below was measured with the `"counts mismatch"` /
> `"mismatch:"` grep this README used to recommend, which cannot match a
> genuine TLP violation (see "Triage protocol" above) — it could report
> 0 violations whether or not any occurred. This pilot's own soak was too
> small (~200s) to say either way with the corrected classifier; the
> re-triage that matters is the full soak below.

`--oracle QUERY_PARTITIONING`, wadjet main @ `b570b5e879404c16e49f5d89f48164660363ad67`,
`--num-threads 1`, patches above applied. Two soaks after the patch set
landed, ~200s / up to 60-200 database rounds each, `--test-collations=false`:

- **0 genuine oracle violations** (`"counts mismatch"` / `"mismatch:"`) in
  either soak — see the correction above.
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

## Full soak results (2026-08-24, wadjet#289)

The deep soak the pilot deferred. wadjet main @ `1cf758ba`, `--num-threads 1`
per pair, patches above applied. Ran as many independently-supervised
(wadjet server + SQLancer process) pairs in parallel — one pair per
oracle/seed, each on its own ports/data-dir/nats-store-dir — rather than
one long single-threaded run, since wadjet crashes discovered during this
soak (see below) made single-pair sequential throughput too slow to reach
a meaningful volume in a reasonable wall-clock budget. Each pair's
supervisor auto-restarts wadjet on crash and keeps going with a fresh
SQLancer invocation, so a crash costs only a few seconds of downtime
instead of ending that seed's run.

**Total: 182,037 queries** across 33 oracle/seed pairs, ~135 minutes of
soak execution (a 90-minute base run — WHERE/HAVING/QUERY_PARTITIONING/
NOREC at 5 seeds each, PQS at 3 seeds, all concurrent — followed by a
45-minute extension adding 5 more seeds each to WHERE and HAVING, whose
crash rate had left them well under the other oracles' per-wall-clock-
minute yield). **Zero genuine oracle violations** (`"counts mismatch"` /
`"... mismatch:"`) at any volume — every TLP-WHERE, TLP-HAVING,
TLP-Aggregate (via QUERY_PARTITIONING), NoREC, and PQS check that actually
completed agreed with wadjet's own answer.

> **Correction (2026-08-25, wadjet#289): that "zero genuine violations"
> reading was wrong — the grep it was measured with cannot match a TLP
> violation.** Re-run with `tools/sqlancer/triage` over this soak's own
> retained logs (`soak-run/<oracle>-seed<seed>/session-*.log.gz` +
> `crash-stacktraces.log`), the corrected counts are:
>
> | Oracle | Reported (old grep) | Corrected (triage tool) |
> |---|---:|---:|
> | WHERE | 0 | 0 (see caveat below — this is not evidence TLP-WHERE is clean) |
> | HAVING | 0 | **9,913** (TLP result-set) |
> | QUERY_PARTITIONING | 0 | **2,259** (1,760 TLP result-set + 499 TLP-Aggregate) |
> | NOREC | 0 | 0 |
> | PQS | 0 | 0 |
> | CERT | not run | 0 (this classifier check applies retroactively; CERT was never one of this soak's oracles) |
> | **Total** | **0** | **12,172** |
>
> **The WHERE row's 0 is not proof TLP-WHERE is clean.** Of the 1,760
> QUERY_PARTITIONING TLP-result-set violations, 418 carry a
> `TLPWhereOracle.check` stack frame (via `Finding.OracleCheck` —
> `tools/sqlancer/triage` now records which oracle method's frame produced
> each finding, precisely to make this check possible) — TLP-WHERE
> genuinely fires there. The *dedicated* WHERE-only soak (10 seeds,
> 31,576 queries) reported 0 not because the check is clean, but because
> it barely ran: only 10 `AssertionError`s of any kind (mismatch or
> otherwise) occurred across all 31,576 queries in that run — every other
> query attempt was a connection-refused/reset symptom of that soak's own
> crash-restart loop (see "The crash class that dominated this soak"
> below), so TLP-WHERE's own check almost never got to execute to
> completion there. "0 because clean" and "0 because starved" are not the
> same finding, and only `QUERY_PARTITIONING`'s mixed run (where the crash
> rate was lower) can currently tell them apart.
>
> Every TLP result-set violation sampled (dozens, spanning single-table
> and multi-table, WHERE-only and HAVING shapes) has a `GROUP BY` with a
> `SELECT`-list column outside it and not wrapped in an aggregate —
> wadjet#590's exact shape, still open and unfixed at this soak's `main @
> 1cf758ba` (unchanged as of this writing). Of the 499 TLP-Aggregate
> violations, 85 (17.0%) have a partition predicate (the `p` in the
> `WHERE p` / `WHERE NOT p` / `WHERE (p) IS NULL` split) that is *itself*
> exactly `CAST(... AS BOOLEAN)` or `(...)::BOOLEAN` — wadjet#592's exact
> shape. (A looser count — a `CAST`-to-`BOOLEAN` pattern appearing
> *anywhere* in the query, not necessarily as the whole partition
> predicate — reaches 125 (25.1%); that looser number was this
> correction's first draft and is not what wadjet#592 actually describes,
> so the 85/17.0% figure is the one to trust.) The remaining TLP-Aggregate
> violations match a TLP-Aggregate/multi-table-join lead documented (but
> not reproduced) on wadjet#289's 2026-08-25 standing-soak comment — one
> concrete instance was replayed here byte-for-byte from this soak's own
> log (exact `CREATE`/`INSERT`/query sequence) against a fresh wadjet
> instance and still did not reproduce, so it is **not** filed as a new
> issue; it remains an open, unconfirmed lead. No violation sampled from
> this soak needed a new issue — all trace to wadjet#590/#591/#592
> (`bug,correctness,priority:high`, all open) or the unconfirmed lead
> above, both already on record before this correction.

| Oracle | Seeds | Queries (sum) | Crashes (auto-recovered) |
|---|---|---|---|
| WHERE (TLP) | 1001–1010 (10) | 31,576 | 824 |
| HAVING (TLP) | 1001–1010 (10) | 19,436 | 1,149 |
| QUERY_PARTITIONING (TLP-WHERE+HAVING+Aggregate composite) | 1001–1005 (5) | 33,299 | 638 |
| NOREC | 1001–1005 (5) | 34,077 | 435 |
| PQS | 1001–1003 (3) | 63,649 | 84 |
| **Total** | | **182,037** | **3,130** |

(Per-seed breakdowns are in each pair's `progress.log`/`summary.txt` under
`soak-run/<oracle>-seed<seed>/` from this soak's own scratch output — not
committed, not part of the harness itself.)

QUERY_PARTITIONING and NOREC cleared 50k+ comfortably per the plan's
target; PQS well exceeded it. WHERE and HAVING, even after doubling their
seed count, landed short of 50k — see "The crash class that dominated
this soak" below for why.

### The crash class that dominated this soak

Five new, genuine wadjet bugs were found and filed, **all of them
process-crashing panics**, none of them wrong-value oracle violations:

- **#508** — `HashJoin`'s build-side goroutine (`buildJoin` in
  `internal/planner/physical/plan.go`) has no panic recovery at all, so
  even the *designed* `FatalEvalPanic` error class (invalid cast,
  division by zero, the #361 type-mismatch guard) crashes the entire
  server instead of returning a client error, whenever the panicking
  expression is evaluated on a join's build side. Dominant single crash
  cause in this soak (~1,050 of 3,130+ crashes were the invalid-cast
  variant alone).
- **#509** — `CONCAT()`/`||` with a non-text argument after position 0
  (e.g. `CONCAT(text_col, int_col)`) indexes an empty offsets array and
  crashes the server — no join needed, an ordinary single-table `SELECT`
  triggers it. Root cause: `FuncCall.EvalVec`'s text-readability guard
  only checks argument index 0. Seen at dozens of different argument
  counts/offsets (not just the small-n example in the filed issue).
- **#510** — `joinFlushSource.Close()` (RIGHT/FULL OUTER JOIN unmatched-row
  flush) dereferences a nil `*pipelineSource` when `Close()` runs without
  a preceding `Init()` — observed nested under a set operation.
- **#511** — the architectural pattern behind why #509/#510 (and any
  future undiscovered panic) crash the whole process: only the deliberately-
  raised `FatalEvalPanic` class is converted to a client error anywhere in
  the call stack; every other panic is *designed* to re-panic past every
  existing recovery point, with nothing left to catch it. Filed as its own
  issue since fixing #508/#509/#510 individually doesn't close the class.
- **#512** — `Pipeline.runParallel`'s "first error wins" tracking uses a
  plain `sync/atomic.Value`, which panics if two racing workers store
  different *concrete* error types — which two different panic/error
  sources in the same function routinely do.

All five are `bug,correctness,priority:high`. None overlap the pre-existing
open-issue list (`gh issue list --state open`, including #493, #497,
#500/#501, #504-#507, #478, #482, #488-#490) checked at triage time —
these are new failure modes, not recurrences of that territory (which
needs FLOAT/DECIMAL/network types this harness's type pool structurally
excludes).

**Every one of the 3,130 crashes in the entire soak matches one of these
five signatures** — confirmed by grepping all captured stack traces for
anything outside {invalid-cast/division-by-zero (#508), index-out-of-range
(#509), nil-pointer (#510), atomic.Value race (#512)}: zero unmatched. No
sixth crash class was hiding in the volume.

This is the practical reason WHERE and HAVING undershot 50k even after
doubling their seed count: HAVING's crash rate was roughly 1 restart per
17 queries executed (worse than any other oracle — its generated
predicates apparently hit the #508 goroutine path more often than the
other oracles' shapes do), so a large fraction of each pair's wall-clock
budget went to wadjet-restart + re-establishing schema rather than
executing oracle checks. **Fixing #508 alone (add
`exec.RecoverFatalEval` to `buildJoin`'s goroutine) would likely multiply
achievable soak throughput several-fold**, since it's both the single
largest crash contributor and the cheapest of the five to fix.

### Operational hazards found running this soak (beyond the crash bugs)

- **A runaway multi-way join can fill the disk in minutes.** Early in
  this soak, with `--query-timeout` unset (server default, unbounded),
  several of the ~20 parallel pairs independently generated an unlimited
  multi-way join; one worker's shuffle spill alone reached 10+ GB, and
  collectively they took the box from 40 GB free to 1.6 GB free in under
  two minutes — this soak's own near-miss, caught and recovered before
  data loss. **Fix: run the wadjet server under `--query-timeout`** (this
  soak used `8s` — generous for the tiny generated schemas) — it aborts
  the runaway query with a normal client error instead of an unbounded
  spill. A companion disk-watchdog (poll `df`, kill every soak process if
  free space drops below a floor) is cheap insurance for any soak that
  didn't have `--query-timeout` from the start.
- **The same runaway-join shape can also balloon server RSS** (observed
  ~19 GB RSS for one query within the `--query-timeout=15s` window, on a
  server configured with `--memory-budget=256MiB`) — the memory budget
  does not appear to bound in-flight cross-join row generation before a
  spill-eligible operator is reached. Not filed as its own issue (out of
  this soak's scope to fully characterize), but worth a memory-budget
  follow-up look, and a reason to keep `--query-timeout` tight and to run
  each soak pair with an explicit `-Xmx`/`GOMEMLIMIT` cap plus the same
  kind of external memory watchdog used here.
- **`--num-threads` must stay 1 per wadjet instance** (unchanged from the
  pilot's finding) — this soak's parallelism came from running many
  *independent* wadjet-server + SQLancer pairs side by side (own ports,
  own data-dir, own NATS store-dir), never from `--num-threads > 1`
  against one server.
- **A crashed wadjet mid-session makes SQLancer blitz through its
  remaining `--num-tries`/`--max-generated-databases` budget with
  near-instant `Connection refused` failures** if nothing intervenes —
  harmless to data but pure wasted wall-clock and (with a naive logger)
  thousands of near-empty per-round log files. This soak's supervisor
  detects the dead server via `kill -0` and restarts immediately rather
  than letting a session run to its own timeout; a soak without that
  supervision should bound `--timeout-seconds` tightly per session for
  the same reason (note: SQLancer's `--timeout-seconds` is the *whole
  run's* wall-clock budget via `execService.awaitTermination`, not a
  per-query timeout as this repo's pilot notes implied — size it to the
  session length you actually want, not "a few seconds").

### PQS — feasibility verdict: feasible, worth including by default

The pilot never tried PQS. This soak did: 63,649 queries across 3 seeds,
45 minutes, **zero violations**, and every crash it hit matched an
already-filed signature (#508, #512) — nothing PQS-specific. Noise-wise,
PQS leans heavily on `BETWEEN SYMMETRIC` (unsupported wadjet syntax,
`unknown function: symmetric` — SQLancer's PQS pivot-predicate synthesis
uses it far more than TLP/NoREC's expression generator does) and the same
unpatched `IS UNKNOWN` gap as the other oracles. Despite that noise floor,
PQS was the single highest-throughput oracle in this soak (comfortably
above the other four's per-seed rate, and the lowest crash rate by a wide
margin) and found nothing structurally different from them.
**Recommendation: include PQS by default in the standing soak.** A
`BETWEEN SYMMETRIC` rendering patch (drop it the same way
`patches/0001-wadjet-dialect-fixups.patch` already drops `ONLY table`/
`SELECT ALL`/`ISNULL` — SQLancer's Postgres dialect can always fall back to
plain `BETWEEN`) would likely raise its useful-query fraction further; not
implemented in this soak (timeboxed per the task).

### Noise profile (unexpected-error classes, full soak)

173,938 "unexpected error" events (an uncaught exception SQLancer's own
`ExpectedErrors` list doesn't recognize — see "Triage protocol" above),
across 7,693 distinct normalized messages, excluding the crash-class
panics covered separately above. Bucketed by normalized wadjet error text:

| Count | Class | Verdict |
|---|---|---|
| 39,574 | `join ON residual "..." not evaluable as a probe residual` (right/left/full) | Loud, by-design rejection (CLAUDE.md's loud-failure philosophy) — not a defect |
| 29,952 | `join ON "...": ... cannot be represented as an equi-join key` (incl. `BETWEEN`/`BETWEEN SYMMETRIC` residuals) | Same — loud, by-design rejection |
| 27,754 | `expected NULL, TRUE, FALSE, or DISTINCT FROM after IS [NOT]` (WHERE/JOIN ON/GROUP BY/HAVING/derived-table contexts) | **The pilot's unpatched `IS UNKNOWN`/`IS NOT UNKNOWN` follow-up** — now confirmed as one of the single largest noise sources across the harness, not just TLP-WHERE. Same fix shape as the already-applied `ISNULL`/`NOTNULL` patch; highest-value harness patch to add next |
| 21,694 | `batch: cannot store ... vector (#361 silent-write guard)` (string→BOOL, string→INT*, bool→INT*) | The #361 guard firing and being caught gracefully on every path except the one goroutine #508 fixes — high volume suggests SQLancer's INT/BOOL/TEXT corpus frequently generates implicit-coercion shapes wadjet's type resolution rejects; not independently investigated further this soak |
| 18,472 | (long tail: thousands of distinct normalized messages, most under 50 occurrences each) | Not bucketed individually — see the soak's raw per-message counts if resuming this triage |
| 17,360 | `ORDER BY tN.cN: over a GROUP BY, only a grouped column, a grouping expression, or a select-list alias can be sorted on` | New dominant class not seen in the pilot's smaller sample — appears to be a real, intentional SQL restriction message; not examined further, candidate for a closer look given the volume |
| 10,380 | `parse: parsing SQL: expected )` (WHERE/JOIN ON/GROUP BY/HAVING/derived-table contexts) | Parser edge case, still unexamined (carried over from pilot) |
| 4,762 | `native DAG: stage scan-N has no dependencies and no ScanFiles` | Carried over from the pilot's own "unexamined, worth a follow-up look" — still true at ~10x the pilot's volume |
| 3,566 | `physical plan: UNION ALL: ...` (cites issue #346) | #346 is closed; this is an intentional remaining unsupported-shape rejection referencing its historical context, not a regression — confirmed via `gh issue view 346` |
| 424 | `hash aggregate: ... schemaless batch (#277)` | Same — #277 is closed; intentional guard, not a regression |

Follow-up for the harness itself (mirrors the pilot's own recommendation,
now with much higher confidence given the volume): patch `BETWEEN
SYMMETRIC` and `IS UNKNOWN`/`IS NOT UNKNOWN` out of the generator the same
way `ONLY table`/`SELECT ALL`/`ISNULL` already are — between them these
two account for roughly a third of all unexpected-error noise across the
whole soak.

### Recommendation for the standing pre-release soak configuration

1. **Fix #508 first** (add `exec.RecoverFatalEval` recovery to
   `buildJoin`'s build-side goroutine) — it is both the single largest
   crash contributor in this soak and the cheapest of the five filed bugs
   to fix, and fixing it should multiply achievable query throughput per
   wall-clock minute several-fold by eliminating the dominant
   restart-overhead tax. Fix #509/#510/#512 too before the next full soak
   — none is individually large, but all three are unconditionally
   process-fatal today.
2. Land the `BETWEEN SYMMETRIC` and `IS UNKNOWN` harness patches (same
   shape as the existing `ONLY table`/`SELECT ALL`/`ISNULL` patches) —
   together they're roughly a third of this soak's noise floor, and
   PQS in particular is noise-dominated by the first of the two.
3. Run all five oracles used here (`WHERE`, `HAVING`, `QUERY_PARTITIONING`,
   `NOREC`, `PQS`) by default — none showed a reason to exclude it, and
   PQS in particular earns a place after this soak's positive verdict.
4. Keep this soak's operational scaffolding: many independent
   (wadjet-server + SQLancer) pairs run in parallel rather than one
   long `--num-threads 1` run, `--query-timeout` set on every server
   (a few seconds is plenty for a small generated schema), an external
   disk-space watchdog, and a supervisor that restarts wadjet on crash
   and keeps accumulating query volume rather than letting one crash end
   a seed's run early.
5. Target 50k+ queries per oracle per the original plan, but budget wall
   time by *crash rate*, not by raw query count, until #508 lands — this
   soak needed roughly 2x the wall time for WHERE/HAVING that
   QUERY_PARTITIONING/NOREC/PQS needed to reach a comparable volume.
6. Every fix lands as a permanent gate entry per
   `feedback-external-adversarial-tooling` — a minimal repro for each of
   #508–#512 is in the filed issues; none has a regression test yet.

## Licensing

`sqlancer/sqlancer` is GPLv3-licensed. It is cloned fresh by `build.sh`
into a scratch directory and never vendored into or committed to this
(AGPL-3.0) repository — only this harness's own thin adapter
(`adapter-src/`) and the small dialect patch (`patches/`) are ours, kept
here as plain text against SQLancer's public source, same as any other
patch file.
