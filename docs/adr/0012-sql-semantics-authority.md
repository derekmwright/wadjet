# ADR-0012: PostgreSQL is the SQL semantics authority; DuckDB is the performance goal and an oracle

Status: Accepted (2026-08-19)

## Context

Wadjet ships the PostgreSQL wire protocol, and DuckDB serves as both the
performance bar and the differential correctness oracle. Those two facts
pull in different directions the moment the engines disagree about what a
query *means* rather than how fast it runs.

SQL leaves a surprising amount implementation-defined, and the two engines
resolve it differently. Default NULL placement is the worked example:
PostgreSQL sorts NULLS LAST for ASC and NULLS FIRST for DESC; DuckDB's
`default_null_order` is NULLS LAST in both directions. Neither is wrong.

The question surfaced during the 2026-08-18/19 correctness work and was
escalated for a decision, which exposed the real problem: **the answer was
already implied by a commitment nobody had written down.** CLAUDE.md states
pgwire compatibility is non-negotiable because Superset, psql and JDBC depend
on it; once that holds, semantics follow. ADR-0001 exists because settled
questions were getting re-asked, and this was one.

Escalation is not a neutral cost here. A semantics question presented as an
open preference arrives stripped of the commitment that settles it, so it can
be answered wrongly by someone without that context — and the gates then
*encode* the wrong answer. The stored DuckDB baseline is regenerated from
whatever the oracle says, so a wrong choice becomes self-verifying within
minutes, and the eventual correction shows up red. That is the poisoned-
baseline failure mode (`baseline-local-small.json` held a signature captured
from a broken engine, so a *correct* engine failed our own gate) one level up.

## Decision

1. **PostgreSQL decides semantics.** Where PostgreSQL and DuckDB disagree
   about the meaning of a query — result values, NULL handling, ordering
   placement, type coercion, error-vs-not, catalog introspection — wadjet
   follows PostgreSQL. The wire protocol is a behavioral contract, not just a
   byte format: clients and the tools above them encode PostgreSQL's behavior
   in their query generation.

2. **DuckDB remains the performance goal and a correctness oracle.** It finds
   real bugs (dozens during the 2026-08 correctness arc) and sets the speed
   bar. Neither role makes it the semantic authority.

3. **On a semantic divergence, configure the oracle — do not exempt the
   entry.** An exemption blinds the gate permanently to real bugs in exactly
   the queries most likely to have them; a configured oracle still compares
   every row. The differential gate runs DuckDB with
   `default_null_order='nulls_last_on_asc_first_on_desc'`.

4. **A decision a gate encodes must be asserted where regeneration cannot
   rewrite it.** `TestOracleIsConfiguredForPostgresSemantics` fails if either
   oracle invocation loses that setting, because otherwise deleting one line
   silently changes what "ground truth" means.

5. **Deliberate divergences from PostgreSQL.** (Amended 2026-08-23: collation
   was not the only one — the other three below were never PostgreSQL's call
   to begin with, and are recorded here so a future gate does not mistake
   them for undecided.)

   - **Collation.** Wadjet compares and sorts strings with BINARY collation,
     not PostgreSQL's locale-dependent collation. Locale-sensitive comparison
     costs real work on every string compare and sort, surprises analysts
     more than it helps, and no BI client depends on it. Where the oracle
     needs to agree, use a `C`-collation database rather than exempting
     string ordering.
   - **MIN/MAX over BOOL.** PostgreSQL has no `min(boolean)`/`max(boolean)`
     aggregate (verified against live PostgreSQL: it errors, "function
     min(boolean) does not exist") — `bool_and`/`bool_or` are its idiom for
     the same question. Wadjet supports MIN/MAX over BOOL as a deliberate
     extension, not a divergence PostgreSQL took a position on; `bool_and`/
     `bool_or` remain available and are still the PostgreSQL-idiomatic
     spelling.
   - **MAP and VECTOR ordering.** Both are wadjet-only types — PostgreSQL has
     neither — so their total orders (`internal/engine/exec/kernel/
     container_sort.go`) are wadjet-defined, not a choice against a
     PostgreSQL answer.
   - **MIN(bytes) declares BYTEA, not STRING.** The output type follows the
     input type, matching PostgreSQL's own `min(bytea)`; no divergence, noted
     here only because early declared-schema code guessed STRING for every
     MIN/MAX before the input-typed fix.

6. **Semantics decisions are technical, not product.** They are made and
   executed, then reported — not escalated. An existing project commitment
   settles everything downstream of it; check for the commitment before
   drafting the question.

## Consequences

- `ORDER BY x DESC` places NULLs first (changed 2026-08-19). The default had
  been unreachable before that: the DESC comparator negated the kernel's null
  handling along with its values, so the code *declared* PostgreSQL's rule and
  *emitted* DuckDB's.
- The differential gate keeps full strength across the ordering corpus rather
  than carrying exemptions.
- Open questions in the same territory, to be decided by this ADR's rule and
  recorded when they are: integer overflow behavior, `timestamptz` and the
  session TimeZone GUC, empty-string versus NULL, identifier case folding.
- DuckDB cannot adjudicate "would a PostgreSQL client expect this," so a
  PostgreSQL differential arm (small scale, plus a pgwire protocol comparison)
  is the natural completion of this decision. See ADR-0013.

## Related

- ADR-0001 (record architecture decisions — the re-asked-question problem)
- ADR-0013 (correctness gates and their deliberate boundaries)
- CLAUDE.md, "Don't skip pgwire compatibility"
- `benchmarks/tpch/oracle_semantics_test.go`, `internal/planner/physical/plan.go`
  (`resolveNullsLast`), `internal/distributed/messages.go` (`PlaceNullsLast`)
