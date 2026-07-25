# ADR-0001: Record architecture decisions

Status: Accepted (2026-07-25)

## Context

The architecture solidified through dozens of measured arcs whose
reasoning lives in `docs/design/` memos, PR threads, and benchmark
artifacts. The memos are deep but per-feature; nothing indexes the
*decisions* — which positions are settled, what they beat, and what
evidence would reopen them. Settled questions were getting re-asked.

## Decision

Keep ADRs in `docs/adr/`, one page each: Context, Decision,
Consequences, with links to the owning design memos and the validating
runs. ADRs ship with the code like design memos do — they are records,
not review requests. A decision is reopened only by a new ADR citing new
evidence.

## Consequences

- Relitigating a settled position starts by reading its ADR, including
  the refuted alternatives — cheaper than re-deriving them.
- The set must stay small and honest: only decisions with validation
  behind them get a record; open experiments stay in design memos until
  they settle.
