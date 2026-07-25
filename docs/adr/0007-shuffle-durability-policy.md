# ADR-0007: Shuffle durability is a policy spectrum; eager stays the default

Status: Accepted (2026-07-24)

## Context

The shuffle-io ledger (PR #261) measured SF100 ground truth: **424.7 GB
of stage-output scratch PUT to S3 per suite pair, zero bytes ever read
back** — the durable copies are pure fault-tolerance insurance under
ADR-0004's overlay. That made "do we have to pay for durability we never
consume?" a policy question rather than a structural one.

## Decision

`--shuffle-durability=eager|lazy|off` (PR #262), stamped per task:

- **eager (default)**: background uploads start as outputs finalize.
- **lazy**: uploads queue unstarted; released by a demand broadcast
  (consumer missing-input retry against a live producer, coordinator
  read miss) or worker drain; elided at query end. Eliminates the full
  ~425 GB PUT per pair.
- **off**: never upload; graceful drains also degrade to whole-query
  re-execution.
- Scalar-subquery producer stages always stay eager (the coordinator has
  no peer tier), and the adoption-failure sync-upload fallback is
  unconditional.

**Eager remains the default because the wall-clock effect of elision is
unstable**: two clean same-window SF100 pairs measured steady −7.1%
(2026-07-23) and +16.1% (2026-07-24) for identical mechanics — the
eager upload's read-back of all scratch is an accidental page-cache
actor whose removal cuts both ways (the same cache-composition regime as
the Q18/#259/#260 arc). Lazy is a validated **cost knob** (S3
requests/egress), not a performance lever, until that interaction is
tamed.

## Consequences

- Deployments that care about S3 cost flip lazy per cluster; benchmark
  and default configurations keep eager.
- Any future attempt to make lazy a wall win must come with a
  cache-composition mechanism hypothesis, not another pair of runs
  (n-of-2 contradiction already on record).
- Mixed versions are safe: the policy rides the task and unknown fields
  degrade to eager.

References: `docs/design/shuffle-durability.md`; PR #262 comments carry
both A/B tables.
