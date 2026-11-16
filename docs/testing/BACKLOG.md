# Backlog: how work is tracked to completion

The issue list is a work queue, not a parking lot. Every open issue is one of two things: an
**epic** that a release is committed to finishing, or a **standalone** issue that can be picked
up on its own. Nothing is filed and then left to wait for attention.

## The two units

**Epic** (label `epic`) — one root cause or one capability, with a checklist of acceptance
lines in its body. Each line is a repro; the line is checked when the repro agrees with
PostgreSQL on every arm it names (or is recorded as a kept divergence in ADR-0012) **and** a
regression test pins it. An epic rides one release cycle as that release's flagship: it is
milestoned to the release when work starts, and the release is what closes it.

**Standalone** — independently fixable, movable between releases, workable in parallel. These
fill in around the flagship epic. A standalone carries exactly one kind:

| Kind | Labels |
|---|---|
| defect | `bug` + one `priority:P0..P3` |
| PostgreSQL accepts, we refuse | `pg-gap` (no priority; it is not a defect) |
| everything else | `perf`, `feature`, `testing`, `flaky`, `chore`/`cleanup`/`refactor`, `documentation` |

`engine` / `distributed` keep their existing meaning (which license region is wrong).

## Priority (defects only)

| | Meaning | Rule |
|---|---|---|
| `priority:P0` | data loss, or a policy leak | preempts everything, including the flagship epic |
| `priority:P1` | silent wrong answer | next; a release does not ship a known new P1 |
| `priority:P2` | loud false refusal, crash, wrong declared type | filler, ordered after P1 |
| `priority:P3` | strictness, wording, cosmetic | filler |

An epic carries the priority of its highest unchecked line.

## Filing a finding

1. **Does it belong to an open epic's family?** Then it is a new checklist line on that epic,
   with the repro in a comment on the epic. Do **not** open a new issue. This is the rule that
   keeps one defect from fanning out into thirty orphans.
2. **Otherwise** file one standalone issue, labeled at filing: kind, priority if a defect,
   `engine` or `distributed`.
3. A finding that is neither is never left only in notes.

A closed issue carrying the `folded` label was closed *into* an epic line. It is not fixed; its
body is the repro for that line.

## Planning a release

- An epic bumps the **minor** version: one flagship epic per minor release. A second epic
  does not start until the first is closed or has taken the escape hatch.
- **Patch** releases carry standalone issues (and carry-overs); they never wait on an epic.
- Open P0 work is scheduled before anything else.
- Standalone issues are added as filler in priority order; a standalone that is not finished
  moves to the next milestone as-is (that is what makes it standalone).
- Before a line is worked, re-run its repro: some lines were fixed in passing by other work,
  and those are checked off with the commit that fixed them and a pinning test.

## The escape hatch (the exception, not the norm)

An epic is done when every line is checked. A release may ship with the epic incomplete only
when all of these hold:

1. at least 80% of its lines are checked;
2. no P0 line is unchecked, and every unchecked P1 line has a written reason;
3. the remainder is moved to **one** issue labeled `carry-over`, milestoned to the **next
   patch release**, and the epic is closed pointing at it;
4. the release retrospective records that the hatch was used and why.

A `carry-over` issue cannot be carried over again: it completes in that patch release. If
carry-overs show up in consecutive releases, the epics are being scoped too large — fix the
scoping, do not widen the hatch.
