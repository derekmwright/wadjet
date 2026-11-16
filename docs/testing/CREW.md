# The crew: architect, dispatcher, workers, judge

How agent work on Wadjet is organised. This is the maintainer's crew pattern
(<https://gist.github.com/derekmwright/3c49cf484754de92d383a79edff7dc95>) applied to this repo;
where this file and the gist differ, the difference is listed under *Adaptations* with its
reason. Backlog rules are in `BACKLOG.md`, gates in `GATES.md`, releases in `RELEASE.md`.

## Roles

| Role | Here | Does | Never does |
|---|---|---|---|
| **Maintainer** | Derek | Sets direction, answers design questions, owns anything public or irreversible | Relay messages between agents |
| **Architect** | the most capable model, the session that talks to the maintainer | Writes ADR skeletons, briefs and sorts the queue, rules on escalations, audits the process, pushes and tags when told | Dispatch routine work, read every hand-back |
| **Dispatcher** | `tools/crew/dispatch-stretch.js`, a Workflow script whose judgment steps run on the mid-tier model | Runs one bounded stretch: starts workers, verifies their hand-backs, lands on local main | Decide design, reword public claims, push, tag |
| **Workers** | the strong coding model, each in its own worktree | Build and review | Push, edit another agent's branch, rerun a failed test |
| **Judge** | an independent model from another vendor, read-only | Attacks design records round by round | Edit anything |

### The dispatcher is a script, not a long-running agent

A mid-tier model is reliable at one judgment at a time and unreliable at holding a long
horizon: it loses the queue, the round count and its own stop conditions. So the loop is code.
The script owns the order of work, the round counter, the stop conditions and the ledger; the
mid-tier model is called only for single bounded steps (check this hand-back with your own
commands; land this reviewed commit) and returns a typed result. A stretch is bounded (three
items by default), runs one item at a time, and stops at the first escalation, returning
control to the architect.

```
Workflow({ scriptPath: "tools/crew/dispatch-stretch.js",
           args: { base, notesDir, items: [{ id, title, brief }] } })
```

## Escalation triggers

A worker or the dispatcher stops and hands back to the architect when:

1. a new ADR or an amendment to one is needed;
2. a loop fails its third round, or a judge and a reviewer disagree;
3. the change alters what a user would see (an answer, an error, a wire type) beyond what the
   brief states, or drops user data;
4. a public statement about quality, exactness or security would be written or reworded
   (README, docs claims, release notes);
5. a gate or test failed, a conflict is not mechanical, or the base branch moved;
6. the brief and the runbooks do not cover the situation, or a rule would have to bend.

Triggers are written narrowly on purpose: a vague one costs an agent and a round trip every
time it fires wrongly.

## Design records

No design change gets code before its ADR (`docs/adr/`) is accepted.

1. The architect writes a **skeleton**: the governing rule, each decision in one line, what
   is explicitly out, and the replays the judge will run.
2. An author fills it in and verifies every statement against the code.
3. The judge attacks it; the author revises; repeat until the judge says settled.
4. The maintainer answers the open questions, each written to be answerable in one line with
   its cost.
5. Accepted. Only then does the first slice start.

**No stopgaps for work already planned.** If an accepted record replaces something, nobody
ships an interim version of it.

## The round-loop rule

For every loop (judge verdicts, code reviews, gate fixes): rounds one and two get a fix and a
retry. A third failed round stops the loop and goes to the architect: is the design wrong, or
does a simpler rule remove the whole class of defect? Escalate earlier if a round finds more
problems than the last, or breaks an earlier round's fix. The dispatcher script counts this;
no agent is asked to remember it.

## Engineering guardrails

- Each agent gets its own worktree. Binaries go to `dist/` of that worktree.
- Commits name their files. Never `git add -A`.
- No force-push, no history rewrites of shared branches, no hard resets.
- Root cause over workaround: keep the reproduction, find the cause, record it.
- Every defect that escapes becomes a check: write down what got past which gate, add the gate.
- Agents never run against the maintainer's data or the do-not-wipe benchmark buckets.
- Every agent writes a result note before handing back. When a parent dies, the note is how
  the work is recovered.

## Testing rules

- A failed test is never rerun and never called a flake. Stop and report.
- Every new test has a negative control: revert the fix, watch the test fail for the right
  reason, restore.
- No wall-clock tests.
- A pinned value is proven against PostgreSQL before review (ADR-0012).
- A change to a shared primitive gets the full suite, not `task affected`.

## Evidence

- Every claim links to a test, a record or a measurement, or it is cut. Release notes included.
- A computed figure is never called measured. An assumption is never stated as fact.
- Reviewers verify the builder's note against the code, not the code against the note.
- A hand-back is model output, not truth: the dispatcher re-checks the commit, the note and
  the gate result with its own commands.

## What the maintainer tests by hand

One checklist per release, ten items at most, holding only what a test cannot prove: how a
client tool (psql, DataGrip, Superset) behaves against a build, and changes to how the product
is used. Before the maintainer gets a build an agent drives each changed feature and reports;
defects found there are fixed or filed first. Unfinished items are carried forward on purpose
or archived, never left to pile up.

## Measurement

Each stretch returns typed ledger records (role, model, round, outcome, required-change count,
escalations); the architect appends them to the metrics ledger, where the SubagentStop hook
already records tokens and duration per agent. Per release: issues filed against issues
closed, and lines checked out of total per epic.

## Adaptations from the gist

- **Workers are the strong coding model for all code.** The gist allows the mid-tier model for
  scoped work; this repo's own trials (September 2026) had mid-tier authoring rounds regress
  or stall, so the mid-tier model here does verification, landing and filing only.
- **The dispatcher is a Workflow script** (above). The gist describes a mid-tier agent.
- **Result notes live outside the repo** (`notesDir`), not committed to it, so product history
  stays product.
- **"Proven on a second platform" becomes "proven against PostgreSQL"**: the cross-check that
  matters for a pinned value here is the semantic oracle.

## Not yet in place

Rules above that the repo does not enforce today. Each needs a decision from the maintainer
before it becomes work:

- the gist's **file-size budget with a ratchet** (no such gate exists);
- a **findings ledger** for audits, and audits handing back work packages rather than issues;
- existing **wall-clock tests** and the `flaky` label, which the testing rules above do not allow;
- the **patch-release cap** (a number has not been set).
