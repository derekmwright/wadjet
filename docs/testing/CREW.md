# The crew: architect, dispatcher, workers, judge

How agent work on Wadjet is organised. This is the maintainer's crew pattern
(<https://gist.github.com/derekmwright/3c49cf484754de92d383a79edff7dc95>) applied to this repo;
where this file and the gist differ, the difference is listed under *Adaptations* with its
reason. Backlog rules are in `BACKLOG.md`, gates in `GATES.md`, releases in `RELEASE.md`.

## Roles

| Role | Here | Does | Never does |
|---|---|---|---|
| **Maintainer** | Derek | Sets direction, answers design questions, owns anything public or irreversible | Relay messages between agents |
| **Architect** | the most capable model, the session that talks to the maintainer; the **escalation agent** | Writes ADR skeletons, briefs and sorts the queue and names each item's builder, rules on every escalation, audits the process, pushes and tags when told | Build or review routine work, dispatch it by hand, read every hand-back |
| **Dispatcher** | `tools/crew/dispatch-stretch.js`, a Workflow script whose judgment steps run on the fast model | Runs one bounded stretch: starts workers, verifies their hand-backs, lands on local main | Decide design, reword public claims, push, tag |
| **Senior engineer** | the strong coding model | Builds the risky code; **reviews every build**, whoever built it; judges an implementation dispute between a builder and the brief | Rule on design, or on anything a trigger names: that goes to the architect |
| **Builders** | the mid-tier model for scoped code, the fast model for well-structured grunt work and docs, each in its own worktree | Build | Push, edit another agent's branch, rerun a failed test unchanged, review their own tier's work |
| **Adversarial judge** | Codex: an independent model from another vendor, read-only | Attacks design records (ADRs) round by round until settled | Edit anything |

The chain of judgment runs upward: a builder's work is reviewed by the senior engineer, a design
record is attacked by the adversarial judge, and anything either of them cannot settle, or that a trigger
names, by the architect. The architect is the top of the chain and is spent only there.

### The dispatcher is a script, not a long-running agent

The cheaper tiers are reliable at one judgment at a time (the fast model at a quick check or
a docs change, the mid-tier model at a scoped piece of code) and unreliable at holding a long
horizon: it loses the queue, the round count and its own stop
conditions. So the loop is code.
The script owns the order of work, the round counter, the stop conditions and the ledger; the
fast model is called only for single bounded steps (check this hand-back with your own
commands; land this reviewed commit) and returns a typed result. A stretch is bounded (three
items by default), runs one item at a time, and stops at the first escalation, returning
control to the architect.

```
Workflow({ scriptPath: "tools/crew/dispatch-stretch.js",
           args: { base, notesDir, items: [{ id, title, builder, brief }] } })
```

## Escalation triggers

A worker or the dispatcher stops and hands back to the architect when:

1. a design decision has to be made or changed: a new ADR, or an amendment that changes what
   one decides. (Bringing a record's factual sentence up to date when the brief asks for it,
   such as what a gate measures, is not this.)
2. an implementation loop fails its third round, or a judge and a reviewer disagree;
3. the change alters what a user would see (an answer, an error, a wire type) beyond what the
   brief states, or drops user data;
4. a public statement about quality, exactness or security would be written or reworded
   (README, docs claims, release notes);
5. a gate or test failed for a reason the change does not explain, a conflict is not
   mechanical, or the base branch moved. (An existing test that pins the behaviour or plan
   shape the fix intentionally changes is not this: the worker updates the pin and lists it
   in the note, and the reviewer judges it.)
6. the brief and the runbooks do not cover the situation, or a rule would have to bend.

When the architect rules that an escalated build stands, the item goes back into a stretch
with `resume: { commit, branch, note, ruling }`: the build is not repeated, and the reviewer
is given the ruling as part of the brief.

Triggers are written narrowly on purpose: a vague one costs an agent and a round trip every
time it fires wrongly.

## Design records

No design change gets code before its ADR (`docs/adr/`) is accepted.

1. The architect writes a **skeleton**: the governing rule, each decision in one line, what
   is explicitly out, and the replays the judge will run.
2. An author fills it in and verifies every statement against the code.
3. The judge attacks it; the author revises; repeat **until the judge says settled, however
   many rounds that takes**. Six rounds here are cheap: a round costs one read-only judge run
   and an edit to a document. Six rounds on an implementation are not.
4. The maintainer answers the open questions, each written to be answerable in one line with
   its cost.
5. Accepted. Only then does the first slice start.

The judge is run by the architect as a read-only subprocess, at high reasoning effort, from
the repo root:

```sh
codex exec -C . -s read-only -c model_reasoning_effort=high \
  -o <verdict file> < <prompt: the record, the skeleton's replays, the previous verdict>
```

Its verdict names each attack with the line it attacks and ends SETTLED or NOT SETTLED. It
never edits; the author applies the revisions.

**No stopgaps for work already planned.** If an accepted record replaces something, nobody
ships an interim version of it.

## The round-loop rule

Spend rounds on the record, not on the code. The two loops are bounded differently on purpose.

**Design-record loop (judge and author): no cap.** It runs until settled. It still goes back
to the architect when a round finds more problems than the last or breaks an earlier
revision, because that means the skeleton is wrong, not the wording.

**Implementation loops (code review, hand-back verification, gate fixes): three rounds.**
Rounds one and two get a fix and a retry. A third failed round stops the loop and goes to the
architect with one question: what did the record leave undecided? The answer is an amendment
judged before more code is written, or a simpler rule that removes the whole class of defect.
Escalate earlier if a round finds more problems than the last, or breaks an earlier round's
fix. The dispatcher script counts this, in blocked REVIEWS; a hand-back that fails the
dispatcher's clerical checks (a missing note, a trailer, a wrong base) gets its own fix round
and its own limit, and is not a blocked review. No agent is asked to remember any of it.

The script has a self-test, `node tools/crew/dispatch-stretch.selftest.js`, which runs it
against a scripted mock. Run it after any edit to the script or its prompts.

A lower tier that needs an extra implementation round has usually cost more than it saved:
each round is a build, a verification and a strong-model review. Tier choices are judged on
total cost to a clean review, not on the price of the first build.

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

- A failed test is never rerun unchanged and never called a flake. Stop and report. After a
  diagnosed fix the gate runs again on the new commit; that is a new run, not a rerun.
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

## Tiers are versions, not names

The architect writes each item's brief and names its builder. All three tiers build:

| Builder | Give it |
|---|---|
| strong coding model | risky code: a shared primitive, a pipeline breaker's spill/drain/merge path, a planner seam, the parquet package, anything the brief cannot fully bound |
| mid-tier model | scoped code: named files, a stated repro, a stated shape for the fix |
| fast model | well-structured grunt work: the brief names the cause or the exact edit, the change repeats a pattern across sites, or it is docs only |

Review is always the strong model, whoever built. That is what makes a cheap build safe to
take, and it is also the floor on cost: an extra round on a cheap builder costs a strong-model
review, so pick the cheapest builder that will be clean in one round, not the cheapest one.

A rule about what a tier can do is evidence about one model version and expires with it:

- The ledger records the exact model id of every agent, so a tier's record is per version.
- When a new version of a tier ships, earlier limits on that tier are re-tested, not carried
  forward. The September 2026 finding that mid-tier authoring rounds regressed or stalled was
  measured on Sonnet 5 and does not describe Sonnet 5.5.
- The procedure for testing a tier, and every result so far, is in `TIER-BENCHMARK.md`.
- How an item is scored and routed today, the ledger every attempt is recorded in, and the
  benchmark set are in `ROUTING.md`. Until a profile cell has three rows, its conservative
  rule decides, not the table above.

## Measurement

Each stretch returns typed records (role, model, round, outcome, required-change count,
escalations); the architect turns them into one row per item in `tools/crew/ledger.jsonl`
(`ROUTING.md`), with cost taken from the agents' transcripts (`tools/crew/ledger.py cost`). Per release: issues filed against issues
closed, and lines checked out of total per epic.

## Adaptations from the gist

- **The dispatcher is a Workflow script** (above), with the fast model for its steps. The gist
  describes a mid-tier agent.
- **The design-record loop has no round cap.** The gist stops every loop at the third failed
  round; here that applies to implementation loops only, on the maintainer's instruction.
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
