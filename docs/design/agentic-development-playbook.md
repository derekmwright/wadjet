# Building a large system with agentic coding: what transferred, and what only grew

Written 2026-08-29, after the numeric-parity arc (ADR-0024). Wadjet is a
columnar analytics engine — parser, planner, vectorized executor,
distributed stage DAG, Parquet storage, PostgreSQL wire protocol — built
greenfield with AI agents doing nearly all of the writing and a human
setting direction, approving deploys, and deciding what "done" means. This
note is an attempt to separate what would transfer to another project from
what was specific to this one, so the next project starts closer to where
this one ended up.

The short version: **the rules transfer; the conviction does not.** Every
durable practice below was installed *after* a failure made its absence
expensive. A fresh project handed the full set on day one would ignore or
misapply most of it, because none of it would yet have a failure attached.
What transfers is a small starter kit plus one growth rule that generates
the rest.

## The starter kit (day one, cheap, uncontroversial)

1. **One named external authority for semantics.** For Wadjet it is
   PostgreSQL (ADR-0012): "is this what a PostgreSQL client expects" is a
   question only PostgreSQL can answer. DuckDB is the performance target
   and a second oracle. Every project has an equivalent — a spec, a
   reference implementation, a competitor's behaviour. Pick one, write it
   down, and make *disagreement with it* the definition of a bug. This ends
   most arguments before they start, and it gives the agents something to
   disagree with other than their own priors.

2. **A differential oracle against that authority, from the first week.**
   Hand-written tests encode what the author already believed. An oracle
   finds what nobody thought to ask. Wadjet's runs every corpus query
   through both engines and compares values *and* what the wire carries
   (type OIDs, typmods, SQLSTATEs); a value oracle cannot see a right value
   under a wrong type.

3. **Pins that fail when they start agreeing.** A known divergence is
   recorded in the corpus as a `knownBug` entry that *fails the day the two
   engines agree*. Deleting the pin is the proof of the fix. This turns
   every known defect into a ratchet and makes "configure the oracle, never
   exempt the entry" enforceable.

4. **Regression test first, fix second** — the test must fail before the
   fix and pass after, and the commit says which. This is in `CLAUDE.md`
   and it is the one rule agents follow without being reminded.

5. **Decision records with a reopen clause.** An ADR states the position,
   the alternatives it beat, and what new evidence would reopen it. Agents
   read them before proposing changes in their territory. Without this, a
   settled question is re-litigated by every fresh context.

6. **Persistent memory for the agent, with the *why*.** Not code
   structure — the repo has that — but the guidance the human gave and the
   reason behind it, dated, with relative dates made absolute. The
   feedback file that says "never gate a correctness fix on a perf A/B"
   carries the incident that taught it.

## The growth rule (the actual framework)

> **Every escaped defect produces a gate that would have caught it — and
> the gate's *class*, not just the test, is written into the protocol.**

This is the whole thing. A test suite grows by tests; a correctness
*system* grows by classes of gate. The difference is whether the next
defect of the same shape is caught by the machinery or by luck. Some of
Wadjet's, in the order they were forced:

| Escaped defect | What it forced | Class it added |
|---|---|---|
| A bug in `GetValue` for BYTES was invisible to every gate — no corpus had a BYTES column | A coverage matrix: every type × every consumer class must appear in some corpus | *Gates must exercise the whole type system; a type absent from every fixture is untested by definition* |
| A BI tool found 16 engine bugs in one day; every one silent; the two execution paths disagreed and nothing compared them | The two-path invariance suite, and the DuckDB fingerprint gate on *both* paths | *Two engines that must agree are compared on every shape, not assumed equal because they share a file* |
| The local fast path fell back to the distributed path on error, hiding that the two disagreed | Strict mode: a deterministic failure is reported, never retried on a path that may answer differently | *A fallback that can change the answer is a silent-wrong generator; loud beats plausible* |
| A pinned divergence quietly un-diverged and the pin kept passing | Pins that FAIL when they agree | *Every known-wrong is a ratchet* |
| Three fixes landed CI-red because the top-level package was outside the pre-push hook's path | `./wadjet/` added to the land battery; the rule written down | *The gate battery is enumerated, not assumed; a green hook proves only what the hook runs* |
| An optimization changed the row set; nothing could localize which one | Every row-set-affecting optimization registers a kill switch; an invariance oracle runs the corpus with each switch off | *Optimizations are individually disableable, and the oracle names the culprit* |
| Half of first-pass fixes in the numeric arc were green on every gate and still silently wrong; two turned a correct answer wrong | An adversarial review layer whose only currency is a concrete failing input; then the reviewers' methods written as the *author's* checklist | *Refutation is a role; boundary probing is the author's job before it is the reviewer's* |

Each row is a story with a cost attached. That cost is the conviction the
rule needs. It cannot be shipped in a framework — but the *expectation*
that every escape produces a row in this table can, and that expectation is
what makes the system compound instead of accrete.

## What an agent workflow needs that a human one does not

- **Refutation as a separate role.** An agent that wrote a fix will find
  its own tests convincing. A second agent told to *refute* it — concrete
  input, expected answer per the authority, observed answer per path, or
  nothing — catches what the author's gates cannot, because it probes
  boundaries the corpus never named. Roughly half of first passes in the
  numeric arc had a real defect; every one was found this way.

- **Issues filed at the mechanism.** Reviewers surface symptoms faster than
  anyone fixes them. Six symptoms of one producer-emits-no-stage gap are
  one issue with a shape table, fixed and reviewed as one change; six
  issues are six review cycles for one fix.

- **Serial landing on a combined tree.** Parallel agents in isolated
  worktrees, but one cherry-pick at a time onto `main`, the full battery on
  the *combined* tree, and nothing touching the tree while a push runs.
  Parallelism in authoring, none in landing.

- **Hard rules in the harness, not the prompt.** "Bug fix ⇒ regression
  test", "commit with `-s`", "never build into the package directory" —
  these live in `CLAUDE.md`, the pre-push hook, and the Taskfile. A rule an
  agent has to remember is a rule it will eventually forget.

- **Memory that carries corrections.** The human's corrections ("never
  explain a benchmark delta with time-of-day lore", "release per arc, not
  per improvement") are the highest-value bytes in the system and the
  easiest to lose across contexts.

## What did not transfer, and why

- **The specific gate set.** TPC-H, ClickBench, the type-matrix fixture,
  the pg-oracle corpus — all Wadjet's. Another project has other fixtures.
  What transfers is that each was added in response to a named gap.

- **The order.** The two-path suite before the type matrix before the
  invariance oracle before the adversarial layer — that order was dictated
  by which failure came first, not by design. Installing them in a
  different order would have been fine; installing them all at once
  without the failures would not have taken.

- **The human's taste.** Which divergences are acceptable (a superset that
  answers where PostgreSQL refuses), which are P0 (a value divergence,
  always), when to tag a release, when to stop an arc. These are decisions
  the ADRs record after the fact; they were not derivable in advance.

## One sentence

The agents are only as good as the oracle you give them to disagree with —
and the oracle only gets good by being wrong in public, one recorded
failure at a time.
