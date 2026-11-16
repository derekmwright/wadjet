# Tier benchmark: which model builds what

`CREW.md` routes work by tier: the strong coding model for risky code, the mid-tier model for
scoped work, the fast model for docs and the dispatcher's own steps. That routing is evidence
about specific model versions. This file is how the evidence is produced and where it is kept.

## When to run it

- A new version of any tier ships.
- A tier rule is about to be changed in either direction.
- A tier's record in the ledger drifts: more failed rounds or more escalations than its last
  benchmark showed.

## The question

**Total cost to a clean review**, not the price of the first build. An extra implementation
round is a build, a verification and a strong-model review, so a cheaper builder that needs
one more round has usually cost more than it saved. A tier wins an item class when it reaches
a CLEAN review for fewer total tokens without more rounds.

## Protocol

1. Pick an open standalone defect that is small, has a stated repro with PostgreSQL's answer,
   and needs no heavy gate. Write **one brief**.
2. Run one arm per tier from the **same base**, each as its own stretch so one arm's
   escalation cannot block another:

   ```
   Workflow({ scriptPath: "tools/crew/dispatch-stretch.js",
              args: { base, notesDir, land: false, maxItems: 1,
                      items: [{ id: "<issue>-<tier>", risk, title, brief }] } })
   ```

   `risk` selects the builder: `risky` the strong model, `scoped` the mid-tier model, `docs`
   the fast model. Verification and review are the same for every arm, so the builder is the
   only variable. `land: false`: nothing reaches main.
3. Record per arm, from the stretch's result and the workflow's usage figures: the exact model
   id, rounds to CLEAN (or the escalation and its trigger), total tokens across build, verify
   and review, wall time, and the reviewer's required changes per round.
4. Read the diffs. Two CLEAN arms can differ in where they fixed the defect; note which fix is
   at the cause.
5. Land at most one arm, through a normal stretch. Delete the other arms' worktrees and
   branches.

## Reading a result

- One item is a data point. Do not change a tier rule on fewer than three items of the class
  in question (a planner fix, an expression fix, a docs change are different classes).
- A stop on a correctly fired trigger is a good outcome for that arm, not a failure; a stop on
  a trigger that should not have fired is a defect in the brief or the trigger, and the run is
  repeated after fixing it.
- Compare arms only within one run. Bases, briefs and host load differ between runs.

## Results

Newest first. Each run names its item, base and brief so it can be repeated.

<!-- results -->

### 2026-11-16 — #1584, a planner fix (one item: a data point, not a rule)

`count(*)` beside `count(DISTINCT x)` over zero rows answered NULL; PostgreSQL answers 0.
Base `2bcd3bb4`. One brief, three arms, each its own no-landing stretch. Verification on
`claude-haiku-5-5` and review on `claude-opus-5-5` in every arm.

| Builder | Rounds to CLEAN | Required changes | Build | Verify | Review | Total | Tokens | Wall |
|---|---|---|---|---|---|---|---|---|
| `claude-opus-5-5` | 1 | 0 | $2.03 | $0.01 | $0.72 | **$2.76** | 252k | 14.6 min |
| `claude-sonnet-5-5` | 1 | 0 | $0.66 | $0.01 | $1.53 | **$2.20** | 231k | 28.1 min |
| `claude-haiku-5-5` | 1 | 0 | $0.06 | $0.01 | $1.25 | **$1.32** | 275k | 25.3 min |

Dollars are API-rate equivalents computed from each agent's transcript usage (input, output,
cache read, cache write). Input, output and Opus/Sonnet cache-read rates are published;
cache-write (1.25x input) and the Haiku cache-read rate are assumptions. Tokens are the
workflow's reported subagent total, which does not count cache reads.

What it shows:

- All three builders found the same cause (the two-level DISTINCT rewrite re-sums COUNT
  partials, and an ungrouped SUM over zero rows is NULL) and made the same fix (a
  `coalesce(..., 0)` in the finalizing projection, ungrouped only), each with a regression
  test that fails at base. The Opus arm also updated the internals doc; the others did not.
- With every arm clean in one round, the cheaper builders were cheaper overall: Sonnet 80% and
  Haiku 48% of the Opus arm's total.
- **Review is the floor.** On the Sonnet and Haiku arms the Opus review cost more than the
  build (2.3x and 21x). One extra round on either arm, another build plus another review,
  would have erased the saving against Opus.
- Review cost varied by 2x across arms ($0.72 to $1.53) for the same fix. Wall time on the
  two slower arms was mostly the reviewer's `./wadjet/` test run while two other arms were
  using the host, so wall time here is not comparable across arms.

What it does not show: anything about items that are not this easy. Every arm converged in
one round, so this run could not measure the thing the question is about, the cost of a
cheaper builder needing an extra round. That needs harder items.

Process defect found: the first Sonnet attempt ($0.48, 4.5 min) reached the same fix and
stopped on escalation trigger 5, because an existing test pinned the plan shape the fix
changes and the brief said any failed test is a stop. The trigger was narrowed (`CREW.md`)
and the run repeated.
