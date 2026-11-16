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
                      items: [{ id: "<issue>-<tier>", builder, title, brief }] } })
   ```

   `builder` is `opus`, `sonnet` or `haiku`. Verification and review are the same for every arm, so the builder is the
   only variable. `land: false`: nothing reaches main.
3. **Keep the arms blind to each other, and prove it.** Arms share one `.git` and one host,
   so nothing physically stops a builder from reading another arm's branch, worktree or note.
   Give each arm its own `notesDir`; remove the branches, worktrees and notes of any earlier
   attempt at the same item before starting; and after the run, audit the builders'
   transcripts:

   ```
   tools/crew/contamination-audit.py <arm>=<transcript dir> <arm>=<transcript dir> ...
   ```

   A builder call that names another arm voids that arm. A builder that was only *shown*
   another arm's artifact in a listing is a leak in the setup: report it and fix the setup.
4. Record per arm, from the stretch's result and the workflow's usage figures: the exact model
   id, rounds to CLEAN (or the escalation and its trigger), total tokens across build, verify
   and review, wall time, and the reviewer's required changes per round.
5. Read the diffs. Two CLEAN arms can differ in where they fixed the defect; note which fix is
   at the cause.
6. Land at most one arm, through a normal stretch. Delete the other arms' worktrees and
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

### 2026-11-16 — #1558, an aggregate-input fix (a harder item; still one data point)

*Landed:* the Opus arm of each run below is on main (#1558 as `58d52152` + `379889c0`, #1584 as
`db7afe99`). The other arms' branches and worktrees were deleted.

`SUM(x op NULL)` answered a sum or 0 on the single-process path; PostgreSQL answers NULL. The
brief did not name the file. Base `538d0d66`. Three arms, verification on `claude-haiku-5-5`,
review on `claude-opus-5-5`. Contamination audit: exit 0 (the Haiku builder was shown the
other arms' branch names in a listing and opened neither).

| Builder | Outcome | Rounds | Total | Wall | Noticed the behaviour change |
|---|---|---|---|---|---|
| `claude-opus-5-5` | CLEAN, after one escalation and the architect's ruling | 2 | **$4.23** | about 40 min | yes: escalated on trigger 3 |
| `claude-sonnet-5-5` | reviewed CLEAN, **wrongly** | 1 | **$3.14** | 45 min | no: "not examined" |
| `claude-haiku-5-5` | stopped before review, fix uncommitted | 1 | **$0.09** | 33 min | no: "not probed" |

All three found the same cause (the aggregate pre-projection discards the bulk float kernel's
"some row is NULL" report that `exec.Project` honours) and wrote the same hunk. What separated
them was everything around the hunk:

- **The same hunk changes two statements the brief called out of scope.** `SUM(t.n / '0')`
  and `SUM(t.i / 'abc')` go from a silent 0 to error 22012 on the single-process path (the
  second where PostgreSQL raises 22P02). Only the Opus builder measured them, and it escalated
  rather than decide. The Sonnet builder shipped the change unexamined and **the Opus
  reviewer passed it**, reading "out of scope" as "need not be looked at". That is an escape
  through review, found only because another arm escalated.
- **The Haiku builder did not follow the brief's test command.** It ran `./wadjet/` without
  `-short`, the package ran past the 10-minute limit, and it stopped on trigger 5 naming the
  test that happened to be running. Run alone at base that test passes in 0.04 s. Its stop
  was correct by the rule; the cause was its own command.
- **An extra round costs a build.** The Opus arm's second round only added two pinned cells
  to a test and cost $1.78, against $2.01 for the first build, because the round re-ran the
  package. Its review was $0.41.
- Cost to the *same end state* is not what the table shows: the Sonnet arm is cheaper only
  because it stopped short. Bringing it to where the Opus arm ended (cells measured, ruled on,
  pinned) needs at least one more round.

Checks added from this run: workers must measure related and out-of-scope statements that
reach the code they changed, and report any that moved; reviewers must do the same and treat
"not examined" there as a required change (`tools/crew/dispatch-stretch.js`). The
architect-upheld resume path was added and fixed during this run (`CREW.md`).

Cost of the harness's own defect: the first resume ran the verifier without the ruling and it
re-raised the escalation ($0.01, 11 s).

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

Contamination audit (run after the fact, exit 0): no builder made a call naming another arm.
The setup leaked once: all arms shared one notes directory, the stopped first attempt's note
(which states the cause and the fix) was still in it, and the Haiku builder listed that
directory and saw the file's name. It did not open it. The three arms reaching the same fix
is therefore not explained by copying, but the arms were not isolated, only honest.

Process defect found: the first Sonnet attempt ($0.48, 4.5 min) reached the same fix and
stopped on escalation trigger 5, because an existing test pinned the plan shape the fix
changes and the brief said any failed test is a stop. The trigger was narrowed (`CREW.md`)
and the run repeated.
