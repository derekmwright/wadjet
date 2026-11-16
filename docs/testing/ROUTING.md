# Routing work to a builder

Which model builds an item is the architect's call (`CREW.md`). This file is how that call is
made today, and how the record is kept so the call can later be made from evidence: a
difficulty score written **before** dispatch, a ledger row written **after**, and a profile
that compares the two.

## Classes

Every item has one class. A profile cell is a class, a difficulty band and a builder.

| Class | What it is |
|---|---|
| `engine-defect` | a wrong answer in expressions, operators or the single-process executor |
| `planner-defect` | a wrong answer or false refusal from the logical or physical planner |
| `distributed-defect` | the single-process path is right and a stage-DAG arm differs |
| `pgwire` | wire types, parameters, Describe |
| `storage` | parquet, catalog, object store, ingest |
| `mechanical` | one known edit repeated across sites; a deletion; adding a table entry |
| `docs` | documentation only |
| `test` | a gate or fixture, no product change |

## Difficulty score

Six dimensions, each 0 to 2, scored from the issue and the code before the brief is written.

| | 0 | 1 | 2 |
|---|---|---|---|
| **locate** | the brief names the file or function | it names the subsystem | the cause is unknown |
| **spread** | one file | two or three files, or one package | several packages, or many sites |
| **reach** | code only this feature runs | code a family of statements shares | a shared primitive, a pipeline breaker's spill/drain/merge path, a planner seam, the parquet package |
| **oracle** | PostgreSQL's answer is stated | it has to be measured | a position has to be decided (that is an ADR, not an item) |
| **side** | the fix cannot change another statement | sibling statements share the path | the fix changes a rule many statements go through |
| **gate** | package tests | a `-short` suite | a heavy gate: the coordinator package, an oracle arm |

`D` is the sum, 0 to 12. Bands: 0-2, 3-4, 5-6, 7+.

After the item closes, the ledger records what the score missed: a `side` scored 0 that turned
out to be 1 is the most useful row there is.

## The rule in force (conservative)

Until a profile cell has at least three rows, route by this, not by the profile:

1. `reach` = 2 or `side` = 2: the strong model builds. No exceptions.
2. `docs`, or `mechanical` with `D` of 2 or less: the fast model.
3. `D` of 4 or less with `reach` and `side` both at most 1: the mid-tier model.
4. Everything else: the strong model.

The strong model reviews every build, and the reviewer measures statements the change
reaches, whoever built (`tools/crew/dispatch-stretch.js`).

A cell graduates from the rule to the profile when it has three or more rows for a builder,
none of them `clean-wrong`, and its mean total cost beats the next tier up.

## The ledger

`tools/crew/ledger.jsonl`, one row per item per attempt, benchmark arms and real work alike.
The architect writes the row when the stretch returns.

| Field | Meaning |
|---|---|
| `kind` | `benchmark` or `work` |
| `class`, `difficulty`, `scored` | as above; `scored` is `before` or `after` (an after-the-fact score is weaker evidence) |
| `builder`, `reviewer` | exact model ids |
| `outcome` | `clean`; `clean-wrong` (review passed something it should not have); `stopped`; `abandoned` |
| `rounds` | build rounds to that outcome |
| `escalations` | each with its trigger and its cause: `correct`, `brief` (the brief or a trigger was wrong), `harness`, `own-deviation` |
| `side_effects_missed` | statements the change altered that the builder did not report |
| `review_escapes` | defects the review passed |
| `brief_deviations` | instructions in the brief the builder did not follow |
| `cost` | API-rate dollars for `build`, `verify`, `review`, `total` (`ledger.py cost <transcript dir>`) |
| `wall_min`, `base`, `commit`, `landed` | the rest of the record |

`tools/crew/ledger.py report` prints the profile: per cell, the count, clean-in-one-round,
mean rounds, mean total cost, side effects missed, brief deviations and stops. A cell with
fewer than three rows is an anecdote and the report says so.

## Benchmark set

Open issues chosen to cover the classes and bands the profile is empty in. Each is small
enough to run as a three-arm benchmark (`TIER-BENCHMARK.md`) and real enough to land one arm.
Scores are estimates from the issue text; the brief's author rescoring them against the code
is part of the run. An item leaves this list when it lands.

| Issue | Class | locate/spread/reach/oracle/side/gate | D | What it probes |
|---|---|---|---|---|
| #1147 embedded serve starts NATS on :4222, undocumented | docs | 0/1/0/0/0/0 | 1 | a one-sentence change in three docs |
| #1109 internals map tells only the build side | docs | 1/0/0/0/0/0 | 1 | writing from code to doc accurately |
| #1173 `initcap` is absent | mechanical | 0/1/0/0/0/0 | 1 | adding a registry entry by pattern |
| #1365 four functions with no caller | mechanical | 0/1/0/0/0/1 | 2 | a deletion that must not over-reach |
| #1625 `substring(... FROM pattern FOR escape)` answers NULL | engine-defect | 1/0/0/0/0/0 | 1 | an easy, local defect |
| #1593 bytea LIKE ignores the backslash escape | engine-defect | 1/0/1/0/1/1 | 4 | a sibling-statement side effect |
| #1335 ROLLUP over zero rows answers no row | planner-defect | 1/0/1/0/1/1 | 4 | same shape as #1584, second sample |
| #1480 `GROUP BY ROLLUP (1)` with an ordinal | planner-defect | 2/0/1/0/1/1 | 5 | an unknown cause |
| #1311 nine network/UUID functions declare TEXT | mechanical | 0/1/1/1/1/1 | 5 | a repeated edit that changes wire types |
| #1267 `read_parquet` reads INT96 as raw bytes | storage | 1/1/2/0/1/1 | 6 | the parquet package (strong model by rule) |
| #1209 LIKE's default backslash escape, four matchers | engine-defect | 0/2/2/0/2/1 | 7 | one rule in four places |
| #1288 `approx_distinct` answers 2 on the DAG arms | distributed-defect | 2/1/1/0/1/2 | 7 | a distributed cause with a heavy gate |
| #1350 ORDER BY a name that is both input and output | planner-defect | 1/1/2/0/2/2 | 8 | a resolution rule many statements use |

One calibration item is not an issue: a **seeded review check**. A correct fix with a planted
side effect on a sibling statement is handed to the reviewer as an ordinary build. The review
gate is what makes a cheap build safe to take, so this runs first, and again whenever the
reviewer prompt or the reviewer's model version changes.

## What the profile cannot tell you

- Items in this set are small by construction. A flagship epic's slices are scored and
  ledgered the same way, and they are the only source for the 7+ band at real size.
- Cost is at API rates and wall time depends on what else the host is doing; compare cost
  within a run and wall time not at all.
- A tier's row is about one model version. Rows carry the model id so an old version's rows
  can be excluded, not averaged in.
