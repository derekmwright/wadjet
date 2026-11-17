# Documentation Is Part of Done, and Release Housekeeping

Moved out of the root `CLAUDE.md` (arc TK, token-savings reorg, 2026-09-23) to
keep that file's per-turn load small. The root's Code Guidelines section now
just points here; this is the full text, verbatim, unchanged.

### Documentation Is Part of Done

- **User-facing docs move with the code.** An arc that changes a flag, a default, a config key, a type rule, a refusal, a supported-SQL statement or a benchmark number updates `docs/*.md` and the README in the same branch — the way ADRs already ship with the code. The 2026-09-02 drift audit (`docs/testing/docs-drift-audit-2026-09-02.md`) found 130 drifted and 29 unsupported claims across 20 docs after twelve releases that kept only CLAUDE.md and the ADRs current; it is the baseline the next audit diffs against.

### Landing on main

- **No commit that changes Go code reaches main without a gate run on that sha whose HOOK lane is green** (`tooling/gaterun.sh <sha>`; ADR-0046 and its 2026-11-16 amendment). The pre-push hook refuses AI attribution trailers and nothing else; it does not run tests. The gate runner's HOOK lane is the only full race-detector pass there is, so skipping the gate run skips the race detector.
- A docs-only or tooling-only commit on top of a gated sha needs no second run.
- A fully green gate run is pushed, and its issues closed, without asking.
- A red lane is diagnosed, not rerun: the failing test is run alone on the base and on the candidate and the cause is written down. The architect lands without asking only when ALL of these hold, and reports it afterwards: exactly one lane is red and the HOOK lane is green; the failing test passes at least three times out of three alone on both the base and the candidate; the mechanism is identified and does not involve the code being landed; an issue for it is open before the push. Anything else goes to the maintainer.
- A test that reddens a lane this way is hunted, not tolerated: its issue goes on the current patch milestone and is dispatched promptly, and the fix sweeps for siblings of the same kind (an order-sensitive pin on a result nothing sorts, a wall-clock bound, an error whose identity is a race).

### Release Housekeeping

- **Every tag is preceded by a housekeeping pass over the whole bundle.** Arcs merge tag-less; a release batches them into something cohesive, and before `release.sh` runs, one pass over the bundle's diff checks what no single arc can see: (1) every flag, default, config key, type rule, refusal, supported statement or benchmark number the bundle touched is reflected in `docs/*.md` and the README, and `go run ./tools/docscheck .` is green; (2) the doc comment of every function the bundle changed states the CURRENT invariant, boundary and pointers — no stale claim, and the count of comment blocks over twenty lines in touched files does not grow; (3) every settled position has an ADR, new or amended, superseded ADRs say `Status: Superseded by ADR-NNNN`, ADR-0012's divergence catalog (`docs/adr/0012-divergences/`) and the differences page agree row for row (every `kept superset` and `value divergence` row is on the page, every page entry names its row) with the amendment log current, and ADR-0013's nondeterminism classes are current; (4) every issue the bundle closes is closed with a comment and every filing candidate from the landing notes is filed and labeled; (5) the release notes read as a user would read them, one section per arc, no process narration; (6) gofmt, `go vet`, `go mod tidy` are no-ops and no stray binary or orphaned worktree remains. The mechanical checks are `task housekeeping`; the judgement checks are an authored pass reviewed like any other arc (Derek, 2026-09-11).
