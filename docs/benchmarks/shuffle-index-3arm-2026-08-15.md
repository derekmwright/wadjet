# Extent-index 3-arm A/B: walk wins the early-warm regime, ceiling lift rejected, penalty re-attributed to readahead (2026-08-16 night)

**Verdicts.** (1) The index reader costs the EARLY-WARM regime ~5–8s
on q08 and ~12–18s on the steady-suite wall at R2, while deep-warm
runs (R3+) are equal-or-better — the penalty is warmth-dependent, so
the cause is the I/O warm-up pattern, NOT the token economics; the
fix shipped next is scanner-side extent readahead, and the reader
default stays ON pending its confirm window. (2) The decode-worker
ceiling lift (4→8) is REJECTED: the join-6 trio did not move (eff
6.6–7.4, dry ~7 — the FOURTH falsified trio lever), and the extra
workers hammered the pool (token_stall +60%, worst steady suite of
the day, q09 R2 30.2s). (3) Correctness: rows and vsigs byte-identical
across all four same-day windows.

Same-day back-to-back windows (protocol of the 08-14 3-arm), bin
`d2874c5`, 2 runs each, on-demand, each destroyed before the next;
EC2 zero verified at the end. An accidental second control window
(the first Arm-B deploy whose env never reached the workers — see
§Gotchas) is kept as ctl2.

| arm | reader | DA workers | results | R1 (cold) | R2 | q08 R2 | q09 R2 |
|---|---|---|---|---|---|---|---|
| A (control) | index | 4 | 225843 | 206.4s | 176.4s | 23.1s | 18.9s |
| ctl2 (accidental) | index | 4 | 231122 | 202.6s | 178.5s | 26.5s | 25.5s |
| B | **walk** | 4 | 232354 | 224.3s | **164.0s** | **18.3s** | 19.9s |
| C | index | **8** | 233347 | 194.0s | 182.0s | 24.8s | 30.2s |

## Finding 1: the index penalty is early-warm only → readahead

Every index R2 today ran q08 at 23.1–26.5s (A, ctl2, C; plus
yesterday's 222607 R2 at 26.5s). Every WALK R2 on record runs it
17.5–18.3s (arm B; 204851; 175927). But 222607's R3/R4 — index,
deep-warm — ran 17.8/17.2s with suites in the record cluster. A
steady-state cost (token competition) cannot explain a penalty that
vanishes with warmth; a page-cache warm-up difference explains it
exactly: the walk scanner reads staged files SEQUENTIALLY (kernel
readahead + fadviseSequential at full effectiveness), while index
workers issue k=4 interleaved extent preads that defeat readahead on
not-yet-resident files. Once pages are resident (R3+), pread ≡
memcpy and the modes converge — with the index keeping its collapsed
serial floor.

Token data is consistent with this ranking: walk-arm token_stall
(11.4–20.4s) < index arms (16.5–28.7s) < workers=8 (34.0–46.7s), but
the steady-run equality at R3/R4 shows the token delta is not what
moves the wall at k=4.

**Fix (next commit): scanner-side extent readahead.** The index
scanner is now idle by construction and holds every chunk's extent;
it advises POSIX_FADV_WILLNEED over upcoming extents a bounded
distance ahead of the delivery cursor (same posture as
rowgroup-readahead.md, same fadvise infra, GC-safe — advice is a
syscall in the scanner, no faults). Confirm-window judgment: R2 q08
returns to ~18s and R2 suite to the walk arm's ~164s, R3/R4
unchanged; if it does NOT, the fallback decision is flipping the
reader default to walk (writer footer stays — the reader remains
one env flip away).

## Finding 2: ceiling lift rejected; the trio outlives its fourth lever

Arm C (workers=8): trio tasks 7.3–7.9s at eff 6.6–7.4 / dry 6.1–7.0
— indistinguishable from every window since 154200. The trio has now
survived: shuffle decode-ahead, pressure exemption, token donation
(both paths), stage-walk elimination, and doubled decode workers.
Its pacer is none of: scanner, tokens, pressure, decode width,
machine saturation. Remaining candidates for a fresh attribution
pass (log-first, no deploy): the morsel dispenser's parent/split
cadence on those tasks, probe-side chain cost per parent
(process_ms/morsel), and the sink. Meanwhile workers=8 made the pool
measurably worse — the override stays available but default stays 4.

## Gotchas burned (recorded for the next session)

- **/etc/environment does not reach workers.** The first Arm-B deploy
  wrote the env arm there; workers run under `systemd-run --scope`
  and inherit the LAUNCHING SHELL's exports — the terraform
  `extra_env` seam now emits into the user-data export block
  (2660646), and arm validity is verified via `/proc/<pid>/environ`
  at T+5min, which is how the miss was caught.
- **zsh does not word-split unquoted `$var`** — every earlier
  completion monitor's `for p in $out` compared the whole prefix
  list as one string, which is why THREE windows ended in false
  "coordinator stopped, no results" alarms. Monitors now reduce to
  a single `sort | tail -1` word before comparing.

## Window log

Four deploys, all on-demand (spot fix holding), each verified at
T+5min (env in worker process + engagement counters live via SSM),
each destroyed before analysis; final describe-instances: all
terminated. Suites 22:58–23:45Z.
