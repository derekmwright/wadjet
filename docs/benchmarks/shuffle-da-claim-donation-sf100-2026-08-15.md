# Claim-path donation: SF100 window — engaged at scale, q08 warm slow-mode absent, deep trio now stage-walk-bound (2026-08-15)

**Verdict: KEEP a8a227b (claim-path donation stays default).** Zero
cost (88/88 rows identical; the only value delta is Q19's last vsig
digit, which flips across PRIOR windows too — see §Correctness; zero
reaps, zero watchdog firings, zero panics), engagement proven at scale
(50.4k claim donations on top of 94.0k yield donations — total donated
144.5k vs 96.6k in the prior window, +50%), and this is the first
window with NO warm-run q08 slow mode (17.7–18.1s in all three warm
runs). But the §2.3 target — the three substantial warm join-6 tasks —
did not move: still eff ~6.5 / dry ~7.1 at ~7.8s, with zero donations
of either path on those tasks. The counters re-attribute that residual:
the producer there is no longer token-starved but **stage-walk-bound**
(stage_ms 30–34s/worker > decode_ms ~24s), so there is no token stall
to donate into. The §4 extent skip-walk staging is confirmed as the
next lever.

Window `results/20260815-204851`, bin `a8a227b`, standard 4-run
config, ON-DEMAND (spot-default terraform bug fixed this window,
783b432 — the first cluster launched spot, was destroyed pre-suite and
redeployed clean), suite 20:49–21:01Z, torn down before analysis, EC2
zero verified. Single-arm window — cross-window wall comparisons are
context, not A/B claims.

## Walls

| run | suite | q08 | q09 |
|---|---|---|---|
| 1 (cold) | 3m31.1s | 25.6s | 27.9s |
| 2 | 2m52.2s | 17.9s | **28.9s** |
| 3 | 2m43.4s | 18.1s | 18.7s |
| 4 | **2m40.6s** | 17.7s | **16.6s** |

- **q08 warm slow-mode absent**: every prior window carried at least
  one warm q08 run at ~25s (175927 R3 25.4; 163743 R4-flip; 154200
  R3-flip). Here all three warm runs sit in the fast mode. q09 16.6s
  (R4) is the best observed on this config.
- **The q08↔q09 run-level correlation broke**: R2 carries the
  straggler mode in q09 alone (28.9s; two join-6 tasks at eff 3.3–3.5
  / dry ~10.5, 20:54:16–18Z) while q08 in the same run stays fast
  (17.9s). In 175927 the slow runs dragged both queries together.
- Suite R4 160.6s / R3 163.4s are in the record cluster (records
  158.6/160.2/161.7); no new record, but also no ~190s+ slow suite —
  the worst warm run is 172.2s.

## Counters (4-run cumulative, final per-worker samples)

| marker | w0 | w1 | w2 |
|---|---|---|---|
| donated | 44,200 | 47,934 | 52,336 |
| token_stall_ms | 54,170 | 42,992 | 40,494 |
| pressure_stall_ms | 0 | 0 | 0 |
| stage_ms / decode_ms | 34.2s / 24.4s | 30.1s / 23.7s | 33.0s / 23.8s |

Donations by stage (fragment-side, all workers, yield / claim):
join-4 73,085 / **39,934**; join-13 12,527 / 4,961; join-10 6,765 /
4,530; **join-6 1,633 / 994**; join-2 30 / 11.

## Correctness

88/88 rows identical to 175927. vsig identical on 20 of 21
vsig-carrying queries (Q20 emits no vsig in any window — pre-existing
format gap). Q19's single vsig column differs in the 10th significant
digit (`5.985878904e8` vs `…903e8`), stable within each window (4/4
runs agree) but **already flipping across prior windows on unchanged
binaries' comparisons**: 154200 = 904, 163743 = 903, 123209 = 904.
It is float-summation reassociation noise under width-timing changes,
not a data defect; not attributable to this change.

## The mechanism finding

Claim-path donation fires exactly where its precondition exists:
fragments whose consumers cycle through slot-less claim waits while
their scanner token-stalls — join-4 class, 39.9k donations, and total
donation volume up 50% with token_stall_ms roughly flat (donations
resolve stalls faster than the pool would; the stall class is being
served, not eliminated).

The §2.3 deep-starvation target — the warm join-6 trio (three ~7.8s
tasks per warm run, eff ~6.5, dry ~7.1) — shows zero donations of
EITHER path on those tasks, and the reason is now measurable: with
pressure at 0 and per-worker stage_ms exceeding decode_ms, those
fragments' scanners are not parked in token stalls at all — they are
walking the chunk stream (readChunkBytes staging, the serial floor the
design memo §4 predicted would surface next). A donation mechanism
cannot help a producer that is I/O/memcpy-bound on one goroutine.
The residual lever is **extent skip-walk staging** (§4: stage headers
only, bulk pread by workers), not further admission work.

Straggler mode (eff ≈ 3 / dry ≈ 10–11) persisted in R1 (cold, 2×) and
R2 (2×, the q09 28.9s run) — same signature as 175927, now decoupled
from q08. Separate attribution if pursued; not window-timing lore.

## Window log

Preflight caught zero orphans; first apply launched SPOT workers
(terraform eff_spot fallback defaulted true when no profile loaded —
fixed in 783b432, spot now explicit opt-in), destroyed, redeployed
on-demand, verified (InstanceLifecycle empty on all four). Startup
verified at T+5min (scan/join/exchange stages completing on all three
workers). Suite 20:49–21:01Z; coordinator auto-stopped post-upload
(instance_initiated_shutdown_behavior=stop, by design). Completion
monitor missed the upload because the results-dir stamp (20:48:51,
cloud-init benchmark start) PREDATES the tofu-apply-return stamp
(20:49:52) — gotcha addendum: compare results prefixes against
instance LAUNCH time, not apply completion. Destroyed before analysis;
post-destroy describe-instances: all terminated, none stopped.
