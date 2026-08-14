# SF100 3-arm validation: morsel width (0af1ed5) + geometric growth (1388bf8)

Same-window (2026-08-14 afternoon, arms 14:44 / 15:20 / 15:52 UTC,
runs=2 each), standard sf100-distributed profile. Arms:

| arm | bin | results | suite cold | suite steady |
|---|---|---|---|---|
| control | b88159e (pre-both) | 20260814-144438 | 368.7s | 1410.0s* |
| width | d52f260 (≡0af1ed5) | 20260814-152012 | 361.9s | 969.4s* |
| composed | 1388bf8 (width+growth) | 20260814-155202 | **257.3s (−30.2%)** | 415.1s* |

\* Every arm's run 2 hit ambient disturbances (§3) — steady totals are
not clean references. Local artifacts:
`~/wadjet-artifacts/20260814-width-pair/{control,treatment,arm3}/`
(results + wlogs each).

**Correctness: 44/44 rows in all three arms; vsigs identical across
arms (spot set Q01/Q05/Q08/Q09/Q11/Q17/Q18/Q21 all byte-match; rows
identical across all 22).** Control's Q18-run2 completed OK only after
the §3.1 grind.

## 1. Target queries (cold run, same-window)

| query | control | width | composed | vs Trino FTE (steady ref) |
|---|---|---|---|---|
| q08 | 32.3 | 20.0 (−38%) | **17.1 (−47%)** | 14.0 → gap 2.07× → **1.22×** |
| q17 | 17.1 | 26.5 (regressed!) | **16.9**, steady 12.5 | 11.6 → ~**1.08×** steady |
| q18 | 34.7 | 14.9 | **12.0 / 12.4** best ever | 22.1 → wadjet **1.8× faster** |
| q21 | 28.7 | — | **10.5 / 10.7** best ever | 20.4 → wadjet **~2× faster** |
| q11 | 14.9 | 13.2 | 8.8 / **7.0** | 5.2 → 1.35× (dedup lever pending) |

## 2. Mechanism verification (q08 join-6 fragment phases, run 1)

| arm | join-6 wall | task elapsed | ops_ms (cum) | eff. width | sink_ms (cum) |
|---|---|---|---|---|---|
| baseline (morning 121720) | 24.5s | 20–24s | 123–158s | 6–7.5 of 16 | 7.4s |
| width | 16.1s | 14.2–15.5s | 145–152s | **~10** | **35–50s** ← convoy amplified |
| composed | **14.9s** | 13.3–14.2s | 132–144s | ~10 | 24–28s |

The width change alone amplified the O(n²) sink convoy (more consumers
convoying on the exact-size-growth accumulator) — visible as q17's
cold regression (17.1→26.5) and join-6 sink_ms 7→35–50s. The
geometric-growth fix removes it; composed q17 is back to par cold and
best-ever steady, and Q18/Q21 (shuffle-sink accumulators, same append
path) jumped to best-ever by wide margins.

RESIDUAL: join-6's exchange sink still shows 24–28s cumulative sink_ms
(~13 surviving rows/consume × ~100K consumes into the partitioned
shuffle sink: per-consume partition hashing + per-partition locks).
Candidate: per-consumer partition pre-accumulation. Also effective
width plateaus at ~10 of 15 admitted — width_wait/yield telemetry
would say whether tokens or the dispenser pace it.

## 3. Run-2 disturbances — ROOT-CAUSED same evening (config drift, fixed 085a6ce)

**Post-analysis verdict (corpus forensics over all arms' wlogs + dumps
vs the clean morning window):** the "ambient" disturbances were
self-inflicted. All three arms deployed via `-var=profile=` which did
NOT carry five tfvars-only knobs — the arms ran the legacy **NATS data
plane**, **unpaced uploads**, **s2 wire** (not zstd), no locality
placement, no catalog restore. The morning window was a tfvars deploy
(gRPC etc.) on the same binary: zero firings, walls −46%.

Mechanism chain (18 frozen-spin firings, 5 SIGABRT kills, none in the
morning): synchronized query-boundary bursts (shuffle completion waves
+ 24-way unpaced upload fan-outs + scratch purge + next scan wave)
push heaps to 15.8–19.3 GB against the 13.8 GB GOMEMLIMIT (GOGC=off →
GC only runs at the limit) → single GC pauses of 2–12s (worst 30s
window: 14.3s of pause) → watchdog's 4s port-dead threshold fires →
SIGABRT on the >15s tail. The triple simultaneous freeze across all
three control workers at 14:52:18–27 (a query boundary) rules out
per-instance environmental noise. Zero gcAssistAlloc / ReadMemStats /
journald-jam signatures — the closed stall family's mechanisms did NOT
recur; this was config-drift-induced GC-STW stretch.

Kill amplification: a SIGABRT'd worker loses its deferred (lazy
durability) shuffle outputs → consumers hit "no durable copy" → 3
retries exhaust in ~60s → "task failed terminally" → the query then
waits for **NATS JetStream redelivery at ~600s ack-wait** (kill
15:01:10 → redelivery 15:11:06) — that is the 10–12-minute wreck
anatomy (control Q18 11m52, treatment Q03 10m50). ENGINE FOLLOW-UPS
regardless of config: (a) terminal task failure should trigger
immediate stage re-dispatch, not sit out the ack-wait; (b) watchdog
capture tail should record /proc/pressure/* + meminfo + /gc/pauses;
(c) restart-surviving stage outputs (or eager-upload-on-drain before
SIGABRT); (d) the GC-STW burst class at query boundaries is real at
memory-limit heaps even on the right config — worth a boundary-heap
headroom look.

IMPLICATION for §1/§4 numbers: all three arms ran the handicapped
config uniformly, so the cross-arm deltas stand, but absolute walls
are understated vs the canonical config — the clean-window headline
pair (main vs pre-arc, gRPC config) will supersede them.

## 3b. Original same-evening notes (superseded by §3 above)

Two distinct classes wrecked every arm's steady pass in this window
(control worst: q04 2m20, q08 1m40, q18 11m52, q20 2m24):

1. **Streaming-exchange input lost** (control Q18-run2 only): worker
   3edc280a's just-completed join-17 outputs unreachable via BOTH peer
   fetch AND S3 for 11 min — "peer fetch failed and no durable copy
   after 15s" ×≥4 partitions, 3-attempt×15s retry grind, then stage
   retry re-ran the producer and the query finished OK. Worker
   heartbeats stayed alive throughout. Demand-release should have
   force-uploaded within seconds — it did not. Aug-5 breaker-404
   family, new sub-signature. Forensics: results/20260814-144438/wlogs/.
2. **Slow-read class** (all arms): scan stages at ~10× normal wall
   (control q04-run2 scans 134s each, zero errors; arm3 q08-run2 55s /
   q09-run2 3m20 with zero failure lines). Read-path/ENA degradation
   shape. Stall-watchdog fired on one treatment worker (8 dumps,
   15:25–15:42) with exchange_zstd=1 pinned — relevant to the open
   GC-assist frozen-spin lead, which was zstd-vs-s2-linked until now.
   Dumps: results/20260814-152012/wlogs/stall-*.

The morning window (results/20260814-121720, 4 runs, same control
binary) was clean — the disturbances are window/steady-state-linked,
not introduced by either change.

## 4. Verdict

Both changes KEEP (already on main: 0af1ed5, 1388bf8). Composed
same-window cold −30.2%; q08 gap vs Trino 2.07×→1.22×; q17 steady
~1.08×; q18/q21 now beat Trino outright by 1.8–2×. A clean-morning-
window headline pair (main vs pre-arc) is worth running for the record
books; the remaining q08/q11 gap levers are the shared-subplan dedup
(diagnosis memo) and the join-6 exchange-sink residual above.
