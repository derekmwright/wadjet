# q08 join-6 "exchange-sink residual": attribution verdict (2026-08-15)

**Verdict: the sink was never the lever.** Three same-day SF100 windows
plus in-sink phase counters bound the entire join-6 sink cost at **~2s of
q08 wall**; the remaining q08 residual worth chasing is the
**effective-width plateau** (~10 of 15 admitted cores on the 20–24s probe
stage — worth ~5–7s), exactly as the width-growcap memo's second bullet
suggested. This memo closes the sink line with the data that killed it.

## What was tried (all kept — correct, race-hardened, no regressions)

| commit | change | join-6 total sink_ms |
|---|---|---|
| (baseline, window 121238) | — | 624.8s / 24 tasks |
| `e50fd1b` (window 123209) | consumer-local slab pre-accumulation (~100× fewer partition-lock acquires) | 611.7s (−2%) |
| `f47a6e8` (window 125500) | accumulator flush encode moved outside `pw.mu` (ping-pong swap) | 646.0s (unchanged) |
| `c24e28f` (window 131741) | per-phase counters in the partitioned sink | attribution below |

## What the counters said (window 131741, 2 runs)

1. **The partitioned sink is innocent everywhere.** Across the whole
   suite its in-consume buckets are small: e.g. the biggest repartition
   consumer (join-10) totals ~27s across 144 tasks; q08's join-6 shows
   only ~9.5s in-sink across 48 tasks — **~3% of the sink_ms the fragment
   phases attribute to join-6**.
2. **q08's join-6 doesn't even use the partitioned sink.** Its tasks are
   broadcast probe-split with unpartitioned per-task output —
   `fragmentUnpartitionedSink` → `unpartitionedStageSink`. The
   width-growcap memo's "exchange sink: per-consume partition hashing +
   per-partition locks" mechanism never applied to this stage.
3. **sink_ms is cumulative consumer-seconds, not wall.** `fp.sinkNs`
   sums time-in-Consume across all ~16 concurrent morsel consumers.
   join-6's ~28s/task over a ~20s stage is ~10% of consumer time —
   an upper bound of ~2s wall even if Consume became free.

## Bookkeeping corrections for future readers

- The width-growcap memo's "~13 surviving rows/consume × ~100K consumes
  → per-consume hashing + per-partition locks" description conflated the
  partitioned repartition sinks (where consumes ARE tiny but total cost
  is small) with join-6's unpartitioned sink (where sink_ms is larger
  but is mostly a small fraction of consumer time).
- When reading `sink_ms` (and `ops_ms`) from "fragment task phases":
  divide by the effective consumer width before comparing to wall.

## What remains for q08 (ranked)

1. **Effective-width plateau** — ~10 of 15 admitted cores during join-6
   (`ops_ms` 123–158s over 20–24s elapsed). width_wait/yield telemetry
   would say whether the CPU-token pool or the morsel dispenser paces
   it. This is the 5–7s lever.
2. Sink micro-residual (~2s ceiling): the unpartitioned sink's coalesce
   path takes one sink-wide mutex per consume; if it ever matters, the
   partitioned sink's slab pattern (`e50fd1b`) ports directly. Not worth
   a window on its own.

## Window log (all zero reaps, all rows exact, EC2 zero after each)

- 121238 (bin 6fa0237): run 4 = **164.5s** (record at the time)
- 123209 (bin e50fd1b): run 4 = **160.2s** (current record)
- 125500 (bin f47a6e8): run 4 = 165.1s
- 131741 (bin c24e28f, 2 runs): 212.9 / 182.4s
