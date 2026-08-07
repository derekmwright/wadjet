# Dimension bloom cascades

Status: shipped. 2-hop 2026-08-06; N-hop fixpoint + generalized target
2026-08-07; exchange-fed fact gate + alias provenance guard 2026-08-07
(late). Kill switch `WADJET_DIMENSION_CASCADE=0`.

## The idea

Snowflake filters reach facts transitively: region σ(name) restricts
nation, which restricts supplier, which restricts lineitem — but no
predicate ever touches the fact's columns, so static pushdown can't help.
The cascade wires the transitive semijoin reduction as a chain of
dynamic-filter hops: each dimension emits a bloom over its join key
(AtOutput — observing POST-consume rows, so each hop's key set narrows
through the previous hop's bloom), and the next stage consumes it at
row/row-group level. Mids keep WAIT stat-deps — the barrier protects both
the emitted bloom's tightness AND the mid's own output volume (see
attach-on-arrival-dynamic-filters.md §Guarded re-emit for the SF100
measurement that proved the second half); only the chain TIP (the fact
consume) is attach-eligible, riding incremental partial publication.

Eligibility is structural: inner-form legs only (a probe row failing the
leg produces nothing, so drop-only holds), single-column integer keys,
emitters capped at `cascadeMaxEmitterRows` (2M — emitters bypass the
dispatch semaphore and ride the priority lane, whose memory rationale
assumes dimension-class scans), blooms clamped to the L2-residency ceiling
(512 KB — probe cost is set by residency, not theory).

## N-hop fixpoint (2026-08-07)

The 2-hop matcher required D (the chain head) to carry its own
FilterExprs — Q05's nation is filtered only TRANSITIVELY (through
region's bloom) and never matched; SF100 coord logs showed only Q07/Q21
2-hop shapes. Two generalizations, both structural:

1. **Fixpoint iteration** (≤4 sweeps): a mid that gained a cascade
   consume in an earlier sweep qualifies as the next sweep's D — its
   emitted key set is narrowed by the upstream bloom exactly as a
   predicate would. Chain segments share edges: segment 1's hop-B
   (nation→supplier) IS segment 2's hop-A, so the hop-A dedup is a
   REUSE, not a skip; the hop-B dedup (target already consumes from this
   mid) terminates the fixpoint.
2. **Generalized bloom target**: the leg's probe column needn't live on
   the probe root. In build-heavy plans (Q05: orders is the probe root;
   lineitem arrives as a shuffle BUILD) the supplier leg's probe column
   (l_suppkey) lives on another leg's build scan. The target is the
   UNIQUE leaf scan among {probe root} ∪ {leg build scans} owning the
   column — ambiguity rejects (a wrong target falsely rejects rows). The
   target must be dispatchable (filters / fused exchange / pass-through
   feeding an exchange, whose tasks receive forwarded specs) — OR
   dimension-class, because an intermediate chain scan only becomes
   dispatchable when the NEXT sweep makes it an emitter; until then its
   consume is a harmless no-op.

Q05's marking (SF10-local 2026-08-07, mirrored by the fixture test):

```
sweep 1: dim=region  mid=nation   target=supplier   (r_regionkey → n_nationkey)
sweep 2: dim=nation  mid=supplier target=LINEITEM   (hop_a_reused=true, s_suppkey → l_suppkey)
```

lineitem — 1200 files, the expensive scan, a shuffle build — receives the
supplier bloom (~20% pass rate for region='ASIA') on l_suppkey. The same
sweep also discovered Q07's customer-side chain
(nation→customer→orders) at scales where customer fits the emitter cap.

## Exchange-fed fact gate (2026-08-07 late)

The "orders is the probe root" premise above is the SF10 shape. At SF100
the join order FLIPS: lineitem is the UNFILTERED probe root feeding
exchange-repartition as a shuffle side, orders is the build — and the
matcher's original fact gate (`len(f.FilterExprs) == 0 → continue`)
rejected every Q05 join before leg enumeration, silently (the reject
logging sat after the gate). The fixpoint shipped SF100-inert; both
2026-08-07 SF100 arms marked only the Q07/Q21 2-hop shapes
(coordinator-log ground truth, results/20260807-{213833,215900}).

Fix: the fact-as-target obeys the SAME dispatchability rule as any
generalized target — pushed filters, a fused exchange, or an exchange-fed
pass-through (the consume forwards into the exchange's tasks, where the
row-level BloomFilterOp applies it; runtime proven at SF10 on the
identical lineitem-exchange shape). With the gate fixed, the SF100 sweep
marks Q02 (region→nation→supplier), Q05 (both segments,
region→nation→supplier→lineitem), and Q08 (region→n1→customer), leaving
Q07/Q21 untouched.

## Alias provenance guard (2026-08-07 late)

Widening the gate exposed a latent wrong-results hazard in hop-A
pairing: B0 "owning" D's probed column was tested by BARE COLUMN NAME.
Q08 scans nation twice with identical column sets — n1 (customer's
nation, region-filtered) and n2 (supplier's nation, feeding the
market-share CASE, which must see EVERY nation). Both own `n_regionkey`
by name, so the region leg paired with the n2 leg and would have
region-filtered n2's build — silently dropping every
non-AMERICA-supplier row from Q08's denominator.

The guard: when more than one candidate scan owns the hop-A column, the
alias qualifiers on the join keys are the only surviving provenance —
D's probe key ("n1.n_regionkey") must name the same instance as B0's
build key ("n1.n_nationkey"); unqualified or mismatched contests REJECT.
Same discipline as `uniqueColumnOwner` on hop-B targets: a bloom on the
wrong instance is a correctness bug, not a missed optimization.

## What the chain costs

Serialization of the WAIT mids: region scan + merge + nation scan +
merge + supplier scan + merge before the tip's bloom exists — tiny scans
on the priority lane, each merge one round of S3 GET/PUT. The tip's
consume is attach-mode where its stage shape allows (dispatched
fragments) and WAIT otherwise (pass-through forwarding), so the fact
scan is never start-blocked in the attach case and pays the chain
latency only as head-of-scan coverage loss.

## Observability

- `dimension_cascade: marked` with dim/mid/target/fact, hop columns, and
  `hop_a_reused` (chain segments).
- `dimension_cascade: debug no-match` with per-leg reject reasons.
- `DimensionCascadesPlanned` counter (one per segment).
