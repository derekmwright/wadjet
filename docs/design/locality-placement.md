# Input-locality task placement (`--locality-placement`)

Status: implemented 2026-07-24. Default off pending SF100 validation.

## Motivation

The shuffle-io ledger (PR #261) split SF100 exchange reads into 237.7 GB
same-worker mmap vs 351.9 GB peer gRPC streams per suite pair. Decomposing
that peer share by plan structure:

- **Repartition exchanges** (`partition=NNNN` fan-in): every producer task
  hash-partitions its slice of the input, so each producer contributes
  ~uniformly to every partition. Wherever a consumer runs, ~(N−1)/N of its
  partition's bytes were produced elsewhere. This share is irreducible
  data movement — no placement can shrink it.
- **1:1 stage chains** (consumer stage with the same `distribution_count`
  as its dependency — the `join-8 → join-10` shape, 24→24): consumer task
  *i* reads exactly producer task *i*'s output. Today placement is
  memory-binpack/round-robin, so ~1/N of these land co-located by chance;
  the rest stream over the NIC for no structural reason.
- Single-producer outputs (replicated builds, small stages) sit between:
  one producer, one or many consumers.

Placement can convert the second and (partially) third classes to local
mmaps. Q18 — the #1 wall query — is dominated by exactly the 1:1
`join-8`/`join-10` class edges (~37 GB/run on that one edge).

## Mechanism

`Scheduler.pickWorkerFor` gains a locality tier between the eager-consumer
reservation and memory binpack:

1. The streaming-exchange annotator already stamps every dispatched task
   with `InputLocations` (input key → producer's peer address) before the
   scheduler picks a worker — the locality signal costs nothing new.
2. `pickLocalityWorkerFrom`: if every hinted input resolves to ONE address,
   map it back to a connected worker (registry `PeerAddr`) and place the
   task there.
3. **Same-batch cap**: at most `ceil(batchLen / connectedWorkers)` tasks of
   one fan-out per worker. 1:1 chains inherit their producer stage's
   uniform spread, so the cap never bites them; batches whose tasks all
   hint at one worker (replicated build files) get exactly their fair
   share placed locally and the rest fall through — preserving the
   anti-clump property the binpack spread established (2026-07-20 Q20
   diagnosis: a stacked fan-out cost +24s serialization).

Preference, not correctness: hints are best-effort, and wherever the task
lands its reads fall back through local/KV/peer/S3 tiers unchanged. Tasks
with hints on multiple workers (repartition consumers) and tasks with no
hints fall through to binpack/round-robin exactly as before.

Ranked above binpack deliberately: for a 1:1 chain the input bytes already
live on the hinted worker, so reading locally is also the memory-cheapest
option; worker-side memory admission remains the backstop.

## Scope and interactions

- Requires `--streaming-exchange` (hint source) and the gRPC data plane
  (targeted dispatch; the NATS path is worker-pull and places nothing).
  `coordinator.New` only arms the tier when both configs allow.
- Skew-split sub-tasks divide a hot group's probe files across producers →
  hints span workers → fall through. Unaffected.
- Retries re-annotate with current hints and re-pick; a dead hinted worker
  is not connected → fall through.
- Shuffle-durability lazy/off: independent knobs; locality reduces peer
  streams, durability reduces uploads. Combined arm is the target state.

## Observability

`published tasks` placement counts gain a `local:N` method alongside
`eager`/`binpack`/`rr`. The #261 ledger's local-vs-peer byte split is the
decision metric: local share should rise on 1:1-heavy queries.

## Validation

Unit (pure `pickLocalityWorkerFrom` core), coordinator suite + `-race`,
TPC-H SF0.01, tpch-harness local SF0.01+SF1 both arms, then SF100
same-window pair (baseline, then `WADJET_LOCALITY_PLACEMENT=1`, ideally on
top of `WADJET_SHUFFLE_DURABILITY=lazy` once that flips). Success axes:
peer_bytes share falls, Q18 steady improves, rows 44/44, placement
`local:N` visible on 1:1 chains, no fan-out clumping (spread stays
uniform on dispatch lines).
