# Worker source backing recycle contract

Source: internal/worker/morsel_dispenser.go — type batchRecycler interface {, moved 2026-09-11 (#1026)

batchRecycler is implemented by fragment sources that OWN the storage of
the batches they emit and can hand it to a later decode once the consumer
is done with it — today, the parquet scan source's row-group backing pool
(scan.BackingPool, docs/design/scan-output-backing-reuse.md).

RecycleBatch is the RELEASE half of that pool's ownership rule and the
dispenser's retire edge is the only place that can supply it: retire fires
once every zero-copy view minted over the parent has been retired, which is
after the whole op chain AND the sink consume — strictly later than
ChainDriver's ReleaseInputs edge, which is what makes it safe for the
late-materialization views that read the parent's columns through
Vector.Base after Execute returned. The CLAIM half (Detach) is checked
inside the pool, not here: this call is "I am done", never "nobody kept
it".

The mint stamp is the batch's identity at the moment the consumer took
delivery of it (batch.MintStamp). It is captured then, not at release time,
so a retire that somehow fires twice around a re-mint names the OLD
generation and the pool refuses it — a stale release must never re-admit a
live backing.

armBackingReuse is the other half of the contract: a source only builds a
pool when a consumer with a release edge asks for the hook. Consumers
without one (the shuffle task's plain Next loop, planner.StreamingSources,
the hash-join build source, the post-breaker phases) never call
batchRecyclerOf, so those sources allocate no pool at all rather than one
that could never take anything back.
