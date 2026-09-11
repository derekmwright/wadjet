# Batch mint stamp storage identity

Source: internal/engine/batch/batch.go — type MintStamp struct {, moved 2026-09-11 (#1026)

MintStamp records WHICH producer minted a batch's storage and WHICH issue of
that storage this is. It exists so a producer that hands storage out and
takes it back — today the scan row-group backing pool,
docs/design/scan-output-backing-reuse.md — can recognize its own batch at
the release edge WITHOUT keeping a reference to it.

A registry of outstanding batches keyed by pointer is the obvious
implementation and the wrong one: it is a strong reference the GC cannot
collect and the memory ledger cannot see. A consumer with no release edge,
or a batch the pipeline simply drops, pins whole decoded row groups (~280 MB
each at SF100) for the producer's lifetime, and any bound on the registry's
SIZE silently turns reuse off instead. The stamp inverts the direction: the
batch points at nothing, the producer holds nothing, and identity survives
in a pair of integers the batch carries.

Owner is a process-unique producer id from NewMintOwner. Zero means
unstamped — what a WSHF shuffle chunk, a row-based fallback batch or any
batch from a different producer carries — and a release edge must treat it
as foreign, because adopting it would create a second owner for storage
somebody else recycles.

Seq is bumped on every re-issue of the SAME storage. A release names the Seq
it was handed, so a stale release from a previous generation (a retire that
fired twice around a re-mint) names an older Seq and is ignored: re-admitting
a LIVE backing to the free list would give two decoders one buffer, the one
failure this design must not have.

The stamp is written by the producer while it owns the batch exclusively and
read at the release edge; the producer's own publication edge (the decode
ring's channel, the dispenser's channel send) and the pool mutex order every
access.
