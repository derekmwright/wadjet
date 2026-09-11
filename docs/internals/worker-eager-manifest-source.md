# Worker eager manifest source

Source: internal/worker/manifest_stream_source.go — const StaleInputAttemptMarker = "eager-dispatch stale input attempt", moved 2026-09-11 (#1026)

manifestStreamSource is the eager-consumer input source
(docs/design/eager-consumer-dispatch.md §3.2): it consumes one producer
stage's shuffle files as each producer TASK completes, instead of a
frozen file list built after the whole stage drained.

Contract:
  - The candidate set (spec.ProducerTaskIDs) is fixed at task build;
    which files exist, and where, streams in as ProducerTaskManifests
    (Replay for tasks completed before dispatch, NATS for the rest —
    subscribe happens in Init, before any wait, so nothing is missed;
    duplicates are idempotent).
  - Files outside [PartitionStart, PartitionEnd] are ignored.
  - Reads go through the standard tiered fetch (LocalStageCache → peer
    → S3) by delegating each resolved manifest's in-range files to an
    inner cachedFileStreamSource; manifest PeerAddr hints are
    registered so the peer tier works for files the task spec could
    not have hinted.
  - Attempt fencing (§5): consuming any file of producer task T pins
    T's attempt. A later manifest for T with a higher attempt poisons
    the source; Next returns errStaleInputAttempt and the task fails
    loudly (coordinator retries it against the stable attempt set).

StaleInputAttemptMarker tags the poison error so the coordinator's
result classification can retry the consumer task without burning the
generic failure path's diagnostics.
