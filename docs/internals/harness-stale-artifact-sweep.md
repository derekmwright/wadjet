# Harness stale artifact sweep

Source: internal/harness/preflight.go — func SweepStaleRunArtifacts(harnessRoot, dataDir string, pruneOlderThan time.Duration, logger *slog.Logger) {, moved 2026-09-11 (#1026)

SweepStaleRunArtifacts removes leftover transient state from prior
harness runs that crashed, timed out, or otherwise didn't reach
the deferred cleanup paths. Called from Run() *before* CheckPreflight
so the disk-space check sees a clean slate.

Two sources of leakage we observe in practice:

  - /tmp/wadjet-harness/run-<unix>/  - per-run logs + spill + JetStream
    store. The harness removes it on success unless WADJET_HARNESS_KEEP=1
    is set, but a panic / external SIGKILL leaves it. Each abandoned
    SF1 run dir is several GB.

  - <dataDir>/wadjet/queries/<query_id>/ - per-query intermediates that
    the coordinator's cleanupQuery now removes on completion (committed
    2542260), but a coordinator killed mid-flight by harness teardown
    never reaches that hook. Each orphan query is ~1 GB at SF1 and
    ~100 GB at SF10.

Safety: the caller must invoke checkNoOrphanedWadjet first OR otherwise
guarantee no concurrent wadjet process is touching these paths. With
pruneOlderThan = 0 the function deletes everything; otherwise only
entries with mtime older than that threshold are removed (use this to
avoid sweeping a sibling harness's just-created run dir).
