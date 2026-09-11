# Worker prefetch panic delivery

Source: internal/worker/scan_prefetch.go — defer exec.CatchQueryPanic(ctx, "scan file prefetch", func(err error) {, moved 2026-09-11 (#1026)

fetch reaches decode and decompression paths on a goroutine nobody
joins for errors. Unrecovered, a panic there ends the worker process;
recovered but undelivered, the consumer waits forever on an empty
result slot. Deliver it as that file's error (#511). One defer per
prefetch goroutine, not per file.

The in-flight index is not the only obligation this goroutine owes.
jobs is pre-filled with EVERY index and closed, and the pool is the
only thing draining it: a panicking worker that resolves just its own
index leaves the rest queued, and once all scanPrefetchConcurrency
workers have died that way nothing will ever fill those slots — take()
blocks forever on a result nobody owns. So the boundary takes
ownership of what is left and fails it with the same error.

It deliberately does NOT cancel: p.cancel reaches only this
prefetcher's child context, while take() waits on the CALLER's, so
cancelling would make live siblings abandon indices they had already
received and reintroduce the hang from the other side. Draining is
race-free instead — every index is delivered by channel receive, so
exactly one goroutine owns it, whether that is a healthy sibling
mid-fetch or this drain loop.
