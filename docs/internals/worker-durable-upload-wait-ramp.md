# Worker durable upload wait ramp

Source: internal/worker/peer_exchange.go — var durableWaitPollMin = 25 * time.Millisecond, moved 2026-09-11 (#1026)

durableWaitPollMin is the FIRST re-poll delay, and the base the ramp
doubles from. Derivation, not a tuning knob:

The wait is for an upload the producer has already FINALIZED locally —
entering the wait publishes SubjectUploadRelease, which makes the
producer's queued or foreground-yielded job urgent (upload_manager.go).
The consumer therefore cannot learn anything faster than the rate at
which the producer's own upload state can change, and that rate is
uploadSlotPollMs = 50ms: the interval at which a released job re-checks
for an admission slot. 25ms is half of it — the coarsest cadence that
still observes every state the producer can enter between two polls.
Anything finer only spends S3 GETs on a state that cannot have moved.

What the old flat 500ms cost: it quantized every wait to a multiple of
itself. SF100 window 2 (docs/benchmarks/sf100-window2-analysis-2026-08-22.md
§7.1) measured the 4-row gather-merge tasks completing at
0.74 / 1.25 / 1.75 / 2.25 / 2.77 / 3.34 / 3.83 / 4.30 / 4.81 s — a
500ms grid sitting on the critical path of every aggregate query, worth
12–14.5 s per suite run. The ramp keeps the same 15s budget, the same
ceiling and the same MissingInputKey failure semantics, and pays at most
one interval of overshoot: ~25ms for a copy that is already landing,
500ms only once the wait is already seconds long.
