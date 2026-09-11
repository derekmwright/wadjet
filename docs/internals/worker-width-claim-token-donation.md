# Worker width claim token donation

Source: internal/worker/width_gate.go — func (g *widthGate) claim(ctx context.Context, fed bool) (widthSlot, error) {, moved 2026-09-11 (#1026)

claim blocks until the consumer may process a morsel, preferring the
fragment baseline, then a free pool token, then parking FIFO for either.
The caller holds an admitted morsel; see cpuTokens.enqueueWaiter for why
parking here cannot deadlock.

fed is the morsel ring's occupancy behind the morsel in hand
(len(dispenser.ch) > 0). It rides into the FIFO because the pool's
admission policy turns on it: a fed waiter keeps strict priority over
decode-ahead (the original rule, in the regime it was written for), a dry
one does not — feeding a consumer whose ring is empty cannot buy
throughput until a decoder refills it (cpu_tokens.go).

Claim-path donation (§2.3): a granted waiter whose own fragment's scanner
is at that moment parked token-stalled cedes its grant to the scanner and
re-enqueues, at most once per claim — the second grant always sticks, so
per-morsel delay is bounded at one extra FIFO wait. This is the only
producer admission path in the deep-starvation mode, where every consumer
is a slot-less waiter and the §2.2 yield-path precondition (a HELD token
going dry) never occurs. The redirected token never re-enters the pool:
grant → tryDonateToken → chunk decode → worker release, the same bounded
ownership chain as a yield donation.
