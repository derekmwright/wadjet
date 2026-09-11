# Heap pressure drain response

Source: internal/engine/exec/pressure_drain.go — PressureDrainer / TryPressureDrain, moved 2026-09-11 (#1026)

Drain-instead-of-sleep heap-backpressure response (#326).

The heap-backpressure valve reacts to GLOBAL heap pressure by sleeping
whoever pulls the next batch — which, in a breaker-phase pipeline, is the
loop feeding the very operator whose LIVE state caused the pressure.
Sleeping the holder of live state reclaims nothing: no amount of GC
removes a hash aggregate's group table or a sort's buffered runs. The
measured #325/#326 run parked 23,153 × 50 ms = 1,158 s that way while the
state it was "waiting out" only grew.

The response the memory system exists to make is memory-for-I/O: a
breaker under pressure should DRAIN (spill its state) rather than sleep.
PressureDrainer connects the valve to the breakers' existing spill paths,
with two guards carried over from ADR-0006 machinery:

  - Dominance: only an operator holding the dominant share of TRACKED
    bytes — pressure it plausibly caused — answers the valve. A breaker
    holding little (a scan-side pipeline where the pressure is transient
    decode garbage) still sleeps, because there the 50 ms GC catch-up is
    exactly right (Q17 SF100, 2026-05-07).
  - Productivity: the HashAggregate route runs through the #325 drain
    gate, so the valve cannot re-open the drain-per-batch livelock. When
    the gate refuses (not enough new state since the last drain) the
    valve neither drains nor sleeps — the bytes are live, so the sleep
    would have been pure parked wall time.

WADJET_PRESSURE_DRAIN=0 is the kill switch: it restores the
sleep-on-pressure behavior everywhere.
