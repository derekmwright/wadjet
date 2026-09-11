# Bounded inline integer join probe

Source: internal/engine/exec/join.go — HashJoinProbe.inlineIntProbe, moved 2026-09-11 (#1026)

inlineIntProbe is the fast probe path for single int key inner joins.
It inlines the hash table lookup with typed data access, eliminating
per-row function call overhead from lookupBuild/intProbeKey/intKeyFromVector.
The probe logic is fully inlined (no closure) to avoid heap allocation of
the closure + captured pairs slice, which saves ~2.5GB of allocations at SF1.

It fills at most limit-len(pairs) pairs and returns done=false when it
stopped short; p.res then names the probe row and the chain position to
resume from.

The loops write through a pre-sized window (buf[:limit], n) rather than
appending, so `n >= limit` — the test that suspends the fan-out — is the
same compare append already made against cap. Nothing is added per probe
row either: a resumed chain is drained by resumeIntChain before the typed
loops start, so they still begin at a row boundary. What the chain walk
actually compiles to on amd64 is the arena/arenaNext load pair, the
16-byte store, and three compares — `ref >= 0`, the suspend test, and a
bounds check on buf[n] that the prover does not fold into the suspend test
(it knows n != limit, not n < len(buf)). Measured against the unbounded
version: 1:1 fan-out is unchanged, 1:4 costs ~1.5% best-case, and 1:64 is
~45% faster because pairsBuf stops growing. Callers guarantee
cap(pairs) >= limit.
