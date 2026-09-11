# Wshf cursor cold error path

Source: internal/wshf/cursor.go — func (c *Cursor) takeErr(n int, what string) error {, moved 2026-09-11 (#1026)

takeErr builds the error for Take/Peek's failure branch: a negative n is
a corrupt length field, never a rewind; anything else means fewer than n
bytes remain.

noinline is load-bearing, not cosmetic, and so is having exactly ONE
call site for it in each of Take/Peek (this used to be two: a
fmt.Errorf built directly in each function's own body for the negative
case, plus a call to a separate short() for the other). Every read in
this package funnels through Take, so its own compiled body is the one
piece of code that runs once per field of every chunk of every WSHF
payload decoded — keeping the rare, string-heavy error formatting out
of it, in one shared function instead of duplicated across Take AND
Peek, is a real reduction in what that hot body carries.

It does not, despite that, make Take or Peek inlinable at their own
call sites: the compiler charges a fixed extraCallCost (57,
cmd/compile/internal/inline.inlineExtraCallCost) against a function's
own inlining budget (80) for every call it makes to something the
compiler won't inline, REGARDLESS of which runtime branch is actually
taken — the inliner's cost model is a static syntactic estimate, not a
hot/cold-path-weighted one. Take's own logic (the bounds check, the
slice, the pointer advance) already costs ~34 on its own
(`go build -gcflags="-m -m" ./internal/wshf/` with the noinline call
swapped for a plain `io.EOF` return shows this), and 34+57 still clears
the 80 budget; Peek fares only slightly better. Before this change each
carried a SECOND such call (or a fully inlined fmt.Errorf, worse
still), so the prior costs were 192 (Take) and 183 (Peek) against the
current 94 and 85 — smaller, but not under the line. Confirmed no
regression either way: BenchmarkWSHFDecodeHot (internal/worker) shows
no statistically significant change (benchstat, n=10 interleaved,
p=0.579) — expected, since nothing outside this file's own compiled
functions changes shape.

go:noinline
