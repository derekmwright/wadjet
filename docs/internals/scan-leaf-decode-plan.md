# Scan leaf decode plan

Source: internal/engine/scan/columnar_native.go — func columnDecodePlan(leaves []*pqt.SchemaNode, colIdx int, catalogType pqt.TypeID) (, moved 2026-09-11 (#1026)
Superseded: The live compatibility boundary is CoercibleTo, not a fixed historical count of three pairings.

columnDecodePlan resolves everything readColumnNative needs to know about
one leaf before it starts reading pages: the type the FILE recovers for
it, its maximum definition level, and whether the values need converting
on the way into the catalog's vector — refusing the pairings where they
cannot get there at all.

The copy paths switch on the FILE's type while writing into a vector
allocated for the CATALOG's, so the two must agree on which typed array
the values land in; storageClass answers that. A pairing that agrees is
copied verbatim, the three CoercibleTo pairings are converted, and nothing
else is decodable — the ones that used to reach the copy anyway did not
fail, they indexed the wrong array and panicked.

It is one non-inlined function, and that is load-bearing rather than
tidy. readColumnNative's frame sits on the stack of every per-column
errgroup goroutine, and those start at the runtime's minimum: doing this
work inline grew the frame past what the initial stack holds, so EVERY
column read paid an extra runtime.newstack + copystack. That measured as
+7% on BenchmarkReadColumnar/rows=1000 with runtime.newstack going from
4.9% to 8.8% of a GOMAXPROCS=1 profile. Check the frame with
`go build -gcflags=-S | grep readColumnNative STEXT` — it must stay at or
under the 0x268 it was before this guard existed.

go:noinline
