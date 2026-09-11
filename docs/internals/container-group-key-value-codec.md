# Container group key value codec

Source: internal/engine/exec/aggregate_container_key.go — container key tags, moved 2026-09-11 (#1026)

Lossless codec for a CONTAINER group-key VALUE in the partial-state spill.

A drained partial aggregate carries two different things per group, and
they are not the same object: the merge KEY (appendSerializedKey, sort.go),
which only has to be injective and order-stable, and the group's VALUE,
which has to come back out of Next() as the row the query selected. For
every flat type the value fits a tagged scalar (partialKeyValue), but an
ARRAY, ROW, MAP or VECTOR has none — so setPartialKeyFromAny's default arm
rendered it with fmt.Sprint and writePartialKeyFallback then handed that
TEXT to a container vector's SetValue, which refuses it (#361's guard,
#566). The same site is #576: a morsel-parallel aggregate hands its clones'
partials to the primary as run FILES (mergeSinkState → drainStateToRuns),
so a VECTOR group key took this path with no memory pressure anywhere.

The merge key cannot be reused as the value. It is deliberately NOT
lossless: kernel.CidrOrderKey rewrites a CIDR leaf into inet order, and
keyFloat32bits/keyFloat64bits fold every NaN payload onto one and -0.0 onto
+0.0, because "compares equal" and "serializes alike" have to name the same
relation there. Decoding a value back out of it would answer a different
value than the un-spilled path does, which is the failure this whole file
exists to prevent — so the value gets its own encoding.

What it encodes is exactly the set of boxed shapes Vector.GetValue produces
(that is where a group key's `any` comes from) and Vector.SetValue accepts
(that is where it goes): nil, bool, int32, int64, float32, float64, string,
[]byte, []any (ARRAY, and a MAP as its list of entry ROWs), map[string]any
(ROW) and []float32 (VECTOR), nested to any depth. batch.Int128 rides along
because a DECIMAL key reaches the same slot from the compact-key path.
Round-tripping through those shapes is what makes the spilled answer
IDENTICAL to the in-memory one: the un-spilled emit path is itself a
GetValue → SetValue round trip (aggregate.go's ext.keyValues loop), so both
paths reconstruct the value the same way from the same box.

Every element is self-delimiting — a kind tag, then a fixed-width or
length-prefixed payload — for the reason appendKeyElem's tag block gives:
there is no separator inside a container, so an untagged element cannot be
told from the next one. Floats keep their RAW bits here (no NaN/-0.0 fold),
because this is the value, not the key.
