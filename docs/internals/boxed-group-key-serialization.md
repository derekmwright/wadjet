# Boxed group key serialization

Source: internal/engine/exec/aggregate_partial_drain_cursor.go — appendSerializedKey, moved 2026-09-11 (#1026)

appendSerializedKey writes the same byte sequence as serializeKey directly
into buf, returning the extended buf. The two share one definition of the
on-the-wire format; callers track the pre-call length to recover offset
boundaries.

types carries each value's declared GROUP BY column type, one per vals
entry, so a CIDR value re-keys into PostgreSQL's inet order
(kernel.CidrOrderKey) instead of its raw stored text (#520).
appendKeyValue's boxed `any` has no type tag of its own — a CIDR value
boxes as a plain Go string, indistinguishable there from a STRING
column's — so the re-key has to happen here, where the caller still has
groupColTypes in hand. Without it, this spill/merge key would disagree
with the in-memory hash key appendColumnValue (aggregate.go) already
builds, silently splitting '10.0.0.1' and '10.0.0.1/32' back into two
groups across a spill boundary the un-spilled path already calls one
value. types may be shorter than vals (or nil); a value with no
corresponding type serializes as before.

meta is the same GROUP BY columns' full declared metadata, needed for a
CIDR value ONE LEVEL DOWN: an ARRAY, MAP or ROW whose types[i] entry is
batch.TypeArray/TypeMap/TypeRow carries no element type of its own, so a
CIDR leaf below it fell all the way through to appendKeyValue's plain-text
encoding — the same drift one level up that types[i] closes, and the same
failure mode: GROUP BY arr_cidr answers one group in memory
(appendColumnValue → appendNestedElem, which walks the real child vector's
own type) and can answer two once a cross-batch, cross-worker or spill
boundary routes the SAME groups through this boxed path instead. meta may
be shorter than vals (or nil); a value with no corresponding entry, or
whose declared type is not a container, serializes exactly as before via
appendKeyValueWithMeta's own fallback.
