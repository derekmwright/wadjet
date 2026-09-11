# Parquet map key carrier text

Source: internal/storage/parquet/reader.go — func MapKeyCarrierText(typeID TypeID, decScale int32, k any) string {, moved 2026-09-11 (#1026)

MapKeyCarrierText renders a decoded map-KEY leaf CARRIER into a canonical,
PARSEABLE text — the string a Go map's key must be, from which the key child
(batch.Vector.SetValue) or a re-write (decomposeMap) reconstructs the exact
value.

The nested leaf decode hands back CARRIERS, not display values
(StorageClassOf's classes: IPv4/MAC as int64, IPv6/UUID as raw 16-byte
slices, DECIMAL as the unscaled integer, DATE as the day count). fmt.Sprint
of those is a lossy carrier print — "3232235786", "[10 0 0 5]", "127500" —
that the key child cannot re-parse, so EVERY family whose carrier is not
already its own text (issue #883's title) was corrupted or lost on the round
trip: DECIMAL re-scaled, DATE/IPv4/MAC/IPv6/UUID lost to a zero value. A map
VALUE is unaffected because it stays the typed box and SetValue reads it
directly; only the key is forced through text because a Go map's key must be
a string.

The final user-visible key is re-rendered by GetValue from the RECONSTRUCTED
carrier, so this text need only PARSE to the right carrier — it is not the
display spelling. IPv6 is emitted as the uncompressed eight-group form so it
round-trips to the exact sixteen bytes (net.ParseIP of a v4-mapped display
form would not), and GetValue re-compresses it on the way out.
