# Typematrix nested declared fixture

Source: internal/oracle/typematrix/nested_declared.go — const (, moved 2026-09-11 (#1026)
Superseded: Flat-versus-nested parity can expose an overlay difference but cannot rule out a defect shared by both positions; the anchor is not an independent absolute-value oracle.

The #589 fixture: the parquet-inexpressible types INSIDE containers.

Parquet has no annotation for IPv6 or UUID and spells CIDR as plain UTF8
text, so all three survive a round trip only because the writer stamps the
declared schema into the footer and the reader overlays it back. That
overlay used to stop at the top level, so the identical value inside a ROW,
an ARRAY or a MAP recovered as STRING: the row reader boxed sixteen intact
bytes as a Go string, batch.Vector.SetValue handed the string to
net.ParseIP, and the value read back as the EMPTY STRING. Silently — "" is
indistinguishable from a real empty value.

The main matrix cannot see this. Its container columns (c_arr, c_row,
c_rownest, c_map) carry STRING and INT64 leaves only, which are exactly the
types parquet CAN annotate, so no gate built on it has ever put one of the
nine below a container. This fixture is that gap: the same value written to
a top-level column AND into every container position, so the flat column is
the anchor and any container that disagrees with it has lost the value.

Anchoring on the flat column rather than on a literal is deliberate. A
differential between two engines can agree while BOTH are wrong; a
comparison against the position the overlay always covered cannot.
