# Scan row presence definition levels

Source: internal/engine/scan/columnar_native.go — type rowPresence struct {, moved 2026-09-11 (#1026)

rowPresence reconstructs a ROW column's OWN null bitmap from the
definition levels of one of its leaves.

A ROW is a parquet GROUP, and a group has no column chunk of its own: the
only record that a whole group was absent is that its leaves' definition
levels stop short of the group's level. The native reader used to read
only the leaves, so an absent group and a present group whose every field
is null decoded identically — a NULL ROW came back as a present ROW of
nulls (#425). Every consumer that separates "no value" from "a value made
of nothing" then answered wrong: IS NULL, COUNT, an outer join's padding
check, and any copy of the row across a shuffle.

The rule is the format's: with the group's max definition level d, a leaf
definition level >= d means the group was PRESENT (the leaf may still be
null at a higher level), and < d means the group itself was absent.

newRowPresence returns nil — no recording, no cost — when the group cannot
be null (d == 0, every ancestor REQUIRED) or when the leaf sits under a
REPEATED ancestor, where definition levels are per ELEMENT and no longer
line up one-to-one with the row group's rows. A ROW inside an ARRAY or MAP
never reaches this reader anyway (HasUnsupportedColumnarTypes routes those
schemas to the row reader), so the second guard is a backstop.
