# Declared type group merge key

Source: internal/engine/exec/group_key_encoder.go — appendGroupKeyColumn, moved 2026-09-11 (#1026)

ONE merge-key encoder per column TYPE (ADR-0023 item 5, ADR-0026).

A group's merge key is what the k-way merger compares, so it is a
DEFINITION OF EQUALITY: two producers that write different bytes for one
value are two definitions, and the merger keeps their groups apart. ADR-0023
item 5 says a key producer is SHARED and states it for the one consumer
outside this package; #788 is the same rule INSIDE one operator, where a
HashAggregate has four producers of the same key:

  - the int-mode SoA drain and the packed-int drain, which have the key's
    int64 STORAGE (partialGroupCursor.appendIntModeSortKey);
  - the compact and str/generic drains, which have the key as an `any`
    boxed at consume time by (*batch.Vector).GetValue (appendSerializedKey);
  - migrateToGenericMap, which re-boxes an int-keyed group's key as the RAW
    int64 and hands it to that same boxed path;
  - decodeSerializedKey, which re-boxes from the binary hash key.

Those three boxes are not one encoding. GetValue FORMATS the three
int-stored types whose storage is not their text — DATE, IPv4, MAC — so the
int drain wrote `14610` for a DATE while the boxed remainder wrote
`\n2010-01-01`, the merge never combined them, and every DATE group came out
TWICE with the right total and the wrong grouping (#788: 421 groups, 772-841
rows, sum unchanged). The DATE output column's SetValue accepts an integer
and a date string alike, so both rows even rendered the same date.

The encoding is the value's STORAGE, not its display: it is what the hot
int path already writes for INT32/PORT/PROTOCOL and what the coordinator's
own cross-worker re-aggregation already writes for DATE/IPv4/MAC
(coordinator.go's keyEncoders read Int32Data/Int64Data), and ADR-0023 item 1
says a key is keyed on what the comparator compares rather than on how a
value displays. The VALUE a group emits is untouched by all of this — it
stays the boxed round trip of ADR-0023 items 2-3, and nothing decodes a
value out of a key.

Dispatch is on the DECLARED TYPE, never on the Go box: the box is exactly
what disagreed.
