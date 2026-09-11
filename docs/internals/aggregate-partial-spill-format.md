# Aggregate partial spill format

Source: internal/engine/exec/aggregate_partial_spill.go — partialSpillMagic, moved 2026-09-11 (#1026)

External-merge spill format for HashAggregate.

Background: the legacy spill path wrote raw INPUT rows under memory pressure
and re-aggregated them all in Finalize. At SF100+ that re-read 100GB of input
to produce a 6GB hash table — pathological. The partial-state path spills
already-aggregated group state (key + accumulators) per spill event, then
Finalize k-way merges the runs, combining accumulators on equal keys.

File format (little-endian):

  magic       "WAGS\x02"             (5 bytes)
  numGroupCols   uint32
  for each group col:
    nameLen     uint16
    name        bytes
    typeID      uint16
  numAggs        uint32
  for each agg:
    func        uint32   (AggFunc)
    isFloat     byte
    isDecimal   byte
    decScale    int32
  numGroups      uint32                (groups in this run, exact)
  for each group, sorted by sortKey:
    rowMarker   byte (0x01)
    sortKeyLen  uint32
    sortKey     bytes
    for each group col:    typed value (tagged like raw-row spill)
    for each agg:           accumulator (variable layout — see emitAcc;
                            a DECIMAL SUM prefixes its Int128 with the
                            overflow flag)
  endMarker     byte (0x00)

The sortKey is the drain cursor's group key (appendSerializedKey /
appendKeyValue, sort.go — NOT appendColumnValue's in-memory hash-table
encoding, which is a different format). Every run's key comes from that
one producer, which is what makes keys equal-comparable across runs; we
sort & merge by it rather than by display values for that reason. It must
be injective, or equal bytes merge two different groups.

The per-column VALUE is a separate thing from that key and cannot be
decoded out of it: the key deliberately folds what the comparator calls
equal (a NaN payload, a -0.0) and re-keys a CIDR into inet order, none of
which is reversible. Flat types carry their value in a tagged scalar; an
ARRAY, ROW, MAP or VECTOR carries a lossless encoded tree under
partialTagContainer (aggregate_container_key.go), which is what lets a
container group key survive the round trip at all (#566, #576).
