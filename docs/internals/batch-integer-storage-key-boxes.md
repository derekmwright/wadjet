# Batch integer storage key boxes

Source: internal/engine/batch/vector.go — func KeyStorageInt(v any, t TypeID) (int64, bool) {, moved 2026-09-11 (#1026)

KeyStorageInt is the inverse of GetValue's boxing for the types
IntStorageType names: it answers the int64 a column of type t STORES for
the boxed value v, whichever of that type's legal boxes v happens to be.

It exists because one value of such a type has more than one box in this
engine and they are not interchangeable as bytes. GetValue FORMATS the
three whose storage is not their text — DATE to "2006-01-02", IPv4 to its
dotted quad, MAC to its colon form — while the aggregate's int-keyed SoA
path, its migration to the generic map (migrateToGenericMap) and the
packed-key unpacking all hand back the raw integer. A group key that
serializes whichever box it is given therefore has TWO identities for one
value, which is #788: a k-way merge compares bytes, so "14610" and
"\n2010-01-01" never combined and every DATE group came out twice with the
right total and the wrong grouping.

The answer is a function of (t, value) and of nothing else, so it lives
beside GetValue and SetValue — the two boxings it has to agree with — and
its parses are literally theirs (parseDateString, net.ParseIP, net.ParseMAC).

ok is false for a box that no column of type t can produce (a string that
is not a date/address, a float where an integer is stored). The caller
decides what an impossible box means; this never guesses an integer for it,
because a wrong integer is a wrong GROUP.
