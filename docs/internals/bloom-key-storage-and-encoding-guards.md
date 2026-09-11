# Bloom key storage and encoding guards

Source: internal/engine/exec/bloom_filter_op.go — BloomFilterOp.keyTypesAgree, moved 2026-09-11 (#1026)

keyTypesAgree reports whether the column this op just resolved can be read
the way this op intends to read it.

Two claims are checked, and they ask DIFFERENT questions of the type — which
is the whole subtlety here, because the two predicates differ by exactly
four types and using either one for both claims is a defect.

The first claim is about STORAGE: useIntKey says "index Int32Data or
Int64Data directly", so the question is isIntKeyColumn — the same predicate
that set the flag in the first place (join.go). That admits TIMESTAMP, IPv4,
MAC and DURATION alongside the obvious five, because all four live in
Int64Data and intKeyFromVector reads them correctly. This claim holds even
for a bloom that arrived over the WIRE with no record of what built it: the
DAG's dynamic filters are integer-only by planner gate (columnIntType at
every emit site) and the worker's apply path hardcodes UseIntKey, so nothing
between the two re-checks it against the column that actually shows up.
Pointed at a column with neither slice, that is not a filter answering
wrongly, it is a panic.

The second claim is about ENCODING, and needs a builder: the resolved column
must key the way the inserted column keyed. That question is bloomIntKey,
which admits only the five types appendColumnValue does NOT own — a
TIMESTAMP bloom key goes through appendColumnValue's eight bytes, so a
bloom built on INT64 must refuse a TIMESTAMP column even though both are
Int64Data and both pass the first claim. Widening this one to
isIntKeyColumn would let two incompatible encodings meet.

Disengaging costs a scan filter; guessing costs rows, or the process.
