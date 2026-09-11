# Parquet leaf temporal conversion

Source: internal/storage/parquet/file_writer.go — if norm, ok, err := normalizeTemporalBox(col.Type, val); err != nil {, moved 2026-09-11 (#1026)

A DATE text literal is converted HERE too, at the leaf, so a DATE
nested in a ROW/ARRAY/MAP is validated on the same path a top-level
one is (prepareRows only rewrites top-level columns). An unparseable
or nonexistent calendar date used to reach toInt32 -> parseDateForWrite
and store the epoch silently — data corruption inside a container the
top-level guard never saw (#560). ParseDateDays is the one accept-set
and classification the filter path shares.

A time.Time for a DATE column, and a string or a time.Duration for the
other two temporal types, are normalised HERE for the same reason and
by the same rule: every one of them is a box ingest.checkType DECLARES
acceptable, and every one of them used to reach toInt32/toInt64's
default arm and store ZERO — 1970-01-01 for a DATE, 1970-01-01T00:00Z
for a TIMESTAMP, a zero interval for a DURATION — with no error
anywhere. That is how `INSERT INTO t VALUES (1, '2020-01-01')` stored
the epoch while ingest.Ingest with the same text stored the date: the
SQL path boxes a DATE as time.Time and the programmatic one boxes it as
a string, and only the string had a converter (#673).

The accept-set is the boxes checkType admits, so the two boundaries
agree by construction, and what they cannot convert FAILS the write
rather than storing a wrong instant.
