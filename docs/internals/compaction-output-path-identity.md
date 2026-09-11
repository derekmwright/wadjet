# Compaction output path identity

Source: internal/storage/compaction/compactor.go — func partitionedOutputPath(tableName, partPath, base string) string {, moved 2026-09-11 (#1026)

partitionedOutputPath builds "<base>_<uuidv7>.parquet" under the
partition's directory. Some writers store the partition path already
table-prefixed (the harness datagen primes "tables/<name>/"); blindly
joining prefix+partPath then yields "tables/orders/tables/orders//compacted_*"
— consistent (write, manifest, and read all use it) but wrong. A prefixed
partPath is treated as the full base.

The suffix used to be a nanosecond timestamp (see #494): the only thing
separating two output paths in the same partition directory, and
RewriteTable emits them back to back — one per memory-bounded group —
where compaction emitted at most one per pass, and ForceCompactFile's
delete-marker rewrites run from independent workers entirely. A repeated
value is not a name clash the store reports: the second Put OVERWRITES the
first, and the first group's manifest entry then points at the second
group's bytes, so those rows are gone with no error anywhere. Wall-clock
resolution — worse, a process-local monotonic counter racing OTHER
processes' clocks — is not a property to bet that on. A UUIDv7 carries
enough random bits to make a collision astronomically unlikely across
every writer in the cluster, and its leading 48-bit millisecond timestamp
keeps outputs roughly sorted by creation order, same as the counter did
within one process.
