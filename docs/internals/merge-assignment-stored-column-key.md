# Merge assignment stored column key

Source: wadjet/dml.go — row[target.Name] = val, moved 2026-09-11 (#1026)

Under the SCHEMA's spelling, which is what `target` carries — not
under the reference's. `row` is a copy of `readMergeTarget`'s
`batch.RecordBatch.RowAt`, keyed by the catalog schema, while `col`
is the name the statement wrote, and an unquoted identifier reaches
a SET list FOLDED (#731) where a parquet-born schema keeps
`UserAgent`. Writing `row[col]` added a SECOND key `useragent`
beside an untouched `UserAgent`; the ingester's byte-exact
`row[col.Name]` then re-wrote the OLD value while the delete marker
and the replacement row were committed anyway. Measured over
`hits(WatchID, counterid, UserAgent)`:

	MERGE INTO hits USING (SELECT 1 AS k) s ON hits.WatchID = s.k
	  WHEN MATCHED THEN UPDATE SET useragent = 'MERGED'
	before: MERGE 1, table [1 10 old-1] [2 20 old-2] [3 30 old-3]
	after:  MERGE 1, table [1 10 MERGED] [2 20 old-2] [3 30 old-3]

The COUNT was the lie: the MATCHED branch ran, the statement
reported one affected row, and nothing changed. This is
ResolveDMLSetClauses' fix one door over — UPDATE already carries
`col.Name`, and the two statements have to agree about what one
assignment does.
