# Parquet read path coercion set

Source: internal/storage/parquet/reader.go — func CoercibleTo(file, want TypeID) bool {, moved 2026-09-11 (#1026)

CoercibleTo reports whether values decoded as the type the FILE stores can
be converted, after decode, to the type the CATALOG declares.

This set is the contract between the two read paths. Which one runs is
decided by the SHAPE of the schema — a table one column of ARRAY/MAP away
from the row reader sends every query on it down that path (#393) — not by
the query, so a pairing one path converts and the other refuses is a
two-path divergence waiting for a schema change to expose it. The native
scan implements exactly this set in copyNativeCoercedDirect /
copyNativeCoercedScatter; readColumnToAny implements it here.

Anything outside the set stays an error on both paths.

LOSSLESS WIDENING is in the set, and is the one class here that cannot
change a value: every INT32 is an INT64 and every FLOAT32 is a FLOAT64,
exactly. It was left out on the reasoning that a widening drift is
indistinguishable from catalog/file drift — true, and the wrong conclusion,
because it is a drift whose repair is exact. #428 made compaction read
through this gate, so refusing it stopped the partition compacting AT ALL
(#440): every pass failed the merge, the failure was a log line, and the
partition accumulated small files forever. Before that the compactor read
the file's own types and the writer widened on the way out, which is to say
the system already performed this coercion — just without anything vetting
it.

The NARROWING pairings are a different matter and stay for their own
reasons: INT64→INT32 truncates and INT64→FLOAT64 loses precision past 2^53,
and both are admitted because a file that predates a narrowing catalog
change is otherwise unreadable. They are not evidence that any conversion
belongs here — and #439 is the proof, from the other direction:

TypeInt32 → TypeString was admitted here until then. A bare INT32 leaf
carries no evidence its values are day counts, only a leaf the file itself
ANNOTATED as DATE does. Admitting it rendered arbitrary integers as ISO
dates (100 became "1970-04-11") on any table where a plain INT32 column
landed under a catalog STRING column, and once compaction started taking
this same coercion (#428) it wrote the fabricated dates over the inputs.
