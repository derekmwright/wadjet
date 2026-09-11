# Sql merge by source feature boundary

Source: internal/planner/sql/parser.go — if l.peekToken().typ == TokenKWBy {, moved 2026-09-11 (#1026)

PostgreSQL 17's WHEN NOT MATCHED BY SOURCE / BY TARGET. Both are
real clause kinds with different meanings (BY SOURCE walks the
TARGET rows no source row matched), so reading past the BY and
treating the clause as an ordinary NOT MATCHED would act on the
wrong rows. It is an unimplemented FEATURE, not bad SQL, so it is
0A000 and it refuses (#686 R2-3, wadjet#718).

DEFERRED — #718. Re-examined by arc D3 against PostgreSQL 17.11 and
deferred again: the MERGE builder does not make it cheap, because
its clause SCOPE is a boolean by construction and BY SOURCE needs a
third value. Three changes, and the third is the one that is not
local:

 1. The parser needs a CLAUSE-KIND field. MergeWhenClause carries
    only `Matched bool`, which cannot express the difference
    between NOT MATCHED, NOT MATCHED BY SOURCE and NOT MATCHED BY
    TARGET. BY TARGET is a synonym for plain NOT MATCHED and maps
    onto the existing branch.

 2. The executor needs a SECOND set beside matchedTargetIndices.
    That one records the targets a clause FIRED on, and BY SOURCE's
    complement is "no source row matched this target AT ALL",
    fired or not. PostgreSQL confirms the distinction is real:
    `WHEN MATCHED AND t.n > 99 THEN DELETE WHEN NOT MATCHED BY
    SOURCE THEN UPDATE SET n = 0` leaves the matched row ALONE even
    though its MATCHED clause did not fire (measured, MERGE 2).

 3. A BY SOURCE clause's scope is TARGET-ONLY, and that is a THIRD
    scope. `wadjet.mergeEvaluator` threads a `matched bool` through
    resolveRefIn, checkClauseColumns, checkConditionType, condition
    and value, where false means SOURCE-only and true means the
    merged namespace. Under BY SOURCE, `UPDATE SET n = s.n` is
    42P01 "invalid reference to FROM-clause entry for table s" and
    a BARE `n` resolves to the TARGET without ambiguity (both
    measured). Every one of those signatures changes.

PostgreSQL also answers `WHEN NOT MATCHED BY SOURCE THEN INSERT`
and `WHEN NOT MATCHED BY TARGET THEN DELETE` with a SYNTAX error
(42601) rather than a feature refusal — the action sets differ per
clause kind.

This is a FEATURE, not a wrong answer. It is pinned three ways: the
ELEVEN #718 rows in the DML census, each carrying PostgreSQL 17's
own answer beside the 0A000 so the implementing arc measures
nothing; TestMergeNotMatchedBySourceIsReportedAsUnsupported; and the
limitation bullet in docs/sql-reference.md.
