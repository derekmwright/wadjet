# Update set target identity

Source: wadjet/dml.go — name := strings.TrimSpace(sc.Column), moved 2026-09-11 (#1026)

A QUALIFIED target (`SET t.n = 1`) never arrives here: the UPDATE
parser requires `=` after the column name and refuses the dot.
PostgreSQL refuses it too, reading the qualifier as a column of the
relation and raising 42703 where this raises 42601; both refuse,
and the statement writes nothing either way. MERGE spells its own
qualified targets and strips them in applySetClauses.
The reference is FOLDED and the schema is not: an unquoted
identifier lower-cases at the lexer (#731) while a catalog schema
keeps the spelling the parquet file gave it, and CamelCase column
names are ordinary there. `byName` already concedes that on the
LOOKUP — but the assignment carried the FOLDED name forward, and
the row it writes into is `batch.RecordBatch.RowAt`, keyed by the
SCHEMA's spelling. So `SET UserAgent = 'x'` added a second key
`useragent` beside the untouched `UserAgent`, the writer's
byte-exact `row[col.Name]` read the OLD value, and the statement
reported `UPDATE 1` having changed nothing. Carry the schema's
spelling, the way the statistics path does (#881).

And RESOLVE, rather than lowercase-and-look-up. The two halves of
the rule are separable only at the reference: `sc.Column` comes
from the UPDATE parser, so an unquoted name is already folded and a
name still carrying an upper-case letter can only have been
DELIMITED. A map keyed by the fold answers both the same, so
`SET "USERAGENT" = 'X'` WROTE to `UserAgent` — the write landed,
`UPDATE 1`, the value changed — where PostgreSQL raises 42703 for a
delimited name that is not the column's own bytes. The refusal is
the one a genuinely absent column already gets, same class and same
wording, and `SET useragent = 'X'` still resolves.
