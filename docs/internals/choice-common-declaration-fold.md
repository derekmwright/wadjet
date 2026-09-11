# Choice common declaration fold

Source: internal/engine/expr/rettype.go — func CommonDeclType(decided []DeclType, sawUnknown bool) (DeclType, bool) {, moved 2026-09-11 (#1026)

CommonDeclType answers a polymorphic declaration from the argument types
that DECIDED one. It is the shared rule for every construct that CHOOSES
BETWEEN operands — COALESCE/NULLIF/IFNULL/IF/GREATEST/LEAST here, and
CASE's branches in the physical planner, which calls this so the two can
never disagree.

ok=false means DECLINE: the caller must answer as if nothing had decided,
which is what it did before a DECIMAL operand could decide anything.

The NUMERIC deciders fold through PostgreSQL's select_common_type ladder —
INT32 → INT64 → DECIMAL → FLOAT32 → FLOAT64 — and not through "the first
decider wins", which is what this did until #724. The difference is a VALUE,
not an OID: `GREATEST(bigint, real, double)` is double precision in
PostgreSQL, and declaring it bigint from argument 0 does not narrow the
double the call produces, it WRAPS it — 1e39 stored into an int64 vector is
int64's MINIMUM, #462's failure mode. The ladder is verified live on
postgres:17-alpine for every ordered pair of the six numeric widths and is
the same one setOpWiden pins for set operations and joinFoldKinds runs over
the compiled tree.

A DECIMAL is not a type on its own: COALESCE over DECIMAL(9,2) and
DECIMAL(18,4) has to answer a type that holds BOTH, or the narrower
declaration truncates the wider argument's digits on the way into the output
vector. So when the ladder lands on DECIMAL, every decider's fixed-point
contribution is folded through batch.DecimalCommon — the same rule a set
operation reconciles its arms with (ADR-0024 item 2).

A QUOTED literal contributes NO rung. PostgreSQL types one `unknown` and
resolves it from the other operands, which is exactly what DeclType.Quoted
says here; a composite whose every argument is quoted is `text` there and
answers TypeString here.

sawUnknown is the safety clause and it is not optional. A branch that
decided nothing still PRODUCES a value at runtime — a scalar subquery, a
container element, anything this layer cannot type — and a DECIMAL one
arrives as text at ITS OWN scale, not at the fold's. Folding only the
branches that spoke declared DECIMAL(9,2) for
`COALESCE(a, (SELECT MAX(b) FROM t))`, which then TRUNCATED the subquery's
12.7501 to 12.75 and, at the comparison sites, left the operand
unclassifiable so the extremum was picked by BYTE order. A declined fold
answers exactly what it answered before ADR-0024 — a loud mismatch or the
STRING fallback — which is the only honest answer while the operand has no
declaration to fold in.

A DECIMAL beside an INTEGER — a column, or a numeric literal — resolves to
numeric in PostgreSQL, and does here (#695, verified live on 17.11:
`pg_typeof(CASE WHEN true THEN 1.5::numeric(15,2) ELSE 0 END)` is numeric,
and so are COALESCE/GREATEST/LEAST/NULLIF over the same pair). The integer
contributes its fixed-point form to the fold — its whole range at scale 0
for a COLUMN, its own spelling for a LITERAL (DeclType.Exact) — and the
value materializes through the exact-TEXT box every DECIMAL producer here
answers with, never as the already-scaled carrier an integer box means to
SetValue (ADR-0018 §4). That was the deferral this function carried until
#695: `GREATEST(dec_col, 100)` declared INT64, answered 100 on every row the
integer won, and failed at the #361 store guard on the first row the decimal
won — data-dependent, which is why it could not stand.

A DECIMAL beside a FLOAT is the float, which is PostgreSQL's rule (both
float types are preferred in the numeric category, and only float8 beats
float4) — in EITHER argument order now. `COALESCE(numeric, real)` answered
real before #724 and `COALESCE(real, numeric)` answered real too, but
`GREATEST(numeric(15,2), c_i64, real)` answered bigint, because the first
non-DECIMAL decider was the bigint. The rows the DECIMAL arm wins hand over
that branch's TEXT, which the float vector then has to read: choice_decimal.go
does that at the box, so the declaration and the value agree (#555's float
half).
