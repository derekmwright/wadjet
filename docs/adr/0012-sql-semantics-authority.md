# ADR-0012: PostgreSQL is the SQL semantics authority; DuckDB is the performance goal and an oracle

Status: Accepted (2026-08-19)

## Context

Wadjet ships the PostgreSQL wire protocol, and DuckDB serves as both the
performance bar and the differential correctness oracle. Those two facts
pull in different directions the moment the engines disagree about what a
query *means* rather than how fast it runs.

SQL leaves a surprising amount implementation-defined, and the two engines
resolve it differently. Default NULL placement is the worked example:
PostgreSQL sorts NULLS LAST for ASC and NULLS FIRST for DESC; DuckDB's
`default_null_order` is NULLS LAST in both directions. Neither is wrong.

The question surfaced during the 2026-08-18/19 correctness work and was
escalated for a decision, which exposed the real problem: **the answer was
already implied by a commitment nobody had written down.** CLAUDE.md states
pgwire compatibility is non-negotiable because Superset, psql and JDBC depend
on it; once that holds, semantics follow. ADR-0001 exists because settled
questions were getting re-asked, and this was one.

Escalation is not a neutral cost here. A semantics question presented as an
open preference arrives stripped of the commitment that settles it, so it can
be answered wrongly by someone without that context — and the gates then
*encode* the wrong answer. The stored DuckDB baseline is regenerated from
whatever the oracle says, so a wrong choice becomes self-verifying within
minutes, and the eventual correction shows up red. That is the poisoned-
baseline failure mode (`baseline-local-small.json` held a signature captured
from a broken engine, so a *correct* engine failed our own gate) one level up.

## Decision

1. **PostgreSQL decides semantics.** Where PostgreSQL and DuckDB disagree
   about the meaning of a query — result values, NULL handling, ordering
   placement, type coercion, error-vs-not, catalog introspection — wadjet
   follows PostgreSQL. The wire protocol is a behavioral contract, not just a
   byte format: clients and the tools above them encode PostgreSQL's behavior
   in their query generation.

2. **DuckDB remains the performance goal and a correctness oracle.** It finds
   real bugs (dozens during the 2026-08 correctness arc) and sets the speed
   bar. Neither role makes it the semantic authority.

3. **On a semantic divergence, configure the oracle — do not exempt the
   entry.** An exemption blinds the gate permanently to real bugs in exactly
   the queries most likely to have them; a configured oracle still compares
   every row. The differential gate runs DuckDB with
   `default_null_order='nulls_last_on_asc_first_on_desc'`.

4. **A decision a gate encodes must be asserted where regeneration cannot
   rewrite it.** `TestOracleIsConfiguredForPostgresSemantics` fails if either
   oracle invocation loses that setting, because otherwise deleting one line
   silently changes what "ground truth" means.

5. **Deliberate divergences from PostgreSQL.** (Amended 2026-08-23: collation
   was not the only one — the other three below were never PostgreSQL's call
   to begin with, and are recorded here so a future gate does not mistake
   them for undecided.)

   - **Collation.** Wadjet compares and sorts strings with BINARY collation,
     not PostgreSQL's locale-dependent collation. Locale-sensitive comparison
     costs real work on every string compare and sort, surprises analysts
     more than it helps, and no BI client depends on it. Where the oracle
     needs to agree, use a `C`-collation database rather than exempting
     string ordering.
   - **MIN/MAX over BOOL, UUID, MACADDR, BYTEA and ROW.** (Widened
     2026-08-25, #569: BOOL was the only one recorded, and the rest are the
     same class.) PostgreSQL's `min`/`max` are defined over exactly 22 input
     types, enumerated live from `pg_proc` on postgres:17-alpine:
     `anyarray`, `anyenum`, `bigint`, `character`, `date`, `double
     precision`, `inet`, `integer`, `interval`, `money`, `numeric`, `oid`,
     `pg_lsn`, `real`, `smallint`, `text`, `tid`, `time`/`timetz`,
     `timestamp`/`timestamptz`, `xid8`. `boolean`, `uuid`, `macaddr` and
     `bytea` are NOT among them — each errors with "function min(...) does
     not exist" — and neither is `record`: `min(ROW(…))` errors the same way
     (verified live), so wadjet's MIN/MAX over its ROW type is in this set
     too. All five are EXTENSIONS, not divergences PostgreSQL took a position
     on. `bool_and`/`bool_or` remain available and are still the
     PostgreSQL-idiomatic spelling for the boolean question.

     The consequence for the gates is the part worth writing down: those
     five types cannot be gated against PostgreSQL at all, in any shape —
     grouped, windowed or otherwise. `internal/oracle/typematrix` is their
     differential coverage (wadjet against itself across the stage DAG, the
     kill switches and the pooled/poisoned batch arms), and the four
     PostgreSQL DOES have among wadjet's network types — CIDR, IPV6 and IPV4,
     all of which map onto `inet`, plus DECIMAL onto `numeric` — are gated
     live in `benchmarks/tpch`'s `net_probe`/`dec_probe` fixtures.
   - **MAP and VECTOR ordering.** Both are wadjet-only types — PostgreSQL has
     neither — so their total orders (`internal/engine/exec/kernel/
     container_sort.go`) are wadjet-defined, not a choice against a
     PostgreSQL answer. This bullet is ONLY MAP and VECTOR: ROW is `record`,
     which PostgreSQL HAS as a type but offers no `min`/`max` over (it is in
     the extension set above), and ARRAY maps to PostgreSQL's `anyarray`,
     which DOES have `min`/`max` — so ARRAY is the one container whose
     ordering is a choice measurable against a PostgreSQL answer, even though
     no fixture gates it there today.
   - **MIN/MAX over BYTES.** (Corrected 2026-08-25, #570. The original said
     this matched "PostgreSQL's own `min(bytea)`", which does not exist:
     verified live, `min(bytea)` raises "function min(bytea) does not
     exist", exactly as `min(boolean)` does.) So this is the same kind of
     deliberate extension the BOOL bullet above is, over a type whose order
     wadjet defines anyway — bytewise, which is what every bytea comparison
     uses and what PostgreSQL's own `bytea` operators use. The output type
     still follows the INPUT type and declares bytea (OID 17), which is what
     the extension has to do to be self-consistent; early declared-schema
     code guessed STRING for every MIN/MAX before the input-typed fix.
     `ByteaMinMax` in the wire arm's error list pins it, so the claim is
     checkable rather than remembered.

   - **A window's MIN/MAX declares its input's type too.** (Added
     2026-08-25, #569.) `MIN(c) OVER (…)` and `MIN(c) … GROUP BY g` are the
     same question asked twice and must answer under the same type, so
     `exec.WindowMinMaxType` names every type the engine has, exactly as
     `exec.minMaxOutputType` does for the grouped form. They differ on TWO
     types, INT32 and FLOAT32: the grouped aggregate widens INT32 to INT64
     and FLOAT32 to FLOAT64 because its accumulator is the wider type, while
     the window copies the value and keeps INT32 and FLOAT32 — which are
     PostgreSQL's own answers, `min(integer)` returning `integer` and
     `min(real)` returning `real`.
     Choosing the value in the DECLARED type's order is the other half of the
     same rule; a boxed comparator that reads a rendered address or a
     formatted decimal is not that order (`internal/engine/exec/
     compare_boxed.go`).
   - **A TEXT value compared against a NUMBER.** (Added 2026-08-24, #504.)
     PostgreSQL refuses the pair outright — verified live, `WHERE s = 1.5`
     over a `text` column is 42883 "operator does not exist: text = numeric",
     and so are `>`, `text = bigint`, `CASE s WHEN 1.5`, `s IS DISTINCT FROM
     1.5` and `GREATEST(s, 1.5)`. That is an OVERLOAD RESOLUTION failure, the
     same class as the unary-minus bullet below: wadjet has ONE generic
     comparison operator and no overload set to fail resolution against, so
     reproducing 42883 would mean building the overload machinery first.

     Wadjet instead gives the pair the column's own rule: a STRING column
     compares its BYTES against the literal's SOURCE TEXT, on the vectorized
     path and the row-at-a-time path alike. The literal's text is the carrier
     for the same reason item 6 makes it one — `s = 1.50` and `s = 1.5` are
     different predicates, exactly as `s = '1.50'` and `s = '1.5'` already
     were.

     This is a narrower divergence than it looks. Every answer it produces is
     PostgreSQL's answer to the QUOTED spelling of the same predicate, checked
     entry by entry against live postgres:17-alpine over a `text COLLATE "C"`
     column: `s = '1.5'` 1, `s > '1.5'` 4, `s > '10'` 2, `s < '10'` 2, and the
     three boxed sites likewise. The only thing wadjet does that PostgreSQL
     does not is RESOLVE the unquoted spelling at all.

     What was there before was neither rule. `compare()` read any string
     operand that PARSED as a number numerically — a guess about where the box
     came from, since a DECIMAL column and a STRING column both box as Go
     strings — while the vectorized kernel rendered the numeric constant as
     the EMPTY STRING and compared against that. So `WHERE s = 1.5` found the
     row holding "1.50" through a projected CASE and no rows at all through a
     scan-pushed filter, and `WHERE s > 1.5` admitted every row including
     "1.5" itself. One predicate, two answers, decided by which lowering the
     query happened to take. The guess is gone: `expr.boxedPair` selects the
     rule from the operands' DECLARED types (item 8), and
     `kernel.toString`/`exec.decimalLitValue` give the kernel the same
     literal text the row path uses.

     **This rule is about a NUMERIC literal meeting a TEXT column, and it does
     not run backwards.** (Added 2026-08-25, from the #504 review.) A QUOTED
     literal meeting a NUMBER column is the opposite pair and takes the
     opposite rule: PostgreSQL types an unknown-typed literal FROM the operand
     it meets, so `WHERE k > '2'` over a BIGINT column is the integer
     comparison `k > 2` — not a text comparison, and not the comparison
     against ZERO the constant used to become. Deleting the box-sniffing
     branch removed all three of the readings it was doing at once, and only
     the DECIMAL one was re-stated; `boxedPair` carries a distinct `boxQuoted`
     kind for exactly this reason, so the two directions cannot be collapsed
     again. Verified live on `k BIGINT` 0..11: `k > '2'` 9, `k >= '2'` 10,
     `k < '2'` 2, `k = '2'` 1, and the same for a FLOAT column under the
     float rule.

     Two more of the box-sniff's jobs came back with it. `compare()`'s
     temporal branch guarded itself with "the string parsed OR the number is
     zero", which is true of ANY unparseable string against a zero — so
     `0 = '0.0001'` was TRUE, and once the sniff above it was gone every
     `int_col = 'anything'` matched the row holding zero. It asks the parser
     whether it parsed now (`parseTemporalInt64OK`). And `IN`/`BETWEEN` are
     `=` and `>=`/`<=` chained, so they take the same binding: `s = 2.00` and
     `s IN (2.00)` disagreed until they did.

     `kernel.toString`'s empty string was not only for numbers. A BOOL
     constant reached it too, so `WHERE s = TRUE` compared every row against
     `""` on the scan path and matched the row spelled `""` rather than the
     one spelled `"true"` — a wrong ROW, not a wrong count. It renders
     `true`/`false`, which is PostgreSQL's own `boolean::text` (the
     single-letter `t` is psql's display) and what the row path's `fmt.Sprint`
     already produced.

     **The equivalent question for CIDR is open, and must not be answered one
     site at a time.** (Added 2026-08-25, #546.) A CIDR value is stored as
     TEXT, and every KEY and every column-to-column comparison uses that text
     while the column-to-LITERAL comparison re-keys through
     `kernel.CidrSortKey` (#492) — so `WHERE c = '10.0.0.1'` finds both
     spellings of one address and `WHERE c = d`, `GROUP BY c`, `DISTINCT` and
     every set operation find neither. PostgreSQL says they are one value
     (`inet '10.0.0.1' = inet '10.0.0.1/32'` is TRUE). Both wadjet paths agree
     with each other today, so keying the local set operation by inet ALONE
     would create the divergence it looks like it closes; the fix moves the
     whole key layer and the shuffle router together, the way #459 did for
     floats — predicate kernels, hash keys, and the set-operation key
     (`physical.keyValueText`, `internal/planner/physical/set_op_key.go`) all
     at once, not one at a time. (Amended 2026-08-25. Closed 2026-08-25 — see
     item 10's #546 and #565 residuals for the landed fix and the
     column-to-column comparison it turned out to share a cause with.)

   - **Unary minus over a QUOTED string literal.** (Added 2026-08-24, #505.)
     PostgreSQL refuses EVERY `-'…'` form, numeric-looking or not, with
     42725 "operator is not unique: - unknown" — verified live: both
     `SELECT -'5'` and `SELECT -'abc'` error there, because unary minus
     resolves before the comparison can type the literal from context and
     several overloads match an unknown-typed operand equally well. Wadjet
     has ONE generic unary-minus operator and so has no overload ambiguity
     to report. It folds the literal instead: a numeric-looking string
     becomes a negated `Lit` carrying its exact text (so `d = -'5.00'`
     enters the same exact-DECIMAL path `d = -5.00` does, per item 6), and a
     non-numeric one is refused with 22P02. The visible consequence is that
     `SELECT -'5'` SUCCEEDS here and answers `-5` typed **varchar** — the
     literal keeps its own type, since nothing in that statement asks for a
     number — where PostgreSQL errors and, in the shapes where it does
     resolve (`-'5'::numeric`), answers a numeric. This is a deliberate
     extension of item 6's convention for the column type it exists for, not
     a position against PostgreSQL's overload resolution; reproducing 42725
     would mean building the overload ambiguity first.

   - **The grouping rule has no functional-dependency escape here.** (Added
     2026-08-25, #590.) PostgreSQL refuses a non-aggregated SELECT / HAVING /
     ORDER BY expression that is not one of the grouped expressions — 42803 —
     with one relaxation: a column FUNCTIONALLY DEPENDENT on a grouped
     PRIMARY KEY is allowed, so `SELECT id, name FROM t GROUP BY id` works
     when `id` is the primary key and fails when it is not. Wadjet has no
     primary keys and no unique constraints, so there is nothing for the
     relaxation to apply to and every such reference is refused. This is the
     STRICTER end of PostgreSQL's own rule, not a divergence from it: every
     query wadjet refuses here, a PostgreSQL table without the matching
     primary key refuses too. Should key constraints ever arrive, the
     relaxation arrives with them rather than being invented separately.

   - **HAVING sees the SELECT list's output aliases; PostgreSQL's does not.**
     (Added 2026-08-25, from the #591 corpus work.) PostgreSQL makes an
     output alias visible to GROUP BY and ORDER BY but NOT to HAVING or WHERE
     — verified live, `SELECT k, COUNT(*) AS c FROM t GROUP BY k HAVING c > 1`
     is 42703 "column \"c\" does not exist" there. Wadjet resolves it, because
     its binder puts output names in one scope shared by GROUP BY, HAVING,
     QUALIFY and ORDER BY, and because the spelling is what a user writing
     the query expects to work.

     This one cannot be recorded the way the others are. The PostgreSQL
     oracle's semantics arm FAILS when the oracle refuses a query — an entry
     PostgreSQL cannot answer is not ground truth for anything — so a
     corpus entry is not available to carry it, and the wire arm's error
     list is for statements BOTH engines should refuse. It is gated in
     `internal/planner/physical/validate_grouping_test.go` instead, where the
     question is about wadjet's own rule. Recorded here so a later reading of
     the oracle's silence does not mistake the extension for an oversight.

   - **A JOIN's ON condition can reference comma-join siblings; PostgreSQL rejects this.** (Closed #617.) A join predicate like `SELECT ... FROM a, b JOIN c ON a.k = c.k WHERE ...` references a sibling of the comma join in its ON clause. PostgreSQL 17 rejects this with "invalid reference to FROM-clause entry"; wadjet answers it, matching DuckDB. This is a strict SUPERSET: errors on PostgreSQL, runs on wadjet; not a value divergence and not a wire-protocol violation. Gated against DuckDB and the two-path oracle (PostgreSQL offers no value to assert). #593 fixed the prior silent-zero wrong answer in this shape. The reject-like-PostgreSQL alternative was considered and declined because no client should rely on the error and the planner lacks the ON-scope validation it would require.

6. **A numeric literal's carrier is its TEXT, not a float64.** (Added
   2026-08-23, from #452.) PostgreSQL types an unsuffixed decimal literal as
   `numeric` and compares it at full precision, so `WHERE d = 493827160549382.7160549350`
   must find the row holding that value. A float64 carries ~15-16 significant
   decimal digits and a `DECIMAL(38,10)` carries 38, so the box the compiler
   builds for arithmetic cannot also be the record of which number was
   written: it is a different number by the time it meets the column, and the
   damage is not uniform — the rounded literal landed just BELOW the stored
   value, so `=` matched nothing, `>` gained a row, `<>` gained it back, and
   `>=` and `<` agreed by luck. Four operators agreeing is not partly right.

   Three rules follow. They hold for a bare DECIMAL column compared, matched
   against an IN list, or bounded by BETWEEN, against a numeric literal — the
   vectorized kernel, the row-at-a-time expression, the raw-text predicate,
   and the row-group prune all bind that one shape. (Amended 2026-08-24,
   #465: `CASE d WHEN lit`, `d IS DISTINCT FROM lit` and
   `GREATEST`/`LEAST(d, lit)` now hold them too. Those three compare through
   the BOXED path, where the column is rendered text and the literal is the
   float64 box, so they carry the literal's `Text` into the comparison and
   order the two exact decimals — `expr.boxedPair`'s literal arm and
   `batch.CompareDecimalTexts`. (That carry-through was `expr.compareWithText`
   until #504 replaced it: it applied the exact reading to ANY string box
   against a literal's text, which read a genuine STRING column numerically.
   The rule it applied is unchanged; what selects it is now the operands'
   declared kinds.) An arithmetic-wrapped operand — `d + 0 = lit`
   — still does not: arithmetic over DECIMAL goes through float64 before any
   comparison sees it, which is the separate limit recorded at the end of this
   item.)

   - The literal's source text travels with its box (`expr.Lit.Text`,
     `logical.Predicate.ValueText`, `exec.KernelFilter.LitText`) and is what
     a DECIMAL comparison converts, at the COLUMN's scale.
   - A literal the column's scale cannot hold exactly — `0.255` against a
     `DECIMAL(9,2)` — equals nothing, and still has its place in the ORDER:
     it sits strictly between two representable values, so `> 0.255` excludes
     the row holding `0.25` that `>= 0.25` admits. Truncating the literal
     instead would answer a different question; the residual of the discarded
     digits is carried rather than dropped.
   - A literal the CARRIER cannot hold at that scale — `1e39`, or `10^30`
     against a `DECIMAL(38,10)`, whose unscaled integer needs more than the
     128 bits `Int128` has — keeps its place in the order by SATURATING: it
     compares strictly greater (or strictly less, when negative) than every
     value the column can hold, which is what it is. It never wraps and never
     errors. Narrowing it two's-complement instead put it back INSIDE the
     ordinary range as a plausible number of either sign, so `WHERE d < 1e39`
     — true of every row — selected none of them (#462). The order this
     produces is the order of the exact rationals, saturation included, which
     `batch.TestScaledDecimalOrderIsTransitiveAtTheBoundary` asserts over
     every triple of stored values and constants either side of the ends.
   - One conversion serves the prune and the filter
     (`kernel.StatsDomainValue`, `kernel.decimalLiteralAt`), because a prune
     that reads the predicate differently from the filter deletes rows the
     filter would have kept.
   - **A DECIMAL meeting a value of another type is compared by VALUE, and
     the rule is the other type's.** (Added 2026-08-24, #476/#477.) A DECIMAL
     column boxes as its rendered TEXT, so every boxed comparison against it
     used to fall through to a LEXICOGRAPHIC one, where "9" sorts above "10".
     Against an INTEGER the comparison is exact (`expr.decimalTextOrder`, the
     same `ScaledDecimal` carrier); against a FLOAT it is a float64
     comparison, because PostgreSQL's `numeric <op> double precision` casts
     the numeric; against another DECIMAL it is the unscaled Int128s at their
     two scales (`kernel.CompareDecimalValues`), which no box can be
     dispatched on — two rendered DECIMALs are indistinguishable from two
     strings — so that pair is bound from the column DECLARATIONS, per item 8.

     **Every boxed site is now bound from the declarations, not from one
     call site.** (Amended 2026-08-24, #506.) `expr.bindDecimalCols` used to
     be called from `NewCmp` alone, so a direct `d1 op d2` got the exact
     Int128 comparison while the SAME two columns at a BOXED site — a simple
     `CASE d1 WHEN d2 THEN ...`, `d1 IS DISTINCT FROM d2`,
     `GREATEST(d1, d2)`, and a Cmp whose operand is a COMPOSITE like
     `GREATEST(d1, d2) = d2` — fell through `compare()`'s two-rendered-strings
     path and compared LEXICOGRAPHICALLY, the same defect #477 fixed for the
     direct comparison. Verified against live postgres:17-alpine on the
     `declit` fixture: `CASE d_2 WHEN d_4 THEN 1 ELSE 0 END = 1` answered 0
     against PostgreSQL's 1, `GREATEST(d_2, d_4) = d_4` answered 167 against
     101, `LEAST(d_2, d_4) = d_2` 164 against 98, and
     `d_2 IS DISTINCT FROM d_4` 199 against 198.

     `expr.boxedPair` (`internal/engine/expr/boxed_pair.go`) is the binding.
     It classifies each operand's DECLARED kind — DECIMAL, a non-DECIMAL
     number, genuine text, or unknown — resolving a bare column from the
     batch and a composite (`GREATEST`/`LEAST`, `CASE`, `COALESCE`) from the
     join of the alternatives one of its values comes from, and then applies
     one rule per kind PAIR. The kinds are cached once settled, and a pair
     whose declarations can select no rule disarms itself, so the generic
     path's per-row cost stays one atomic load. Four sites hold one:
     `Cmp`'s generic arm, `Case`'s simple-CASE arm (one per WHEN),
     `IsDistinctFrom`, and `pickExtremum` (one operand per ARGUMENT, since
     the best-so-far moves between iterations). Nothing in it reads a box to
     decide which RULE applies — only to decide which side of the chosen rule
     holds this row's value, which is what item 8's boxed-value rule requires
     generally.
   - **A constant that is not a number is a query ERROR, never a value.**
     (Added 2026-08-24, #463.) The conversion used to answer ZERO for
     anything it could not parse, so `WHERE d = 'abc'` — and `WHERE d = 1e400`,
     which the float64 expansion could not read either — matched every row
     holding zero. PostgreSQL refuses both spellings of "this is not a
     number" with SQLSTATE 22P02, so wadjet raises the same, from the
     vectorized filter (`exec.decimalConstError`) and from the row-at-a-time
     comparison (`expr.raiseInvalidTextRepresentation`) alike: one path
     erroring while the other answers is the two-path defect class. Exponent
     form is not that case — `1e400` IS a number, and is now read as one, by
     folding the exponent into the scaling instead of through
     `strconv.ParseFloat`.
   - **The #463 refusal reaches the #465 boxed sites and a negated string
     literal.** (Added 2026-08-24, #505.) `CASE d WHEN 'abc'`,
     `d IS DISTINCT FROM 'abc'` and `GREATEST`/`LEAST(d, 'abc')` answered
     instead of erroring: the boxed sites' exact-text arm only fires when the
     literal already looks numeric (`Lit.Text` set by `compileLit`), so a
     non-numeric string never reached ANY refusal on these three sites and
     fell through to `compare()`'s ordinary string comparison instead.
     `expr.refuseArm` closes it: whichever operand resolves, in THIS batch,
     to a materialized DECIMAL column is checked against the other operand's
     literal text before the comparison runs, raising the same 22P02
     `decimalLitCmp.order` already raises for the direct-comparison shapes —
     so the refusal depends only on the column's REAL type, never on the
     operand's Go box, the way item 8's boxed-value rule requires generally.
     (#517 lifted the same question to bind time, where it also stops
     depending on a row and on operand order; this runtime half remains for
     the shapes the binder cannot prove.)

     `d = -'abc'` and `d = -'1e400'` were the same failure mode wearing a
     `UnaryOp`: a unary minus over a STRING literal was deliberately left
     unfolded and evaluated through the generic numeric-coercion path,
     which reads anything unparseable as the float64 zero — both matched
     the row holding 0.00, #463's exact failure mode on the one shape #463
     never touched. Compile-time folding (`compileWithCtx`'s `UnaryOp` case,
     `expr/compile.go`) now treats a QUOTED string literal the way an
     UNQUOTED one already was: a numeric-looking string folds into a
     negated `Lit` carrying its exact text — so `d = -'5.00'` enters the
     same `DecimalLiteral` path `d = -5.00` does, saturating past the
     carrier exactly as this item already specifies rather than matching
     the wrong row — and a non-numeric string is refused at COMPILE time.

     (Corrected 2026-08-24. That last sentence used to continue "before any
     row, batch, or short-circuiting conjunct exists to hide it", which
     described the refusal and not the QUERY. The refusal was raised at
     compile time and then SWALLOWED: the physical planner's six compile
     sites fall back to copying an input column when an expression will not
     compile, and only `expr.IsUnknownFunc` was exempt, so `SELECT -'abc'`
     came back as `column "-'abc'" does not exist in the input schema` with
     no SQLSTATE. A refused literal is now its own error TYPE
     (`expr.InvalidLiteralError`, SQLSTATE 22P02) and
     `expr.IsCompileRefusal` names both classes the planner must propagate;
     `TestRefusedLiteralReachesTheClientAsItsOwnError` gates the SELECT,
     WHERE, ORDER BY, GROUP BY and aggregate-argument forms. With that, the
     sentence is true of the fold — and of nothing else.)

     **The refusal is a PLAN-TIME check now, not a per-row one.** (Amended
     2026-08-24, #517.) It used to live inside the comparison, so it
     depended on two things a type rule may not depend on:

     - **A row.** `d = 'abc'` against an EMPTY table, or behind a conjunct
       no row survives to (`k > 100000 AND d IS DISTINCT FROM 'abc'`),
       answered zero rows instead of erroring, on this shape and on the
       original #463 shape alike: the comparison — and therefore the
       refusal — never ran.
     - **Which operand won.** `GREATEST`/`LEAST` compare (best-so-far,
       candidate) pairs, and a pair refused only when a DECIMAL column was on
       one side and the bad literal on the other. Which argument is the
       running best depends on the VALUES, so the SAME three arguments
       refused under one and answered under the other: `GREATEST(k, 'abc',
       d)` raised and `LEAST(k, 'abc', d)` returned a row.

     PostgreSQL resolves an unknown-typed literal's type from the column's
     DECLARATION and refuses at parse/bind time, independent of any row
     existing and of any operand order — verified live for all nine shapes,
     `=`, `<>`, IN, BETWEEN, the three boxed sites, and both extremum
     orders, each with the same 22P02 and the same message wadjet raises.

     `physical.checkLiteralTypes` is the check, in the AST binder
     (`validate.go`) that already resolves every column reference against the
     catalog before a plan exists. `colScope` carries which columns a BASE
     TABLE declares DECIMAL, and the refusal fires only when the column
     PROVABLY resolves to one in a CLOSED scope — the same conservatism the
     rest of the binder is built on, since a false positive breaks a working
     query. The runtime refusals STAY, for the shapes the binder cannot
     prove (an open scope, a derived table or CTE column, an expression it
     does not parse); both call `expr.IsNumericLiteralText`, so the two
     cannot disagree about which strings are numbers.

     **What this does NOT cover, deliberately.** The same silent reading of
     an unparseable constant as the type's ZERO is still live for the
     INTEGER and FLOAT families — `WHERE k = 'abc'` over a BIGINT column
     matches the rows holding 0, where PostgreSQL raises 22P02 "invalid
     input syntax for type bigint". That is #463's failure mode on the
     families #463 never covered, tracked as #536 and pinned in the wire
     corpus, and closing it means carrying the destination TYPE into the
     refusal rather than extending its timing. `'NaN'` is a third case:
     PostgreSQL's numeric HAS a NaN and wadjet's exact carrier does not, so
     `d = 'NaN'` is refused here and answered there (#534).

     **The refusal's cost is a plan-time cost, not a per-row one.**
     (Added 2026-08-24, from the #505 review.) The first version asked both
     of the refusal's questions on EVERY ROW — allocating a
     `kernel.DecimalLiteral` and re-walking the literal's digits to answer
     "is this a number", then re-resolving the column to answer "is this a
     DECIMAL" — which cost +42% on a simple CASE over a DECIMAL column,
     +37% on IS DISTINCT FROM, and +69% with 7x the bytes on an
     exponent-form literal. Both answers are fixed for the query's lifetime,
     which is why `decimalLitCmp.numeric` was already a cached slice rather
     than a per-row `Numeric()` call, and the same discipline now applies at
     the three boxed sites (`expr.refuseArm`, `caseArms`,
     `extremumRefusal`): the literal is judged once when the node is first
     evaluated, and the column's answer is cached the way
     `decimalLitCmp.notDecimal` caches its own. Residual overhead against
     removing the check entirely is inside the noise. A correctness fix that
     re-introduces a documented performance regression has not finished.

   Arithmetic over DECIMAL still goes through float64, and so do MIN/MAX/SUM
   over a DECIMAL column. That is a separate, visible limit — comparison is
   where a rounded value silently changes the ROW SET, which is why it is
   settled here first.

7. **Semantics decisions are technical, not product.** They are made and
   executed, then reported — not escalated. An existing project commitment
   settles everything downstream of it; check for the commitment before
   drafting the question.

8. **Float ordering follows PostgreSQL, not IEEE754, in every ORDER/
   PARTITION/peer/key context; a boxed value's comparison order follows the
   column's declaration, not the box's Go type.** (Added 2026-08-23, #444/
   #446 follow-up.)

   - **Float order.** PostgreSQL's `float8_cmp_internal`/
     `float4_cmp_internal` give FLOAT a total order that IEEE754's own
     comparison operators do not: NaN sorts ABOVE every other value and
     equals itself, and -0.0 equals +0.0. `ORDER BY`, `GROUP BY`'s peer
     grouping, window PARTITION/peer groups, and any key built to represent
     "the same value for merge/comparison purposes" now apply that rule —
     `kernel.CompareFloat64`/`CompareFloat32`
     (`internal/engine/exec/kernel/float_order.go`) is the one place the
     rule is stated, every scalar FLOAT32/FLOAT64 comparator and the
     VECTOR/ARRAY(FLOAT) element comparators are built on it, and the boxed
     k-way MERGE key a spilled aggregate's drain step reifies
     (`appendKeyValue`/`keyFloat32bits`/`keyFloat64bits`,
     `internal/engine/exec/sort.go`) is canonicalized to agree: two values
     the comparator calls equal must also serialize alike, or a query's
     answer depends on how much memory it had (the same failure mode
     `appendKeyValue`'s BYTES/ARRAY/ROW fix addressed for a different type
     class).
   - **A boxed value's order is the column's DECLARATION, not a property of
     its Go box.** `Vector.GetValue` erases declaration order — a ROW boxes
     as `map[string]any`, which has none — so a comparator that dispatches
     on the BOX's own Go type (a `map[string]any`'s keys, sorted
     alphabetically) can disagree with the COLUMNAR comparator, which reads
     the real declared field order. `internal/engine/exec/compare_boxed.go`
     resolves the boxed comparator FROM the declaration (a closure built
     once per column), so both paths order a ROW's fields positionally and a
     DECIMAL numerically, matching PostgreSQL's `record_cmp`. The dynamic
     fallback (`compareAny`, used only when no declaration is available)
     still orders a ROW by field name — no production path reaches it — and
     is not addressed by this decision.
   - **What is now covered, and what is left.** (Updated 2026-08-24, #459's
     close.) The predicate kernels (`=`, `>`, `IN` — `internal/engine/expr/
     expr.go`'s `cmpFloat64Op`/`cmpFloat32Op`, `internal/engine/exec/kernel/
     compare.go`'s `ResolveFilterKernel`), the PRIMARY (non-spilled) GROUP
     BY/DISTINCT hash key (`internal/engine/exec/aggregate.go`'s
     `typedRowHash`/`serializeGroupKey`/`appendColumnValue`), and the
     hash-join key (`internal/engine/exec/join.go`'s `buildKeyFromBatch`/
     `buildProbeKey`) now compare/hash the canonical bits — a `WHERE f = f`
     over a NaN row, and a `GROUP BY`/`DISTINCT`/hash-join over `{-0.0,
     0.0}`, agree with PostgreSQL in the single-process engine. MIN/MAX over
     a NaN column was fixed earlier and separately (`kernel.CompareFloat64`
     in the accumulator loop, #457). The DISTRIBUTED half of the same rule
     closed alongside: `hashRowsIntoPartitions`
     (`internal/worker/partitioned_shuffle_sink.go`) is the shuffle's own
     router, keyed independently of the in-process hash above, and its
     scalar FLOAT32/FLOAT64 arms moved with #459 — its VECTOR arm did not,
     because a VECTOR element's canonicalization lives in a different
     function (`appendVectorKey`, aggregate.go) that #459 did not touch;
     the router disagreeing with that key for one type was the same
     defect class one type over (`hashVectorValue` too, kept in step per
     its own comment requiring the two hash the same byte stream), closed
     in the same fold-in that closed #459. Nothing named in this item's
     original list remains open. Three findings adjacent to it — not float
     ordering — surfaced during the same work and are tracked separately
     rather than folded in: RIGHT/FULL joins losing a NULL-keyed BUILD row
     on the integer key paths (#496), `BuildFromRows` routing a dual-int key
     join down the string branch (#498), and cross-scale DECIMAL set
     operations not deduplicating (#499). A float row-group statistics bound
     can also HIDE a NaN
     that this order says must have kept the row group in a `>`/`>=`/`<>`
     prune — that is a pruning-input question, not an ordering one, and is
     recorded in ADR-0018's territory instead (its §5).

9. **Exact numeric aggregates: what MIN/MAX/SUM/AVG over a DECIMAL answer.**
   (Added 2026-08-23, #455.) PostgreSQL's `min`/`max`/`sum`/`avg` over
   `numeric` are exact and answer in `numeric`. Wadjet's were answering in
   `float64`: the accumulators were already exact Int128 at the column's
   scale, but the declared OUTPUT type was a double, so everything past ~16
   significant digits was gone before any consumer saw it —
   `MAX(numeric(38,10))` returned `9.777777778877776e+14` for
   `977777777887777.7577887713`, and `HAVING MAX(d) = <that value>` therefore
   matched nothing. The contract now:

   - **MIN/MAX(DECIMAL(p,s)) → DECIMAL(p,s).** The answer is a value the
     column holds, so it keeps the column's own precision and scale. Exact,
     and identical to PostgreSQL.
   - **SUM(DECIMAL(p,s)) → DECIMAL(38,s).** Exact, accumulated in Int128 at
     the input's scale. The declared precision is the carrier's full width
     rather than the input's, because a sum genuinely exceeds its column's
     precision and a narrower declaration would hand the parquet writer a
     leaf too small for the value.
   - **SUM overflow is an ERROR, not a wrapped total.** PostgreSQL's numeric
     is unbounded; wadjet's exact carrier is 128 bits, which holds every
     DECIMAL(38) value but not every sum of them (two values near 10^38
     suffice). A wrapped sum is a different number wearing the right type, so
     the query fails with a message naming the aggregate. This is a
     deliberate, documented limit of the carrier — not a semantic
     disagreement with PostgreSQL. The flag is STICKY, so a running total
     that leaves the range and comes back — `+9e37, +9e37, -9e37`, whose
     exact total is representable — also fails; refusing a sum we did carry
     exactly is the conservative side of a limit whose other side is a
     wrapped number nobody can see is wrong.
   - **AVG(DECIMAL(p,s)) → DECIMAL(38, min(s+4, 38))**, computed as exact
     division of the Int128 sum by the row count, rounded half away from
     zero. This is a **deliberate divergence in the number of digits kept**:
     PostgreSQL's numeric division picks a scale giving at least 16
     significant digits (and never below the dividend's scale), so its answer
     may carry more or fewer fractional digits than wadjet's. Both are exact
     to the digits they keep and agree to `min(both scales)`. A fixed
     increment — the Spark and SQL Server rule — is the honest choice for a
     128-bit carrier: the digits kept do not depend on the magnitude of the
     answer, so the same query over more rows cannot silently change the
     scale of its own output column. An average with no exact 128-bit value
     is an error, for SUM overflow's reason.
   - **STDDEV / VARIANCE / CORR / COVAR / MEDIAN / PERCENTILE over a DECIMAL
     stay float64.** PostgreSQL answers those in `numeric` too. This is a
     KNOWN, deliberate deviation, recorded rather than hidden: they need
     square roots and running means, which an exact fixed-point tower does
     not provide, and the oracle's float tolerance covers the difference.
     Reopening it means building the tower, not widening an accumulator.
   - **Both execution paths and the DAG's partial/final merge answer the
     same thing.** The partial ships SUM as an Int128 DECIMAL plus a COUNT,
     and the final divides — `internal/worker/avg_fold.go`. A DECIMAL
     aggregate that answered exactly in one process and approximately across
     three workers would be the two-path defect class all over again
     (ADR-0018 §3).
   - **The oracle compares these entries EXACTLY.** MIN/MAX/SUM over a
     DECIMAL are compared digit for digit on both engines
     (`pgCase.exactNumeric`), because a float-rendered comparison is what let
     the defect ship green. AVG keeps the float comparison, for the scale
     contract above and for no other reason.

10. **A network-literal comparison follows the ADDRESS's own order and
    PostgreSQL's `inet` rules — item 8's boxed-value rule applied to IPv6
    and CIDR — and a literal that names no address is a query ERROR.**
    (Added 2026-08-24, #492. Rewritten 2026-08-24 after review: the first
    pass got the ordering RULE wrong for CIDR and invented a match-nothing
    answer for a literal it could not parse.) `tryNetworkLit`/
    `CmpNetworkLit` (`internal/engine/expr`) already pre-parsed an IPv4 or
    MAC literal into its column's raw int64 encoding at compile time, so
    ordering compared the address numerically. IPv6 and CIDR literals had
    no such preparser: `compileCmp` fell back to a plain `*expr.Cmp`, whose
    generic path compares the column's RENDERED TEXT lexically —
    `"2001:db8::9" > "2001:db8::10"` as text (`'1' < '9'` byte-wise), the
    opposite of the numeric truth, and not even a total order (a value can
    fail BOTH `<` and `>` against the same literal). CIDR was worse: even
    the KERNEL's scan-pushdown path (`ResolveFilterKernel`'s `TypeCIDR`
    case) was lexical, because the column stores CIDR as plain text
    (`parquet/schema.go`) with no raw-byte form to fall back on the way
    IPv6 already had.

    **The order is PostgreSQL's `inet`, not its `cidr`.** Those are two
    types there and only one of them can hold what wadjet's column holds:
    `'10.0.0.1/8'::cidr` is an ERROR in PostgreSQL ("Value has bits set to
    right of mask") while `'10.0.0.1/8'::inet` is an ordinary value, and
    wadjet's CIDR column is unvalidated text (`internal/storage/ingest`)
    into which host-bearing prefixes are routinely written — they are what
    most network telemetry carries. Choosing `cidr`'s semantics would mean
    declaring most of the real data invalid; choosing `inet`'s means every
    value has a place in the order. `inet` it is.

    `network_cmp_internal` (`src/backend/utils/adt/network.c`) compares, in
    order: the address FAMILY; the common bits under the SMALLER of the two
    prefix lengths; the prefix length; the FULL, UNMASKED address. Three
    consequences a simpler rule gets wrong, all verified against live
    PostgreSQL 17:

	'9.255.255.255/32' < '10.0.0.0/8'    — common bits decide before the mask
	'192.168.1.5/24'   < '192.168.1.0/32' — the MASK outranks the address
	'10.0.0.0/8'       < '10.0.0.1/8'    — host bits are KEPT, ordered last

    `kernel.CidrSortKey` is that order as a byte string —
    `[family][address masked to its own prefix][prefix length][full unmasked
    address]` — and is the ONE implementation both the kernel's
    scan-pushdown path and `expr.CmpNetworkLit`'s generic-evaluation path
    call, exported for exactly the reason its own doc comment gives: two
    structural parsers maintained separately is the two-path defect class
    this item closes, not a shape to reintroduce by duplicating it.
    `TestCidrSortKeyMatchesPostgresInetOrder` pins the whole order against a
    PostgreSQL-derived table of host-bearing and canonical values, v4 and
    v6, at mixed prefix lengths.

    **The first pass keyed the MASKED network alone**, which threw the host
    bits away: `10.0.0.1/8` and `10.0.0.0/8` became one value, so
    `WHERE c_cidr = '10.0.0.1/8'` answered a row holding a DIFFERENT
    address. Every value in the corpus was a canonical `192.168.N.0/24`, so
    no gate could see it; the fixture now mixes canonical and host-bearing
    prefixes at four mask lengths, with host-bearing addresses INSIDE
    networks the fixture also holds (`typematrix.cidrValue`).

    **A BARE address is a /32 or /128 host route**, which is what
    PostgreSQL's inet does with the same input (`'10.0.0.1'::inet =
    '10.0.0.1/32'::inet` is true). The first pass could not parse one, and
    answered `c_cidr = '10.0.0.1'` with a match-nothing kernel through the
    scan while the row-at-a-time path compared the text — so a WHERE clause
    and a SELECT list disagreed about the same row.

    **A literal that names NO address is a query ERROR — SQLSTATE 22P02 —
    never a value and never a match-nothing kernel.** This is #463's rule
    for DECIMAL, one type family over, and for the same reason: a
    match-nothing answer to `c_cidr <> 'garbage'` deletes every row of a
    query that cannot mean anything, silently. Both paths raise it: the
    kernel returns no kernel and `exec.networkConstError` turns that into
    the error (the mechanism `compareFilterDecimal` already used), and the
    row path raises from `CmpNetworkLit`'s CIDR/IPv6 arms for a literal that
    parses as some OTHER address kind, or from the binding `Cmp` already
    carries for a literal that parses as nothing at all. The lexical
    `genericFallback` is gone from both arms. The comment that claimed
    `parseIPv4ToInt64`/`parseIPv6ToRawString` already answered such a
    literal with a match-nothing sentinel was simply wrong — the first
    returns 0, which MATCHES the rows holding `0.0.0.0` — and TypeIPv4,
    TypeMAC and TypeUUID still take that silent path. That is the same
    defect one type over, filed as #519 rather than widened into this fix.

    **A malformed STORED value is UNKNOWN, not an error and not a text
    comparison.** The column is unvalidated, so a row can hold something
    that is not an address. It matches nothing for every operator, `<>`
    included — the answer a NULL row gets — on both paths. Raising instead
    would fail a whole query over one bad row in a column the engine never
    promised to validate; falling back to a lexical comparison on one path
    only (what `evalCIDR` did) is how one bad row split the two paths apart.

    **That UNKNOWN rule is a PREDICATE rule, not a value rule.** A malformed
    row is invisible to `WHERE c_cidr = anything` and `WHERE c_cidr <>
    anything`, but it is still an ORDINARY value everywhere a query retains
    or orders values rather than testing them against one: GROUP BY gives it
    its own group, ORDER BY gives it a definite position, and MIN/MAX can
    return it. `kernel.CidrOrderKey` is exactly this split, by construction —
    it is `CidrSortKey`'s `ok` collapsed to always succeed, falling back to
    the row's own raw text when the value does not parse, so "no defined
    place in `WHERE`'s order" becomes "the row's own bytes decide its place"
    the moment the consumer is a key or a comparator instead of a predicate.
    One consequence worth naming because it looks like a bug and is not:
    `MAX(c_cidr)` can return a value that no `WHERE c_cidr = '<that text>'`
    will ever match — a malformed value sorts (as its raw text, which a
    real address's inet-ordered key does not collide with except by exact
    byte match) and predicates refuse it. PostgreSQL has no equivalent case
    to verify against, because its `inet` input function rejects a
    malformed literal before a row can ever hold one; wadjet's column is
    unvalidated text, so this asymmetry is the shape the "don't fail a whole
    query over one bad row" decision above takes once #520 gave GROUP BY/
    ORDER BY/MIN-MAX their own inet-ordered key.

    **IPv6 against a v4-shaped literal is a FAMILY comparison.**
    PostgreSQL's inet puts every v4 address below every v6 one
    (`'255.255.255.255'::inet < '::'::inet`), including below a v4-MAPPED v6
    address, which it still calls family 6. `kernel.IPv6LitKey` keys a v4
    literal to the EMPTY string — shorter than, and a prefix of, every
    16-byte row value, so it compares strictly below all of them with no
    per-row re-keying. The kernel used to read that literal as its v4-mapped
    16 bytes, landing it in the MIDDLE of the v6 range, while the expr path
    fell through to a lexical text compare: two paths, two orders, neither
    PostgreSQL's.

    IPv6's ordinary case needed the same preparse CIDR did: the literal is
    the address's raw 16 bytes, a fixed-width big-endian encoding, so Go's
    own string ordering IS the address's numeric order.

    UUID needed no fix: its literal always zero-pads to a fixed 32-hex-digit
    form, so lexical order of that FIXED-WIDTH text happens to equal the
    address's own byte order — an accident of representation pinned by a
    test (`TestUUIDOrderingIsCorrectByHexAccident`) rather than relied on
    silently, because it does not generalize to IPv6's variable-width
    `::`-compressed form or CIDR's variable-width prefix notation, which is
    exactly why those two needed a real fix and UUID did not.

    **One key, every consumer of the comparison.** `IN` shares it
    (`kernel.inFilterKeyed`), because `c = 'X'` and `c IN ('X')` answering
    differently is the two-kernel version of the same defect. The row-group
    PRUNE does not, and cannot: the footer bounds are the address TEXT's
    extremes and the inet-order extremes are different rows, so TypeCIDR is
    withheld from pruning entirely — ADR-0018 §6, which is where that
    reasoning lives; restoring it needs an inet-ordered bound written at
    WRITE time, filed as #523.

    **Residual, closed 2026-08-25 (#520): `ORDER BY`, `GROUP BY`, DISTINCT,
    COUNT(DISTINCT), MIN/MAX and hash-join keys over a CIDR column used TEXT
    order.** The two engines agreed with each other there, so no two-path
    gate saw it, but PostgreSQL sorts those by inet order too — '10.0.0.1'
    and '10.0.0.1/32' are one value there, so text-order GROUP BY/DISTINCT
    answered TWO groups/values for a pair `=` already called one. Closing it
    needed an inet-ordered comparator in the sort kernels, the group/hash
    key (`appendColumnValue` and the spill/boxed key path), the
    declaration-resolved boxed comparator, MIN/MAX (both the batch kernel
    and `Accumulator.Merge`, which combines partials across a scan-batch or
    parallel-worker boundary and had the identical TEXT-order bug one level
    up), and BOTH partition routers — the distributed shuffle sink and a
    second, LOCAL one (`legacyCompositeHash`, the morsel-parallel
    aggregation router), which was the hardest of the sites to find because
    it only misroutes rows across a `workers > 1` boundary. `kernel.CidrOrderKey`
    is the one implementation every site now calls, the same breadth item
    8's float rule needed for its own primitive (`kernel.KeyFloat64Bits`).

    **Residual, closed 2026-08-25 (#546): the single-process SET OPERATION
    dedupped a CIDR column by its raw stored TEXT** while the stage DAG —
    which lowers a set operation to a `GroupByAll` aggregate, so #520's key
    already reached it — dedupped it by inet. The identical `UNION` answered
    4 rows locally and 3 on the DAG. `physical.keyValueText`'s
    `parquet.TypeCIDR` arm is the missing consumer, and it takes
    `CidrOrderKey` rather than `CidrSortKey` for the reason the split above
    gives: this is a KEY, so a value that names no address needs a position
    rather than a refusal, and discarding those to the empty string would
    dedup three DISTINCT malformed values into one member.

    **Residual, closed 2026-08-25 (#565): a column-to-COLUMN comparison was
    inet-ordered on the VECTORIZED kernel alone.** `WHERE c = d` answered
    PostgreSQL's 2 in process and 0 on the stage DAG, and — the sharper
    form, needing no second engine — one process selected two rows through
    the kernel and then said `SELECT c = d` was FALSE about those same rows.
    `expr.compare()`'s both-string fast path is what a projection and a later
    DAG stage's re-parsed filter reach, and it compared the stored texts.

    The fix is item 8's boxed-value rule applied to this family: the pair is
    bound from the two operands' DECLARATIONS (`expr/boxed_pair.go`'s
    `boxCidr`/`boxIPv6` kinds), which is the only place the answer exists,
    since a CIDR value and a STRING value are the same Go box. That reaches
    the three sites #506 needed for DECIMAL — a simple `CASE`'s operand, `IS
    DISTINCT FROM`, `GREATEST`/`LEAST` — which were wrong on BOTH arms and
    so invisible to every two-path gate.

    Two findings from sweeping the other network types on that shape:

    - **IPv6 column-to-column ORDERING had the identical split**, and is
      fixed with it. The kernel compares the stored raw 16 bytes;
      `ColRef.Eval` boxes the RENDERED text, and "2001:db8::9" sorts above
      "2001:db8::10" as text and below it as an address.
      `kernel.IPv6RowKey` re-keys the rendering back to the stored bytes —
      exactly, v4-MAPPED addresses included, which is why it is NOT
      `IPv6LitKey`: a LITERAL dotted quad is a v4 address and keys below
      every v6 row by family, while a STORED one is a v4-mapped v6 address
      and keys among them. IPv4, MAC and UUID were checked on the same shape
      and AGREE — the first two box the raw encoded int64, UUID by the
      fixed-width-hex accident recorded above — and are pinned that way.
    - **A malformed STORED value was compared by TEXT at both col-col
      sites**, the kernel included, contradicting this item's own UNKNOWN
      rule. `colColFilterCidr` keyed through `CidrOrderKey`, whose text
      fallback is right for a KEY and wrong for a PREDICATE — the split this
      item draws, missed at one site — so `WHERE c = 'garbage'` and
      `WHERE c = d` followed two different rules about the same bad row.
      Both take `CidrSortKey` and UNKNOWN now.

    **Open residuals, filed 2026-08-25.** Three places a CIDR value still
    does not get the order this item specifies. None is a predicate, which is
    why none of them is fixed here:

    - **#568: a ROW FIELD PATH is declared STRING, so `ORDER BY rw.c` sorts
      by text** — `9.0.0.0/8` LAST, where `ORDER BY rw` over the same values
      puts it FIRST. The sort is not what is wrong: `colRefDeclaredType`
      resolves the field name against the input's own COLUMNS, where it is
      not one, and never consults the parent's `parquet.Column.Fields`, so
      the projection keeps its STRING default and `ResolveSortCompare` is
      handed a STRING column. It is not a CIDR defect — `ORDER BY rw.n`
      sorts an INT64 field as text by the same mechanism — which is why it
      is its own issue rather than a line in this list.
      `wadjet.TestRowFieldPathLosesTheFieldsDeclaredType` pins it.
    - **#569: windowed MIN/MAX declares FLOAT64 and fails the query** for
      CIDR and seven other types, where the plain aggregate answers
      correctly. `exec.WindowMinMaxType`'s allow-list is deliberate — the
      window picks its answer with `compareAny`, which dispatches on the Go
      box and has no type tag to route a CIDR to `CidrOrderKey`, so the
      FLOAT64 declaration turns a silently wrong ordering into a loud
      failure. Widening it means giving that comparison the declaration
      (`exec.newBoxedCompare`), which is this item's rule again at a site
      that never had it.
    - **#523: the row-group PRUNE**, unchanged and recorded above.

    The two-path divergence this item closes was reproducible directly: the
    single-process engine answered 16 rows and the stage DAG answered 2 for
    the same `WHERE c_ipv6 < '2001:db8::10'` — both engines compile the
    predicate through the identical `expr.Compile` (the worker's
    `compileFilterExprs` calls it too, never a separate re-implementation),
    so the fix lives entirely in `internal/engine/expr` and
    `internal/engine/exec/kernel` and reaches both paths through the shape
    both already shared. `internal/oracle/typematrix`'s corpus, which had
    deliberately excluded an ordering literal comparison against IPv6/CIDR
    to avoid gating an already-known bug, now includes one
    (`litcmp_ord_c_ipv6`, `litcmp_ord_c_cidr`) plus the shapes the second
    pass needed (`litcmp_bare_c_cidr`, `litcmp_hostbits_c_cidr`,
    `litcmp_xfamily_c_ipv6` and their ordering forms).

11. **LIKE renders the column to TEXT for every type — the value's own
    printed form, identical at both evaluation sites — rather than refusing
    the way PostgreSQL does.** (Added 2026-08-24, #497. Scoped and corrected
    2026-08-24 after review: the original claimed the rendering matches
    `CAST AS STRING` across all 22 types, which is true of the seven types
    the fix was about and false of DATE.)

    PostgreSQL's `inet`/`cidr`/`macaddr` — and its `date`, `numeric`,
    `integer` and `boolean` — refuse LIKE outright (verified live:
    `'10.0.0.1'::inet LIKE '10.%'` raises "operator does not exist: inet ~~
    unknown"). That is not a semantics PostgreSQL DECIDED for wadjet's own
    network types to disagree with (item 1's territory); it is PostgreSQL
    not having the "these types are text everywhere" contract #484 already
    built for them. **Wadjet renders and matches.** The reasons, in order:
    the rendering contract already exists for the six network types and
    UUID; the engine has answered `int_col LIKE '1%'` since before there was
    a decision to make, and turning that into an error is a breaking change
    with no correctness payoff; and the invariant that actually failed here
    was never "does this operator exist" but "does it read the right backing
    store, and does it answer the same at both sites".

    **What the text is.** The value's own printed form — `Vector.GetValue`'s
    rendering, which is what the projection shows: `2011-02-02` for a DATE,
    `10.0.0.1` for an IPv4, `1.0001` for a DECIMAL, epoch MILLISECONDS for a
    TIMESTAMP, `0.14285715` for a FLOAT32 (float32 shortest round-trip, not
    its float64 widening). For the six network types and UUID this is also
    exactly what `CAST AS STRING` and every scalar function argument produce.
    (Amended 2026-08-25, #521: it did NOT hold for DATE — `CAST(c_date AS
    STRING)` answered the epoch DAY, `15007`, where the projection and LIKE
    answered `2011-02-02` — and the review that closed it found the identical
    gap for FLOAT32, where CAST answered the float64-widened digits instead
    of the float32-shortest-round-trip form. Both were `expr.Cast.Eval`'s
    string-family case reading the operand through `ColRef.Eval`'s raw-box
    fast path (epoch-day int32 for DATE, float64-widened for FLOAT32)
    instead of the column's own rendering — the identical mechanism LIKE's
    operand had via `likeOperand`, which #497's review had already fixed
    for these same two types on the LIKE side and left CAST's separate,
    narrower `networkOperand` (IPv4/MAC only) unfixed. The two resolvers
    are now one function, `boxedTextOperand`, shared by both call sites —
    the two-implementation drift this ADR calls out elsewhere (`CidrSortKey`,
    `appendColumnValue`) for the same reason: two structural renderers
    maintained separately is how one gets fixed and the other doesn't. The
    claim now holds for every flat type PostgreSQL gives wadjet a printed
    form to agree on, verified against live PostgreSQL 17: `date::text` and
    a `real` column's `::text` match wadjet's rendering exactly.
    `wadjet.TestCastStringAgreesWithProjectionAcrossFixture` sweeps DATE and
    FLOAT32 across the type-matrix fixture; item 11 below records LIKE's own
    sweep, `wadjet.TestLikeAnswersTheSameAtBothSites`, which already covered
    every flat type and is what caught FLOAT32 for LIKE in the first place.)

    **Both sites must render alike, and two did not.** `ResolveLikeFilterKernel`
    (`internal/engine/exec/kernel/compare.go`) unconditionally read the
    column's `BytesData`, with no per-type dispatch at all — the same shape
    of gap `ResolveFilterKernel` (the `=`/`<`/`>` kernel) is explicitly built
    to avoid. TypeIPv4/TypeMAC/TypePort/TypeProtocol store into
    `Int64Data`/`Int32Data`, so this INDEXED AN EMPTY BACKING STORE — a panic
    that is not the one deliberate `FatalEvalPanic` shape the pipeline
    drivers convert back into a query error, so it re-raised untouched all
    the way up: a process killer, not a wrong answer. TypeIPv6/TypeUUID
    store into `BytesData` but as the address's RAW binary form, so the
    pattern silently matched nothing. `likeTextRenderer` now resolves a
    per-type row-to-text function once per column — the same
    resolve-once-dispatch-in-the-loop discipline every other kernel here
    follows — covering every one of the 22 types (a default arm renders any
    other type's own boxed value, never indexing `BytesData` on a column
    that does not have it).

    The row-at-a-time `expr.Like` path had the same class of gap for the
    four types `ColRef.Eval` boxes differently from `GetValue`. #497 closed
    two of them (TypeIPv4/TypeMAC, through the shared `networkOperand`
    resolver `Cast`'s string-family case already used) and left two open,
    which the review found: `c_date LIKE '20%'` matched 4949 rows through
    the scan and 83 through a projection — the epoch DAY, not the date —
    and `c_f32 LIKE '%1%'` differed by 237 rows, a float64-widened rendering
    of a float32. `expr.likeOperand` closes both, and
    `wadjet.TestLikeAnswersTheSameAtBothSites` sweeps EVERY flat type
    through both sites so a fifth type that starts boxing differently is a
    failing test rather than another quiet divergence. An enumerated list of
    "types that box differently" is the same shape of gap #497 was filed
    for; the sweep is what makes the list checkable.

    **Containers refuse, closed 2026-08-25 (#522).** ARRAY, ROW, MAP and
    VECTOR used to reach the default arm and match Go's own `fmt.Sprint` of
    the boxed value (`[1 2 3]`, `map[k0:0]`) — both sites agreed on it, so it
    was not a two-path divergence, but it was not a text form the engine
    produces anywhere else and not a contract: it would have changed if the
    boxing did. Of the two honest answers the open question named — define a
    real container text, or refuse the way PostgreSQL refuses its own
    composite/array types — the decision is REFUSE, on rule 1 above: this is
    not one of the seven types wadjet has its own reason to diverge from
    PostgreSQL for (they have no PostgreSQL equivalent at all), it is an
    ordinary composite/array value, and PostgreSQL's answer for `LIKE`
    against one is unambiguous (verified live: `ARRAY[1,2,3] LIKE '1'` and a
    composite-typed value both raise "operator does not exist: <type> ~~
    unknown", SQLSTATE 42883/undefined_function). Inventing a text form
    wadjet has never committed to anywhere else would be manufacturing a
    contract this ADR's own rules argue against creating casually.
    `kernel.ResolveLikeFilterKernel` returns nil for the four container
    types (`exec.LikeFilter` turns that into the 42883, the same shape
    `decimalConstError`/`networkConstError` already use for a different type
    family) and `expr.Like.EvalBoolNull` raises the same error from the row
    path (`containerLikeKind`/`raiseNoLikeOperator`) — both verified against
    live PostgreSQL 17, and `wadjet.TestLikeAnswersTheSameAtBothSites` (no
    longer skipping non-flat columns) and
    `wadjet.TestLikeAgainstContainerRefusesWithPostgresErrorCode` pin the
    SQLSTATE at both sites.

    **Both refusals are RUNTIME, not plan-time — the same open question
    #517 tracks for DECIMAL, one type family over.** `WHERE id > 100000 AND
    col = 'garbage'` answers 0 rows SILENTLY rather than raising, when no row
    satisfies `id > 100000`: row-group pruning (or simply zero matching
    rows) means the scan never delivers a batch to the `col = 'garbage'`
    filter at all, so `KernelFilter.Execute`/`expr.Like.EvalBoolNull` — and
    item 10's own `networkConstError`/`CmpNetworkLit` raise for a literal
    that names no address — never RUN for that column, and a refusal that
    only fires when data flows through it is not the same guarantee as one
    PostgreSQL's planner gives by type-checking the literal against the
    column's declared type before any row is read. #517 names the identical
    mechanism for DECIMAL's boxed CASE/IS DISTINCT FROM/GREATEST sites —
    those still ANSWER for a malformed literal reaching them, the same way a
    container or a malformed network literal still answers through a
    conjunct no row survives to. Neither this pair nor #517 is fixed here;
    #517's own "fix direction" (a plan-time check against the column's
    declaration, before any row is scanned) is the shape a fix for any of
    these would need to take, and it is one check for all of them precisely
    because the column's declaration is known before any type's differing
    runtime code path matters. (Unrelated to this pair, and mentioned only
    because it was found alongside them and is not fixed here either: #544,
    `CAST(timestamp AS STRING)` and LIKE render epoch milliseconds rather
    than the instant pgwire renders.)

    **Item 6's PLAN-TIME refusal has the identical gap, for the identical
    structural reason.** (Added 2026-08-25, #579.) `checkLiteralTypes`/
    `refuseLiteralAgainstColumn` (`internal/planner/physical/
    validate_literal.go`) ask `colScope` whether a column is DECIMAL — a
    plain BOOLEAN (`colScope.addQualifiedTyped`'s `isDecimal bool`,
    `internal/planner/physical/validate.go`), not the column's declared
    `parquet.TypeID` — so there is no way to ask the same question for CIDR
    or any other network type even in principle; this is not a rule that
    excludes them, it is a bool that only ever meant one type. A non-numeric
    literal against a CIDR column therefore never reaches item 6's #517
    fix and falls all the way back to THIS item's runtime-only story: it is
    evaluated as ordinary text at the three boxed sites (CASE, IS DISTINCT
    FROM, GREATEST/LEAST) and only refuses at the scan-filter runtime layer,
    and only once a row actually reaches it. Verified live against
    postgres:17 on a `cidrtest(id BIGINT, c CIDR)` fixture: `WHERE id >
    100000 AND c = 'zzz'` raises 22P02 there unconditionally and here only
    when a row satisfies `id > 100000`; `CASE c WHEN 'zzz' THEN 1 ELSE 0
    END = 1`, `c IS DISTINCT FROM 'zzz'` and `GREATEST(c, 'zzz')` all answer
    here and all raise there. Widening `colScope` to carry the column's
    declared type (or an enum covering DECIMAL/network/neither) instead of
    a bare bool, with a per-type literal validator alongside
    `expr.IsNumericLiteralText`, is the fix shape #579 records.

    **BYTES is bytea, and it is the one type where LIKE and CAST
    deliberately disagree.** (Added 2026-08-25, #570. Recorded as an open
    divergence when this item was written; closed the same day.)

    PostgreSQL's `bytea::text` is `\x` followed by lowercase hex, under the
    default `bytea_output = hex` — a setting `expr.pgcompat` already reports
    to a client that asks for it. Wadjet had THREE renderings of one BYTES
    value and none of them was that: the embedded projection handed back a
    `[]byte` (`Vector.GetValue`'s TypeBytes arm), `CAST AS STRING` and LIKE
    handed back the RAW BYTES as a Go string, and the WIRE handed back Go's
    `%v`, `[255 254 0 65]`, under OID **25** because `pgwire.pgTypeOID` had
    no BYTES case and `formatPgValueTyped` no `[]byte` case. `oidBytea = 17`
    existed in the tree and was used only for inbound Bind parameters and
    the `pg_type` catalog row.

    The CAST rendering was not only wrong, it was a HAZARD. For the four
    bytes `0xff 0xfe 0x00 0x41` the raw form is invalid UTF-8 and holds an
    embedded NUL. PostgreSQL cannot represent that in a text-format field at
    all — `text` rejects `\0`, so no PG server ever puts one inside a
    DataRow text field — and libpq TRUNCATES at it: `PQgetvalue` returns a
    NUL-terminated `char*`, so a length-aware client (pgx, JDBC) read four
    bytes and a strlen-based one read two. One query, two answers, decided
    by the client library.

    **What landed.** Rule 1 decides all of it, because PostgreSQL does give
    BYTES a printed form:

    - `pgTypeOID` returns **17 (bytea)**, `pgTypeSize` -1 and `pgTypeMod` -1
      (bytea is varlena and takes no modifier), and `pgFormatType` answers
      `bytea` so the catalog stops contradicting the wire — the same pairing
      #454 had to restore for DECIMAL.
    - `formatPgValueTyped` renders `\x` + lowercase hex, and
      `appendBinaryValue` writes the raw bytes, which IS bytea's binary form
      (`byteasend`). The binary arm is what the OID change makes
      load-bearing: under OID 25 the `%v` fallback was at least
      self-consistent, because the binary form of a text column is its
      bytes.
    - `CAST(b AS STRING)` renders the same `\x` hex, which removes the
      embedded-NUL hazard for free — hex is pure ASCII. This REVERSES the
      direction of the change that made CAST agree with `likeTextRenderer`
      by handing back the raw bytes; agreeing with each other was the right
      half, agreeing on the raw bytes was not.
    - A bytea **Bind parameter** decodes to BYTES. pgwire has no
      bound-parameter path below the parser, so Bind renders each parameter
      as a LITERAL, and it was writing the SPELLING of the bytes
      (`'\x6869'`) — a ten-character string compared against a two-byte
      column, matching nothing. `bindparams.decodeByteaText` reads both of
      `byteain`'s spellings (hex, and the escape form with `\\` and
      `\ooo`) and the binary form is the bytes themselves; a malformed
      spelling is an error, never a fallback to the raw characters.

    **LIKE does NOT follow the CAST, and that is the PostgreSQL answer.**
    `~~` over bytea EXISTS there and is BYTEWISE — verified live,
    `'\xfffe0041'::bytea LIKE '%A%'` is TRUE, matching the 0x41 BYTE and not
    a digit of the hex spelling. So PostgreSQL itself renders one way and
    matches another for this type, and `kernel.likeTextRenderer` keeps
    matching the raw bytes. The "identical at both evaluation sites" claim
    this item makes is about LIKE's two sites (the kernel and the row path),
    which still agree; the additional claim that CAST agrees with LIKE holds
    for every flat type EXCEPT this one, and only because PostgreSQL breaks
    it first. `wadjet.TestCastStringPinsPostgresRenderingPerType` pins the
    hex spelling and `TestCastStringAgreesWithProjectionAcrossFixture` builds
    the BYTES expectation from the projected bytes rather than from CAST, so
    an implementation and a comparator agreeing on the same wrong hex still
    fail.

    **The engine's own answers were already PostgreSQL's**, which is why the
    fix moved the wire and the CAST and nothing else: comparison and
    ordering over BYTES are bytewise, an unknown-typed literal beside a
    BYTES column is read as its bytes (`b = 'hi'` finds the same row on both
    engines), `LENGTH`/`OCTET_LENGTH` count octets, and GROUP BY, DISTINCT,
    joins and NULL handling all agree. That is a finding of the coverage
    below, not an assumption.

    **The coverage hole is closed, and it found a second defect.** `bytea`
    appeared NOWHERE in `benchmarks/` or `internal/oracle/` — the pg-oracle
    ran only over the TPC-H fixture, which has no BYTES column, so
    `EngineSemantics` had never compared the value and `WireProtocol` had
    never compared the OID. The fixture now has a `bytea_probe` table
    (`postgres_oracle_test.go`, PG side `bytea`, wadjet side BYTES) holding
    the empty value, four NULs, the invalid-UTF-8-with-embedded-NUL value
    above, ASCII text, single high bytes, a prefix pair, and NULLs. Over it
    run 33 gated semantics entries (plus the one pin below), 10 wire-metadata
    entries and 2 in the wire error list; the DuckDB arm loads the SAME rows
    — not a second copy of "the same" fixture — for 13 more BLOB entries. Standing that
    up immediately failed a query the type matrix had never asked:
    `ResolveColColFilterKernel`'s string arm listed STRING, IPv6 and UUID
    but not BYTES, and because two BYTES columns share a TypeID the
    mixed-type row-at-a-time fallback did not apply either — so
    `WHERE b_val = b_other` came back as "could not resolve kernel", the
    identical shape #477 found for two DECIMALs. This is the coverage-matrix
    argument in concrete form: the gap was not in what the gates ASSERT but
    in which types they had a value for.

    **What is not fixed, and is pinned rather than forgotten.** A BYTES
    LITERAL does not exist: `b_val = '\x6869'` in a hand-written statement
    compares six characters, where PostgreSQL's byteain reads two bytes
    (#582, `BytesEqHexSpelledLiteral`). The Bind path escapes this only
    because it has a declared parameter OID to decode against. And the
    scalar function layer has no BYTES notion at all — every function reads
    its operand through `expr.toString`, so `UPPER(b)` ANSWERS where
    PostgreSQL raises 42883, and `b || b` returns TEXT where PostgreSQL
    returns bytea, which puts the raw-bytes-under-OID-25 hazard back in
    through a derived value (#583, `ByteaTextFunctionOverBytes` and
    `ByteaConcat`). `OCTET_LENGTH` over bytea declares float8 for #530's
    reason, unrelated to this type.

12. **A set operation's result type is the COMMON type of its arms, and every
    arm is MOVED into it — never reinterpreted.** (Added 2026-08-25, #533.)

    The numeric ladder, verified against live postgres:17-alpine and pinned
    by `physical.TestSetOpWidenLadder`:

        INT32 → INT64 → DECIMAL → FLOAT64

    `numeric UNION ALL bigint` is **numeric** (an integer converts to numeric
    implicitly and not back); `numeric UNION ALL double precision` is
    **double precision** (float8 is the PREFERRED type of PostgreSQL's
    numeric category). Arm ORDER changes neither, there or here. Nothing
    outside the numeric family widens: rendering a number as text to make two
    arms line up would answer a different query.

    **When the common type is DECIMAL, the (p,s) is:**

    - **scale = max over the arms.** The only choice that moves no value; a
      narrower one DROPS digits the wider arm holds.
    - **precision = max over the arms of (precision − scale), plus that
      scale**, capped at 38. Rebuilt from the widest INTEGER part rather than
      taken as max(precision), because max(precision) is not a bound on the
      widened values: `DECIMAL(18,2)` alongside `DECIMAL(9,4)` needs 16
      integer digits at scale 4 — 20 — where max(precision) would declare 18
      and hand the parquet writer a leaf too small for its own values
      (ADR-0018 §4's encoding rule keys off precision). For every pair whose
      integer parts order the same way as their precisions the two rules
      agree, `(9,2)/(18,4)/(38,10)` included.
    - An INTEGER arm contributes its whole range's digits (10 for INT32, 19
      for INT64) at scale 0.

    **The arms are moved, and that is the whole point.** A DECIMAL value is
    an unscaled integer plus the column's declared scale (ADR-0018 §4), and on
    the stage DAG the two travel apart: each arm's task writes its own `.wshf`
    file carrying its own scale in the header, and the task that reads several
    such files writes ONE file under the schema of the first batch it saw.
    Reconciling only the `TypeID` reconciles nothing for two DECIMALs — they
    ARE the same TypeID — so the wider arm's unscaled integer was read at the
    narrower arm's scale and every value from it came back 100× too large,
    silently (#533). `exec.DecimalCoerce` multiplies the carrier instead, in
    the union arm's own fragment, before the rows meet. A CAST cannot do this
    job: the cast evaluator's DECIMAL destination produces a float64, which is
    the precision loss the exact carrier exists to prevent.

    **A value with no exact carrier at the output scale is an ERROR.** Same
    rule and same reason as SUM's overflow in item 9: a wrapped value is a
    different number wearing the right type, and nothing downstream can see
    that it is wrong. The bound checked is the DECLARED PRECISION (`10^p`),
    not the Int128 carrier's — they are different bounds and only the first is
    the type. A DECIMAL(38,2) whose unscaled value lands in `[10^38, 2^127-1]`
    fits the carrier and not the declaration, and admitting it would write a
    number the declared type cannot hold into a column the parquet writer
    sizes from that precision (ADR-0018 §4).

    **The 38-digit cap is a RANGE REDUCTION, and it costs answers** (#552).
    The cap can pull the output precision below what an arm's own values need,
    so a value BOTH arms held before the union becomes a hard failure after
    it: `DECIMAL(38,0)` holding `10^30` beside `DECIMAL(11,10)` resolves to
    `DECIMAL(38,10)`, and `10^30` at scale 10 needs `10^40`. PostgreSQL's
    numeric is unbounded and never reaches this. Worse, a filter or a `LIMIT`
    above the union does NOT rescue the query — the coercion runs in the arm's
    own fragment, ahead of the post-filter and ahead of the Singleton `LIMIT`
    stage — so `… WHERE v < 100` over that union fails on a row it does not
    want. This is the honest side of a 128-bit carrier and it is RECORDED
    rather than hidden; `TestSetOpDecimalCapIsARangeReduction` pins all three
    shapes so a future widening shows up as that test failing.

    **A computed DECIMAL expression is not typed DECIMAL, and the new
    DECIMAL↔FLOAT64 rung makes that visible** (#555). `inferProjectionTypeCols`
    resolves `d + d`, `COALESCE(d, d)` and even `CAST(d AS DECIMAL)` to
    something other than DECIMAL. Before this item's ladder those arms were
    REFUSED at plan time when they met a real DECIMAL arm; now the pair
    resolves to FLOAT64 and the query answers with float-rounded values where
    PostgreSQL answers `numeric`. Trading a loud refusal for a quiet
    approximation is the wrong direction, and it is the underlying typing gap
    that has to close, not the rung — the rung is what PostgreSQL resolves for
    a genuine float arm. `COALESCE` over two DECIMALs fails at runtime on both
    paths (#361's guard catches the store), which is at least loud.

    **The dedup key is the columnar one.** UNION, INTERSECT and EXCEPT decide
    membership by EQUALITY, so their key must agree with the comparator. On
    the DAG they already do — the operation lowers to a hash aggregate whose
    DECIMAL key is `batch.AppendDecimalKey` at the column's scale (#474), the
    canonical minimal-scale form — and after this the arms reach it at one
    scale anyway. The single-process path keys a boxed row by its RENDERED
    TEXT and needs its own fix (#499); the two paths are held to one answer by
    `internal/coordinator/setop_decimal_scale_two_path_test.go`.

    **Where an arm's `(p,s)` cannot be resolved, nothing is coerced and the
    answer is WRONG — not refused.** An earlier draft of this item said
    `shuffleWriter.writeChunk`'s scale check turns that residual into a failed
    task. It does not: that check sees a SINGLE writer handed two scales, and
    in a union stage each arm writes its own consistent file while the
    reinterpretation happens in the downstream stage that reads several of
    them (ADR-0010 carries the corrected statement). The open resolution holes
    are a JOIN arm — `inputColTypes`/`inputColDecimal` merge a join's two sides
    and delete any name they disagree about, which is the fact being
    reconciled (#551) — and a computed DECIMAL expression, which carries no
    `(p,s)` (#458, #555). Both are pinned, not claimed covered.

    **Two carrier properties are deliberate divergences, not defects.** A
    wadjet DECIMAL column has ONE declared scale, so the narrow arm's rows
    render with the unified scale's trailing zeros (`12.7500` beside
    `12.7501`) where PostgreSQL's variable-scale numeric prints `12.75` — the
    same number, the same row set, the class item 9 already records for AVG's
    digits. And PostgreSQL declares a cross-scale set operation's numeric
    result UNCONSTRAINED on the wire (`\gdesc` says `numeric`, typmod −1,
    where a single-arm `SELECT` of the same column says `numeric(9,2)`);
    wadjet still declares a real (p,s) there. That is wire METADATA only and
    is tracked separately (#542), not a value difference.

    **The single-process path does not yet resolve two different TypeIDs**
    (#541): it builds the result under the FIRST arm's schema, so an integer
    arm under a DECIMAL first arm reads as an unscaled carrier (1 becomes
    0.0001) and a DECIMAL arm under a FLOAT64 first arm fails the store
    outright. `unifySetOpSchemas` should delegate to `setOpWiden` and
    `setOpDecimalTarget` rather than keep its own rule, so the two paths
    cannot drift on what the output type IS. It also SATURATES rather than
    failing when the value it re-reads has no Int128 at the result scale
    (#553) — `batch.DecimalTextAt` returns `Int128Max` with a `Sat` flag the
    caller discards, so `10^30` comes back as
    `17014118346046923173168730371.5884105727`. Saturation is right where it
    was built, in #462's COMPARISON path, where an out-of-range literal
    genuinely orders above every value the column holds; as a stored VALUE it
    is a lie, and the rule above (a value with no exact carrier is an error)
    is what the value-producing callers owe.

## Consequences

- `ORDER BY x DESC` places NULLs first (changed 2026-08-19). The default had
  been unreachable before that: the DESC comparator negated the kernel's null
  handling along with its values, so the code *declared* PostgreSQL's rule and
  *emitted* DuckDB's.
- The differential gate keeps full strength across the ordering corpus rather
  than carrying exemptions.
- A DECIMAL predicate answers the same on the kernel path and the row-at-a-time
  path (changed 2026-08-23). The row path had compared a DECIMAL column's
  RENDERED TEXT against a float64 literal and, finding no numeric reading of
  that pair, fell through to a lexicographic string comparison — so
  `CASE WHEN d = 1339815.97` was false for the row holding exactly that value
  even at a scale a float64 represents perfectly.
- Open questions in the same territory, to be decided by this ADR's rule and
  recorded when they are: integer overflow behavior, `timestamptz` and the
  session TimeZone GUC, empty-string versus NULL, identifier case folding.
- DuckDB cannot adjudicate "would a PostgreSQL client expect this," so a
  PostgreSQL differential arm (small scale, plus a pgwire protocol comparison)
  is the natural completion of this decision. See ADR-0013.

## Related

- ADR-0001 (record architecture decisions — the re-asked-question problem)
- ADR-0013 (correctness gates and their deliberate boundaries)
- CLAUDE.md, "Don't skip pgwire compatibility"
- `benchmarks/tpch/oracle_semantics_test.go`, `internal/planner/physical/plan.go`
  (`resolveNullsLast`), `internal/distributed/messages.go` (`PlaceNullsLast`)
- `internal/engine/exec/kernel/decimal_literal.go` (the literal, resolved at a
  column's scale), `internal/engine/expr/decimal_literal.go` (the row path's
  bindings and the boxed comparisons), `wadjet/decimal_literal_test.go` (the
  operator sweep at three scales, both paths)
- `internal/engine/batch/decimal.go` (`ScaledDecimal`, `DecimalTextAt`,
  `CompareDecimalTexts` — one carrier and one text comparison for every
  DECIMAL predicate), `internal/engine/exec/kernel/compare.go`
  (`colColFilterDecimal`, `DecimalConstText`),
  `internal/engine/exec/kernel/sort.go` (`CompareDecimalValues`)
- #462 (a literal past the carrier wrapped two's complement), #463 (an
  unreadable literal answered ZERO), #465 (CASE / IS DISTINCT FROM /
  GREATEST / LEAST did not carry the literal's text), #476 (a boxed DECIMAL
  against a number compared lexicographically), #477 (two DECIMAL columns had
  no kernel at all), #505 (the #463 refusal did not reach the #465 boxed
  sites or a negated string literal), #506 (two DECIMAL columns still compare
  lexically at the same three boxed sites — open), #517 (the boxed-site
  refusal is per-row, not plan-time — open) — the work items 6's amendments
  record
- #492 (IPv6/CIDR literal ordering was lexical-text, not numeric/structural,
  and disagreed between the single-process engine and the stage DAG) and
  #497 (LIKE against a network-native type or UUID panicked or silently
  matched nothing) — item 10 and item 11 above record the settled position
  for; `internal/engine/exec/kernel/compare.go` (`CidrSortKey`,
  `likeTextRenderer`), `internal/engine/expr/compile.go` (`tryNetworkLit`,
  `kernel.IPv6LitKey`), `internal/engine/expr/expr.go` (`CmpNetworkLit`,
  `Like.EvalBoolNull`), `internal/oracle/typematrix/typematrix.go`
  (`networkOrdLit`)
- #521 (CAST AS STRING did not render DATE or FLOAT32 the way the projection
  and LIKE already did — closed, item 11's amendment above) and #520 (ORDER
  BY/GROUP BY/DISTINCT/COUNT(DISTINCT)/MIN/MAX/hash-join keys over a CIDR
  column still used TEXT order — item 10's own "known residual," now
  closed) — `internal/engine/expr/expr.go` (`boxedTextOperand`),
  `internal/engine/exec/kernel/compare.go` (`CidrOrderKey`),
  `internal/engine/exec/kernel/sort.go`, `internal/engine/exec/kernel/agg.go`,
  `internal/engine/exec/kernel/types.go` (`Accumulator.Merge`),
  `internal/engine/exec/aggregate.go` (`appendColumnValue`),
  `internal/engine/exec/partitioned_agg.go` (`legacyCompositeHash`),
  `internal/worker/partitioned_shuffle_sink.go`
- #546 (the single-process set operation dedupped CIDR by stored TEXT while
  the stage DAG dedupped it by inet — closed) and #565 (a column-to-COLUMN
  CIDR comparison, and IPv6's ordering, were inet-ordered on the vectorized
  kernel only, so a WHERE clause and a projection of the same comparison
  disagreed inside one process — closed) — `internal/planner/physical/
  set_op_key.go` (`keyValueText`), `internal/engine/expr/boxed_pair.go`
  (`boxCidr`, `boxIPv6`, `netOrder`, `netKeyFor`, `compareNull`),
  `internal/engine/exec/kernel/compare.go` (`IPv6RowKey`,
  `colColFilterCidr`), `internal/coordinator/cidr_col_col_two_path_test.go`,
  `internal/engine/expr/network_col_col_test.go`
- #568 (a ROW field path is declared STRING, so `ORDER BY rw.c` sorts CIDR by
  text and `ORDER BY rw.n` sorts an INT64 by text, while `ORDER BY rw` is
  correct — open, pinned by
  `wadjet.TestRowFieldPathLosesTheFieldsDeclaredType`), #569 (windowed
  MIN/MAX declares FLOAT64 for eight types and fails the query where the
  plain aggregate answers — open), #570 (BYTES is not `bytea` on the wire,
  and `CAST AS STRING` yields an invalid-UTF-8 string with an embedded NUL
  that libpq truncates — open) — items 10 and 11's open residual lists
- #566 (a GROUP BY over an ARRAY/ROW/MAP/VECTOR column fails the query past a
  partial-aggregate spill — open, pinned by
  `exec.TestContainerGroupByPastASpillFails`), and the merge key underneath
  it: `internal/engine/exec/sort.go` (`appendKeyElemWithMeta` — a container's
  elements were written in the TOP-LEVEL encoding on the meta path, so
  ARRAY[1,23] and ARRAY[12,3] were one key)
- #522 (LIKE against a container column matched Go's own `fmt.Sprint` of the
  boxed value, an unspecified text form — item 11's own open question,
  settled as a refusal, closed) — `internal/engine/exec/kernel/compare.go`
  (`ResolveLikeFilterKernel`), `internal/engine/exec/filter.go`
  (`likeConstError`), `internal/engine/expr/expr.go` (`containerLikeKind`),
  `internal/engine/expr/fatal.go` (`raiseNoLikeOperator`)
- #444 (boxed ROW comparator ordered fields by name, not declared position),
  #446 (VECTOR/ARRAY(FLOAT) comparators not transitive under NaN) — the work
  item 8 above records the settled position for
- #459 (predicate kernels, the primary GROUP BY/DISTINCT hash key, and
  hash-join keys compared floats as raw IEEE754 — closed), #457 (MIN/MAX over
  a NaN column — closed) — item 8's remainder, now closed; see "What is now
  covered, and what is left" above for the distributed VECTOR-router
  follow-on that closed alongside
- `internal/engine/exec/kernel/float_order.go`, `internal/engine/exec/
  compare_boxed.go`, `internal/engine/exec/kernel/
  container_order_property_test.go` (the P1-P4 total-order property test)
- Exact numeric aggregates (item 9): `internal/engine/batch/decimal.go`
  (`AddChecked`, `AvgScale`, `DecimalAvg`), `internal/engine/exec/kernel/types.go`
  (`Accumulator.FinalSum`/`FinalAvg`/`FinalMin`/`FinalMax`),
  `internal/engine/exec/aggregate.go` (`outputSchema`, `minMaxOutputType`),
  `internal/planner/physical/plan.go` (`aggSpecOutputType`),
  `internal/worker/avg_fold.go`
