# ADR-0012 divergences: Text, bytes and collation

String collation, pattern matching, VARCHAR/CHAR length and declaration, the missing blank-padded type, and text functions over bytes. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**Binary collation** (E12)
Strings compare and sort by their bytes, not by PostgreSQL's locale-dependent collation. Locale-sensitive comparison costs work on every compare and sort, surprises analysts more than it helps, and no BI client depends on it. Where the oracle needs to agree, it runs against a `C`-collation database rather than exempting string ordering.

**A string cast enforces its length and declares it** (E56)
`CAST(x AS VARCHAR(n))` and `CAST(x AS CHAR(n))` truncate to n characters, which is what PostgreSQL counts; `expr.Cast.Eval` used to match the lowered type name exactly and let `varchar(4)` fall through to `default: return v`. The value was fixed before the declaration, because declaring `character varying(4)` over a six-character value would let a client size a buffer that overflows. `physical.declaredStringLength` (twin of `declaredTypmod`) carries the length through `ColumnMeta.StringLength` on the single-process and DAG paths (`CollectSink.SchemaHintStringLength`, `Stage.OutputStringLength`), and `pgwire.TypeMod` sends n+4 under OID 1043. The OID follows the destination name and the typmod the length: `CAST(x AS VARCHAR)` is 1043 at -1, and `parquet.VarcharNoLength` names the family's spellings; `TEXT` stays 25.

**One modifier accept-set** (E56)
Both the CAST and the DDL door read a string modifier with `parquet.StringTypeLength`: `VARCHAR(0)`/`CHAR(0)` and `VARCHAR(10485761)` are 22023, `VARCHAR(abc)`, `VARCHAR(-1)` and `TEXT(5)` are 42601, with PostgreSQL's messages. The DDL door prefixes `column "v": ` and echoes the folded upper-case token. DDL accepts `VARCHAR(n)` (it used to refuse it as an unknown type) and drops n, so an overlong INSERT stores where PostgreSQL raises 22001.

**No blank-padded string type** (E56)
There is one `TypeString` and no bpchar, so `CHAR(n)` declares `character varying(n)`, does not pad a short value, and bare `CHAR` is the unparameterized string rather than `character(1)`. PostgreSQL pads and then strips the blanks for `length()`, `||` and every comparison; padding without the stripping would put blanks into GROUP BY keys, join keys and equality, a wrong row set for a right rendering. The structural fix is a distinct blank-padded type the comparison, length and concatenation kernels dispatch on. Pinned by `wadjet.TestStringCastEnforcesItsLengthAndStillDropsTheDeclaration` and `pgwire.TestVarcharCastDeclaresItsLengthOnTheWire`.

**ENCODE takes bytes** (E62)
`encode('hi'::text, 'hex')` is 42883 on 17.11 while `md5`, `length` and `substring` take text there, so `encode`'s first position is `expr.ArgBytes` rather than `ArgTextOrBytes`. An unknown-typed literal still fits, because the server coerces it to bytea: `encode('hi','hex')` is `6869`.

**Text-only functions over bytes are refused at plan time** (E63)
`upper`, `lower`, `trim`, `reverse`, `replace`, `starts_with`, `split_part`, `lpad`, `repeat` and `char_length` over a BYTES argument raise 42883 with the server's message shape, from `expr.RefuseUnresolvableCall` run in the binder's walk (`physical.refuseInvalidRowFields`), which `Plan` and `dagplan.PlanDistributed` both reach before any stage exists; `expr.compileFuncCallNamed` is the backstop. `strpos(bytea, bytea)` answers because the parser rewrites `POSITION(sub IN b)` into it. `length` over bytes is the byte count on the vectorized and scalar arms, `substring` indexes bytes, `bytea || bytea` is bytea (OID 17) and `text || bytea` is text; `ENCODE`/`DECODE` (hex, base64, escape), `GET_BYTE` and `SET_BYTE` are the bridge.

**Why the text-with-bytes concatenation value still differs** (E63)
Rendering the bytea operand through `bytea_out` needs the operand's declared type at the evaluator, and the only carrier, `expr.compileContext.colTypes`, is nil at every compile site with no schema; deciding there would give one `||` two answers by compile site, the defect the #627 census closed. On the wire the spliced raw bytes go out under OID 25, so a non-UTF-8 or NUL-bearing value can reach a text field (#570's hazard).

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT 'B' < 'a'` | f under a locale collation such as en_US; t in a C-collation database | t (measured): strings compare and sort by bytes | — | value divergence | — · [E12](#e12), P081 | — | — |
| **r2** `SELECT 'a' < 'b' COLLATE "en_US"` | t (compared under en_US) | ERROR 0A000 collation "en_US" is not supported (measured); C, POSIX, ucs_basic and default are accepted | 0A000 | refusal | — · P111 | — | — |
| **r3** `SELECT 'abab' ~ '(a)b\1'` | t | ERROR 0A000 regular expression back reference \1 is not supported (measured): RE2 matches; `regexp_replace` reads its pattern through the same translation and refuses the same forms (2026-10-02) | 0A000 | refusal | — · P110, [RN](../0012-amendments.md#2026-10-02-the-scalar-renderers-write-a-value-as-its-declared-types-text-arc-rn-1474-1466-1467-1481) | #1481 | `coordinator.TestArcRNRendererTableEveryArm` rx/backrefPattern |
| **r4** `SELECT 'É' ~* 'é'` | t under a UTF-8 locale collation | f: case-insensitive matching folds ASCII letters only, as PostgreSQL does under the C collation; `regexp_replace`'s `i` flag the same, and its `\y` counts ASCII letters, digits and `_` as word characters (2026-10-02, measured) | — | value divergence | — · P110, [RN](../0012-amendments.md#2026-10-02-the-scalar-renderers-write-a-value-as-its-declared-types-text-arc-rn-1474-1466-1467-1481) | #1481 | `coordinator.TestArcRNRendererTableEveryArm` rx/icaseNonASCII, rx/wordY |
| **r5** `SELECT CAST('ab' AS CHAR(4))` | 'ab  ' (blank-padded to 4) | 'ab' (measured): no blank-padded string type; length, \|\| and = agree with PostgreSQL | — | value divergence | 2026-09-03 · [E56](#e56), P006 | #708, #838 | `wadjet.TestStringCastEnforcesItsLengthAndStillDropsTheDeclaration` |
| **r6** `SELECT CAST('abcdef' AS CHAR(4))` | abcd, declared character(4) (OID 1042) | abcd, declared character varying(4) (OID 1043) (measured) | — | value divergence | 2026-09-04 · [E56](#e56), P006 | #708 | `pgwire.TestVarcharCastDeclaresItsLengthOnTheWire` |
| **r7** `SELECT CAST('abcdef' AS CHAR)` | a, declared character(1) | abcdef, declared text (measured): bare CHAR is the unparameterized string | — | value divergence | 2026-09-04 · [E56](#e56), P006 | #708 | `pgwire.TestVarcharCastDeclaresItsLengthOnTheWire` |
| **r8** `CREATE TABLE vt (v VARCHAR(4)); INSERT INTO vt VALUES ('abcdefgh')` | ERROR 22001 value too long for type character varying(4) | INSERT 0 1; abcdefgh stored (measured): DDL keeps no length | — | kept superset | 2026-09-03 · [E56](#e56), P072 | #708 | — |
| **r9** `SELECT v FROM vt` | declared character varying(4) (atttypmod 8) | declared text (measured) | — | value divergence | 2026-09-03 · [E56](#e56), P072 | #708 | — |
| **r10** `CREATE TABLE vt2 (v VARCHAR(abc))` | ERROR 42601 syntax error at or near "abc" | ERROR 42601 column "v": syntax error at or near "ABC": the DDL lexer folds the identifier first | 42601 | value divergence | 2026-09-03 · [E56](#e56) | #708 | — |
| **r11** `SELECT STRPOS(b, DECODE('69','hex')) FROM bt` | ERROR 42883 function strpos(bytea, bytea) does not exist | 2 (measured): POSITION(sub IN b), which PostgreSQL answers, is rewritten into strpos | — | kept superset | 2026-09-18 · [E63](#e63), P074 | #583 | — |
| **r12** `SELECT t \|\| b, LENGTH(t \|\| b) FROM bt` | hi\x6869, 8 (the bytea operand rendered through bytea_out) | hihi, 4 (measured): the raw bytes are spliced; declared text on both | — | value divergence | 2026-09-05 · [E63](#e63) | #583 | `wadjet.TestByteaFunctionsAnswerInBytes` |
| **r13** `SELECT b \|\| t FROM bt` | \x6869hi | hihi (measured) | — | value divergence | 2026-09-05 · [E63](#e63) | #583 | `wadjet.TestByteaFunctionsAnswerInBytes` |
| **r14** `SELECT b \|\| '\x41' FROM bt` | \x686941 (the literal is one byte) | \x68695c783431 (measured): the unknown literal contributes its four-character spelling | — | value divergence | 2026-09-05 · [E63](#e63) | #583 | `wadjet.TestByteaFunctionsAnswerInBytes` |
| **r15** `SELECT SUBSTR(d, 1, 4), UPPER(n) FROM dt` | ERROR 42883 function substr(date, integer, integer) does not exist | 2024, 7 (measured): a non-text column is rendered as text first; a numeric literal is 42883 on both | — | kept superset | — · P075 | #500, #1056 | — |
| **r16** `SELECT 'a%b' LIKE 'a\%b'` | t | f (measured): \ is an ordinary character; LIKE 'a!%b' ESCAPE '!' is t | — | value divergence | — · P012 | #1169 | — |
| **r17** `SELECT SUBSTRING('foobar' SIMILAR '%#"o_b#"%' ESCAPE '#')` | oob | ERROR 0A000 SUBSTRING(text SIMILAR pattern ESCAPE escape) is not supported (measured) | 0A000 | refusal | — · P037 | #1169 | — |
| **r18** `SELECT 'abc' SIMILAR TO '*'` | ERROR 2201B naming the regex engine's complaint | ERROR 2201B the SIMILAR TO pattern "*" cannot be matched (measured) | 2201B | value divergence | — · P038 | #1168 | — |
| **r19** `SELECT NORMALIZE('abc', 'NFC')` | ERROR 42601 (only the bare keyword NFC is admitted) | abc (measured); NORMALIZE('abc', NFC) also answers | — | kept superset | — · P102 | #1169 | — |
| **r20** `SELECT LOCALTIME` | the current local time, declared time | ERROR 42703 unknown column "localtime" (measured): no TIME type | 42703 | documented gap | — · P112 | #1169 | — |
| **r21** `SELECT 'abc' IS NORMALIZED` | t | ERROR 42601 syntax error at or near "NORMALIZED" (measured) | 42601 | documented gap | — · P112 | #1169 | — |
| **r22** `SELECT U&'\0065\0301'` | é (e plus combining acute) | ERROR 42601 unexpected character: & (measured) | 42601 | documented gap | — · P112 | #1169 | — |
| **r23** `SELECT regexp_replace(E'x\nab', '^a', 'X', 'n')` | x, newline, Xb (newline-sensitive) | ERROR 0A000 regexp_replace flag "n" is not supported (measured); `m`, `p`, `w`, `x`, `b` and `e` the same, and an integer fourth argument (a start position) | 0A000 | refusal | 2026-10-02 · [RN](../0012-amendments.md#2026-10-02-the-scalar-renderers-write-a-value-as-its-declared-types-text-arc-rn-1474-1466-1467-1481) | #1481 | `coordinator.TestArcRNRendererTableEveryArm` rx/flagN, rx/flagP, rx/flagW, rx/flagX, rx/flagB, rx/flagE |
| **r24** `SELECT regexp_replace('Hello', 'x*?H*', '#')` | #Hello: a non-greedy RE takes the shortest match | #ello (measured): an RE holding a non-greedy quantifier is matched leftmost-first, and among equal-length matches the groups are RE2's leftmost-first choice (`regexp_replace('abcd', '(a\|ab)(c\|bcd)(d*)', '[\1\|\2\|\3]')` is [a\|bcd\|] where PostgreSQL writes [ab\|c\|d]); an RE whose quantifiers are all greedy takes the longest match, as PostgreSQL does | — | value divergence | 2026-10-02 · [RN](../0012-amendments.md#2026-10-02-the-scalar-renderers-write-a-value-as-its-declared-types-text-arc-rn-1474-1466-1467-1481) | #1481 | `coordinator.TestArcRNRendererTableEveryArm` rx/lazyRE, rx/posixCaptures |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E12

ADR lines 562-567. Catalog rows: r1. Stated in [Mechanisms](#mechanisms).

- **Collation.** Wadjet compares and sorts strings with BINARY collation,
  not PostgreSQL's locale-dependent collation. Locale-sensitive comparison
  costs real work on every string compare and sort, surprises analysts
  more than it helps, and no BI client depends on it. Where the oracle
  needs to agree, use a `C`-collation database rather than exempting
  string ordering.

### E56

ADR lines 2215-2326. Catalog rows: r5, r6, r7, r8, r9, r10. Stated in [Mechanisms](#mechanisms).

- **A `VARCHAR(n)` or `CHAR(n)` cast ENFORCES its `n` and still does not
  DECLARE it; a short `CHAR(n)` is not padded.** (Added 2026-09-03 as #708's
  other half; the VALUE half CLOSED by #838 on the same day, the rest open.)
  Measured live on 17.11 against the same query through the embedded API:

  | shape | PostgreSQL 17.11 | wadjet |
  |---|---|---|
  | `CAST('abcdef' AS VARCHAR(4))` | `abcd` | `abcd` |
  | `CAST('abcdef' AS CHAR(4))` | `abcd` | `abcd` |
  | `CAST('éàüxyz' AS VARCHAR(3))` | `éàü` | `éàü` |
  | `CAST('abcdef' AS VARCHAR(0))` | 22023 | 22023 |
  | `\gdesc` of the first | `character varying(4)`, atttypmod 8, OID 1043 | unconstrained STRING, OID 25 |
  | `CAST('ab' AS CHAR(4))` | `ab  ` (padded) | `ab` |
  | `LENGTH(CAST('ab' AS CHAR(4)))` | 2 | 2 |
  | `CAST('ab' AS CHAR(4)) \|\| 'x'` | `abx` | `abx` |
  | `CAST('ab' AS CHAR(4)) = 'ab'` | true | true |
  | `CREATE TABLE t (v VARCHAR(4))` | accepted, atttypmod 8 | accepted, `n` not stored |
  | `INSERT` of a too-long value into it | 22001 | accepted |

  **The order was value-first, and that is the decision this entry records.**
  Declaring `character varying(4)` while returning six characters is a worse
  lie than declaring nothing: a client that trusts the description to size a
  buffer is then wrong in the direction that overflows. `expr.Cast.Eval`
  matches the lowered type name exactly, so `varchar(4)` matched no case
  label at all and the cast reached `default: return v` — the length was
  parsed by the SQL parser and dropped. It now truncates to n CHARACTERS,
  which is what PostgreSQL counts.

  **CHAR(n) truncates and does NOT pad, deliberately.** PostgreSQL's bpchar
  pads the stored value to n and then strips trailing blanks for `length()`,
  for `||` and for every comparison — the four rows above are all measured.
  This engine has one `TypeString` and no bpchar type, so padding would put
  the blanks into GROUP BY keys, join keys and equality, where PostgreSQL
  strips them: it would buy a right rendering with a WRONG ROW SET. The
  residual is therefore the rendered value of a SHORT `CHAR(n)`, and the
  three consumer rows above are the fixtures that say why.

  **Still open: the DECLARATION.** `physical.declaredTypmod` answers only
  for DECIMAL and `pgwire.TypeMod` has no string arm, so RowDescription
  says unconstrained `text` where PostgreSQL says `character varying(4)`.
  Closing it means carrying the length beside `batch.TypeString` from the
  cast's declared type through `ColumnMeta` to `TypeMod` (n+4) and moving
  the OID to 1043 / 1042 for a length-carrying string only — a plain TEXT
  column must stay 25.

  **Still open: DDL keeps no `n`.** `parquet.ParseTypeID` used to REFUSE
  `VARCHAR(4)` outright ("unknown type"), so a PostgreSQL user's ordinary
  `VARCHAR(255)` failed the whole CREATE TABLE; it is accepted now and the
  length is dropped, which makes an INSERT past n a superset (PostgreSQL
  raises 22001) rather than a refused table.

  **An INVALID modifier is refused identically by both doors**, because
  both read it with `parquet.StringTypeLength` — one accept-set, the model
  `ParseDateDays` set. Measured live and asserted at both doors:

  | modifier | SQLSTATE | message |
  |---|---|---|
  | `VARCHAR(0)`, `CHAR(0)` | 22023 | `length for type varchar \| char must be at least 1` |
  | `VARCHAR(10485761)` | 22023 | `length for type varchar cannot exceed 10485760` |
  | `VARCHAR(abc)` | 42601 | `syntax error at or near "abc"` |
  | `VARCHAR(-1)` | 42601 | `syntax error at or near "-"` |
  | `TEXT(5)` | 42601 | `type modifier is not allowed for type "text"` |

  Two SPELLING differences on the DDL door, both recorded rather than
  fixed: it prefixes `column "v": `, and the DDL lexer folds an unquoted
  identifier to upper case before the type name is read, so `VARCHAR(abc)`
  echoes `"ABC"` there and `"abc"` from a CAST. The code and the rule are
  the same; only the echoed token's case differs, and moving it would mean
  changing when the lexer folds.

  **The DECLARATION half closed on 2026-09-04.** `physical.declaredStringLength`
  is `declaredTypmod`'s twin for a modifier that is a LENGTH rather than a
  (p,s), the answer rides to the wire through `ColumnMeta.StringLength` on
  both the single-process and the DAG path (`CollectSink.SchemaHintStringLength`
  and `Stage.OutputStringLength`, exactly as the unconstrained-DECIMAL map
  does), and `pgwire.TypeMod` sends n+4 under OID 1043. `CAST(x AS
  VARCHAR(4))` is `character varying(4)`, atttypmod 8 — PostgreSQL 17.11's
  own \gdesc.

  The first cut of that read the LENGTH as what makes a column varchar, so
  `CAST(x AS VARCHAR)` with no length declared `text` (25). It is 1043 at
  atttypmod -1 on the server, measured: the OID follows the destination
  NAME and the typmod follows the length, and conflating them made the same
  cast change its declared TYPE when its modifier was dropped. The channel
  carries all three answers now — a positive length, `character varying`
  unconstrained (`parquet.StringLengthUnconstrainedVarchar`), and `text` —
  and `parquet.VarcharNoLength` is the one place the family's spellings are
  named (round-1 review, P2). Bare `CHAR` is deliberately not in it: the
  server reads that as `character(1)` and TRUNCATES, which is part of the
  bpchar residual below rather than a declaration question.

  **What stays**: the bpchar family, as ONE residual with one mechanism.
  `CAST(x AS CHAR(n))` declares `character varying(n)` and not `character(n)`
  (OID 1042), does not PAD a short value to n, and reads bare `CHAR` as the
  unparameterized string where the server reads `character(1)`. All three are
  the same fact: this engine has one `TypeString` and no bpchar, and
  PostgreSQL's bpchar pads the stored value and then strips the blanks again
  for `length()`, for `||` and for every comparison — all three measured
  live. Padding without the stripping would move those three cells AWAY from
  the server (a wrong ROW SET for a right rendering), and declaring 1042
  would name a type whose defining behaviours are not implemented. The
  structural fix is a distinct blank-padded string type the comparison,
  length and concatenation kernels dispatch on; it is its own change.

  Pinned by `wadjet.TestStringCastEnforcesItsLengthAndStillDropsTheDeclaration`
  (the values, the declared length per destination, and the three bpchar
  consumers that assert the agreement padding would cost) and
  `pgwire.TestVarcharCastDeclaresItsLengthOnTheWire` (OID and atttypmod, with
  the unparameterized VARCHAR spellings asserted at 1043/-1, `TEXT` and a
  non-string column as the controls, and a `residual_bare_char` cell). User-facing: `docs/data-types.md`
  §`VARCHAR(n)` and `CHAR(n)`, `docs/sql-reference.md` §Casts and errors.

### E62

ADR lines 2574-2581. Stated in [Mechanisms](#mechanisms).

- **`ENCODE` takes BYTES and not text.** (Added 2026-09-18, arc EX's
  round-1 review, N6.) `encode('hi'::text, 'hex')` is
  `42883 function encode(text, unknown) does not exist` on 17.11 while
  `md5(text)`, `length(text)` and `substring(text)` all answer there — the
  asymmetry is PostgreSQL's own, so `encode`'s first position is
  `expr.ArgBytes` rather than the family's `ArgTextOrBytes`. An
  unknown-typed LITERAL still fits, because the server coerces it to bytea:
  `encode('hi','hex')` is `6869`.

### E63

ADR lines 2582-2647. Catalog rows: r11, r12, r13, r14. Stated in [Mechanisms](#mechanisms).

- **A text-only function over a BYTES argument raises 42883.** (Added
  2026-09-05, #583; CLOSED 2026-09-18, arc EX.) `upper(b)`, `lower(b)`,
  `trim(b)`, `reverse(b)`, `replace(b, ...)`, `starts_with(b, ...)`,
  `split_part(b, ...)`, `lpad(b, ...)`, `repeat(b, ...)` and `char_length(b)` /
  `character_length(b)` have no bytea overload on the server —
  `function upper(bytea) does not exist` — and this engine answered the text
  those bytes spell. They refuse now, with the server's own code and message
  shape, from a PLAN-TIME check over the argument's DECLARED type
  (`expr.RefuseUnresolvableCall`, run from the binder so every arm reaches
  one answer).

  `strpos(bytea, bytea)` is the ONE that still answers, and deliberately:
  `POSITION(sub IN b)` — which PostgreSQL DOES have over bytea — is
  rewritten into `strpos` by the parser, so refusing `strpos` over bytes
  would refuse a spelling the server answers. One spelling answering where
  the other refuses is the residue, recorded in docs/postgres-differences.md.

  `ENCODE` and `DECODE` are the bridge the entry below asked for and this
  engine now has, in all three of PostgreSQL's formats (hex, base64,
  escape), with `GET_BYTE` and `SET_BYTE` beside them.

  The functions the server DOES have over bytea agree since #583:
  `length` is the byte count — over a bare column as well as a derived
  value, which took a second pass (the vectorized `vecCharLength` counted
  runes while the scalar arm counted bytes) — `substring` is bytea indexed
  by bytes, and `bytea || bytea` is bytea under OID 17. `text || bytea` is
  TEXT there and here: the server resolves that pair through
  `text || anynonarray`, and declaring it bytea was a wrong class this arc
  briefly introduced and its review caught. The plan-time
  argument-type check the rest needed is `expr.RefuseUnresolvableCall`, run
  from the binder's own walk (`physical.refuseInvalidRowFields`), which BOTH
  `Plan` and `dagplan.PlanDistributed` reach before any stage exists — so the
  refusal is one answer for every arm, and `expr.compileFuncCallNamed` keeps
  the same call as the backstop for the doors that walk does not see
  (2026-09-18, arc EX).

  TWO value divergences ride with it, both pinned by
  `wadjet.TestByteaFunctionsAnswerInBytes`.

  `residual_concat_hex_spelled_literal`: an unknown-typed LITERAL beside a
  bytea operand of `||` contributes its own SPELLING, so `b || '\x41'`
  appends four characters where the server appends one byte. That is
  #582's rule at a function ARGUMENT rather than at a comparison.

  `residual_text_concat_bytea_value` (×3, added 2026-09-05 in round 3 —
  the round-2 cells asserted these values as PostgreSQL's, which they are
  not): where the pair is TEXT, the server RENDERS the bytea operand
  through `bytea_out` and concatenates the `\x` hex text, and this engine
  splices the raw bytes.

    t = 'hi', b = '\x6869'      PostgreSQL        wadjet
    t || b                      hi\x6869          hihi
    b || t                      \x6869hi          hihi
    length(t || b)              8                 4

  The CLASS is right on both engines (text, OID 25) — that half was fixed
  in this arc. The VALUE needs the operand's DECLARED type at the
  EVALUATOR, and the only carrier is `expr.compileContext.colTypes`, which
  is nil at every compile site with no schema: deciding there would give
  one `||` expression two answers depending on which site compiled it,
  which is the defect the #627 census closed. It is the same plan-time
  argument-type check the 42883 family above is deferred for, and it should
  land with it. On the WIRE the raw bytes go out under OID 25, so a
  non-UTF-8 or NUL-bearing value reaches a text field — #570's hazard
  through a derived value, which is why this is recorded loudly rather than
  left as a footnote.
