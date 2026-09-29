# ADR-0012 divergences: Network types

Network-native types (IPV4, IPV6, CIDR, MAC, PORT, PROTOCOL): PostgreSQL inet/macaddr/uuid input grammar, storage-domain refusals, text wire declarations. One family of the [ADR-0012 divergence catalog](README.md): the rule that decides each disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision), and the dated history of every entry is the [amendment log](../0012-amendments.md).

Columns: `cell` is the smallest statement that shows the difference; `PostgreSQL 17.11` and `this engine` are the answers; `SQLSTATE` is this engine's (`PG …` when only PostgreSQL raises); `since` names the date and the source entries (`E…` below, `P…` on the [differences page](../../postgres-differences.md)); `gate` is the test that pins the row.

## Mechanisms

**One text grammar per network type** (E55, A09)
`parquet.NetworkTextValue` is the one accept-set for IPV4, IPV6, CIDR, MAC and UUID, measured against PostgreSQL 17.11's own input functions (`inet` for the address types and CIDR, `macaddr`, `uuid`), and the writer, the CAST, the plan-time literal classifier and the runtime comparison all read it. A comparison reads inet's grammar even beside a CIDR column, because `cidr` has no operators of its own and `=` resolves through `=(inet, inet)`; `GREATEST`, `LEAST` and `COALESCE` agree with the server for the type these columns actually are, `inet` (a wadjet CIDR holds host bits, which PostgreSQL's `cidr` refuses; the differential oracle maps all three to `inet`). The classful `cidr` grammar (`'10'` is `10.0.0.0/8`) is not implemented; it belongs to `CAST(<text> AS CIDR)` beside its own measured domain. MAC grouped hex is 6+6 or 4+4+4 and nothing else; an out-of-range octet is 22003. The INSERT, COPY and UPDATE doors do not trim.

**Two classes of refusal for a bare-address column** (E61, A12, A09)
The refusal is decided at typing time for every type and site (`kernel.QuotedLitStatus`), so no evaluator choice changes it: text that names no address is 22P02 (`'zzz'`, and the maskless abbreviation `'192.168'`, which inet also refuses); PostgreSQL-valid text that an IPV4 or IPV6 column has no room for is 0A000, one class at every door, whether it names a network (`'10/8'`) or an address of the other family. A host-width prefix (`/32`, `/128`) is the address and is accepted. Reproducing the server would mean rewriting the operator per literal or carrying a prefix beside the address, which is what CIDR is.

**PORT and PROTOCOL** (A09)
Both carry int4 on the wire (OID 23) and read their own input function at every door, the comparison included (`kernel.NetworkIntLitText` over `parquet.NetworkTextValue`), so protocol names are accepted and int4's radix spellings are not. The type's range (0..65535, 0..255) is checked when a value enters the type, by CAST or by write, through `parquet.NetworkIntRangeError`, asked before the carrier's width; arithmetic keeps int4's rules and may leave the range. A CAST's operand is read by the destination's input function when its declaration is text (`expr.castOperandDeclaresText`), so `CAST(CONCAT('2','.5') AS PORT)` is 22P02 while `CAST(d + 1 AS PORT)` over a DECIMAL rounds, PostgreSQL's numeric-to-int answer.

**Unknown-typed literals in INSERT ... SELECT** (A09)
A bare quoted literal is `unknown` and is typed from the INSERT's target, as on the server (#1088). A literal reached through a UNION, a CTE or a derived table is typed by that construct first and stays 42804, which is also PostgreSQL's answer for a UNION of literals.

**A mask that is not a value of the column's type stops a CAST** (A09)
An ABAC `mask_column` obligation replaces the column's expression, so a cast over a masked column parses the mask text; a non-value mask is 22P02, PostgreSQL's answer for that text. The fix belongs at the policy boundary (an unenforceable mask), not at the cast, which must not hand a non-value back.

## Catalog

| cell | PostgreSQL 17.11 | this engine | SQLSTATE | disposition | since | issue | gate |
|---|---|---|---|---|---|---|---|
| **r1** `SELECT ip, ip6, cd, mac, port, proto, u FROM nt` | inet, inet, cidr, macaddr, integer, integer, uuid | text (OID 25) for IPV4, IPV6, CIDR and MAC; integer (23) for PORT and PROTOCOL; uuid (2950) (measured) | — | value divergence | 2026-09-15 · [E55](#e55), P084, [A09](../0012-amendments.md#a09) | #627, #834 | — |
| **r2** `SELECT CAST('192.168.5.7/24' AS CIDR)` | ERROR 22P02 invalid cidr value (host bits right of the mask) | 192.168.5.7/24: a CIDR keeps host bits, as inet does (measured) | — | kept superset | 2026-09-05 · [E55](#e55), P084 | #627 | — |
| **r3** `SELECT CAST('10' AS CIDR)` | 10.0.0.0/8 (cidr classful inference) | ERROR 22P02 invalid input syntax for type inet (measured): the CAST reads inet's grammar | 22P02 | refusal | 2026-09-15 · [E55](#e55), [A09](../0012-amendments.md#a09), P084 | #1092, #627 | `wadjet.TestEveryNetworkTypeReadsOneTextGrammarAtEveryBoundary` |
| **r4** `SELECT CAST('10/8' AS CIDR)` | 10.0.0.0/8 | 10/8: the abbreviated text is kept as written (measured); it compares equal to 10.0.0.0/8 | — | value divergence | 2026-09-05 · [E55](#e55) | #627 | — |
| **r5** `SELECT GREATEST(mac, '08:002b:010203') FROM nt` | ERROR 22P02 (grouped hex is 6+6 or 4+4+4 only) | ERROR 22P02 invalid input syntax for type macaddr at eq, IN, CASE, IS DISTINCT FROM, GREATEST and a predicate no row reaches (measured) | 22P02 | documented gap | 2026-09-05 · [E55](#e55) | #627, #579 | `coordinator.TestANetworkLiteralHasOneDispositionAtEverySite` |
| **r6** `SELECT CAST('10/8' AS IPV4)` | 10.0.0.0/8 (an inet network) | ERROR 0A000 a network prefix is not representable in an IPV4 column (measured); 10.0.0.1/32 and ::1/128 are accepted | 0A000 | refusal | 2026-09-05 · [E61](#e61), [A12](../0012-amendments.md#a12), P126 | #627, #1092 | `kernel.TestNetworkLiteralRefusalIsOnePredicate`, `physical.TestPlanTimeNeverRefusesPGValidNetworkLiteral` |
| **r7** `SELECT id FROM nt WHERE ip > '10.0.1/24'` | answers: a network orders just below its network address | ERROR 0A000 decided at typing time at every site, CASE included (measured) | 0A000 | refusal | 2026-09-05 · [E61](#e61), [A12](../0012-amendments.md#a12) | #627 | `coordinator.TestArcF3ExprTypingOnEveryArm` |
| **r8** `SELECT id FROM nt WHERE ip = '::1'` | answers (no row: an IPv6 inet never equals an IPv4 one) | ERROR 0A000 (measured; the message names a network prefix, not the other family) | 0A000 | refusal | 2026-09-15 · [A09](../0012-amendments.md#a09), [E61](#e61) | #1092 | — |
| **r9** `SELECT CAST('' AS IPV4)` | ERROR 22P02 | ERROR 22P02 at every SQL door (measured); the embedded ingester reads an empty CSV, JSON or Go-map field as NULL | 22P02 | documented gap | 2026-09-15 · [A09](../0012-amendments.md#a09) | #1092 | `wadjet.TestEveryNetworkTypeReadsOneTextGrammarAtEveryBoundary` |
| **r10** `SELECT CAST(70000 AS PORT)` | no PORT type; int4 would answer 70000 | ERROR 22003 PORT value 70000 out of range [0, 65535] (measured); CAST(-1 AS PROTOCOL) 22003; port + 70000 answers 70443 | 22003 | kept superset | 2026-09-15 · [A09](../0012-amendments.md#a09), P085 | #1092, #901 | `expr.TestAValueEnteringPortOrProtocolIsHeldToTheTypesRange`, `coordinator.TestNetworkTextGrammarAnswersTheSameOnEveryArm`, `pgwire.TestANetworkCastOnTheWire` |
| **r11** `SELECT id FROM nt WHERE proto = 'udp'` | ERROR 22P02 invalid input syntax for type integer (int4 column) | matches protocol 17, as CAST('udp' AS PROTOCOL) is 17 (measured) | — | kept superset | 2026-09-18 · [A09](../0012-amendments.md#a09), P085 | #1137 | — |
| **r12** `SELECT id FROM nt WHERE port = '0x1bb'` | matches 443 (int4 reads the radix spelling) | ERROR 22P02 invalid input syntax for type integer (measured) | 22P02 | refusal | 2026-09-18 · [A09](../0012-amendments.md#a09), P085 | #1137 | — |
| **r13** `INSERT INTO t (arr) SELECT '{1,2}'` | stores {1,2}: the unknown literal is typed from the target | ERROR 42804 column "arr" is ARRAY and a string value is not an ARRAY (measured); BOOL and BYTES targets now answer | 42804 | refusal | 2026-09-15 · [A09](../0012-amendments.md#a09) | #1088 | — |
| **r14** `SELECT CAST(ip AS IPV4) FROM t` | no analogue; a masked column cast answers only if the mask text is a value | ERROR 22P02 when an ABAC mask_column replaces ip with '***'; filed as an unenforceable-mask gap at the policy boundary | 22P02 | documented gap | 2026-09-15 · [A09](../0012-amendments.md#a09) | — | — |
| **r15** `SELECT INT_TO_IP(167772161), UUID(), IP_ADD(ip, 1) FROM nt` | analogues typed inet / uuid | text (OID 25) for all three (measured) | — | value divergence | — · P017 | #1254 | — |
| **r16** `INSERT INTO t (ip) SELECT INT_TO_IP(167772162)` | no INT_TO_IP; a text expression into inet is 42804 | stores 10.0.0.2: the call is read by the column input function (measured); CONCAT(...) is 42804 | — | kept superset | — · P017 | #1254 | — |

## Source entries

The ADR-0012 §5 entries this family was built from, verbatim as they stood at 0da8399a (line numbers are that revision's). Dated blocks that recorded a closure, withdrawal or correction moved to the [amendment log](../0012-amendments.md) and are replaced here by a pointer.

### E40

ADR lines 1488-1657. Moved to the log: [A09](../0012-amendments.md#a09).

  *(Moved to the amendment log: [A09](../0012-amendments.md#a09).)*

### E55

ADR lines 2120-2214. Catalog rows: r1, r2, r3, r4, r5. Stated in [Mechanisms](#mechanisms).

- **Abbreviated CIDR and inet literals.** (Added 2026-09-03, #627. CLOSED
  and RE-SCOPED 2026-09-05: the divergence was real, and half of what this
  entry described was the wrong grammar.)
  PostgreSQL has TWO v4 literal parsers and they are not the same language:

    inet (inet_net_pton_ipv4)   '10'        22P02
                                '10/8'      10.0.0.0/8   host bits KEPT
                                '10.1/8'    10.1.0.0/8
                                '10/16'     22P02        mask past the octets
                                '0x0a'      22P02
    cidr (inet_cidr_pton_ipv4)  '10'        10.0.0.0/8   CLASSFUL inference
                                '10.1/8'    22P02        bits right of mask
                                '0x0a'      10.0.0.0/8

  A comparison reads the INET one, even beside a `cidr` column: `cidr`
  carries no operators of its own, so `cd = '<literal>'` resolves through
  `=(inet, inet)` and the server's own message names the type it used —
  `invalid input syntax for type inet: "239"`. The cidr grammar is
  reachable only through an explicit cast, which this engine does not
  implement (`CAST(<text> AS CIDR)` passes its text through — the entry
  above), so nothing here reads it and `parquet.PgIPv4Pton` implements
  inet's grammar alone, measured cell by cell: all 256 one-octet values
  maskless (0 accepted), the 4×34 octet-count-by-mask grid, and the
  leading-zero and trailing-dot forms.

  **The FOLD sites agree too, and the round-3 entry that said otherwise
  compared this engine against a PostgreSQL type it cannot be.** (Corrected
  2026-09-05 in round 4.) `GREATEST`, `LEAST` and `COALESCE` resolve no
  operator — they UNIFY their arguments' types — so beside a `cidr` column
  the literal is read by the CIDR parser, and round 3 recorded wadjet's
  refusal of `GREATEST(cidr_col, '239')` as a divergence on that basis.

  It is not one, because a wadjet CIDR column is not a PostgreSQL `cidr`
  column and cannot be: it holds host bits under a mask, and
  `'192.168.5.7/24'::cidr` is `22P02 invalid cidr value` — the type-matrix
  fixture's own values do not fit. This repository's differential oracle
  already says so and maps IPV4, IPV6 and CIDR alike to **`inet`**
  (`benchmarks/tpch/postgres_oracle_test.go`'s postgresType: "inet (unlike
  cidr) also accepts host bits, which the net fixture deliberately
  carries"). Measured against that type, on the same server:

    column type `inet`      GREATEST(c,'239')     22P02   (wadjet 22P02)
                            LEAST(c,'239')        22P02   (wadjet 22P02)
                            COALESCE(c,'239')     22P02   (wadjet 22P02)
                            GREATEST(c,'192.168') 22P02   (wadjet 22P02)
                            GREATEST(c,'1/0')     answers (wadjet answers)
                            COALESCE(c,'zzz')     22P02   (wadjet 22P02)

  So ONE grammar at every site is not an approximation of the server here —
  it is what the server does for the type this engine's columns actually
  are. The classful reading belongs to `cidr`, a type nothing in this
  engine maps onto, and the census asserts the agreement at the fold sites
  rather than recording a divergence there.

  The engine agreed with neither parser before: it refused `'10/8'`, which
  inet accepts, and (for one commit of this arc) it accepted `'239'`, which
  no comparison on the server accepts. Both directions are closed by the
  one grammar; `kernel.TestNetworkLiteralGrammarIsPostgresInet`,
  `kernel.TestEveryFirstOctetFollowsPostgresInet`,
  `kernel.TestMaskMayNotNameAByteTheLiteralDidNotWrite` and the census in
  `coordinator.TestArcF3ExprTypingOnEveryArm` assert both sides. When
  `CAST(<text> AS CIDR)` is implemented, the classful table belongs THERE,
  beside its own measured domain — not in the comparison path.

  The MAC and UUID halves of the same issue are NOT divergences: every
  spelling PostgreSQL accepts is accepted, at every comparison site, and
  nothing more. That last clause is the part the first version of this entry
  asserted without holding — `pgMACGroupedHex` counted SEPARATORS rather
  than GROUP SIZES, so twelve hex digits with one or two separators anywhere
  parsed, and six spellings PostgreSQL refuses with 22P02
  (`0-8-002b010203`, `0:8002b010203`, `08-002b010203`, `08002b:01:0203`,
  `08:002b:010203`, `08002b:0102:03`) were answered. Measured on 17.11: the
  grouped-hex grammar is 6+6 and 4+4+4 and nothing else. The equality is
  enforced now and the six are cells in the refused half of
  `coordinator.TestANetworkLiteralHasOneDispositionAtEverySite`, rendered at
  all seven sites — but the 22P02 refusal is ASSERTED at two of them (`eq`
  and `in`, on all three arms) and the other five are pinned as ANSWERING.
  Four of those five (`case`, `is_distinct`, `greatest`, `least`) answer
  because the boxed-pair comparators reach them while the refusal lives in
  the kernel and the row-at-a-time path; the fifth (`empty_scan`) answers for
  a different reason — the refusal is per ROW, so a predicate no row reaches
  never raises (the gate states both, at
  `arc_a2_network_literal_two_path_test.go:154-158` and `:62-64`). That
  residual is what #579 recorded before it closed COMPLETED on 2026-08-28,
  and **#627 is the open tracker that carries it**: its body names the same
  data-dependent runtime refusal (`exec/filter.go`'s `networkConstError`),
  states that #579's original defect "is therefore still open for the
  network types", and prescribes the unification — plan-time and runtime on
  the same predicate, with a guard test asserting the disposition "at every
  site (WHERE =, IS DISTINCT FROM, IN, simple CASE, GREATEST/LEAST)", which
  is this entry's site list. So this entry, the gate's pins and #627 are one
  record, and it fires when #627 closes. A fixture at every site, a refusal asserted
  at two: that distinction is the same one this entry's first version lost,
  and a bound with no fixture is how the defect survived review.

### E61

ADR lines 2531-2573. Catalog rows: r6, r7, r8. Stated in [Mechanisms](#mechanisms). Moved to the log: [A12](../0012-amendments.md#a12).

- **A network PREFIX has no place in an IPV4 or IPV6 column.** (Added
  2026-09-05, #627.) `'10/8'`, `'192.168/16'` and `'2001:db8::/64'` are
  ordinary `inet` values on the server — a NETWORK, which it compares
  FALSE against every host address and orders just below its own network
  address. This engine's IPV4 and IPV6 hold a bare address with no room
  for a prefix, so such a literal is refused with `0A000` and a message
  naming the type as the limit, rather than with the `22P02` that would
  call PostgreSQL-valid text an input-syntax error.

  Reproducing the server exactly would mean rewriting the OPERATOR per
  literal (`=` becomes never-true, `>` becomes `>= network`) at every
  comparison site, or carrying a prefix beside the address in the vector —
  which is what the CIDR type already is, and CIDR answers these literals
  exactly. A HOST-width prefix (`'10.0.0.1/32'`, `'::1/128'`) IS the
  address on the server and is accepted here.

  *(Moved to the amendment log: [A12](../0012-amendments.md#a12).)*
