# ADR-0012 divergence catalog

Every place this engine deliberately answers differently from PostgreSQL 17.11, one file per family, one table row per cell. The rule that decides a disposition is [ADR-0012 §5](../0012-sql-semantics-authority.md#decision); the dated history is the [amendment log](../0012-amendments.md); the user-facing rendering of these rows is the [differences page](../../postgres-differences.md). A new divergence is a new row here, a row on the differences page when its disposition is `kept superset` or `value divergence`, and an entry in the log. A row is cited as `<family>#r<N>`; row ids are stable, and a retired id is not reused.

| family | rows | covers |
|---|---|---|
| [Comparisons and membership](comparison-membership.md) | 17 | Comparisons and IN/ANY/EXISTS memberships across text and typed values, numeric literals in memberships, and bind-time type refusals. |
| [Temporal types](temporal.md) | 24 | Timestamp resolution, infinity, clock zone, integer and text casts to DATE/TIMESTAMP, date/time arithmetic and the INTERVAL declaration. |
| [Numbers and DECIMAL](numeric-decimal.md) | 18 | Numeric and DECIMAL divergences: integer widening, float4 rounding positions, decimal carrier limits, casts and operator spellings PostgreSQL declines. |
| [Set operations](set-operations.md) | 9 | UNION, INTERSECT and EXCEPT: VECTOR widths, declared decimal and integer carriers, refused type pairs and literals, and ORDER BY qualification. |
| [Containers (ARRAY, ROW, MAP, VECTOR)](containers.md) | 23 | ROW, ARRAY and MAP containers: field-path spellings, declared OIDs, nested-array semantics, container ordering and array casts. |
| [DML, DDL and assignment](dml-assignment.md) | 22 | Assignment rounding and typing on every write door, DDL and CTAS result forms, and the DML and DDL statements that are refused. |
| [Parameters and the wire](parameters-pgwire.md) | 4 | The wire protocol: multi-statement strings without transactions, PORT/PROTOCOL/DURATION wire types, and syntax errors without a POSITION field. |
| [Names, scopes, joins and stars](names-scopes.md) | 27 | Name resolution and scoping: identifier case folding, storable names, published output names, column-alias lists, USING merges, star expansion and duplicate-name handling. |
| [LATERAL and subqueries](lateral-subqueries.md) | 21 | Correlated subqueries and LATERAL joins: shapes the per-row re-run or the join decorrelation cannot express are refused loudly; star naming over laterals. |
| [Recursion](recursion.md) | 12 | Recursive CTEs: an iteration-bounded eager fixed point, WITH RECURSIVE list ordering, and the seed-typed recursive terms PostgreSQL refuses. |
| [Aggregates and windows](aggregates-windows.md) | 19 | Aggregate and window-function divergences: accepted extra types and argument types, declared result types, and refusals of window forms PostgreSQL answers. |
| [Text, bytes and collation](text-collation.md) | 24 | String collation, pattern matching, VARCHAR/CHAR length and declaration, the missing blank-padded type, and text functions over bytes. |
| [Network types](network.md) | 16 | Network-native types (IPV4, IPV6, CIDR, MAC, PORT, PROTOCOL): PostgreSQL inet/macaddr/uuid input grammar, storage-domain refusals, text wire declarations. |
| [Extension functions](extensions.md) | 33 | Functions PostgreSQL lacks (TIME_BUCKET, OHLCV, TCP flags, bitwise helpers, semver): each names its PostgreSQL spelling or its external authority, and where it departs. |
| [Table functions and readers](table-functions.md) | 15 | File, database and series readers in FROM, and set-returning functions in SELECT: when their columns are known, what they refuse, and COPY's grammar. |
| [Access control, catalog and other](other.md) | 16 | Casts to unknown or unconverted types, array casts and comparisons, reserved column names, catalog contents and access-filtered metadata. |

## Conservation

This catalog replaced the prose of ADR-0012 §5 (lines 57–4351 at 0da8399a). The split was mechanical: the §5 text divides into 111 entries — 93 bullets (`E01`–`E93`), the section header (`E00`) and 17 dated amendment blocks (`A01`–`A17`) — and every line of §5 sits in exactly one of them. Every entry id reappears as a catalog row's source, a family's mechanism paragraph, or an amendment-log entry; the [differences page](../../postgres-differences.md)'s 178 entries (158 of the original 159 kept, `P023` retired, 20 added) each map to a row too. Rows out: 291. Rows added since the split are new ids, each with a dated line in the log: [temporal](temporal.md) r24 and [aggregates-windows](aggregates-windows.md) r18–r20 (2026-09-29) and r21–r22 (2026-10-02); counted at 2026-10-02 the catalog holds 303 rows and the page 185 entries. The evidence is `inventory.tsv` beside this page and the arc DS conservation check.
