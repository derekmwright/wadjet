# Type specific literal refusal

Source: internal/planner/physical/validate_literal.go — refuseLiteralForType, moved 2026-09-11 (#1026)

refuseLiteralForType raises when text names no value of a column type that
has a plan-time literal rule.

It is the WHOLE numeric family now — DECIMAL (#517), the integer types
(#536) and the FLOAT types (#646) — through the one predicate
expr.RefuseNumericLiteral, which is kernel.QuotedLitStatus, which is what
the vectorized kernel, the row-at-a-time evaluator and the boxed sites all
read. The plan-time refusal and the runtime one CANNOT disagree about which
strings name a value, because they are the same function; that identity is
the property, not the coverage.

The rule is per type because PostgreSQL's input functions are:

	'3.1'    bigint 22P02   real 3.1     numeric 3.1
	'1_000'  bigint 1000    real 22P02   numeric 1000 (wadjet: 22P02, #634)
	'0x1p3'  bigint 22P02   real 8       numeric 16   (wadjet: 22P02, #634)
	'NaN'    bigint 22P02   real NaN     numeric NaN-as-a-bound (ADR-0024 item 6)
	'1e400'  bigint 22P02   real 22003   numeric a very large number

all verified live on postgres:17-alpine. A range failure is 22003, a
different SQLSTATE with different wording, so the error type carries the
distinction rather than collapsing it.

The NETWORK types (CIDR/IPv4/IPv6/MAC/UUID) are NOT wired here yet:
wadjet's network literal parsers (net.ParseCIDR, net.ParseMAC, the
brace-unaware UUID parser) are STRICTER than PostgreSQL's input grammar —
they reject abbreviated cidr/inet ('192.168', '10/8'), several macaddr
notations ('08002b:010203', '0800-2b01-0203') and the brace/no-dash/
uppercase UUID forms that PostgreSQL ACCEPTS. Refusing on those parsers
here would raise 22P02 for input PostgreSQL answers — a PG-superset
regression the binder must never make (ADR-0012 item 1: never refuse what
PostgreSQL accepts), net-new at the boxed sites (GREATEST/LEAST, simple
CASE, IN, IS DISTINCT FROM) that had no refusal before. That
over-strictness is a latent RUNTIME bug too (exec.networkConstError refuses
the same PG-valid forms data-dependently), and both halves are deferred to
#627: widen the parsers to a SUPERSET of PostgreSQL's grammar first, then a
network arm can be added here using that same predicate without ever
refusing a PG-valid literal.

Types with no rule return nil: a legal comparison must still work, and the
binder refuses only what PostgreSQL refuses.
