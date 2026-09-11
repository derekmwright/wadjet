# Cast destination name acceptance

Source: internal/engine/expr/cast_dest.go — var castOnlyDestTypes = map[string]bool{, moved 2026-09-11 (#1026)

A CAST to a type name this engine does not have is 42704, not a STRING
column over the operand (#652).

`CAST(1 AS bogustype)` answered the string "1" under DataTypeOID 25, and
`CAST(c_f64 AS bogustype)` answered "0.3333333333333333" the same way:
Cast.Eval's switch fell to `default: return v` and physical.inferCastType's
fell to `default: return TypeString`, so the two layers agreed with each
other about a column PostgreSQL says cannot be described at all — the
#310/#443 shape, a numeric VALUE published under a STRING declaration.
PostgreSQL 17.11 raises `type "bogustype" does not exist` at parse time.

The accept-set is deliberately the UNION of the two doors:

  - every name parquet.ParseTypeID takes, which is what a CREATE TABLE
    column type may be. `CAST(x AS ARRAY(INT))` is not implemented by
    Cast.Eval and passes its operand through, and that stays exactly as it
    was: this pass refuses names that name NOTHING, not casts that are
    unimplemented.
  - the PostgreSQL spellings Cast.Eval implements that the DDL door has no
    column type for — `int4`, `smallint`, `real`, `double precision`,
    `signed`, `timestamptz` and the rest of the list below.

  - the PASS-THROUGH names, below: PostgreSQL type names this engine has no
    type for, whose text it nevertheless hands back UNCHANGED and whose
    unchanged text is what the server answers.

The union means the two doors agree about which NAMES exist, which is the
property #838's both-doors gate already states for the length modifier: one
type name, one disposition.

What is REFUSED is a name that answers to nothing at all, plus the
PostgreSQL names whose value this engine gets WRONG — `bytea`, `money`,
`inet`. Those three are a refusal the server does not make, and they are
recorded in ADR-0012's divergence list: the alternative is a value under a
text declaration that is not the server's (`abc` for `\x616263`, `1.5` for
`$1.50`, `192.168.1.1` for `192.168.1.1/32`). Loud beats plausible there and
only there.
