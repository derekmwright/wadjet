# Auth relation attribute comparison

Source: internal/auth/abac_eval.go — var relationAttributes = map[string]bool{, moved 2026-09-11 (#1026)

relationAttributes are the attributes whose value is a RELATION NAME rather
than an opaque string, and so are compared under the engine's IDENTIFIER
rule instead of byte-exactly.

A relation name has two legitimate spellings for one relation: the catalog's
(`Hits`, the spelling a parquet dataset or an Iceberg import brings) and the
one an unquoted reference arrives in, which the lexer folded (#731). The
query planner reconciles them with `catalog.ResolveTableName`, so a
statement naming `Hits` and a statement naming `hits` read the SAME table —
and a policy scoped to that relation has to bind to both, or it binds to
neither door consistently.

It did not. `resource.name` went through the generic attribute comparator
(`compareEq`, `fmt.Sprintf("%v")`), which is byte-exact, so with a catalog
table `Hits` and a rule scoped `resource.name eq "hits"` the rule did not
match — and an unmatched scoped rule is not a REFUSAL. The broad allow that
every `roles:`-to-ABAC migration emits still matched, so `Evaluate` returned
Allowed with NO obligations: the masked column came back in plaintext, the
denied column came back at all, and a DML predicate on the masked column
became a working oracle for its own stored value (#882, ADR-0033 rule 2).
The legacy `PolicySet` had the same hole in the opposite direction, so on a
CamelCase table there was no single spelling an operator could write that
bound on both paths.

The fix is not to fold `compareEq` — `eq` must stay byte-exact for ordinary
attributes, or a rule reading `classification eq "SECRET"` would start
matching `"secret"` and quietly widen every clearance in the file. Only the
attributes that carry an IDENTIFIER get the identifier rule.
