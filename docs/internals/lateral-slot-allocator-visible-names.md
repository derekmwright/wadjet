# Lateral slot allocator visible names

Source: internal/planner/logical/builder.go — lateralScopeNames, moved 2026-09-11 (#1026)

lateralScopeNames lists every name a LATERAL subquery's own text binds, for
seeding the slot allocator that publishes its correlation key.

It is what this layer can see without a catalog: the select items' aliases
and column references, and the GROUP BY terms. A STORED column named
`__key_0` is not in it — reading is not minting, so the reservation does not
refuse such a column (ADR-0012) and only the allocator's seed could step off
it. This layer cannot see one: it runs before AnnotateScanColumns supplies
the inner relation's schema.

It does not have to. For a stored `__key_0` to reach the lateral's OUTPUT
and meet the minted one, the SELECT list has to carry it, and there are only
two ways:

  - it NAMES the column — which puts it in the seed above, so the allocator
    steps to `__key_1` (verified by review over a catalog-door fixture);
  - it is a STAR — and `lateralSelectsColumn` reports a star as publishing
    the key already, so nothing is injected and no slot is minted at all.

Any other list does not publish the stored column, so the two never share an
output batch. The gap is in what this function CAN SEE, not in what can
collide.
