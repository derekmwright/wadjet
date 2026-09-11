# Typematrix network extra literal shapes

Source: internal/oracle/typematrix/typematrix.go — var networkExtraLit = []struct{ col, suffix, op, lit string }{, moved 2026-09-11 (#1026)

networkExtraLit adds the literal SHAPES the equality/ordering pair above
cannot reach, one per column, as (suffix, operator, literal) triples.

Each is a shape #492's first fix answered differently on the two engines,
and none of them is exotic:

  - c_cidr against a BARE address. PostgreSQL's inet reads "172.16.2.187"
    as "172.16.2.187/32" and so does wadjet now; the kernel used to answer
    an unparseable-literal sentinel that matched NOTHING while the
    row-at-a-time path compared the text, so `WHERE c_cidr = '<a bare
    address the fixture holds>'` answered zero rows on one engine and the
    row on the other. `>=` is the ordering half of the same literal.
  - c_cidr against a HOST-BEARING prefix. Two rows share the /24 network
    and differ only in host bits, which the masked-network key erased.
  - c_ipv6 against a v4-shaped literal. Different FAMILIES: PostgreSQL puts
    every v4 address below every v6 one, so every non-NULL row is `>` it.
    The kernel read the literal as its v4-MAPPED v6 bytes (mid-range) and
    the expr path fell through to a lexical text compare — two engines,
    two answers, neither PostgreSQL's.

The literal shape that is NOT here is the one that must ERROR — `c_cidr <>
'garbage'`, which raised zero rows through the scan and every row through
the row evaluator before #492's second pass. A corpus entry cannot carry it:
oracle.Run fails the whole run on any query error, so an entry whose correct
answer IS an error would read as a broken corpus. It lives in
wadjet.TestNonAddressLiteralAgainstACidrColumnIsAQueryError instead, where
both sites are asserted to raise the same SQLSTATE.
