# Dml literal conversion type boundary

Source: wadjet/dml.go — func convertValue(s string, typ parquet.TypeID) (any, error) {, moved 2026-09-11 (#1026)
Superseded: DURATION now uses parquet.ParseDurationNanos via convertTemporalValue rather than integer-only parsing; the implementation is split into convertValue and convertUnquoted.

convertValue's default case (return the trimmed, unquoted string as-is)
is exactly right for six of the fourteen types that have no explicit case
below, so they are deliberately left to it rather than given a
pass-through case that would say nothing extra:

  - TypeBytes: ingest.checkType accepts a string for BYTES, and the
    writer's toBytes/convertStringToBytes takes the string's raw bytes.
  - TypeIPv4, TypeIPv6, TypeMAC, TypeCIDR, TypeUUID: the writer's
    decomposeLeaf/convertNetworkLiteral (file_writer.go) is the
    authoritative text→binary conversion for these — it already runs on
    whatever string reaches it, validates the literal, and raises a
    descriptive error for a bad one (ADR-0012: PostgreSQL decides what an
    invalid literal means, and there it is an error). Converting here too
    would either duplicate that logic or race two different validators
    over the same literal; TypeCIDR stores its text form directly and
    needs no conversion either way.
  - TypeDecimal: the literal's TEXT is the exact carrier and is passed
    through unchanged. Reading it into a number here would be wrong twice
    over: this function is handed the column's TypeID and nothing else, so
    it does not know the (p, s) the value has to land at, and an integer
    literal converted to an int64 box would then be read as the ALREADY-
    UNSCALED value ADR-0018 §4 defines (INSERT 5 into DECIMAL(9,2) would
    store 0.05, not 5.00). parquet.DecimalValueFromBox, at the leaf where
    the declared (p, s) is known, is the one checked converter — it parses
    the text exactly, rounds to the column's scale as PostgreSQL does on
    assignment, and raises 22003/22P02 rather than storing a wrapped int64
    or a zero (#647).

TypePort and TypeProtocol (BUG: INSERT into either always failed) and
TypeDuration (BUG: silently wrote 0 — see below) are NOT in that set:
their writer-side converters only accept already-numeric Go values
(writer.go's prepareRows int/int32/float64 switch has no string case for
any of the three, and file_writer.go's convertStringToInt64 — the network
string→int64 path decomposeLeaf delegates to — only knows TypeIPv4 and
TypeMAC), so a bare string reaching them is silently read as int64(0) by
toInt64's default arm. There is also no established literal syntax
anywhere in the system (parser, ingest, or a named-form registration like
Port/Protocol never got) beyond a plain integer for these three, so that
is the form parsed here — matching checkType's accepted Go types
(int/int32/int64/...) and TypeDuration's schema.go contract ("nanoseconds,
stored as int64").

TypeArray, TypeRow and TypeMap have no case: the INSERT VALUES parser
itself (dml_parser.go's insertValueText) accepts only a single literal
token per value — an array/row/map literal is a composite expression it
explicitly refuses ("Anything else is an EXPRESSION, and this path has no
evaluator"), so convertValue never receives one. Supporting them would
start at the parser grammar, not here.

TypeVector was in that list and did not belong: a vector literal's text
form is `'[1,2]'`, a single QUOTED STRING token, which the parser accepts
like any other. So convertValue did receive one, the default arm passed the
text through, and the writer wrote the string's bytes into a fixed-width
leaf — a corrupt page for every VECTOR insert, right width or wrong. It has
a case now (parseVectorLiteral), and the width is checked in
columnChecked where the declaration is.
