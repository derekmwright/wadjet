# Parquet network literal write contract

Source: internal/storage/parquet/file_writer.go — func convertNetworkLiteral(colType TypeID, s string) (any, error) {, moved 2026-09-11 (#1026)

convertNetworkLiteral turns a text literal into the binary form its column
is defined to hold: an int64 for IPV4 and MAC, sixteen bytes for IPV6 and
UUID. It has three outcomes, and the two that are not "it converted" are
the point.

There used to be one. Every converter answered garbage with a zero value:
ipv4StringToInt64 and macStringToInt64 returned 0, so "zz" landed in a MAC
column as 00:00:00:00:00:00, indistinguishable from an address somebody
meant; ipv6StringToBytes returned nothing; and convertStringToBytes stored
an unparseable UUID as THE RAW STRING BYTES, so "not-a-uuid" became ten
bytes in a column whose entries are sixteen. That last one produced a file
wadjet WROTE that wadjet's own row reader then refused — "UUID is 16 bytes
per value but row 2 holds 10" — while the native columnar reader read it.
One file, two paths, two answers, and the row path is the one compaction
and ANALYZE run on.

PostgreSQL decides what a bad literal means (ADR-0012) and there it is an
error: `invalid input syntax for type uuid`. So a literal that parses
converts, and anything else is an error naming the column, the row and the
literal.

The empty literal is the third outcome: it is an absence, and it is written
as NULL. "" is the one input for which "a value" has no stable meaning here
— stored as a value it is a zero-length entry in a fixed-width column,
which the row reader called an error and the columnar reader called a
value, and which answers false to IS NULL and equal to the empty string
when what was meant was that there is no address. The readers hold the
other end of this contract: a zero-length entry in an IPV6 or UUID column
reads back as NULL on both paths (reader.go unpackAllPresent /
unpackWithNulls, scan/columnar_native.go).
