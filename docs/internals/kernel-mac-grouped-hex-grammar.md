# Kernel mac grouped hex grammar

Source: internal/engine/exec/kernel/compare.go — func pgMACGroupedHex(s string) ([]byte, bool) {, moved 2026-09-11 (#1026)
Superseded: The implementation accepts three additional grouped spellings, not two; INET abbreviation support now lives in parquet.PgIPv4Pton and does not use CIDR classful inference.

pgMACGroupedHex reads the two macaddr spellings PostgreSQL accepts and Go's
net.ParseMAC does not (#627).

PostgreSQL takes six spellings for one address; Go's parser takes four of
them (`xx:xx:xx:xx:xx:xx`, `xx-xx-...`, the dotted `xxxx.xxxx.xxxx`, and the
same in upper case). The two it does not are the ones that group the twelve
hex digits into halves:

	08002b:010203      a colon between two 6-digit groups
	08002b-010203      a hyphen between them
	0800-2b01-0203     three 4-digit groups (Go takes `0800.2b01.0203`, not this)

This is a VALUE-PRESERVING widening: every spelling names the same six
bytes, and the address a query means does not depend on which one the user
typed. It is #627's half that ships; the abbreviated CIDR/inet grammar is
its own decision, because PostgreSQL's abbreviation is CLASSFUL address
inference (`'10'` is 10.0.0.0/8 and `'192.168'` is 192.168.0.0/24) and
reproducing inet_net_pton bit-exactly is a different size of change.

The digits must be exactly twelve hexadecimal characters AND the separators
must split them 6+6 or 4+4+4, which is the whole of PostgreSQL's grouped-hex
grammar; the size check below carries the measurement. A string Go rejected
for a real reason is still rejected here.
