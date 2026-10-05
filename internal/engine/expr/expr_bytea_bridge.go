// SPDX-License-Identifier: MIT

package expr

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/derekmwright/wadjet/internal/engine/exec/kernel"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// encode / decode are the BRIDGE between the text family and the bytes family,
// and #583 named their absence: the text-only functions had to refuse over
// BYTES, which leaves a user who wants to SEE those bytes with nothing to call.
// `to_hex`/`from_hex` are integer-only and are not that bridge.
//
// Measured on PostgreSQL 17.11 over `'\xfffe0041'::bytea`:
//
//	encode(b,'hex')     fffe0041         text
//	encode(b,'base64')  //4AQQ==         text
//	encode(b,'escape')  \377\376\000A    text  (a NUL and every non-printable
//	                                            byte as a three-digit octal
//	                                            escape, backslash doubled)
//	decode('6869','hex')     \x6869      bytea
//	decode('aGk=','base64')  \x6869      bytea
//	decode('hi','escape')    \x6869      bytea
//
// An unrecognized encoding NAME is `22023 unrecognized encoding: "zzz"` on both
// functions; bad hex input is 22023 (`invalid hexadecimal digit: "z"`) and bad
// escape input 22P02 (`invalid input syntax for type bytea`), the server's own
// SQLSTATE and wording, from the one bytea reading (kernel.ByteaIn's halves).

func fnEncode(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	raw := byteaArg(args[0])
	switch byteaEncodingName(args[1]) {
	case "hex":
		return hex.EncodeToString(raw)
	case "base64":
		return base64.StdEncoding.EncodeToString(raw)
	case "escape":
		return byteaEscapeEncode(raw)
	}
	raiseUnrecognizedEncoding(toString(args[1]))
	return nil
}

func fnDecode(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	in := toString(args[0])
	switch byteaEncodingName(args[1]) {
	case "hex":
		// PostgreSQL's hex_decode, the same function byteain's `\x` form
		// reads, so whitespace between digit pairs is skipped and a bad digit
		// is named (#1501's one reading).
		out, err := kernel.ByteaHexDecode(in)
		if err != nil {
			panic(fatalEval{err})
		}
		return out
	case "base64":
		out, err := base64.StdEncoding.DecodeString(in)
		if err != nil {
			raiseInvalidEncodedInput("invalid symbol in base64 input", in)
		}
		return out
	case "escape":
		out, err := kernel.ByteaEscapeDecode(in)
		if err != nil {
			panic(fatalEval{err})
		}
		return out
	}
	raiseUnrecognizedEncoding(toString(args[1]))
	return nil
}

// fnGetByte reads ONE byte as a number, and refuses an index outside the
// value — `2202E index 5 out of valid range, 0..1` on the server, which is a
// different class from 22P02 and names the bound. Answering NULL for it would
// make a scan past the end look like a NULL byte.
//
// The result is an INT32, because `pg_typeof(get_byte('hi'::bytea,0))` is
// `integer` on 17.11 and the wire declares what Ret says: RetInt64 put OID 20
// on a column the server declares 23, beside three siblings that do declare
// PostgreSQL's type (round-1 review, P1). A byte is 0..255 and always fits.
func fnGetByte(args []any) any {
	if len(args) < 2 || args[0] == nil || args[1] == nil {
		return nil
	}
	raw := byteaArg(args[0])
	i := ToInt64(args[1])
	if i < 0 || i >= int64(len(raw)) {
		raiseByteaIndexOutOfRange(i, len(raw))
	}
	return int32(raw[i])
}

// fnSetByte writes ONE byte and answers the new value. The byte is taken mod
// 256, which is what the server does: `set_byte('hi',0,300)` is `\x2c69` there,
// 300 & 0xff = 44 = 0x2c. The INDEX is still bounded.
func fnSetByte(args []any) any {
	if len(args) < 3 || args[0] == nil || args[1] == nil || args[2] == nil {
		return nil
	}
	raw := byteaArg(args[0])
	i := ToInt64(args[1])
	if i < 0 || i >= int64(len(raw)) {
		raiseByteaIndexOutOfRange(i, len(raw))
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	out[i] = byte(ToInt64(args[2]) & 0xff)
	return out
}

// byteaArg is the bytea argument of encode / get_byte / set_byte, PostgreSQL's
// own bytea functions: a BYTES value is its bytes, and text — a quoted literal,
// which the server coerces to bytea — is read by byteain, the one reading of a
// text as bytea (kernel.ByteaIn; #1501, arc BY round 2). It was toBytes, the
// extension functions' reader, which takes an even-length hex text as the
// bytes it spells and anything else as its characters: `encode('6869', 'hex')`
// answered 6869 where PostgreSQL answers 36383639, and `encode('\x6869',
// 'hex')` 5c7836383639 where it answers 6869. The packet, payload, DNS, TLS
// and JA3 functions keep toBytes: they are this engine's, and their contract
// is the hex text a capture tool writes.
func byteaArg(v any) []byte {
	switch tv := v.(type) {
	case []byte:
		return tv
	case string:
		raw, err := kernel.ByteaIn(tv)
		if err != nil {
			panic(fatalEval{err})
		}
		return raw
	}
	return toBytes(v)
}

func byteaEncodingName(v any) string {
	return strings.ToLower(strings.TrimSpace(toString(v)))
}

//go:noinline
func raiseUnrecognizedEncoding(name string) {
	panic(fatalEval{sqlerr.New("22023", "unrecognized encoding: %s", sqlerr.Quote(name))})
}

//go:noinline
func raiseInvalidEncodedInput(what, input string) {
	panic(fatalEval{sqlerr.New("22023", "%s: %s", what, sqlerr.Quote(input))})
}

//go:noinline
func raiseByteaIndexOutOfRange(i int64, n int) {
	panic(fatalEval{sqlerr.New("2202E", "index %d out of valid range, 0..%d", i, n-1)})
}

// byteaEscapeEncode is PostgreSQL's `escape` output: a printable, non-backslash
// byte as itself, a backslash doubled, everything else as a three-digit OCTAL
// escape. Measured: `'\xfffe0041'` is `\377\376\000A`.
func byteaEscapeEncode(raw []byte) string {
	var b strings.Builder
	for _, c := range raw {
		switch {
		case c == '\\':
			b.WriteString(`\\`)
		case c >= 0x20 && c <= 0x7e:
			b.WriteByte(c)
		default:
			fmt.Fprintf(&b, `\%03o`, c)
		}
	}
	return b.String()
}
