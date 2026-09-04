package kernel

import "encoding/hex"

// ByteaLiteral reads PostgreSQL's `byteain` — the input function every
// unknown-typed literal beside a bytea column goes through on the server — and
// returns the BYTES the text denotes (#582).
//
// byteain takes two spellings and this takes the same two:
//
//	\x6869        the HEX form, which is what bytea_output produces
//	hi\000\\x     the ESCAPE form: \\ is one backslash, \ooo one octal byte,
//	              and every other byte stands for itself
//
// So the ordinary spelling is unchanged — `b_val = 'hi'` still names the two
// bytes 0x68 0x69, which is what the whole bytea corpus compares by — and
// `b_val = '\x6869'` names those same two bytes instead of the six characters
// of its own spelling, which is what it named before: the row was never found,
// on any path, while the server found it.
//
// ok=false is text byteain itself refuses (a lone backslash, a bad hex digit,
// an octal escape past one byte). PostgreSQL raises 22P02 for those, and the
// caller decides whether it can.
func ByteaLiteral(s string) ([]byte, bool) {
	if len(s) >= 2 && s[0] == '\\' && (s[1] == 'x' || s[1] == 'X') {
		raw, err := hex.DecodeString(s[2:])
		if err != nil {
			return nil, false
		}
		return raw, true
	}
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != '\\' {
			out = append(out, s[i])
			continue
		}
		if i+1 < len(s) && s[i+1] == '\\' {
			out = append(out, '\\')
			i++
			continue
		}
		if i+3 < len(s) && isOctalByte(s[i+1]) && isOctalByte(s[i+2]) && isOctalByte(s[i+3]) {
			v := (int(s[i+1]-'0') << 6) | (int(s[i+2]-'0') << 3) | int(s[i+3]-'0')
			if v > 0xFF {
				return nil, false
			}
			out = append(out, byte(v))
			i += 3
			continue
		}
		return nil, false
	}
	return out, true
}

func isOctalByte(c byte) bool { return c >= '0' && c <= '7' }

// ByteaConstText is ByteaLiteral for a comparison CONSTANT, as the string the
// bytes-comparison kernels hold. A literal byteain refuses keeps its own
// spelling rather than becoming an error here: this is the arm every BYTES
// filter reads, the refusal belongs at the site that can raise it, and a
// silent empty constant would match nothing while `<>` matched everything.
func ByteaConstText(s string) string {
	if raw, ok := ByteaLiteral(s); ok {
		return string(raw)
	}
	return s
}
