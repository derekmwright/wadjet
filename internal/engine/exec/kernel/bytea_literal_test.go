// SPDX-License-Identifier: MIT

package kernel

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestByteaInIsPostgresByteain holds the one bytea reading to byteain's
// grammar as PostgreSQL 17.11 answers it: every row is `SELECT
// encode('<in>'::bytea, 'hex')` there (by_author/pgcells.txt), the accepted
// and the refused forms, whitespace, case and trailing characters included.
func TestByteaInIsPostgresByteain(t *testing.T) {
	for _, c := range []struct {
		in, hex, state, msg string
	}{
		// The hex form.
		{in: `\x6869`, hex: "6869"},
		{in: `\xC3A9`, hex: "c3a9"},
		{in: `\x`, hex: ""},
		{in: `\x68`, hex: "68"},
		{in: `\x 68 69`, hex: "6869"},
		{in: `\x68 69`, hex: "6869"},
		{in: "\\x68\t69", hex: "6869"},
		{in: "\\x68\n69", hex: "6869"},
		{in: "\\x68\r69", hex: "6869"},
		{in: "\\x\t6869", hex: "6869"},
		{in: `\x6869 `, hex: "6869"},
		{in: `\x68  69`, hex: "6869"},
		{in: "\\x6869\n", hex: "6869"},
		{in: `\x686`, state: "22023", msg: "invalid hexadecimal data: odd number of digits"},
		{in: `\x 6`, state: "22023", msg: "invalid hexadecimal data: odd number of digits"},
		{in: `\x68 6`, state: "22023", msg: "invalid hexadecimal data: odd number of digits"},
		{in: `\x68zz`, state: "22023", msg: `invalid hexadecimal digit: "z"`},
		{in: `\xG0`, state: "22023", msg: `invalid hexadecimal digit: "G"`},
		{in: `\x6869z`, state: "22023", msg: `invalid hexadecimal digit: "z"`},
		{in: `\x68\x69`, state: "22023", msg: `invalid hexadecimal digit: "\"`},
		{in: `\x6 869`, state: "22023", msg: `invalid hexadecimal digit: " "`},
		{in: `\x 6 `, state: "22023", msg: `invalid hexadecimal digit: " "`},
		{in: "\\x68\v69", state: "22023", msg: "invalid hexadecimal digit: \"\v\""},
		{in: "\\x68\f69", state: "22023", msg: "invalid hexadecimal digit: \"\f\""},
		{in: `\x68é`, state: "22023", msg: `invalid hexadecimal digit: "é"`},
		// Upper-case X and a leading space are the ESCAPE form, where a
		// backslash before anything but a backslash or three octal digits
		// is 22P02.
		{in: `\X6869`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: ` \x6869`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `abc\x6869`, state: "22P02", msg: "invalid input syntax for type bytea"},
		// The escape form.
		{in: ``, hex: ""},
		{in: `hi`, hex: "6869"},
		{in: `69`, hex: "3639"},
		{in: `é`, hex: "c3a9"},
		{in: `a\\b`, hex: "615c62"},
		{in: `\\`, hex: "5c"},
		{in: `a\134b`, hex: "615c62"},
		{in: `a\000b`, hex: "610062"},
		{in: `a\377b`, hex: "61ff62"},
		{in: `a\3777`, hex: "61ff37"},
		{in: `\101`, hex: "41"},
		{in: `a\b`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `a\`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `\`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `a\400b`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `a\12b`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `a\08b`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `a\12`, state: "22P02", msg: "invalid input syntax for type bytea"},
		{in: `a\x`, state: "22P02", msg: "invalid input syntax for type bytea"},
	} {
		got, err := ByteaIn(c.in)
		if c.state != "" {
			if err == nil {
				t.Errorf("ByteaIn(%q) = %x; PostgreSQL 17.11 refuses %s %s", c.in, got, c.state, c.msg)
				continue
			}
			if st := sqlerr.StateOf(err); st != c.state || err.Error() != c.msg {
				t.Errorf("ByteaIn(%q) refused %s %q; PostgreSQL 17.11: %s %q", c.in, st, err.Error(), c.state, c.msg)
			}
			continue
		}
		if err != nil {
			t.Errorf("ByteaIn(%q) refused %v; PostgreSQL 17.11: %s", c.in, err, c.hex)
			continue
		}
		if h := hex.EncodeToString(got); h != c.hex {
			t.Errorf("ByteaIn(%q) = %s; PostgreSQL 17.11: %s", c.in, h, c.hex)
		}
	}
	// A 1 MiB hex text is 512 KiB of bytes.
	big, err := ByteaIn(`\x` + strings.Repeat("ab", 512*1024))
	if err != nil || len(big) != 512*1024 {
		t.Errorf("1 MiB hex text: %d bytes, %v", len(big), err)
	}
}
