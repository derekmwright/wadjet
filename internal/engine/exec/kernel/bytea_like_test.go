// SPDX-License-Identifier: MIT

package kernel

import "testing"

// TestByteaLikeMatchesPostgreSQL pins the bytea matcher (#1593) on shapes
// measured on PostgreSQL 17.11: the backslash is the escape, a lone trailing
// backslash is 22025 only where the matcher reaches it, % and _ are bytes.
func TestByteaLikeMatchesPostgreSQL(t *testing.T) {
	for _, c := range []struct {
		s, p string
		want bool
		err  string
	}{
		{`\A`, `\A`, false, ""},
		{`A`, `\A`, true, ""},
		{`%`, `\%`, true, ""},
		{`AB`, `\%`, false, ""},
		{`_`, `\_`, true, ""},
		{`\`, `\\`, true, ""},
		{`\\`, `\\`, false, ""},
		{``, `\`, false, ""},
		{`A`, `\`, false, "22025"},
		{`A`, `%\`, false, "22025"},
		{``, `%\`, false, ""},
		{`A`, `A\`, false, ""},
		{`A\`, `A\`, false, "22025"},
		{"\x80", `_`, true, ""},
		{"\x80", `%`, true, ""},
		{"\x00", `_`, true, ""},
		{"A\x00", `A%`, true, ""},
		{``, ``, true, ""},
		{`A`, ``, false, ""},
		{``, `%`, true, ""},
		{``, `_`, false, ""},
		{`AB`, `%_`, true, ""},
		{`AB`, `_B`, true, ""},
	} {
		got, err := ByteaLike([]byte(c.s), []byte(c.p))
		state := ""
		if err != nil {
			state = "22025"
		}
		if got != c.want || state != c.err {
			t.Errorf("%q LIKE %q: got (%v, %q), want (%v, %q)", c.s, c.p, got, state, c.want, c.err)
		}
	}
}

// TestByteaLikeEscapeMatchesPostgreSQL pins like_escape(bytea, bytea).
func TestByteaLikeEscapeMatchesPostgreSQL(t *testing.T) {
	for _, c := range []struct {
		pat, esc, want string
		err            bool
	}{
		{`!A`, `!`, `\A`, false},
		{`\A`, `!`, `\\A`, false},
		{`!\A`, `!`, `\\A`, false},
		{`A!`, `!`, `A\`, false},
		{`\A`, `\`, `\A`, false},
		{`\A`, ``, `\\A`, false},
		{`A%`, ``, `A%`, false},
		{`A`, `!!`, ``, true},
	} {
		got, err := ByteaLikeEscape([]byte(c.pat), []byte(c.esc))
		if (err != nil) != c.err || string(got) != c.want {
			t.Errorf("like_escape(%q, %q): got (%q, %v), want (%q, err=%v)", c.pat, c.esc, got, err, c.want, c.err)
		}
	}
}
