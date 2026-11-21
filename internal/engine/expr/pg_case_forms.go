// SPDX-License-Identifier: MIT

package expr

// The engine's ONE case mapping (#1209): the C locale's, which is the
// reference installation's (PostgreSQL 17.11, `initdb --locale=C
// --encoding=UTF8`) and the collation this engine declares (it compares text
// by its bytes and refuses any COLLATE other than C). Under it PostgreSQL maps
// ASCII A-Z and a-z and leaves every other character as it is: lower(),
// upper(), ILIKE (`~~*`) and the regex case-insensitive forms (`~*`, the i
// flag, (?i)) all take this mapping, so 'É' ILIKE 'é', 'É' ~* 'é' and
// lower('É') = 'é' are false there. Measured over U+0001..U+1FFFF
// (testdata/pg17_case_pairs.csv: only the 52 ASCII letters map; and none of
// the 2,690 non-ASCII pairs an en_US.utf8 server folds matches under `~*` or
// ILIKE on the reference).
//
// lower / upper (fnLower, fnUpper, vecLower, vecUpper), ILIKE (the parser
// rewrites it to lower(x) LIKE lower(p)) and the regex translation
// (caseForms) all read these functions and no other; Unicode case mapping is
// a property of a collation (pg_c_utf8, ICU), which arrives with collation
// support, not here.

// cLowerByte is c lowered under the C locale: A-Z to a-z, every other byte
// (each byte of a multi-byte UTF-8 character included, which is never in
// A-Z) unchanged.
func cLowerByte(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

// cUpperByte is c upper-cased under the C locale: a-z to A-Z, every other
// byte unchanged.
func cUpperByte(c byte) byte {
	if c >= 'a' && c <= 'z' {
		return c - ('a' - 'A')
	}
	return c
}

// cLowerBytes lowers b in place under the C locale; cUpperBytes upper-cases
// it. The column kernels (vecLower, vecUpper) and the string forms below run
// these loops, so a value folds to the same bytes whichever path evaluates it.
func cLowerBytes(b []byte) {
	for j, c := range b {
		if c >= 'A' && c <= 'Z' {
			b[j] = c + ('a' - 'A')
		}
	}
}

func cUpperBytes(b []byte) {
	for j, c := range b {
		if c >= 'a' && c <= 'z' {
			b[j] = c - ('a' - 'A')
		}
	}
}

// cLowerString is s with every ASCII upper-case letter lowered; s itself when
// it holds none.
func cLowerString(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'A' && c <= 'Z' {
			b := []byte(s)
			cLowerBytes(b[i:])
			return string(b)
		}
	}
	return s
}

// cUpperString is s with every ASCII lower-case letter upper-cased; s itself
// when it holds none.
func cUpperString(s string) string {
	for i := 0; i < len(s); i++ {
		if c := s[i]; c >= 'a' && c <= 'z' {
			b := []byte(s)
			cUpperBytes(b[i:])
			return string(b)
		}
	}
	return s
}

// caseForms is r's lower- and upper-case forms under the C locale: an ASCII
// letter's two cases, and r itself twice for every other character.
func caseForms(r rune) (lower, upper rune) {
	if r < 0x80 {
		return rune(cLowerByte(byte(r))), rune(cUpperByte(byte(r)))
	}
	return r, r
}
