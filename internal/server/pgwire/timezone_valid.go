// SPDX-License-Identifier: MIT

package pgwire

import (
	"math"
	"strconv"
	"strings"
)

// pgTimeZoneByLowerName is pgTimeZoneNames keyed by lower case: PostgreSQL
// finds a zone file without regard to case (`SET TimeZone =
// 'AMERICA/NEW_YORK'` is America/New_York).
var pgTimeZoneByLowerName = func() map[string]bool {
	m := make(map[string]bool, len(pgTimeZoneNames))
	for _, n := range pgTimeZoneNames {
		m[strings.ToLower(n)] = true
	}
	return m
}()

// validTimeZone reports whether PostgreSQL 17.11 accepts v as a TimeZone
// setting (check_timezone): a number of hours (strtod over the whole value,
// leading white space allowed, magnitude under 168), the name of a zone it
// ships (any case), or a POSIX zone specification as its tzparse reads one.
// A value it rejects is its 22023 `invalid value for parameter "TimeZone"`
// (`Not/AZone`, `Z`, `abc`, `PST`, `1000`, `inf`, a value with surrounding
// white space after a name), and so is an offset with nonzero seconds ("time
// zone … appears to use leap seconds", the same 22023). Measured on
// postgres:17-alpine, #1680 line d; the cells are in
// TestSetTimeZoneRefusesEveryZoneButUTC.
func validTimeZone(v string) bool {
	if f, err := strconv.ParseFloat(strings.TrimLeft(v, " \t\n\v\f\r"), 64); err == nil {
		return !math.IsNaN(f) && !math.IsInf(f, 0) && math.Abs(f) < 168
	}
	if pgTimeZoneByLowerName[strings.ToLower(v)] {
		return true
	}
	return posixTimeZone(v)
}

// posixTimeZone reports whether v is a POSIX time zone specification
// PostgreSQL's tzparse accepts: STD offset [DST [offset] [,rule,rule]].
// STD may be empty (PostgreSQL allows it, unlike IEEE: `+05:30`, `5:30`
// are accepted), a run of characters other than digits, `,`, `+` and `-`,
// or a `<…>`-quoted name; the offset is [+-]h[h…][:m[m][:s[s]]] with hours
// under 168. DST, when present, is a non-empty name; without a rule it takes
// the default rule.
func posixTimeZone(v string) bool {
	if strings.HasPrefix(v, ":") {
		return false
	}
	rest, ok := tzName(v, true)
	if !ok || rest == "" {
		return false
	}
	if rest, ok = tzOffset(rest); !ok {
		return false
	}
	if rest == "" {
		return true
	}
	if rest, ok = tzName(rest, false); !ok {
		return false
	}
	if rest != "" && rest[0] != ',' && rest[0] != ';' {
		if rest, ok = tzOffset(rest); !ok {
			return false
		}
	}
	if rest == "" {
		return true
	}
	if rest[0] != ',' && rest[0] != ';' {
		return false
	}
	if rest, ok = tzRule(rest[1:]); !ok || rest == "" || rest[0] != ',' {
		return false
	}
	rest, ok = tzRule(rest[1:])
	return ok && rest == ""
}

// tzName consumes a zone abbreviation: `<…>` quoted, or a run of characters
// other than digits, `,`, `+`, `-`. An empty run is allowed only for STD.
func tzName(s string, allowEmpty bool) (string, bool) {
	if strings.HasPrefix(s, "<") {
		i := strings.IndexByte(s, '>')
		if i < 0 {
			return "", false
		}
		return s[i+1:], true
	}
	i := strings.IndexAny(s, "0123456789,+-")
	if i < 0 {
		i = len(s)
	}
	if i == 0 && !allowEmpty {
		return "", false
	}
	return s[i:], true
}

// tzOffset consumes [+-]hours[:minutes[:seconds]]. Nonzero seconds in a
// zone offset are refused: PostgreSQL rejects such a zone as one that
// "appears to use leap seconds" (a rule's transition time may carry them).
func tzOffset(s string) (string, bool) { return tzTime(s, false) }

func tzTime(s string, secondsOK bool) (string, bool) {
	if s != "" && (s[0] == '+' || s[0] == '-') {
		s = s[1:]
	}
	s, _, ok := tzNum(s, 0, 167)
	if !ok {
		return "", false
	}
	if strings.HasPrefix(s, ":") {
		if s, _, ok = tzNum(s[1:], 0, 59); !ok {
			return "", false
		}
		if strings.HasPrefix(s, ":") {
			var sec int
			if s, sec, ok = tzNum(s[1:], 0, 60); !ok || (sec != 0 && !secondsOK) {
				return "", false
			}
		}
	}
	return s, true
}

// tzRule consumes one DST transition rule: Jn (1–365), n (0–365) or
// Mm.w.d (month 1–12, week 1–5, day 0–6), with an optional /offset time.
func tzRule(s string) (string, bool) {
	var ok bool
	switch {
	case strings.HasPrefix(s, "J"):
		s, _, ok = tzNum(s[1:], 1, 365)
	case strings.HasPrefix(s, "M"):
		if s, _, ok = tzNum(s[1:], 1, 12); ok && strings.HasPrefix(s, ".") {
			if s, _, ok = tzNum(s[1:], 1, 5); ok && strings.HasPrefix(s, ".") {
				s, _, ok = tzNum(s[1:], 0, 6)
			} else {
				ok = false
			}
		} else {
			ok = false
		}
	default:
		s, _, ok = tzNum(s, 0, 365)
	}
	if !ok {
		return "", false
	}
	if strings.HasPrefix(s, "/") {
		return tzTime(s[1:], true)
	}
	return s, true
}

// tzNum consumes one or more decimal digits naming a number in [lo, hi].
func tzNum(s string, lo, hi int) (string, int, bool) {
	n, i := 0, 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		n = n*10 + int(s[i]-'0')
		if n > hi {
			return "", 0, false
		}
		i++
	}
	if i == 0 || n < lo {
		return "", 0, false
	}
	return s[i:], n, true
}
