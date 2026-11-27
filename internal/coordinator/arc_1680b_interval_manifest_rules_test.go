// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// The stage manifest's rules, round 3 (#1680b; the second judge record,
// 1680b_codex_judge_r2.md, corrections 2 and 6):
//
//   - the refusal outcome checks the NAMED reason (iv80Reasons' sentence
//     patterns), so an unrelated 0A000, or an internal failure presented as
//     0A000, earns nothing;
//   - the internal-failure exclusion (panic, uncoded error, XX000, the two
//     store guards, a runtime error's sentence) is applied before ANY outcome,
//     PostgreSQL's own error included;
//   - a representative divergence is checked against an explicit rule-3
//     answer per cell (iv80RepSpecs): the printed key is an eligible input
//     member, the whole winning tuple, ascending; the latitude covers only
//     the key or MIN/MAX columns the spec names, never SUM/AVG beside them;
//     types are checked;
//   - unordered results compare as multisets, ordered ones positionally;
//   - each cell is owned by the EARLIEST stage at which everything it needs is
//     admitted (iv80EarliestStage), every refusal by A (it is enforced from A
//     onward; F audits).

// --- The named refusals --------------------------------------------------

// iv80Reasons are the named capability refusals: each an identity the engine
// emits with its 0A000 (sqlerr.Refuse; recorded by iv80Err between the
// SQLSTATE and the sentence) and what it covers. A refusal outcome is met by
// the identity alone (iv80Meets): the sentence is the client's and may be
// reworded; no word pattern over it credits anything (#1680c, the third
// judge: `time-type` matched a timestamptz sentence and
// `function-date_trunc` any sentence that named date_trunc).
var iv80Reasons = map[string]string{
	"bare-interval-result":           "a query result column of type interval (ruling 1: no wire change)",
	"intervalstyle":                  "SET IntervalStyle other than postgres (ruling 7)",
	"typmod":                         "an interval typmod: field qualifier or precision (ruling 7)",
	"timestamptz":                    "timestamptz arithmetic or a timestamptz operand (ruling 7; ruling 8)",
	"time-type":                      "a TIME / TIMETZ operand (no TIME type: ADR-0012 temporal r23; ruling 8)",
	"container":                      "an interval inside ARRAY / ROW / MAP or array_agg",
	"ordered-aggregate":              "an ordered or ordered-set aggregate over an interval (ruling 4)",
	"distinct-aggregate":             "sum / avg (DISTINCT interval) (ruling 5)",
	"numeric-infinity":               "an EXTRACT whose numeric result is infinite (ruling 6)",
	"timestamp-millisecond-carrier":  "a timestamp value or result with sub-millisecond digits (TIMESTAMP is a millisecond carrier, E36)",
	"interval-write":                 "storing an interval: INSERT / UPDATE / MERGE / CTAS (no persistent interval)",
	"function-date_trunc":            "date_trunc over an interval (its own item)",
	"function-to_char":               "to_char over an interval (its own item)",
	"function-generate_series":       "generate_series with an interval step (its own item)",
	"function-overlaps":              "OVERLAPS with an interval end (its own item)",
	"function-width_bucket":          "width_bucket over intervals (its own item)",
	"range-offset-frame":             "a RANGE frame with an interval offset (offset RANGE frames are refused for every type today)",
	"postgresql-internal-function":   "interval_hash / interval_hash_extended (PostgreSQL's internal hash support)",
	"char-display":                   "an interval cast to CHAR(n) / bpchar: PostgreSQL declares OID 1042 and blank-pads to n; this engine declares CHAR as 1043 and does not pad (pgColumnOID), so only TEXT and VARCHAR are display types (ruling 1, addition (a))",
	"timestamp-difference-to-number": "a timestamp difference compared with or written as a number",
	"implicit-text-conversion":       "an interval converted to text implicitly: || concatenation (ADR-0052 section 5; the third judge)",
	"interval-lineage-unresolved":    "a consumer of a slot whose interval lineage the planner cannot resolve (ADR-0052 section 8: unresolved lineage is a loud refusal, never a pass)",
	// The rulings on 1680c r3 (the earliest truthful rule):
	"timestamp-date-difference": "timestamp - date / date - timestamp, an interval in PostgreSQL the engine does not compute (the binder's named refusal)",
	"interval-extract":          "EXTRACT over an interval argument (the interim's refusal; stage C admits it)",
	"interval-opaque-value":     "an interval value the engine cannot hold or apply, decided at run time from the row (the interim's refusal)",
	// The rulings on 1680c r4 (rule 4: an untruthful code at an existing
	// raise point becomes the named refusal; rule 2: the earliest truthful
	// rule):
	"function-age":            "age(...), a function PostgreSQL has and this engine does not (raised where unknown function was; age is not declared an interval producer)",
	"interval-input-form":     "an INTERVAL literal PostgreSQL accepts in a form the literal reader cannot read (raised where 22007 was, at the parse)",
	"interval-numeric-factor": "interval * / a NUMERIC value (raised where operator does not exist: interval * text was)",
	"dml-values-subquery":     "a subquery in an INSERT ... VALUES expression (the write door's statement-shape refusal)",
	"dml-set-subquery":        "a subquery in an UPDATE's SET list (the write door's statement-shape refusal)",
	"merge-on-equality":       "a MERGE ON that is not an equality between two columns (the write door's statement-shape refusal)",
}

// iv80InternalFailure: an engine answer no outcome may accept, decided by
// the error's PROVENANCE as the recorder read it off the error's type chain
// (iv80Err), never by words in its sentence (#1680c; the third judge): a
// panic, an error without a SQLSTATE (the batch store's guards are such
// errors), XX000, or a code attached by a layer that did not raise it
// (sqlerr.Attached: `unsupported shuffle type: 255` under 0A000 is one).
func iv80InternalFailure(engine string) bool {
	if strings.HasPrefix(engine, "PANIC") {
		return true
	}
	if !strings.HasPrefix(engine, "ERR ") {
		return false
	}
	code, _ := iv80ErrCode(engine)
	return code == "" || code == "XX000" || iv80ErrIdentity(engine) == iv80Attached
}

// --- Answers: types, rows, order ------------------------------------------

// iv80Ordered: the statement's top level states ORDER BY (an ORDER BY inside
// a subquery or a window does not order the result).
func iv80Ordered(sql string) bool {
	return len(iv80TopLevel(sql, " ORDER BY ")) > 1
}

// iv80TiedOrder are statements whose top-level ORDER BY leaves ties PostgreSQL
// broke arbitrarily, each with the result columns (0-based) that are the
// LEADING sort keys visible in the result: rows compare ordered between
// distinct values of those columns and as a multiset within one (a tie
// there), never wholly unordered (#1680c; the third judge). Checked by hand.
var iv80TiedOrder = map[string][]int{
	// ROLLUP: (1, k, 3) and (1, NULL, 3) tie on g, min(id), n; so do (4, k, 2)
	// and (4, NULL, 2).
	"SELECT g, CAST(CAST(s AS INTERVAL) AS TEXT) AS k, count(*) AS n FROM iv80_r GROUP BY ROLLUP (g, CAST(s AS INTERVAL)) ORDER BY g NULLS LAST, min(id), n": {0},
}

// iv80SameAnswer: the engine's answer has PostgreSQL's column names, types
// and rows: in order when ordered, as a multiset otherwise.
func iv80SameAnswer(engine, pg string, ordered bool) bool {
	return iv80SameAnswerTies(engine, pg, ordered, nil)
}

// iv80SameAnswerTies is iv80SameAnswer for an ordered result whose order
// leaves ties: ordered between distinct values of the ties columns, a
// multiset within each run of equal values (nil ties: wholly ordered).
func iv80SameAnswerTies(engine, pg string, ordered bool, ties []int) bool {
	pg, _ = iv80Notice(pg)
	if engine == "ok" || pg == "ok" {
		return engine == pg
	}
	if strings.HasPrefix(engine, "ERR ") || strings.HasPrefix(pg, "ERR ") || !strings.Contains(engine, " | ") {
		return false
	}
	if ivTypeNames(ivHeaderTypes(engine)) != iv80PGTypes(pg) {
		return false
	}
	eh, eb, _ := strings.Cut(ivNormalize(engine), " | ")
	ph, pb, _ := strings.Cut(iv80PGValue(pg), " | ")
	if eh != ph {
		return false
	}
	er, pr := strings.Split(eb, "; "), strings.Split(pb, "; ")
	if len(er) != len(pr) {
		return false
	}
	if !ordered {
		er, pr = append([]string(nil), er...), append([]string(nil), pr...)
		sort.Strings(er)
		sort.Strings(pr)
	} else if len(ties) > 0 {
		er, pr = iv80SortWithinTies(er, ties), iv80SortWithinTies(pr, ties)
		for i := range er {
			if iv80TieKey(er[i], ties) != iv80TieKey(pr[i], ties) {
				return false
			}
		}
	}
	for i := range er {
		if er[i] != pr[i] {
			return false
		}
	}
	return true
}

// iv80TieKey is a row's values at the ties columns.
func iv80TieKey(row string, ties []int) string {
	f := strings.Split(row, ",")
	var k []string
	for _, i := range ties {
		if i < len(f) {
			k = append(k, f[i])
		}
	}
	return strings.Join(k, ",")
}

// iv80SortWithinTies sorts each run of consecutive rows with one tie key,
// keeping the runs in their order.
func iv80SortWithinTies(rows []string, ties []int) []string {
	out := append([]string(nil), rows...)
	for i := 0; i < len(out); {
		j := i + 1
		for j < len(out) && iv80TieKey(out[j], ties) == iv80TieKey(out[i], ties) {
			j++
		}
		sort.Strings(out[i:j])
		i = j
	}
	return out
}

// --- Representative expectations -----------------------------------------

// iv80RepSpec is a representative-divergence cell's rule-3 answer (the
// architect's ruling 3: among SQL-equal members, the minimum under
// ascending lexicographic (months, days, micros), over the WHOLE key tuple,
// eligible members only, the whole winning tuple kept).
//
//	units   the column groups (0-based) whose printed value is a
//	        representative; one unit is one key tuple (a GROUP BY or DISTINCT
//	        key) or one MIN / MAX column. Every other column must equal
//	        PostgreSQL's exactly.
//	members per unit, the eligible input tuples, as PostgreSQL prints them
//	        (","-joined for a multi-column unit); for INTERSECT / EXCEPT the
//	        left arm's only (ADR-0024 §12 item 4).
//	rows    the answer's rows under rule 3 (types and names are PostgreSQL's).
//
// A spec without units is a value computed from a representative
// (reparith: TIMESTAMP '2024-01-31' + the rule-3 member 30 days).
// TestArc1680bRepExpectations proves every spec against its PostgreSQL
// answer: same row count, other columns equal, unit columns SQL-equal, each
// printed unit tuple an eligible member and the minimum of its SQL-equal
// members.
type iv80RepSpec struct {
	units   [][]int
	members [][]string
	rows    string
}

var (
	iv80MembersO = []string{"1 mon", "30 days", "720:00:00", "1 mon -30 days", "00:00:00", "-1 days", "-24:00:00", "00:00:00.000001", "-00:00:00.000001",
		"1 year", "360 days", "213503982 days 08:01:49.551616", "106751991 days 04:00:54.775808", "-178956970 years -8 mons", "178956970 years 7 mons",
		"infinity", "-infinity", "29 days 23:59:59.999999"}
	iv80MembersK = []string{"1 mon", "30 days", "720:00:00", "1 mon -30 days", "00:00:00", "-1 days", "-24:00:00", "1 day", "24:00:00",
		"213503982 days 08:01:49.551616", "106751991 days 04:00:54.775808", "1 year", "360 days"}
	iv80MembersR   = []string{"1 mon", "30 days", "720:00:00", "1 mon -30 days", "00:00:00", "infinity", "-infinity", "-1 days", "-24:00:00", "1 day 02:00:00", "1 year -00:00:01"}
	iv80MembersPM  = []string{"1 day", "infinity", "-infinity", "2 days"}
	iv80Members3   = []string{"1 mon", "30 days", "720:00:00"}
	iv80MembersBig = []string{"30 days", "720:00:00", "1 day", "2 days", "3 days"}
	iv80MembersDup = []string{"1 mon", "30 days"}
)

func iv80Unit(cols []int, members []string) ([][]int, [][]string) {
	return [][]int{cols}, [][]string{members}
}

func iv80MinMax(lo, hi int, members []string) iv80RepSpec {
	return iv80RepSpec{units: [][]int{{lo}, {hi}}, members: [][]string{members, members}}
}

func iv80RepSpecs() map[string]iv80RepSpec {
	out := map[string]iv80RepSpec{}
	one := func(key string, cols []int, members []string, rows string) {
		u, m := iv80Unit(cols, members)
		out[key] = iv80RepSpec{units: u, members: m, rows: rows}
	}
	mm := func(key string, lo, hi int, members []string, rows string) {
		s := iv80MinMax(lo, hi, members)
		s.rows = rows
		out[key] = s
	}
	// 03_order_cast: unordered min / max over SQL-tied extremes.
	mm("03_order:order/minMax~text", 0, 1, iv80MembersO, "-infinity,infinity")
	mm("03_order:order/minMaxFinite~text", 0, 1, iv80MembersO, "-178956970 years -8 mons,178956970 years 7 mons")
	mm("03_order:order/minTieFwd~text", 0, 1, iv80Members3, "720:00:00,720:00:00")
	mm("03_order:order/minTieRev~text", 0, 1, iv80Members3, "720:00:00,720:00:00")
	mm("03_order:order/minTieTable~text", 0, 1, iv80Members3, "720:00:00,720:00:00")
	// 04_keys_cast: GROUP BY over iv80_k / iv80_kr (the same rows reversed).
	groupK := "720:00:00,3,1; 00:00:00,3,4; -1 days,2,7; NULL,2,9; 24:00:00,2,11; 213503982 days 08:01:49.551616,1,13; 106751991 days 04:00:54.775808,1,14; 360 days,3,15"
	for _, tbl := range []string{"iv80_k", "iv80_kr"} {
		for _, p := range []string{"default", "sortagg", "hashagg"} {
			one("04_keys:group/"+tbl+"/"+p+"~text", []int{0}, iv80MembersK, groupK)
		}
	}
	// 07_functions_cast: min / max beside count, sum, avg (exact).
	mm("07_functions:agg/all~text", 1, 2, []string{"1 mon", "-1 days +02:00:00", "00:00:00.000001"},
		"3,-1 days +02:00:00,1 mon,1 mon -1 days +02:00:00.000001,10 days -07:20:00")
	// 11_relational.
	grp := "720:00:00,3; 00:00:00,2; NULL,1; infinity,1; -infinity,1; -1 days,2; 1 day 02:00:00,1; 1 year -00:00:01,1"
	for _, p := range []string{"default", "sortagg"} {
		one("11_relational:grp/text/"+p, []int{0}, iv80MembersR, grp)
		one("11_relational:grp/derived/"+p, []int{0}, iv80MembersR, grp)
		// ORDER BY k is the output column: the text, bytewise (C collation).
		one("11_relational:distinct/text/"+p, []int{0}, iv80MembersR,
			"NULL; -1 days; -infinity; 00:00:00; 1 day 02:00:00; 1 year -00:00:01; 720:00:00; infinity")
		pairs := []string{"1 mon,24:00:00", "30 days,1 day"}
		one("11_relational:tuple/distinct/"+p, []int{0, 1}, pairs, "30 days,1 day")
		one("11_relational:tuple/distinctRev/"+p, []int{0, 1}, pairs, "30 days,1 day")
		one("11_relational:tuple/group/"+p, []int{0, 1}, pairs, "30 days,1 day,2")
		one("11_relational:tuple/table/"+p, []int{0, 1}, []string{"1 mon,24:00:00", "30 days,1 day", "720:00:00,24:00:00", "30 days,1 mon"},
			"720:00:00,24:00:00,3,1; 30 days,1 mon,1,4")
		mix := []string{"1 mon,1.0,0", "30 days,1.00,-0", "720:00:00,1.000,0"}
		one("11_relational:mixkey/group/"+p, []int{0, 1, 2}, mix, "720:00:00,1.000,0,3")
		one("11_relational:mixkey/distinct/"+p, []int{0, 1, 2}, mix, "720:00:00,1.000,0")
		out["11_relational:reparith/"+p] = iv80RepSpec{rows: "2024-03-01 00:00:00"}
		out["11_relational:reparithRev/"+p] = iv80RepSpec{rows: "2024-03-01 00:00:00"}
		// Set operations: a left-arm member (ADR-0024 §12 item 4).
		one("11_relational:set/intersect/smallBig/"+p, []int{0}, []string{"1 mon"}, "1 mon")
		one("11_relational:set/intersect/bigSmall/"+p, []int{0}, iv80MembersBig, "720:00:00")
		one("11_relational:set/intersectAll/smallBig/"+p, []int{0}, []string{"1 mon"}, "1 mon")
		one("11_relational:set/intersectAll/bigSmall/"+p, []int{0}, iv80MembersBig, "720:00:00")
		one("11_relational:set/intersectAll/dupRight/"+p, []int{0}, iv80MembersDup, "30 days; 30 days")
		one("11_relational:set/exceptAll/dupRight1/"+p, []int{0}, iv80MembersDup, "30 days; 30 days")
		one("11_relational:set/except/bigSmall/"+p, []int{0}, iv80MembersBig, "1 day; 2 days; 3 days")
	}
	mm("11_relational:ordagg/unordered", 0, 1, iv80MembersDup, "30 days,30 days")
	one("11_relational:gsets/sets", []int{0}, iv80MembersR,
		"NULL,1; -infinity,1; 1 day 02:00:00,1; 1 year -00:00:01,1; infinity,1; -1 days,2; 00:00:00,2; 720:00:00,3; NULL,12")
	one("11_relational:gsets/rollup", []int{1}, iv80MembersR,
		"1,720:00:00,3; 1,NULL,3; 2,00:00:00,2; 2,NULL,3; 2,NULL,1; 3,infinity,1; 3,NULL,2; 3,-infinity,1; 4,-1 days,2; 4,NULL,2; 5,1 day 02:00:00,1; 5,NULL,2; 5,1 year -00:00:01,1; NULL,NULL,12")
	// MIN per group (the classes do not span groups of iv80_r).
	one("11_relational:sub/correlatedMin", []int{1}, iv80MembersR,
		"1,720:00:00; 2,720:00:00; 3,720:00:00; 4,00:00:00; 5,00:00:00; 6,00:00:00; 7,-infinity; 8,-infinity; 9,-1 days; 10,-1 days; 11,1 day 02:00:00; 12,1 day 02:00:00")
	// pm/*: sum and avg exact; min / max representatives.
	mm("11_relational:pm/null", 3, 4, iv80MembersPM, "1 day,1 day,1,1 day,1 day")
	mm("11_relational:pm/posInf", 3, 4, iv80MembersPM, "infinity,infinity,2,1 day,infinity")
	mm("11_relational:pm/negInf", 3, 4, iv80MembersPM, "-infinity,-infinity,2,-infinity,2 days")
	mm("11_relational:pm/posInfTwice", 3, 4, iv80MembersPM, "infinity,infinity,2,infinity,infinity")
	mm("11_relational:pm/groups", 4, 5, iv80MembersPM,
		"1,1 day,1 day,1,1 day,1 day; 2,infinity,infinity,2,1 day,infinity; 3,-infinity,-infinity,2,-infinity,2 days; 5,NULL,NULL,0,NULL,NULL; 6,infinity,infinity,2,infinity,infinity")
	return out
}

// iv80RepAnswer is PostgreSQL's answer with its rows replaced by the spec's
// (header, names and types kept).
func iv80RepAnswer(pg string, spec iv80RepSpec) string {
	pg, _ = iv80Notice(pg)
	head, _, _ := strings.Cut(pg, " | ")
	return head + " | " + spec.rows
}

// iv80Triple reads PostgreSQL's postgres-style interval output into its
// fields (the infinities as the reserved triples).
func iv80Triple(s string) (months, days, micros int64, ok bool) {
	switch s {
	case "infinity":
		return 2147483647, 2147483647, 9223372036854775807, true
	case "-infinity":
		return -2147483648, -2147483648, -9223372036854775808, true
	}
	toks := strings.Fields(s)
	if len(toks) == 0 {
		return 0, 0, 0, false
	}
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if strings.Contains(t, ":") {
			neg := strings.HasPrefix(t, "-")
			t = strings.TrimLeft(t, "+-")
			hms := strings.Split(t, ":")
			if len(hms) != 3 {
				return 0, 0, 0, false
			}
			h, e1 := strconv.ParseInt(hms[0], 10, 64)
			m, e2 := strconv.ParseInt(hms[1], 10, 64)
			sec, frac, _ := strings.Cut(hms[2], ".")
			sv, e3 := strconv.ParseInt(sec, 10, 64)
			if e1 != nil || e2 != nil || e3 != nil {
				return 0, 0, 0, false
			}
			us := int64(0)
			if frac != "" {
				v, err := strconv.ParseInt((frac + "000000")[:6], 10, 64)
				if err != nil {
					return 0, 0, 0, false
				}
				us = v
			}
			total := ((h*60+m)*60+sv)*1000000 + us
			if neg {
				total = -total
			}
			micros += total
			continue
		}
		if i+1 >= len(toks) {
			return 0, 0, 0, false
		}
		n, err := strconv.ParseInt(toks[i], 10, 64)
		if err != nil {
			return 0, 0, 0, false
		}
		switch strings.TrimSuffix(toks[i+1], "s") {
		case "year":
			months += 12 * n
		case "mon":
			months += n
		case "day":
			days += n
		default:
			return 0, 0, 0, false
		}
		i++
	}
	return months, days, micros, true
}

// iv80RepLess: tuple a precedes tuple b under rule 3 (interval columns by
// (months, days, micros); any other column by its text, which never decides
// in the corpus's specs: the interval column comes first in each).
func iv80RepLess(a, b []string) bool {
	for i := range a {
		am, ad, au, aok := iv80Triple(a[i])
		bm, bd, bu, bok := iv80Triple(b[i])
		switch {
		case aok && bok:
			if am != bm {
				return am < bm
			}
			if ad != bd {
				return ad < bd
			}
			if au != bu {
				return au < bu
			}
		case a[i] != b[i]:
			return a[i] < b[i]
		}
	}
	return false
}

// iv80SQLEqualText: two printed values are SQL-equal (intervals by N,
// numbers by value, otherwise the same text).
func iv80SQLEqualText(a, b string) bool {
	if a == b {
		return true
	}
	if na, ok := iv80ParsePGInterval(a); ok {
		if nb, ok := iv80ParsePGInterval(b); ok {
			return na.Cmp(nb) == 0
		}
	}
	fa, ea := strconv.ParseFloat(a, 64)
	fb, eb := strconv.ParseFloat(b, 64)
	return ea == nil && eb == nil && fa == fb
}

// --- Stage assignment by rule ---------------------------------------------

// iv80Stage order: A (guards) < C (inline) < D (columns, keys) < E
// (aggregates, windows). F owns no cell: it audits the publication policy
// that A enforces.
var iv80StageRank = map[string]int{"A": 0, "C": 1, "D": 2, "E": 3}

// iv80EarliestStage is the earliest stage at which everything a cell needs
// has been admitted, read from its SQL (iv80Needs).
func iv80EarliestStage(sql string) string {
	needs := iv80Needs(sql)
	switch {
	case needs["E"] != "":
		return "E"
	case needs["D"] != "":
		return "D"
	}
	return "C"
}

var (
	iv80CallRe   = regexp.MustCompile(`^([a-z_]+)\((.*)\)$`)
	iv80IdentRe  = regexp.MustCompile(`^(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)$`)
	iv80TsAtomRe = regexp.MustCompile(`^(?:TIMESTAMP '[^']*'|DATE '[^']*'|(?:[a-z]\.)?ts|(?:[a-z]\.)?d|ts[0-9])$`)
	iv80AggRe    = regexp.MustCompile(`\b(min|max|sum|avg|count|mode|array_agg|string_agg|percentile_cont|percentile_disc|first_value|last_value|nth_value|lag|lead|bool_and)\(`)
	iv80ValuesRe = regexp.MustCompile(`^\(VALUES (.*)\) (?:AS )?[a-z_][a-z0-9_]*\(([^)]*)\)`)
)

// iv80Strip removes one pair of enclosing parentheses.
func iv80Strip(e string) string {
	e = strings.TrimSpace(e)
	for strings.HasPrefix(e, "(") && strings.HasSuffix(e, ")") && iv80Close(e, 0) == len(e)-1 {
		e = strings.TrimSpace(e[1 : len(e)-1])
	}
	return e
}

// iv80Close is the index of the parenthesis closing the one at open.
func iv80Close(s string, open int) int {
	depth, inQuote := 0, false
	for i := open; i < len(s); i++ {
		switch ch := s[i]; {
		case ch == '\'':
			inQuote = !inQuote
		case inQuote:
		case ch == '(':
			depth++
		case ch == ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// iv80IvValued: the expression is interval-typed (lexically, with the
// aliases inner blocks declared interval).
func iv80IvValued(e string, aliases map[string]bool) bool {
	e = iv80Strip(e)
	if e == "" {
		return false
	}
	for _, op := range []string{" = ", " <> ", " < ", " > ", " <= ", " >= ", " IS ", " BETWEEN ", " IN ", " AND ", " OR ", " LIKE "} {
		if len(iv80TopLevel(e, op)) > 1 {
			return false
		}
	}
	if strings.HasPrefix(e, "NOT ") || strings.HasPrefix(e, "EXISTS") {
		return false
	}
	// Binary arithmetic.
	for _, op := range []string{" + ", " - "} {
		if parts := iv80TopLevel(e, op); len(parts) > 1 {
			ts, iv := 0, 0
			for _, p := range parts {
				switch {
				case iv80IsTs(p):
					ts++
				case iv80IvValued(p, aliases):
					iv++
				}
			}
			if ts > 0 {
				return op == " - " && ts >= 2 && iv == 0
			}
			return iv > 0
		}
	}
	for _, op := range []string{" * ", " / "} {
		if parts := iv80TopLevel(e, op); len(parts) > 1 {
			for _, p := range parts {
				if iv80IvValued(p, aliases) {
					return true
				}
			}
			return false
		}
	}
	if strings.HasPrefix(e, "-") {
		return iv80IvValued(e[1:], aliases)
	}
	if strings.HasPrefix(e, "INTERVAL '") {
		return true
	}
	if strings.HasPrefix(e, "CAST(") && iv80Close(e, 4) == len(e)-1 {
		parts := iv80TopLevel(e[5:len(e)-1], " AS ")
		return strings.HasPrefix(strings.TrimSpace(parts[len(parts)-1]), "INTERVAL")
	}
	if strings.HasPrefix(e, "CASE ") {
		for _, p := range regexp.MustCompile(` (?:THEN|ELSE) `).Split(strings.TrimSuffix(e, " END"), -1)[1:] {
			if iv80IvValued(strings.SplitN(p, " WHEN ", 2)[0], aliases) {
				return true
			}
		}
		return false
	}
	if strings.HasPrefix(e, "(SELECT ") {
		return false
	}
	if m := iv80CallRe.FindStringSubmatch(e); m != nil && iv80Close(e, len(m[1])) == len(e)-1 {
		args := iv80TopLevel(m[2], ", ")
		switch m[1] {
		case "min", "max", "sum", "avg", "first_value", "last_value", "nth_value", "lag", "lead", "mode":
			return len(args) > 0 && iv80IvValued(strings.SplitN(args[0], " ORDER BY ", 2)[0], aliases)
		case "justify_days", "justify_hours", "justify_interval", "make_interval", "age", "date_trunc":
			return true
		case "coalesce", "nullif", "greatest", "least":
			for _, a := range args {
				if iv80IvValued(a, aliases) {
					return true
				}
			}
		}
		return false
	}
	if m := iv80IdentRe.FindStringSubmatch(e); m != nil {
		return aliases[m[1]]
	}
	return false
}

// iv80IsTs: a timestamp or date operand: a literal or column, or a window or
// value function whose arguments carry one (lag(NULL, 1, ts) OVER (…) is
// typed by its timestamp default).
func iv80IsTs(p string) bool {
	p = iv80Strip(p)
	if iv80TsAtomRe.MatchString(p) {
		return true
	}
	if i := strings.Index(p, " OVER ("); i > 0 {
		p = p[:i]
	}
	m := iv80CallRe.FindStringSubmatch(p)
	if m == nil || iv80Close(p, len(m[1])) != len(p)-1 {
		return false
	}
	switch m[1] {
	case "lag", "lead", "first_value", "last_value", "coalesce", "greatest", "least", "min", "max":
		for _, a := range iv80TopLevel(m[2], ", ") {
			if iv80TsAtomRe.MatchString(iv80Strip(a)) {
				return true
			}
		}
	}
	return false
}

// iv80Block is one SELECT block: its select list and the rest.
type iv80Block struct {
	list   []string
	rest   string
	nested bool
}

// iv80Blocks splits the statement into its SELECT blocks (every set-operation
// arm a block of its own) and VALUES lists.
func iv80Blocks(sql string) (blocks []iv80Block, values []string) {
	var walk func(s string, nested bool)
	walk = func(s string, nested bool) {
		s = strings.TrimSpace(s)
		if strings.HasPrefix(s, "WITH ") {
			// WITH name AS (body)[, …] main
			rest := s[5:]
			for {
				i := strings.Index(rest, "(")
				if i < 0 {
					break
				}
				j := iv80Close(rest, i)
				if j < 0 {
					break
				}
				walk(rest[i+1:j], true)
				rest = strings.TrimSpace(rest[j+1:])
				if !strings.HasPrefix(rest, ",") {
					break
				}
				rest = rest[1:]
			}
			s = rest
		}
		for _, arm := range iv80Arms(s) {
			arm = iv80Strip(arm)
			if !strings.HasPrefix(arm, "SELECT ") {
				continue
			}
			body := strings.TrimPrefix(strings.TrimPrefix(arm, "SELECT "), "DISTINCT ")
			parts := iv80TopLevel(body, " FROM ")
			rest := ""
			if len(parts) > 1 {
				rest = " FROM " + strings.Join(parts[1:], " FROM ")
			}
			blocks = append(blocks, iv80Block{list: iv80TopLevel(parts[0], ", "), rest: rest, nested: nested})
		}
		// Nested blocks anywhere in s.
		for i := 0; i < len(s); i++ {
			if s[i] == '\'' {
				if k := strings.IndexByte(s[i+1:], '\''); k >= 0 {
					i += k + 1
				}
				continue
			}
			if s[i] != '(' {
				continue
			}
			j := iv80Close(s, i)
			if j < 0 {
				return
			}
			inner := strings.TrimSpace(s[i+1 : j])
			switch {
			case strings.HasPrefix(inner, "SELECT ") || strings.HasPrefix(inner, "WITH "):
				walk(inner, true)
				i = j
			case strings.HasPrefix(inner, "VALUES "):
				values = append(values, s[i:])
				i = j
			}
		}
	}
	walk(sql, false)
	return blocks, values
}

// iv80Arms splits a statement at its top-level set operators.
func iv80Arms(s string) []string {
	out := []string{s}
	for _, op := range []string{" UNION ALL ", " UNION ", " INTERSECT ALL ", " INTERSECT ", " EXCEPT ALL ", " EXCEPT "} {
		var next []string
		for _, a := range out {
			next = append(next, iv80TopLevel(a, op)...)
		}
		out = next
	}
	return out
}

// iv80Aliases are the names inner blocks and VALUES lists declare interval.
func iv80Aliases(blocks []iv80Block, values []string) map[string]bool {
	aliases := map[string]bool{}
	for round := 0; round < 4; round++ {
		for _, v := range values {
			m := iv80ValuesRe.FindStringSubmatch(v)
			if m == nil {
				continue
			}
			cols := strings.Split(m[2], ", ")
			rows := iv80TopLevel(m[1], ", ")
			if len(rows) == 0 {
				continue
			}
			first := iv80TopLevel(iv80Strip(rows[0]), ", ")
			for i, c := range cols {
				if i < len(first) && iv80IvValued(first[i], aliases) {
					aliases[strings.TrimSpace(c)] = true
				}
			}
		}
		for _, b := range blocks {
			if !b.nested {
				continue
			}
			for _, it := range b.list {
				pieces := iv80TopLevel(it, " AS ")
				name := ""
				expr := it
				if len(pieces) > 1 {
					name = strings.TrimSpace(pieces[len(pieces)-1])
					expr = strings.Join(pieces[:len(pieces)-1], " AS ")
				} else if m := iv80IdentRe.FindStringSubmatch(strings.TrimSpace(it)); m != nil {
					name = m[1]
				}
				if name != "" && iv80IvValued(expr, aliases) {
					aliases[name] = true
				}
			}
		}
	}
	return aliases
}

// iv80Needs: why a statement needs stage D or E ("" when it does not):
//
//	E  an aggregate or window function over an interval argument; a window
//	   whose PARTITION BY or ORDER BY is an interval;
//	D  an interval crossing a block boundary (a derived table's, CTE's,
//	   subquery's, set-operation arm's or VALUES list's interval column), or an
//	   interval as a GROUP BY / DISTINCT / ORDER BY / join key.
func iv80Needs(sql string) map[string]string {
	needs := map[string]string{}
	if i := strings.Index(sql, " AS SELECT "); strings.HasPrefix(sql, "CREATE TABLE ") && i > 0 {
		sql = strings.TrimSuffix(sql[i+4:], " WITH NO DATA")
	}
	if strings.HasPrefix(sql, "INSERT INTO ") {
		if i := strings.Index(sql, " SELECT "); i > 0 {
			sql = sql[i+1:]
		}
	}
	blocks, values := iv80Blocks(sql)
	aliases := iv80Aliases(blocks, values)
	// E: aggregates and windows.
	for _, loc := range iv80AggRe.FindAllStringSubmatchIndex(sql, -1) {
		open := loc[3]
		end := iv80Close(sql, open)
		if end < 0 {
			continue
		}
		args := iv80TopLevel(sql[open+1:end], ", ")
		arg := strings.TrimPrefix(strings.SplitN(args[0], " ORDER BY ", 2)[0], "DISTINCT ")
		if arg != "*" && iv80IvValued(arg, aliases) {
			needs["E"] = "an aggregate or window function over an interval: " + sql[loc[0]:end+1]
		}
		if strings.HasPrefix(sql[end+1:], " WITHIN GROUP (ORDER BY ") {
			w := sql[end+len(" WITHIN GROUP (")+1:]
			if iv80IvValued(strings.TrimSuffix(strings.SplitN(strings.TrimPrefix(w, "ORDER BY "), ")", 2)[0], " DESC"), aliases) {
				needs["E"] = "an ordered-set aggregate over an interval"
			}
		}
	}
	for _, w := range regexp.MustCompile(` OVER \(`).FindAllStringIndex(sql, -1) {
		end := iv80Close(sql, w[1]-1)
		if end < 0 {
			continue
		}
		spec := sql[w[1]:end]
		spec = regexp.MustCompile(` (ROWS|RANGE|GROUPS) .*$`).ReplaceAllString(spec, "")
		for _, kw := range []string{"PARTITION BY ", "ORDER BY "} {
			if i := strings.Index(spec, kw); i >= 0 {
				keys := spec[i+len(kw):]
				if j := strings.Index(keys, " ORDER BY "); j >= 0 {
					keys = keys[:j]
				}
				for _, k := range iv80TopLevel(keys, ", ") {
					if iv80IvValued(iv80KeyExpr(k), aliases) {
						needs["E"] = "a window over an interval key: " + spec
					}
				}
			}
		}
	}
	// D: an interval crossing a block boundary.
	for _, b := range blocks {
		if !b.nested {
			continue
		}
		for _, it := range b.list {
			pieces := iv80TopLevel(it, " AS ")
			expr := it
			if len(pieces) > 1 {
				expr = strings.Join(pieces[:len(pieces)-1], " AS ")
			}
			if iv80IvValued(expr, aliases) {
				needs["D"] = "an interval column leaves a block: " + it
			}
		}
	}
	for _, v := range values {
		if m := iv80ValuesRe.FindStringSubmatch(v); m != nil {
			for _, row := range iv80TopLevel(m[1], ", ") {
				for _, f := range iv80TopLevel(iv80Strip(row), ", ") {
					if iv80IvValued(f, aliases) {
						needs["D"] = "a VALUES list's interval column"
					}
				}
			}
		}
	}
	// D: keys.
	keyed := func(clause, sep string) {
		for _, b := range blocks {
			text := b.rest
			i := strings.Index(text, clause)
			for i >= 0 {
				seg := text[i+len(clause):]
				for _, stop := range []string{" HAVING ", " ORDER BY ", " LIMIT ", " FETCH ", " OFFSET ", " UNION ", " INTERSECT ", " EXCEPT ", " WHERE ", " GROUP BY ", " WINDOW "} {
					if parts := iv80TopLevel(seg, stop); len(parts) > 1 {
						seg = parts[0]
					}
				}
				for _, k := range iv80TopLevel(seg, sep) {
					for _, pre := range []string{"ROLLUP ", "CUBE ", "GROUPING SETS "} {
						k = strings.TrimPrefix(k, pre)
					}
					for _, kk := range iv80TopLevel(iv80Strip(k), ", ") {
						if iv80IvValued(iv80KeyExpr(kk), aliases) {
							needs["D"] = "an interval key: " + strings.TrimSpace(clause) + " " + kk
						}
					}
				}
				j := strings.Index(text[i+len(clause):], clause)
				if j < 0 {
					break
				}
				i += len(clause) + j
			}
		}
	}
	keyed(" GROUP BY ", ", ")
	keyed(" ORDER BY ", ", ")
	for _, b := range blocks {
		if strings.Contains(sql, "SELECT DISTINCT "+strings.Join(b.list, ", ")) {
			for _, it := range b.list {
				pieces := iv80TopLevel(it, " AS ")
				expr := it
				if len(pieces) > 1 {
					expr = strings.Join(pieces[:len(pieces)-1], " AS ")
				}
				if iv80IvValued(expr, aliases) {
					needs["D"] = "an interval DISTINCT key: " + it
				}
			}
		}
		// A correlated interval equality in a nested block (EXISTS, NOT
		// EXISTS, a correlated scalar subquery) is a join key once the
		// planner decorrelates it into a semi, anti or outer join.
		if b.nested {
			if w := iv80TopLevel(b.rest, " WHERE "); len(w) > 1 {
				for _, eq := range iv80TopLevel(w[1], " AND ") {
					sides := iv80TopLevel(eq, " = ")
					if len(sides) == 2 && iv80IvValued(sides[0], aliases) && iv80IvValued(sides[1], aliases) &&
						iv80Qualifiers(sides[0]) != "" && iv80Qualifiers(sides[1]) != "" && iv80Qualifiers(sides[0]) != iv80Qualifiers(sides[1]) {
						needs["D"] = "a correlated interval equality (a join key once decorrelated): " + eq
					}
				}
			}
		}
		for _, on := range iv80TopLevel(b.rest, " ON ")[1:] {
			for _, stop := range []string{" WHERE ", " JOIN ", " LEFT ", " RIGHT ", " FULL ", " INNER ", " CROSS ", " GROUP BY ", " ORDER BY ", " WHEN ", " UNION ", " INTERSECT ", " EXCEPT "} {
				on = iv80TopLevel(on, stop)[0]
			}
			for _, eq := range iv80TopLevel(on, " AND ") {
				sides := iv80TopLevel(eq, " = ")
				if len(sides) == 2 && (iv80IvValued(sides[0], aliases) || iv80IvValued(sides[1], aliases)) {
					needs["D"] = "an interval join key: " + eq
				}
			}
		}
	}
	return needs
}

// iv80Qualifiers are the table qualifiers an expression names ("a.s" names a).
func iv80Qualifiers(e string) string {
	var qs []string
	for _, m := range regexp.MustCompile(`\b([a-z][a-z0-9_]*)\.[a-z]`).FindAllStringSubmatch(e, -1) {
		qs = append(qs, m[1])
	}
	sort.Strings(qs)
	return strings.Join(qs, ",")
}

// iv80KeyExpr is a sort or group key without its direction and NULLS words.
func iv80KeyExpr(k string) string {
	k = strings.TrimSpace(k)
	for _, suf := range []string{" NULLS FIRST", " NULLS LAST", " DESC", " ASC"} {
		k = strings.TrimSuffix(k, suf)
	}
	return k
}
