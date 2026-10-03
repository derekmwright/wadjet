// SPDX-License-Identifier: MIT

package expr

import (
	"sort"
	"strconv"
	"strings"
	"testing"
)

// castExactnessPassThrough is every registered function a cast-made-exact
// numeric passes through on the way to a quotient (CastExactnessArgs), with
// the argument positions it passes through: "*" is every argument. Over an
// exact DECIMAL argument each answers an exact DECIMAL of it — the decimal
// scalar functions and mod by decimalScalarFn, the choosing functions by
// their arm fold — so a quotient over it takes the one-scale rule exactly
// when a quotient over the argument would.
var castExactnessPassThrough = map[string]string{
	"abs": "0", "ceil": "0", "ceiling": "0", "floor": "0", "round": "0",
	"trunc": "0", "truncate": "0", "sign": "0", "mod": "0,1",
	"coalesce": "*", "greatest": "*", "least": "*", "nullif": "0",
	"ifnull": "0,1", "if": "1,2",
}

// castExactnessStops is every other registered numeric function: a fixed
// FLOAT64 or integer declaration, whose result is not its argument's exact
// type, so the walk ends at it.
var castExactnessStops = []string{
	"acos", "array_length", "array_lower", "array_upper", "ascii", "asin",
	"atan", "atan2", "bit_count", "bit_length", "bitwise_and",
	"bitwise_arithmetic_shift_right", "bitwise_left_shift", "bitwise_not",
	"bitwise_or", "bitwise_right_shift", "bitwise_xor", "cardinality",
	"cast_float", "cast_int", "cbrt", "char_length", "character_length",
	"codepoint", "cos", "cosine_similarity", "crc32", "date_diff",
	"date_part", "day", "day_of_week", "day_of_year", "degrees",
	"dns_answer_count", "dns_question_count", "dns_transaction_id",
	"domain_depth", "dot_product", "e", "entropy", "epoch", "exp",
	"extract", "from_base", "from_hex", "from_iso8601_timestamp",
	"geoip_asn", "geoip_latitude", "geoip_longitude", "get_byte",
	"hamming_distance", "hosts_in_cidr", "hour", "http_content_length",
	"http_status_code", "icmp_code", "icmp_type", "infinity", "ip_diff",
	"ip_dscp", "ip_header_length", "ip_to_int", "ip_total_length", "ip_ttl",
	"ip_version", "json_array_length", "l2_distance", "len", "length",
	"levenshtein_distance", "ln", "log", "log10", "log2", "minute", "month",
	"nan", "octet_length", "parse_bytes", "parse_rate", "payload_entropy",
	"payload_length", "pg_backend_pid", "pg_current_xact_id",
	"pg_my_temp_schema", "pi", "position", "pow", "power", "prefix_length",
	"protocol_number", "quarter", "radians", "rand", "random", "regclassin",
	"regexp_count", "regnamespacein", "regrolein", "regtypein",
	"round_half_even", "second", "semver_cmp", "semver_major",
	"semver_minor", "semver_patch", "sin", "sqrt", "strlen", "strpos",
	"tan", "tcp_flag_mask", "tcp_flags_from_string", "timezone_hour",
	"timezone_minute", "to_milliseconds", "to_regclass", "to_unixtime",
	"txid_current", "url_extract_port", "uuid_version", "vector_dims",
	"vector_norm", "vlan_id", "week", "width_bucket", "year",
}

// TestCastExactnessArgsEveryRegisteredNumericFunction states, per name, what
// the quotient's exactness walk does at every registered numeric scalar
// function — a fixed numeric declaration, a same-as-argument one, or a name
// with an exact DECIMAL kernel — and holds expr.castMadeExactIn to it at
// each argument position. A function registered later fails here until it
// is placed in one of the two tables; physical's
// TestCastMadeExactInReadsTheSharedFunctionList holds the plan's twin to the
// same list. With two lists, round / ceil / ceiling / floor / trunc ended
// the runtime walk (decimalScalarFn deferred to a FuncCall arm that knew
// only abs and mod) while their exact kernels answered a DECIMAL, so
// `ceil(CAST(t.i AS NUMERIC)) / t.n` took the one-scale quotient.
func TestCastExactnessArgsEveryRegisteredNumericFunction(t *testing.T) {
	stop := map[string]bool{}
	for _, n := range castExactnessStops {
		if stop[n] {
			t.Fatalf("%s listed twice in castExactnessStops", n)
		}
		if _, ok := castExactnessPassThrough[n]; ok {
			t.Fatalf("%s is in both tables", n)
		}
		stop[n] = true
	}
	names := DefaultRegistry.Names()
	sort.Strings(names)
	registered := map[string]bool{}
	for _, n := range names {
		registered[n] = true
		r := DefaultRegistry.ReturnType(n)
		_, poly := r.SameAsArgs(1)
		_, exact := decimalScalarOps[n]
		if !r.Numeric() && !poly && !exact && n != "mod" {
			continue
		}
		if _, ok := castExactnessPassThrough[n]; !ok && !stop[n] {
			t.Errorf("registered numeric function %s (%s) is in neither table: place it in castExactnessPassThrough "+
				"(its result over an exact DECIMAL argument is an exact DECIMAL of it) or castExactnessStops", n, r)
		}
	}
	for n := range decimalScalarOps {
		if _, ok := castExactnessPassThrough[n]; !ok {
			t.Errorf("%s has an exact DECIMAL kernel (decimalScalarOps) but is not in castExactnessPassThrough", n)
		}
	}

	col := mustCompileExpr(t, "i")
	cast := mustCompileExpr(t, "CAST(i AS INTEGER)")
	check := func(n string, want map[int]bool) {
		const nargs = 3
		for p := 0; p < nargs; p++ {
			args := []Expr{col, col, col}
			args[p] = cast
			got := castMadeExactIn(&FuncCall{Name: n, Args: args})
			if got != want[p] {
				verdict := "stops"
				if want[p] {
					verdict = "passes through"
				}
				t.Errorf("%s with the integer CAST at argument %d: castMadeExactIn = %v, want %v (%s)", n, p, got, want[p], verdict)
			}
		}
	}
	for n, pos := range castExactnessPassThrough {
		if !registered[n] {
			if _, exact := decimalScalarOps[n]; !exact {
				t.Errorf("%s is in castExactnessPassThrough but not registered", n)
			}
		}
		want := map[int]bool{}
		if pos == "*" {
			want = map[int]bool{0: true, 1: true, 2: true}
		} else {
			for _, s := range strings.Split(pos, ",") {
				i, err := strconv.Atoi(s)
				if err != nil {
					t.Fatalf("%s: position %q", n, s)
				}
				want[i] = true
			}
		}
		check(n, want)
	}
	for _, n := range castExactnessStops {
		if !registered[n] {
			t.Errorf("%s is in castExactnessStops but not registered", n)
		}
		check(n, nil)
	}

	// The compiled shapes: the exact node (decimalScalarFn) wraps the call,
	// and the walk reads the wrapped call's name.
	for _, tc := range []struct {
		sql  string
		want bool
	}{
		{"ceil(CAST(i AS NUMERIC))", true},
		{"ceiling(CAST(i AS NUMERIC))", true},
		{"floor(CAST(i AS NUMERIC))", true},
		{"trunc(CAST(i AS NUMERIC))", true},
		{"truncate(CAST(i AS NUMERIC))", true},
		{"round(CAST(i AS NUMERIC))", true},
		{"round(CAST(i AS NUMERIC), 2)", true},
		{"abs(round(CAST(i AS NUMERIC)))", true},
		{"sign(CAST(i AS NUMERIC)) * 4", true},
		{"round(CAST(i AS INTEGER) * 1.0)", true},
		{"trunc(CAST(i AS INTEGER) + 0.0)", true},
		{"mod(CAST(i AS NUMERIC), 7)", true},
		{"round(i)", false},
		{"round(n, CAST(i AS INTEGER))", false},
		{"round(CAST(i AS NUMERIC(10,0)))", false},
		{"sqrt(CAST(i AS NUMERIC))", false},
		{"power(CAST(i AS NUMERIC), 1)", false},
	} {
		if got := castMadeExactIn(mustCompileExpr(t, tc.sql)); got != tc.want {
			t.Errorf("castMadeExactIn(%s) = %v, want %v", tc.sql, got, tc.want)
		}
	}
}
