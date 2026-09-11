package expr

import (
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// THE TABLE AND THE REGISTRY NAME THE SAME SET (#966 round 3 review B1).
//
// This is the gate that makes the table a TABLE rather than a list somebody
// remembered to extend. A function whose result is an integer and whose
// PostgreSQL width nobody decided is how the defect comes back: the aggregate
// walk would fall through to "not an integer-result function" and read the
// expression as int4, so `SUM(new_function(…))` would quietly declare bigint
// whatever PostgreSQL says.
//
// Both directions, because both are failure modes: a registered function
// missing from the table, and a table row for a name that is not registered or
// no longer returns an integer (a rename leaving a row nothing reads).
func TestEveryIntegerDeclaredFunctionNamesItsPostgresResultWidth(t *testing.T) {
	declared := map[string]bool{}
	for _, name := range DefaultRegistry.Names() {
		r := DefaultRegistry.ReturnType(name)
		if r.kind != retFixed {
			continue
		}
		if r.typ == batch.TypeInt32 || r.typ == batch.TypeInt64 {
			declared[name] = true
		}
	}
	if len(declared) == 0 {
		t.Fatal("no integer-declared function found: the registry walk is broken, " +
			"not the table")
	}

	var missing []string
	for name := range declared {
		if _, ok := PGIntegerResultWidth(name); !ok {
			missing = append(missing, name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("these functions return an integer and no one decided what width "+
			"PostgreSQL declares for them, so SUM over them would take the int4 "+
			"reading by accident:\n  %s\nMeasure `pg_typeof(f(…))` on PostgreSQL "+
			"where it has the function, else decide by the domain, and add the row "+
			"to pgIntegerResultWidths.", strings.Join(missing, "\n  "))
	}

	var stale []string
	for _, name := range pgIntegerResultWidthNames() {
		if !declared[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("these table rows name a function that is not registered or no "+
			"longer returns an integer, so nothing reads them:\n  %s",
			strings.Join(stale, "\n  "))
	}

	// The two width rules must not both claim a function. ABS and MOD answer
	// in their argument's own numeric domain and the walk asks
	// NumericDomainScalarFn for them BEFORE this table; a row here would be
	// unreachable and would disagree the day that order changed.
	for _, name := range pgIntegerResultWidthNames() {
		if _, ok := NumericDomainScalarFn(name); ok {
			t.Errorf("%s is in BOTH the numeric-domain set and the width table; "+
				"one function, one rule", name)
		}
	}
}

// THE WIDTHS POSTGRESQL 17.11 WAS MEASURED TO DECLARE.
//
// The transcript, so a row that changes has to be re-measured rather than
// re-reasoned. Every `want` below is `pg_typeof` on the shared oracle server
// over a two-row table `a2r4_c(id int, f4 int, f8 bigint, s text, b bytea,
// c cidr, a int[])`:
//
//	length(s)                    integer     sum -> bigint
//	octet_length(s)              integer     sum -> bigint
//	bit_length(s)                integer     sum -> bigint
//	char_length(s)               integer     sum -> bigint
//	strpos(s,'b')                integer     sum -> bigint
//	position('b' in s)           integer     sum -> bigint
//	array_length(a,1)            integer     sum -> bigint
//	cardinality(a)               integer     sum -> bigint
//	width_bucket(5.,0.,10.,4)    integer     sum -> bigint
//	ascii(s)                     integer     sum -> bigint
//	regexp_count(s,'a')          integer     sum -> bigint
//	masklen(c)                   integer     sum -> bigint
//	pg_backend_pid()             integer     sum -> bigint
//	bit_count(b)                 bigint      sum -> numeric
//	txid_current()               bigint      sum -> numeric
//	f4 & 18 | # ~ << >>          integer     sum -> bigint
//	f8 & 18 | # ~ << >>          bigint      sum -> numeric
func TestThePostgresResultWidthTableMatchesTheMeasuredTranscript(t *testing.T) {
	for _, tc := range []struct {
		fn   string
		want PGIntWidth
		args []int
	}{
		{fn: "length", want: PGIntWidth4},
		{fn: "len", want: PGIntWidth4},
		{fn: "octet_length", want: PGIntWidth4},
		{fn: "bit_length", want: PGIntWidth4},
		{fn: "char_length", want: PGIntWidth4},
		{fn: "character_length", want: PGIntWidth4},
		{fn: "strpos", want: PGIntWidth4},
		{fn: "position", want: PGIntWidth4},
		{fn: "array_length", want: PGIntWidth4},
		{fn: "cardinality", want: PGIntWidth4},
		{fn: "width_bucket", want: PGIntWidth4},
		{fn: "codepoint", want: PGIntWidth4},
		{fn: "regexp_count", want: PGIntWidth4},
		{fn: "prefix_length", want: PGIntWidth4},
		{fn: "payload_length", want: PGIntWidth4},
		{fn: "pg_backend_pid", want: PGIntWidth4},
		{fn: "vector_dims", want: PGIntWidth4},
		{fn: "bit_count", want: PGIntWidth8},
		{fn: "txid_current", want: PGIntWidth8},
		{fn: "pg_current_xact_id", want: PGIntWidth8},
		// The domain readings, both sides.
		{fn: "ip_ttl", want: PGIntWidth4},
		{fn: "vlan_id", want: PGIntWidth4},
		{fn: "tcp_flag_mask", want: PGIntWidth4},
		{fn: "tcp_flags_from_string", want: PGIntWidth4},
		{fn: "http_content_length", want: PGIntWidth8},
		{fn: "geoip_asn", want: PGIntWidth8},
		{fn: "hosts_in_cidr", want: PGIntWidth8},
		{fn: "parse_bytes", want: PGIntWidth8},
		// The bitwise family follows its operands, and a shift follows the
		// value it shifts rather than the count.
		{fn: "bitwise_and", want: PGIntWidthOperands},
		{fn: "bitwise_or", want: PGIntWidthOperands},
		{fn: "bitwise_xor", want: PGIntWidthOperands},
		{fn: "bitwise_not", want: PGIntWidthOperands},
		{fn: "bitwise_left_shift", want: PGIntWidthOperands, args: []int{0}},
		{fn: "bitwise_right_shift", want: PGIntWidthOperands, args: []int{0}},
		{fn: "bitwise_arithmetic_shift_right", want: PGIntWidthOperands, args: []int{0}},
	} {
		t.Run(tc.fn, func(t *testing.T) {
			got, ok := PGIntegerResultWidth(tc.fn)
			if !ok {
				t.Fatalf("%s has no width row", tc.fn)
			}
			if got.Width != tc.want {
				t.Errorf("%s: width %d, want %d", tc.fn, got.Width, tc.want)
			}
			if len(got.WidthArgs) != len(tc.args) {
				t.Fatalf("%s: WidthArgs %v, want %v", tc.fn, got.WidthArgs, tc.args)
			}
			for i := range tc.args {
				if got.WidthArgs[i] != tc.args[i] {
					t.Errorf("%s: WidthArgs %v, want %v", tc.fn, got.WidthArgs, tc.args)
				}
			}
		})
	}

	// The name is folded, exactly as the registry folds it, so an uppercase
	// spelling in SQL reaches the same row.
	if got, ok := PGIntegerResultWidth("  BITWISE_AND "); !ok || got.Width != PGIntWidthOperands {
		t.Errorf("a folded, padded name did not reach its row: %v %v", got, ok)
	}
	if _, ok := PGIntegerResultWidth("upper"); ok {
		t.Error("a function that returns text has a width row")
	}
}
