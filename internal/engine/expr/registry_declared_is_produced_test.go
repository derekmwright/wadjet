// SPDX-License-Identifier: MIT

package expr

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// TestRegistryDeclaredTypeIsTheProducedType is the census gate of arc VL
// round 3: for every registered scalar function with a FIXED declaration it
// calls the kernel over a pool of sample arguments and checks that every value
// it produces is the box its DECLARED type is carried in — a DATE is int64
// epoch days, a TIMESTAMP int64 epoch milliseconds, a number a Go number, a
// text a string. A kernel whose values are temporal TEXT under a TEXT
// declaration (to_date and date_add used to be) is the same lie in the other
// direction and is caught by shape.
//
// The disagreement is the defect family the round found: current_date
// declared DATE and returned "2026-09-24", so `CURRENT_DATE + 1 - 1 -
// CURRENT_DATE` answered 18694; date_add declared TEXT and returned a date, so
// `UPDATE … SET d = date_add(d, 1)` was refused as a text source. Every
// TEMPORAL entry must agree — no exceptions. The non-temporal entries that
// still declare TEXT for a network or UUID value are pinned below by name,
// each with its reason; one that starts agreeing FAILS (delete it from the
// list — that is the proof), and a new one fails until it is either fixed or
// listed.
func TestRegistryDeclaredTypeIsTheProducedType(t *testing.T) {
	// Declared TEXT, produce a NETWORK / UUID value's text. Out of arc VL's
	// scope (the network-type lane): their values are right, their declared
	// type is text where PostgreSQL's is inet / uuid. An assignment of one to
	// a typed column parses the text as an unknown-typed literal (the
	// documented superset in docs/postgres-differences.md).
	declaredElsewhere := map[string]string{
		// Numeric, not text, and not temporal: abs/mod answer in their
		// argument's domain, which physical.scalarFnDeclaredNumericDomain
		// declares per call (#768) — the registry's fixed FLOAT64 is the
		// fallback. strlen declares double precision and returns an int4
		// count (the output vector converts it); a filing candidate.
		"abs":    "argument-domain numeric; declared per call by the planner",
		"mod":    "argument-domain numeric; declared per call by the planner",
		"strlen": "declares FLOAT64, returns an int4 count; filing candidate",
	}
	// Declared TEXT, produce a NETWORK / UUID value's text: the ONE list the
	// assignment rule also reads (DeclaresTextForTypedValue). Out of arc VL's
	// scope (the network-type lane): their values are right, their declared
	// type is text where PostgreSQL's is inet / uuid.
	for n := range declaredTextTypedValue {
		declaredElsewhere[n] = "declares text for a network / uuid value (DeclaresTextForTypedValue)"
	}
	// Declared TEXT by design: the value IS a string that happens to spell a
	// date or an address for the sample pool — a formatter, or a function
	// PostgreSQL itself types text.
	textByDesign := map[string]bool{
		"to_iso8601": true, "date_format": true, "ip_to_string": true,
		"at_timezone": true,
	}
	// Functions whose kernel reaches a network, the clock of another host, or
	// allocates by argument: not sampled.
	skip := func(n string) bool {
		return strings.Contains(n, "embed") || strings.HasPrefix(n, "geoip") ||
			n == "reverse_dns" || n == "repeat" || n == "lpad" || n == "rpad" ||
			n == "space" || n == "hosts_in_cidr" || strings.Contains(n, "sequence") ||
			strings.Contains(n, "series") || n == "range" || strings.Contains(n, "sleep")
	}
	pool := []any{"2026-03-03 10:20:30", "2026-03-03", int64(1), float64(1.5), "day",
		"10.0.0.1", "abc", true, "%Y-%m-%d", int64(3), "UTC", IntervalValue{Hours: 1}, "10.1.2.3/8"}

	names := DefaultRegistry.Names()
	sort.Strings(names)
	noValue := map[string]bool{}
	walked := 0
	var census []string
	temporalSeen := map[string]bool{}
	for _, n := range names {
		if skip(n) {
			continue
		}
		decl, conf := DefaultRegistry.ReturnType(n).Resolve(0, nil)
		if conf != Decided || DefaultRegistry.ReturnType(n).Boolean() && decl.ID != batch.TypeBool {
			continue
		}
		shapes := sampleShapes(DefaultRegistry.Lookup(n), n, pool)
		if len(shapes) == 0 {
			noValue[n] = true
			continue
		}
		walked++
		temporal := decl.ID == batch.TypeDate || decl.ID == batch.TypeTimestamp
		if temporal {
			temporalSeen[n] = true
		}
		var bad []string
		for s := range shapes {
			if !shapeFits(decl.ID, s) {
				bad = append(bad, s)
			}
		}
		// A TEXT declaration whose every sampled value is temporal or
		// network text is a typed value declared text.
		if decl.ID == batch.TypeString && !shapes["string"] && !textByDesign[n] {
			for s := range shapes {
				bad = append(bad, s)
			}
		}
		if len(bad) == 0 {
			if _, pinned := declaredElsewhere[n]; pinned {
				t.Errorf("%s now produces what it declares — delete its pin (the proof)", n)
			}
			continue
		}
		sort.Strings(bad)
		census = append(census, fmt.Sprintf("%s: declared %s, produced %s", n, decl.ID, strings.Join(bad, ",")))
		if temporal || strings.Contains(strings.Join(bad, ","), "text:date") ||
			strings.Contains(strings.Join(bad, ","), "text:timestamp") {
			t.Errorf("TEMPORAL entry %s declares %s and produces %v — a temporal value's box must be its declared type's", n, decl.ID, bad)
			continue
		}
		if _, pinned := declaredElsewhere[n]; !pinned {
			t.Errorf("%s declares %s and produces %v: fix the declaration or pin it with its reason", n, decl.ID, bad)
		}
	}
	// Every temporal-declared function must have been exercised, or the gate
	// says nothing about it.
	for _, n := range names {
		if skip(n) {
			continue
		}
		d, c := DefaultRegistry.ReturnType(n).Resolve(0, nil)
		if c == Decided && (d.ID == batch.TypeDate || d.ID == batch.TypeTimestamp) && !temporalSeen[n] {
			t.Errorf("temporal entry %s produced no value from the sample pool — extend the pool", n)
		}
	}
	// COVERAGE: every registry name is walked, or is listed below with the
	// reason it cannot be evaluated GENERICALLY — and none of those is
	// temporal-declared (the loop above already fails a temporal entry the
	// pool cannot reach). A listed name that starts producing a value FAILS
	// (move it into the walk by deleting it here), and a new name the pool
	// cannot reach fails until it is listed.
	notEvaluated := map[string]bool{}
	for _, group := range censusNotEvaluated {
		for _, n := range strings.Fields(group) {
			notEvaluated[n] = true
		}
	}
	undecided, skipped, builtin := 0, 0, 0
	for _, n := range names {
		if _, ok := funcSignatures[n]; !ok {
			// Registered by another test at runtime (a UDF such as expr_test's
			// `double`), not a built-in: the registry and the signature table
			// are held equal by their own gate.
			continue
		}
		builtin++
		d, c := DefaultRegistry.ReturnType(n).Resolve(0, nil)
		switch {
		case skip(n):
			skipped++
			if !notEvaluated[n] {
				t.Errorf("%s is skipped by the census but not listed in censusNotEvaluated", n)
			}
		case c != Decided || DefaultRegistry.ReturnType(n).Boolean() && d.ID != batch.TypeBool:
			undecided++
			if !notEvaluated[n] {
				t.Errorf("%s has no fixed declaration and is not listed in censusNotEvaluated", n)
			}
		case noValue[n] && !notEvaluated[n]:
			t.Errorf("%s produced no value from the sample pool: extend the pool or list it in censusNotEvaluated", n)
		case !noValue[n] && notEvaluated[n]:
			t.Errorf("%s is listed in censusNotEvaluated but the census walks it — delete it from the list", n)
		}
		if notEvaluated[n] && c == Decided && (d.ID == batch.TypeDate || d.ID == batch.TypeTimestamp) {
			t.Errorf("%s is temporal-declared and may not be left out of the census", n)
		}
	}
	t.Logf("registry %d names: %d walked, %d without a fixed declaration, %d skipped, %d unreachable by the generic pool",
		builtin, walked, undecided, skipped, len(noValue))
	t.Logf("declared ≠ produced (pinned, non-temporal): %d\n%s", len(census), strings.Join(census, "\n"))
}

// censusNotEvaluated are the registry names the census cannot evaluate
// GENERICALLY, by why. None is temporal-declared; the temporal entries are
// all walked here and, for their UNIT, evaluated on a known instant by
// TestTemporalRegistryEntriesProduceTheirDeclaredUnit.
var censusNotEvaluated = map[string]string{
	// No fixed declaration: the value is one of the arguments (or an element
	// or field of one), declared per call by the planner.
	"pass-through": "array_max array_min coalesce element_at greatest if ifnull json_extract json_extract_scalar least nullif row_field struct_field",
	// The kernel reaches the network or a host database, or allocates by an
	// argument's magnitude.
	"not sampled": "geoip_asn geoip_city geoip_continent geoip_country geoip_country_name geoip_latitude geoip_longitude geoip_org " +
		"geoip_postal_code geoip_subdivision geoip_timezone has_sequence_privilege hosts_in_cidr lpad pg_get_serial_sequence repeat reverse_dns rpad",
	// Their arguments are containers, vectors, packet / HTTP / TLS / DNS
	// payload bytes, semantic versions, catalog OIDs or MAC / TCP-flag values
	// the text-and-number pool does not spell; each has its own tests.
	"argument domain outside the pool": "map_entries map_keys map_values map_from_entries array_contains array_length array_lower array_upper " +
		"array_join cardinality width_bucket json_array_length cosine_similarity dot_product l2_distance vector_norm decode encode " +
		"has_tcp_flag tcp_flags_has_all tcp_flags_has_any tcp_flags_has_none tcp_flag_mask tcp_flags_from_string tcp_session_id " +
		"mac_is_local mac_is_unicast mac_format mac_to_string mac_vendor_oui ipv6_to_eui64 sixto4_gateway teredo_client teredo_server " +
		"semver_satisfies semver_cmp semver_major semver_minor semver_patch semver_parse semver_parse_strict semver_build semver_normalize " +
		"semver_normalize_strict semver_prerelease semver_sort_key url_extract_port uuid_version uuid_to_string " +
		"http_content_length http_status_code http_content_type http_header http_host http_method http_status_class http_user_agent http_version " +
		"dns_query_name dns_query_type ja3_fingerprint ja3_string ja3s_fingerprint ja3s_string tls_handshake_type tls_sni " +
		"protocol_number port_name vlan_id pg_relation_is_publishable regclassin regrolein regclassout regroleout to_regclass " +
		"col_description obj_description shobj_description current_query inet_client_addr inet_server_addr " +
		"pg_get_constraintdef pg_get_expr pg_get_function_arguments pg_get_function_result pg_get_indexdef pg_get_ruledef " +
		"pg_get_statisticsobjdef_columns pg_get_triggerdef pg_get_userbyid pg_get_viewdef",
}

var (
	censusDateRe = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}$`)
	censusTsRe   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}(:?\d{2})?)?$`)
	censusInetRe = regexp.MustCompile(`^\d+\.\d+\.\d+\.\d+(/\d+)?$`)
	censusUUIDRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// sampleShapes calls fn over every argument tuple the pool spells within the
// function's signature (up to three arguments) and returns the set of value
// shapes it produced: the Go box type, or for a string whether it spells a
// date, a timestamp, an address or a UUID.
func sampleShapes(fn ScalarFunc, name string, pool []any) map[string]bool {
	sig, _ := SignatureOf(name)
	minA, maxA := sig.Min, sig.Max
	if maxA < 0 || maxA > 3 {
		maxA = 3
	}
	if minA > 3 {
		minA = 3
	}
	shapes := map[string]bool{}
	calls := 0
	var try func(args []any)
	try = func(args []any) {
		if len(args) >= minA && len(args) <= maxA {
			calls++
			func() {
				defer func() { _ = recover() }()
				if v := fn(append([]any(nil), args...)); v != nil {
					shapes[valueShape(v)] = true
				}
			}()
		}
		if len(args) < maxA && calls < 4000 {
			for _, p := range pool {
				try(append(args, p))
			}
		}
	}
	try(nil)
	return shapes
}

func valueShape(v any) string {
	if s, ok := v.(string); ok {
		switch {
		case censusDateRe.MatchString(s):
			return "text:date"
		case censusTsRe.MatchString(s):
			return "text:timestamp"
		case censusInetRe.MatchString(s):
			return "text:inet"
		case censusUUIDRe.MatchString(s):
			return "text:uuid"
		}
		return "string"
	}
	return fmt.Sprintf("%T", v)
}

// shapeFits reports whether a produced shape is the box a declared type is
// carried in.
func shapeFits(t batch.TypeID, shape string) bool {
	switch t {
	case batch.TypeDate, batch.TypeTimestamp:
		return shape == "int64"
	case batch.TypeInt32, batch.TypeInt64:
		return shape == "int64" || shape == "int32" || shape == "int"
	case batch.TypeFloat32, batch.TypeFloat64:
		return shape == "float64" || shape == "float32"
	case batch.TypeBool:
		return shape == "bool"
	case batch.TypeString:
		return strings.HasPrefix(shape, "text:") || shape == "string"
	}
	return true
}
