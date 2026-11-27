// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"math/big"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// The stage manifest (#1680b round 2; the judge's correction 3, the
// architect's ruling 9). Every statement of the corpus (a fixture or a plan
// observation is not one) is owned by exactly one stage of the definition's
// staging (docs/design/interval-execution-type.md §9) and has exactly one of
// four outcomes that stage must reach on every arm:
//
//	supported              the value AND the declared types equal PostgreSQL's
//	pg-error <SQLSTATE>    PostgreSQL's own error, the same SQLSTATE
//	refusal <reason>       an intentional capability refusal: 0A000, its reason named
//	rep-divergence         an approved representative divergence (ruling 3): the
//	                       same rows and types, every printed interval SQL-equal to
//	                       PostgreSQL's, the member the representative rule picks
//
// No cell is "expected to agree later" without a stage that owns it. The
// manifest is derived by rules (iv80ManifestOutcome) from the statement and
// PostgreSQL's answer, written to <stem>_manifest.tsv with each cell's "now"
// (whether today's baseline already meets the outcome: met, unmet, or door
// for a cell whose outcome is decided by a session setting the statement
// itself does not carry), and held by TestArc1680bStageManifest. A stage that
// lands adds its letter to iv80LandedStages; from then on every cell it owns
// must be met.

// iv80LandedStages are the stages that have landed (none: this item writes
// no engine code).
const iv80LandedStages = ""

// iv80Stages are the stages of the definition's §9 (B owns no corpus cell:
// its gates are the carrier's and the transport's own tests).
var iv80Stages = map[string]string{
	"A": "declarations and guards (nothing admitted)",
	"C": "inline expressions, one proven operation at a time",
	"D": "internal columns and keys behind the operator allowlist",
	"E": "aggregates and windows, one family at a time",
	"F": "publication policy: continued refusal of a bare interval result",
}

// iv80Reasons are the named capability refusals.
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
	"timestamp-difference-to-number": "a timestamp difference compared with or written as a number",
}

// iv80RepExpect is the answer a rep-divergence cell must give where the
// representative changes a value computed from it (whole-row consequence):
// the rule-3 member (ascending lexicographic (months, days, micros), the
// minimum wins) is 30 days, so 2024-01-31 + 30 days.
var iv80RepExpect = map[string]string{
	"11_relational:reparith/default":    "t | 2024-03-01 00:00:00",
	"11_relational:reparith/sortagg":    "t | 2024-03-01 00:00:00",
	"11_relational:reparithRev/default": "t | 2024-03-01 00:00:00",
	"11_relational:reparithRev/sortagg": "t | 2024-03-01 00:00:00",
}

var (
	iv80TypmodRe    = regexp.MustCompile(`INTERVAL\(\d+\)|INTERVAL '[^']*' (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)|AS INTERVAL (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)|SECOND\(\d+\)`)
	iv80TimeRe      = regexp.MustCompile(`\bTIME '|\bTIMETZ\b|AS TIME\)`)
	iv80DistinctAgg = regexp.MustCompile(`(sum|avg)\(DISTINCT`)
	iv80SubMilliTs  = regexp.MustCompile(`TIMESTAMP '[^']*\.\d{4,}'`)
	iv80RepFamily   = regexp.MustCompile(`GROUP BY|DISTINCT|UNION|INTERSECT|EXCEPT|\bmin\(|\bmax\(`)
)

// iv80BaseStem is a section's stem without its "_cast" suffix.
func iv80BaseStem(stem string) string { return strings.TrimSuffix(stem, "_cast") }

// iv80StageOf is the stage that owns a cell by its family: scalar
// expressions are C, keys and internal columns D, aggregates and windows E,
// writes A.
func iv80StageOf(stem, name string) string {
	base := strings.TrimSuffix(name, "~text")
	has := func(prefixes ...string) bool {
		for _, p := range prefixes {
			if strings.HasPrefix(base, p) {
				return true
			}
		}
		return false
	}
	switch iv80BaseStem(stem) {
	case "03_order":
		switch {
		case has("cmp/", "between", "in", "filter/"):
			return "C"
		case has("order/minMax", "order/minTie"):
			return "E"
		}
		return "D"
	case "04_keys":
		if has("partition/", "peers/", "countDistinct/", "withTies") {
			return "E"
		}
		return "D"
	case "05_arith", "07_functions":
		if has("agg/", "frame/", "widthBucket") {
			return "E"
		}
		return "C"
	case "09_model":
		if has("model/fields", "model/hash") {
			return "C"
		}
		return "D"
	case "10_definitions":
		if has("sentinel/sumToNegInf") {
			return "E"
		}
		return "C"
	case "11_relational":
		switch {
		case has("rt/", "opaque/", "filt/", "tsdate/"):
			return "C"
		case has("ordagg/", "distagg/", "empty/agg", "null/agg", "ov/", "pm/", "sw/", "frame/", "lag/"):
			return "E"
		}
		return "D"
	case "12_writes":
		return "A"
	}
	return "C"
}

// iv80HasNumericInfinity: PostgreSQL's answer has an infinite value in a
// numeric column.
func iv80HasNumericInfinity(ans string) bool {
	ans, _ = iv80Notice(ans)
	types := iv80HeaderTypes(ans)
	_, body, ok := strings.Cut(ans, " | ")
	if !ok {
		return false
	}
	for _, row := range strings.Split(body, "; ") {
		f := strings.Split(row, ",")
		if len(f) != len(types) {
			continue
		}
		for i, v := range f {
			if types[i] == "numeric" && (v == "Infinity" || v == "-Infinity") {
				return true
			}
		}
	}
	return false
}

// iv80HasSubMilliTimestamp: a timestamp column of PostgreSQL's answer carries
// more than three fractional digits.
func iv80HasSubMilliTimestamp(ans string) bool {
	ans, _ = iv80Notice(ans)
	types := iv80HeaderTypes(ans)
	_, body, ok := strings.Cut(ans, " | ")
	if !ok {
		return false
	}
	frac := regexp.MustCompile(`:\d\d\.\d{4,}`)
	for _, row := range strings.Split(body, "; ") {
		f := strings.Split(row, ",")
		if len(f) != len(types) {
			continue
		}
		for i, v := range f {
			if (types[i] == "timestamp" || types[i] == "text") && frac.MatchString(v) && strings.Count(v, "-") >= 2 {
				return true
			}
		}
	}
	return false
}

// iv80ManifestOutcome is the outcome the owning stage must reach for one
// statement, by the rules of the definition (§9) and the architect's rulings
// 1 to 9, first match wins.
func iv80ManifestOutcome(stem string, c iv80Cell, pg string) (outcome string) {
	sql := c.sql
	body, _ := iv80Notice(pg)
	pgErr := strings.HasPrefix(body, "ERR ")
	pgCode, _ := iv80ErrCode(body)
	types := iv80HeaderTypes(body)
	hasInterval := false
	for _, t := range types {
		if t == "interval" || t == "interval[]" {
			hasInterval = true
		}
	}
	// A typing error PostgreSQL decides before anything runs.
	if pgErr && (pgCode == "42883" || pgCode == "42804" || pgCode == "42846" || pgCode == "42601" || pgCode == "42725") {
		return "pg-error " + pgCode
	}
	switch {
	case c.write && strings.Contains(sql, "ts - ts"):
		return "refusal interval-write"
	case c.write && strings.Contains(sql, "CAST(CAST(s AS INTERVAL) AS TEXT)"):
		return "supported"
	case c.write:
		return "refusal interval-write"
	}
	for _, kv := range strings.Split(c.settings, ";") {
		if k, v, _ := strings.Cut(kv, "="); k == "IntervalStyle" && v != "postgres" {
			return "refusal intervalstyle"
		}
	}
	switch {
	case strings.Contains(sql, "TIMESTAMPTZ") || strings.Contains(sql, "date_subtract(") || (strings.Contains(sql, "date_add(") && strings.Count(sql, ",") >= 2 && strings.Contains(sql, "'America/")):
		return "refusal timestamptz"
	case iv80TimeRe.MatchString(sql):
		return "refusal time-type"
	case iv80TypmodRe.MatchString(sql):
		return "refusal typmod"
	case strings.Contains(sql, "ARRAY[") || strings.Contains(sql, "array_agg(") || strings.Contains(strings.Join(types, ","), "[]"):
		return "refusal container"
	case strings.Contains(sql, "interval_hash"):
		return "refusal postgresql-internal-function"
	case iv80OrderedAgg(sql):
		return "refusal ordered-aggregate"
	case iv80DistinctAgg.MatchString(sql):
		return "refusal distinct-aggregate"
	case strings.Contains(sql, "date_trunc("):
		return "refusal function-date_trunc"
	case strings.Contains(sql, "to_char("):
		return "refusal function-to_char"
	case strings.Contains(sql, "generate_series("):
		return "refusal function-generate_series"
	case strings.Contains(sql, "OVERLAPS"):
		return "refusal function-overlaps"
	case strings.Contains(sql, "width_bucket(INTERVAL"):
		return "refusal function-width_bucket"
	case strings.Contains(sql, "RANGE BETWEEN") && (strings.Contains(sql, "PRECEDING") || strings.Contains(sql, "FOLLOWING")) && strings.Contains(sql, "INTERVAL"):
		return "refusal range-offset-frame"
	case iv80SubMilliTs.MatchString(sql) || iv80HasSubMilliTimestamp(body):
		return "refusal timestamp-millisecond-carrier"
	case iv80HasNumericInfinity(body):
		return "refusal numeric-infinity"
	case hasInterval:
		return "refusal bare-interval-result"
	case pgErr:
		return "pg-error " + pgCode
	}
	if _, ok := iv80RepExpect[iv80BaseStem(stem)+":"+c.name]; ok {
		return "rep-divergence"
	}
	if iv80RepFamily.MatchString(strings.ReplaceAll(sql, "UNION ALL", "")) && iv80PrintsInterval(body) && !strings.Contains(sql, "GREATEST") && !strings.Contains(sql, "LEAST") {
		return "rep-divergence"
	}
	return "supported"
}

// iv80RefusalStage is the stage that owns a refusal: the publication policy
// is F's; the refusals decided where aggregates and windows are admitted are
// E's; every other is installed (or kept) by A's declarations and guards.
func iv80RefusalStage(reason string) string {
	switch reason {
	case "bare-interval-result":
		return "F"
	case "ordered-aggregate", "distinct-aggregate", "range-offset-frame":
		return "E"
	}
	return "A"
}

// iv80OrderedAgg: an aggregate call over an interval states an order: min /
// max / sum / avg / array_agg with an ORDER BY inside the call, a WITHIN
// GROUP aggregate, or a string_agg ordered by an interval.
func iv80OrderedAgg(sql string) bool {
	if strings.Contains(sql, "WITHIN GROUP") {
		return true
	}
	for _, fn := range []string{"min(", "max(", "sum(", "avg(", "array_agg(", "string_agg("} {
		for i := 0; ; {
			j := strings.Index(sql[i:], fn)
			if j < 0 {
				break
			}
			start := i + j + len(fn)
			if k := i + j; k > 0 && (sql[k-1] == '_' || (sql[k-1] >= 'a' && sql[k-1] <= 'z')) {
				i = start
				continue
			}
			depth, end := 1, start
			for end < len(sql) && depth > 0 {
				switch sql[end] {
				case '(':
					depth++
				case ')':
					depth--
				}
				end++
			}
			args := sql[start : end-1]
			if parts := iv80TopLevel(args, " ORDER BY "); len(parts) > 1 {
				if fn != "string_agg(" || strings.Contains(parts[1], "INTERVAL") || strings.HasPrefix(parts[1], "v") {
					return true
				}
			}
			i = start
		}
	}
	return false
}

// iv80PrintsInterval: some text column of the answer holds interval output.
func iv80PrintsInterval(ans string) bool {
	types := iv80HeaderTypes(ans)
	_, body, ok := strings.Cut(ans, " | ")
	if !ok || body == "(0 rows)" {
		return false
	}
	for _, row := range strings.Split(body, "; ") {
		f := strings.Split(row, ",")
		if len(f) != len(types) {
			return false
		}
		for i, v := range f {
			if types[i] == "text" && v != "NULL" {
				if _, ok := iv80ParsePGInterval(v); ok {
					return true
				}
			}
		}
	}
	return false
}

// iv80ParsePGInterval reads PostgreSQL's postgres-style interval output into
// its comparison value N = (months*30 + days)*86400e6 + micros (infinity as
// the reserved triples' N).
func iv80ParsePGInterval(s string) (*big.Int, bool) {
	var months, days, micros int64
	switch s {
	case "infinity":
		return iv80CmpValue(iv80ModelValue{2147483647, 2147483647, 9223372036854775807}), true
	case "-infinity":
		return iv80CmpValue(iv80ModelValue{-2147483648, -2147483648, -9223372036854775808}), true
	}
	toks := strings.Fields(s)
	if len(toks) == 0 {
		return nil, false
	}
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if strings.Contains(t, ":") {
			neg := strings.HasPrefix(t, "-")
			t = strings.TrimLeft(t, "+-")
			hms := strings.Split(t, ":")
			if len(hms) != 3 {
				return nil, false
			}
			h, e1 := strconv.ParseInt(hms[0], 10, 64)
			m, e2 := strconv.ParseInt(hms[1], 10, 64)
			sec, frac, _ := strings.Cut(hms[2], ".")
			sv, e3 := strconv.ParseInt(sec, 10, 64)
			if e1 != nil || e2 != nil || e3 != nil {
				return nil, false
			}
			us := int64(0)
			if frac != "" {
				frac = (frac + "000000")[:6]
				v, err := strconv.ParseInt(frac, 10, 64)
				if err != nil {
					return nil, false
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
			return nil, false
		}
		n, err := strconv.ParseInt(toks[i], 10, 64)
		if err != nil {
			return nil, false
		}
		switch strings.TrimSuffix(toks[i+1], "s") {
		case "year":
			months += 12 * n
		case "mon":
			months += n
		case "day":
			days += n
		default:
			return nil, false
		}
		i++
	}
	d := new(big.Int).Mul(big.NewInt(months), big.NewInt(30))
	d.Add(d, big.NewInt(days))
	d.Mul(d, big.NewInt(86400000000))
	return d.Add(d, big.NewInt(micros)), true
}

// iv80Meets: one engine answer reaches the outcome against PostgreSQL's.
func iv80Meets(outcome, engine, pg, repExpect string) bool {
	kind, arg, _ := strings.Cut(outcome, " ")
	cls := iv80Classify(engine, pg)
	switch kind {
	case "supported":
		return cls == "agree" || cls == "ok"
	case "pg-error":
		code, _ := iv80ErrCode(engine)
		return strings.HasPrefix(engine, "ERR ") && code == arg
	case "refusal":
		code, _ := iv80ErrCode(engine)
		return strings.HasPrefix(engine, "ERR ") && code == "0A000" && cls == "refusal"
	case "rep-divergence":
		if repExpect != "" {
			return ivNormalize(engine) == repExpect
		}
		return iv80RepEqual(engine, pg)
	}
	return false
}

// iv80RepEqual: the same declared types and rows, and every value equal to
// PostgreSQL's or, where both print an interval, SQL-equal to it.
func iv80RepEqual(engine, pg string) bool {
	pg, _ = iv80Notice(pg)
	if !strings.Contains(engine, " | ") || ivTypeNames(ivHeaderTypes(engine)) != iv80PGTypes(pg) {
		return false
	}
	_, eb, _ := strings.Cut(ivNormalize(engine), " | ")
	_, pb, _ := strings.Cut(iv80PGValue(pg), " | ")
	er, pr := strings.Split(eb, "; "), strings.Split(pb, "; ")
	if len(er) != len(pr) {
		return false
	}
	// Rows are compared as a multiset of rows (a representative changes a
	// row's text, not the count of rows; ORDER BY a representative-free key
	// keeps the order, so compare in order first, then as sorted).
	cmpRows := func(a, b []string) bool {
		for i := range a {
			fa, fb := strings.Split(a[i], ","), strings.Split(b[i], ",")
			if len(fa) != len(fb) {
				return false
			}
			for j := range fa {
				if fa[j] == fb[j] {
					continue
				}
				na, oka := iv80ParsePGInterval(fa[j])
				nb, okb := iv80ParsePGInterval(fb[j])
				if !oka || !okb || na.Cmp(nb) != 0 {
					return false
				}
			}
		}
		return true
	}
	return cmpRows(er, pr)
}

// iv80Manifest is a section's manifest lines.
func iv80Manifest(t *testing.T, sec iv80Section) []string {
	pg, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
	eng, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_engine.tsv"))
	var out []string
	for _, c := range sec.cells() {
		if c.pgOnly || (c.ddl && !c.write) {
			continue
		}
		ans := pg[c.name][0]
		outcome := iv80ManifestOutcome(sec.stem, c, ans)
		stage := iv80StageOf(sec.stem, c.name)
		if kind, reason, _ := strings.Cut(outcome, " "); kind == "refusal" {
			stage = iv80RefusalStage(reason)
		}
		now := "met"
		if strings.HasPrefix(outcome, "refusal intervalstyle") {
			now = "door"
		} else {
			expect := iv80RepExpect[iv80BaseStem(sec.stem)+":"+c.name]
			for _, l := range eng[c.name] {
				f := strings.SplitN(l, "\t", 3)
				if len(f) == 3 && !iv80Meets(outcome, iv80Unescape(f[2]), ans, expect) {
					now = "unmet"
				}
			}
		}
		out = append(out, c.name+"\t"+stage+"\t"+outcome+"\t"+now)
	}
	return out
}

func iv80ManifestFile(lines []string, stem string) string {
	return "# The stage manifest of section " + stem + " (arc_1680b_interval_manifest_test.go): name<TAB>owning stage<TAB>outcome<TAB>today's baseline meets it (met / unmet / door).\n" +
		strings.Join(lines, "\n") + "\n"
}

// TestArc1680bStageManifest: every statement has one owning stage and one of
// the four outcomes; a pg-error outcome only where PostgreSQL raised that
// error, supported or rep-divergence only where it answered; every refusal
// names a known reason; the files are the rules' output over today's
// baseline (IV80_RECORD_MANIFEST=1 rewrites them); and every cell a landed
// stage owns is met.
func TestArc1680bStageManifest(t *testing.T) {
	record := os.Getenv("IV80_RECORD_MANIFEST") != ""
	counts := map[string]int{}
	for _, sec := range iv80Sections() {
		lines := iv80Manifest(t, sec)
		pg, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
		for _, l := range lines {
			f := strings.Split(l, "\t")
			name, stage, outcome, now := f[0], f[1], f[2], f[3]
			if _, ok := iv80Stages[stage]; !ok {
				t.Errorf("%s:%s: unknown stage %q", sec.stem, name, stage)
			}
			kind, arg, _ := strings.Cut(outcome, " ")
			body, _ := iv80Notice(pg[name][0])
			pgCode, _ := iv80ErrCode(body)
			switch kind {
			case "supported", "rep-divergence":
				if strings.HasPrefix(body, "ERR ") {
					t.Errorf("%s:%s: %s where PostgreSQL raised %s", sec.stem, name, kind, body)
				}
			case "pg-error":
				if !strings.HasPrefix(body, "ERR ") || pgCode != arg {
					t.Errorf("%s:%s: pg-error %s where PostgreSQL answered %s", sec.stem, name, arg, body)
				}
			case "refusal":
				if _, ok := iv80Reasons[arg]; !ok {
					t.Errorf("%s:%s: refusal without a known reason: %q", sec.stem, name, arg)
				}
			default:
				t.Errorf("%s:%s: outcome %q is none of the four", sec.stem, name, outcome)
			}
			if strings.Contains(iv80LandedStages, stage) && now != "met" {
				t.Errorf("%s:%s: stage %s has landed but the cell is %s (%s)", sec.stem, name, stage, now, outcome)
			}
			counts[stage+" "+kind+" "+now]++
		}
		want := iv80ManifestFile(lines, sec.stem)
		path := iv80Path(sec.stem, "_manifest.tsv")
		if record {
			if err := os.WriteFile(path, []byte(want), 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != want {
			t.Errorf("%s: the manifest file is not the rules' output over the baseline (IV80_RECORD_MANIFEST=1)", sec.stem)
		}
	}
	var keys []string
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		t.Logf("%-40s %d", k, counts[k])
	}
}

// TestArc1680bOutcomeChecks: the four outcome checks accept what they must
// and reject an internal store-guard error, a panic, a wrong SQLSTATE and a
// representative that is not SQL-equal.
func TestArc1680bOutcomeChecks(t *testing.T) {
	pgIv := "a:text(25) | 1 mon"
	pgErr := "ERR 22008: interval out of range"
	for _, c := range []struct {
		outcome, engine, pg string
		want                bool
	}{
		{"supported", "a:TEXT | 1 mon", pgIv, true},
		{"supported", "a:TEXT | 30 days", pgIv, false},
		{"supported", "a:INT64 | 1 mon", pgIv, false},
		{"supported", "ERR 0A000: refused", pgIv, false},
		{"pg-error 22008", "ERR 22008: interval out of range", pgErr, true},
		{"pg-error 22008", "ERR 0A000: interval values cannot yet be used", pgErr, false},
		{"pg-error 22008", "ERR 22003: out of range", pgErr, false},
		{"pg-error 22008", "a:TEXT | 1 mon", pgErr, false},
		{"refusal typmod", "ERR 0A000: an interval typmod is not supported", pgIv, true},
		{"refusal typmod", "ERR 42601: syntax error", pgIv, false},
		{"refusal typmod", "ERR : batch: cannot store expr.IntervalValue into STRING vector (#361 silent-write guard)", pgIv, false},
		{"refusal typmod", "PANIC runtime error", pgIv, false},
		{"refusal typmod", "ERR XX000: internal", pgIv, false},
		{"rep-divergence", "a:TEXT | 30 days", pgIv, true},
		{"rep-divergence", "a:TEXT | 720:00:00", pgIv, true},
		{"rep-divergence", "a:TEXT | 31 days", pgIv, false},
		{"rep-divergence", "a:TEXT | 30 days; 30 days", pgIv, false},
		{"rep-divergence", "a:INT64 | 30 days", pgIv, false},
		{"rep-divergence", "a:TEXT | -1 days +24:00:00", "a:text(25) | 00:00:00", true},
		{"rep-divergence", "a:TEXT | infinity", "a:text(25) | 2147483647 mons 2147483647 days 2562047788:00:54.775807", true},
	} {
		if got := iv80Meets(c.outcome, c.engine, c.pg, ""); got != c.want {
			t.Errorf("iv80Meets(%q, %q, %q) = %v, want %v", c.outcome, c.engine, c.pg, got, c.want)
		}
	}
	if !iv80Meets("rep-divergence", "t:TIMESTAMP | 2024-03-01 00:00:00", "t:timestamp(1114) | 2024-02-29 00:00:00", "t | 2024-03-01 00:00:00") {
		t.Errorf("an explicit representative expectation is not honoured")
	}
	if iv80Meets("rep-divergence", "t:TIMESTAMP | 2024-02-29 00:00:00", "t:timestamp(1114) | 2024-02-29 00:00:00", "t | 2024-03-01 00:00:00") {
		t.Errorf("PostgreSQL's plan-dependent member passed where the rule's member is expected")
	}
	// The baseline classes keep an intentional refusal apart from the rest.
	for engine, want := range map[string]string{
		"ERR 0A000: interval values cannot yet be used as a query result column":                    "refusal",
		"ERR : batch: cannot store expr.IntervalValue into STRING vector (#361 silent-write guard)": "store-guard",
		"ERR : something without a code":   "internal",
		"PANIC boom":                       "panic",
		"ERR 22007: invalid input":         "wrong-sqlstate",
		"ERR 22008: interval out of range": "pg-error",
	} {
		if got := iv80Classify(engine, pgErr); got != want {
			t.Errorf("iv80Classify(%q) = %q, want %q", engine, got, want)
		}
	}
	if got := iv80Classify("ERR 42601: syntax error", pgIv); got != "error" {
		t.Errorf("a coded non-0A000 error where PostgreSQL answered: %q, want error", got)
	}
	if got := iv80Classify("a:TEXT | 1 mon", pgErr); got != "answers-pg-error" {
		t.Errorf("an answer where PostgreSQL raised an error: %q, want answers-pg-error", got)
	}
}
