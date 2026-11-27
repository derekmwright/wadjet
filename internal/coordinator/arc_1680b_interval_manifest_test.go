// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"math/big"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The stage manifest (#1680b round 2; the judge's correction 3, the
// architect's ruling 9). Every statement of the corpus (a fixture or a plan
// observation is not one) is owned by exactly one stage of the definition's
// staging (docs/design/interval-execution-type.md §9) and has exactly one of
// four outcomes that stage must reach on every arm:
//
//	supported              the value AND the declared types equal PostgreSQL's
//	pg-error <SQLSTATE>    PostgreSQL's own error, the same SQLSTATE
//	refusal <reason>       an intentional capability refusal: 0A000 with the named
//	                       reason's sentence (iv80Reasons)
//	rep-divergence         an approved representative divergence (ruling 3): the
//	                       cell's explicit rule-3 answer (iv80RepSpecs), types
//	                       and the columns outside its units exactly PostgreSQL's
//
// The internal-failure exclusion precedes every outcome; unordered results
// compare as multisets (round 3: arc_1680b_interval_manifest_rules_test.go).
// No cell is "expected to agree later" without a stage that owns it. The
// manifest is derived by rules (iv80ManifestOutcome, iv80CellStage) from the
// statement and PostgreSQL's answer, written to <stem>_manifest.tsv with
// each cell's "now" (whether today's baseline already meets the outcome; an
// IntervalStyle cell by the engine's SET answer at the door, from
// pgwire.TestIntervalClientEvidence) and its legal alternatives, and held by
// TestArc1680bStageManifest. A stage that lands adds its letter to
// iv80LandedStages; from then on every cell it owns must be met.

// iv80LandedStages are the stages that have landed (none: this item writes
// no engine code).
const iv80LandedStages = ""

// iv80Stages are the stages of the definition's §9. B owns no corpus cell
// (its gates are the carrier's and the transport's own tests); F owns none
// either: the publication policy is enforced from A onward (every refusal is
// A's) and F audits it with the client transcript.
var iv80Stages = map[string]string{
	"A": "declarations and guards (nothing admitted; every named refusal, the bare-result and write refusals included)",
	"C": "inline expressions, one proven operation at a time",
	"D": "internal columns and keys behind the operator allowlist",
	"E": "aggregates and windows, one family at a time",
}

// iv80Alternative is a legal alternative to a cell's outcome: a plan-dependent
// answer PostgreSQL's own semantics permit (ADR-0013), checked like an
// outcome against its own reference answer.
type iv80Alternative struct {
	outcome, ref, why string
}

// iv80Alternatives are the cells whose outcome has a legal alternative.
// Ruling 11 is PENDING with the maintainer: these are written to its
// recommended option (PostgreSQL's finite-width checked accumulator; which
// inputs overflow may depend on the combination order of an unordered
// parallel sum, a loud plan-dependent refusal, never a wrong value). A
// window's prescribed order (ov/winForward, sw/all) has no alternative.
var iv80Alternatives = map[string][]iv80Alternative{
	// {2147483647 days, 1 day, -1 day}: (a + b) + c overflows (PostgreSQL's
	// serial order), (a + c) + b and (b + c) + a are 2147483647 days
	// (ov/aggDesc measures that order).
	"11_relational:ov/aggUnordered": {{"supported", "s:text(25) | 2147483647 days", "another combination order of the unordered sum answers the exact sum (ruling 11, pending)"}},
	// The same three values with a bare result: the finite order publishes
	// an interval, which is refused.
	"05_arith:agg/sumOverflowThenBack": {{"refusal bare-interval-result", "", "another combination order publishes the finite sum, a bare interval result (ruling 11, pending; ruling 1)"}},
}

// iv80DoorPath is the client transcript the IntervalStyle cells are decided
// by (the corpus applies a cell's IntervalStyle on the reference only).
const iv80DoorPath = "../server/pgwire/testdata/interval_client_evidence_engine.tsv"

// iv80Door is the engine's answer to SET IntervalStyle at the door, per
// style (pgwire.TestIntervalClientEvidence, door/IntervalStyle/<style>).
func iv80Door(t *testing.T) map[string]string {
	t.Helper()
	raw, err := os.ReadFile(iv80DoorPath)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, line := range strings.Split(string(raw), "\n") {
		if name, ans, ok := strings.Cut(line, "\t"); ok && strings.HasPrefix(name, "door/IntervalStyle/") {
			out[strings.TrimPrefix(name, "door/IntervalStyle/")] = ans
		}
	}
	return out
}

// iv80CharCastRe: an interval cast to CHAR(n) / CHARACTER(n) / bpchar,
// outside the display path (§7; ruling 1 as clarified by addition (a): a
// character type the engine declares correctly).
var iv80CharCastRe = regexp.MustCompile(`AS (CHAR|CHARACTER|BPCHAR)\b(\(\d+\))?\)|::(char|character|bpchar)\b`)

var (
	iv80TypmodRe    = regexp.MustCompile(`INTERVAL\(\d+\)|INTERVAL '[^']*' (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)|AS INTERVAL (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)|SECOND\(\d+\)`)
	iv80TimeRe      = regexp.MustCompile(`\bTIME '|\bTIMETZ\b|AS TIME\)`)
	iv80DistinctAgg = regexp.MustCompile(`(sum|avg)\(DISTINCT`)
	iv80SubMilliTs  = regexp.MustCompile(`TIMESTAMP '[^']*\.\d{4,}'`)
	// age(DATE, DATE) resolves to the timestamptz overload (measured:
	// 10_definitions:explain/age/date, age/dateGapHavana against
	// age/dateGapUTC).
	iv80DateAge = regexp.MustCompile(`\bage\(DATE '`)
)

// iv80BaseStem is a section's stem without its "_cast" suffix.
func iv80BaseStem(stem string) string { return strings.TrimSuffix(stem, "_cast") }

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
	case strings.Contains(sql, "TIMESTAMPTZ") || iv80DateAge.MatchString(sql) || strings.Contains(sql, "date_subtract(") || (strings.Contains(sql, "date_add(") && strings.Count(sql, ",") >= 2 && strings.Contains(sql, "'America/")):
		return "refusal timestamptz"
	case iv80TimeRe.MatchString(sql):
		return "refusal time-type"
	case iv80TypmodRe.MatchString(sql):
		return "refusal typmod"
	case iv80CharCastRe.MatchString(sql) && strings.Contains(sql, "INTERVAL"):
		return "refusal char-display"
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
	// A representative divergence only where a spec names the key or MIN /
	// MAX columns and their rule-3 answer (round 3: a GROUP BY beside
	// SUM / AVG alone is not one).
	if _, ok := iv80RepSpecs()[iv80BaseStem(stem)+":"+c.name]; ok {
		return "rep-divergence"
	}
	return "supported"
}

// iv80CellStage is the stage that owns a cell: every refusal is A's (a named
// refusal holds at every stage, from the first: the bare-result, write,
// ordered- and DISTINCT-aggregate and frame refusals included); every other
// cell is owned by the earliest stage at which everything it needs is
// admitted (iv80EarliestStage).
func iv80CellStage(c iv80Cell, outcome string) string {
	if strings.HasPrefix(outcome, "refusal ") {
		return "A"
	}
	return iv80EarliestStage(c.sql)
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

// iv80ParsePGInterval reads PostgreSQL's postgres-style interval output into
// its comparison value N = (months*30 + days)*86400e6 + micros (infinity as
// the reserved triples' N).
func iv80ParsePGInterval(s string) (*big.Int, bool) {
	months, days, micros, ok := iv80Triple(s)
	if !ok {
		return nil, false
	}
	switch s {
	case "infinity", "-infinity":
		return iv80CmpValue(iv80ModelValue{int32(months), int32(days), micros}), true
	}
	d := new(big.Int).Mul(big.NewInt(months), big.NewInt(30))
	d.Add(d, big.NewInt(days))
	d.Mul(d, big.NewInt(86400000000))
	return d.Add(d, big.NewInt(micros)), true
}

// iv80Check is what an outcome check needs beyond the two answers: whether
// the statement orders its result, and a representative cell's spec.
type iv80Check struct {
	ordered bool
	rep     *iv80RepSpec
}

// iv80Meets: one engine answer reaches the outcome against PostgreSQL's. The
// internal-failure exclusion comes first, before any outcome.
func iv80Meets(outcome, engine, pg string, x iv80Check) bool {
	if iv80InternalFailure(engine) {
		return false
	}
	kind, arg, _ := strings.Cut(outcome, " ")
	switch kind {
	case "supported":
		return iv80SameAnswer(engine, pg, x.ordered)
	case "pg-error":
		code, _ := iv80ErrCode(engine)
		return strings.HasPrefix(engine, "ERR ") && code == arg
	case "refusal":
		// 0A000 with the NAMED reason's sentence; PostgreSQL may itself
		// answer 0A000 for the cell (trunc/week): the refusal is still met.
		code, msg := iv80ErrCode(engine)
		r, ok := iv80Reasons[arg]
		return ok && strings.HasPrefix(engine, "ERR ") && code == "0A000" && r.re.MatchString(msg)
	case "rep-divergence":
		return x.rep != nil && iv80SameAnswer(engine, iv80RepAnswer(pg, *x.rep), x.ordered)
	}
	return false
}

// iv80MeetsAny: the outcome or one of its legal alternatives is met.
func iv80MeetsAny(outcome string, alts []iv80Alternative, engine, pg string, x iv80Check) bool {
	if iv80Meets(outcome, engine, pg, x) {
		return true
	}
	for _, a := range alts {
		if iv80Meets(a.outcome, engine, a.ref, x) {
			return true
		}
	}
	return false
}

// iv80Manifest is a section's manifest lines.
func iv80Manifest(t *testing.T, sec iv80Section) []string {
	pg, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
	eng, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_engine.tsv"))
	door := iv80Door(t)
	specs := iv80RepSpecs()
	var out []string
	for _, c := range sec.cells() {
		if c.pgOnly || (c.ddl && !c.write) {
			continue
		}
		ans := pg[c.name][0]
		key := iv80BaseStem(sec.stem) + ":" + c.name
		outcome := iv80ManifestOutcome(sec.stem, c, ans)
		stage := iv80CellStage(c, outcome)
		x := iv80Check{ordered: iv80Ordered(c.sql)}
		if s, ok := specs[key]; ok {
			x.rep = &s
		}
		alts := iv80Alternatives[key]
		now := "met"
		if style := iv80CellStyle(c); style != "" {
			// Decided at the door: the engine's SET IntervalStyle answer.
			a, ok := door[style]
			if !ok || !iv80Meets(outcome, a, "ok", x) {
				now = "unmet"
			}
		} else {
			for _, l := range eng[c.name] {
				f := strings.SplitN(l, "\t", 3)
				if len(f) == 3 && !iv80MeetsAny(outcome, alts, iv80Unescape(f[2]), ans, x) {
					now = "unmet"
				}
			}
		}
		outcome, stage, now = iv80Ruled(c, outcome, stage, now, eng[c.name], ans, x)
		alt := "-"
		if len(alts) > 0 {
			var as []string
			for _, a := range alts {
				as = append(as, a.outcome)
			}
			alt = strings.Join(as, " / ")
		}
		out = append(out, c.name+"\t"+stage+"\t"+outcome+"\t"+now+"\t"+alt)
	}
	return out
}

// iv80OutsideStage is the owner of a cell outside the program: no stage
// owns it and no stage's landing requires it (ruling 2 of 1680c r3).
const iv80OutsideStage = "-"

// iv80Ruled applies the architect's three rulings on 1680c's conflicts (r3)
// to a cell the rules gave an A refusal that today's baseline does not meet,
// decided by the baseline when the manifest is recorded:
//
//  1. right on main: every arm already answers PostgreSQL's value and type,
//     so the cell's outcome is supported (owned by A: nothing it needs waits
//     for a later stage) and stage A must keep it right;
//  2. not parsed: this engine's SQL parser (plansql.Parse, the statement's
//     first boundary on every arm, the interval literal reader included)
//     rejects the statement, so it is outside the program: outcome
//     "not-parsed <SQLSTATE>" with no owning stage. It is "met" while every
//     arm still refuses it with the parser's SQLSTATE, so a later change that
//     lets it answer, or answers it differently, shows;
//  3. the TIME cells (TIME - TIME, DATE ± TIME): unchanged, an A refusal
//     time-type (they answer wrong on main).
//
// An IntervalStyle cell is decided at the door (the SET), never by its
// statement, so neither ruling touches it. Every other cell is unchanged.
func iv80Ruled(c iv80Cell, outcome, stage, now string, eng []string, pg string, x iv80Check) (string, string, string) {
	if stage != "A" || !strings.HasPrefix(outcome, "refusal ") || now == "met" || iv80CellStyle(c) != "" || len(eng) == 0 {
		return outcome, stage, now
	}
	right := true
	for _, l := range eng {
		f := strings.SplitN(l, "\t", 3)
		if len(f) != 3 || !iv80Meets("supported", iv80Unescape(f[2]), pg, x) {
			right = false
		}
	}
	if right {
		return "supported", "A", "met"
	}
	if _, err := plansql.Parse(c.sql); err != nil {
		code := sqlerr.StateOf(err)
		now = "met"
		for _, l := range eng {
			f := strings.SplitN(l, "\t", 3)
			if len(f) != 3 {
				now = "unmet"
				continue
			}
			a := iv80Unescape(f[2])
			if got, _ := iv80ErrCode(a); !strings.HasPrefix(a, "ERR ") || got != code {
				now = "unmet"
			}
		}
		return "not-parsed " + code, iv80OutsideStage, now
	}
	return outcome, stage, now
}

// iv80SQL is a section's statement by cell name ("" when none).
func iv80SQL(sec iv80Section, name string) string {
	for _, c := range sec.cells() {
		if c.name == name {
			return c.sql
		}
	}
	return ""
}

// iv80CellStyle is the non-postgres IntervalStyle a cell's settings name
// ("" when none).
func iv80CellStyle(c iv80Cell) string {
	for _, kv := range strings.Split(c.settings, ";") {
		if k, v, _ := strings.Cut(kv, "="); k == "IntervalStyle" && v != "postgres" {
			return v
		}
	}
	return ""
}

func iv80ManifestFile(lines []string, stem string) string {
	return "# The stage manifest of section " + stem + " (arc_1680b_interval_manifest_test.go): name<TAB>owning stage<TAB>outcome<TAB>today's baseline meets it (met / unmet; an IntervalStyle cell by the door's SET answer)<TAB>legal alternatives (ADR-0013; ruling 11 pending).\n" +
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
			if strings.HasPrefix(outcome, "refusal ") && stage != "A" {
				t.Errorf("%s:%s: a refusal owned by %s, not A", sec.stem, name, stage)
			}
			kind, arg, _ := strings.Cut(outcome, " ")
			if _, ok := iv80Stages[stage]; !ok && !(stage == iv80OutsideStage && kind == "not-parsed") {
				t.Errorf("%s:%s: unknown stage %q", sec.stem, name, stage)
			}
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
			case "not-parsed":
				// Outside the program (ruling 2 of 1680c r3): the parser
				// rejects a statement PostgreSQL parses (it may still raise
				// its own error running it).
				if stage != iv80OutsideStage {
					t.Errorf("%s:%s: not-parsed owned by %q", sec.stem, name, stage)
				}
				if _, err := plansql.Parse(iv80SQL(sec, name)); err == nil || sqlerr.StateOf(err) != arg {
					t.Errorf("%s:%s: not-parsed %s, but the parser answers %v", sec.stem, name, arg, err)
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
