// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// The outcome checks and the stage rules, held by adversarial cases
// (#1680b round 3; the second judge record's correction 2, items i to vii).
// Each case names the defect it would let through.

// iv80MeetsCell is iv80Meets for a corpus cell: its order and its spec.
func iv80MeetsCell(outcome, engine, pg, key, sql string) bool {
	x := iv80Check{ordered: iv80Ordered(sql), ties: iv80TiedOrder[sql]}
	if s, ok := iv80RepSpecs()[key]; ok {
		x.rep = &s
	}
	return iv80MeetsAny(outcome, iv80Alternatives[key], engine, pg, x)
}

// iv80OutcomeCase is one adversarial case: the answer, the cell it is
// judged as, and whether the outcome must accept it.
type iv80OutcomeCase struct {
	why                      string
	outcome, engine, pg, key string
	sql                      string
	want                     bool
}

// iv80OutcomeCases are the outcome checks' adversarial cases.
func iv80OutcomeCases() []iv80OutcomeCase {
	const unordered = "SELECT CAST(x AS TEXT) AS a FROM t"
	const ordered = "SELECT CAST(x AS TEXT) AS a FROM t ORDER BY id"
	pgIv := "a:text(25) | 1 mon"
	pgErr := "ERR 22008: interval out of range"
	tupleSQL := "SELECT CAST(a AS TEXT) AS a, CAST(b AS TEXT) AS b FROM (SELECT DISTINCT a, b FROM (VALUES (INTERVAL '1 mon', INTERVAL '24 hours'), (INTERVAL '30 days', INTERVAL '1 day')) v(a, b)) q"
	tuplePG := "a:text(25),b:text(25) | 1 mon,24:00:00"
	minTieSQL := "SELECT CAST(min(v) AS TEXT) AS lo, CAST(max(v) AS TEXT) AS hi FROM (VALUES (3, INTERVAL '720:00:00'), (2, INTERVAL '30 days'), (1, INTERVAL '1 mon')) t(id, v)"
	minTiePG := "lo:text(25),hi:text(25) | 1 mon,1 mon"
	pmSQL := "SELECT CAST(sum(CAST(s AS INTERVAL)) AS TEXT) AS s, CAST(avg(CAST(s AS INTERVAL)) AS TEXT) AS a, count(CAST(s AS INTERVAL)) AS n, CAST(min(CAST(s AS INTERVAL)) AS TEXT) AS lo, CAST(max(CAST(s AS INTERVAL)) AS TEXT) AS hi FROM iv80_pm WHERE g = 1"
	pmPG := "s:text(25),a:text(25),n:bigint(20),lo:text(25),hi:text(25) | 1 day,1 day,1,1 day,1 day"
	exceptSQL := "SELECT CAST(x AS TEXT) AS x FROM (SELECT x FROM (VALUES (INTERVAL '30 days'), (INTERVAL '720:00:00'), (INTERVAL '1 day'), (INTERVAL '2 days'), (INTERVAL '3 days')) r(x) EXCEPT SELECT x FROM (VALUES (INTERVAL '1 mon')) l(x)) u"
	exceptPG := "x:text(25) | 3 days; 1 day; 2 days"
	reparithSQL := "SELECT TIMESTAMP '2024-01-31' + v AS t FROM (SELECT DISTINCT v FROM (VALUES (INTERVAL '1 mon'), (INTERVAL '30 days')) q(v)) d"
	reparithPG := "t:timestamp(1114) | 2024-02-29 00:00:00"
	truncPG := "ERR 0A000: unit \"week\" not supported for type interval"
	rollupSQL := "SELECT g, CAST(CAST(s AS INTERVAL) AS TEXT) AS k, count(*) AS n FROM iv80_r GROUP BY ROLLUP (g, CAST(s AS INTERVAL)) ORDER BY g NULLS LAST, min(id), n"
	return []iv80OutcomeCase{
		// supported: value and type.
		{"the same answer", "supported", "a:TEXT | 1 mon", pgIv, "", unordered, true},
		{"a SQL-equal but different value", "supported", "a:TEXT | 30 days", pgIv, "", unordered, false},
		{"a wrong type", "supported", "a:INT64 | 1 mon", pgIv, "", unordered, false},
		{"a refusal where PostgreSQL answers", "supported", "ERR 0A000: refused", pgIv, "", unordered, false},
		// (vi) order: unordered results as multisets, ordered ones positionally.
		{"(vi) an unordered result in another order", "supported", "a:TEXT | 2 days; 1 day", "a:text(25) | 1 day; 2 days", "", unordered, true},
		{"(vi) an ordered result in another order", "supported", "a:TEXT | 2 days; 1 day", "a:text(25) | 1 day; 2 days", "", ordered, false},
		{"(vi) a multiset with a row doubled", "supported", "a:TEXT | 1 day; 1 day", "a:text(25) | 1 day; 2 days", "", unordered, false},
		// pg-error.
		{"PostgreSQL's SQLSTATE", "pg-error 22008", "ERR 22008: interval out of range", pgErr, "", unordered, true},
		{"another SQLSTATE", "pg-error 22008", "ERR 22003: out of range", pgErr, "", unordered, false},
		{"0A000 where PostgreSQL raised 22008", "pg-error 22008", "ERR 0A000: interval values cannot yet be used", pgErr, "", unordered, false},
		{"an answer where PostgreSQL raised", "pg-error 22008", "a:TEXT | 1 mon", pgErr, "", unordered, false},
		// (vii) the internal-failure exclusion before every outcome.
		{"(vii) a store guard coded like PostgreSQL's error", "pg-error 22008", "ERR 22008 {attached}: batch: cannot store expr.IntervalValue into STRING vector (#361 silent-write guard)", pgErr, "", unordered, false},
		{"(vii) a runtime error coded like PostgreSQL's error", "pg-error 22008", "ERR 22008 {attached}: runtime error: index out of range [3] with length 3", pgErr, "", unordered, false},
		{"(vii) XX000", "pg-error XX000", "ERR XX000: internal", "ERR XX000: internal", "", unordered, false},
		{"(vii) an uncoded error", "refusal typmod", "ERR : interval typmod", pgIv, "", unordered, false},
		{"(vii) a panic", "refusal typmod", "PANIC runtime error", pgIv, "", unordered, false},
		{"(vii) a runtime error presented as 0A000", "refusal typmod", "ERR 0A000 {attached}: runtime error: interval typmod slice bounds out of range", pgIv, "", unordered, false},
		{"(vii) a store guard presented as 0A000", "refusal container", "ERR 0A000 {attached}: batch: array element declared without its element or fields", pgIv, "", unordered, false},
		// (i) the refusal's NAMED reason: its identity, never its words.
		{"(i) the named reason", "refusal typmod", "ERR 0A000 {typmod}: an interval typmod is not supported", pgIv, "", unordered, true},
		{"(i) an unrelated 0A000", "refusal typmod", "ERR 0A000 {bare-interval-result}: interval values cannot yet be used as a query result column", pgIv, "", unordered, false},
		{"(i) the wrong capability refusal", "refusal function-to_char", "ERR 0A000 {function-date_trunc}: date_trunc over an interval argument is not supported", pgIv, "", unordered, false},
		{"(i) a syntax error", "refusal typmod", "ERR 42601: syntax error", pgIv, "", unordered, false},
		{"(i) the bare-result refusal", "refusal bare-interval-result", "ERR 0A000 {bare-interval-result}: interval values cannot yet be used as a query result column: compare or apply an interval inline", pgIv, "", unordered, true},
		// The third judge (1680c): a reason credited by words, and an
		// internal failure under an acceptable SQLSTATE.
		{"(judge 3) a timestamptz refusal is no time-type refusal, though its sentence names the time zone", "refusal time-type", "ERR 0A000 {timestamptz}: timestamptz arithmetic is not supported: the time zone is refused", pgIv, "", unordered, false},
		{"(judge 3) another refusal's sentence that names date_trunc", "refusal function-date_trunc", "ERR 0A000 {bare-interval-result}: interval values cannot yet be used as a query result column (date_trunc('hour', v))", pgIv, "", unordered, false},
		{"(judge 3) the named sentence without its identity", "refusal bare-interval-result", "ERR 0A000: interval values cannot yet be used as a query result column", pgIv, "", unordered, false},
		{"(judge 3) an internal failure under PostgreSQL's 0A000", "pg-error 0A000", "ERR 0A000 {attached}: unsupported shuffle type: 255", truncPG, "", unordered, false},
		{"(judge 3) a capability refusal under PostgreSQL's own SQLSTATE: the SQLSTATE is the outcome (ADR-0052 section 8)", "pg-error 0A000", "ERR 0A000 {function-date_trunc}: date_trunc over an interval argument is not supported", truncPG, "", unordered, true},
		{"(judge 3) PostgreSQL's own 0A000", "pg-error 0A000", "ERR 0A000: unit \"week\" not supported for type interval", truncPG, "", unordered, true},
		// (ii) a refusal where PostgreSQL itself answers 0A000.
		{"(ii) trunc/week: the named refusal over PostgreSQL's own 0A000", "refusal function-date_trunc", "ERR 0A000 {function-date_trunc}: date_trunc over an interval argument is not supported", truncPG, "07_functions:trunc/week", unordered, true},
		{"(ii) trunc/week: another 0A000", "refusal function-date_trunc", "ERR 0A000 {interval-unit}: interval unit \"week\" is not supported", truncPG, "07_functions:trunc/week", unordered, false},
		// The third judge: ROLLUP's ORDER BY leaves ties only within g.
		{"(judge 3) rollup: a permutation within a tie on g", "supported", "g:bigint,k:text,n:bigint | 1,NULL,3; 1,1 day,3; 4,2 days,2", "g:bigint(20),k:text(25),n:bigint(20) | 1,1 day,3; 1,NULL,3; 4,2 days,2", "", rollupSQL, true},
		{"(judge 3) rollup: rows out of order between distinct g", "supported", "g:bigint,k:text,n:bigint | 4,2 days,2; 1,1 day,3; 1,NULL,3", "g:bigint(20),k:text(25),n:bigint(20) | 1,1 day,3; 1,NULL,3; 4,2 days,2", "", rollupSQL, false},
		// (iii) the representative: an eligible input member, the whole
		// winning tuple, ascending.
		{"(iii) the rule-3 whole tuple", "rep-divergence", "a:TEXT,b:TEXT | 30 days,1 day", tuplePG, "11_relational:tuple/distinct/default", tupleSQL, true},
		{"(iii) a tuple fabricated from two rows", "rep-divergence", "a:TEXT,b:TEXT | 30 days,24:00:00", tuplePG, "11_relational:tuple/distinct/default", tupleSQL, false},
		{"(iii) PostgreSQL's non-winning representative", "rep-divergence", "a:TEXT,b:TEXT | 1 mon,24:00:00", tuplePG, "11_relational:tuple/distinct/default", tupleSQL, false},
		{"(iii) a member absent from the input", "rep-divergence", "a:TEXT,b:TEXT | 720:00:00,1 day", tuplePG, "11_relational:tuple/distinct/default", tupleSQL, false},
		{"(iii) min / max: the ascending member", "rep-divergence", "lo:TEXT,hi:TEXT | 720:00:00,720:00:00", minTiePG, "03_order:order/minTieRev~text", minTieSQL, true},
		{"(iii) min / max: the wrong direction (the maximum wins)", "rep-divergence", "lo:TEXT,hi:TEXT | 1 mon,1 mon", minTiePG, "03_order:order/minTieRev~text", minTieSQL, false},
		{"(iii) min / max: one column each way", "rep-divergence", "lo:TEXT,hi:TEXT | 720:00:00,1 mon", minTiePG, "03_order:order/minTieRev~text", minTieSQL, false},
		{"(iii) no spec: no latitude", "rep-divergence", "a:TEXT | 30 days", pgIv, "", unordered, false},
		// (iv) the latitude covers the key / MIN / MAX columns only.
		{"(iv) sum and avg exact beside min / max", "rep-divergence", "s:text,a:text,n:bigint,lo:text,hi:text | 1 day,1 day,1,1 day,1 day", pmPG, "11_relational:pm/null", pmSQL, true},
		{"(iv) sum and avg SQL-equal but not PostgreSQL's fields", "rep-divergence", "s:text,a:text,n:bigint,lo:text,hi:text | 24:00:00,24:00:00,1,1 day,1 day", pmPG, "11_relational:pm/null", pmSQL, false},
		// (v) types on the explicit representative branch.
		{"(v) the rule-3 value, the right type", "rep-divergence", "t:TIMESTAMP | 2024-03-01 00:00:00", reparithPG, "11_relational:reparith/default", reparithSQL, true},
		{"(v) the rule-3 value under TEXT", "rep-divergence", "t:TEXT | 2024-03-01 00:00:00", reparithPG, "11_relational:reparith/default", reparithSQL, false},
		{"(v) PostgreSQL's plan-dependent member", "rep-divergence", "t:TIMESTAMP | 2024-02-29 00:00:00", reparithPG, "11_relational:reparith/default", reparithSQL, false},
		// (vi) on a representative: unordered EXCEPT as a multiset.
		{"(vi) EXCEPT in another order", "rep-divergence", "x:TEXT | 1 day; 2 days; 3 days", exceptPG, "11_relational:set/except/bigSmall/default", exceptSQL, true},
		{"(vi) EXCEPT with a member dropped and one doubled", "rep-divergence", "x:TEXT | 1 day; 1 day; 3 days", exceptPG, "11_relational:set/except/bigSmall/default", exceptSQL, false},
		// Ruling 11 (pending): the unordered sum's legal alternative.
		{"ruling 11: PostgreSQL's 22008", "pg-error 22008", "ERR 22008: interval out of range", pgErr, "11_relational:ov/aggUnordered", "SELECT CAST(sum(CAST(s AS INTERVAL)) AS TEXT) AS s FROM iv80_ov WHERE id <= 3", true},
		{"ruling 11: another combination order's exact sum", "pg-error 22008", "s:TEXT | 2147483647 days", pgErr, "11_relational:ov/aggUnordered", "SELECT CAST(sum(CAST(s AS INTERVAL)) AS TEXT) AS s FROM iv80_ov WHERE id <= 3", true},
		{"ruling 11: a wrong finite sum", "pg-error 22008", "s:TEXT | 2147483646 days", pgErr, "11_relational:ov/aggUnordered", "SELECT CAST(sum(CAST(s AS INTERVAL)) AS TEXT) AS s FROM iv80_ov WHERE id <= 3", false},
		{"ruling 11: no alternative for a window's prescribed order", "pg-error 22008", "s:TEXT | 2147483647 days", pgErr, "11_relational:ov/winForward", "SELECT id, CAST(sum(CAST(s AS INTERVAL)) OVER (ORDER BY id ROWS UNBOUNDED PRECEDING) AS TEXT) AS s FROM iv80_ov WHERE id <= 3 ORDER BY id", false},
	}
}

// TestArc1680bOutcomeChecks: the four outcomes accept what they must and
// reject what the judge showed they let through.
func TestArc1680bOutcomeChecks(t *testing.T) {
	for _, c := range iv80OutcomeCases() {
		if got := iv80MeetsCell(c.outcome, c.engine, c.pg, c.key, c.sql); got != c.want {
			t.Errorf("%s: iv80Meets(%q, %q) = %v, want %v", c.why, c.outcome, c.engine, got, c.want)
		}
	}
	pgErr := "ERR 22008: interval out of range"
	// The baseline classes keep an intentional refusal apart from the rest.
	for engine, want := range map[string]string{
		"ERR 0A000 {bare-interval-result}: interval values cannot yet be used as a query result column": "refusal",
		"ERR : batch: cannot store expr.IntervalValue into STRING vector (#361 silent-write guard)":     "internal",
		"ERR 0A000 {attached}: unsupported shuffle type: 255":                                           "internal",
		"ERR : something without a code":                                                                "internal",
		"PANIC boom":                                                                                    "panic",
		"ERR 22007: invalid input":                                                                      "wrong-sqlstate",
		"ERR 22008: interval out of range":                                                              "pg-error",
	} {
		if got := iv80Classify(engine, pgErr); got != want {
			t.Errorf("iv80Classify(%q) = %q, want %q", engine, got, want)
		}
	}
}

// TestArc1680bRecordedIdentity: the recorder reads an error's identity off
// its TYPE chain, where it was raised, and the checker credits nothing else
// (#1680c, the third judge): an intentional refusal carries its reason
// through the door's sentence and a worker's rebuilt failure; a code
// attached to an uncoded failure is recorded as such and is an internal
// failure whatever its SQLSTATE; a refusal sentence without its identity
// earns nothing.
func TestArc1680bRecordedIdentity(t *testing.T) {
	refusal := sqlerr.Refuse("typmod", "an interval typmod is not supported")
	cases := []struct {
		why, outcome, pg string
		err              error
		want             bool
	}{
		{"a refusal under its identity", "refusal typmod", "a:text(25) | 1 mon", refusal, true},
		{"the same refusal through the door's sentence", "refusal typmod", "a:text(25) | 1 mon", sqlerr.Sentence(fmt.Errorf("planning: %w", refusal)), true},
		{"the same refusal rebuilt from a worker", "refusal typmod", "a:text(25) | 1 mon",
			stageTaskFailure(taskFailure{SQLState: "0A000", SQLMessage: refusal.Message, SQLReason: "typmod"}, errors.New("stage s: task t: "+refusal.Message)), true},
		{"the refusal's words under no identity", "refusal typmod", "a:text(25) | 1 mon", sqlerr.New("0A000", "an interval typmod is not supported"), false},
		{"another refusal naming the reason's words", "refusal typmod", "a:text(25) | 1 mon", sqlerr.Refuse("bare-interval-result", "interval typmod values cannot yet be used as a query result column"), false},
		{"an internal failure under PostgreSQL's 0A000", "pg-error 0A000", "ERR 0A000: unit \"week\" not supported for type interval", sqlerr.Wrap("0A000", errors.New("unsupported shuffle type: 255")), false},
		{"the same, rebuilt from a worker", "pg-error 0A000", "ERR 0A000: unit \"week\" not supported for type interval",
			stageTaskFailure(taskFailure{SQLState: "0A000", SQLMessage: "unsupported shuffle type: 255", SQLAttached: true}, errors.New("unsupported shuffle type: 255")), false},
		{"PostgreSQL's own 0A000, raised", "pg-error 0A000", "ERR 0A000: unit \"week\" not supported for type interval", sqlerr.New("0A000", "unit \"week\" not supported for type interval"), true},
		{"a code over a raised code is the raised one's", "pg-error 22008", "ERR 22008: interval out of range", sqlerr.Wrap("0A000", sqlerr.New("22008", "interval out of range")), false},
	}
	for _, c := range cases {
		rec := iv80Err(c.err)
		if got := iv80Meets(c.outcome, rec, c.pg, iv80Check{}); got != c.want {
			t.Errorf("%s: recorded %q; iv80Meets(%q) = %v, want %v", c.why, rec, c.outcome, got, c.want)
		}
	}
}

// TestArc1680bRepExpectations proves every representative spec against its
// PostgreSQL answer: the same row count; every column outside the spec's
// units equal to PostgreSQL's; every unit column SQL-equal to PostgreSQL's
// (rows matched as a multiset: the rule can reorder an ORDER BY of the
// text); each printed unit tuple an eligible member, and the minimum, under
// rule 3, of the members SQL-equal to it. And every spec belongs to a cell.
func TestArc1680bRepExpectations(t *testing.T) {
	specs := iv80RepSpecs()
	seen := map[string]bool{}
	for _, sec := range iv80Sections() {
		pg, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_pg17.tsv"))
		for _, c := range sec.cells() {
			key := iv80BaseStem(sec.stem) + ":" + c.name
			spec, ok := specs[key]
			if !ok || c.pgOnly || c.ddl {
				continue
			}
			seen[key] = true
			ans, _ := iv80Notice(pg[c.name][0])
			_, pb, _ := strings.Cut(ans, " | ")
			pr, er := strings.Split(pb, "; "), strings.Split(spec.rows, "; ")
			if len(pr) != len(er) {
				t.Errorf("%s: %d rows, PostgreSQL %d", key, len(er), len(pr))
				continue
			}
			inUnit := map[int]bool{}
			for _, u := range spec.units {
				for _, i := range u {
					inUnit[i] = true
				}
			}
			used := make([]bool, len(pr))
			for _, row := range er {
				ef := strings.Split(row, ",")
				matched := false
				for j, prow := range pr {
					pf := strings.Split(prow, ",")
					if used[j] || len(pf) != len(ef) {
						continue
					}
					ok := true
					for i := range ef {
						if inUnit[i] && spec.units != nil {
							ok = ok && iv80SQLEqualText(ef[i], pf[i])
						} else if spec.units != nil {
							ok = ok && ef[i] == pf[i]
						}
					}
					if ok {
						used[j], matched = true, true
						break
					}
				}
				if !matched && spec.units != nil {
					t.Errorf("%s: row %q matches no PostgreSQL row by its other columns and SQL equality", key, row)
				}
				for ui, u := range spec.units {
					var tuple []string
					null := false
					for _, i := range u {
						tuple = append(tuple, ef[i])
						null = null || ef[i] == "NULL"
					}
					if null {
						continue
					}
					member := false
					var best []string
					for _, m := range spec.members[ui] {
						mt := strings.Split(m, ",")
						eq := len(mt) == len(tuple)
						for i := 0; eq && i < len(mt); i++ {
							eq = iv80SQLEqualText(mt[i], tuple[i])
						}
						if !eq {
							continue
						}
						if strings.Join(mt, ",") == strings.Join(tuple, ",") {
							member = true
						}
						if best == nil || iv80RepLess(mt, best) {
							best = mt
						}
					}
					switch {
					case !member:
						t.Errorf("%s: %q is not an eligible input member", key, strings.Join(tuple, ","))
					case strings.Join(best, ",") != strings.Join(tuple, ","):
						t.Errorf("%s: %q is not the rule-3 minimum %q", key, strings.Join(tuple, ","), strings.Join(best, ","))
					}
				}
			}
		}
	}
	for key := range specs {
		if !seen[key] {
			t.Errorf("%s: a spec without a cell", key)
		}
	}
}

// TestArc1680bStageRules: the cells the judge found owned too early or too
// late are owned by the earliest stage that admits what they need, and the
// controls stay where they are.
func TestArc1680bStageRules(t *testing.T) {
	for _, c := range []struct{ why, sql, want string }{
		{"12_writes:w/ctasText: an inline multi-field text cast (C, not A)", "CREATE TABLE iv80_wc6 AS SELECT CAST(CAST(s AS INTERVAL) AS TEXT) AS v FROM iv80_r", "C"},
		{"10_definitions:closure/unionNull: a set-operation arm's interval column (D, not C)", "SELECT CAST(x AS TEXT) AS t FROM (SELECT CAST(NULL AS INTERVAL) AS x UNION ALL SELECT INTERVAL '1 day') q ORDER BY t NULLS FIRST", "D"},
		{"10_definitions:closure/whereFalseText: a derived table's interval column (D, not C)", "SELECT CAST(x AS TEXT) AS t FROM (SELECT CAST(NULL AS INTERVAL) AS x) q WHERE false", "D"},
		{"11_relational:sub/scalarMax: MAX over an interval (E, not D)", "SELECT (SELECT CAST(max(CAST(s AS INTERVAL)) AS TEXT) FROM iv80_r) AS m", "E"},
		{"11_relational:sub/correlatedMin: MIN over an interval (E, not D)", "SELECT id, (SELECT CAST(min(CAST(b.s AS INTERVAL)) AS TEXT) FROM iv80_r b WHERE b.g = a.g) AS m FROM iv80_r a ORDER BY id", "E"},
		{"a VALUES list's interval column (D)", "SELECT id, CAST(v AS TEXT) AS t FROM (VALUES (0, INTERVAL '1 mon')) t(id, v) ORDER BY id", "D"},
		{"an interval GROUP BY key (D)", "SELECT CAST(CAST(s AS INTERVAL) AS TEXT) AS k, count(*) AS n FROM iv80_r GROUP BY CAST(s AS INTERVAL) ORDER BY min(id)", "D"},
		{"an interval sort key (D)", "SELECT id FROM iv80_o ORDER BY CAST(s AS INTERVAL), id", "D"},
		{"an interval join key (D)", "SELECT a.id FROM iv80_r a JOIN iv80_r b ON CAST(a.s AS INTERVAL) = CAST(b.s AS INTERVAL)", "D"},
		{"a window PARTITION BY an interval (E)", "SELECT id, count(*) OVER (PARTITION BY CAST(s AS INTERVAL)) AS n FROM iv80_r ORDER BY id", "E"},
		{"count over an interval (E)", "SELECT count(CAST(s AS INTERVAL)) AS n FROM iv80_r", "E"},
		{"a correlated interval equality: an anti-join key once decorrelated (D)", "SELECT id FROM iv80_k a WHERE NOT EXISTS (SELECT 1 FROM iv80_k b WHERE CAST(b.s AS INTERVAL) = CAST(a.s AS INTERVAL) AND b.id <> a.id) ORDER BY id", "D"},
		{"a derived timestamp difference over lag's timestamp default (D)", "SELECT id FROM (SELECT id, lag(NULL, 1, ts) OVER (ORDER BY id) - ts AS t FROM iv80_rt) q WHERE t = INTERVAL '0' ORDER BY id", "D"},
		{"control: an inline timestamp difference over lag's default (C)", "SELECT id, CAST(lag(NULL, 1, ts) OVER (ORDER BY id) - ts AS TEXT) AS t FROM iv80_rt ORDER BY id", "C"},
		{"control: an uncorrelated interval comparison in a subquery (C)", "SELECT id FROM iv80_r a WHERE a.id IN (SELECT b.id FROM iv80_r b WHERE CAST(b.s AS INTERVAL) = INTERVAL '30 days') ORDER BY id", "C"},
		{"control: an inline comparison (C)", "SELECT id FROM iv80_o WHERE CAST(s AS INTERVAL) = INTERVAL '30 days' ORDER BY id", "C"},
		{"control: a runtime-column text cast (C)", "SELECT id, CAST(CAST(s AS INTERVAL) * 1.5 AS TEXT) AS x FROM iv80_r ORDER BY id", "C"},
		{"control: timestamp plus interval in a derived table is a timestamp (C)", "SELECT CAST(t AS TEXT) AS t FROM (SELECT ts + INTERVAL '1 day' AS t FROM iv80_r) q", "C"},
		{"control: a timestamp difference in a derived table is an interval (D)", "SELECT CAST(d AS TEXT) AS t FROM (SELECT ts - ts AS d FROM iv80_r) q", "D"},
		{"control: a GROUP BY beside count(*) over a text key (C)", "SELECT g, count(*) AS n FROM iv80_r WHERE CAST(s AS INTERVAL) > INTERVAL '0' GROUP BY g ORDER BY g", "C"},
	} {
		if got := iv80EarliestStage(c.sql); got != c.want {
			t.Errorf("%s: stage %s, want %s (needs %v)", c.why, got, c.want, iv80Needs(c.sql))
		}
	}
}
