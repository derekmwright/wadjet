package multikey

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// The env var naming the PostgreSQL server. Same spelling the TPC-H oracle
// uses, so one exported DSN serves both.
const dsnEnv = "WADJET_PG_DSN"

// TestCorpusAnswersComeFromPostgres is the audit trail for every Want in
// Corpus(). It loads PostgresSetup() into a live PostgreSQL and asserts that
// the constants the wadjet and coordinator gates compare against are what
// PostgreSQL answers — not what an engine answered on the day they were
// written.
//
// The constants exist at all because those gates run without a container:
// wadjet's is an in-process unit test and the coordinator's stands up NATS,
// and neither can depend on Docker. That makes the numbers a claim about
// PostgreSQL made in their absence, and this is where the claim is checked.
//
// It SKIPS when no server is named, exactly like the TPC-H oracle:
//
//	task pg-oracle:up   # prints the DSN
//	WADJET_PG_DSN=… go test ./internal/oracle/multikey/
//
// task pg-oracle:test covers the same corpus continuously from the other
// direction — benchmarks/tpch's oracle loads this fixture into both engines
// and compares them cell by cell, with no constant in between.
func TestCorpusAnswersComeFromPostgres(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set — start a server with `task pg-oracle:up` to check the corpus "+
			"constants against the engine that decided them", dsnEnv)
	}
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connecting to %s: %v", dsnEnv, err)
	}
	defer conn.Close(ctx)

	if _, err := conn.Exec(ctx, PostgresSetup()); err != nil {
		t.Fatalf("loading the fixture: %v", err)
	}
	for _, c := range Corpus() {
		t.Run(c.Name, func(t *testing.T) {
			var got int64
			if err := conn.QueryRow(ctx, c.SQL).Scan(&got); err != nil {
				t.Fatalf("%s: %v\n  SQL: %s", c.Name, err, c.SQL)
			}
			if got != c.Want {
				t.Errorf("PostgreSQL answers %d, the corpus claims %d\n  SQL: %s", got, c.Want, c.SQL)
			}
		})
	}
}

// Corpus() is only as strong as its spread. An entry answering 0 or the whole
// probe side proves nothing about a multi-column key: #562 took a semi join to
// zero and its anti twin to everything, so both of those ARE the failure. The
// entries that legitimately sit at a boundary say so by name.
func TestCorpusIsNotDegenerate(t *testing.T) {
	boundary := map[string]bool{
		// A probe row whose first key is NULL matches nothing, whatever the
		// second key does — 0 is the assertion, not an accident.
		"exists_null_probe_key": true,
		// Its complement: every such row survives NOT EXISTS.
		"notexists_null_probe_key": true,
		// Inner covers every value of n, so a single-key EXISTS on n excludes
		// nothing. It is the control that says the SECOND key is load-bearing.
		"notexists_one_key": true,
		"exists_one_key_n":  true,
		"exists_one_key_d":  true,
		"exists_one_key_c":  true,
		// The distinct-name arm's twins of the same three.
		"dn_exists_null_probe":    true,
		"dn_notexists_null_probe": true,
		"dn_exists_1key_n":        true,
	}
	for _, c := range Corpus() {
		if boundary[c.Name] {
			continue
		}
		if c.Want == 0 {
			t.Errorf("%s expects 0 rows, which is also what a semi join with a deleted "+
				"build key answers — the entry cannot tell the two apart", c.Name)
		}
		probe := int64(OuterRows)
		if c.Name == "exists_noswap_str_i64" {
			probe = WideRows
		}
		if c.Want == probe {
			t.Errorf("%s expects every one of the %d probe rows, which is also what an anti "+
				"join with a deleted build key answers", c.Name, probe)
		}
	}
}

// Every corpus name is unique: two entries sharing one would make a subtest
// name collide and let a pin or a skip cover the wrong query.
func TestCorpusNamesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, c := range Corpus() {
		if seen[c.Name] {
			t.Errorf("duplicate corpus entry %q", c.Name)
		}
		seen[c.Name] = true
	}
}

// A pin that matches no corpus entry exempts nothing and hides nothing — it
// is a renamed or deleted entry whose exemption outlived it. Half of the
// ratchet; the other half (a pin whose entry starts AGREEING) lives in the
// gates, because only they run the engine.
func TestPinsNameRealEntries(t *testing.T) {
	inCorpus := map[string]bool{}
	for _, c := range Corpus() {
		inCorpus[c.Name] = true
	}
	for _, set := range []map[string]struct{ issue, reason string }{pins, dnPins} {
		for name, p := range set {
			if !inCorpus[name] {
				t.Errorf("pin %q (%s) matches no corpus entry — the entry was renamed or removed, "+
					"so the pin now exempts nothing. Delete it or fix the name.", name, p.issue)
			}
		}
	}
}

// The two arms differ in exactly one thing, and it is the thing under test.
// The shared-schema arm's relations carry one schema, so every correlated
// conjunct reads `s = s`, extractRightJoinKeys cannot attribute either side
// and the narrowing DECLINES. If both arms declined, this corpus would gate
// the decline and never once exercise the code path #562 lived on.
//
// This asserts the SHAPE of the corpus rather than an engine answer, so it
// runs everywhere and costs nothing: an entry whose two relations share a
// column prefix cannot make the pass fire, and the dn_ arm's do not.
func TestTheTwoArmsDifferInKeyAttributability(t *testing.T) {
	shared, distinct := 0, 0
	for _, c := range Corpus() {
		if strings.HasPrefix(c.Name, "dn_") {
			distinct++
			if pair, bad := selfCorrelatesOnOneName(c.SQL); bad {
				t.Errorf("%s correlates on %q, whose two sides strip to one bare name; the "+
					"distinct-name arm exists so a correlation's outer and inner columns differ, "+
					"which is the only shape dedupSemiAntiBuildSide can attribute and narrow",
					c.Name, pair)
			}
			continue
		}
		shared++
	}
	if shared == 0 || distinct == 0 {
		t.Fatalf("corpus has %d shared-schema and %d distinct-name entries; both arms are required",
			shared, distinct)
	}
}

// correlationPairRe matches an equality of two qualified columns, X.a = Y.b —
// the only join/correlation shape either arm writes. Whitespace around "=" is
// flexible so a re-indented entry is still seen.
var correlationPairRe = regexp.MustCompile(`([a-z0-9]+)\.([a-z_][a-z0-9_]*)\s*=\s*([a-z0-9]+)\.([a-z_][a-z0-9_]*)`)

// selfCorrelatesOnOneName reports the first CORRELATION in sql whose two sides
// strip to the same bare column name, and true when one exists.
//
// A correlation links the OUTER query to the subquery, and in the
// distinct-name arm the outer relation is the only one whose columns carry the
// "p_" prefix (dnPrefix: outer p_, inner q_/w_, dim k_). So a pair is a
// correlation exactly when one of its operands is a p_ column — which is what
// lets this ignore the subquery's OWN join clauses (`b1.q_g = b2.q_g`, both
// inner; `k.k_k = b.q_g`, dim and inner) that legitimately share, or don't,
// a bare name and are none of this invariant's business.
//
// It keys on the p_ column rather than on the outer ALIAS, so it catches the
// mutation that renames the outer alias (b.p_s = x.p_s) as readily as the
// two that keep it (b.p_n = a.p_n, a.p_s = b.p_s reversed).
func selfCorrelatesOnOneName(sql string) (string, bool) {
	for _, m := range correlationPairRe.FindAllStringSubmatch(sql, -1) {
		leftCol, rightCol := m[2], m[4]
		isCorrelation := strings.HasPrefix(leftCol, "p_") || strings.HasPrefix(rightCol, "p_")
		if isCorrelation && strings.EqualFold(leftCol, rightCol) {
			return m[0], true
		}
	}
	return "", false
}

// The invariant above is only as good as its ability to REJECT. These are the
// self-correlated mutations the substring check it replaced let through:
// keying on the wrong column pair, reversing the operands, and renaming the
// outer alias. Each must be caught, and each real corpus pair must pass.
func TestSelfCorrelationCheckerCatchesEveryMutation(t *testing.T) {
	mustCatch := []struct{ name, sql string }{
		{"different column pair", `EXISTS (SELECT 1 FROM dn_inner b WHERE b.p_n = a.p_n AND b.p_g = a.p_g)`},
		{"operands reversed", `EXISTS (SELECT 1 FROM dn_inner b WHERE a.p_s = b.p_s)`},
		{"outer alias renamed", `EXISTS (SELECT 1 FROM dn_inner b WHERE b.p_s = x.p_s)`},
		{"the shape it was written for", `EXISTS (SELECT 1 FROM dn_inner b WHERE b.p_s = a.p_s)`},
	}
	for _, m := range mustCatch {
		if _, bad := selfCorrelatesOnOneName(m.sql); !bad {
			t.Errorf("mutation %q was NOT caught: %s", m.name, m.sql)
		}
	}

	mustPass := []string{
		"b.q_s = a.p_s",   // the ordinary correlation
		"b1.q_s = a.p_s",  // over a self-joined inner
		"k.k_k = b.q_g",   // the dim join's ON, not a correlation
		"b1.q_g = b2.q_g", // the self-join's ON, both inner, same bare name and fine
		"b.q_n = a.p_n AND b.q_s = a.p_s",
	}
	for _, sql := range mustPass {
		if pair, bad := selfCorrelatesOnOneName(sql); bad {
			t.Errorf("real pair set %q flagged on %q", sql, pair)
		}
	}

	// And every real dn_ entry must pass — the same assertion the corpus test
	// makes, restated here so this test fails on its own if an entry regresses.
	for _, c := range Corpus() {
		if !strings.HasPrefix(c.Name, "dn_") {
			continue
		}
		if pair, bad := selfCorrelatesOnOneName(c.SQL); bad {
			t.Errorf("%s flagged on %q", c.Name, pair)
		}
	}
}
