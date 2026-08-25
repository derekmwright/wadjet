package triage

import (
	"compress/gzip"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
)

// Fixtures below are real snippets pulled verbatim from the 2026-08-25
// standing soak's logs under scratchpad/soak-run-0825b/ and scratchpad/
// soak-run/ (see wadjet#289), except norecMismatchFixture,
// pqsMismatchFixture, and certMismatchFixture: no NoREC, PQS, or CERT run
// in any soak this harness has ever done found a violation, so there is
// no real occurrence to pull from a log for any of the three. Those are
// instead built to match their source method's exact format string
// (NoRECOracle.check / PivotedQuerySynthesisBase.reportMissingPivotRow /
// CERTOracle.check in SQLancer 2.0.0, /tmp/sqlancer-wadjet) byte-for-byte.

// Real: soak-run-0825b/HAVING-seed3001/sqlancer.log, lines 276-286 —
// ComparatorHelper.assumeResultSetsAreEqual's "size" variant.
const tlpSizeMismatchFixture = `java.lang.AssertionError: The size of the result sets mismatch (14 and 0)!
First query: "SELECT t3.c1 FROM t3, t0 INNER JOIN t1 ON t0.c1 CROSS JOIN t2 GROUP BY t2.c0, t0.c1, ((t3.c0)||(((((((((t0.c4)OR(t3.c3)))OR(((t0.c3)AND(t0.c3)))))OR(((t1.c0)LIKE(t1.c0)))))AND(t3.c5))))", whose cardinality is: 14
Second query:"SELECT t3.c1 FROM t3, t0 JOIN t1 ON t0.c1 CROSS JOIN t2 GROUP BY t2.c0, t0.c1, ((t3.c0)||(((((((((t0.c4)OR(t3.c3)))OR(((t0.c3)AND(t0.c3)))))OR(((t1.c0)LIKE(t1.c0)))))AND(t3.c5)))) HAVING t0.c3;SELECT t3.c1 FROM t3, t0 JOIN t1 ON t0.c1 CROSS JOIN t2 GROUP BY t2.c0, t0.c1, ((t3.c0)||(((((((((t0.c4)OR(t3.c3)))OR(((t0.c3)AND(t0.c3)))))OR(((t1.c0)LIKE(t1.c0)))))AND(t3.c5)))) HAVING NOT (t0.c3);SELECT t3.c1 FROM t3, t0 INNER JOIN t1 ON t0.c1 CROSS JOIN t2 GROUP BY t2.c0, t0.c1, ((t3.c0)||(((((((((t0.c4)OR(t3.c3)))OR(((t0.c3)AND(t0.c3)))))OR(((t1.c0)LIKE(t1.c0)))))AND(t3.c5)))) HAVING (t0.c3) IS NULL", whose cardinality is: 0
	at sqlancer.ComparatorHelper.assumeResultSetsAreEqual(ComparatorHelper.java:105)
	at sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle.havingCheck(PostgresTLPHavingOracle.java:50)
	at sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle.check(PostgresTLPHavingOracle.java:25)
	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
	at sqlancer.Main$DBMSExecutor.run(Main.java:483)
	at sqlancer.Main$2.run(Main.java:734)
	at sqlancer.Main$2.runThread(Main.java:716)
	at sqlancer.Main$2.run(Main.java:707)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base/java.lang.Thread.run(Thread.java:840)`

// Real: soak-run-0825b/QP-seed3001/sqlancer.log, lines 1186-1197 —
// ComparatorHelper.assumeResultSetsAreEqual's "content" variant.
const tlpContentMismatchFixture = `java.lang.AssertionError: The content of the result sets mismatch!
First query : "SELECT t4.c0, t1.c1, t0.c1, t0.c0, t0.c2, t1.c3, t1.c2, t1.c5 FROM t0, t4 CROSS JOIN t1"
Second query: "-- Query: "SELECT t4.c0, t1.c1, t0.c1, t0.c0, t0.c2, t1.c3, t1.c2, t1.c5 FROM t0, t4 CROSS JOIN t1 WHERE t0.c0 UNION ALL SELECT t4.c0, t1.c1, t0.c1, t0.c0, t0.c2, t1.c3, t1.c2, t1.c5 FROM t0, t4 CROSS JOIN t1 WHERE NOT (t0.c0) UNION ALL SELECT t4.c0, t1.c1, t0.c1, t0.c0, t0.c2, t1.c3, t1.c2, t1.c5 FROM t0, t4 CROSS JOIN t1 WHERE (t0.c0) IS NULL"; It misses: "[null]""
	at sqlancer.ComparatorHelper.assumeResultSetsAreEqual(ComparatorHelper.java:128)
	at sqlancer.common.oracle.TLPWhereOracle.check(TLPWhereOracle.java:193)
	at sqlancer.common.oracle.CompositeTestOracle.check(CompositeTestOracle.java:22)
	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
	at sqlancer.Main$DBMSExecutor.run(Main.java:483)
	at sqlancer.Main$2.run(Main.java:734)
	at sqlancer.Main$2.runThread(Main.java:716)
	at sqlancer.Main$2.run(Main.java:707)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base/java.lang.Thread.run(Thread.java:840)`

// Real: soak-run-0825b/QP-seed3001/sqlancer.log, lines 2936-2949 —
// PostgresTLPAggregateOracle.aggregateCheck.
const tlpAggregateMismatchFixture = `java.lang.AssertionError: the results mismatch!
-- SELECT BOOL_AND(t3.c5) FROM t4, t0 FULL OUTER JOIN t3 ON t4.c1;
-- result: f
-- SELECT BOOL_AND(agg0) FROM (SELECT BOOL_AND(t3.c5)  as agg0 FROM t4, t0 FULL OUTER JOIN t3 ON t4.c1 WHERE t0.c1 UNION ALL SELECT BOOL_AND(t3.c5)  as agg0 FROM t4, t0 FULL OUTER JOIN t3 ON t4.c1 WHERE NOT (t0.c1) UNION ALL SELECT BOOL_AND(t3.c5)  as agg0 FROM t4, t0 FULL OUTER JOIN t3 ON t4.c1 WHERE (t0.c1) IS NULL) as asdf;
-- result: t
	at sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle.aggregateCheck(PostgresTLPAggregateOracle.java:83)
	at sqlancer.postgres.oracle.tlp.PostgresTLPAggregateOracle.check(PostgresTLPAggregateOracle.java:47)
	at sqlancer.common.oracle.CompositeTestOracle.check(CompositeTestOracle.java:22)
	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
	at sqlancer.Main$DBMSExecutor.run(Main.java:483)
	at sqlancer.Main$2.run(Main.java:734)
	at sqlancer.Main$2.runThread(Main.java:716)
	at sqlancer.Main$2.run(Main.java:707)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)`

// Real: soak-run-0825b/HAVING-seed3001/sqlancer.log, lines 6-24 — an
// ordinary "unexpected error" (SQLQueryAdapter.checkException, the join-ON-
// residual class), NOT a genuine violation. Its message is bare, unquoted
// SQL text with no "mismatch" wording at all — the exact shape a PQS
// violation's AssertionError(query) also has, which is why the stack trace
// (not the message) has to be the discriminator.
const unexpectedErrorFixture = `java.lang.AssertionError: SELECT MAX('A') FROM t0 RIGHT OUTER JOIN (SELECT t0.c2 FROM t0) AS sub0 ON t0.c0 RIGHT OUTER JOIN (SELECT t3.c3 FROM t0, t3, t2 LIMIT 1644114242785501602) AS sub1 ON NOT (t0.c0) RIGHT OUTER JOIN (SELECT - (t0.c1) FROM t1, t0, t3) AS sub2 ON ((((CAST((t0.c2) BETWEEN SYMMETRIC (t0.c2) AND (t0.c2) AS INT))/(t0.c1)))<(t0.c1)) GROUP BY t0.c2, (CAST((((('353R}rT')||('')))||(t0.c0)) AS INT))::VARCHAR, t0.c0;
	at sqlancer.common.query.SQLQueryAdapter.checkException(SQLQueryAdapter.java:166)
	at sqlancer.common.query.SQLQueryAdapter.internalExecuteAndGet(SQLQueryAdapter.java:207)
	at sqlancer.common.query.SQLQueryAdapter.executeAndGet(SQLQueryAdapter.java:177)
	at sqlancer.common.query.SQLQueryAdapter.executeAndGet(SQLQueryAdapter.java:172)
	at sqlancer.ComparatorHelper.getResultSetFirstColumnAsString(ComparatorHelper.java:56)
	at sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle.havingCheck(PostgresTLPHavingOracle.java:35)
	at sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle.check(PostgresTLPHavingOracle.java:25)
	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
	at sqlancer.Main$DBMSExecutor.run(Main.java:483)
	at sqlancer.Main$2.run(Main.java:734)
	at sqlancer.Main$2.runThread(Main.java:716)
	at sqlancer.Main$2.run(Main.java:707)
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635)
	at java.base/java.lang.Thread.run(Thread.java:840)
Caused by: org.postgresql.util.PSQLException: ERROR: physical plan: join ON "((((cast((t0.c2) between symmetric(t0.c2) and (t0.c2) as int)) / (t0.c1))) < (t0.c1))": (((cast((t0.c2) between symmetric(t0.c2) and (t0.c2) as int)) / (t0.c1))) < (t0.c1) cannot be represented as an equi-join key (the right join executor matches on column names, and only an equality between two bare columns is one); it must be lifted into a filter above the join, which is legal for an inner join only
	at org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2676)
	at org.postgresql.core.v3.QueryExecutorImpl.processResults(QueryExecutorImpl.java:2366)
	at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:356)
	at sqlancer.common.query.SQLQueryAdapter.internalExecuteAndGet(SQLQueryAdapter.java:196)
	... 13 more`

// Real: soak-run-0825b/WHERE-seed3001/sqlancer.log, lines 849-850 — the
// JDBC-side symptom of a dead wadjet server.
const crashEchoIOFixture = `org.postgresql.util.PSQLException: An I/O error occurred while sending to the backend.
	at org.postgresql.core.v3.QueryExecutorImpl.execute(QueryExecutorImpl.java:383)
	at org.postgresql.jdbc.PgStatement.executeInternal(PgStatement.java:496)`

// Real: soak-run/HAVING-seed1004/crash-stacktraces.log, lines 71-78 — a
// wadjet server panic (the #508 class: FatalEvalPanic escaping a build-side
// goroutine with no recovery).
const serverPanicFixture = `panic: invalid input syntax for type integer: ""

goroutine 31300 [running]:
github.com/derekmwright/wadjet/internal/engine/expr.raiseInvalidTextRepresentation({0x1c16ed2?, 0x0?}, {0x0, 0x0})
	/home/dwright/Projects/caelum/internal/engine/expr/fatal.go:34 +0x108
github.com/derekmwright/wadjet/internal/engine/expr.(*Cast).Eval(0x102418a22e20, 0x1024192e6d80, 0x0)
	/home/dwright/Projects/caelum/internal/engine/expr/expr.go:4918 +0x52d`

// Synthetic: no NoREC run in any soak (pilot, full soak, or the 2026-08-25
// standing soak) found a violation to pull a real snippet from. Built to
// match NoRECOracle.check's exact format string in SQLancer 2.0.0
// (src/sqlancer/common/oracle/NoRECOracle.java):
//
//	String assertionMessage = String.format("the counts mismatch (%d and %d)!\n%s\n%s", ...)
const norecMismatchFixture = `java.lang.AssertionError: the counts mismatch (5 and 7)!
-- SELECT COUNT(*) FROM t0 WHERE t0.c0;
-- count: 5
-- SELECT SUM(agg0) FROM (SELECT (t0.c0) IS TRUE as agg0 FROM t0) as res;
-- count: 7
	at sqlancer.common.oracle.NoRECOracle.check(NoRECOracle.java:162)
	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
	at sqlancer.Main$DBMSExecutor.run(Main.java:483)`

// Synthetic: no PQS run in any soak found a violation either. Built to
// match PivotedQuerySynthesisBase.reportMissingPivotRow's exact shape:
// `throw new AssertionError(query)` where query.toString() is the bare,
// semicolon-terminated SQL string — the same shape unexpectedErrorFixture
// has, which is why the stack frame below (not the message) is what makes
// this PQS.
const pqsMismatchFixture = `java.lang.AssertionError: SELECT t0.c0 FROM t0 WHERE t0.c0 = 1;
	at sqlancer.common.oracle.PivotedQuerySynthesisBase.reportMissingPivotRow(PivotedQuerySynthesisBase.java:82)
	at sqlancer.common.oracle.PivotedQuerySynthesisBase.check(PivotedQuerySynthesisBase.java:47)
	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
	at sqlancer.Main$DBMSExecutor.run(Main.java:483)`

// Synthetic: no CERT run in any soak found a violation either (the
// README advises against running CERT at all, for reasons unrelated to
// whether this classifier can recognize its output). Built to match
// CERTOracle.check's exact shape:
// `throw new AssertionError("Inconsistent result for query: " + queryString1
// + "; --" + rowCount1 + "\n" + queryString2 + "; --" + rowCount2)`. Note
// the second query line is NOT quoted and NOT "-- "-prefixed like NoREC's/
// TLP-Aggregate's own embedded queries — extractQueries falls back to
// returning the whole message as one string for this shape (see its doc
// comment).
const certMismatchFixture = `java.lang.AssertionError: Inconsistent result for query: SELECT COUNT(*) FROM t0; --5
SELECT COUNT(*) FROM t0 WHERE t0.c1 > 0; --3
	at sqlancer.common.oracle.CERTOracle.check(CERTOracle.java:84)
	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
	at sqlancer.Main$DBMSExecutor.run(Main.java:483)`

// Real: soak-run/HAVING-seed1002/logs/wadjet/database3.log, lines 1-9 — a
// genuine TLP-HAVING violation captured ONLY in SQLancer's own
// "--"-commented per-round reproduction-log form (this is exactly a
// logs/wadjet/database<N>[-cur].log file — the file
// tools/sqlancer/README.md's "Reproducing a finding from a seed" section
// directly points readers at). This file has no unprefixed counterpart
// anywhere: state.getState().getLocalState().log(...) writes only the
// "--"-commented echo here, never the raw exception print a session's
// captured stdout also carries alongside it. A classifier that treats
// every "--"-prefixed line as a duplicate to skip (needed for session
// logs — see TestClassifyDedupesEchoedDuplicate) would find nothing at
// all in a file shaped like this and silently report 0 — see
// hasUnprefixedAssertionHeader / stripLeadingDashDash in classify.go.
const roundLogEchoOnlyFixture = `--java.lang.AssertionError: The size of the result sets mismatch (7 and 0)!
--First query: "SELECT t2.c1 FROM t0, t3, t2 LEFT OUTER JOIN t5 ON NOT (t1.c6) JOIN t1 ON t3.c5 GROUP BY t1.c1", whose cardinality is: 7
--Second query:"SELECT t2.c1 FROM t0, t3, t2 LEFT OUTER JOIN t5 ON NOT (t1.c6) JOIN t1 ON t3.c5 GROUP BY t1.c1 HAVING t0.c3;SELECT t2.c1 FROM t0, t3, t2 LEFT OUTER JOIN t5 ON NOT (t1.c6) INNER JOIN t1 ON t3.c5 GROUP BY t1.c1 HAVING NOT (t0.c3);SELECT t2.c1 FROM t0, t3, t2 LEFT OUTER JOIN t5 ON NOT (t1.c6) JOIN t1 ON t3.c5 GROUP BY t1.c1 HAVING (t0.c3) IS NULL", whose cardinality is: 0
--	at sqlancer.ComparatorHelper.assumeResultSetsAreEqual(ComparatorHelper.java:105)
--	at sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle.havingCheck(PostgresTLPHavingOracle.java:50)
--	at sqlancer.postgres.oracle.tlp.PostgresTLPHavingOracle.check(PostgresTLPHavingOracle.java:25)
--	at sqlancer.ProviderAdapter.generateAndTestDatabase(ProviderAdapter.java:61)
--	at sqlancer.Main$DBMSExecutor.run(Main.java:483)
--	at sqlancer.Main$2.run(Main.java:734)
--	at sqlancer.Main$2.runThread(Main.java:716)
--	at sqlancer.Main$2.run(Main.java:707)
--	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136)`

func classifyText(t *testing.T, text string) *Report {
	t.Helper()
	r := NewReport()
	if err := r.Classify("fixture", strings.NewReader(text)); err != nil {
		t.Fatalf("Classify: %v", err)
	}
	return r
}

func TestClassify(t *testing.T) {
	tests := []struct {
		name             string
		log              string
		wantCategory     Category
		wantQuery        string // substring expected somewhere in the finding's extracted queries; "" skips the check
		wantOracleCheck  string // exact Finding.OracleCheck; "" skips the check (crash echoes and unexpected errors never set it)
		wantDetailSubstr string // substring expected in the finding's raw Detail even with no Queries extracted; "" skips the check
	}{
		{
			name:            "TLP WHERE/HAVING size mismatch",
			log:             tlpSizeMismatchFixture,
			wantCategory:    CategoryTLPResultSet,
			wantQuery:       "GROUP BY t2.c0",
			wantOracleCheck: "TLP-HAVING",
		},
		{
			name:            "TLP WHERE/HAVING content mismatch",
			log:             tlpContentMismatchFixture,
			wantCategory:    CategoryTLPResultSet,
			wantQuery:       "CROSS JOIN t1",
			wantOracleCheck: "TLP-WHERE",
		},
		{
			name:            "TLP-Aggregate mismatch",
			log:             tlpAggregateMismatchFixture,
			wantCategory:    CategoryTLPAggregate,
			wantQuery:       "BOOL_AND",
			wantOracleCheck: "TLP-AGGREGATE",
		},
		{
			name:            "NoREC mismatch",
			log:             norecMismatchFixture,
			wantCategory:    CategoryNoREC,
			wantQuery:       "SELECT COUNT(*)",
			wantOracleCheck: "NoREC",
		},
		{
			name:            "PQS violation, distinguished from an unexpected error purely by stack frame",
			log:             pqsMismatchFixture,
			wantCategory:    CategoryPQS,
			wantQuery:       "SELECT t0.c0",
			wantOracleCheck: "PQS",
		},
		{
			name:             "CERT violation — its message is unquoted like PQS's, but its own distinct text ('Inconsistent result for query:') needs no stack-frame check to classify",
			log:              certMismatchFixture,
			wantCategory:     CategoryCERT,
			wantOracleCheck:  "CERT",
			wantDetailSubstr: "Inconsistent result for query:",
		},
		{
			name:            "TLP-HAVING violation captured ONLY in the '--'-commented per-round log form (logs/wadjet/database<N>[-cur].log) — no unprefixed counterpart exists anywhere in this file",
			log:             roundLogEchoOnlyFixture,
			wantCategory:    CategoryTLPResultSet,
			wantQuery:       "GROUP BY t1.c1",
			wantOracleCheck: "TLP-HAVING",
		},
		{
			name:         "unexpected error (join ON residual) is not a mismatch or a PQS violation despite an unquoted-SQL message identical in shape to PQS's",
			log:          unexpectedErrorFixture,
			wantCategory: CategoryUnexpectedError,
		},
		{
			name:         "crash echo: I/O error to a dead backend",
			log:          crashEchoIOFixture,
			wantCategory: CategoryCrashEcho,
		},
		{
			name:             "crash echo: wadjet server panic",
			log:              serverPanicFixture,
			wantCategory:     CategoryCrashEcho,
			wantDetailSubstr: "panic: invalid input syntax for type integer",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := classifyText(t, tc.log)

			total := 0
			for _, n := range r.Counts {
				total += n
			}
			if total != 1 {
				t.Fatalf("Counts sum to %d, want exactly 1 across all categories: %+v", total, r.Counts)
			}
			if got := r.Counts[tc.wantCategory]; got != 1 {
				t.Errorf("Counts[%s] = %d, want 1; full counts: %+v", tc.wantCategory, got, r.Counts)
			}

			if tc.wantQuery != "" {
				var gotQueries []string
				for _, f := range r.Findings {
					gotQueries = append(gotQueries, f.Queries...)
				}
				found := false
				for _, q := range gotQueries {
					if strings.Contains(q, tc.wantQuery) {
						found = true
					}
				}
				if !found {
					t.Errorf("no extracted query contains %q; got %v", tc.wantQuery, gotQueries)
				}
			}

			if tc.wantOracleCheck != "" {
				found := false
				for _, f := range r.Findings {
					if f.OracleCheck == tc.wantOracleCheck {
						found = true
					}
				}
				if !found {
					var got []string
					for _, f := range r.Findings {
						got = append(got, f.OracleCheck)
					}
					t.Errorf("no finding has OracleCheck %q; got %v", tc.wantOracleCheck, got)
				}
			}

			if tc.wantDetailSubstr != "" {
				found := false
				for _, f := range r.Findings {
					if strings.Contains(f.Detail, tc.wantDetailSubstr) {
						found = true
					}
				}
				if !found {
					var got []string
					for _, f := range r.Findings {
						got = append(got, f.Detail)
					}
					t.Errorf("no finding's Detail contains %q; got %v", tc.wantDetailSubstr, got)
				}
			}
		})
	}
}

// TestClassifyDedupesEchoedDuplicate guards against SQLancer's own
// behavior: it echoes every failure a second time, "--"-prefixed, into
// the round's reproduction log, and a naive grep over that counts each
// real violation twice.
func TestClassifyDedupesEchoedDuplicate(t *testing.T) {
	lines := strings.Split(tlpSizeMismatchFixture, "\n")
	echoed := make([]string, len(lines))
	for i, l := range lines {
		echoed[i] = "--" + l
	}
	log := tlpSizeMismatchFixture + "\n" + strings.Join(echoed, "\n")

	r := classifyText(t, log)
	if got := r.Counts[CategoryTLPResultSet]; got != 1 {
		t.Errorf("Counts[CategoryTLPResultSet] = %d, want 1 (echoed duplicate must not double-count)", got)
	}
}

// TestClassifyFileGzip exercises the transparent-gunzip path ClassifyFile
// needs for real soak logs, which get gzip'd after each session to bound
// disk use (see tools/sqlancer/README.md, "Reproducing a finding from a
// seed").
func TestClassifyFileGzip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "session-1.log.gz")

	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	gz := gzip.NewWriter(f)
	if _, err := gz.Write([]byte(tlpAggregateMismatchFixture)); err != nil {
		t.Fatalf("gzip Write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip Close: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := NewReport()
	if err := r.ClassifyFile(path); err != nil {
		t.Fatalf("ClassifyFile: %v", err)
	}
	if got := r.Counts[CategoryTLPAggregate]; got != 1 {
		t.Errorf("Counts[CategoryTLPAggregate] = %d, want 1", got)
	}
}

func TestCategoryIsGenuineViolation(t *testing.T) {
	genuine := []Category{CategoryTLPResultSet, CategoryTLPAggregate, CategoryNoREC, CategoryPQS, CategoryCERT}
	notGenuine := []Category{CategoryOther, CategoryCrashEcho, CategoryUnexpectedError}

	for _, c := range genuine {
		if !c.IsGenuineViolation() {
			t.Errorf("%s.IsGenuineViolation() = false, want true", c)
		}
	}
	for _, c := range notGenuine {
		if c.IsGenuineViolation() {
			t.Errorf("%s.IsGenuineViolation() = true, want false", c)
		}
	}
}

// TestReportZeroValueDoesNotPanic guards against a bare &Report{} (Counts
// left nil, not built through NewReport) panicking on its first write —
// Counts is a public field and nothing stops a caller from constructing
// one directly.
func TestReportZeroValueDoesNotPanic(t *testing.T) {
	r := &Report{}
	if err := r.Classify("fixture", strings.NewReader(tlpSizeMismatchFixture)); err != nil {
		t.Fatalf("Classify: %v", err)
	}
	if got := r.Counts[CategoryTLPResultSet]; got != 1 {
		t.Errorf("Counts[CategoryTLPResultSet] = %d, want 1", got)
	}
}

// TestClassifyReturnsPartialResultsOnScanError guards against a single bad
// read (a pathological line past maxLineBytes, a truncated gzip stream,
// anything bufio.Scanner surfaces as a non-nil Err()) discarding every
// line successfully read before it. The old behavior collected lines into
// a slice, then only classified them if the scan finished with no error —
// one bad line anywhere in a soak log threw away every genuine violation
// found earlier in that same file and reported "No genuine violations",
// silently, with a 0 exit status.
func TestClassifyReturnsPartialResultsOnScanError(t *testing.T) {
	boom := errors.New("boom")
	rd := io.MultiReader(strings.NewReader(tlpSizeMismatchFixture+"\n"), iotest.ErrReader(boom))

	r := NewReport()
	err := r.Classify("fixture", rd)
	if err == nil {
		t.Fatal("Classify: want a non-nil error when the underlying reader fails, got nil")
	}
	if !errors.Is(err, boom) {
		t.Errorf("Classify error = %v, want it to wrap %v", err, boom)
	}
	if got := r.Counts[CategoryTLPResultSet]; got != 1 {
		t.Errorf("Counts[CategoryTLPResultSet] = %d, want 1 — the line read before the scan error must still be classified", got)
	}
}

// TestClassifyDoesNotStripSessionLogsWithNoAssertions guards against a
// regression the WHERE-oracle soak's own logs found immediately: a
// session log dominated by a crash-restart loop can have thousands of
// unprefixed crash-echo lines (every query attempted against a dead
// server) and exactly zero unprefixed assertion headers, because no
// oracle check ever got to run to completion. Triggering the "--"-only
// round-log fallback (see TestClassify's roundLogEchoOnlyFixture case) on
// assertion-header absence ALONE would strip this session log's own
// crash lines' "--"-prefixed echo duplicates too, double-counting every
// one of them.
func TestClassifyDoesNotStripSessionLogsWithNoAssertions(t *testing.T) {
	plain := "org.postgresql.util.PSQLException: An I/O error occurred while sending to the backend."
	log := plain + "\n" + "--" + plain

	r := classifyText(t, log)
	if got := r.Counts[CategoryCrashEcho]; got != 1 {
		t.Errorf("Counts[CategoryCrashEcho] = %d, want 1 (the echoed duplicate must not be un-skipped and double-counted)", got)
	}
}
