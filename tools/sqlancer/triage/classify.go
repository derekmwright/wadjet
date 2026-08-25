// Package triage classifies SQLancer soak-log output (and the wadjet
// server logs captured alongside it) the way wadjet#289's harness actually
// needs, which is not the way its README long documented.
//
// The README's triage grep was `"counts mismatch" / "mismatch:"`. That
// matches NoREC's own assertion text, but NOT the TLP family's:
// ComparatorHelper.assumeResultSetsAreEqual (shared by TLP-WHERE,
// TLP-HAVING, and QUERY_PARTITIONING's composite) throws "The size of the
// result sets mismatch (%d and %d)!" or "The content of the result sets
// mismatch!" — no colon after "mismatch", and not "counts". TLP-Aggregate
// throws its own "the results mismatch!". A soak triaged with the old grep
// could not have reported a genuine TLP violation even if one occurred —
// see wadjet#289's 2026-08-25 methodology finding.
//
// PQS is harder still: PivotedQuerySynthesisBase.reportMissingPivotRow
// throws `new AssertionError(query)`, where query is a Query whose
// toString() is just the raw SQL string — byte-for-byte the same shape as
// the ordinary "wadjet rejected this SQL with a message Postgres's
// ExpectedErrors list doesn't recognize" AssertionError that
// SQLQueryAdapter.checkException throws everywhere else in the harness.
// The only way to tell them apart is the stack trace: a genuine PQS
// violation's frames include
// sqlancer.common.oracle.PivotedQuerySynthesisBase.reportMissingPivotRow.
package triage

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
)

// Category is the bucket one classified block of log output falls into.
type Category int

const (
	// CategoryOther is ordinary log content — nothing a triage pass needs
	// to look at twice.
	CategoryOther Category = iota
	// CategoryTLPResultSet is a genuine TLP-WHERE or TLP-HAVING violation:
	// ComparatorHelper.assumeResultSetsAreEqual threw.
	CategoryTLPResultSet
	// CategoryTLPAggregate is a genuine TLP-Aggregate violation (the
	// "aggregateCheck" arm QUERY_PARTITIONING composes in).
	CategoryTLPAggregate
	// CategoryNoREC is a genuine NoREC violation (NoRECOracle.check).
	CategoryNoREC
	// CategoryPQS is a genuine PQS violation
	// (PivotedQuerySynthesisBase.reportMissingPivotRow).
	CategoryPQS
	// CategoryCrashEcho is a wadjet process crash: either observed
	// directly (a Go "panic:"/"fatal error:" line from a wadjet server
	// log) or its symptom on the SQLancer/JDBC side (connection
	// refused/reset, an I/O error sending to the backend) — plus the
	// harness's own JVM heap exhaustion, which reads the same way at
	// triage time (the run is dead, not a wadjet answer). A raw line
	// count here is NOT a crash-event count: one dead wadjet process
	// produces a flood of near-identical connection-refused lines from
	// every query attempted until the session's timeout, so this
	// category exists to keep that flood out of CategoryUnexpectedError,
	// not to size it. The soak supervisor's own restart count (its
	// crashes.log / summary.txt) is the authoritative crash-event count.
	CategoryCrashEcho
	// CategoryUnexpectedError is an AssertionError SQLancer raised because
	// wadjet's response to a generated statement didn't match anything in
	// the Postgres dialect's ExpectedErrors list — a SQL-surface gap or a
	// by-design loud rejection, not an oracle-detected wrong answer.
	CategoryUnexpectedError
)

func (c Category) String() string {
	switch c {
	case CategoryTLPResultSet:
		return "TLP (WHERE/HAVING) violation"
	case CategoryTLPAggregate:
		return "TLP-Aggregate violation"
	case CategoryNoREC:
		return "NoREC violation"
	case CategoryPQS:
		return "PQS violation"
	case CategoryCrashEcho:
		return "crash/panic/connection-loss"
	case CategoryUnexpectedError:
		return "unexpected error (noise)"
	default:
		return "other"
	}
}

// IsGenuineViolation reports whether c is one of the four oracle-detected
// wrong-answer categories, as opposed to noise or a crash echo.
func (c Category) IsGenuineViolation() bool {
	switch c {
	case CategoryTLPResultSet, CategoryTLPAggregate, CategoryNoREC, CategoryPQS:
		return true
	default:
		return false
	}
}

// Finding is one classified block of log output worth keeping around —
// every genuine violation, plus the Go-side panic/fatal-error lines from a
// crash echo (the connection-refused/reset flood a dead server produces on
// the SQLancer side is tallied in Counts but not retained; there is
// nothing more to learn from the 4000th "Connection refused" than the
// 1st).
type Finding struct {
	Category Category
	Source   string // file path this was found in
	Line     int    // 1-based line number of the header line
	Header   string // the AssertionError/panic header line itself
	Detail   string // header plus any message-continuation lines, before the stack trace
	Queries  []string
}

// Report accumulates classification results across one or more sources.
type Report struct {
	Counts   map[Category]int
	Findings []Finding
}

// NewReport returns an empty Report ready for Classify/ClassifyFile calls.
func NewReport() *Report {
	return &Report{Counts: make(map[Category]int)}
}

// ClassifyFile opens path (transparently gunzipping a ".gz" suffix) and
// classifies its lines into r.
func (r *Report) ClassifyFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var rd io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return fmt.Errorf("gzip: %w", err)
		}
		defer gz.Close()
		rd = gz
	}
	return r.Classify(path, rd)
}

// Classify reads every line from rd and classifies it into r, attributing
// findings to source (typically a file path, or "" for an ad hoc snippet).
func (r *Report) Classify(source string, rd io.Reader) error {
	sc := bufio.NewScanner(rd)
	// SQLancer's generated queries (especially the UNION ALL triples TLP
	// builds) can run past bufio.Scanner's 64KB default token size.
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	var lines []string
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("reading %s: %w", source, err)
	}
	r.classifyLines(source, lines)
	return nil
}

// assertionHeaderPrefix is how every uncaught java.lang.AssertionError with
// a non-null message prints, in both an oracle's own stdout and its "-- "
// echo into the round's reproduction log.
const assertionHeaderPrefix = "java.lang.AssertionError: "

var (
	pqsFrame = "PivotedQuerySynthesisBase.reportMissingPivotRow"

	// crashSignatures are matched as plain substrings against any
	// non-echoed line. Each entry is specific enough that it doesn't
	// occur in ordinary generated SQL or ExpectedErrors noise.
	crashSignatures = []string{
		"panic: ",
		"fatal error: ",
		"PSQLException: An I/O error occurred while sending to the backend",
		"java.net.ConnectException: Connection refused",
		"java.net.SocketException: Connection reset",
		"This connection has been closed",
		"OutOfMemoryError: Java heap space",
	}

	quotedQuery = regexp.MustCompile(`"([^"]{8,})"`)
)

func (r *Report) classifyLines(source string, lines []string) {
	for i := 0; i < len(lines); i++ {
		line := lines[i]
		// SQLancer echoes every failure a second time, prefixed with "--",
		// into the round's reproduction log for later replay. Counting
		// both would double every count.
		if strings.HasPrefix(line, "--") {
			continue
		}

		if strings.HasPrefix(line, assertionHeaderPrefix) {
			i = r.classifyAssertion(source, lines, i)
			continue
		}

		for _, sig := range crashSignatures {
			if strings.Contains(line, sig) {
				r.Counts[CategoryCrashEcho]++
				if strings.HasPrefix(line, "panic: ") || strings.HasPrefix(line, "fatal error: ") {
					r.Findings = append(r.Findings, Finding{
						Category: CategoryCrashEcho,
						Source:   source,
						Line:     i + 1,
						Header:   line,
						Detail:   line,
					})
				}
				break
			}
		}
	}
}

// classifyAssertion handles the AssertionError block starting at lines[i]
// (already confirmed to have the header prefix). It returns the index of
// the last line it consumed, so the caller's loop can resume after it.
func (r *Report) classifyAssertion(source string, lines []string, i int) int {
	header := lines[i]
	message := header[len(assertionHeaderPrefix):]

	category := CategoryUnexpectedError
	switch {
	case strings.Contains(message, "The size of the result sets mismatch"),
		strings.Contains(message, "The content of the result sets mismatch"):
		category = CategoryTLPResultSet
	case strings.Contains(message, "the results mismatch!"):
		category = CategoryTLPAggregate
	case strings.Contains(message, "the counts mismatch"):
		category = CategoryNoREC
	}

	detailLines := []string{header}
	inStack := false
	j := i + 1
	for ; j < len(lines); j++ {
		l := lines[j]
		if strings.HasPrefix(l, "--"+assertionHeaderPrefix) {
			// The echoed duplicate of this same block's header — definitely
			// the end. A bare "--" prefix is NOT enough to conclude that on
			// its own: NoREC's and TLP-Aggregate's own (non-echoed) message
			// text legitimately embeds "-- <query>;" lines (their format
			// strings SQL-comment each embedded query), so a message line
			// starting with a single "--" can be genuine content, not the
			// echo.
			break
		}
		trimmed := strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(trimmed, "at "):
			inStack = true
			if category == CategoryUnexpectedError && strings.Contains(trimmed, pqsFrame) {
				category = CategoryPQS
			}
		case strings.HasPrefix(trimmed, "Caused by:"):
			inStack = true
		case strings.HasPrefix(trimmed, "... ") && strings.HasSuffix(trimmed, "more"):
			inStack = true
		case !inStack:
			detailLines = append(detailLines, l)
		default:
			// A non-stack-trace line after the stack trace already
			// started: this block is over, and j hasn't been consumed.
			goto done
		}
	}
done:
	if category == CategoryUnexpectedError {
		r.Counts[CategoryUnexpectedError]++
		return j - 1
	}

	detail := strings.Join(detailLines, "\n")
	r.Counts[category]++
	r.Findings = append(r.Findings, Finding{
		Category: category,
		Source:   source,
		Line:     i + 1,
		Header:   header,
		Detail:   detail,
		Queries:  extractQueries(detail),
	})
	return j - 1
}

// extractQueries pulls out the best-effort "minimized query" text embedded
// in a violation's message. The three genuine-violation shapes each embed
// it differently:
//   - TLP-WHERE/HAVING (ComparatorHelper) quotes each query inline.
//   - TLP-Aggregate and NoREC embed each query as its own "-- <query>;"
//     SQL-comment line instead (see PostgresTLPAggregateOracle.aggregateCheck
//     / NoRECOracle.check's format strings) — no quoting at all.
//   - PQS's AssertionError(query) message IS the bare query string, with
//     neither quoting nor a comment prefix.
func extractQueries(detail string) []string {
	if matches := quotedQuery.FindAllStringSubmatch(detail, -1); len(matches) > 0 {
		queries := make([]string, 0, len(matches))
		for _, m := range matches {
			queries = append(queries, m[1])
		}
		return queries
	}

	var queries []string
	for _, line := range strings.Split(detail, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "-- ") {
			continue
		}
		line = strings.TrimPrefix(line, "-- ")
		if strings.HasPrefix(line, "result:") || strings.HasPrefix(line, "count:") {
			continue // the paired "-- result: %s" / "-- count: %d" annotation, not a query
		}
		queries = append(queries, line)
	}
	if len(queries) > 0 {
		return queries
	}

	firstLine := detail
	if idx := strings.IndexByte(detail, '\n'); idx >= 0 {
		firstLine = detail[:idx]
	}
	firstLine = strings.TrimPrefix(firstLine, assertionHeaderPrefix)
	return []string{strings.TrimSpace(firstLine)}
}
