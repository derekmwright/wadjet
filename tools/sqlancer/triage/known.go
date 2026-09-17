// SPDX-License-Identifier: MIT

package triage

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strings"
)

// KnownDifference is one bold heading and its distinctive terms on the page.
type KnownDifference struct {
	Heading   string
	Line      int
	SQLStates []string
	Keywords  []string
}

// KnownDifferences preserves page order for deterministic first-match reasons.
type KnownDifferences []KnownDifference

var (
	differenceHeading = regexp.MustCompile(`^\*\*(.+)\*\*$`)
	stateToken        = regexp.MustCompile(`\b(?:[0-9]{2}[0-9A-Z]{3}|XX[0-9]{3})\b`)
	functionToken     = regexp.MustCompile("\\b([A-Za-z_][A-Za-z_0-9]*)\\s*\\(")
	referenceSuffix   = regexp.MustCompile(`\(ADR-[^\n]*`)
)

// LoadKnownDifferences reads bold one-line entries, stopping at section headings.
// Only recognized SQL vocabulary is extracted, never arbitrary prose words or
// identifiers from example queries. Common SQL building blocks (SELECT, NULL,
// CAST, COUNT, SUM, MIN/MAX, TEXT, INTEGER, etc.) are deliberately excluded:
// their presence says nothing distinctive about a documented difference.
func LoadKnownDifferences(rd io.Reader) (KnownDifferences, error) {
	var result KnownDifferences
	sc := bufio.NewScanner(rd)
	var body strings.Builder
	flush := func() {
		if len(result) == 0 {
			return
		}
		entry := &result[len(result)-1]
		prose := referenceSuffix.ReplaceAllString(body.String(), "")
		entry.SQLStates = stateToken.FindAllString(prose, -1)
		// Keywords come from the HEADING only. An entry's prose names the
		// types and functions of its examples ("returns varchar `-5`"), and
		// an engine message that echoes a refused expression ("join ON
		// (cast(t3.c2 as varchar)) between ...") meets those words by
		// accident: on the first MIT run (2026-09-17) prose keywords filed
		// 11 join refusals under "Unary minus accepts numeric text". The
		// heading is what the entry is about; SQLSTATEs still come from
		// the prose, where they are exact.
		text := strings.ToUpper(entry.Heading)
		terms := strings.Fields("BYTEA MONEY INET IPV4 IPV6 CIDR MACADDR PROTOCOL DURATION VECTOR DECIMAL VARCHAR SMALLINT INT2 TIMESTAMP TIMESTAMPTZ JSON XML QUALIFY LATERAL RETURNING CURRENT_TIMESTAMP CURRENT_DATE")
		terms = append(terms, "NATURAL JOIN", "JOIN USING", "PARTITION BY", "CREATE VIEW", "DROP VIEW", "ALTER TABLE")
		for _, keyword := range terms {
			if containsTerm(text, keyword) {
				entry.Keywords = append(entry.Keywords, keyword)
			}
		}
		for _, m := range functionToken.FindAllStringSubmatch(entry.Heading, -1) {
			fn := strings.ToUpper(m[1])
			switch fn {
			case "CAST", "COUNT", "SUM", "MIN", "MAX", "COALESCE", "SELECT", "IN", "OVER", "AS", "NUMERIC", "DECIMAL", "VARCHAR", "CHAR":
				continue
			}
			// Underscores distinguish extension functions; these standard functions
			// have explicit entries on the page as well.
			if strings.Contains(fn, "_") || containsTerm("AVG STDDEV VARIANCE CORR COVAR MEDIAN PERCENTILE NOW UPPER LOWER TRIM REVERSE STRPOS", fn) {
				entry.Keywords = append(entry.Keywords, fn)
			}
		}
		body.Reset()
	}
	active := false
	line := 0
	for sc.Scan() {
		line++
		s := sc.Text()
		if m := differenceHeading.FindStringSubmatch(s); m != nil {
			if active {
				flush()
			}
			result = append(result, KnownDifference{Heading: m[1], Line: line})
			active = true
		} else if strings.HasPrefix(s, "## ") {
			if active {
				flush()
				active = false
			}
		} else if active {
			body.WriteString(s)
			body.WriteByte('\n')
		}
	}
	if active {
		flush()
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("known-difference page has no bold entries")
	}
	return result, nil
}

func containsTerm(text, term string) bool {
	return regexp.MustCompile(`(?i)\b` + strings.ReplaceAll(regexp.QuoteMeta(term), " ", `\s+`) + `\b`).MatchString(text)
}

// Match uses an exact SQLSTATE in error text, or a whole distinctive function,
// type or clause token in SQL/error text. It never matches bare common words,
// substrings of identifiers, stack frames, or unrelated setup statements.
// SQLSTATEs in SQL literals/numbers do not count: inspect cause text separately.
// Shared SQLSTATEs choose the first page entry; this is a review classification,
// not proof that the documented behavior caused the disagreement.
func (known KnownDifferences) Match(sql, errors string) (KnownDifference, bool) {
	for _, entry := range known {
		for _, state := range entry.SQLStates {
			if containsTerm(errors, state) {
				return entry, true
			}
		}
	}
	for _, entry := range known {
		for _, keyword := range entry.Keywords {
			if containsTerm(sql+"\n"+errors, keyword) {
				return entry, true
			}
		}
	}
	return KnownDifference{}, false
}
