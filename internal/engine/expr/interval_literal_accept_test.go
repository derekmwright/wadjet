// SPDX-License-Identifier: MIT

package expr

import (
	"os"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestIntervalLiteralAcceptedAgreesWithPostgres: the INTERVAL literal
// reader's refusal interval-input-form is named only for text PostgreSQL 17.11
// accepts (ADR-0052 §8, the architect's rules 3 and 4 on 1680c). Over the
// measured matrix (every literal the interval corpus's reader refused 22007,
// and the edge cases around PostgreSQL's field mask, its ranges and the
// accepted spellings), pgIntervalLiteralAccepted never accepts a text
// PostgreSQL refuses, and the literal reader keeps PostgreSQL's own code for
// every refused text it raises 22007 / 22015 for. It may decline a text
// PostgreSQL accepts (the reader then keeps its 22007, as before): every
// such text is listed by the test's log, and none is in the corpus.
func TestIntervalLiteralAcceptedAgreesWithPostgres(t *testing.T) {
	raw, err := os.ReadFile("testdata/interval_literal_pg17.tsv")
	if err != nil {
		t.Fatal(err)
	}
	corpus := map[string]bool{}
	for _, s := range intervalCorpusLiterals {
		corpus[s] = true
	}
	n, declined := 0, 0
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		text, pg, _ := strings.Cut(line, "\t")
		n++
		got := pgIntervalLiteralAccepted(text)
		switch {
		case got && pg != "ok":
			t.Errorf("%q: accepted, but PostgreSQL 17.11 raises %s", text, pg)
		case !got && pg == "ok":
			declined++
			if corpus[text] {
				t.Errorf("%q: a corpus literal PostgreSQL accepts, declined (its cell keeps an untruthful 22007)", text)
			} else {
				t.Logf("declined, PostgreSQL accepts (the reader keeps 22007): %q", text)
			}
		}
		// The reader itself: a text PostgreSQL refuses is never the named
		// refusal.
		if len(strings.Fields(text)) >= 2 {
			_, perr := plansql.Parse("SELECT INTERVAL '" + strings.ReplaceAll(text, "'", "''") + "' AS a")
			if pg != "ok" && sqlerr.ReasonOf(perr) == "interval-input-form" {
				t.Errorf("%q: the reader names interval-input-form, PostgreSQL raises %s", text, pg)
			}
			if got && sqlerr.ReasonOf(perr) != "interval-input-form" && perr != nil {
				if c := sqlerr.StateOf(perr); c == "22007" {
					t.Errorf("%q: accepted, but the reader still raises 22007: %v", text, perr)
				}
			}
		}
	}
	if n < 150 {
		t.Fatalf("only %d measured literals", n)
	}
	t.Logf("%d literals measured; %d PostgreSQL accepts are declined", n, declined)
}

// intervalCorpusLiterals are the texts the interval corpus's literal reader
// raised 22007 for at the 1680c r4 baseline that PostgreSQL accepts (the
// 146 cells rule 2 moves to interval-input-form), a sample sufficient to show
// none is declined.
var intervalCorpusLiterals = []string{
	"1 day 2 hours", "1 year 2 months 3 days 4 hours 5 minutes 6 seconds", "1 year 14 mons 40 days 26:07:08.5",
	"-1 day +02:00:00 ago", "1-2 3 4:05:06", "3 4:05:06 ago", "@ 3 days 4 hours 5 mins 6 secs", "@ 1 mon ago",
	"2147483647 months 2147483647 days 9223372036854775806 microseconds",
	"-2147483648 months -2147483648 days -9223372036854775807 microseconds",
	"178956970 years 7 mons 2147483647 days 2562047788:00:54.775807", "213503982 days 08:01:49.551616",
	"0.0000000125 minutes", "1.5 centuries", "0.0125 decades", "9223372036854775807 microseconds ago",
	"2147483647 days ago", "1 mon -25 hours", "1 day 1 mon 00:00:01", "1-2 ago",
}
