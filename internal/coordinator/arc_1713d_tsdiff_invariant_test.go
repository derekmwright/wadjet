// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"
)

// ivTDUses reports a cell whose statement uses a timestamp difference: the
// td/* cells but the controls, and the tsub/* cells but date - date and the
// workarounds (which subtract no timestamps).
func ivTDUses(name string) bool {
	switch {
	case strings.HasPrefix(name, "td/"):
		return !strings.HasPrefix(name, "td/ctl")
	case strings.HasPrefix(name, "tsub/"):
		switch name {
		case "tsub/dateDate", "tsub/dateVsInterval", "tsub/workaround", "tsub/workaroundCols":
			return false
		}
		return true
	}
	return false
}

// TestArc1713dTimestampDifferenceIsNeverANumber is the round-6 invariant
// (#1713d, the maintainer's ruling on temporal r16): `timestamp - timestamp`
// is an interval to PostgreSQL and a double of milliseconds here, so NO
// statement, on any arm, may answer from that number. Every cell that uses a
// difference (published, keyed, ordered, aggregated, compared with a number
// or an interval, combined by arithmetic, cast, passed to a function, a
// window, a join key, a derived table / CTE / subquery / set-operation arm,
// an array) must be a refusal on every arm — whatever its SQLSTATE, which
// TestArc1713cIntervalOrderEveryArm pins — except IS [NOT] NULL
// (ivTDNumberFree), which must give PostgreSQL's answer.
func TestArc1713dTimestampDifferenceIsNeverANumber(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: six arms over the timestamp-difference cells")
	}
	answers := ivPGAnswers(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	writer, arms := ivArmsFixture(t, ctx)
	uses := 0
	for _, c := range ivCells() {
		if c.ddl {
			if got := writer(c.sql); got != "ok" && ivPin(c.name, "") == "" {
				t.Fatalf("%s: %s", c.name, got)
			}
			continue
		}
		if !ivTDUses(c.name) {
			continue
		}
		uses++
		for _, arm := range arms {
			got := arm.run(c.sql)
			if ivTDNumberFree[c.name] {
				if want := ivNormalize(answers[c.name]); ivNormalize(got) != want {
					t.Errorf("%s on %s\n  %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, arm.name, c.sql, got, want)
				}
				continue
			}
			if !strings.HasPrefix(got, "ERR ") {
				t.Errorf("%s on %s answered from the timestamp difference's millisecond number\n  %s\n  got %s\n  PostgreSQL 17.11 %s",
					c.name, arm.name, c.sql, got, answers[c.name])
			}
		}
	}
	if uses < 90 {
		t.Fatalf("only %d timestamp-difference cells: the invariant must cover the class", uses)
	}
}
