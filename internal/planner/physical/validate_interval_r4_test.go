// SPDX-License-Identifier: MIT

package physical

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestIntervalR4EarliestTruthfulRule: the rulings on 1680c r3 (ADR-0052 §8,
// the earliest truthful rule). Where the binder spoke first with a 42883
// that is untrue of PostgreSQL (it has the operator or the function), the
// refusal is now this engine's, named; where PostgreSQL itself raises 42883
// the binder's own error stands, never pre-empted. PostgreSQL 17.11's
// answers (postgres:17-alpine, --locale=C, TimeZone UTC) are in 1680c_r4.md.
func TestIntervalR4EarliestTruthfulRule(t *testing.T) {
	cells := []struct {
		why, sql, code, reason string
	}{
		// PostgreSQL answers an interval (1 day 01:00:00, -1 days -01:00:00).
		{"timestamp - date", "SELECT TIMESTAMP '2024-01-02 01:00:00' - DATE '2024-01-01' AS a", "0A000", "timestamp-date-difference"},
		{"date - timestamp", "SELECT DATE '2024-01-01' - TIMESTAMP '2024-01-02 01:00:00' AS a", "0A000", "timestamp-date-difference"},
		{"timestamp - date under the display cast", "SELECT CAST(TIMESTAMP '2024-01-02 01:00:00' - DATE '2024-01-01' AS TEXT) AS a", "0A000", "timestamp-date-difference"},
		{"date - timestamp inside a shift", "SELECT TIMESTAMP '2024-03-09' + (DATE '2024-03-10' - TIMESTAMP '2024-03-09') AS a", "0A000", "timestamp-date-difference"},
		// PostgreSQL answers a timestamptz.
		{"timestamptz + a timestamptz difference", "SELECT TIMESTAMPTZ '2024-03-09 12:00:00-05' + (TIMESTAMPTZ '2024-03-10 12:00:00-04' - TIMESTAMPTZ '2024-03-09 12:00:00-05') AS a", "0A000", "timestamptz"},
		{"date_add with a zone", "SELECT date_add(TIMESTAMPTZ '2024-03-09 12:00:00-05', INTERVAL '1 day', 'UTC') AS a", "0A000", "timestamptz"},
		{"date_subtract over a timestamptz", "SELECT date_subtract(TIMESTAMPTZ '2024-03-09 12:00:00-05', INTERVAL '1 day') AS a", "0A000", "timestamptz"},
		// age (the architect's ruling on 1680c r4): every call is the named
		// capability refusal function-age, raised where "unknown function"
		// was (expr.checkKnown; the statement-level cells are
		// wadjet.TestIntervalRuleFourRefusals). age is NOT declared an interval
		// producer, so the binder does not refuse a bare age(...) as a result
		// column (the compiler's function-age does). Where the binder spoke
		// first with an untrue 42883 (an operator beside an interval, the
		// call untyped), the call's refusal comes first. A permanent rule
		// over its arguments (a sub-millisecond literal) is judged first, as
		// before.
		{"a bare age binds (the compiler refuses it)", "SELECT age(TIMESTAMP '2024-03-31', TIMESTAMP '2024-01-31') AS a", "", ""},
		{"age compared with an interval", "SELECT 1 AS a WHERE age(TIMESTAMP '2024-03-31', TIMESTAMP '2024-01-31') > INTERVAL '1 day'", "0A000", "function-age"},
		// interval - <untyped call> binds (shiftOperand admits an operand the
		// plan cannot type); the compiler's function-age refuses it.
		{"age subtracted from an interval binds", "SELECT CAST(INTERVAL '1 year' - age(TIMESTAMP '2024-03-31', TIMESTAMP '2024-01-31') AS TEXT) AS a", "", ""},
		{"a bare age before a sub-millisecond one", "SELECT age(TIMESTAMP '2024-03-31', TIMESTAMP '2024-01-31') AS a, age(TIMESTAMP '2024-01-01 00:00:00.000001', TIMESTAMP '2024-01-01') AS b", "0A000", "timestamp-millisecond-carrier"},
		{"a sub-millisecond age under the display cast", "SELECT CAST(age(TIMESTAMP '2024-01-01 00:00:00.000001', TIMESTAMP '2024-01-01') AS TEXT) AS a", "0A000", "timestamp-millisecond-carrier"},
		// Control: an unknown name beside an interval keeps the operator's
		// 42883 (no refusal names it).
		{"control: an unknown function beside an interval", "SELECT 1 AS a WHERE agez(TIMESTAMP '2024-03-31') > INTERVAL '1 day'", "42883", ""},
		// Controls: PostgreSQL's own 42883, which the binder keeps.
		{"control: date + timestamp is PostgreSQL's 42883", "SELECT DATE '2024-01-01' + TIMESTAMP '2024-01-02' AS a", "42883", ""},
		{"control: timestamptz + (date - date) is PostgreSQL's 42883", "SELECT TIMESTAMPTZ '2024-03-09 12:00:00-05' + (DATE '2024-03-10' - DATE '2024-03-09') AS a", "42883", ""},
		// Control: the comparison the refusal suggests binds.
		{"control: the comparison the refusal suggests", "SELECT TIMESTAMP '2024-01-02 01:00:00' > CAST(DATE '2024-01-01' AS TIMESTAMP) + INTERVAL '1 day' AS a", "", ""},
	}
	cat := brCatalog()
	for _, c := range cells {
		err := validateColumns(context.Background(), cat, mustExtract(t, c.sql))
		code, reason := sqlerr.StateOf(err), sqlerr.ReasonOf(err)
		switch {
		case c.code == "" && err != nil:
			t.Errorf("%s\n  %s\n  refused %v {%s}, want it to bind", c.why, c.sql, err, reason)
		case c.code != "" && (err == nil || code != c.code || reason != c.reason):
			t.Errorf("%s\n  %s\n  got %v (%s {%s}), want %s {%s}", c.why, c.sql, err, code, reason, c.code, c.reason)
		}
	}
}
