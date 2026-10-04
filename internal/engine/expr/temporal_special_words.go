// SPDX-License-Identifier: MIT

package expr

import (
	"strings"
	"time"

	"github.com/derekmwright/wadjet/internal/engine/batch"
)

// SpecialTemporalWord is one of PostgreSQL's special date/time input words
// other than 'epoch' (which the one grammar reads as 1970-01-01 00:00:00):
// 'infinity' / '+infinity' / '-infinity', 'now', 'today', 'tomorrow',
// 'yesterday' — any case, surrounding whitespace ignored (measured on 17.11
// for both timestamp and date; 'allballs' is a time word and both types
// refuse it 22007).
type SpecialTemporalWord int

const (
	SpecialNone SpecialTemporalWord = iota
	SpecialInfinity
	SpecialNegInfinity
	SpecialNow
	SpecialToday
	SpecialTomorrow
	SpecialYesterday
)

// ReadSpecialTemporalWord classifies a quoted literal's text.
func ReadSpecialTemporalWord(text string) SpecialTemporalWord {
	switch strings.ToLower(strings.Trim(text, " \t\n\r\v\f")) {
	case "infinity", "+infinity":
		return SpecialInfinity
	case "-infinity":
		return SpecialNegInfinity
	case "now":
		return SpecialNow
	case "today":
		return SpecialToday
	case "tomorrow":
		return SpecialTomorrow
	case "yesterday":
		return SpecialYesterday
	}
	return SpecialNone
}

// StatementClock is the instant every clock function reads (CURRENT_TIMESTAMP,
// NOW(), CURRENT_DATE), in UTC — the value a 'now' / 'today' word is resolved
// to when the statement is planned, as PostgreSQL resolves it when it parses
// the constant.
func StatementClock() time.Time { return clockNow().UTC() }

// SpecialTemporalText renders a clock word as the literal text the type's
// input function reads: 'now' is the instant (a DATE takes its day), 'today'
// / 'tomorrow' / 'yesterday' midnight of that day. ok=false for the infinity
// words, which have no value in this engine's carriers.
func SpecialTemporalText(w SpecialTemporalWord, typ batch.TypeID, now time.Time) (string, bool) {
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	var t time.Time
	switch w {
	case SpecialNow:
		t = now
	case SpecialToday:
		t = day
	case SpecialTomorrow:
		t = day.AddDate(0, 0, 1)
	case SpecialYesterday:
		t = day.AddDate(0, 0, -1)
	default:
		return "", false
	}
	if typ == batch.TypeDate {
		return t.Format("2006-01-02"), true
	}
	return batch.FormatTimestamp(t.UnixMilli()), true
}
