// SPDX-License-Identifier: MIT

package parquet

import (
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// TestTemporalBoxRangeIsPostgreSQLs holds CheckLeafBox — the writer's own
// box question, which the ingest boundary asks — to PostgreSQL's DATE and
// TIMESTAMP range for every typed box a temporal column accepts (arc VL
// round-4 review P2): one past either end is 22008, the end itself stores.
func TestTemporalBoxRangeIsPostgreSQLs(t *testing.T) {
	d := Column{Name: "d", Type: TypeDate}
	ts := Column{Name: "ts", Type: TypeTimestamp}
	for _, tc := range []struct {
		col  Column
		v    any
		want string
	}{
		{d, int32(MaxDateDay), ""}, {d, int64(MaxDateDay + 1), "22008"},
		{d, int64(MinDateDay), ""}, {d, int(MinDateDay - 1), "22008"},
		{d, int32(2147483647), "22008"}, {d, int32(-2147483648), "22008"},
		{d, time.Date(5874897, 12, 31, 0, 0, 0, 0, time.UTC), ""},
		{d, time.Date(5874898, 1, 1, 0, 0, 0, 0, time.UTC), "22008"},
		{ts, int64(EndTimestampMilli - 1), ""}, {ts, int64(EndTimestampMilli), "22008"},
		{ts, int64(MinTimestampMilli), ""}, {ts, int64(MinTimestampMilli - 1), "22008"},
		{ts, int64(9223372036854775807), "22008"},
		{ts, time.UnixMilli(EndTimestampMilli - 1).UTC(), ""},
		{ts, time.UnixMilli(EndTimestampMilli).UTC(), "22008"},
		{ts, time.Date(300000, 1, 1, 0, 0, 0, 0, time.UTC), "22008"},
		{ts, time.Date(2026, 3, 3, 10, 20, 30, 0, time.UTC), ""},
	} {
		if got := sqlerr.StateOf(CheckLeafBox(tc.col, tc.v)); got != tc.want {
			t.Errorf("CheckLeafBox(%s, %T %v) = %q, want %q", tc.col.Name, tc.v, tc.v, got, tc.want)
		}
	}
}
