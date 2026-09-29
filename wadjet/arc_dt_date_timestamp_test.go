// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"testing"
)

// The embedded arm of #1378 (`DB.Query`, the raw positional
// `Result.RowValues` via QueryResult.Cells) over the stored-text suite's
// `st_pair` fixture (stEmbeddedDB), for the issue's own two statements. A
// DATE membership against a TIMESTAMP body compares as PostgreSQL's `date =
// timestamp`: the DATE promoted to its midnight. At v0.25.2 both counted 0
// and the NOT IN kept every row, where PostgreSQL 17.11 answers the rows
// below. The last cell is the control that the promotion is not a
// truncation: st_pair's TIMESTAMPs fall at 03:04:05, so no DATE equals one.
func TestArcDTEmbeddedDateTimestampMembership(t *testing.T) {
	db := stEmbeddedDB(t)
	ctx := context.Background()
	cases := []struct{ name, sql, want string }{
		{"1378/castBody", "SELECT count(*) AS n FROM st_pair a WHERE a.v_date IN (SELECT CAST(r.v_date AS TIMESTAMP) FROM st_pair r)", "[[3]]"},
		{"1378/fromless", "SELECT count(*) AS n FROM st_pair a WHERE a.v_date IN (SELECT TIMESTAMP '2024-01-02 00:00:00' UNION ALL SELECT TIMESTAMP '2024-01-03 00:00:00')", "[[1]]"},
		{"1378/castBodyRows", "SELECT a.id FROM st_pair a WHERE a.v_date IN (SELECT CAST(r.v_date AS TIMESTAMP) FROM st_pair r) ORDER BY a.id", "[[1] [2] [3]]"},
		{"1378/fromlessRows", "SELECT a.id FROM st_pair a WHERE a.v_date IN (SELECT TIMESTAMP '2024-01-02 00:00:00' UNION ALL SELECT TIMESTAMP '2024-01-03 00:00:00') ORDER BY a.id", "[[1]]"},
		{"1378/notIn", "SELECT a.id FROM st_pair a WHERE a.v_date NOT IN (SELECT CAST(r.v_date AS TIMESTAMP) FROM st_pair r WHERE r.id <= 3) ORDER BY a.id", "[]"},
		{"control/notTruncated", "SELECT a.id FROM st_pair a WHERE a.v_ts IN (SELECT CAST(r.v_ts AS DATE) FROM st_pair r) ORDER BY a.id", "[]"},
	}
	for _, c := range cases {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %s\n  refused: %v\n  want %s", c.name, c.sql, err, c.want)
			continue
		}
		if got := stCells(res); got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.name, c.sql, got, c.want)
		}
	}
}
