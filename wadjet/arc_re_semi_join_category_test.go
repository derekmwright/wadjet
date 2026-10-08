// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A SEMI OR ANTI JOIN PUBLISHES ITS PROBE'S CATEGORIES (#381).
//
// IN, EXISTS and NOT IN become a semi or anti join, which publishes the probe
// side alone. The category walk merged the build side in as for any join, so
// a probe column `b` — `sqrt(6.25 + id * 0)`, numeric to PostgreSQL — met the
// subquery table's double precision `b`, the two disagreed, and the name was
// dropped: round(b) read the carrier and answered 2 where PostgreSQL 17.11
// (and 89cea148) answer 3. The build column is not in the output; a probe
// float8 b stays float8. Every want is PostgreSQL 17.11's.
func TestArcRESemiJoinKeepsItsProbeCategory(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, q := range []string{
		"CREATE TABLE sj_a (id BIGINT, f DOUBLE PRECISION)",
		"INSERT INTO sj_a VALUES (1, 0.5), (3, 2.5), (5, -0.5)",
		"CREATE TABLE sj_w (id BIGINT, b DOUBLE PRECISION)",
		"INSERT INTO sj_w VALUES (1, 0.5), (3, 2.5)",
	} {
		if _, err := db.Query(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	for _, c := range []struct{ q, w string }{
		{"SELECT id, round(b) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM sj_a) s WHERE id IN (SELECT id FROM sj_w) ORDER BY 1", "1,3; 3,3"},
		{"SELECT id, round(b) FROM (SELECT id, 5 / 2.0 + id * 0 AS b FROM sj_a) s WHERE EXISTS (SELECT 1 FROM sj_w WHERE sj_w.id = s.id) ORDER BY 1", "1,3; 3,3"},
		{"SELECT id, CAST(b AS INTEGER) FROM (SELECT id, sqrt(6.25 + id * 0) AS b FROM sj_a) s WHERE id NOT IN (SELECT id FROM sj_w) ORDER BY 1", "5,3"},
		{"SELECT id, round(b) FROM (SELECT id, f AS b FROM sj_a) s WHERE id IN (SELECT id FROM sj_w) ORDER BY 1", "1,0; 3,2"},
	} {
		res, err := db.Query(ctx, c.q)
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		var rows []string
		for i := range res.Rows {
			var f []string
			for _, v := range res.Cells(i) {
				f = append(f, reText(v))
			}
			rows = append(rows, strings.Join(f, ","))
		}
		if got := strings.Join(rows, "; "); got != c.w {
			t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11)", c.q, got, c.w)
		}
	}
}
