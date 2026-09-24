// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// Arc CW round 2, B3: the CAST table for a CONTAINER operand and a VECTOR
// destination, on every arm (single, spilled, dag, dagshuf).
//
// Round 1 made "a cast to a destination the engine does not convert to hands
// back the operand's text" true for containers — and VECTOR(n) is a
// destination the engine DOES convert to, so `CAST(ARRAY[…] AS VECTOR(n))`
// became the text `{…}`, every vector function over it answered NULL, and a
// nearest-neighbour ORDER BY returned the wrong row. The scalar destinations
// were older holes of the same seam: a container reached the integer arm as
// 0, the date arm as NULL. The table (cast_container.go) decides a container
// before any scalar arm reads it; the values are PostgreSQL 17.11's, and
// pgvector's for VECTOR (PostgreSQL has none):
//
//	CAST(ARRAY AS VECTOR(n))   a VECTOR ([]float32), dimension checked 22000,
//	                           NULL element 22004
//	CAST(ARRAY AS JSON)        its to_json text `[1,2]` (PostgreSQL: 42846)
//	CAST(ARRAY AS INT/DATE/…)  42846
//
// At main 6cbe2041 the top-level vector cell answered the Go text `[1 2]`
// under a text declaration and the scalar cells answered 0 / NULL; at the
// round-1 tip 8b50409c every vector-function cell answered NULL and the
// nearest neighbour was row 1.
func TestArcCW2ContainerCastTableOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	// c_vec is [i, i+0.5, -i, 0.25] (typematrix): the query vector IS row 2's,
	// so row 2 is the nearest neighbour by every measure and its distance 0.
	const q = `CAST(ARRAY[2.0, 2.5, -2.0, 0.25] AS VECTOR(4))`
	type cell struct {
		name, sql string
		want      string // fmt of the rows; "" when state is set
		state     string // the SQLSTATE the arm must raise
	}
	cells := []cell{
		{"nn-l2", `SELECT id FROM typemx_nested WHERE id BETWEEN 1 AND 3 ORDER BY l2_distance(c_vec, ` + q + `), id LIMIT 1`, "[[2]]", ""},
		{"nn-cosine", `SELECT id FROM typemx_nested WHERE id BETWEEN 1 AND 3 ORDER BY cosine_similarity(c_vec, ` + q + `) DESC, id LIMIT 1`, "[[2]]", ""},
		{"l2-values", `SELECT id, l2_distance(c_vec, ` + q + `) AS d FROM typemx_nested WHERE id = 2`, "[[2 0]]", ""},
		{"dot", `SELECT dot_product(CAST(ARRAY[1.0, 2.0] AS VECTOR(2)), CAST(ARRAY[3.0, 4.0] AS VECTOR(2))) AS d`, "[[11]]", ""},
		{"projected-vector", `SELECT CAST(ARRAY[1, 2] AS VECTOR(2)) AS v`, "[[[1 2]]]", ""},
		{"projected-unconstrained", `SELECT CAST(ARRAY[1.5, 2] AS VECTOR) AS v`, "[[[1.5 2]]]", ""},
		{"vector-text", `SELECT CAST(CAST(ARRAY[1.5, 2] AS VECTOR(2)) AS TEXT) AS t`, "[[[1.5,2]]]", ""},
		{"vector-from-text", `SELECT l2_distance(CAST('[3,4]' AS VECTOR(2)), CAST(ARRAY[0, 0] AS VECTOR(2))) AS d`, "[[5]]", ""},
		{"vector-dim-mismatch", `SELECT CAST(ARRAY[1, 2] AS VECTOR(3)) AS v`, "", "22000"},
		{"vector-null-element", `SELECT CAST(ARRAY[1, NULL] AS VECTOR(2)) AS v`, "", "22004"},
		{"json", `SELECT CAST(ARRAY[1, 2] AS JSON) AS j, json_array_length(CAST(ARRAY[1, 2] AS JSON)) AS n`, "[[[1,2] 2]]", ""},
		{"json-timestamp", `SELECT CAST(ARRAY[CAST('2024-01-01 01:00:00' AS TIMESTAMP)] AS JSON) AS j`, `[[["2024-01-01T01:00:00"]]]`, ""},
		{"text", `SELECT CAST(ARRAY[1, 2] AS TEXT) AS t`, "[[{1,2}]]", ""},
		{"widen-int8", `SELECT CAST(ARRAY[1, 2] AS BIGINT[]) AS a`, "[[[1 2]]]", ""},
		{"to-int", `SELECT CAST(ARRAY[1, 2] AS INT) AS v`, "", "42846"},
		{"to-double", `SELECT CAST(ARRAY[1, 2] AS DOUBLE) AS v`, "", "42846"},
		{"to-date", `SELECT CAST(ARRAY[1, 2] AS DATE) AS v`, "", "42846"},
		{"to-bytes", `SELECT CAST(ARRAY[1, 2] AS BYTES) AS v`, "", "42846"},
		{"to-interval", `SELECT CAST(ARRAY[1, 2] AS INTERVAL) AS v`, "", "42846"},
	}
	answered := 0
	for _, c := range cells {
		for _, arm := range arms {
			_, rows, err := arm.run(c.sql)
			if c.state != "" {
				if err == nil {
					t.Errorf("%s / %s: %s answered %v, want SQLSTATE %s", c.name, arm.name, c.sql, rows, c.state)
				} else if got := sqlerr.StateOf(err); got != c.state {
					t.Errorf("%s / %s: %s raised %s %v, want %s", c.name, arm.name, c.sql, got, err, c.state)
				} else {
					answered++
				}
				continue
			}
			if err != nil {
				t.Errorf("%s / %s: %s refused: %v", c.name, arm.name, c.sql, err)
				continue
			}
			answered++
			if got := fmt.Sprint(rows); got != c.want {
				t.Errorf("%s / %s: %s\n  got  %s\n  want %s", c.name, arm.name, c.sql, got, c.want)
			}
		}
	}
	if want := len(cells) * len(arms); answered != want {
		t.Errorf("%d of %d (cell, arm) pairs decided", answered, want)
	}
}
