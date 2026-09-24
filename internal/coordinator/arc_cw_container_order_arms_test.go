// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Arc CW on every arm (single, spilled, dag, dagshuf): a CONSTRUCTED array is
// an ARRAY — typed, subscriptable, and ordered ELEMENT-WISE — at the fixture's
// full 5000 rows, where the DAG arms merge sorted runs from three workers and
// the spilled arm merges runs off disk (#1021, #1250, #1303).
//
// The oracle is PostgreSQL's array ordering rule stated as an identity:
// arrays compare element by element under the element type's own order, and
// a NULL element sorts after every non-NULL one — so `ORDER BY ARRAY[x], id`
// IS `ORDER BY x, id` (ASC puts NULLs last in both; DESC puts them first in
// both), and MIN/MAX of the array holds the first/last value of that order
// (MAX is ARRAY[NULL] wherever a NULL element exists, as `ORDER BY x DESC`
// puts the NULL first). The right-hand side uses no container at all,
// so the two can only agree when the array comparator is element-wise.
//
// At base 83cd4a93 the constructor declared TEXT and every numeric, date and
// timestamp cell ordered by the Go rendering (`[10 3]` before `[9 27]`).
func TestArcCWConstructedArraysOrderElementWiseOnEveryArm(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Minute)
	t.Cleanup(cancel)
	arms := e3Arms(t, ctx)

	render := func(rows [][]any) string {
		var out []string
		for _, r := range rows {
			var cells []string
			for _, v := range r {
				cells = append(cells, fmt.Sprint(v))
			}
			out = append(out, strings.Join(cells, ","))
		}
		return strings.Join(out, " | ")
	}
	cols := []string{"c_i32", "c_i64", "c_f64", "c_str", "c_ts", "c_date", "c_bool", "c_uuid", "c_ipv4", "g"}
	type pair struct{ name, got, want string }
	var pairs []pair
	for _, c := range cols {
		pairs = append(pairs,
			pair{"order-asc/" + c,
				fmt.Sprintf("SELECT id FROM typemx ORDER BY ARRAY[%s], id LIMIT 40", c),
				fmt.Sprintf("SELECT id FROM typemx ORDER BY %s, id LIMIT 40", c)},
			pair{"order-desc/" + c,
				fmt.Sprintf("SELECT id FROM typemx ORDER BY ARRAY[%s] DESC, id DESC LIMIT 40", c),
				fmt.Sprintf("SELECT id FROM typemx ORDER BY %s DESC, id DESC LIMIT 40", c)},
			pair{"min-max/" + c,
				fmt.Sprintf("SELECT (MIN(ARRAY[%s]))[1] AS lo, (MAX(ARRAY[%s]))[1] AS hi FROM typemx", c, c),
				fmt.Sprintf("SELECT (SELECT %s FROM typemx ORDER BY %s LIMIT 1) AS lo, "+
					"(SELECT %s FROM typemx ORDER BY %s DESC LIMIT 1) AS hi", c, c, c, c)},
			pair{"derived-subscript/" + c,
				fmt.Sprintf("SELECT a[1] AS x FROM (SELECT ARRAY[%s] AS a, id FROM typemx) s ORDER BY id LIMIT 40", c),
				fmt.Sprintf("SELECT %s AS x FROM typemx ORDER BY id LIMIT 40", c)},
			pair{"distinct/" + c,
				fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT DISTINCT ARRAY[%s] AS a FROM typemx) s", c),
				fmt.Sprintf("SELECT COUNT(*) AS n FROM (SELECT DISTINCT %s FROM typemx) s", c)},
		)
	}
	pairs = append(pairs,
		pair{"union-then-order",
			"SELECT a FROM (SELECT ARRAY[c_i32] AS a FROM typemx WHERE id < 30 UNION ALL " +
				"SELECT ARRAY[c_i32 + 1] FROM typemx WHERE id < 30) u ORDER BY a LIMIT 50",
			"SELECT ARRAY[x] AS a FROM (SELECT c_i32 AS x FROM typemx WHERE id < 30 UNION ALL " +
				"SELECT c_i32 + 1 FROM typemx WHERE id < 30) u ORDER BY x LIMIT 50"},
		pair{"values-any",
			"SELECT COUNT(*) AS n FROM typemx, (VALUES (ARRAY[3, 6, 9])) v(a) WHERE c_i32 = ANY(a)",
			"SELECT COUNT(*) AS n FROM typemx WHERE c_i32 IN (3, 6, 9)"},
	)

	cells := 0
	for _, p := range pairs {
		for _, arm := range arms {
			_, gotRows, err := arm.run(p.got)
			if err != nil {
				t.Errorf("%s / %s: %s\n  refused: %v", p.name, arm.name, p.got, err)
				continue
			}
			_, wantRows, err := arm.run(p.want)
			if err != nil {
				t.Errorf("%s / %s: the oracle %s refused: %v", p.name, arm.name, p.want, err)
				continue
			}
			cells++
			if g, w := render(gotRows), render(wantRows); g != w {
				t.Errorf("%s / %s:\n  %s\n    = %s\n  %s\n    = %s", p.name, arm.name, p.got, g, p.want, w)
			}
		}
	}
	if want := len(pairs) * len(arms); cells != want {
		t.Errorf("%d of %d (cell, arm) pairs answered", cells, want)
	}

	// The value itself is an ARRAY on every arm — a slice, never the string a
	// TEXT declaration boxes it as.
	for _, arm := range arms {
		_, rows, err := arm.run("SELECT ARRAY[c_ts, c_ts] AS a, ARRAY[c_str] AS s FROM typemx WHERE id = 1")
		if err != nil {
			t.Errorf("%s: %v", arm.name, err)
			continue
		}
		if len(rows) != 1 || len(rows[0]) != 2 {
			t.Errorf("%s: %v", arm.name, rows)
			continue
		}
		for i, v := range rows[0] {
			if reflect.ValueOf(v).Kind() != reflect.Slice {
				t.Errorf("%s: column %d is %T %v, not an array", arm.name, i, v, v)
			}
		}
	}
}
