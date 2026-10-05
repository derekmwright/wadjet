// SPDX-License-Identifier: AGPL-3.0-only
package server

import "testing"

// The same twenty statements run through all three coordinator doors.
// UNION still publishes v for the omitted outer projection (UN-F9).
// Stored-scale answers beside a constrained arm follow numeric-decimal r18.
func TestArcUNSetOperationTextEveryDoor(t *testing.T) {
	unibCheckDoors(t, []struct{ name, sql, want, why string }{
		{name: "d_UNIONALL_null", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION ALL SELECT NULL AS v FROM un_u) s", want: "rows=9 x=0.0000000001 | x=1 | x=1 | x=1.5 | x=7 | x=NULL | x=NULL | x=NULL | x=NULL"},
		{name: "d_UNIONALL_typednull", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION ALL SELECT CAST(NULL AS NUMERIC) AS v FROM un_u) s", want: "rows=9 x=0.0000000001 | x=1 | x=1 | x=1.5 | x=7 | x=NULL | x=NULL | x=NULL | x=NULL"},
		{name: "d_UNIONALL_fixed", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION ALL SELECT v FROM rv_n) s", want: "rows=12 x=0.0000000000 | x=0.0000000001 | x=1.0000000000 | x=1.0000000000 | x=1.0000000000 | x=1.5000000000 | x=1.5000000000 | x=2.5000000000 | x=7.0000000000 | x=7.0000000000 | x=NULL | x=NULL"},
		{name: "d_UNIONALL_expr", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION ALL SELECT v+0 AS v FROM un_x) s", want: "rows=12 x=0.0000000001 | x=0.0000000001 | x=1 | x=1 | x=1 | x=1 | x=1.5 | x=1.5 | x=7 | x=7 | x=NULL | x=NULL"},
		{name: "d_UNIONALL_marked", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION ALL SELECT v FROM un_u) s", want: "rows=9 x=0.0000000001 | x=0.1 | x=1 | x=1 | x=1.25 | x=1.5 | x=7 | x=7 | x=NULL"},
		{name: "d_UNION_null", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION SELECT NULL AS v FROM un_u) s", want: "rows=5 v=0.0000000001 | v=1 | v=1.5 | v=7 | v=NULL"},
		{name: "d_UNION_typednull", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION SELECT CAST(NULL AS NUMERIC) AS v FROM un_u) s", want: "rows=5 v=0.0000000001 | v=1 | v=1.5 | v=7 | v=NULL"},
		{name: "d_UNION_fixed", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION SELECT v FROM rv_n) s", want: "rows=7 v=0.0000000000 | v=0.0000000001 | v=1.0000000000 | v=1.5000000000 | v=2.5000000000 | v=7.0000000000 | v=NULL"},
		{name: "d_UNION_expr", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION SELECT v+0 AS v FROM un_x) s", want: "rows=5 v=0.0000000001 | v=1 | v=1.5 | v=7 | v=NULL"},
		{name: "d_UNION_marked", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x UNION SELECT v FROM un_u) s", want: "rows=7 v=0.0000000001 | v=0.1 | v=1 | v=1.25 | v=1.5 | v=7 | v=NULL"},
		{name: "d_INTERSECT_null", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x INTERSECT SELECT NULL AS v FROM un_u) s", want: "rows=1 x=NULL"},
		{name: "d_INTERSECT_typednull", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x INTERSECT SELECT CAST(NULL AS NUMERIC) AS v FROM un_u) s", want: "rows=1 x=NULL"},
		{name: "d_INTERSECT_fixed", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x INTERSECT SELECT v FROM rv_n) s", want: "rows=4 x=1.0000000000 | x=1.5000000000 | x=7.0000000000 | x=NULL"},
		{name: "d_INTERSECT_expr", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x INTERSECT SELECT v+0 AS v FROM un_x) s", want: "rows=5 x=0.0000000001 | x=1 | x=1.5 | x=7 | x=NULL"},
		{name: "d_INTERSECT_marked", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x INTERSECT SELECT v FROM un_u) s", want: "rows=1 x=7"},
		{name: "d_EXCEPT_null", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x EXCEPT SELECT NULL AS v FROM un_u) s", want: "rows=4 x=0.0000000001 | x=1 | x=1.5 | x=7"},
		{name: "d_EXCEPT_typednull", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x EXCEPT SELECT CAST(NULL AS NUMERIC) AS v FROM un_u) s", want: "rows=4 x=0.0000000001 | x=1 | x=1.5 | x=7"},
		{name: "d_EXCEPT_fixed", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x EXCEPT SELECT v FROM rv_n) s", want: "rows=1 x=0.0000000001"},
		{name: "d_EXCEPT_expr", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x EXCEPT SELECT v+0 AS v FROM un_x) s", want: "rows=0 "},
		{name: "d_EXCEPT_marked", sql: "SELECT CAST(v AS TEXT) AS x FROM (SELECT v FROM un_x EXCEPT SELECT v FROM un_u) s", want: "rows=4 x=0.0000000001 | x=1 | x=1.5 | x=NULL"},
	}, []string{"pg-coordinator", "http", "grpc-coordinator"})
}
