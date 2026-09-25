// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/sqlerr"
)

// THE SEED DECIDES A RECURSIVE CTE'S TYPES — every seed type against every term
// type (arc RC round 2).
//
// PostgreSQL resolves `seed UNION ALL term` with the seed first and raises
// 42804 when the resolution does not come back to the seed's type. Round 1
// checked the term's type against a two-entry allow list and refused shapes
// PostgreSQL and the base answer: an integer term under a numeric seed
// (`SELECT 1::numeric UNION ALL SELECT 2 …` → 1, 2, 2) and a bare NULL term
// (→ 1, NULL). This table is the measurement that replaced the list: 14 seed
// spellings × 16 term spellings, each cell's PostgreSQL 17.11 answer recorded
// beside it (`pg`). Where this engine answers otherwise the cell says why
// (`why`), and every such cell is a SUPERSET (an answer where PostgreSQL
// refuses, never a different value) or a refusal on both engines with a
// different class. 163 of the 224 cells fail at 16b924d1 and 37 at round 1's
// tip b9762e3e (rc_author/gate_rc_types_at_*_FAILS.log); none of the 37
// answered correctly at the base AND not at round 1 except the two B1 names —
// integer and NULL terms — which is what made round 1 a regression.
func TestArcRCRecursiveCTESeedTypeDecidesAgainstEveryTermType(t *testing.T) {
	db := rcFixtureDB(t, 0)
	cells := []struct{ seed, term, sql, pg, want, why string }{
		{"int4", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"int4", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "1.0;2.0", "superset: an integer term into an integer seed of the other width is range-checked into the seed (this engine declares n + 1 bigint)"},
		{"int4", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0", "1.0", ""},
		{"int4", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"int4", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int4", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;7.0", "1.0;7.0", ""},
		{"int4", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1::int, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"int8", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"int8", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"int8", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0", "1.0", ""},
		{"int8", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"int8", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"int8", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;7.0", "1.0;7.0", ""},
		{"int8", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1::bigint, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"numeric", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"numeric", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"numeric", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.5", "1.0;2.5", ""},
		{"numeric", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.25", "1.0;2.25", ""},
		{"numeric", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0", "1.0", ""},
		{"numeric", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"numeric", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.5", "1.0;2.5", ""},
		{"numeric", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;7.0", "1.0;7.0", ""},
		{"numeric", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1::numeric, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"numeric102", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.25", "1.5;2.25", ""},
		{"numeric102", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"numeric102", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1.50::numeric(10,2), 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float4", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"float4", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"float4", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"float4", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.25", "1.5;2.25", ""},
		{"float4", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"float4", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float4", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float4", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float4", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float4", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float4", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float4", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5", "1.5", ""},
		{"float4", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"float4", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"float4", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;7.0", "1.5;7.0", ""},
		{"float4", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::real, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float8", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"float8", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"float8", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"float8", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.25", "1.5;2.25", ""},
		{"float8", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"float8", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"float8", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float8", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float8", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float8", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float8", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"float8", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5", "1.5", ""},
		{"float8", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"float8", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"float8", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;7.0", "1.5;7.0", ""},
		{"float8", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1.5::float8, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"text", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "text", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a;b", "a;b", ""},
		{"text", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a;b", "a;b", ""},
		{"text", "date", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "null", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a", "a", ""},
		{"text", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"text", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a;7.0", "a;7.0", ""},
		{"text", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::text, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42883", "ERR 42804", "refusal on both: PostgreSQL finds no operator, this engine refuses the type"},
		{"varchar3", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "text", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "a;b", "superset: text and varchar(n) are one carrier here"},
		{"varchar3", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a;b", "a;b", ""},
		{"varchar3", "date", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "null", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "a", "superset: text and varchar(n) are one carrier here"},
		{"varchar3", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"varchar3", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "a;7.0", "superset: text and varchar(n) are one carrier here"},
		{"varchar3", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 'a'::varchar(3), 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42883", "ERR 42804", "refusal on both: PostgreSQL finds no operator, this engine refuses the type"},
		{"date", "int4", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "int8", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "float4", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "float8", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "text", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "date", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "2020-01-01;2021-01-01", "2020-01-01;2021-01-01", ""},
		{"date", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "bool", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "null", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "2020-01-01", "2020-01-01", ""},
		{"date", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"date", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22007", "ERR 22007", ""},
		{"date", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT DATE '2020-01-01', 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "2020-01-01;2020-01-02", "2020-01-01;2020-01-02", ""},
		{"timestamp", "int4", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "int8", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "float4", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "float8", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "text", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "date", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "2020-01-01 00:00:00;2021-01-01 00:00:00", "2020-01-01 00:00:00;2021-01-01 00:00:00", ""},
		{"timestamp", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "2020-01-01 00:00:00;2021-01-01 00:00:00", "2020-01-01 00:00:00;2021-01-01 00:00:00", ""},
		{"timestamp", "bool", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "null", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "2020-01-01 00:00:00", "2020-01-01 00:00:00", ""},
		{"timestamp", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"timestamp", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22007", "ERR 22007", ""},
		{"timestamp", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT TIMESTAMP '2020-01-01 00:00:00', 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42883", "ERR 42804", "refusal on both: PostgreSQL finds no operator, this engine refuses the type"},
		{"bool", "int4", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "int8", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "float4", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "float8", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "text", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "date", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "bool", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "true;false", "true;false", ""},
		{"bool", "null", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "true", "true", ""},
		{"bool", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"bool", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 22P02", ""},
		{"bool", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT true, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42883", "ERR 42804", "refusal on both: PostgreSQL finds no operator, this engine refuses the type"},
		{"lit_int", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"lit_int", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "1.0;2.0", "superset: an integer term into an integer seed of the other width is range-checked into the seed"},
		{"lit_int", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0", "1.0", ""},
		{"lit_int", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"lit_int", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_int", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;7.0", "1.0;7.0", ""},
		{"lit_int", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.0;2.0", "1.0;2.0", ""},
		{"lit_dec", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"lit_dec", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"lit_dec", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"lit_dec", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.25", "1.5;2.25", ""},
		{"lit_dec", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_dec", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_dec", "text", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_dec", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_dec", "date", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_dec", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_dec", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "ERR 42804", ""},
		{"lit_dec", "null", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5", "1.5", ""},
		{"lit_dec", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.0", "1.5;2.0", ""},
		{"lit_dec", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"lit_dec", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;7.0", "1.5;7.0", ""},
		{"lit_dec", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 1.5, 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "1.5;2.5", "1.5;2.5", ""},
		{"lit_str", "int4", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2::int, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "int8", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2::bigint, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "numeric", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2.5::numeric, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "numeric102", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2.25::numeric(10,2), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "float4", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2.5::real, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "float8", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2.5::float8, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "text", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 'b'::text, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a;b", "a;b", ""},
		{"lit_str", "varchar3", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 'b'::varchar(3), k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42804", "a;b", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "date", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT DATE '2021-01-01', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22007", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "timestamp", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT TIMESTAMP '2021-01-01 00:00:00', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22007", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "bool", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT false, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "null", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT NULL, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a", "a", ""},
		{"lit_str", "lit_int", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "lit_dec", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT 2.5, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 22P02", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
		{"lit_str", "lit_str", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT '7', k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "a;7.0", "a;7.0", ""},
		{"lit_str", "expr_nplus1", `WITH RECURSIVE r(n, k) AS (SELECT 'a', 1 UNION ALL SELECT n+1, k+1 FROM r WHERE k < 2) SELECT n::text AS n FROM r ORDER BY k`, "ERR 42883", "ERR 42804", "refusal on both: a quoted seed is text here and resolved from the term there"},
	}
	for _, c := range cells {
		t.Run(c.seed+"/"+c.term, func(t *testing.T) {
			if c.want != c.pg && c.why == "" {
				t.Fatalf("a divergence from PostgreSQL needs its reason")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			res, err := db.Query(ctx, c.sql)
			got := ""
			if err != nil {
				got = "ERR " + sqlerr.StateOf(err)
			} else {
				vals := make([]string, 0, len(res.Rows))
				for _, r := range res.Rows {
					if r["n"] == nil {
						vals = append(vals, "")
					} else {
						vals = append(vals, fmt.Sprint(r["n"]))
					}
				}
				got = strings.Join(vals, ";")
			}
			if rcNormalize(got) != rcNormalize(c.want) {
				t.Errorf("%s\n  got  %s\n  want %s (PostgreSQL 17.11: %s) %s", c.sql, got, c.want, c.pg, c.why)
			}
		})
	}
}

// rcNormalize compares numbers by value (PostgreSQL renders numeric 1 as "1"
// and this engine at the column's scale, "1.0") and drops a trailing NULL the
// way the recorded PostgreSQL answers were rendered.
func rcNormalize(s string) string {
	if strings.HasPrefix(s, "ERR ") {
		return s
	}
	parts := strings.Split(s, ";")
	for i, p := range parts {
		if f, err := strconv.ParseFloat(p, 64); err == nil {
			parts[i] = strconv.FormatFloat(f, 'g', -1, 64)
		}
	}
	return strings.TrimRight(strings.Join(parts, ";"), ";")
}
