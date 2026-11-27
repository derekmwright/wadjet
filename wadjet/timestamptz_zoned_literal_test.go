// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// A QUOTED LITERAL WITH A ZONE OFFSET BESIDE A TIMESTAMP (#1680 line d,
// round 2; physical/validate_timestamptz_lineage.go).
//
// PostgreSQL types an untyped quoted literal from the operand beside it, and
// reads an offset in it two ways: beside a `timestamp` it is discarded,
// beside a `timestamptz` it is applied. This engine holds a timestamptz in
// its TIMESTAMP carrier, so the planner decides by the operand's declared
// lineage:
//
//   - beside a timestamptz by lineage — written directly, or carried by a
//     derived table, a CTE, a UNION ALL arm, a scalar subquery or a join —
//     the literal is read as a timestamptz. Round 1 made a timestamptz its
//     instant and left these reading the literal as a timestamp: 0 / false
//     where the base (which dropped both offsets) and PostgreSQL answer 1 /
//     true. The common-type consumers (GREATEST, LEAST, COALESCE, CASE,
//     NULLIF) answered the literal's wall clock at the base and round 1;
//   - beside a plain timestamp by lineage the offset is discarded, unchanged;
//   - beside a STORED column — SQL DDL, CREATE TABLE AS of a timestamptz, a
//     Parquet file with or without isAdjustedToUTC — whose declaration the
//     engine does not record, a literal with a nonzero offset is refused
//     (0A000 timestamp-zone-ambiguous) in a predicate and in an assignment;
//     PostgreSQL answers either reading depending on the declaration.
//
// Every value below is PostgreSQL 17.11's under TimeZone=UTC
// (/tmp scratch measurement recorded in the line's note; the carried cells
// are internal/coordinator/testdata/arc_1680d_timestamptz_pg17.tsv's lin/*).
func TestTimestampZonedLiteralFollowsTheDeclaredKind(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	pq, err := filepath.Abs("../internal/storage/parquet/testdata/timestamp_precision.parquet")
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{
		"CREATE TABLE t (id INTEGER, ts TIMESTAMP, s VARCHAR)",
		"INSERT INTO t VALUES (1, TIMESTAMP '2024-07-01 12:00:00', '2024-07-01 12:00:00+05:30')",
		"CREATE TABLE u AS SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS x",
		"CREATE TABLE w (id INTEGER, ts TIMESTAMP)",
	} {
		if _, err := db.Query(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	const (
		L         = "'2024-07-01 12:00:00+05:30'"
		D         = "(SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS x) s"
		ambiguous = "ERR 0A000 timestamp-zone-ambiguous"
	)
	for _, c := range []struct{ name, sql, want string }{
		// Rule 1: a timestamptz by lineage.
		{"derived", "SELECT x = " + L + " AS v FROM " + D, "true"},
		{"cte", "WITH c AS (SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS x) SELECT x = " + L + " AS v FROM c", "true"},
		{"union", "SELECT count(*) AS v FROM (SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS x UNION ALL SELECT TIMESTAMPTZ '2024-07-01 06:30:00+00') s WHERE x = " + L, "2"},
		{"scalar", "SELECT (SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30') = " + L + " AS v", "true"},
		{"join", "SELECT a.x = " + L + " AS v FROM (SELECT 1 AS k, TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS x) a JOIN (SELECT 1 AS k) b ON a.k = b.k", "true"},
		{"joinOutput", "SELECT j.x = " + L + " AS v FROM (SELECT a.x FROM (SELECT 1 AS k, TIMESTAMPTZ '2024-07-01 12:00:00+05:30' AS x) a JOIN (SELECT 1 AS k) b ON a.k = b.k) j", "true"},
		{"joinKey", "SELECT count(*) AS v FROM " + D + " JOIN (SELECT 1 AS k) b ON s.x = " + L, "1"},
		{"castOfText", "SELECT count(*) AS v FROM (SELECT CAST(s AS TIMESTAMPTZ) AS x FROM t WHERE id = 1) q WHERE x = " + L, "1"},
		{"between", "SELECT x BETWEEN " + L + " AND '2024-07-01 12:00:00+05:00' AS v FROM " + D, "true"},
		{"in", "SELECT x IN (" + L + ", '2024-07-01 00:00:00+00') AS v FROM " + D, "true"},
		{"simpleCase", "SELECT CASE x WHEN " + L + " THEN 1 ELSE 0 END AS v FROM " + D, "1"},
		{"isDistinct", "SELECT x IS DISTINCT FROM " + L + " AS v FROM " + D, "false"},
		{"noOffset", "SELECT x = '2024-07-01 06:30:00' AS v FROM " + D, "true"},
		{"direct", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' = '2024-07-01 17:30:00+05:30' AS v", "true"},
		{"greatest", "SELECT CAST(greatest(TIMESTAMPTZ '2024-07-01 12:00:00+00', '2024-07-01 15:00:00+05:30') AS TEXT) AS v", "2024-07-01 12:00:00"},
		{"coalesce", "SELECT CAST(coalesce(CAST(NULL AS TIMESTAMPTZ), '2024-07-01 15:00:00+05:30') AS TEXT) AS v", "2024-07-01 09:30:00"},
		{"case", "SELECT CAST(CASE WHEN false THEN TIMESTAMPTZ '2024-07-01 12:00:00+00' ELSE '2024-07-01 15:00:00+05:30' END AS TEXT) AS v", "2024-07-01 09:30:00"},
		{"leastCarried", "SELECT CAST(least(x, '2024-07-01 15:00:00+05:30') AS TEXT) AS v FROM (SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' AS x) s", "2024-07-01 09:30:00"},
		{"nullif", "SELECT nullif(x, '2024-07-01 17:30:00+05:30') IS NULL AS v FROM (SELECT TIMESTAMPTZ '2024-07-01 12:00:00+00' AS x) s", "true"},
		{"now", "SELECT now() > '2000-01-01 00:00:00+05:30' AS v", "true"},
		// Rule 2: a plain timestamp by lineage discards the offset.
		{"plainDerived", "SELECT x = " + L + " AS v FROM (SELECT TIMESTAMP '2024-07-01 12:00:00' AS x) s", "true"},
		{"plainDirect", "SELECT TIMESTAMP '2024-07-01 12:00:00' = " + L + " AS v", "true"},
		// Rule 3: a stored column of unknown declaration.
		{"storedDDL", "SELECT count(*) AS v FROM t WHERE ts = " + L, ambiguous},
		{"storedThroughDerived", "SELECT count(*) AS v FROM (SELECT ts FROM t) q WHERE ts >= " + L, ambiguous},
		{"storedMax", "SELECT max(ts) = " + L + " AS v FROM t", ambiguous},
		{"storedCoalesce", "SELECT coalesce(ts, " + L + ") AS v FROM t", ambiguous},
		{"ctas", "SELECT count(*) AS v FROM u WHERE x = " + L, ambiguous},
		{"ctasGe", "SELECT count(*) AS v FROM u WHERE x >= " + L, ambiguous},
		{"parquetNaive", "SELECT count(*) AS v FROM read_parquet('" + pq + "') WHERE ts_micros >= " + L, ambiguous},
		{"parquetAdjusted", "SELECT count(*) AS v FROM read_parquet('" + pq + "') WHERE ts_us_utc >= " + L, ambiguous},
		{"dmlUpdateWhere", "UPDATE w SET id = 2 WHERE ts = " + L, ambiguous},
		{"dmlDeleteWhere", "DELETE FROM w WHERE ts = " + L, ambiguous},
		{"dmlUpdateSet", "UPDATE w SET ts = " + L + " WHERE id = 7", ambiguous},
		{"dmlInsertValues", "INSERT INTO w VALUES (9, " + L + ")", ambiguous},
		{"dmlInsertSelect", "INSERT INTO w SELECT 10, " + L, ambiguous},
		// Unaffected beside a stored column: no offset, a zero one, a typed
		// literal.
		{"storedNoOffset", "SELECT count(*) AS v FROM t WHERE ts = '2024-07-01 12:00:00'", "1"},
		{"storedZ", "SELECT count(*) AS v FROM t WHERE ts = '2024-07-01 12:00:00Z'", "1"},
		{"storedTzLiteral", "SELECT count(*) AS v FROM t WHERE ts = TIMESTAMPTZ '2024-07-01 12:00:00+00'", "1"},
		{"ctasTzLiteral", "SELECT count(*) AS v FROM u WHERE x = TIMESTAMPTZ " + L, "1"},
		// PostgreSQL's own refusals (ruling 2).
		{"tstzEqText", "SELECT TIMESTAMPTZ '2024-07-01 12:00:00+05:30' = s AS v FROM t", "ERR 42883 "},
		{"datePartTzTs", "SELECT date_part('timezone', TIMESTAMP '2024-07-01') AS v", "ERR 0A000 "},
		{"extractTzHourDate", "SELECT extract(timezone_hour FROM DATE '2024-07-01') AS v", "ERR 0A000 "},
		{"extractTzHourTs", "SELECT extract(timezone_hour FROM TIMESTAMP '2024-07-01') AS v", "ERR 0A000 "},
		{"extractTzHourTstz", "SELECT extract(timezone_hour FROM TIMESTAMPTZ '2024-07-01 00:00:00+05:30') AS v", "0"},
	} {
		res, err := db.Query(ctx, c.sql)
		got := ""
		switch {
		case err != nil:
			got = "ERR " + sqlerr.StateOf(err) + " " + sqlerr.ReasonOf(err)
		case len(res.Rows) == 0:
			got = "no rows"
		default:
			got = fmt.Sprint(res.Rows[0]["v"])
		}
		if got != c.want {
			t.Errorf("%s: %s\n  got  %s\n  want %s", c.name, c.sql, got, c.want)
		}
	}
	// The refused writes wrote nothing.
	if res, err := db.Query(ctx, "SELECT count(*) AS v FROM w"); err != nil || fmt.Sprint(res.Rows[0]["v"]) != "0" {
		t.Errorf("w after the refused DML: %v %v, want empty", res, err)
	}
}
