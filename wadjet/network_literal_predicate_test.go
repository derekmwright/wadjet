// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// #1137: an unknown-typed literal beside a typed NETWORK column resolves as
// THAT COLUMN'S TYPE, at every predicate site.
//
// PostgreSQL's rule, measured on 17.11 over inet/macaddr/uuid columns: the
// literal is coerced to the column's type by the column's own input function,
// so `ip = '10.0.0.1'` matches, `ip = 'nonsense'` is `22P02 invalid input
// syntax for type inet`, and the same holds at `<>`, `IN`, `BETWEEN`, `CASE`,
// `COALESCE`, a join `ON` and `HAVING`.
//
// Five of the seven types already did that. PORT and PROTOCOL did not: the
// literal was converted against the type's DECLARED WIRE type — `integer`, OID
// 23 (#834) — at five sites that all read int4's text grammar, so
// `WHERE c_proto = 'udp'` was 22P02 while `CAST('udp' AS PROTOCOL)` answered
// 17. The sixth site this arc found is the other direction: `c_port = '0x1bb'`
// MATCHED port 443 through int4's radix prefix, which PORT's own grammar and
// the writer both refuse.
//
// PORT and PROTOCOL have no PostgreSQL equivalent; their expectations are the
// specification — arc NT's one-grammar rule, docs/data-types.md.
func TestAnUnknownLiteralBesideANetworkColumnResolvesAsTheColumnsType(t *testing.T) {
	ctx := context.Background()
	db := nlpOpen(t)

	// Every site the issue names, spelled so that a MATCH answers one row and
	// a miss answers none. `sel` builds the SQL from a column and a literal.
	sites := []struct {
		name string
		sel  func(col, lit string) string
		// rows is what a MATCHING literal answers; a non-matching one answers
		// the complement, which `neg` states.
		match, miss int
	}{
		{"eq", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp WHERE %s = %s`, c, l)
		}, 1, 0},
		{"ne", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp WHERE %s <> %s`, c, l)
		}, 0, 1},
		{"in", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp WHERE %s IN (%s)`, c, l)
		}, 1, 0},
		{"between", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp WHERE %s BETWEEN %s AND %s`, c, l, l)
		}, 1, 0},
		{"case", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp WHERE CASE WHEN %s = %s THEN true ELSE false END`, c, l)
		}, 1, 0},
		{"coalesce", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp WHERE COALESCE(%s, %s) = %s`, c, l, l)
		}, 1, 0},
		{"join-on", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp a JOIN nlp b ON a.%s = b.%s AND a.%s = %s`, c, c, c, l)
		}, 1, 0},
		{"having", func(c, l string) string {
			return fmt.Sprintf(`SELECT COUNT(*) AS n FROM nlp GROUP BY %s HAVING %s = %s`, c, c, l)
		}, 1, 0},
	}

	// One (type, matching literal, non-matching literal, malformed literal)
	// row per network type. The malformed one is the cell that says which
	// GRAMMAR read the text.
	for _, ty := range []struct {
		col, match, other, bad string
		// badState is the SQLSTATE the malformed literal raises, and pg is
		// PostgreSQL 17.11's verbatim answer where the type exists there.
		badState, pg string
	}{
		{col: "c_ip", match: `'10.0.0.1'`, other: `'10.0.0.2'`, bad: `'nonsense'`,
			badState: "22P02", pg: `22P02 invalid input syntax for type inet: "nonsense"`},
		{col: "c_v6", match: `'2001:db8::1'`, other: `'2001:db8::2'`, bad: `'nonsense'`,
			badState: "22P02", pg: `22P02 invalid input syntax for type inet: "nonsense"`},
		{col: "c_cidr", match: `'10.0.0.0/8'`, other: `'11.0.0.0/8'`, bad: `'nonsense'`,
			badState: "22P02", pg: `22P02 invalid input syntax for type inet: "nonsense"`},
		{col: "c_mac", match: `'08:00:2b:01:02:03'`, other: `'08:00:2b:01:02:04'`, bad: `'nonsense'`,
			badState: "22P02", pg: `22P02 invalid input syntax for type macaddr: "nonsense"`},
		{col: "c_uuid", match: `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'`,
			other: `'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12'`, bad: `'nonsense'`,
			badState: "22P02", pg: `22P02 invalid input syntax for type uuid: "nonsense"`},
		// PORT's own grammar: decimal digits, no radix prefix, no underscore.
		{col: "c_port", match: `'443'`, other: `'80'`, bad: `'0x1bb'`,
			badState: "22P02", pg: `wadjet's own type — PORT has no radix prefix, and the writer refuses this text`},
		// PROTOCOL's own grammar: the IANA NAME, or a decimal number in range.
		{col: "c_proto", match: `'udp'`, other: `'tcp'`, bad: `'nosuchproto'`,
			badState: "22P02", pg: `wadjet's own type — the IANA name is PROTOCOL's text form (#986)`},
	} {
		for _, site := range sites {
			t.Run(ty.col+"/"+site.name, func(t *testing.T) {
				// The MATCHING literal, read as the column's type.
				sql := site.sel(ty.col, ty.match)
				if got := nlpCount(t, ctx, db, sql); got != site.match {
					t.Errorf("%s answered %d rows, want %d — the literal must resolve as %s's own "+
						"type\n  PostgreSQL 17.11: %s\n  SQL: %s", site.name, got, site.match, ty.col, ty.pg, sql)
				}
				// A WELL-FORMED literal of the same type that names another
				// value: the complement, never a refusal.
				sql = site.sel(ty.col, ty.other)
				if got := nlpCount(t, ctx, db, sql); got != site.miss {
					t.Errorf("%s answered %d rows for a non-matching literal, want %d\n  SQL: %s",
						site.name, got, site.miss, sql)
				}
				// The MALFORMED one: refused, with the type's own SQLSTATE,
				// and refused at PLAN time — the predicate is over a table
				// that holds one row, so an answer here is the grammar
				// disagreeing with the writer's.
				sql = site.sel(ty.col, ty.bad)
				_, err := db.Query(ctx, sql)
				if err == nil {
					t.Errorf("%s ANSWERED a literal that names no value of %s's type\n"+
						"  PostgreSQL 17.11: %s\n  SQL: %s", site.name, ty.col, ty.pg, sql)
					return
				}
				if got := sqlerr.StateOf(err); got != ty.badState {
					t.Errorf("%s: SQLSTATE %q, want %q\n  err: %v\n  PostgreSQL 17.11: %s\n  SQL: %s",
						site.name, got, ty.badState, err, ty.pg, sql)
				}
			})
		}
	}

	// IN with TWO members, which is the site's own shape: a disjunction of
	// equalities cannot mean something `=` does not.
	t.Run("c_proto/in_two_names", func(t *testing.T) {
		sql := `SELECT COUNT(*) AS n FROM nlp WHERE c_proto IN ('udp','tcp')`
		if got := nlpCount(t, ctx, db, sql); got != 1 {
			t.Errorf("= %d rows, want 1 — one member reads the IANA name and so must the set\n  SQL: %s", got, sql)
		}
		sql = `SELECT COUNT(*) AS n FROM nlp WHERE c_proto IN ('tcp','icmp')`
		if got := nlpCount(t, ctx, db, sql); got != 0 {
			t.Errorf("= %d rows, want 0\n  SQL: %s", got, sql)
		}
		if _, err := db.Query(ctx, `SELECT COUNT(*) AS n FROM nlp WHERE c_proto IN ('udp','0x6')`); err == nil {
			t.Error("an IN list holding a radix-prefixed member ANSWERED; PROTOCOL's grammar has no `0x6`")
		}
	})

	// The PROJECTED value, not only the predicate: a CASE and a COALESCE that
	// CHOOSE the literal must publish the column's type, not the literal's
	// text (the arm the boxed path takes).
	for _, c := range []struct {
		name, sql string
		want      any
	}{
		{"case_else_literal", `SELECT CASE WHEN false THEN c_proto ELSE 'udp' END AS v FROM nlp`, int32(17)},
		{"coalesce_literal", `SELECT COALESCE(c_proto, 'tcp') AS v FROM nlp`, int32(17)},
		{"case_else_port", `SELECT CASE WHEN false THEN c_port ELSE '80' END AS v FROM nlp`, int32(80)},
	} {
		t.Run("projected/"+c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("%v\n  SQL: %s", err, c.sql)
			}
			if len(res.Rows) != 1 || res.Rows[0]["v"] != c.want {
				t.Errorf("= %#v, want %#v\n  SQL: %s", res.Rows, c.want, c.sql)
			}
		})
	}
}

// TestOneBadNetworkLiteralIsReportedOneWayAtEveryDoor is the round-1 review's
// N1. The arc unified the READ side on `integer` — the name a client can look
// up in pg_type for a column that declares OID 23 — and left the writer's door
// and the vector store naming the type itself, so ONE bad literal was reported
// two ways depending on which evaluator saw it first.
//
// The SQLSTATE was already one; this is the sentence.
func TestOneBadNetworkLiteralIsReportedOneWayAtEveryDoor(t *testing.T) {
	ctx := context.Background()
	db := nlpOpen(t)
	if _, err := db.Query(ctx, `CREATE TABLE nlpsink (k BIGINT, c_port PORT)`); err != nil {
		t.Fatalf("create sink: %v", err)
	}

	const want = `invalid input syntax for type integer: "zzz"`
	for _, c := range []struct{ door, sql string }{
		{"filter", `SELECT COUNT(*) AS n FROM nlp WHERE c_port = 'zzz'`},
		{"in-list", `SELECT COUNT(*) AS n FROM nlp WHERE c_port IN ('zzz')`},
		{"between", `SELECT COUNT(*) AS n FROM nlp WHERE c_port BETWEEN 'zzz' AND 'zzz'`},
		{"cast", `SELECT CAST('zzz' AS PORT) AS v FROM nlp`},
		{"case-arm", `SELECT CASE WHEN k = 2 THEN c_port ELSE 'zzz' END AS v FROM nlp`},
		{"coalesce-arm", `SELECT COALESCE(c_port, 'zzz') AS v FROM nlp`},
		{"having", `SELECT c_port AS v FROM nlp GROUP BY c_port HAVING c_port = 'zzz'`},
		{"join-on", `SELECT COUNT(*) AS n FROM nlp a JOIN nlp b ON a.c_port = b.c_port AND a.c_port = 'zzz'`},
		{"set-operation", `SELECT c_port AS v FROM nlp UNION ALL SELECT 'zzz' FROM nlp`},
		{"insert-select", `INSERT INTO nlpsink (k, c_port) SELECT k, 'zzz' FROM nlp`},
		{"insert-values", `INSERT INTO nlpsink (k, c_port) VALUES (9, 'zzz')`},
		{"ctas", `CREATE TABLE nlpctas AS SELECT CAST('zzz' AS PORT) AS v FROM nlp`},
	} {
		t.Run(c.door, func(t *testing.T) {
			var err error
			if strings.HasPrefix(c.sql, "INSERT") {
				_, err = db.Execute(ctx, c.sql)
			} else {
				_, err = db.Query(ctx, c.sql)
			}
			if err == nil {
				t.Fatalf("%s door ANSWERED a literal that names no PORT\n  SQL: %s", c.door, c.sql)
			}
			if got := sqlerr.StateOf(err); got != "22P02" {
				t.Errorf("%s door: SQLSTATE %q, want 22P02\n  err: %v\n  SQL: %s", c.door, got, err, c.sql)
			}
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s door says %q; every other door says %q. ONE refusal has ONE "+
					"sentence, and `port` is a name no client can resolve in pg_type for a "+
					"column that declares OID 23\n  SQL: %s", c.door, err.Error(), want, c.sql)
			}
		})
	}
}

// TestARefusalNeverNamesAnInternalTypeIdentifier is the round-2 review's P3.
//
// N1's repair asks the COLUMN TYPE's own text reader for the sentence when
// both readings of a quoted literal have failed, and
// parquet.NetworkTextTypeName falls back to `typ.String()` for every type it
// does not name — so an INT64 column's INSERT began reporting
// `invalid input syntax for type INT64`, the engine's own identifier, which no
// client can look up in pg_type and which this engine's eleven other doors
// spell `bigint` for the same column. The repair takes the new refusal only
// for a type that HAS such a reader; everything else keeps the message it had.
func TestARefusalNeverNamesAnInternalTypeIdentifier(t *testing.T) {
	ctx := context.Background()
	db := nlpOpen(t)
	mustDDL(t, db, `CREATE TABLE nlpint (c_i64 INT64, c_i32 INT32, c_port PORT)`)

	// No refusal at any door may name an internal identifier. The list is the
	// TypeID spellings a fallback would produce for the types this table has.
	internal := []string{"INT64", "INT32", "PORT", "PROTOCOL", "FLOAT64", "DECIMAL"}
	for _, c := range []struct{ door, sql string }{
		{"insert-values-bigint", `INSERT INTO nlpint (c_i64) VALUES ('zzz')`},
		{"insert-values-bigint-fraction", `INSERT INTO nlpint (c_i64) VALUES ('2.5')`},
		{"insert-values-integer", `INSERT INTO nlpint (c_i32) VALUES ('zzz')`},
		{"insert-values-port", `INSERT INTO nlpint (c_port) VALUES ('zzz')`},
		{"update-port", `UPDATE nlpint SET c_port = 'zzz'`},
		// The two SELECT doors read `nlp`, which HOLDS a row: a projection
		// over an empty relation evaluates nothing, so the cast door would
		// answer zero rows rather than refuse and this cell would measure
		// nothing.
		{"filter-bigint", `SELECT COUNT(*) AS n FROM nlp WHERE k = CAST('zzz' AS BIGINT)`},
		{"cast-bigint", `SELECT CAST('zzz' AS BIGINT) AS v FROM nlp`},
	} {
		t.Run(c.door, func(t *testing.T) {
			var err error
			if strings.HasPrefix(c.sql, "SELECT") {
				_, err = db.Query(ctx, c.sql)
			} else {
				_, err = db.Execute(ctx, c.sql)
			}
			if err == nil {
				t.Fatalf("%s door ANSWERED a literal that names no value of the column's type"+
					"\n  SQL: %s", c.door, c.sql)
			}
			for _, id := range internal {
				if strings.Contains(err.Error(), id) {
					t.Errorf("%s door names the INTERNAL identifier %q: %v. A refusal names a "+
						"type a client can resolve in pg_type — `bigint`, `integer` — or the "+
						"message the door already gave\n  SQL: %s", c.door, id, err, c.sql)
				}
			}
		})
	}

	// And the gain N1 made is kept: the two types that DO have their own text
	// reader still say `integer` rather than falling back to the second
	// reading's `numeric` with the quotes in the literal.
	for _, c := range []struct{ door, sql string }{
		{"insert-values-port", `INSERT INTO nlpint (c_port) VALUES ('zzz')`},
		{"update-port", `UPDATE nlpint SET c_port = 'zzz'`},
	} {
		t.Run("keeps/"+c.door, func(t *testing.T) {
			_, err := db.Execute(ctx, c.sql)
			if err == nil {
				t.Fatalf("answered\n  SQL: %s", c.sql)
			}
			if !strings.Contains(err.Error(), `invalid input syntax for type integer: "zzz"`) {
				t.Errorf("%s door says %q; the type's own reader classified this literal and its "+
					"sentence is the one the other ten doors give\n  SQL: %s",
					c.door, err.Error(), c.sql)
			}
		})
	}

	// A NUMBER is not a quoted literal and keeps the assignment cast, which is
	// what makes the repair a message change and not a behaviour one.
	if _, err := db.Execute(ctx, `INSERT INTO nlpint (c_i64) VALUES (2.5)`); err != nil {
		t.Errorf("an unquoted 2.5 into an INT64 column was refused (%v); PostgreSQL rounds it "+
			"and so does this engine (#699)", err)
	}
}

func nlpCount(t *testing.T, ctx context.Context, db *DB, sql string) int {
	t.Helper()
	res, err := db.Query(ctx, sql)
	if err != nil {
		t.Fatalf("%v\n  SQL: %s", err, sql)
	}
	if len(res.Rows) == 0 {
		return 0
	}
	// A HAVING that filters the only group away answers no rows at all.
	if strings.Contains(sql, "GROUP BY") {
		return len(res.Rows)
	}
	switch n := res.Rows[0]["n"].(type) {
	case int64:
		return int(n)
	case int32:
		return int(n)
	}
	t.Fatalf("COUNT(*) is %#v\n  SQL: %s", res.Rows[0]["n"], sql)
	return 0
}

func nlpOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "c_ip", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_v6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "c_port", Type: parquet.TypePort, Nullable: true},
		{Name: "c_proto", Type: parquet.TypeProtocol, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "nlp", sc, nil); err != nil {
		t.Fatalf("create: %v", err)
	}
	ing := db.NewIngester("nlp", sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{{
		"k": int64(1), "c_ip": "10.0.0.1", "c_v6": "2001:db8::1", "c_cidr": "10.0.0.0/8",
		"c_mac": "08:00:2b:01:02:03", "c_uuid": "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11",
		"c_port": int32(443), "c_proto": int32(17),
	}}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	return db
}
