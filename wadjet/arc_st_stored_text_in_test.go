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

// stEmbeddedType is one typed column of the `st_pair` fixture beside a TEXT
// column holding PostgreSQL 17.11's rendering of the value on rows 1 and 2,
// 'zz' on row 3 and NULL on row 4 — `internal/coordinator`'s five-arm
// fixture, reproduced here (rather than imported) because that one lives in
// an AGPL _test.go file and this package is MIT.
type stEmbeddedType struct {
	key, name   string
	typ         parquet.TypeID
	prec, scale int
	vals        [3]any
	texts       [2]string
}

func stEmbeddedTypes() []stEmbeddedType {
	return []stEmbeddedType{
		{"i64", "bigint", parquet.TypeInt64, 0, 0, [3]any{int64(12), int64(13), int64(14)}, [2]string{"12", "13"}},
		{"i32", "integer", parquet.TypeInt32, 0, 0, [3]any{int32(12), int32(13), int32(14)}, [2]string{"12", "13"}},
		{"dec", "numeric", parquet.TypeDecimal, 18, 4, [3]any{12.5, 13.25, 14.0}, [2]string{"12.5000", "13.2500"}},
		{"f64", "double precision", parquet.TypeFloat64, 0, 0, [3]any{12.5, 0.25, 3.0}, [2]string{"12.5", "0.25"}},
		{"port", "integer", parquet.TypePort, 0, 0, [3]any{int32(80), int32(443), int32(22)}, [2]string{"80", "443"}},
		{"proto", "integer", parquet.TypeProtocol, 0, 0, [3]any{int32(6), int32(17), int32(1)}, [2]string{"6", "17"}},
		{"uuid", "uuid", parquet.TypeUUID, 0, 0, [3]any{"00000000-0000-4000-8000-000000000001",
			"00000000-0000-4000-8000-000000000002", "00000000-0000-4000-8000-000000000003"},
			[2]string{"00000000-0000-4000-8000-000000000001", "00000000-0000-4000-8000-000000000002"}},
		{"ipv6", "inet", parquet.TypeIPv6, 0, 0, [3]any{"2001:db8::1", "2001:db8::2", "2001:db8::3"}, [2]string{"2001:db8::1", "2001:db8::2"}},
		{"cidr", "cidr", parquet.TypeCIDR, 0, 0, [3]any{"10.0.0.0/8", "192.168.0.0/16", "172.16.0.0/12"}, [2]string{"10.0.0.0/8", "192.168.0.0/16"}},
		{"dur", "bigint", parquet.TypeDuration, 0, 0, [3]any{int64(1_000_000), int64(2_000_000), int64(3_000_000)}, [2]string{"1000000", "2000000"}},
		{"date", "date", parquet.TypeDate, 0, 0, [3]any{"2024-01-02", "2024-03-04", "2024-05-06"}, [2]string{"2024-01-02", "2024-03-04"}},
		{"ts", "timestamp without time zone", parquet.TypeTimestamp, 0, 0,
			[3]any{int64(1_704_164_645_000), int64(1_709_521_445_000), int64(1_714_964_645_000)},
			[2]string{"2024-01-02 03:04:05", "2024-03-04 03:04:05"}},
		{"bool", "boolean", parquet.TypeBool, 0, 0, [3]any{true, false, true}, [2]string{"true", "false"}},
	}
}

func stEmbeddedDB(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{{Name: "id", Type: parquet.TypeInt64}}}
	rows := make([]map[string]any, 4)
	for i := range rows {
		rows[i] = map[string]any{"id": int64(i + 1)}
	}
	for _, ty := range stEmbeddedTypes() {
		schema.Columns = append(schema.Columns,
			parquet.Column{Name: "v_" + ty.key, Type: ty.typ, Precision: ty.prec, Scale: ty.scale, Nullable: true},
			parquet.Column{Name: "s_" + ty.key, Type: parquet.TypeString, Nullable: true})
		for i := 0; i < 3; i++ {
			rows[i]["v_"+ty.key] = ty.vals[i]
		}
		rows[0]["s_"+ty.key], rows[1]["s_"+ty.key], rows[2]["s_"+ty.key] = ty.texts[0], ty.texts[1], "zz"
		rows[3]["v_"+ty.key], rows[3]["s_"+ty.key] = nil, nil
	}
	if err := db.CreateTable(ctx, "st_pair", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("st_pair", schema, nil, ingest.Config{MaxBufferRows: 5, RowGroupSize: 2})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// stCells renders every row positionally (QueryResult.Cells: RowValues where
// the engine filled them, the column-ordered row otherwise).
func stCells(res *QueryResult) string {
	var rows [][]any
	for i := 0; ; i++ {
		c := res.Cells(i)
		if c == nil {
			break
		}
		rows = append(rows, c)
	}
	return fmt.Sprint(rows)
}

// The embedded arm of #1308 (`DB.Query`, the raw positional
// `Result.RowValues` via QueryResult.Cells): a typed IN / = ANY / NOT IN / <> ALL / EXISTS / NOT
// EXISTS whose body selects a stored, derived or CTE TEXT column — or a TEXT
// literal — is PostgreSQL 17.11's 42883, in the explicit JOIN's words; at
// v0.25.1 it answered 0 rows (NOT IN / NOT EXISTS every row) over matching
// values. The `CAST(x AS TEXT)` body of a kept pair keeps its conversion.
func TestArcSTEmbeddedStoredTextMembership(t *testing.T) {
	db := stEmbeddedDB(t)
	ctx := context.Background()
	type body struct{ kind, with, from, key string }
	shapes := []struct {
		name, want string
		pred       func(o, k, f string) string
	}{
		{"in", "3", func(o, k, f string) string { return o + " IN (SELECT " + k + " FROM " + f + " WHERE r.id <= 3)" }},
		{"eqAny", "3", func(o, k, f string) string { return o + " = ANY (SELECT " + k + " FROM " + f + " WHERE r.id <= 3)" }},
		{"notIn", "0", func(o, k, f string) string { return o + " NOT IN (SELECT " + k + " FROM " + f + " WHERE r.id <= 3)" }},
		{"neAll", "0", func(o, k, f string) string { return o + " <> ALL (SELECT " + k + " FROM " + f + " WHERE r.id <= 3)" }},
		{"exists", "3", func(o, k, f string) string {
			return "EXISTS (SELECT 1 FROM " + f + " WHERE r.id <= 3 AND " + o + " = " + k + ")"
		}},
		{"notExists", "1", func(o, k, f string) string {
			return "NOT EXISTS (SELECT 1 FROM " + f + " WHERE r.id <= 3 AND " + o + " = " + k + ")"
		}},
	}
	refused, answered := 0, 0
	for _, ty := range stEmbeddedTypes() {
		for _, mirror := range []bool{false, true} {
			v, s, outer := "v_"+ty.key, "s_"+ty.key, "a.v_"+ty.key
			msg := "operator does not exist: " + ty.name + " = text"
			if mirror {
				v, s, outer = s, v, "a.s_"+ty.key
				msg = "operator does not exist: text = " + ty.name
			}
			bodies := []body{
				{"stored", "", "st_pair r", "r." + s},
				{"derived", "", "(SELECT id, " + s + " AS k FROM st_pair) r", "r.k"},
				{"cte", "WITH c AS (SELECT id, " + s + " AS k FROM st_pair) ", "c r", "r.k"},
			}
			if !mirror {
				bodies = append(bodies, body{"cast", "", "st_pair r", "CAST(r." + v + " AS TEXT)"},
					body{"literal", "", "st_pair r", "'" + ty.texts[0] + "'"})
			}
			for _, b := range bodies {
				for _, sh := range shapes {
					exists := strings.Contains(sh.name, "xists")
					if b.kind == "literal" && exists {
						continue // an UNKNOWN literal compared directly: not a TEXT key
					}
					sql := b.with + "SELECT count(*) AS n FROM st_pair a WHERE " + sh.pred(outer, b.key, b.from)
					keep := b.kind == "cast" && (exists || (ty.key != "date" && ty.key != "ts" && ty.key != "bool"))
					res, err := db.Query(ctx, sql)
					if keep {
						answered++
						if err != nil {
							t.Errorf("%s\n  refused: %v\n  want %s (the kept conversion)", sql, err, sh.want)
							continue
						}
						if got := stCells(res); got != "[["+sh.want+"]]" {
							t.Errorf("%s\n  got  %s\n  want [[%s]]", sql, got, sh.want)
						}
						continue
					}
					refused++
					if err == nil {
						t.Errorf("%s\n  got  %s\n  want 42883 %q (PostgreSQL 17.11)", sql, stCells(res), msg)
						continue
					}
					if st := sqlerr.StateOf(err); st != "42883" || !strings.Contains(err.Error(), msg) {
						t.Errorf("%s\n  got  %s %v\n  want 42883 %q", sql, st, err, msg)
					}
				}
			}
		}
	}
	if refused < 300 || answered < 50 {
		t.Fatalf("%d refused / %d answered: the table must hold both", refused, answered)
	}
}
