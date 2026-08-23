package wadjet

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The type-matrix gate for two process-killers.
//
// #392 — MIN_BY/MAX_BY declared a FLOAT64 output for every type outside a
// six-case switch, then handed the finalize write a value that vector cannot
// hold. The #361 guard fired on the parallel-emit goroutine, where no
// caller's recover can reach it, so `SELECT g, MIN_BY(bool_col, id) FROM t
// GROUP BY g` did not fail the query — it killed the process. PORT/PROTOCOL/
// DURATION were the quiet half: they box as int32/int64, which a FLOAT64
// vector accepts, so those answered as floats.
//
// #393 — reading a MAP column killed the process too, in the scan's row
// fallback: the parquet row reader produces a Go map and Vector.SetValue's
// MAP arm took only the storage shape, so the guard fired on scanWorker.
// `SELECT *` over a table that merely CONTAINS a MAP was enough.
//
// Both tests below die with the process on an unfixed build — which is the
// point: a gate that a process-killer can walk past is not a gate.

const mbRows = 5000

// mbSchema is one table carrying all 22 types. One table, not two, so the
// flat columns exercise the columnar decoder and the containers exercise the
// row fallback in the SAME fixture.
func mbSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "g", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c_bool", Type: parquet.TypeBool, Nullable: true},
		{Name: "c_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "c_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "c_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "c_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "c_str", Type: parquet.TypeString, Nullable: true},
		{Name: "c_bytes", Type: parquet.TypeBytes, Nullable: true},
		{Name: "c_ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "c_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "c_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "c_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "c_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "c_port", Type: parquet.TypePort, Nullable: true},
		{Name: "c_proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "c_dur", Type: parquet.TypeDuration, Nullable: true},
		{Name: "c_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "c_date", Type: parquet.TypeDate, Nullable: true},
		{Name: "c_dec", Type: parquet.TypeDecimal, Nullable: true, Precision: 18, Scale: 4},
		{Name: "c_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
		{Name: "c_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "a", Type: parquet.TypeString, Nullable: true},
			{Name: "b", Type: parquet.TypeInt64, Nullable: true},
		}},
		{Name: "c_map", Type: parquet.TypeMap, Nullable: true,
			ElementType: &parquet.Column{Name: "entry", Type: parquet.TypeRow, Fields: []parquet.Column{
				{Name: "key", Type: parquet.TypeString},
				{Name: "value", Type: parquet.TypeInt64, Nullable: true},
			}}},
		{Name: "c_vec", Type: parquet.TypeVector, Nullable: true, Dimension: 4},
	}}
}

// mbTypeCols is the type columns in schema order — everything but id and g.
func mbTypeCols() []parquet.Column { return mbSchema().Columns[2:] }

// mbData builds the fixture. Every type column nulls on its own stride so no
// two go NULL together, and the MAP column cycles through the four shapes
// whose def-level encoding differs: empty, one entry, two entries, and an
// entry whose VALUE is NULL.
func mbData(n int) []map[string]any {
	rows := make([]map[string]any, n)
	for i := range rows {
		r := map[string]any{"id": int64(i), "g": int32(i % 3)}
		put := func(name string, stride int, v any) {
			if i%stride == stride-1 {
				r[name] = nil
				return
			}
			r[name] = v
		}
		put("c_bool", 23, i%3 == 0)
		put("c_i32", 29, int32(i*3))
		put("c_i64", 31, int64(i)*1_000_003)
		put("c_f32", 37, float32(i)/7)
		put("c_f64", 41, float64(i)/3)
		put("c_str", 43, fmt.Sprintf("s-%06d", i))
		put("c_bytes", 47, []byte(fmt.Sprintf("bytes-%06d", i)))
		put("c_ts", 53, int64(1_700_000_000_000+int64(i)*61_000))
		put("c_ipv4", 59, fmt.Sprintf("10.0.%d.%d", (i/256)%256, i%256))
		put("c_ipv6", 61, fmt.Sprintf("2001:db8::%x", i))
		put("c_cidr", 67, fmt.Sprintf("192.168.%d.0/24", i%256))
		put("c_mac", 71, fmt.Sprintf("aa:bb:cc:00:%02x:%02x", (i/256)%256, i%256))
		put("c_port", 73, int32(1024+i%40000))
		put("c_proto", 79, int32(i%256))
		put("c_dur", 83, int64(i)*1_000_000)
		put("c_uuid", 89, fmt.Sprintf("00000000-0000-4000-8000-%012x", i))
		put("c_date", 97, fmt.Sprintf("20%02d-%02d-%02d", 10+i%15, 1+i%12, 1+i%28))
		put("c_dec", 101, float64(i)+0.0001*float64(i%9973))
		put("c_arr", 103, []any{fmt.Sprintf("a%05d", i)})
		put("c_row", 107, map[string]any{"a": fmt.Sprintf("r-%05d", i), "b": int64(i) * 11})
		put("c_map", 109, mbMapValue(i))
		put("c_vec", 113, []float32{float32(i), float32(i) + 0.5, -float32(i), 0.25})
		rows[i] = r
	}
	return rows
}

// mbMapValue cycles the four MAP shapes. Each writes a different definition
// level, and three of the four were broken: a nullable value column wrote
// every value one level too high (so every value read back NULL), an
// explicitly NULL value desynchronised the level and value streams, and an
// empty map was indistinguishable from a NULL one on read.
func mbMapValue(i int) map[string]any {
	switch i % 4 {
	case 0:
		return map[string]any{}
	case 1:
		return map[string]any{fmt.Sprintf("k%d", i%5): int64(i)}
	case 2:
		return map[string]any{"a": int64(i), "b": int64(i) * 2}
	default:
		return map[string]any{"nil": nil}
	}
}

func mbOpen(t *testing.T) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := mbSchema()
	if err := db.CreateTable(ctx, "mbtypes", schema, nil); err != nil {
		t.Fatal(err)
	}
	// Row group size deliberately not a multiple of the 2048-row batch, so
	// batch and row-group boundaries fall in different places.
	ing := db.NewIngester("mbtypes", schema, nil, ingest.Config{MaxBufferRows: mbRows + 1, RowGroupSize: 1100})
	if err := ing.Ingest(ctx, mbData(mbRows)); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db
}

// mbAssertEqual compares against the reference with reflect.DeepEqual, so a
// value that came back under the wrong TYPE (a PORT as a float64) fails on
// the value even where the two render alike.
func mbAssertEqual(t *testing.T, what string, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s = %#v, want %#v", what, got, want)
	}
}
