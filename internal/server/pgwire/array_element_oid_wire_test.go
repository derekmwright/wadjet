package pgwire

// AN ARRAY DECLARES THE ARRAY OF ITS ELEMENT — #992.
//
// PostgreSQL has no generic `array` type. Every array is the array OF
// something and carries that type's own OID: int4[] 1007, int8[] 1016,
// text[] 1009, float8[] 1022, numeric[] 1231, timestamp[] 1115, date[] 1182,
// uuid[] 2951, bool[] 1000, bytea[] 1001 — read off a live 17.11 catalog's
// pg_type.typarray. This engine declared OID 25 (text) for every ARRAY, so
// the CHARACTERS a client read were right and the TYPE it was told was not:
// JDBC's getArray, pgx's array scanning and DataGrip's column typing all key
// on the OID, and an array column arrived as a string.
//
// The gate asserts three things a value oracle cannot see, per element type:
// the OID in RowDescription, the same OID from a DESCRIBE of a prepared
// statement (the path pgJDBC takes and the only one that asks the PLAN rather
// than an executed batch), and — because declaring a real array OID is a
// PROMISE about the bytes — that the BINARY form under that OID decodes.
//
// The element kinds that keep text are asserted too, and they are a fact
// about PostgreSQL rather than a gap here: a NESTED array (PostgreSQL's
// `int4[][]` is rectangular and this engine's nested arrays are ragged) and a
// ROW or MAP element (no registered composite OID; no MAP at all). ADR-0012
// records both.

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

func arrOidSchema() parquet.Schema {
	el := func(t parquet.TypeID) *parquet.Column {
		return &parquet.Column{Name: "element", Type: t, Nullable: true}
	}
	col := func(name string, e *parquet.Column) parquet.Column {
		return parquet.Column{Name: name, Type: parquet.TypeArray, Nullable: true, ElementType: e}
	}
	dec := &parquet.Column{Name: "element", Type: parquet.TypeDecimal,
		Precision: 9, Scale: 2, Nullable: true}
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		col("a_i32", el(parquet.TypeInt32)),
		col("a_i64", el(parquet.TypeInt64)),
		col("a_f32", el(parquet.TypeFloat32)),
		col("a_f64", el(parquet.TypeFloat64)),
		col("a_str", el(parquet.TypeString)),
		col("a_bool", el(parquet.TypeBool)),
		col("a_bytes", el(parquet.TypeBytes)),
		col("a_ts", el(parquet.TypeTimestamp)),
		col("a_date", el(parquet.TypeDate)),
		col("a_dec", dec),
		col("a_uuid", el(parquet.TypeUUID)),
		col("a_ipv4", el(parquet.TypeIPv4)),
		col("a_cidr", el(parquet.TypeCIDR)),
		col("a_mac", el(parquet.TypeMAC)),
		col("a_port", el(parquet.TypePort)),
		col("a_proto", el(parquet.TypeProtocol)),
		col("a_dur", el(parquet.TypeDuration)),
		// The two that keep text, for the reasons in this file's header.
		col("a_nest", &parquet.Column{Name: "element", Type: parquet.TypeArray,
			Nullable: true, ElementType: el(parquet.TypeInt32)}),
		col("a_row", &parquet.Column{Name: "element", Type: parquet.TypeRow, Nullable: true,
			Fields: []parquet.Column{{Name: "x", Type: parquet.TypeInt64, Nullable: true}}}),
	}}
}

func setupArrOidDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, srv := setupRealDB(t)
	schema := arrOidSchema()
	if err := db.CreateTable(ctx, "arroid", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{{
		"id":      int32(1),
		"a_i32":   []any{int32(1), int32(2)},
		"a_i64":   []any{int64(3), int64(4)},
		"a_f32":   []any{float32(1.5), float32(2.5)},
		"a_f64":   []any{1.25, 2.25},
		"a_str":   []any{"a", "b"},
		"a_bool":  []any{true, false},
		"a_bytes": []any{[]byte{0x01, 0x02}},
		"a_ts":    []any{int64(1262304000000)},
		"a_date":  []any{"2010-01-01"},
		"a_dec":   []any{"12.75", "0.25"},
		"a_uuid":  []any{"00000000-0000-4000-8000-000000000001"},
		"a_ipv4":  []any{"10.0.0.1"},
		"a_cidr":  []any{"192.168.0.0/24"},
		"a_mac":   []any{"aa:bb:cc:00:00:01"},
		"a_port":  []any{int32(443)},
		"a_proto": []any{int32(6)},
		"a_dur":   []any{int64(1000)},
		"a_nest":  []any{[]any{int32(1), int32(2)}, []any{int32(3)}},
		"a_row":   []any{map[string]any{"x": int64(7)}},
	}}
	ing := db.NewIngester("arroid", schema, nil, ingest.Config{MaxBufferRows: 4, RowGroupSize: 4})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

// arrOidCells is the table: the column, PostgreSQL 17.11's OID for an array
// of that element, and the text-format bytes both engines send.
func arrOidCells() []struct {
	col  string
	oid  uint32
	text string
} {
	return []struct {
		col  string
		oid  uint32
		text string
	}{
		{"a_i32", 1007, "{1,2}"},
		{"a_i64", 1016, "{3,4}"},
		{"a_f32", 1021, "{1.5,2.5}"},
		{"a_f64", 1022, "{1.25,2.25}"},
		{"a_str", 1009, "{a,b}"},
		{"a_bool", 1000, "{t,f}"},
		{"a_bytes", 1001, `{"\\x0102"}`},
		{"a_ts", 1115, `{"2010-01-01 00:00:00"}`},
		{"a_date", 1182, "{2010-01-01}"},
		{"a_dec", 1231, "{12.75,0.25}"},
		{"a_uuid", 2951, "{00000000-0000-4000-8000-000000000001}"},
		// PORT and PROTOCOL are int4 on the wire (#834), so their arrays are
		// int4[]; DURATION is int8 nanoseconds, so int8[].
		{"a_port", 1007, "{443}"},
		{"a_proto", 1007, "{6}"},
		{"a_dur", 1016, "{1000}"},
		// The engine renders these as text and so declares text[], which is
		// exactly what their SCALAR columns declare.
		{"a_ipv4", 1009, "{10.0.0.1}"},
		{"a_cidr", 1009, "{192.168.0.0/24}"},
		{"a_mac", 1009, "{aa:bb:cc:00:00:01}"},
		// The two that keep OID 25.
		// PostgreSQL cannot hold this value at all: its nested arrays are
		// RECTANGULAR, and `{{1,2},{3}}` is a syntax error there. This engine
		// renders each inner array as a quoted element, which is why the
		// column keeps OID 25 — see the file header.
		{"a_nest", 25, `{"{1,2}","{3}"}`},
		{"a_row", 25, "{(7)}"},
	}
}

// TestAnArrayColumnDeclaresItsElementsArrayOID is the RowDescription half, in
// TEXT format — the bytes psql reads.
func TestAnArrayColumnDeclaresItsElementsArrayOID(t *testing.T) {
	srv := setupArrOidDB(t)
	for _, c := range arrOidCells() {
		t.Run(c.col, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			sql := "SELECT " + c.col + " AS v FROM arroid WHERE id = 1"
			res := conn.ExecParams(context.Background(), sql, nil, nil, nil, []int16{0}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, sql)
			}
			if got := res.FieldDescriptions[0].DataTypeOID; got != c.oid {
				t.Errorf("%s\n  OID %d, want %d (PostgreSQL 17.11 pg_type.typarray)",
					sql, got, c.oid)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%s: %d rows, want 1", sql, len(res.Rows))
			}
			if got := string(res.Rows[0][0]); got != c.text {
				t.Errorf("%s\n  wire sent %q, want %q", sql, got, c.text)
			}
		})
	}
}

// TestAnArrayColumnDeclaresTheSameOIDUnderDescribe is the path pgJDBC takes:
// a prepared statement described BEFORE it has executed, so the answer comes
// from the plan and not from a batch.
func TestAnArrayColumnDeclaresTheSameOIDUnderDescribe(t *testing.T) {
	srv := setupArrOidDB(t)
	for _, c := range arrOidCells() {
		t.Run(c.col, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			sql := "SELECT " + c.col + " AS v FROM arroid WHERE id = 1"
			sd, err := conn.Prepare(context.Background(), "s_"+c.col, sql, nil)
			if err != nil {
				t.Fatalf("prepare: %v\n  SQL: %s", err, sql)
			}
			if len(sd.Fields) != 1 {
				t.Fatalf("Describe returned %d fields, want 1", len(sd.Fields))
			}
			if got := sd.Fields[0].DataTypeOID; got != c.oid {
				t.Errorf("%s\n  Describe OID %d, want %d", sql, got, c.oid)
			}
		})
	}
}

// TestAnArrayColumnsBinaryFormDecodes is the promise the OID makes. Declaring
// 1007 tells a client the bytes are PostgreSQL's array binary form, and until
// #992 this path wrote Go's `%v` of a []any — `[1 2]` — under whatever OID the
// column had. pgconn asks for binary here and the decode is asserted by
// reading the header back.
func TestAnArrayColumnsBinaryFormDecodes(t *testing.T) {
	srv := setupArrOidDB(t)
	for _, c := range arrOidCells() {
		if c.oid == 25 {
			continue // text[] of a nested/composite element keeps text bytes
		}
		t.Run(c.col, func(t *testing.T) {
			conn := connectPgconn(t, srv.Addr())
			sql := "SELECT " + c.col + " AS v FROM arroid WHERE id = 1"
			res := conn.ExecParams(context.Background(), sql, nil, nil, nil, []int16{1}).Read()
			if res.Err != nil {
				t.Fatalf("%v\n  SQL: %s", res.Err, sql)
			}
			raw := res.Rows[0][0]
			if len(raw) < 20 {
				t.Fatalf("%s: binary array is %d bytes, too short for a header", sql, len(raw))
			}
			ndim := int32(raw[0])<<24 | int32(raw[1])<<16 | int32(raw[2])<<8 | int32(raw[3])
			elemOID := uint32(raw[8])<<24 | uint32(raw[9])<<16 | uint32(raw[10])<<8 | uint32(raw[11])
			if ndim != 1 {
				t.Errorf("%s: ndim %d, want 1", sql, ndim)
			}
			if got := pgArrayOID(int(elemOID)); uint32(got) != c.oid {
				t.Errorf("%s: element OID %d is not the element of the declared %d",
					sql, elemOID, c.oid)
			}
		})
	}
}

// TestAZeroRowArrayResultKeepsItsDeclaration is the shape a value check
// cannot reach: with no rows there is no vector to re-type from, so the
// declaration is the plan's alone.
func TestAZeroRowArrayResultKeepsItsDeclaration(t *testing.T) {
	srv := setupArrOidDB(t)
	conn := connectPgconn(t, srv.Addr())
	// Off the READER, not off Read()'s Result: pgconn fills that struct's
	// FieldDescriptions from the rows it accumulated, so a zero-row result
	// reports none there even when the server sent a full RowDescription
	// (arc N1's own note).
	rr := conn.ExecParams(context.Background(),
		`SELECT a_i32 AS v FROM arroid WHERE id = 99`, nil, nil, nil, []int16{1})
	for rr.NextRow() {
	}
	fds := rr.FieldDescriptions()
	if len(fds) != 1 {
		t.Fatalf("RowDescription carries %d fields, want 1", len(fds))
	}
	if _, err := rr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// PINNED at 25, and PostgreSQL 17.11 declares 1007 here.
	//
	// A zero-row result has no vector, so its declaration is the PLAN's
	// alone — and the planner's declaration layer carries no element type for
	// an ARRAY at all: colDecls holds a bare TypeID plus a ROW's Fields and a
	// DECIMAL's (p,s), so colRefDeclaredType DECLINES every ARRAY column and
	// the schema falls to text. That is the pre-existing gap its own comment
	// names, not something #992 introduced, and closing it means giving the
	// scan annotation and colDecls an element map the way #568 gave them a
	// field map.
	//
	// The pin FAILS when it starts agreeing, which is the proof the day that
	// lands.
	if got := fds[0].DataTypeOID; got != 25 {
		t.Errorf("a zero-row ARRAY result now declares OID %d rather than the pinned 25. "+
			"If that is 1007, the planner learned an ARRAY's element type: delete this pin "+
			"and assert PostgreSQL's OID.", got)
	}
}
