package pgwire

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A COMPUTED integer argument declares the same OID windowed as grouped, and
// it is PostgreSQL's (#987 review B1).
//
// PostgreSQL's SUM rule is by the ARGUMENT's width: `sum(int4)` is bigint —
// OID 20 — because there is a wider integer to grow into, and `sum(int8)` is
// numeric, OID 1700, because there is not. It reads the EXPRESSION, not a
// carrier: `sum(CASE WHEN … THEN 1 ELSE 0 END)` is bigint there because the
// CASE is int4, and TPC-H Q12 is exactly that shape.
//
// Every integer expression in this engine computes in int64 (ADR-0024's
// recorded widening), so the materialized argument column cannot tell the two
// apart — `w_i32 * 1` and `w_i64 * 1` both declare INT64. The GROUPED path
// therefore walks the argument's AST (aggComputedInputDecl over
// aggInputIsWideInteger) and answered bigint; the WINDOW path did not carry
// the AST at all, fell to the float8 fallback at plan time and was corrected
// to DECIMAL(38,0) by the operator, so the same six expressions went out under
// OID 1700 windowed and OID 20 grouped. One question, two spellings, two
// boxes — the class #813 was, with the spellings' roles swapped.
//
// The OID is asserted HERE rather than only in the census because a census
// reads the engine's own rendered type name: a value oracle cannot see a right
// value under a wrong OID, and OID 1700 is what a psql, a JDBC ResultSet or a
// Superset column type actually receives.
//
// The VALUE rides beside the OID on purpose. A "fix" that reached bigint by
// narrowing the accumulator would pass an OID-only assertion and lose digits
// on the int8 row; the last entry is the boundary that fails it — one int8 arm
// makes the CASE int8, so that one is numeric in both spellings.
func TestAComputedIntegerWindowArgumentDeclaresPostgresOID(t *testing.T) {
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "k2oid"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "k", Type: parquet.TypeInt64},
		{Name: "i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "i64", Type: parquet.TypeInt64, Nullable: true},
		// The two int4-domain NETWORK types, which declare integer (OID 23)
		// on the wire since #834 and take int4's SUM/AVG result types when
		// they are a BARE argument (#953). Under arithmetic they do not —
		// see the pinned entries below.
		{Name: "pt", Type: parquet.TypePort, Nullable: true},
		{Name: "pr", Type: parquet.TypeProtocol, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "k2oid", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("k2oid", schema, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 8})
	// -20 is here so a MOD and a unary minus have a negative to carry;
	// 2^53+1 is here so the int8 boundary entry loses a digit the moment
	// anything routes it through a float64.
	if err := ing.Ingest(ctx, []map[string]any{
		{"k": int64(1), "i32": int32(2), "i64": int64(2), "pt": int32(1024), "pr": int32(6)},
		{"k": int64(2), "i32": int32(3), "i64": int64(3), "pt": int32(80), "pr": int32(17)},
		{"k": int64(3), "i32": int32(-20), "i64": int64(-20), "pt": int32(443), "pr": int32(1)},
		{"k": int64(4), "i32": int32(16777217), "i64": int64(9007199254740993),
			"pt": int32(8080), "pr": int32(6)},
		{"k": int64(5), "i32": int32(0), "i64": int64(0), "pt": int32(0), "pr": int32(0)},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	srv := NewServer(db, Config{}, nil)
	if err := srv.Start("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Shutdown)
	conn := connectPgconn(t, srv.Addr())

	const (
		oidInt8    = 20
		oidNumeric = 1700
		oidFloat8  = 701
	)
	// Every `want` and every `oid` is PostgreSQL 17.11's, measured over rows
	// identical to the five above.
	for _, c := range []struct {
		name string
		arg  string
		oid  uint32
		want string
	}{
		{"case_of_ones", "CASE WHEN k > 3 THEN 1 ELSE 0 END", oidInt8, "2"},
		{"times_one", "i32 * 1", oidInt8, "16777202"},
		{"unary_minus", "-i32", oidInt8, "-16777202"},
		{"plus_literal", "i32 + 1", oidInt8, "16777207"},
		{"abs", "ABS(i32)", oidInt8, "16777242"},
		{"mod", "MOD(i32, 10)", oidInt8, "12"},
		// The BOUNDARY, and the reason the width is a WALK rather than
		// "a computed argument is int4": one int8 arm makes the whole CASE
		// int8, so this is numeric in both spellings. A narrowing that
		// ignored the walk would pass the six above and fail here.
		{"case_with_int8_arm", "CASE WHEN k > 3 THEN i64 ELSE 0 END",
			oidNumeric, "9007199254740993"},
		// The int8 controls: unchanged by B1 and asserted so a repair that
		// narrowed everything is caught.
		{"int8_times_one", "i64 * 1", oidNumeric, "9007199254740978"},
		{"int8_abs", "ABS(i64)", oidNumeric, "9007199254741018"},

		// A CAST is an operand whose width is its TARGET's, and the shared
		// width walk had no arm for one (#987 review round 3, B1) — so every
		// int8 operand written under a cast read as int4 and came out under
		// OID 20 in both spellings where PostgreSQL sends 1700. It is not
		// only a declaration: past int64 the bigint reading refuses 22003 a
		// query PostgreSQL answers, which the census asserts over 10^5 rows.
		{"cast_int8_to_bigint", "CAST(i64 AS BIGINT)", oidNumeric, "9007199254740978"},
		{"cast_int8_to_bigint_colon", "i64::BIGINT", oidNumeric, "9007199254740978"},
		// A cast WIDENS as well as keeps: the column is int4, the ARGUMENT
		// is int8, and PostgreSQL types the argument.
		{"cast_int4_to_bigint", "CAST(i32 AS BIGINT)", oidNumeric, "16777202"},
		// …and NARROWS. `sum(int8_col::int4)` is bigint there, so an arm
		// that answered "wide" for every cast would fail here. k, not i64:
		// 2^53+1 has no int4 and the cast itself refuses, which is
		// PostgreSQL's `integer out of range` and a different question.
		{"cast_int8_to_integer", "CAST(k AS INTEGER)", oidInt8, "15"},
		{"cast_int4_to_integer", "CAST(i32 AS INTEGER)", oidInt8, "16777202"},
		// A non-integer target leaves the integer table entirely, which is
		// the control that says the arm reads the TARGET and not "is there a
		// cast".
		{"cast_to_numeric", "CAST(i64 AS DECIMAL(20,0))", oidNumeric, "9007199254740978"},

		// PINNED, fail-on-agree (#987 review round 3, P1). A BARE PORT or
		// PROTOCOL takes int4's result types — the two cells first — and the
		// same column under ARITHMETIC does not, in either spelling, because
		// `pt * 1` is evaluated on the FLOAT path: `expr.operandIsInt` keeps
		// the network types there deliberately and `physical.intArithAllInt`
		// mirrors it, so a declaration cannot promise an integer the kernel
		// will not produce. Closing it means moving the KERNEL. PostgreSQL
		// has neither type, so the two spellings agreeing with each other is
		// the property at stake; ADR-0012's #953 entry carries the mechanism.
		{"port_bare", "pt", oidInt8, "9627"},
		{"protocol_bare", "pr", oidInt8, "30"},
		{"port_times_one_PINNED", "pt * 1", oidFloat8, "9627"},
		{"protocol_times_one_PINNED", "pr * 1", oidFloat8, "30"},
		{"protocol_abs_PINNED", "ABS(pr)", oidFloat8, "30"},
	} {
		t.Run(c.name, func(t *testing.T) {
			for _, sp := range []struct {
				spelling string
				sql      string
			}{
				{"grouped", "SELECT SUM(" + c.arg + ") AS v FROM k2oid"},
				{"windowed", "SELECT SUM(" + c.arg + ") OVER () AS v FROM k2oid " +
					"ORDER BY 1 LIMIT 1"},
			} {
				res := conn.ExecParams(ctx, sp.sql, nil, nil, nil, []int16{0}).Read()
				if res.Err != nil {
					t.Fatalf("%s: ExecParams: %v\n  SQL: %s", sp.spelling, res.Err, sp.sql)
				}
				if got := res.FieldDescriptions[0].DataTypeOID; got != c.oid {
					t.Errorf("%s declared OID %d, want %d — PostgreSQL declares "+
						"sum of an int4-domain expression bigint (20) and sum of an "+
						"int8-domain one numeric (1700), in BOTH spellings"+
						"\n  SQL: %s", sp.spelling, got, c.oid, sp.sql)
				}
				if len(res.Rows) != 1 || string(res.Rows[0][0]) != c.want {
					t.Errorf("%s rendered %v, want %q\n  SQL: %s",
						sp.spelling, res.Rows, c.want, sp.sql)
				}
			}
		})
	}
}
