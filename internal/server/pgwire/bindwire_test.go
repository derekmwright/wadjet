package pgwire

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// boundParam is one Bind parameter: the OID the client declared for it at
// Parse, its wire bytes, and whether those bytes are in binary format.
type boundParam struct {
	oid    uint32
	value  []byte // nil = SQL NULL
	binary bool
}

func textParam(oid uint32, s string) boundParam {
	return boundParam{oid: oid, value: []byte(s)}
}

func binaryParam(oid uint32, b []byte) boundParam {
	return boundParam{oid: oid, value: b, binary: true}
}

// paramQuery runs Parse (declaring the parameter OIDs) / Describe(statement) /
// Bind (with the parameter values) / Describe(portal) / Execute / Sync — the
// round pgJDBC takes for a parameterized query — and returns the
// ParameterDescription's OIDs alongside the result.
func (c *pgClient) paramQuery(sqlText string, params []boundParam) (paramOIDs []uint32, names []string, rows [][]string, tag string) {
	c.t.Helper()

	var parseBuf []byte
	parseBuf = append(parseBuf, 0) // unnamed statement
	parseBuf = append(parseBuf, sqlText...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, uint16(len(params)))
	for _, p := range params {
		parseBuf = binary.BigEndian.AppendUint32(parseBuf, p.oid)
	}
	c.writeMsg('P', parseBuf)

	c.writeMsg('D', []byte{'S', 0}) // Describe statement, before Bind

	var bindBuf []byte
	bindBuf = append(bindBuf, 0) // unnamed portal
	bindBuf = append(bindBuf, 0) // unnamed statement
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, uint16(len(params)))
	for _, p := range params {
		code := uint16(0)
		if p.binary {
			code = 1
		}
		bindBuf = binary.BigEndian.AppendUint16(bindBuf, code)
	}
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, uint16(len(params)))
	for _, p := range params {
		if p.value == nil {
			bindBuf = binary.BigEndian.AppendUint32(bindBuf, math.MaxUint32) // -1 = NULL
			continue
		}
		bindBuf = binary.BigEndian.AppendUint32(bindBuf, uint32(len(p.value)))
		bindBuf = append(bindBuf, p.value...)
	}
	bindBuf = binary.BigEndian.AppendUint16(bindBuf, 0) // 0 result format codes
	c.writeMsg('B', bindBuf)

	c.writeMsg('D', []byte{'P', 0}) // Describe portal

	var execBuf []byte
	execBuf = append(execBuf, 0)
	execBuf = binary.BigEndian.AppendUint32(execBuf, 0)
	c.writeMsg('E', execBuf)

	c.writeMsg('S', nil)

	for {
		typ, data, err := c.readMsg()
		if err != nil {
			c.t.Fatalf("reading response for %q: %v", sqlText, err)
		}
		switch typ {
		case 't': // ParameterDescription
			paramOIDs = parseParamDesc(data)
		case 'T':
			names, _ = c.parseRowDescTyped(data)
		case 'D':
			rows = append(rows, c.parseDataRow(data, len(names)))
		case 'C':
			tag = readCString(data)
		case 'E':
			tag = "ERROR: " + c.parseError(data)
		case 'Z':
			return
		}
	}
}

// parseParamDesc reads a ParameterDescription: int16 count + int32 OIDs.
func parseParamDesc(data []byte) []uint32 {
	if len(data) < 2 {
		return nil
	}
	n := int(binary.BigEndian.Uint16(data[:2]))
	data = data[2:]
	oids := make([]uint32, 0, n)
	for i := 0; i < n && len(data) >= 4; i++ {
		oids = append(oids, binary.BigEndian.Uint32(data[:4]))
		data = data[4:]
	}
	return oids
}

func be32(v int32) []byte  { return binary.BigEndian.AppendUint32(nil, uint32(v)) }
func be64b(v int64) []byte { return binary.BigEndian.AppendUint64(nil, uint64(v)) }

// TestBindTypedParamsSelectCorrectRows is the regression test for issue #305
// item 3. Bind used to write every parameter as a quoted string, so an
// integer parameter became `id = '2'` — which this engine compares as a
// string against an integer column and matches nothing. The query succeeded
// with zero rows: a wrong answer with no error attached.
//
// Every case below returns zero rows on the pre-fix tree except the text one.
func TestBindTypedParamsSelectCorrectRows(t *testing.T) {
	_, srv := setupRealDB(t)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	tests := []struct {
		name     string
		sql      string
		params   []boundParam
		wantRows [][]string
	}{
		{
			name:     "int4 equality",
			sql:      "SELECT id, name FROM users WHERE id = $1 ORDER BY id",
			params:   []boundParam{textParam(oidInt4, "2")},
			wantRows: [][]string{{"2", "bob"}},
		},
		{
			name:     "int4 binary equality",
			sql:      "SELECT id, name FROM users WHERE id = $1 ORDER BY id",
			params:   []boundParam{binaryParam(oidInt4, be32(2))},
			wantRows: [][]string{{"2", "bob"}},
		},
		{
			name:     "int8 equality",
			sql:      "SELECT id, name FROM users WHERE visits = $1 ORDER BY id",
			params:   []boundParam{textParam(oidInt8, "42")},
			wantRows: [][]string{{"2", "bob"}},
		},
		{
			name:     "int8 binary equality",
			sql:      "SELECT id, name FROM users WHERE visits = $1 ORDER BY id",
			params:   []boundParam{binaryParam(oidInt8, be64b(42))},
			wantRows: [][]string{{"2", "bob"}},
		},
		{
			name:     "float8 range",
			sql:      "SELECT id, name FROM users WHERE score > $1 ORDER BY id",
			params:   []boundParam{textParam(oidFloat8, "90")},
			wantRows: [][]string{{"1", "alice"}, {"3", "carol"}},
		},
		{
			name: "float8 binary range",
			sql:  "SELECT id, name FROM users WHERE score > $1 ORDER BY id",
			params: []boundParam{
				binaryParam(oidFloat8, be64b(int64(math.Float64bits(90)))),
			},
			wantRows: [][]string{{"1", "alice"}, {"3", "carol"}},
		},
		{
			name:     "bool true",
			sql:      "SELECT id, name FROM users WHERE active = $1 ORDER BY id",
			params:   []boundParam{textParam(oidBool, "t")},
			wantRows: [][]string{{"1", "alice"}, {"3", "carol"}},
		},
		{
			name:     "bool false binary",
			sql:      "SELECT id, name FROM users WHERE active = $1 ORDER BY id",
			params:   []boundParam{binaryParam(oidBool, []byte{0})},
			wantRows: [][]string{{"2", "bob"}},
		},
		{
			name:     "text equality",
			sql:      "SELECT id, name FROM users WHERE name = $1 ORDER BY id",
			params:   []boundParam{textParam(oidText, "bob")},
			wantRows: [][]string{{"2", "bob"}},
		},
		{
			name: "two parameters of different types",
			sql:  "SELECT id, name FROM users WHERE id > $1 AND name = $2 ORDER BY id",
			params: []boundParam{
				textParam(oidInt4, "1"),
				textParam(oidText, "carol"),
			},
			wantRows: [][]string{{"3", "carol"}},
		},
		{
			name: "mixed formats in one Bind",
			sql:  "SELECT id, name FROM users WHERE id = $1 AND name = $2 ORDER BY id",
			params: []boundParam{
				binaryParam(oidInt4, be32(3)),
				textParam(oidText, "carol"),
			},
			wantRows: [][]string{{"3", "carol"}},
		},
		{
			name:     "unknown OID stays a quoted string",
			sql:      "SELECT id, name FROM users WHERE name = $1 ORDER BY id",
			params:   []boundParam{textParam(oidUnknown, "alice")},
			wantRows: [][]string{{"1", "alice"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, names, rows, tag := client.paramQuery(tt.sql, tt.params)
			if strings.HasPrefix(tag, "ERROR") {
				t.Fatalf("%s: %s", tt.sql, tag)
			}
			if len(names) != 2 {
				t.Fatalf("columns = %v, want two", names)
			}
			if len(rows) != len(tt.wantRows) {
				t.Fatalf("got %d rows %v, want %d %v", len(rows), rows, len(tt.wantRows), tt.wantRows)
			}
			for i, want := range tt.wantRows {
				if len(rows[i]) != len(want) {
					t.Fatalf("row %d has %d values, want %d: %v", i, len(rows[i]), len(want), rows[i])
				}
				for j, w := range want {
					if rows[i][j] != w {
						t.Errorf("row %d value %d = %q, want %q", i, j, rows[i][j], w)
					}
				}
			}
		})
	}
}

// TestBindParamsWithQuotesAndNulls covers the values that break a naive
// substitution: a value carrying a single quote (which must not terminate the
// literal it is written into) and a NULL parameter (which is the SQL keyword,
// not the string "NULL").
func TestBindParamsWithQuotesAndNulls(t *testing.T) {
	db, srv := setupRealDB(t)
	ctx := context.Background()

	// A row whose name contains the characters that end a SQL literal.
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "label", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "quoted", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("quoted", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int32(1), "label": "it's fine"},
		{"id": int32(2), "label": "'; DROP TABLE quoted; --"},
		{"id": int32(3), "label": `back\slash`},
		{"id": int32(4), "label": "plain"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, tt := range []struct {
		label  string
		wantID string
	}{
		{"it's fine", "1"},
		{"'; DROP TABLE quoted; --", "2"},
		{`back\slash`, "3"},
		{"plain", "4"},
	} {
		t.Run(tt.label, func(t *testing.T) {
			_, _, rows, tag := client.paramQuery(
				"SELECT id FROM quoted WHERE label = $1",
				[]boundParam{textParam(oidText, tt.label)})
			if strings.HasPrefix(tag, "ERROR") {
				t.Fatalf("%s", tag)
			}
			if len(rows) != 1 || rows[0][0] != tt.wantID {
				t.Fatalf("got %v, want one row with id %s", rows, tt.wantID)
			}
		})
	}

	// The table is still there — the quote-terminating label above was data,
	// not statement text.
	_, _, rows, tag := client.paramQuery("SELECT id FROM quoted WHERE id > $1 ORDER BY id",
		[]boundParam{textParam(oidInt4, "0")})
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("%s", tag)
	}
	if len(rows) != 4 {
		t.Fatalf("got %d rows after the quoting cases, want 4", len(rows))
	}

	// A NULL parameter is the keyword. `= NULL` is never true in SQL, so the
	// result is empty — and not an error, and not a match on the text "NULL".
	_, _, rows, tag = client.paramQuery("SELECT id FROM quoted WHERE label = $1",
		[]boundParam{{oid: oidText, value: nil}})
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("NULL parameter: %s", tag)
	}
	if len(rows) != 0 {
		t.Fatalf("label = NULL matched %v, want no rows", rows)
	}
}

// TestParameterDescription covers issue #305 item 9. The server used to
// answer every Describe(statement) with "zero parameters", so a driver sizing
// its parameter list from the reply saw none for a statement that took three.
func TestParameterDescription(t *testing.T) {
	_, srv := setupRealDB(t)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	tests := []struct {
		name   string
		sql    string
		params []boundParam
		want   []uint32
	}{
		{
			name:   "declared OIDs are echoed",
			sql:    "SELECT id FROM users WHERE id = $1 AND name = $2",
			params: []boundParam{textParam(oidInt4, "2"), textParam(oidText, "bob")},
			want:   []uint32{oidInt4, oidText},
		},
		{
			name:   "one declared OID",
			sql:    "SELECT id FROM users WHERE id = $1",
			params: []boundParam{textParam(oidInt8, "2")},
			want:   []uint32{oidInt8},
		},
		{
			// Nothing declared: one entry per placeholder, all unknown. A
			// count of zero would have been a lie about the statement.
			name:   "undeclared parameters report unknown",
			sql:    "SELECT id FROM users WHERE id = $1 AND name = $2",
			params: []boundParam{textParam(oidUnknown, "2"), textParam(oidUnknown, "bob")},
			want:   []uint32{oidUnknown, oidUnknown},
		},
		{
			name:   "no parameters",
			sql:    "SELECT id FROM users",
			params: nil,
			want:   []uint32{},
		},
		{
			// A repeated placeholder is one parameter, not two.
			name:   "repeated placeholder counts once",
			sql:    "SELECT id FROM users WHERE id = $1 OR visits = $1",
			params: []boundParam{textParam(oidUnknown, "2")},
			want:   []uint32{oidUnknown},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oids, _, _, tag := client.paramQuery(tt.sql, tt.params)
			if strings.HasPrefix(tag, "ERROR") {
				t.Fatalf("%s", tag)
			}
			if len(oids) != len(tt.want) {
				t.Fatalf("ParameterDescription = %v, want %v", oids, tt.want)
			}
			for i, w := range tt.want {
				if oids[i] != w {
					t.Errorf("parameter %d OID = %d, want %d", i, oids[i], w)
				}
			}
		})
	}
}

// TestParameterDescriptionCountsPlaceholdersNotDeclarations covers the case a
// driver creates by declaring fewer types than the statement has placeholders:
// the reply still describes every placeholder, with the declared types first.
func TestParameterDescriptionCountsPlaceholdersNotDeclarations(t *testing.T) {
	_, srv := setupRealDB(t)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	// Parse declares one OID for a two-placeholder statement, then Bind
	// supplies both values.
	sqlText := "SELECT id FROM users WHERE id = $1 AND name = $2"
	var parseBuf []byte
	parseBuf = append(parseBuf, 0)
	parseBuf = append(parseBuf, sqlText...)
	parseBuf = append(parseBuf, 0)
	parseBuf = binary.BigEndian.AppendUint16(parseBuf, 1)
	parseBuf = binary.BigEndian.AppendUint32(parseBuf, oidInt4)
	client.writeMsg('P', parseBuf)
	client.writeMsg('D', []byte{'S', 0})
	client.writeMsg('S', nil)

	var oids []uint32
	for {
		typ, data, err := client.readMsg()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if typ == 't' {
			oids = parseParamDesc(data)
		}
		if typ == 'Z' {
			break
		}
	}
	if len(oids) != 2 || oids[0] != oidInt4 || oids[1] != oidUnknown {
		t.Fatalf("ParameterDescription = %v, want [%d %d]", oids, oidInt4, oidUnknown)
	}
}

// TestBindTenParameters is the $1-inside-$10 regression: substitution used a
// per-parameter strings.Replace, which found "$1" at the start of "$10".
func TestBindTenParameters(t *testing.T) {
	_, srv := setupRealDB(t)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	// Eleven parameters, all int4, so a mis-substituted $10 shows up as a
	// wrong row set rather than a parse error.
	sqlText := "SELECT id FROM users WHERE id IN ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ORDER BY id"
	params := make([]boundParam, 11)
	for i := range params {
		params[i] = textParam(oidInt4, "0")
	}
	params[9] = textParam(oidInt4, "2")  // $10
	params[10] = textParam(oidInt4, "3") // $11

	_, _, rows, tag := client.paramQuery(sqlText, params)
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("%s", tag)
	}
	if len(rows) != 2 || rows[0][0] != "2" || rows[1][0] != "3" {
		t.Fatalf("got %v, want ids 2 and 3 — $10/$11 were mis-substituted", rows)
	}
}

// TestBindUnsupportedBinaryParamErrors checks that bytes the server cannot
// read produce an ErrorResponse rather than a query built from them.
func TestBindUnsupportedBinaryParamErrors(t *testing.T) {
	_, srv := setupRealDB(t)
	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	_, _, rows, tag := client.paramQuery(
		"SELECT id FROM users WHERE name = $1",
		[]boundParam{binaryParam(3802 /* jsonb */, []byte{1, 2, 3})})
	if !strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("tag = %q, want an error for an undecodable binary parameter", tag)
	}
	if len(rows) != 0 {
		t.Fatalf("got rows %v for a rejected Bind", rows)
	}
}

// TestBindTimestampParam covers a timestamp parameter in both formats against
// stored RFC3339 timestamps. The binary form is microseconds from 2000-01-01,
// so a decoder using the Unix epoch is off by thirty years — visibly, not
// subtly.
func TestBindTimestampParam(t *testing.T) {
	db, srv := setupRealDB(t)
	ctx := context.Background()

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "at", Type: parquet.TypeString},
	}}
	if err := db.CreateTable(ctx, "stamps", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("stamps", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, []map[string]any{
		{"id": int32(1), "at": "2026-01-02T03:04:05Z"},
		{"id": int32(2), "at": "2026-06-15T12:00:00Z"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}

	client := newPGClient(t, srv.Addr())
	client.startup("wadjet", "wadjet")
	defer client.terminate()

	for _, tt := range []struct {
		name  string
		param boundParam
	}{
		{"text format", textParam(oidTimestamp, "2026-01-02T03:04:05Z")},
		// 2026-01-02T03:04:05Z as microseconds since 2000-01-01T00:00:00Z.
		{"binary format", binaryParam(oidTimestamp, be64b(820638245000000))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, _, rows, tag := client.paramQuery(
				"SELECT id FROM stamps WHERE at = $1", []boundParam{tt.param})
			if strings.HasPrefix(tag, "ERROR") {
				t.Fatalf("%s", tag)
			}
			if len(rows) != 1 || rows[0][0] != "1" {
				t.Fatalf("got %v, want one row with id 1", rows)
			}
		})
	}

	// A date parameter in binary format: days since 2000-01-01.
	_, _, rows, tag := client.paramQuery(
		"SELECT id FROM stamps WHERE at > $1 ORDER BY id",
		[]boundParam{binaryParam(oidDate, be32(9498))}) // 2026-01-02
	if strings.HasPrefix(tag, "ERROR") {
		t.Fatalf("date parameter: %s", tag)
	}
	if len(rows) != 2 {
		t.Fatalf("got %v, want both rows after 2026-01-02", rows)
	}
}

// --- driver-level coverage ---

// TestPgxBinaryParamsWithDeclaredOIDs drives parameters through pgx's
// low-level ExecParams, where the client declares the OIDs and the format
// codes — the shape a driver takes once it knows a parameter's type, and the
// binary path in particular.
func TestPgxBinaryParamsWithDeclaredOIDs(t *testing.T) {
	_, srv := setupRealDB(t)
	ctx := context.Background()
	addr := srv.Addr()
	connStr := fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		addr[len("127.0.0.1:"):])

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	tests := []struct {
		name     string
		sql      string
		values   [][]byte
		oids     []uint32
		formats  []int16
		wantRows [][]string
	}{
		{
			name:     "int4 binary",
			sql:      "SELECT id, name FROM users WHERE id = $1 ORDER BY id",
			values:   [][]byte{be32(3)},
			oids:     []uint32{oidInt4},
			formats:  []int16{1},
			wantRows: [][]string{{"3", "carol"}},
		},
		{
			name:     "int8 binary",
			sql:      "SELECT id, name FROM users WHERE visits = $1 ORDER BY id",
			values:   [][]byte{be64b(200)},
			oids:     []uint32{oidInt8},
			formats:  []int16{1},
			wantRows: [][]string{{"3", "carol"}},
		},
		{
			name:     "float8 binary",
			sql:      "SELECT id, name FROM users WHERE score < $1 ORDER BY id",
			values:   [][]byte{be64b(int64(math.Float64bits(90)))},
			oids:     []uint32{oidFloat8},
			formats:  []int16{1},
			wantRows: [][]string{{"2", "bob"}},
		},
		{
			name:     "bool binary",
			sql:      "SELECT id, name FROM users WHERE active = $1 ORDER BY id",
			values:   [][]byte{{1}},
			oids:     []uint32{oidBool},
			formats:  []int16{1},
			wantRows: [][]string{{"1", "alice"}, {"3", "carol"}},
		},
		{
			name:     "int4 binary with a text parameter alongside",
			sql:      "SELECT id, name FROM users WHERE id = $1 AND name = $2 ORDER BY id",
			values:   [][]byte{be32(1), []byte("alice")},
			oids:     []uint32{oidInt4, oidText},
			formats:  []int16{1, 0},
			wantRows: [][]string{{"1", "alice"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := conn.PgConn().ExecParams(ctx, tt.sql, tt.values, tt.oids, tt.formats, nil).Read()
			if res.Err != nil {
				t.Fatalf("ExecParams: %v", res.Err)
			}
			if len(res.Rows) != len(tt.wantRows) {
				t.Fatalf("got %d rows, want %d", len(res.Rows), len(tt.wantRows))
			}
			for i, want := range tt.wantRows {
				for j, w := range want {
					if got := string(res.Rows[i][j]); got != w {
						t.Errorf("row %d value %d = %q, want %q", i, j, got, w)
					}
				}
			}
		})
	}
}

// TestPgxAndLibPQTextParams covers what the two Go drivers do on their own.
// Neither declares parameter types at Parse — both read the server's
// ParameterDescription and encode against it — so with every parameter
// reported unknown they send text, and the server writes a quoted literal.
// That is right for a text column, and this is the case the old code was
// already correct about.
func TestPgxAndLibPQTextParams(t *testing.T) {
	_, srv := setupRealDB(t)
	ctx := context.Background()
	addr := srv.Addr()
	connStr := fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		addr[len("127.0.0.1:"):])

	t.Run("pgx", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			t.Fatalf("pgx connect: %v", err)
		}
		defer conn.Close(ctx)

		var id int32
		var name string
		err = conn.QueryRow(ctx, "SELECT id, name FROM users WHERE name = $1", "bob").Scan(&id, &name)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 2 || name != "bob" {
			t.Fatalf("got (%d, %q), want (2, bob)", id, name)
		}

		// Two text parameters, one carrying a quote.
		var n int64
		err = conn.QueryRow(ctx,
			"SELECT COUNT(*) FROM users WHERE name = $1 OR name = $2",
			"it's not here", "carol").Scan(&n)
		if err != nil {
			t.Fatalf("two-parameter query: %v", err)
		}
		if n != 1 {
			t.Fatalf("count = %d, want 1", n)
		}
	})

	t.Run("libpq", func(t *testing.T) {
		db := openPQ(t, addr)
		var id int32
		var name string
		err := db.QueryRow("SELECT id, name FROM users WHERE name = $1", "carol").Scan(&id, &name)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 3 || name != "carol" {
			t.Fatalf("got (%d, %q), want (3, carol)", id, name)
		}

		// A prepared statement round trip: lib/pq reads the
		// ParameterDescription to size and type its parameter list, so a
		// count of zero for a one-parameter statement would fail here.
		stmt, err := db.Prepare("SELECT name FROM users WHERE name = $1")
		if err != nil {
			t.Fatalf("prepare: %v", err)
		}
		defer stmt.Close()
		var got string
		if err := stmt.QueryRow("alice").Scan(&got); err != nil {
			t.Fatalf("prepared query: %v", err)
		}
		if got != "alice" {
			t.Fatalf("got %q, want alice", got)
		}
		if err := stmt.QueryRow("nobody").Scan(&got); err != sql.ErrNoRows {
			t.Fatalf("expected no rows, got %v (%q)", err, got)
		}
	})
}

// TestPgxDescribeReportsParameterCount checks the count a driver sees for a
// statement it has not declared types for: one entry per placeholder, each
// unknown. pgx surfaces it as StatementDescription.ParamOIDs.
func TestPgxDescribeReportsParameterCount(t *testing.T) {
	_, srv := setupRealDB(t)
	ctx := context.Background()
	addr := srv.Addr()
	connStr := fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		addr[len("127.0.0.1:"):])

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	for _, tt := range []struct {
		sql  string
		want int
	}{
		{"SELECT id FROM users WHERE id = $1", 1},
		{"SELECT id FROM users WHERE id = $1 AND name = $2", 2},
		{"SELECT id FROM users WHERE id = $1 OR id = $1", 1},
		{"SELECT id FROM users", 0},
	} {
		t.Run(tt.sql, func(t *testing.T) {
			sd, err := conn.Prepare(ctx, tt.sql, tt.sql)
			if err != nil {
				t.Fatalf("prepare: %v", err)
			}
			if len(sd.ParamOIDs) != tt.want {
				t.Fatalf("ParamOIDs = %v, want %d entries", sd.ParamOIDs, tt.want)
			}
			for i, oid := range sd.ParamOIDs {
				if oid != oidUnknown {
					t.Errorf("parameter %d OID = %d, want %d (unknown — this "+
						"server infers no parameter types)", i, oid, oidUnknown)
				}
			}
		})
	}
}
