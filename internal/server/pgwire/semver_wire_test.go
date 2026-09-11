package pgwire

// WHAT THE WIRE CARRIES FOR THE SEMVER FAMILY (#967).
//
// A value oracle cannot see a right value under a wrong OID, which is why this
// exists beside the value gates. PostgreSQL has no semver at all, so there is
// no `pg_typeof` to copy for the functions themselves; what IS PostgreSQL's
// and is copied exactly is the width arithmetic ABOVE them, measured on 17.11
// over `a3_pkgs(id int, v text)`:
//
//	pg_typeof(split_part(v,'.',1)::bigint)               bigint
//	pg_typeof(sum(split_part(v,'.',1)::bigint))          numeric
//	pg_typeof(sum(split_part(v,'.',1)::int))             bigint
//	pg_typeof(avg(split_part(v,'.',1)::bigint))          numeric
//	pg_typeof(sum(split_part(v,'.',1)::bigint) OVER ())  numeric
//	pg_typeof(sum(split_part(v,'.',1)::int)   OVER ())   bigint
//
// So the three component functions, which `expr.PGIntegerResultWidth` puts on
// the int8 row, must make their SUM NUMERIC (OID 1700) grouped and windowed,
// and `semver_cmp`, which is on the int4 row, must make its SUM BIGINT
// (OID 20). Those four cells are the whole reason those rows exist: reading
// the engine's int64 CARRIER instead would declare numeric for both.

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// setupSemverTable adds a version table to the pgwire fixture database.
// Three rows, so a grouped SUM has something to add: 1.0.0, v1.2.3-rc.1+b7
// and 2.10.0.
func setupSemverTable(t *testing.T) *Server {
	t.Helper()
	db, srv := setupRealDB(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "g", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "pkgs", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"id": int32(1), "g": int32(1), "v": "1.0.0"},
		{"id": int32(2), "g": int32(1), "v": "v1.2.3-rc.1+b7"},
		{"id": int32(3), "g": int32(1), "v": "2.10.0"},
	}
	ing := db.NewIngester("pkgs", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

func TestPGWireDeclaresTheSemverFamily(t *testing.T) {
	srv := setupSemverTable(t)
	conn := connectPgconn(t, srv.Addr())

	res := conn.ExecParams(context.Background(),
		`SELECT semver_valid(v)                  AS b,
		        semver_major(v)                  AS maj,
		        semver_minor(v)                  AS min,
		        semver_patch(v)                  AS pat,
		        semver_cmp(v, '1.2.3')           AS c,
		        semver_prerelease(v)             AS pre,
		        semver_build(v)                  AS bld,
		        semver_normalize(v)              AS nrm,
		        semver_sort_key(v)               AS k,
		        semver_satisfies(v, '^1.0.0')    AS s
		 FROM pkgs ORDER BY id`, nil, nil, nil, []int16{0}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	for _, want := range []struct {
		col  int
		name string
		oid  uint32
		size int16
	}{
		{0, "semver_valid", 16, 1},       // bool
		{1, "semver_major", 20, 8},       // int8 — the domain the specification bounds at nothing
		{2, "semver_minor", 20, 8},       // int8
		{3, "semver_patch", 20, 8},       // int8
		{4, "semver_cmp", 23, 4},         // int4 — the domain is {-1,0,1}
		{5, "semver_prerelease", 25, -1}, // text
		{6, "semver_build", 25, -1},      // text
		{7, "semver_normalize", 25, -1},  // text
		{8, "semver_sort_key", 25, -1},   // text
		{9, "semver_satisfies", 16, 1},   // bool
	} {
		f := res.FieldDescriptions[want.col]
		if f.DataTypeOID != want.oid {
			t.Errorf("%s declared OID %d, want %d", want.name, f.DataTypeOID, want.oid)
		}
		if f.DataTypeSize != want.size {
			t.Errorf("%s declared size %d, want %d", want.name, f.DataTypeSize, want.size)
		}
	}

	// The rendered cells, in id order. The `v` prefix is gone from the
	// canonical spelling; the build metadata survives there and is absent from
	// the sort key, which is the contract ADR-0012 records.
	wantRows := [][]string{
		{"t", "1", "0", "0", "-1", "", "", "1.0.0", a3wKey("1.0.0"), "t"},
		{"t", "1", "2", "3", "-1", "rc.1", "b7", "1.2.3-rc.1+b7", a3wKey("1.2.3-rc.1"), "f"},
		{"t", "2", "10", "0", "1", "", "", "2.10.0", a3wKey("2.10.0"), "f"},
	}
	if len(res.Rows) != len(wantRows) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(wantRows))
	}
	for i, wr := range wantRows {
		for j, w := range wr {
			if got := string(res.Rows[i][j]); got != w {
				t.Errorf("row %d column %d rendered %q, want %q", i, j, got, w)
			}
		}
	}
}

// a3wKey spells a sort key the way the family builds it, so the expectations
// above read as versions rather than as 58 characters of zero padding. It is a
// RENDERING helper for this file only; the key's ORDER is the contract and its
// bytes are not, which is why no other gate names them.
func a3wKey(version string) string {
	major, minor, patch, pre := "", "", "", ""
	fields := []string{}
	core := version
	if i := indexByte(core, '-'); i >= 0 {
		pre, core = core[i+1:], core[:i]
	}
	for _, f := range splitDots(core) {
		fields = append(fields, f)
	}
	major, minor, patch = fields[0], fields[1], fields[2]
	out := pad19(major) + pad19(minor) + pad19(patch)
	if pre == "" {
		return out + "~"
	}
	out += "-"
	for i, id := range splitDots(pre) {
		if i > 0 {
			out += ","
		}
		if allDigits(id) {
			out += "-" + pad19(id)
		} else {
			out += "." + id
		}
	}
	return out
}

func pad19(s string) string {
	for len(s) < 19 {
		s = "0" + s
	}
	return s
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return len(s) > 0
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

func splitDots(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == '.' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	return out
}

// THE BOOLEAN HALF IN THE BINARY FORMAT, which a text comparison cannot see:
// one byte, 0 or 1.
func TestPGWireRendersASemverPredicateAsABinaryBool(t *testing.T) {
	srv := setupSemverTable(t)
	conn := connectPgconn(t, srv.Addr())
	res := conn.ExecParams(context.Background(),
		`SELECT semver_satisfies(v, '>=1.0.0 <2.0.0') AS s FROM pkgs ORDER BY id`,
		nil, nil, nil, []int16{1}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	// 1.0.0 is in; 1.2.3-rc.1 is a PRE-RELEASE and the range names none, so it
	// is out (node-semver's published rule); 2.10.0 is above the bound.
	want := []byte{1, 0, 0}
	if len(res.Rows) != len(want) {
		t.Fatalf("got %d rows, want %d", len(res.Rows), len(want))
	}
	for i, w := range want {
		got := res.Rows[i][0]
		if len(got) != 1 || got[0] != w {
			t.Errorf("row %d binary bool = %v, want [%d]", i, got, w)
		}
	}
}

// SUM OVER A SEMVER FUNCTION DECLARES WHAT POSTGRESQL DECLARES FOR ITS WIDTH,
// grouped and windowed, in both wire formats.
//
// This is the cell the 2026-09-11 width amendment exists for. `semver_major`
// is int8-declared, so its SUM is NUMERIC (OID 1700) — PostgreSQL's
// `sum(bigint)`; `semver_cmp` is int4-declared, so its SUM is BIGINT (OID 20)
// — PostgreSQL's `sum(int)`. Reading the engine's int64 carrier instead would
// make both numeric, which is the defect the table was built to prevent.
func TestPGWireDeclaresSumOverASemverFunction(t *testing.T) {
	srv := setupSemverTable(t)
	conn := connectPgconn(t, srv.Addr())
	for _, format := range []int16{0, 1} {
		for _, tc := range []struct {
			name, sql string
			oid       uint32
		}{
			{"sum_major_grouped", `SELECT SUM(semver_major(v)) AS s FROM pkgs GROUP BY g`, 1700},
			{"sum_minor_grouped", `SELECT SUM(semver_minor(v)) AS s FROM pkgs GROUP BY g`, 1700},
			{"sum_patch_grouped", `SELECT SUM(semver_patch(v)) AS s FROM pkgs GROUP BY g`, 1700},
			{"sum_major_windowed", `SELECT SUM(semver_major(v)) OVER () AS s FROM pkgs`, 1700},
			{"avg_major_grouped", `SELECT AVG(semver_major(v)) AS s FROM pkgs GROUP BY g`, 1700},
			{"sum_cmp_grouped", `SELECT SUM(semver_cmp(v,'1.2.3')) AS s FROM pkgs GROUP BY g`, 20},
			{"sum_cmp_windowed", `SELECT SUM(semver_cmp(v,'1.2.3')) OVER () AS s FROM pkgs`, 20},
			{"count_valid_grouped", `SELECT COUNT(*) AS s FROM pkgs WHERE semver_valid(v) GROUP BY g`, 20},
		} {
			t.Run(tc.name+map[int16]string{0: "/text", 1: "/binary"}[format], func(t *testing.T) {
				res := conn.ExecParams(context.Background(), tc.sql, nil, nil, nil, []int16{format}).Read()
				if res.Err != nil {
					t.Fatalf("ExecParams: %v", res.Err)
				}
				if got := res.FieldDescriptions[0].DataTypeOID; got != tc.oid {
					t.Errorf("%s declared OID %d, want %d", tc.name, got, tc.oid)
				}
			})
		}
	}
}

// A RANGE THAT NAMES NO RANGE REACHES THE CLIENT AS 22023 — through the wire,
// in both formats, and with NO ROWS AT ALL, which is the shape a per-row fold
// gets wrong.
func TestPGWireRefusesAnInvalidSemverRangeWithNoRows(t *testing.T) {
	srv := setupSemverTable(t)
	conn := connectPgconn(t, srv.Addr())
	for _, format := range []int16{0, 1} {
		for _, sql := range []string{
			`SELECT semver_satisfies(v,'^^1.0') AS s FROM pkgs WHERE id < 0`,
			`SELECT COUNT(*) AS n FROM pkgs WHERE id < 0 AND semver_satisfies(v,'^^1.0')`,
			`SELECT id FROM pkgs WHERE id < 0 GROUP BY id HAVING semver_satisfies(MIN(v),'^^1.0')`,
			`SELECT semver_satisfies(v,'') AS s FROM pkgs WHERE id < 0`,
		} {
			res := conn.ExecParams(context.Background(), sql, nil, nil, nil, []int16{format}).Read()
			if res.Err == nil {
				t.Errorf("format %d: %q answered rows instead of refusing", format, sql)
				continue
			}
			if code := pgErrCode(res.Err); code != "22023" {
				t.Errorf("format %d: %q answered SQLSTATE %q, want 22023 (%v)",
					format, sql, code, res.Err)
			}
		}
	}
}

// And the loud twin on the data side reaches the client the same way.
func TestPGWireRefusesAStringTheStrictNormalizerCannotRead(t *testing.T) {
	srv := setupSemverTable(t)
	conn := connectPgconn(t, srv.Addr())
	res := conn.ExecParams(context.Background(),
		`SELECT semver_normalize_strict('latest') AS n FROM pkgs`, nil, nil, nil, []int16{0}).Read()
	if res.Err == nil {
		t.Fatal("semver_normalize_strict answered rows for a string that is not a version")
	}
	if code := pgErrCode(res.Err); code != "22023" {
		t.Errorf("SQLSTATE %q, want 22023 (%v)", code, res.Err)
	}
	// The lenient twin over the same string answers NULL rather than refusing.
	res = conn.ExecParams(context.Background(),
		`SELECT semver_normalize('latest') AS n FROM pkgs`, nil, nil, nil, []int16{0}).Read()
	if res.Err != nil {
		t.Fatalf("semver_normalize refused where it must answer NULL: %v", res.Err)
	}
	for i, r := range res.Rows {
		if r[0] != nil {
			t.Errorf("row %d rendered %q, want NULL", i, string(r[0]))
		}
	}
}

// setupSemverBoundTable adds a table whose versions sit AT the acceptance
// bound, so a range whose desugaring raises a component past int64's maximum
// has rows on both sides of the bound to get right.
func setupSemverBoundTable(t *testing.T) *Server {
	t.Helper()
	db, srv := setupRealDB(t)
	ctx := context.Background()
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "v", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "pkgmax", schema, nil); err != nil {
		t.Fatal(err)
	}
	const max = "9223372036854775807"
	rows := []map[string]any{
		{"id": int32(1), "v": max + ".0.0"},
		{"id": int32(2), "v": "1.0.0"},
		{"id": int32(3), "v": "1." + max + ".0"},
		{"id": int32(4), "v": "2.0.0"},
	}
	ing := db.NewIngester("pkgmax", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

// A RANGE WHOSE BOUND SITS AT THE ACCEPTANCE BOUND CARRIES THE SAME BOOLEAN IN
// BOTH WIRE FORMATS (#967, round-1 review B1).
//
// The value gates say what the answer is; this says the WIRE carries it, in
// text (`t`/`f`) and in binary (one byte), under OID 16. Before the bound was
// saturated the first row answered `f` for every row and the third answered
// `t` for every row — a wrong boolean is exactly as wrong on the wire.
func TestPGWireCarriesARangeAtTheAcceptanceBoundInBothFormats(t *testing.T) {
	srv := setupSemverBoundTable(t)
	conn := connectPgconn(t, srv.Addr())
	const max = "9223372036854775807"
	for _, tc := range []struct {
		name, rng string
		want      []bool
	}{
		{"caret_at_the_maximum_major", "^" + max + ".0.0", []bool{true, false, false, false}},
		{"above_the_maximum_major", ">" + max + ".x", []bool{false, false, false, false}},
		{"the_maximum_minor_band_still_bounds", "1." + max + ".x", []bool{false, false, true, false}},
		{"below_the_maximum", "<=" + max + ".x", []bool{true, true, true, true}},
	} {
		for _, format := range []int16{0, 1} {
			t.Run(tc.name+map[int16]string{0: "/text", 1: "/binary"}[format], func(t *testing.T) {
				res := conn.ExecParams(context.Background(),
					`SELECT semver_satisfies(v, '`+tc.rng+`') AS s FROM pkgmax ORDER BY id`,
					nil, nil, nil, []int16{format}).Read()
				if res.Err != nil {
					t.Fatalf("ExecParams: %v", res.Err)
				}
				if got := res.FieldDescriptions[0].DataTypeOID; got != 16 {
					t.Errorf("declared OID %d, want 16", got)
				}
				if len(res.Rows) != len(tc.want) {
					t.Fatalf("got %d rows, want %d", len(res.Rows), len(tc.want))
				}
				for i, w := range tc.want {
					got := res.Rows[i][0]
					var ok bool
					if format == 0 {
						ok = string(got) == map[bool]string{true: "t", false: "f"}[w]
					} else {
						ok = len(got) == 1 && got[0] == map[bool]byte{true: 1, false: 0}[w]
					}
					if !ok {
						t.Errorf("row %d carried %q, want %v", i, got, w)
					}
				}
			})
		}
	}
}
