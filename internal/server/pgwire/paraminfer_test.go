package pgwire

// Regression tests for #365: a parameter the client did NOT declare a type
// for must (a) get an inferred OID in ParameterDescription where its use
// decides one, and (b) bind to the same row the declared-OID path binds to.
// Before the fix, ParameterDescription answered OID 0 and the text bytes "7"
// rendered as the quoted literal '7', which the engine coerced against an int
// column to 0 — a silently WRONG row.

import (
	"context"
	"testing"
)

func TestUndeclaredParamInfersOIDAndMatchesRightRow(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	// The inference half: Describe must answer int4 for a parameter compared
	// against an int4 column.
	desc, err := conn.Prepare(ctx, "", `SELECT id, name FROM users WHERE id = $1`, nil)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if len(desc.ParamOIDs) != 1 {
		t.Fatalf("ParameterDescription has %d OIDs, want 1", len(desc.ParamOIDs))
	}
	if desc.ParamOIDs[0] != 23 {
		t.Errorf("undeclared parameter described as OID %d, want 23 (int4) inferred from the comparison", desc.ParamOIDs[0])
	}

	// The correctness half: the bound value must match ITS row, exactly as
	// the declared-OID path does.
	res := conn.ExecParams(ctx, `SELECT id, name FROM users WHERE id = $1`,
		[][]byte{[]byte("2")}, nil, nil, nil).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	if len(res.Rows) != 1 {
		t.Fatalf("got %d rows, want 1", len(res.Rows))
	}
	if id, name := string(res.Rows[0][0]), string(res.Rows[0][1]); id != "2" || name != "bob" {
		t.Errorf("undeclared parameter 2 matched row (%s, %s), want (2, bob)", id, name)
	}
}

func TestUndeclaredParamAgainstStringColumnStaysQuoted(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	desc, err := conn.Prepare(ctx, "", `SELECT id FROM users WHERE name = $1`, nil)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if len(desc.ParamOIDs) != 1 || desc.ParamOIDs[0] != 25 {
		t.Errorf("parameter against a text column described as %v, want [25]", desc.ParamOIDs)
	}

	res := conn.ExecParams(ctx, `SELECT id FROM users WHERE name = $1`,
		[][]byte{[]byte("carol")}, nil, nil, nil).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	if len(res.Rows) != 1 || string(res.Rows[0][0]) != "3" {
		t.Errorf("name = $1 bound with carol answered %v rows, want the single id 3", len(res.Rows))
	}
}

func TestDeclaredParamOIDIsNeverOverridden(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	ctx := context.Background()

	// The client's declaration wins even where inference would answer too.
	desc, err := conn.Prepare(ctx, "", `SELECT id FROM users WHERE id = $1`, []uint32{20})
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if len(desc.ParamOIDs) != 1 || desc.ParamOIDs[0] != 20 {
		t.Errorf("declared OID echoed as %v, want [20]", desc.ParamOIDs)
	}
}

func TestUninferableParamStaysUnknown(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	// $1 stands against no column; OID 0 is the honest report.
	desc, err := conn.Prepare(context.Background(), "", `SELECT $1 FROM users`, nil)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if len(desc.ParamOIDs) != 1 || desc.ParamOIDs[0] != 0 {
		t.Errorf("uninferable parameter described as %v, want [0]", desc.ParamOIDs)
	}
}

// comparisonColumn is the lexical half of the inference; pin its shapes.
func TestComparisonColumn(t *testing.T) {
	cases := []struct {
		sql  string
		want string // column for $1
	}{
		{`SELECT * FROM users WHERE id = $1`, "id"},
		{`SELECT * FROM users WHERE id=$1`, "id"},
		{`SELECT * FROM users WHERE $1 = id`, "id"},
		{`SELECT * FROM users WHERE u.id <= $1`, "id"},
		{`SELECT * FROM users WHERE "id" > $1`, "id"},
		{`SELECT * FROM users WHERE id <> $1`, "id"},
		{`SELECT * FROM users WHERE visits >= $1 AND active`, "visits"},
		{`SELECT $1`, ""},
		{`SELECT * FROM users WHERE id + 1 = $1`, "1"}, // literal → dropped by lookup
		{`SELECT * FROM users WHERE f(id) = $1`, ""},   // ) is not an identifier
		{`SELECT * FROM users WHERE NULL = $1`, ""},
	}
	for _, tc := range cases {
		refs := scanParamRefs(tc.sql)
		if len(refs) == 0 {
			if tc.want != "" {
				t.Errorf("%s: no refs found", tc.sql)
			}
			continue
		}
		got := comparisonColumn(tc.sql, refs[0])
		// A numeric "column" never resolves in the schema map, so both "" and
		// the literal are safe; normalize the literal case.
		if got != tc.want && !(tc.want == "1" && got == "") {
			t.Errorf("%s: comparisonColumn = %q, want %q", tc.sql, got, tc.want)
		}
	}
}
