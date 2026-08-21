package pgwire

// Regression tests for #362: a portal bound with binary result formats must
// get a RowDescription that DECLARES those format codes. The DataRow bytes
// were already binary; the declaration said text, so pgx handed four
// big-endian int4 bytes to strconv.ParseInt. Per the protocol, a Describe of
// a PORTAL carries the format codes the Bind chose; only a Describe of a
// STATEMENT is unconditionally text.

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

// TestPortalBinaryResultFormatDeclared drives the exact pgx path from the
// issue: ExecParams with all-binary result formats. Every field must come
// back declared format 1, and the bytes must decode under the declared OID.
func TestPortalBinaryResultFormatDeclared(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	res := conn.ExecParams(context.Background(),
		`SELECT id, name FROM users ORDER BY id`,
		nil, nil, nil, []int16{1, 1}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	if len(res.FieldDescriptions) != 2 {
		t.Fatalf("got %d fields, want 2", len(res.FieldDescriptions))
	}
	for i, f := range res.FieldDescriptions {
		if f.Format != 1 {
			t.Errorf("field %d (%s) declared format %d after the client asked for 1", i, f.Name, f.Format)
		}
	}
	if len(res.Rows) != 3 {
		t.Fatalf("got %d rows, want 3", len(res.Rows))
	}
	// The bytes must decode under the OID and format the server declared.
	m := pgtype.NewMap()
	var id int32
	if err := m.Scan(res.FieldDescriptions[0].DataTypeOID, res.FieldDescriptions[0].Format,
		res.Rows[0][0], &id); err != nil {
		t.Fatalf("row 0 col 0 does not decode under declared OID %d format %d: %v",
			res.FieldDescriptions[0].DataTypeOID, res.FieldDescriptions[0].Format, err)
	}
	if id != 1 {
		t.Errorf("id decoded to %d, want 1", id)
	}
	var name string
	if err := m.Scan(res.FieldDescriptions[1].DataTypeOID, res.FieldDescriptions[1].Format,
		res.Rows[0][1], &name); err != nil {
		t.Fatalf("row 0 col 1 does not decode: %v", err)
	}
	if name != "alice" {
		t.Errorf("name decoded to %q, want alice", name)
	}
}

// TestPortalMixedResultFormatsDeclared asks for one binary and one text
// column: the declaration must be per-field, not a single blanket code.
func TestPortalMixedResultFormatsDeclared(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	res := conn.ExecParams(context.Background(),
		`SELECT id, name FROM users ORDER BY id`,
		nil, nil, nil, []int16{1, 0}).Read()
	if res.Err != nil {
		t.Fatalf("ExecParams: %v", res.Err)
	}
	want := []int16{1, 0}
	for i, f := range res.FieldDescriptions {
		if f.Format != want[i] {
			t.Errorf("field %d (%s) declared format %d, want %d", i, f.Name, f.Format, want[i])
		}
	}
	if got := string(res.Rows[0][1]); got != "alice" {
		t.Errorf("text column carried %q, want alice", got)
	}
	if got := len(res.Rows[0][0]); got != 4 {
		t.Errorf("binary int4 column carried %d bytes, want 4", got)
	}
}

// TestStatementDescribeStaysText pins the other half of the protocol rule: a
// Describe of a STATEMENT (before any Bind) is unconditionally text.
func TestStatementDescribeStaysText(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())

	desc, err := conn.Prepare(context.Background(), "", `SELECT id, name FROM users`, nil)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	for i, f := range desc.Fields {
		if f.Format != 0 {
			t.Errorf("statement-describe field %d (%s) declared format %d, want 0 (text)", i, f.Name, f.Format)
		}
	}
}
