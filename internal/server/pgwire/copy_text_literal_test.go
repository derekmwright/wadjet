package pgwire

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// A COPY field is RAW TEXT, not a SQL literal.
//
// It went through the literal converter, which owns two rules that belong to
// literals and to nothing else: the word `null` is the SQL NULL keyword, and a
// leading and trailing apostrophe are quoting. So a COPY field spelled `NULL`
// became a SQL NULL — even though COPY's own NULL marker is `\N`, handled
// separately and correctly one branch above — and a field whose text happens
// to begin and end with an apostrophe silently lost both of them (#690).
//
// PostgreSQL stores those four letters. The distinction matters most exactly
// where COPY is used: bulk loading somebody else's export, where a column
// legitimately holds the string "NULL" and the loader has no way to say so.
func TestPGWireCopyTreatsAFieldAsTextNotALiteral(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		want  any
	}{
		{name: "the word NULL is four letters", field: "NULL", want: "NULL"},
		{name: "the word null is four letters", field: "null", want: "null"},
		{name: `the backslash-N marker is the NULL`, field: `\N`, want: nil},
		{name: "apostrophes are data", field: "'quoted'", want: "'quoted'"},
		{name: "an ordinary value is unchanged", field: "plain", want: "plain"},
		{name: "an empty field is the empty string", field: "", want: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			db := setupTextCopyDB(t)
			srv := startTestServer(t, db)
			client := newPGClient(t, srv.Addr())
			client.startup("test", "wadjet")

			client.writeMsg('Q', append([]byte("COPY txt (id, s) FROM STDIN"), 0))
			typ, payload, err := client.readMsg()
			if err != nil {
				t.Fatalf("reading CopyInResponse: %v", err)
			}
			if typ != 'G' {
				t.Fatalf("expected CopyInResponse ('G'), got '%c': %s", typ, client.parseError(payload))
			}
			client.sendCopyData("1\t" + tc.field + "\n")
			client.sendCopyDone()
			if typ, payload, err = client.readMsg(); err != nil {
				t.Fatalf("reading the reply: %v", err)
			}
			if typ == 'E' {
				t.Fatalf("COPY of %q was refused: %s", tc.field, client.parseError(payload))
			}
			client.terminate()

			res, err := db.Query(ctx, "SELECT s FROM txt")
			if err != nil {
				t.Fatalf("reading txt back: %v", err)
			}
			if len(res.Rows) != 1 {
				t.Fatalf("%d rows after COPY, want 1", len(res.Rows))
			}
			if got := res.Rows[0]["s"]; got != tc.want {
				t.Errorf("COPY field %q stored %#v, want %#v", tc.field, got, tc.want)
			}
		})
	}
}

func setupTextCopyDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "s", Type: parquet.TypeString, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "txt", schema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}
