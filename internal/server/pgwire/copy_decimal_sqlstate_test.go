package pgwire

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// COPY reports the converter's OWN SQLSTATE.
//
// The branch hardcoded 22P02 invalid_text_representation for every conversion
// failure. Once ConvertValueForColumn could refuse a DECIMAL field past the
// column's declared precision — which is 22003 numeric_value_out_of_range, a
// different condition — COPY told the client the wrong one, and COPY is the
// bulk path where a client most needs to tell a bad VALUE from bad TEXT
// (#647 re-review).
func TestPGWireCopyDecimalReportsTheConvertersSQLSTATE(t *testing.T) {
	for _, tc := range []struct {
		name  string
		field string
		state string
	}{
		{name: "past the declared precision", field: "99999999999999999999.99", state: "22003"},
		{name: "rounding into the overflow", field: "9999999.999", state: "22003"},
		{name: "NaN has no stored value", field: "NaN", state: "22003"},
		{name: "text naming no number", field: "abc", state: "22P02"},
		{name: "a good value still copies", field: "12.34", state: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := setupDecimalCopyDB(t)
			srv := startTestServer(t, db)
			client := newPGClient(t, srv.Addr())
			client.startup("test", "wadjet")

			client.writeMsg('Q', append([]byte("COPY dec (id, d) FROM STDIN"), 0))
			typ, payload, err := client.readMsg()
			if err != nil {
				t.Fatalf("reading CopyInResponse: %v", err)
			}
			if typ != 'G' {
				t.Fatalf("expected CopyInResponse ('G'), got '%c': %s", typ, client.parseError(payload))
			}

			client.sendCopyData("1\t" + tc.field + "\n")
			client.sendCopyDone()

			typ, payload, err = client.readMsg()
			if err != nil {
				t.Fatalf("reading the reply: %v", err)
			}
			if tc.state == "" {
				if typ == 'E' {
					t.Fatalf("a good value was refused: %s", client.parseError(payload))
				}
				client.terminate()
				return
			}
			if typ != 'E' {
				t.Fatalf("COPY of %q was accepted; want SQLSTATE %s", tc.field, tc.state)
			}
			if got := copyErrorCode(payload); got != tc.state {
				t.Fatalf("SQLSTATE %q, want %q (message: %s)", got, tc.state, client.parseError(payload))
			}
			client.terminate()
		})
	}
}

func setupDecimalCopyDB(t *testing.T) *wadjet.DB {
	t.Helper()
	ctx := context.Background()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "dec", schema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}

// copyErrorCode reads the 'C' (SQLSTATE) field of an ErrorResponse, which the
// shared parseError helper discards in favour of the message.
func copyErrorCode(data []byte) string {
	for len(data) > 0 {
		fieldType := data[0]
		data = data[1:]
		if fieldType == 0 {
			break
		}
		val := readCString(data)
		data = data[len(val)+1:]
		if fieldType == 'C' {
			return val
		}
	}
	return ""
}
