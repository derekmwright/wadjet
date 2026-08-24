package pgwire

// Regression coverage for #464: bindparams.go folded oidNumeric into the
// same arm as oidText/oidVarchar, which is only correct for binary format
// because those types' binary form IS their text form. numeric's binary form
// is a base-10000 digit vector, and pgx v5 sends exactly that — OID 1700,
// format 1 — for any parameter compared against a DECIMAL column, once
// paraminfer.go infers the OID from the comparison. So the old code read the
// digit-group bytes as if they were ASCII, which is not valid UTF-8 in
// general and is never a valid number, and renderTextParam's fallback wrote
// it out as a quoted string: `d = $1` compared a DECIMAL column to text and
// matched nothing, silently.
//
// This file drives that path through a REAL pgx v5 connection (not a
// hand-built wire client), because the failure pgx's own NumericCodec
// produces — which bytes it sends, in which format, for which OID — is the
// thing under test, not a restatement of what bindparams_test.go already
// pins at the decoder level.

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// decimal128FromText renders a plain decimal string (optionally signed) into
// the parquet.Decimal128 box a DECIMAL column of the given scale stores,
// working entirely over big.Int so a value past int64 (or past float64's
// exact range) survives ingestion exactly — the same approach
// wadjet/decimal_literal_test.go uses for the same reason.
func decimal128FromText(t *testing.T, s string, scale int) parquet.Decimal128 {
	t.Helper()
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	if len(fracPart) > scale {
		t.Fatalf("value %q has more than %d fraction digits", s, scale)
	}
	for len(fracPart) < scale {
		fracPart += "0"
	}
	n, ok := new(big.Int).SetString(intPart+fracPart, 10)
	if !ok {
		t.Fatalf("%q is not a decimal", s)
	}
	if neg {
		n.Neg(n)
	}
	m := new(big.Int).Set(n)
	if m.Sign() < 0 {
		m.Add(m, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	var b [16]byte
	m.FillBytes(b[:])
	return parquet.Decimal128{
		Hi: int64(binary.BigEndian.Uint64(b[0:8])),
		Lo: binary.BigEndian.Uint64(b[8:16]),
	}
}

// pgNumericArg builds a pgtype.Numeric for s, the way a caller of pgx's own
// API would: as an exact (Int, Exp) pair, never through a float. Passed as a
// pgx query argument this is what triggers NumericCodec's binary encoding —
// PreferredFormat() is BinaryFormatCode — which is the shape under test.
func pgNumericArg(t *testing.T, s string) pgtype.Numeric {
	t.Helper()
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	intPart, fracPart := s, ""
	if i := strings.IndexByte(s, '.'); i >= 0 {
		intPart, fracPart = s[:i], s[i+1:]
	}
	n, ok := new(big.Int).SetString(intPart+fracPart, 10)
	if !ok {
		t.Fatalf("%q is not a decimal", s)
	}
	if neg {
		n.Neg(n)
	}
	return pgtype.Numeric{Int: n, Exp: int32(-len(fracPart)), Valid: true}
}

// setupDecimalDB creates a DECIMAL(38,10) fixture wide enough to need every
// part of the decoder: an ordinary value, a value past int64 (the reason a
// DECIMAL column exists), its negation, and a 25-significant-digit value
// (the review's "wide 25-digit values" case) that also exercises a nonzero
// weight spanning several base-10000 digit groups.
func setupDecimalDB(t *testing.T) *Server {
	t.Helper()
	ctx := context.Background()
	db, srv := setupRealDB(t)

	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt32},
		{Name: "d", Type: parquet.TypeDecimal, Precision: 38, Scale: 10, Nullable: true},
	}}
	if err := db.CreateTable(ctx, "amounts", schema, nil); err != nil {
		t.Fatal(err)
	}

	const scale = 10
	rows := []struct {
		id int32
		d  string
	}{
		{1, "10.0000000000"},
		{2, "9346828825.8671214869"},
		{3, "-9346828825.8671214869"},
		{4, "25000.0000000000"},
		{5, "1234567890123456789012345.6789012345"}, // 25-digit integer part
	}
	ingestRows := make([]map[string]any, len(rows))
	for i, r := range rows {
		ingestRows[i] = map[string]any{
			"id": r.id,
			"d":  decimal128FromText(t, r.d, scale),
		}
	}
	ing := db.NewIngester("amounts", schema, nil, ingest.Config{MaxBufferRows: 10, RowGroupSize: 10})
	if err := ing.Ingest(ctx, ingestRows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return srv
}

func pgxConnStr(addr string) string {
	return fmt.Sprintf("host=127.0.0.1 port=%s user=wadjet dbname=wadjet sslmode=disable",
		addr[len("127.0.0.1:"):])
}

// TestIssue464BinaryNumericParamSelectsCorrectRows is the shape the review
// found: `d = $1` and `d > $1` bound through a real pgx v5 connection, which
// encodes the pgtype.Numeric argument as OID 1700 in BINARY format by
// default (NumericCodec.PreferredFormat). On the pre-fix tree every case
// here returns the wrong row set — equality returns none, and range
// comparisons return whatever the quoted-string fallback happened to coerce
// to — because the parameter never carried the value it was bound with.
func TestIssue464BinaryNumericParamSelectsCorrectRows(t *testing.T) {
	srv := setupDecimalDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	t.Run("equality on an ordinary wide value", func(t *testing.T) {
		var id int32
		err := conn.QueryRow(ctx, "SELECT id FROM amounts WHERE d = $1",
			pgNumericArg(t, "9346828825.8671214869")).Scan(&id)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 2 {
			t.Fatalf("id = %d, want 2", id)
		}
	})

	t.Run("equality on the negative of that value", func(t *testing.T) {
		var id int32
		err := conn.QueryRow(ctx, "SELECT id FROM amounts WHERE d = $1",
			pgNumericArg(t, "-9346828825.8671214869")).Scan(&id)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 3 {
			t.Fatalf("id = %d, want 3", id)
		}
	})

	// The review's "wide 25-digit values" case: past float64's exact range,
	// and wide enough to span multiple base-10000 digit groups in both the
	// integer part and (trivially) the fraction.
	t.Run("equality on a 25-digit integer part", func(t *testing.T) {
		var id int32
		err := conn.QueryRow(ctx, "SELECT id FROM amounts WHERE d = $1",
			pgNumericArg(t, "1234567890123456789012345.6789012345")).Scan(&id)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 5 {
			t.Fatalf("id = %d, want 5", id)
		}
	})

	t.Run("range comparison", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT id FROM amounts WHERE d > $1 ORDER BY id",
			pgNumericArg(t, "9999999999"))
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var ids []int32
		for rows.Next() {
			var id int32
			if err := rows.Scan(&id); err != nil {
				t.Fatalf("scan: %v", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		// Only the 25-digit value (id 5) exceeds ten billion; the other
		// wide value (id 2, ~9.3 billion) and everything smaller do not.
		if len(ids) != 1 || ids[0] != 5 {
			t.Fatalf("ids = %v, want [5]", ids)
		}
	})

	t.Run("no match returns no rows, not an error", func(t *testing.T) {
		rows, err := conn.Query(ctx, "SELECT id FROM amounts WHERE d = $1",
			pgNumericArg(t, "0.0000000001"))
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		n := 0
		for rows.Next() {
			n++
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("rows.Err: %v", err)
		}
		if n != 0 {
			t.Fatalf("got %d rows, want 0", n)
		}
	})
}

// TestIssue464TextFormatNumericParamStillWorks confirms the fix to the
// binary arm did not disturb the text arm: a numeric parameter sent as text
// (the shape pgx and lib/pq already used to take, and still do for a client
// that has not learned the column's binary codec) keeps working.
func TestIssue464TextFormatNumericParamStillWorks(t *testing.T) {
	srv := setupDecimalDB(t)
	ctx := context.Background()

	t.Run("pgx simple protocol (text)", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
		if err != nil {
			t.Fatalf("pgx connect: %v", err)
		}
		defer conn.Close(ctx)

		var id int32
		err = conn.QueryRow(ctx, "SELECT id FROM amounts WHERE d = $1", pgx.QueryExecModeSimpleProtocol,
			"9346828825.8671214869").Scan(&id)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 2 {
			t.Fatalf("id = %d, want 2", id)
		}
	})

	t.Run("lib/pq (text)", func(t *testing.T) {
		db := openPQ(t, srv.Addr())
		var id int32
		err := db.QueryRow("SELECT id FROM amounts WHERE d = $1", "25000.0000000000").Scan(&id)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		if id != 4 {
			t.Fatalf("id = %d, want 4", id)
		}
	})

	t.Run("ExecParams with an explicit text format code", func(t *testing.T) {
		conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
		if err != nil {
			t.Fatalf("pgx connect: %v", err)
		}
		defer conn.Close(ctx)

		res := conn.PgConn().ExecParams(ctx, "SELECT id FROM amounts WHERE d = $1",
			[][]byte{[]byte("10.0000000000")}, []uint32{oidNumeric}, []int16{0}, nil).Read()
		if res.Err != nil {
			t.Fatalf("ExecParams: %v", res.Err)
		}
		if len(res.Rows) != 1 || string(res.Rows[0][0]) != "1" {
			t.Fatalf("rows = %v, want one row with id 1", res.Rows)
		}
	})
}

// TestIssue464BinaryNumericNaNErrors covers the value PostgreSQL's binary
// numeric format can carry that wadjet's DECIMAL cannot: NaN. A pgx client
// that sends one must see an ErrorResponse naming the reason, never a query
// silently built from an unparseable value.
func TestIssue464BinaryNumericNaNErrors(t *testing.T) {
	srv := setupDecimalDB(t)
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgxConnStr(srv.Addr()))
	if err != nil {
		t.Fatalf("pgx connect: %v", err)
	}
	defer conn.Close(ctx)

	var id int32
	err = conn.QueryRow(ctx, "SELECT id FROM amounts WHERE d = $1",
		pgtype.Numeric{NaN: true, Valid: true}).Scan(&id)
	if err == nil {
		t.Fatalf("query with a NaN parameter succeeded with id %d, want an error", id)
	}
	if !strings.Contains(err.Error(), "NaN") {
		t.Errorf("error = %v, want it to mention NaN", err)
	}
}
