package pgwire

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/coordinator"
	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// boxedRows wraps a name-keyed result the way the legacy db.Query path hands
// one to sendResultRows.
func boxedRows(columns []string, rows []map[string]any) *wadjet.QueryResult {
	return &wadjet.QueryResult{Columns: columns, Rows: rows}
}

// A result may legally carry two output columns of the SAME NAME: PostgreSQL
// answers `SELECT abs(a), abs(b)` with two columns called `abs`, and #513
// made this engine agree. The DataRow path used to look each cell up in a
// name-keyed map, so the second column was sent carrying the FIRST column's
// value — verified against postgres:17-alpine, which answers 100|200 where
// this engine answered 100|100.
//
// These tests drive the send path directly, which is where the loss happened;
// the end-to-end spelling is gated by the wire oracle's corpus.

func dupNameBatch(t *testing.T) *batch.RecordBatch {
	t.Helper()
	schema := []parquet.Column{
		{Name: "abs", Type: parquet.TypeInt64},
		{Name: "abs", Type: parquet.TypeInt64},
	}
	// Built column by column, because the batch's own name-keyed
	// constructor (FromRows) cannot express two columns of one name either.
	b := batch.NewRecordBatch(schema, 2)
	b.Columns[0].SetValue(0, int64(100))
	b.Columns[0].SetValue(1, int64(101))
	b.Columns[1].SetValue(0, int64(200))
	b.Columns[1].SetValue(1, int64(201))
	return b
}

// TestDataRowKeepsBothColumnsOfADuplicateName is the coord (batch) path.
func TestDataRowKeepsBothColumnsOfADuplicateName(t *testing.T) {
	b := dupNameBatch(t)
	rc := &recordConn{}
	c := &pgConn{conn: rc}
	sent, err := c.sendResultRows(context.Background(), []string{"abs", "abs"},
		coordinator.NewSliceStream([]*batch.RecordBatch{b}), nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	if sent != 2 {
		t.Fatalf("sent %d rows, want 2", sent)
	}
	assertDataRowCells(t, rc, [][]string{{"100", "200"}, {"101", "201"}})
}

// TestDataRowKeepsBothColumnsOfADuplicateNameBoxed is the legacy db.Query
// path, which reaches the wire as a boxed result rather than a batch.
func TestDataRowKeepsBothColumnsOfADuplicateNameBoxed(t *testing.T) {
	res := &wadjet.QueryResult{
		Columns: []string{"abs", "abs"},
		// What the map form can hold: the LAST column wins. RowValues is
		// what makes the answer representable.
		Rows: []map[string]any{
			{"abs": int64(200)},
			{"abs": int64(201)},
		},
		RowValues: [][]any{
			{int64(100), int64(200)},
			{int64(101), int64(201)},
		},
	}
	rc := &recordConn{}
	c := &pgConn{conn: rc}
	sent, err := c.sendResultRows(context.Background(), res.Columns, nil, res, nil, nil, nil)
	if err != nil {
		t.Fatalf("sendResultRows: %v", err)
	}
	if sent != 2 {
		t.Fatalf("sent %d rows, want 2", sent)
	}
	assertDataRowCells(t, rc, [][]string{{"100", "200"}, {"101", "201"}})
}

// TestBoxedResultCellsFallBackToTheMap: a result whose names are unique needs
// no positional form, and Cells must still answer positionally from the map.
func TestBoxedResultCellsFallBackToTheMap(t *testing.T) {
	res := &wadjet.QueryResult{
		Columns: []string{"a", "b"},
		Rows:    []map[string]any{{"a": int64(1), "b": int64(2)}},
	}
	got := res.Cells(0)
	if len(got) != 2 || got[0] != int64(1) || got[1] != int64(2) {
		t.Errorf("Cells(0) = %v, want [1 2]", got)
	}
	if res.Cells(1) != nil {
		t.Errorf("Cells past the end returned %v, want nil", res.Cells(1))
	}
}

// TestBoxedResultCellsRejectsOutOfRangeIndices: the range check has to come
// FIRST and cover both forms. A negative index passes `i < len(RowValues)`,
// so testing that before the sign made Cells(-1) index the slice with it.
func TestBoxedResultCellsRejectsOutOfRangeIndices(t *testing.T) {
	for _, tc := range []struct {
		name string
		res  *wadjet.QueryResult
	}{
		{"positional form present", &wadjet.QueryResult{
			Columns:   []string{"abs", "abs"},
			Rows:      []map[string]any{{"abs": int64(2)}},
			RowValues: [][]any{{int64(1), int64(2)}},
		}},
		{"map only", &wadjet.QueryResult{
			Columns: []string{"a", "b"},
			Rows:    []map[string]any{{"a": int64(1), "b": int64(2)}},
		}},
		{"empty result", &wadjet.QueryResult{Columns: []string{"a"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, i := range []int{-1, -2, 1 << 20} {
				if got := tc.res.Cells(i); got != nil {
					t.Errorf("Cells(%d) = %v, want nil", i, got)
				}
			}
		})
	}
	var nilRes *wadjet.QueryResult
	if got := nilRes.Cells(-1); got != nil {
		t.Errorf("Cells(-1) on a nil result = %v, want nil", got)
	}
}

// assertDataRowCells decodes the recorded DataRow messages and compares every
// cell's TEXT form. Decoding rather than substring-matching is the point: a
// value sent under the wrong column is still present in the byte stream.
func assertDataRowCells(t *testing.T, rc *recordConn, want [][]string) {
	t.Helper()
	got := decodeDataRows(t, rc.buf.Bytes())
	if len(got) != len(want) {
		t.Fatalf("decoded %d DataRows, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if len(got[i]) != len(want[i]) {
			t.Fatalf("row %d has %d cells, want %d: %v", i, len(got[i]), len(want[i]), got[i])
		}
		for j := range want[i] {
			if got[i][j] != want[i][j] {
				t.Errorf("row %d cell %d = %q, want %q (whole row %v) — a duplicate column "+
					"name must not make one column carry another's value",
					i, j, got[i][j], want[i][j], got[i])
			}
		}
	}
}

// decodeDataRows pulls the cells out of every 'D' message in a wire stream.
func decodeDataRows(t *testing.T, buf []byte) [][]string {
	t.Helper()
	var out [][]string
	for len(buf) >= 5 {
		typ := buf[0]
		n := int(uint32(buf[1])<<24 | uint32(buf[2])<<16 | uint32(buf[3])<<8 | uint32(buf[4]))
		if n < 4 || len(buf) < 1+n {
			t.Fatalf("truncated message %q: length %d, %d bytes left", typ, n, len(buf))
		}
		body := buf[5 : 1+n]
		buf = buf[1+n:]
		if typ != 'D' {
			continue
		}
		if len(body) < 2 {
			t.Fatalf("DataRow body too short")
		}
		count := int(uint16(body[0])<<8 | uint16(body[1]))
		body = body[2:]
		cells := make([]string, 0, count)
		for k := 0; k < count; k++ {
			if len(body) < 4 {
				t.Fatalf("DataRow cell %d truncated", k)
			}
			l := int(int32(uint32(body[0])<<24 | uint32(body[1])<<16 | uint32(body[2])<<8 | uint32(body[3])))
			body = body[4:]
			if l < 0 {
				cells = append(cells, "<NULL>")
				continue
			}
			if len(body) < l {
				t.Fatalf("DataRow cell %d claims %d bytes, %d left", k, l, len(body))
			}
			cells = append(cells, strings.Clone(string(body[:l])))
			body = body[l:]
		}
		out = append(out, cells)
	}
	return out
}
