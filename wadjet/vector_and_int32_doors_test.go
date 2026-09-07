package wadjet

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// s4VectorTable opens a database with one VECTOR(dim) table and nothing in it.
func s4VectorTable(t *testing.T, table string, dim int) *DB {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "s4"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(db.Close)
	schema := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeVector, Dimension: dim, Nullable: true},
	}}
	if err := db.CreateTable(ctx, table, schema, nil); err != nil {
		t.Fatal(err)
	}
	return db
}

// A VECTOR(N) value has exactly N components AT EVERY DOOR, and a value of any
// other width is refused rather than written.
//
// docs/data-types.md said so from the moment the in-memory rule landed, and no
// SQL door kept the promise: convertValue had no VECTOR case at all, so the
// literal's raw TEXT reached the parquet writer and went into a
// FIXED_LEN_BYTE_ARRAY(N*4) leaf verbatim. `INSERT INTO t (v) VALUES ('[1]')`
// answered "INSERT 1" and left a page whose body is 3 bytes where its header
// promises 8 — the table unqueryable from then on — and `'[1,2]'`, the RIGHT
// width, did exactly the same with 5 bytes. Round-2 review B2.
func TestAVectorLiteralIsExactlyTheDeclaredWidthAtEveryDoor(t *testing.T) {
	ctx := context.Background()

	// The accepted case first, because it never worked either: a correct-width
	// literal round trips.
	t.Run("insert-correct-width-round-trips", func(t *testing.T) {
		db := s4VectorTable(t, "vok", 2)
		if _, err := db.Execute(ctx, `INSERT INTO vok (id, v) VALUES (1, '[1,2]')`); err != nil {
			t.Fatalf("a VECTOR(2) literal of the right width was refused: %v", err)
		}
		r, err := db.Query(ctx, `SELECT v FROM vok`)
		if err != nil {
			t.Fatalf("reading back a vector this door wrote: %v", err)
		}
		if got := fmt.Sprint(r.Rows); got != "[map[v:[1 2]]]" {
			t.Errorf("read back %s; want [map[v:[1 2]]]", got)
		}
	})

	refusals := []struct {
		name  string
		lit   string
		state string
		frag  string
	}{
		{"short", `'[1]'`, "22000", "expected 2 dimensions, not 1"},
		{"empty", `'[]'`, "22000", "expected 2 dimensions, not 0"},
		{"long", `'[1,2,3]'`, "22000", "expected 2 dimensions, not 3"},
		{"no-brackets", `'1,2'`, "22P02", "invalid input syntax for type vector"},
		{"not-a-number", `'[1,x]'`, "22P02", "invalid input syntax for type vector"},
		{"nan", `'[1,NaN]'`, "22P02", "invalid input syntax for type vector"},
	}

	for _, door := range []string{"INSERT", "UPDATE", "MERGE"} {
		for _, c := range refusals {
			t.Run(door+"/"+c.name, func(t *testing.T) {
				db := s4VectorTable(t, "vd", 2)
				if _, err := db.Execute(ctx, `INSERT INTO vd (id, v) VALUES (1, '[9,9]')`); err != nil {
					t.Fatal(err)
				}
				var sql string
				switch door {
				case "INSERT":
					sql = fmt.Sprintf(`INSERT INTO vd (id, v) VALUES (2, %s)`, c.lit)
				case "UPDATE":
					sql = fmt.Sprintf(`UPDATE vd SET v = %s WHERE id = 1`, c.lit)
				case "MERGE":
					sql = fmt.Sprintf(`MERGE INTO vd USING (SELECT 1 AS k) s ON vd.id = s.k `+
						`WHEN MATCHED THEN UPDATE SET v = %s`, c.lit)
				}
				_, err := db.Execute(ctx, sql)
				if err == nil {
					t.Fatalf("%s accepted %s into a VECTOR(2)", door, c.lit)
				}
				if !strings.Contains(err.Error(), c.frag) {
					t.Errorf("refusal %q does not say %q", err, c.frag)
				}
				if got := sqlerr.StateOf(err); got != c.state {
					t.Errorf("refusal SQLSTATE %q; want %q", got, c.state)
				}
				// The table is still readable and still holds what it held:
				// the refusal came BEFORE anything was written.
				r, qerr := db.Query(ctx, `SELECT id, v FROM vd ORDER BY id`)
				if qerr != nil {
					t.Fatalf("the table is unreadable after a refused %s: %v", door, qerr)
				}
				if got := fmt.Sprint(r.Rows); got != "[map[id:1 v:[9 9]]]" {
					t.Errorf("after a refused %s the table holds %s; want the untouched row", door, got)
				}
			})
		}
	}

	// The ingest API is the other door onto the same leaf, and it takes Go
	// boxes rather than literal text.
	//
	// Its rule lives in parquet.CheckLeafBox (reached from ingest.checkType),
	// and it answered a DIFFERENT class and a different sentence for the same
	// value: 22023, `column "v" is VECTOR(2); the value has 3 components`,
	// where every SQL door says 22000, `expected 2 dimensions, not 3`. One
	// value, one rule, two answers decided by which door it arrived at —
	// docs/data-types.md recorded the split and said it was expected to
	// converge. It has (#913), and this arm asserts the CLASS and the WORDING
	// rather than error-or-not, which is the assertion that cannot see a
	// divergence.
	t.Run("ingest-api", func(t *testing.T) {
		for _, c := range []struct {
			name string
			val  any
			frag string // "" = must be accepted
		}{
			{"correct", []float32{1, 2}, ""},
			{"short", []float32{1}, "expected 2 dimensions, not 1"},
			{"long", []float32{1, 2, 3}, "expected 2 dimensions, not 3"},
			{"empty", []float32{}, "expected 2 dimensions, not 0"},
			{"nil-typed", []float32(nil), "expected 2 dimensions, not 0"},
		} {
			t.Run(c.name, func(t *testing.T) {
				err := s4IngestVector(t, "vi"+c.name, c.val)
				if c.frag == "" {
					if err != nil {
						t.Fatalf("a %s vector was refused: %v", c.name, err)
					}
					return
				}
				if err == nil {
					t.Fatalf("the ingest API admitted a %s vector into a VECTOR(2)", c.name)
				}
				if got := sqlerr.StateOf(err); got != "22000" {
					t.Errorf("SQLSTATE %q; want 22000, the class every SQL door gives "+
						"for the same value (%v)", got, err)
				}
				if !strings.Contains(err.Error(), c.frag) {
					t.Errorf("refusal %q does not say %q, which is what every SQL door "+
						"says for the same value", err, c.frag)
				}
				// The column name still localizes it: the ingest door takes a
				// whole ROW, and which column was wrong is what pgvector's own
				// message does not carry.
				if !strings.Contains(err.Error(), `"v"`) {
					t.Errorf("refusal %q does not name the column", err)
				}
			})
		}
	})
}

// A number with no int32 is refused at the DOOR, not wrapped into a plausible
// value.
//
// The arc's own record said "no SQL door reaches this seam today", and that was
// measured on a STRING literal (`'2147483648'::date`). The reachable shape is
// an INTEGER literal cast: at de5bc970 `SELECT 3000000000::DATE` answered
// -3543531-12-19, and -2147483649::DATE and 2147483647::DATE answered the SAME
// date. Round-2 review B1.
//
// PostgreSQL has no int-to-date cast at all (42846, "cannot cast type bigint to
// date"), so wadjet's cast is a deliberate superset (ADR-0012 item 5); inside a
// superset the rule is that a value it cannot represent is LOUD with the class
// PostgreSQL uses for the same magnitude reaching an int4, and never a
// different number. The four-arm version of this is arc S1's
// coordinator.TestAnInt32DomainRefusalHoldsOnEveryArm; this is the door and
// the SQLSTATE.
func TestAnOutOfRangeCastRefusesAtTheDoor(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "s4"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, c := range []struct {
		sql  string
		want string // "" = must answer
	}{
		{`SELECT 2147483647::DATE`, ""},
		{`SELECT (-2147483648)::DATE`, ""},
		{`SELECT 2147483648::DATE`, "22003"},
		{`SELECT (-2147483649)::DATE`, "22003"},
		{`SELECT 3000000000::DATE`, "22003"},
		{`SELECT 1000000000000::DATE`, "22003"},
		{`SELECT 1000000000000000::DATE`, "22003"},
		// #911's six, moved up from the PIN below them. They escaped the guard
		// because `parseDateValue` reads a bare number as
		// `time.Date(1970,1,1).AddDate(0, 0, n)` and time.Date multiplies the
		// day count by 86400 in an unmodulated uint64: 2^63−1 days came back as
		// epoch minus one, so what reached the store was the int64 -1 and the
		// store had nothing left to reject. The last two are not int64 values
		// at all — they parse as float64 — and Go's float-to-int conversion is
		// implementation-defined for them.
		{`SELECT 9223372036854775807::DATE`, "22003"},
		{`SELECT 9223372036854775806::DATE`, "22003"},
		{`SELECT (-9223372036854775808)::DATE`, "22003"},
		{`SELECT 4611686018427387904::DATE`, "22003"},
		{`SELECT 9223372036854775808::DATE`, "22003"},
		{`SELECT (-9223372036854775809)::DATE`, "22003"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			r, err := db.Query(ctx, c.sql)
			if c.want == "" {
				if err != nil {
					t.Fatalf("an in-range cast was refused: %v", err)
				}
				if len(r.Rows) != 1 {
					t.Errorf("got %d rows; want 1", len(r.Rows))
				}
				return
			}
			if err == nil {
				t.Fatalf("answered %v where the value has no int32", r.Rows)
			}
			if got := sqlerr.StateOf(err); got != c.want {
				t.Errorf("SQLSTATE %q; want %q (%v)", got, c.want, err)
			}
		})
	}

	// The DATE cast still ANSWERS across the whole int32 day range, which is
	// what says the refusals above are about the VALUE and not about the cast
	// having been withdrawn. Both ends of the carrier and a fractional
	// operand, whose truncation toward zero is the reading parseDateValue's
	// numeric arms already had.
	for _, c := range []struct{ sql, answers string }{
		{`SELECT 0::DATE`, "1970-01-01"},
		{`SELECT (-1)::DATE`, "1969-12-31"},
		{`SELECT 1.9::DATE`, "1970-01-02"},
	} {
		t.Run("answers/"+c.sql, func(t *testing.T) {
			r, err := db.Query(ctx, c.sql)
			if err != nil {
				t.Fatalf("a day count an int32 holds was refused: %v", err)
			}
			if got := fmt.Sprint(r.Rows); !strings.Contains(got, c.answers) {
				t.Errorf("answered %s; want %s", got, c.answers)
			}
		})
	}

	// The two CodeQL siblings, and INT32 and FLOAT32 beside them (#901).
	//
	// This block used to RECORD that `3000000000::PORT` answers the widened
	// number "because the cast never declares the result PORT". It reached no
	// guard at all: `int32`, `float32`, `port` and `protocol` matched no label
	// in Cast.Eval's switch and fell to `default: return v`, while
	// inferCastType declared STRING for the same four names — a number
	// published as TEXT under OID 25, which is the #310/#443 shape and the one
	// #652 closed for names that answer to nothing at all.
	//
	// PostgreSQL raises `integer out of range` for `3000000000::int4`, and the
	// PORT/PROTOCOL bound is the engine's own, stated in docs/data-types.md
	// before this fix existed. Both sides of the boundary are here, because a
	// table of refusals alone cannot say whether the cast still WORKS.
	for _, c := range []struct {
		sql   string
		want  string // "" = must answer
		value any    // the answer, when it answers
		decl  parquet.TypeID
	}{
		{`SELECT 3000000000::INT32 AS v`, "22003", nil, 0},
		{`SELECT 2147483648::INT32 AS v`, "22003", nil, 0},
		{`SELECT (-2147483649)::INT32 AS v`, "22003", nil, 0},
		{`SELECT 9223372036854775807::INT32 AS v`, "22003", nil, 0},
		{`SELECT 2147483647::INT32 AS v`, "", int64(2147483647), parquet.TypeInt64},
		{`SELECT (-2147483648)::INT32 AS v`, "", int64(-2147483648), parquet.TypeInt64},
		{`SELECT 3000000000::PORT AS v`, "22003", nil, 0},
		{`SELECT 9223372036854775807::PORT AS v`, "22003", nil, 0},
		{`SELECT 443::PORT AS v`, "", int32(443), parquet.TypePort},
		{`SELECT 3000000000::PROTOCOL AS v`, "22003", nil, 0},
		{`SELECT 6::PROTOCOL AS v`, "", int32(6), parquet.TypeProtocol},
		{`SELECT CAST(1e40 AS FLOAT32) AS v`, "22003", nil, 0},
		{`SELECT CAST(1.5 AS FLOAT32) AS v`, "", float32(1.5), parquet.TypeFloat32},
	} {
		t.Run(c.sql, func(t *testing.T) {
			r, err := db.Query(ctx, c.sql)
			if c.want != "" {
				if err == nil {
					t.Fatalf("answered %v where the value has no place in the destination", r.Rows)
				}
				if got := sqlerr.StateOf(err); got != c.want {
					t.Errorf("SQLSTATE %q; want %q (%v)", got, c.want, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("an in-range cast was refused: %v", err)
			}
			if len(r.Rows) != 1 || r.Rows[0]["v"] != c.value {
				t.Errorf("= %v, want %#v", r.Rows, c.value)
			}
			// The DECLARATION beside the value: a number under OID 25 is what
			// a driver reads as a string, and it is the half a value-only
			// assertion cannot see.
			if len(r.ColumnMetas) != 1 || r.ColumnMetas[0].TypeID != c.decl {
				t.Errorf("declares %v, want %v", r.ColumnMetas, c.decl)
			}
		})
	}
}

// A set operation over two VECTOR columns of different declared widths.
//
// At de5bc970 `v2 UNION ALL v3` SILENTLY TRUNCATED the wider arm's [1,2,3] to
// [1,2] and answered. It refuses now, because the output column is declared
// with the first arm's width and a value of another width has nowhere to go.
//
// INTERSECT and EXCEPT answer, and that is not an inconsistency: they emit
// values from the LEFT arm only, so no value is ever materialized at a width it
// does not have. UNION is the divergence in intent: PostgreSQL's vector is one
// type with a width typmod, so its union drops the typmod, and wadjet has no
// mixed-width VECTOR carrier to return that in.
//
// What PostgreSQL answers for any of these is NOT measured and is not claimed:
// the shared oracle server carries no vector extension, and pgvector's
// comparison operators are documented to RAISE on differing dimensions rather
// than compare unequal — which would make it error where this engine answers.
// The cells below assert THIS engine's behaviour only. Round-2 review P1,
// round-3 review P4; ADR-0012 item 5 carries the same caveat.
func TestASetOperationOverTwoVectorWidths(t *testing.T) {
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "s4"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for name, dim := range map[string]int{"sv2": 2, "sv3": 3} {
		sc := parquet.Schema{Columns: []parquet.Column{
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "v", Type: parquet.TypeVector, Dimension: dim, Nullable: true},
		}}
		if err := db.CreateTable(ctx, name, sc, nil); err != nil {
			t.Fatal(err)
		}
		val := []float32{1, 2}
		if dim == 3 {
			val = []float32{1, 2, 3}
		}
		ing := db.NewIngester(name, sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
		if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "v": val}}); err != nil {
			t.Fatal(err)
		}
		if err := ing.FlushAll(ctx); err != nil {
			t.Fatal(err)
		}
	}

	for _, c := range []struct {
		sql  string
		want string // a SQLSTATE, or the rows it must answer
	}{
		{`SELECT v FROM sv2 UNION ALL SELECT v FROM sv3`, "22000"},
		{`SELECT v FROM sv3 UNION ALL SELECT v FROM sv2`, "22000"},
		{`SELECT v FROM sv2 UNION SELECT v FROM sv3`, "22000"},
		{`SELECT v FROM sv3 UNION SELECT v FROM sv2`, "22000"},
		{`SELECT v FROM sv2 EXCEPT SELECT v FROM sv3`, "[map[v:[1 2]]]"},
		{`SELECT v FROM sv3 EXCEPT SELECT v FROM sv2`, "[map[v:[1 2 3]]]"},
		{`SELECT v FROM sv2 INTERSECT SELECT v FROM sv3`, "[]"},
		{`SELECT v FROM sv3 INTERSECT SELECT v FROM sv2`, "[]"},
	} {
		t.Run(c.sql, func(t *testing.T) {
			r, err := db.Query(ctx, c.sql)
			if strings.HasPrefix(c.want, "2") {
				if err == nil {
					t.Fatalf("answered %v where the two arms' widths differ", r.Rows)
				}
				if got := sqlerr.StateOf(err); got != c.want {
					t.Errorf("SQLSTATE %q; want %q (%v)", got, c.want, err)
				}
				if !strings.Contains(err.Error(), "dimensions") {
					t.Errorf("refusal %q does not name the dimension", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused a set operation that emits only left-arm values: %v", err)
			}
			if got := fmt.Sprint(r.Rows); got != c.want {
				t.Errorf("answered %s; want %s", got, c.want)
			}
		})
	}
}

// s4IngestVector ingests one row carrying v into a fresh VECTOR(2) table and
// returns whatever the door said, flush included — a width the door admits but
// the writer refuses is still a refusal, and it is the FLUSH that reports it.
func s4IngestVector(t *testing.T, table string, v any) error {
	t.Helper()
	ctx := context.Background()
	db := s4VectorTable(t, table, 2)
	sc := parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "v", Type: parquet.TypeVector, Dimension: 2, Nullable: true},
	}}
	ing := db.NewIngester(table, sc, nil, ingest.Config{MaxBufferRows: 8, RowGroupSize: 4})
	if err := ing.Ingest(ctx, []map[string]any{{"id": int64(1), "v": v}}); err != nil {
		return err
	}
	if err := ing.FlushAll(ctx); err != nil {
		return err
	}
	// Admitted and written. A table that cannot be READ back is the worst of
	// the three outcomes and is reported as a failure of the write, not as a
	// success.
	if _, err := db.Query(ctx, "SELECT v FROM "+table); err != nil {
		return fmt.Errorf("written but unreadable: %w", err)
	}
	return nil
}
