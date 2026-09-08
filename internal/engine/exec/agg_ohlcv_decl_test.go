package exec

import (
	"reflect"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/batch"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// THE STATE CARRIES ITS OWN DECLARED ROW (#965, ADR-0035).
//
// The planner cannot always supply one — it has no catalog column to walk for
// a COMPUTED argument, and `ohlcv(ts, price*2, volume)` is exactly that — and
// the coordinator's fold sees a string and no vectors at all. So the operator
// that COMPUTED the values writes their declared types beside them, in the
// encoded state's own header.
//
// Before that, two things went wrong and neither was a wrong number:
//
//   - a computed price ANSWERED in process and failed loud on the DAG, because
//     the fold took the planner's list and the planner had declined;
//   - a MERGE stage that derived a list of its own from its STRING input got a
//     float bar, and writing a DECIMAL bar's digits into FLOAT64 children
//     tripped the #361 silent-write guard.
//
// Both are gated end to end in coordinator.TestTheBarIsTheSameOnEveryArm; this
// is the unit that says why the header carries what it carries.
func TestTheBarsStateCarriesItsDeclaredRow(t *testing.T) {
	fields, ok := OhlcvOutputFields(
		parquet.Column{Name: "px", Type: parquet.TypeDecimal, Precision: 11, Scale: 2},
		parquet.Column{Name: "vol", Type: parquet.TypeInt64})
	if !ok {
		t.Fatal("no bar over a decimal price and an int8 volume")
	}
	s := &ohlcvState{
		dom:    ohlcvDomain{exact: true, priceScale: 2, volScale: 0, pvScale: 2},
		fields: fields,
	}
	for _, r := range ohlcvTestRows(11) {
		s.observeExact(r.ts, batch.Int128From(r.px), batch.Int128From(r.vol))
	}

	back, ok := decodeOhlcvState(s.encode())
	if !ok {
		t.Fatal("decode refused its own encoding")
	}
	if !reflect.DeepEqual(back.fields, fields) {
		t.Fatalf("the declared ROW did not survive the encoding:\n got  %v\n want %v",
			back.fields, fields)
	}

	// The fold's entry point needs NOTHING but the string.
	v, got, ok, err := FinalizeOhlcvState(s.encode())
	if err != nil || !ok {
		t.Fatalf("FinalizeOhlcvState: %v (ok=%v)", err, ok)
	}
	if !reflect.DeepEqual(got, fields) {
		t.Errorf("FinalizeOhlcvState returned fields %v, want %v", got, fields)
	}
	m, isRow := v.(map[string]any)
	if !isRow {
		t.Fatalf("the finished bar is %T, want a ROW box", v)
	}
	if _, isText := m["open"].(string); !isText {
		t.Errorf("open is %T (%v); a DECIMAL(11,2) field renders as its exact text, and a "+
			"float here is the declaration having been lost", m["open"], m["open"])
	}

	// A state with rows and NO declaration REFUSES rather than guessing: a
	// guessed field list builds a ROW whose children are the wrong types and
	// then writes the values into them.
	undeclared := *s
	undeclared.fields = nil
	if _, _, _, err := FinalizeOhlcvState(undeclared.encode()); err == nil {
		t.Error("a state with rows and no declared fields was finished anyway")
	}

	// An EMPTY one is NULL, declaration or not. A partial task whose filter
	// matched nothing never resolved its input columns, so it has nothing to
	// declare — and that identity row is legitimate, the shape #685 records
	// for a DECIMAL one type over.
	empty := &ohlcvState{}
	v, _, ok, err = FinalizeOhlcvState(empty.encode())
	if err != nil || ok || v != nil {
		t.Errorf("an empty state finished as %v (ok=%v, err=%v), want SQL NULL", v, ok, err)
	}
}
