package shapegen

import (
	"fmt"

	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TypeMatrix is the generation universe over the type-matrix fixture
// (internal/oracle/typematrix): the same two tables that package's corpus uses,
// described so the generator can put EVERY wadjet type through the shapes it
// knows — projection, GROUP BY, ORDER BY, DISTINCT, joins, set operations,
// aggregates, window functions, predicates.
//
// TPCH() spans three storage types, so nineteen of the engine's twenty-two were
// unreachable by this generator: a defect that needs a BYTES, IPv4, UUID or
// DECIMAL column to fire could not be generated even in principle. This schema
// closes that, and it is DERIVED from typematrix.Columns() rather than written
// out, so a type added there is generated here without a second edit.
//
// The four container types (ARRAY, ROW, MAP, VECTOR) are deliberately NOT
// exposed as generatable columns: the generator would order and group by them,
// which is not defined, and the engine's answer to that is a separate question
// from the one this generator asks. The nested table is present for its id and
// group key, so joins across the two still happen.
func TypeMatrix() *Schema {
	flat := Table{Name: typematrix.Table, PK: []string{"id"}, Cols: []Column{
		{Name: "id", Kind: KindInt, Lits: []string{"7", "1500", "4999"}},
		{Name: "g", Kind: KindInt, Lits: []string{"0", "3", "6"}},
		{Name: "m1", Kind: KindFloat, Lits: []string{"10.5", "50.5", "90.5"}},
		{Name: "m2", Kind: KindFloat, Lits: []string{"-3.25", "40.75", "80.75"}},
	}}
	for _, c := range typematrix.Columns() {
		if !c.Flat {
			continue
		}
		flat.Cols = append(flat.Cols, Column{
			Name: c.Name, Kind: kindOf(c.Type), Lits: typeMatrixLits(c.Name),
			Bool: c.Type == parquet.TypeBool,
		})
	}
	// typematrix.Nested is deliberately ABSENT. It would contribute only an id
	// and a group key here — its four container columns are not generatable —
	// but the generator emits star projections, and `t.*` over that table
	// expands to its MAP column, which KILLS THE PROCESS: the columnar decoder
	// declines MAP, the scan falls back to the row reader, and SetValue rejects
	// the map[string]any it is handed, on the scan-worker goroutine where no
	// recover reaches it. A generator that takes the process down reports
	// nothing about the seeds after the one that did it.
	//
	// Restoring the table (with its Edges to typemx on id and g, which is what
	// makes a cross-table join generatable here) is part of fixing that defect:
	// wadjet.TestTypeMatrixNoProcessKillers fails when its pins stop crashing,
	// and those pins say so.
	//
	// ROW FIELD PATHS (#568) are absent for a second, independent reason, and
	// restoring the table alone does not bring them: this generator QUALIFIES
	// a column reference with its table alias whenever a table appears twice,
	// and often when it does not (Gen.name), while the parser accepts only a
	// TWO-part reference — `rw.f` parses, `t.rw.f` is "trailing input after
	// the end of the statement". A generated field path would therefore be
	// unparseable SQL on most draws, which reports nothing about the engine.
	// Generating them needs either three-part path support in the parser or a
	// per-column "never qualify" flag honoured by Gen.name and nameOf; until
	// then the field-path shapes are covered by the fixed corpus
	// (typematrix.Corpus's rowfield_* entries) and by
	// wadjet.TestRowFieldPathCarriesTheFieldsDeclaredType.
	return &Schema{Tables: []Table{flat}}
}

// kindOf maps a storage type to the generator's kind. The default is
// KindOpaque, which is the safe answer for a type whose domain the generator
// does not model: it still gets projected, grouped, ordered, joined and
// aggregated, just never arithmetic'd or string-functioned.
func kindOf(t parquet.TypeID) Kind {
	switch t {
	case parquet.TypeInt32, parquet.TypeInt64:
		return KindInt
	case parquet.TypeFloat32, parquet.TypeFloat64:
		return KindFloat
	case parquet.TypeString:
		return KindText
	case parquet.TypeDate:
		return KindDate
	case parquet.TypeDecimal:
		return KindDecimal
	default:
		return KindOpaque
	}
}

// typeMatrixLits draws literals from the fixture's real domain, so a predicate
// on the column is selective without being always-empty. Values here MUST stay
// in step with typematrix.Data — the row indices are named in each entry so a
// change there is checkable against this.
func typeMatrixLits(col string) []string {
	switch col {
	case "c_bool":
		return []string{"true", "false"}
	case "c_i32": // 3*i at i = 5, 1500, 4000
		return []string{"15", "4500", "12000"}
	case "c_i64": // i*1000003
		return []string{"5000015", "1500004500", "4000012000"}
	case "c_f32": // i/7
		return []string{"0.7142857", "214.28572"}
	case "c_f64": // i/3
		return []string{"1.6666666666666667", "500.0"}
	case "c_str":
		return []string{"'s-000005'", "'s-001500'", "'s-004000'"}
	case "c_bytes":
		return []string{"'bytes-000005-xxxxx'", "'bytes-001500-'", "'bytes-004000-xxxx'"}
	case "c_ts": // 1700000000000 + i*61000
		return []string{"1700000305000", "1700091500000"}
	case "c_ipv4":
		return []string{"'10.0.0.5'", "'10.0.5.220'", "'10.0.15.160'"}
	case "c_ipv6":
		return []string{"'2001:db8::5'", "'2001:db8::5dc'"}
	case "c_cidr":
		return []string{"'192.168.5.0/24'", "'192.168.220.0/24'"}
	case "c_mac":
		return []string{"'aa:bb:cc:00:00:05'", "'aa:bb:cc:00:05:dc'"}
	case "c_port": // 1024 + i%40000
		return []string{"1029", "2524", "5024"}
	case "c_proto": // i%256
		return []string{"5", "220", "160"}
	case "c_dur": // i*1000000
		return []string{"5000000", "1500000000"}
	case "c_uuid":
		return []string{fmt.Sprintf("'00000000-0000-4000-8000-%012x'", 5),
			fmt.Sprintf("'00000000-0000-4000-8000-%012x'", 1500)}
	case "c_date":
		return []string{"'2015-06-06'", "'2010-01-01'"}
	case "c_dec": // i + 0.0001*(i%9973)
		return []string{"5.0005", "1500.15"}
	}
	return nil
}
