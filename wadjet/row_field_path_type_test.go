package wadjet

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The #568 gate: a ROW FIELD PATH answers exactly what the same value in a
// FLAT COLUMN answers — same values, same Go types, same declared type, in
// every consumer.
//
// The defect was a declaration, not a kernel. `rw.c` was typed STRING all the
// way down, whatever the field really was: physical.colRefDeclaredType looked
// the field name up among the INPUT's own columns, where `c` is not one, so
// the projection kept its STRING default and nothing downstream could correct
// it (exec.Project's schema pass resolves a field path to no column index at
// all). `SELECT rw.n` over an INT64 field returned string("9"), `ORDER BY
// rw.n` sorted it as text — "10" and "192" above "9" — pgwire declared OID 25
// for it, and GROUP BY / MIN / MAX could not run at all, because
// exec.HashAggregate resolves its inputs by NAME through columnIndexFallback,
// which has no ROW arm.
//
// This replaces wadjet.TestRowFieldPathLosesTheFieldsDeclaredType, the pin
// that asserted the wrong answers. The comparison is against a SIBLING FLAT
// COLUMN rather than against literal expectations on purpose: it is the
// self-disagreement the issue was filed on ("ORDER BY rw is correct, ORDER BY
// rw.c is not"), it needs no expectation to be re-derived when a type's own
// order changes — a CIDR field follows whatever the CIDR column follows — and
// it makes every one of the 18 flat types a case for free.
func rowFieldTypes() []parquet.Column {
	return []parquet.Column{
		{Name: "f_bool", Type: parquet.TypeBool, Nullable: true},
		{Name: "f_i32", Type: parquet.TypeInt32, Nullable: true},
		{Name: "f_i64", Type: parquet.TypeInt64, Nullable: true},
		{Name: "f_f32", Type: parquet.TypeFloat32, Nullable: true},
		{Name: "f_f64", Type: parquet.TypeFloat64, Nullable: true},
		{Name: "f_str", Type: parquet.TypeString, Nullable: true},
		{Name: "f_bytes", Type: parquet.TypeBytes, Nullable: true},
		{Name: "f_ts", Type: parquet.TypeTimestamp, Nullable: true},
		{Name: "f_date", Type: parquet.TypeDate, Nullable: true},
		{Name: "f_ipv4", Type: parquet.TypeIPv4, Nullable: true},
		{Name: "f_ipv6", Type: parquet.TypeIPv6, Nullable: true},
		{Name: "f_cidr", Type: parquet.TypeCIDR, Nullable: true},
		{Name: "f_mac", Type: parquet.TypeMAC, Nullable: true},
		{Name: "f_port", Type: parquet.TypePort, Nullable: true},
		{Name: "f_proto", Type: parquet.TypeProtocol, Nullable: true},
		{Name: "f_dur", Type: parquet.TypeDuration, Nullable: true},
		{Name: "f_uuid", Type: parquet.TypeUUID, Nullable: true},
		{Name: "f_dec", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
		// A field that is itself a container: the shape a bare TypeID cannot
		// carry, and the one that came back as the Go rendering of a map.
		{Name: "f_row", Type: parquet.TypeRow, Nullable: true, Fields: []parquet.Column{
			{Name: "k", Type: parquet.TypeInt32, Nullable: true},
		}},
		{Name: "f_arr", Type: parquet.TypeArray, Nullable: true,
			ElementType: &parquet.Column{Name: "element", Type: parquet.TypeString, Nullable: true}},
	}
}

// flatName is the flat mirror of a field. The prefix is load-bearing: a flat
// column that SHARES the field's name would SHADOW it. ColRef.resolveSlow
// resolves `rw.f_i64` by trying the whole dotted spelling, then the BARE
// name, and only then the ROW field — so a same-named flat column answers
// every query and the field path is never taken. An earlier draft of this
// gate did exactly that and passed against the unfixed engine.
func flatName(field string) string { return "x" + field }

// rowFieldSchema pairs every field of the ROW column `rw` with a flat column
// of the same TYPE under a distinct name. The two carry identical values, so
// any question asked of both must come back with the same answer.
func rowFieldSchema() parquet.Schema {
	cols := []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "rw", Type: parquet.TypeRow, Nullable: true, Fields: rowFieldTypes()},
	}
	for _, f := range rowFieldTypes() {
		flat := f
		flat.Name = flatName(f.Name)
		cols = append(cols, flat)
	}
	return parquet.Schema{Columns: cols}
}

// rowFieldRows builds rows whose VALUE order and TEXT order disagree for
// every ordered type — 9 sorts after 10 and 192 as text — which is what makes
// a field path typed STRING visible as a wrong ORDER BY rather than as a
// cosmetic difference. Two rows repeat a value so GROUP BY has a group of
// more than one, and every field is NULL somewhere: on its own stride inside
// a present ROW (i%5), and for the whole ROW at once (i%7 == 6).
func rowFieldRows() []map[string]any {
	nums := []int{9, 10, 192, 9, 3, 255, 64, 10, 1, 128, 77, 200}
	rows := make([]map[string]any, len(nums))
	for i, v := range nums {
		fields := map[string]any{
			"f_bool":  v%2 == 0,
			"f_i32":   int32(v),
			"f_i64":   int64(v),
			"f_f32":   float32(v) + 0.5,
			"f_f64":   float64(v) + 0.25,
			"f_str":   fmt.Sprintf("s-%03d", v),
			"f_bytes": []byte(fmt.Sprintf("b-%03d", v)),
			"f_ts":    int64(1_700_000_000_000 + int64(v)*61_000),
			"f_date":  fmt.Sprintf("20%02d-%02d-%02d", 10+v%15, 1+v%12, 1+v%28),
			"f_ipv4":  fmt.Sprintf("%d.0.0.1", v),
			"f_ipv6":  fmt.Sprintf("2001:db8::%x", v),
			"f_cidr":  fmt.Sprintf("%d.0.0.0/8", v),
			"f_mac":   fmt.Sprintf("aa:bb:cc:dd:ee:%02x", v),
			"f_port":  int32(v),
			"f_proto": int32(v % 256),
			"f_dur":   int64(v) * 1_000_000,
			"f_uuid":  fmt.Sprintf("00000000-0000-4000-8000-%012x", v),
			"f_dec":   fmt.Sprintf("%d.2500", v),
			"f_row":   map[string]any{"k": int32(v)},
			"f_arr":   []any{fmt.Sprintf("e-%03d", v)},
		}
		// One field NULL per row, cycling, so a null FIELD inside a PRESENT
		// ROW is covered for every type — a shape a flat column does not have.
		names := rowFieldTypes()
		fields[names[i%len(names)].Name] = nil

		r := map[string]any{"id": int64(i)}
		for k, val := range fields {
			r[flatName(k)] = val
		}
		if i%7 == 6 {
			// The whole ROW is NULL while the flat columns keep their values,
			// so the two sides deliberately DISAGREE on these rows. Every
			// query below excludes them.
			r["rw"] = nil
		} else {
			r["rw"] = fields
		}
		rows[i] = r
	}
	return rows
}

const rowFieldPresent = "rw IS NOT NULL"

// rowFieldContainerLoss names the field types whose value does not survive
// being stored INSIDE a ROW at all, so the comparison against the flat column
// cannot be gated for them.
//
// It is not a field-path defect and not #568: reading the WHOLE ROW loses
// them the same way — `SELECT rw` hands back "" for the IPv6 and the UUID
// while the identical value in a flat column reads back correctly — so the
// field path is faithfully reporting what the container holds. Verified on
// origin/main before any of #568's changes, which is what rules out a
// regression here.
//
// Ratcheted, not skipped: rowFieldContainerLossStillLoses asserts the loss is
// still happening, so the day the container starts carrying these the pin
// fails and every shape below is gated for them too.
var rowFieldContainerLoss = map[string]string{
	"f_ipv6": "an IPv6 stored in a ROW field reads back as the empty string, whole-ROW read included",
	"f_uuid": "a UUID stored in a ROW field reads back as the empty string, whole-ROW read included",
}

// TestRowFieldContainerLossIsStillReal is the ratchet on rowFieldContainerLoss.
// A pinned type that starts round-tripping through a ROW fails here, which is
// the signal to delete its entry and gate it alongside the rest.
func TestRowFieldContainerLossIsStillReal(t *testing.T) {
	db, ctx := rowFieldOpen(t)
	res, err := db.Query(ctx, `SELECT rw FROM rowfld WHERE id = 0`)
	if err != nil {
		t.Fatal(err)
	}
	row, ok := res.Rows[0]["rw"].(map[string]any)
	if !ok {
		t.Fatalf("rw came back as %T, not a ROW", res.Rows[0]["rw"])
	}
	for field, why := range rowFieldContainerLoss {
		if v, isStr := row[field].(string); !isStr || v != "" {
			t.Errorf("the whole-ROW read now carries %s as %#v — the container no longer loses it.\n"+
				"  Delete its rowFieldContainerLoss entry so every shape is gated for it.\n  was: %s",
				field, row[field], why)
			continue
		}
		t.Logf("known container-level loss, NOT gated (not #568): %s — %s", field, why)
	}
}

func rowFieldOpen(t *testing.T) (*DB, context.Context) {
	t.Helper()
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := rowFieldSchema()
	if err := db.CreateTable(ctx, "rowfld", schema, nil); err != nil {
		t.Fatal(err)
	}
	ing := db.NewIngester("rowfld", schema, nil, ingest.DefaultConfig())
	if err := ing.Ingest(ctx, rowFieldRows()); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

// TestRowFieldPathCarriesTheFieldsDeclaredType is the gate. For every field
// type, a query over `rw.<f>` must answer exactly what the same query over
// the flat `<f>` answers — rows, Go types and the DECLARED type alike.
func TestRowFieldPathCarriesTheFieldsDeclaredType(t *testing.T) {
	db, ctx := rowFieldOpen(t)

	// %[1]s is the reference under test: the flat column in one rendering,
	// the field path in the other.
	shapes := []struct{ name, sql string }{
		{"project", `SELECT id, %[1]s AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		{"order_asc", `SELECT id, %[1]s AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY %[1]s, id`},
		{"order_desc", `SELECT id, %[1]s AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY %[1]s DESC, id`},
		{"group", `SELECT %[1]s AS k, COUNT(*) AS n FROM rowfld WHERE ` + rowFieldPresent + ` GROUP BY %[1]s ORDER BY k, n`},
		{"minmax", `SELECT MIN(%[1]s) AS lo, MAX(%[1]s) AS hi FROM rowfld WHERE ` + rowFieldPresent},
		{"isnull", `SELECT COUNT(*) AS n FROM rowfld WHERE ` + rowFieldPresent + ` AND %[1]s IS NULL`},
		{"cast_text", `SELECT id, CAST(%[1]s AS VARCHAR) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		// LIKE has its own vectorized filter, and it resolved a dotted name
		// by stripping the qualifier and then matching NO ROWS when nothing
		// answered — an empty result indistinguishable from real data. Every
		// type reaches the kernel here, not just the text one: the pattern
		// is matched against the value's own rendering.
		{"like", `SELECT id FROM rowfld WHERE ` + rowFieldPresent + ` AND %[1]s LIKE '%%1%%' ORDER BY id`},

		// A field as a FUNCTION ARGUMENT. This is where boxing and
		// declaration meet: ColRef.Eval hands IPv4/MAC over as the raw
		// encoded int64 and DATE as the epoch day, and every family that has
		// to undo that — stringInputFuncs, networkTextFuncs,
		// temporalInputFuncs, the date-arithmetic operator — keys on the
		// reference's declared type and reads its vector. All of them
		// resolved only a bare COLUMN, so a field path reached them still
		// boxed as a number: UPPER(rw.ipv4) rendered "150994945",
		// LENGTH(rw.ipv4) counted that number's digits, and
		// DATE_TRUNC('month', rw.date) read the epoch day as epoch SECONDS
		// and answered 1970-01-01 (#568; the #484/#500/#319 family one level
		// down).
		//
		// Each shape runs for EVERY field type, not just the one it looks
		// written for: the comparison is against the same value in a flat
		// column, so whatever the engine does to an IPv4 in UPPER() it must
		// do to a ROW's IPv4 field.
		{"fn_upper", `SELECT id, UPPER(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		{"fn_lower", `SELECT id, LOWER(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		{"fn_length", `SELECT id, LENGTH(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		{"fn_concat", `SELECT id, CONCAT('x=', %[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		{"fn_substring", `SELECT id, SUBSTRING(CAST(%[1]s AS VARCHAR), 1, 3) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		{"fn_starts_with", `SELECT id, STARTS_WITH(%[1]s, '1') AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},

		// The ROW column reached through a DERIVED TABLE and a CTE. A
		// rename-only projection forwards the column unchanged, so the field
		// path above it must type exactly as it does at the base — but the
		// declared-schema walk handed the outer projection an emitted-type
		// map with no FIELDS in it, so the path fell back to STRING and the
		// aggregate could not resolve it at all (#568).
		// The subquery projects the CONTAINER ALONE on purpose: carrying an
		// `id` alongside it took a different route and hid the defect.
		{"derived", `SELECT %[1]s AS v FROM (SELECT rw FROM rowfld WHERE ` + rowFieldPresent + `) s ORDER BY v`},
		{"cte", `WITH s AS (SELECT rw FROM rowfld WHERE ` + rowFieldPresent + `) SELECT %[1]s AS v FROM s ORDER BY v`},
		{"derived_minmax", `SELECT MIN(%[1]s) AS lo, MAX(%[1]s) AS hi FROM (SELECT rw FROM rowfld WHERE ` + rowFieldPresent + `) s`},
		{"derived_group", `SELECT %[1]s AS k, COUNT(*) AS n FROM (SELECT rw FROM rowfld WHERE ` + rowFieldPresent + `) s GROUP BY %[1]s ORDER BY k, n`},
	}

	// Shapes that only make sense for a subset of the types, keyed by the
	// field they apply to. Kept apart from the generic list above because
	// the reference query has to be answerable for the flat column too, and
	// `ip_version(a_string)` is not the same question as `ip_version(an_ip)`.
	typed := map[string][]struct{ name, sql string }{
		"f_ipv4": {
			{"fn_ip_version", `SELECT id, ip_version(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"fn_ip_to_string", `SELECT id, ip_to_string(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"fn_is_private", `SELECT id, is_private_ip(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		},
		"f_mac": {
			{"fn_mac_to_string", `SELECT id, mac_to_string(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		},
		"f_date": {
			{"fn_date_trunc", `SELECT id, DATE_TRUNC('month', %[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"fn_year", `SELECT id, YEAR(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"fn_extract", `SELECT id, EXTRACT(MONTH FROM %[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"op_date_minus_interval", `SELECT id, %[1]s - INTERVAL '1' DAY AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		},
		"f_ts": {
			{"fn_date_trunc", `SELECT id, DATE_TRUNC('month', %[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"fn_year", `SELECT id, YEAR(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		},
		"f_cidr": {
			{"fn_prefix_length", `SELECT id, prefix_length(%[1]s) AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"pred_gt_literal", `SELECT id FROM rowfld WHERE ` + rowFieldPresent + ` AND %[1]s > '10.0.0.0/8' ORDER BY id`},
		},
		"f_dec": {
			{"arith_plus_one", `SELECT id, %[1]s + 1 AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
		},
		"f_i64": {
			{"arith_plus_one", `SELECT id, %[1]s + 1 AS v FROM rowfld WHERE ` + rowFieldPresent + ` ORDER BY id`},
			{"pred_between", `SELECT id FROM rowfld WHERE ` + rowFieldPresent + ` AND %[1]s BETWEEN 10 AND 100 ORDER BY id`},
			{"pred_in", `SELECT id FROM rowfld WHERE ` + rowFieldPresent + ` AND %[1]s IN (9, 10, 192) ORDER BY id`},
		},
	}

	for _, f := range rowFieldTypes() {
		if why, pinned := rowFieldContainerLoss[f.Name]; pinned {
			t.Run(f.Name, func(t *testing.T) { t.Skipf("not gated: %s", why) })
			continue
		}
		t.Run(f.Name, func(t *testing.T) {
			for _, sh := range append(append([]struct{ name, sql string }{}, shapes...), typed[f.Name]...) {
				t.Run(sh.name, func(t *testing.T) {
					flatSQL := strings.ReplaceAll(fmt.Sprintf(sh.sql, flatName(f.Name)),
						"SELECT rw FROM rowfld", "SELECT "+flatName(f.Name)+" FROM rowfld")
					pathSQL := fmt.Sprintf(sh.sql, "rw."+f.Name)
					want, err := db.Query(ctx, flatSQL)
					if err != nil {
						// The reference must work. If it does not, the shape
						// is not supported for this type at all and the
						// comparison would be vacuous.
						t.Skipf("flat reference not answerable: %s: %v", flatSQL, err)
					}
					got, err := db.Query(ctx, pathSQL)
					if err != nil {
						t.Fatalf("%s\n  errored while the same query over the flat column answered.\n  %v",
							pathSQL, err)
					}
					compareRowFieldResults(t, flatSQL, pathSQL, want, got)
				})
			}
		})
	}
}

// TestRowFieldPathComparesAgainstItsOwnColumn takes the field path through
// the PREDICATE path, where the operands' declared types decide the
// comparison rule (expr.classifyOperand). A field path used to be classified
// boxUnknown outright — "the container's declaration does not type the field
// here" — so `WHERE rw.f = f` fell through to a guess.
func TestRowFieldPathComparesAgainstItsOwnColumn(t *testing.T) {
	db, ctx := rowFieldOpen(t)
	for _, f := range rowFieldTypes() {
		switch f.Type {
		case parquet.TypeRow, parquet.TypeArray, parquet.TypeMap:
			continue // container equality is a separate question (#415/#426)
		}
		if _, pinned := rowFieldContainerLoss[f.Name]; pinned {
			continue
		}
		t.Run(f.Name, func(t *testing.T) {
			// Every present row whose field is non-NULL holds the same value
			// on both sides, so the field path must match its own column
			// exactly as often as the column is non-NULL, and never exceed it.
			var wantN int64
			ref, err := db.Query(ctx, fmt.Sprintf(
				`SELECT COUNT(*) AS n FROM rowfld WHERE %s AND %s IS NOT NULL`, rowFieldPresent, flatName(f.Name)))
			if err != nil {
				t.Fatalf("reference count: %v", err)
			}
			wantN = ref.Rows[0]["n"].(int64)

			eq, err := db.Query(ctx, fmt.Sprintf(
				`SELECT COUNT(*) AS n FROM rowfld WHERE %s AND rw.%s = %s`,
				rowFieldPresent, f.Name, flatName(f.Name)))
			if err != nil {
				t.Fatalf("field path = its own column: %v", err)
			}
			if got := eq.Rows[0]["n"].(int64); got != wantN {
				t.Errorf("`rw.%s = %s` matched %d rows, want %d — the field path and the column hold "+
					"the same value in every one of them", f.Name, flatName(f.Name), got, wantN)
			}
		})
	}
}

// TestRowFieldPathDeclaresTheFieldsTypeOnAnEmptyResult is the wire half. A
// zero-row result has no batch to read a schema off, so pgwire declares the
// PLAN's schema (declaredOutputSchema, #416) — and that walk resolved a field
// path by stripping the qualifier and finding no column, so every field path
// declared STRING and went out as OID 25.
func TestRowFieldPathDeclaresTheFieldsTypeOnAnEmptyResult(t *testing.T) {
	db, ctx := rowFieldOpen(t)
	for _, f := range rowFieldTypes() {
		t.Run(f.Name, func(t *testing.T) {
			full, err := db.Query(ctx, fmt.Sprintf(
				`SELECT rw.%s AS v FROM rowfld WHERE %s`, f.Name, rowFieldPresent))
			if err != nil {
				t.Fatalf("non-empty query: %v", err)
			}
			empty, err := db.Query(ctx, fmt.Sprintf(
				`SELECT rw.%s AS v FROM rowfld WHERE id < 0`, f.Name))
			if err != nil {
				t.Fatalf("empty query: %v", err)
			}
			if len(empty.Rows) != 0 {
				t.Fatalf("the empty query returned %d rows", len(empty.Rows))
			}
			wantType := metaTypeName(full.ColumnMetas, "v")
			gotType := metaTypeName(empty.ColumnMetas, "v")
			if wantType == "" {
				t.Fatalf("the non-empty result declared no type for v")
			}
			if gotType != wantType {
				t.Errorf("empty result declares v as %s, the same query with rows declares %s — "+
					"a client binding by OID sees two different columns for one query", gotType, wantType)
			}
		})
	}
}

// TestRowFieldPathRefusesAMalformedNetworkLiteral is the error half of the
// same rule: a field path must REFUSE what its column refuses.
//
// `cidr_col = 'not-a-cidr'` raises 22P02 — the literal names no value the
// column can hold, so there is no comparison to make (#492). The field path
// delegated to the row predicate without that check and answered ZERO ROWS,
// which is a value answer to a question that has none, and the empty result
// is indistinguishable from a genuine no-match.
func TestRowFieldPathRefusesAMalformedNetworkLiteral(t *testing.T) {
	db, ctx := rowFieldOpen(t)
	for _, tc := range []struct{ field, lit string }{
		{"f_cidr", "not-a-cidr"},
		{"f_ipv4", "not-an-ip"},
		{"f_ipv6", "aa:bb:cc:dd:ee:ff"},
		{"f_mac", "10.0.0.1"},
	} {
		t.Run(tc.field, func(t *testing.T) {
			flat := fmt.Sprintf(`SELECT COUNT(*) AS n FROM rowfld WHERE %s = '%s'`, flatName(tc.field), tc.lit)
			path := fmt.Sprintf(`SELECT COUNT(*) AS n FROM rowfld WHERE rw.%s = '%s'`, tc.field, tc.lit)
			_, flatErr := db.Query(ctx, flat)
			if flatErr == nil {
				t.Skipf("the column itself does not refuse %q, so the field path has nothing to match", tc.lit)
			}
			_, pathErr := db.Query(ctx, path)
			if pathErr == nil {
				t.Fatalf("%s answered rows; the same predicate on the column refuses it: %v", path, flatErr)
			}
			if pathErr.Error() != flatErr.Error() {
				t.Errorf("the two spellings refuse differently\n  column: %v\n  field:  %v", flatErr, pathErr)
			}
		})
	}
}

func metaTypeName(metas []ColumnMeta, name string) string {
	for _, m := range metas {
		if m.Name == name {
			return m.TypeName
		}
	}
	return ""
}

// compareRowFieldResults asserts two results are the same answer: same
// declared column types, and same rows POSITIONALLY down to the Go type of
// every cell. reflect.DeepEqual is the point — string("9") and int64(9) are
// the divergence this whole gate exists for, and any comparison that
// stringifies cannot see it (oracle.Compare, which the type-matrix gates use,
// renders every cell with fmt.Sprint).
func compareRowFieldResults(t *testing.T, flatSQL, pathSQL string, want, got *QueryResult) {
	t.Helper()
	if len(want.Columns) != len(got.Columns) {
		t.Fatalf("column count %d vs %d\n  flat: %s\n  path: %s",
			len(want.Columns), len(got.Columns), flatSQL, pathSQL)
	}
	for _, c := range want.Columns {
		w, g := metaTypeName(want.ColumnMetas, c), metaTypeName(got.ColumnMetas, c)
		if w != g {
			t.Errorf("column %q declared %s over the flat column and %s over the field path\n  flat: %s\n  path: %s",
				c, w, g, flatSQL, pathSQL)
		}
	}
	if len(want.Rows) != len(got.Rows) {
		t.Fatalf("row count %d vs %d\n  flat: %s\n  path: %s",
			len(want.Rows), len(got.Rows), flatSQL, pathSQL)
	}
	for i := range want.Rows {
		for _, c := range want.Columns {
			w, g := want.Rows[i][c], got.Rows[i][c]
			if !reflect.DeepEqual(w, g) {
				t.Errorf("row %d column %q: flat %#v (%T), field path %#v (%T)\n  flat: %s\n  path: %s",
					i, c, w, w, g, g, flatSQL, pathSQL)
				return
			}
		}
	}
}
