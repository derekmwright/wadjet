package sql

import "testing"

// A qualifier that names a COLUMN of a relation the subquery reads is a ROW
// FIELD PATH, not a lost correlation — ADR-0022 rule 7, #866.
//
// Both directions, because the resolver's job is to SEPARATE two things that
// parse identically: `c_row.b` over a relation with a `c_row` column is
// self-contained, and `o.id` over the same relation is a correlation the
// classifier lost. Without the resolver — the worker's own compile sites,
// which have no catalog — both stay dangling, which is the pre-#866 answer and
// the safe direction.
func TestDanglingTableRefsTellsAFieldPathFromALostCorrelation(t *testing.T) {
	resolve := func(table string) []string {
		switch table {
		case "typemx_nested":
			return []string{"id", "c_row"}
		case "decpair":
			return []string{"id", "a", "b"}
		}
		return nil
	}

	for _, tc := range []struct {
		name, sql string
		resolve   TableColumns
		want      []string // qualifier.column of each ref still reported
	}{
		{"a field path is not dangling",
			"SELECT c_row.b FROM typemx_nested", resolve, nil},
		{"a field path in the WHERE is not dangling",
			"SELECT id FROM typemx_nested WHERE c_row.b > 3", resolve, nil},
		{"a lost correlation still is",
			"SELECT id FROM typemx_nested WHERE id = o.id", resolve, []string{"o.id"}},
		{"a lost correlation beside a field path still is",
			"SELECT c_row.b FROM typemx_nested WHERE id = o.id", resolve, []string{"o.id"}},
		{"with NO resolver a field path stays dangling — the pre-#866 answer",
			"SELECT c_row.b FROM typemx_nested", nil, []string{"c_row.b"}},
		{"a relation the resolver cannot name keeps its refs",
			"SELECT c_row.b FROM unknown_table", resolve, []string{"c_row.b"}},
		{"a self-contained subquery reports nothing either way",
			"SELECT b FROM decpair WHERE id < 3", resolve, nil},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			refs := DanglingTableRefsWithScope(tc.sql, tc.resolve)
			got := make([]string, 0, len(refs))
			for _, r := range refs {
				got = append(got, r.Table+"."+r.Column)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("DanglingTableRefsWithScope(%q) = %v, want %v", tc.sql, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("DanglingTableRefsWithScope(%q) = %v, want %v", tc.sql, got, tc.want)
				}
			}
		})
	}
}
