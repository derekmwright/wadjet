package physical

import (
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/expr"
	"github.com/derekmwright/wadjet/internal/planner/logical"
	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// nodeDeclaredType is where a projection's output vector type comes from, so a
// wrong answer here is a kernel writing values into a vector that cannot hold
// them — silently, since the write is simply dropped.
//
// This table passes no column types, so a bare column decides nothing and the
// polymorphic fallbacks are all that is left — the state of the world before
// #333. TestNodeDeclaredTypeFromColumnTypes below is the same table with the
// catalog in hand.
//
// The case that named this file is #331:
//
//	SELECT COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback') FROM nation
//
// came back as the integer 0 on every row. coalesce mirrors the type of the
// first argument that decides one; its argument 0 is nullif, which mirrors ITS
// argument 0 — a bare column, whose type is not known until the input schema
// arrives at runtime. Unable to decide, nullif answered with its numeric
// fallback and said nothing about that being a guess, so coalesce stopped
// there and never asked the string literal in argument 1.
//
// The fallback is not the defect: NULLIF(int_col, 1) as a projection is
// numeric and has nothing better to go on, which the last cases below pin.
func TestNodeDeclaredTypeThroughNestedCalls(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		want  parquet.TypeID
		wantC expr.Confidence
	}{
		{
			name: "nested guess does not stop the search — a later argument decides",
			sql:  "COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback')",
			// Float64 here is the bug: a string column typed numeric, whose
			// values are dropped on the way out.
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "two levels of nesting",
			sql:  "COALESCE(NULLIF(NULLIF(n_name, 'ALGERIA'), 'BRAZIL'), 'fallback')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "three levels of nesting",
			sql:  "COALESCE(NULLIF(NULLIF(NULLIF(n_name, 'a'), 'b'), 'c'), 'fallback')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "guess through a fixed-return call in between",
			sql:  "COALESCE(UPPER(NULLIF(n_name, 'ALGERIA')), 'fallback')",
			// upper() is RetString, so this one was never in doubt — the
			// control that says the recursion itself is not what broke.
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "ifnull mirrors two arguments, and skips the guess in the first",
			sql:  "IFNULL(NULLIF(n_name, 'ALGERIA'), 'fallback')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "greatest consults every argument",
			sql:  "GREATEST(NULLIF(n_name, 'ALGERIA'), 'MMM')",
			want: parquet.TypeString, wantC: expr.Decided,
		},
		{
			name: "nothing decides: the fallback answers, as a guess",
			sql:  "COALESCE(NULLIF(n_name, 'ALGERIA'), n_comment)",
			want: parquet.TypeFloat64, wantC: expr.Guessed,
		},
		{
			name: "a bare column decides nothing",
			sql:  "n_name",
			want: 0, wantC: expr.Undecided,
		},
		{
			name: "numeric nullif keeps its numeric fallback",
			sql:  "NULLIF(n_regionkey, 1)",
			want: parquet.TypeFloat64, wantC: expr.Guessed,
		},
		{
			name: "numeric stays numeric through a nested guess",
			sql:  "COALESCE(NULLIF(n_regionkey, 0), 1)",
			want: parquet.TypeInt64, wantC: expr.Decided,
		},
	}
	for _, tc := range tests {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("%s: parse %q: %v", tc.name, tc.sql, err)
		}
		got, c := nodeDeclaredType(node, nil)
		if got != tc.want || c != tc.wantC {
			t.Errorf("%s\n  %s\n  declared (%s, %s), want (%s, %s)",
				tc.name, tc.sql, got, c, tc.want, tc.wantC)
		}
	}
}

// The projection's own fallback is only reached when nothing decided a type at
// all: a polymorphic function's guess is an answer and outranks it. Without
// that, NULLIF(int_col, 1) would be typed String here — the fallback a
// projection starts from — and its numeric kernel would write into a vector
// with no Float64Data.
func TestInferProjectionTypeUsesAGuessOverItsOwnFallback(t *testing.T) {
	tests := []struct {
		sql  string
		want parquet.TypeID
	}{
		{"COALESCE(NULLIF(n_name, 'ALGERIA'), 'fallback')", parquet.TypeString},
		{"NULLIF(n_regionkey, 1)", parquet.TypeFloat64},
		{"COALESCE(NULLIF(n_name, 'ALGERIA'), n_comment)", parquet.TypeFloat64},
		{"n_name", parquet.TypeString}, // undecided: the caller's fallback
	}
	for _, tc := range tests {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.sql, err)
		}
		if got := inferProjectionType(node, parquet.TypeString); got != tc.want {
			t.Errorf("%s: projection typed %s, want %s", tc.sql, got, tc.want)
		}
	}
}

// nationColTypes is the catalog answer AnnotateScanColumns writes into
// logical.Node.ScanColTypes, with columns added to nation's real schema for
// the cases a TypeID alone cannot describe (decimal, vector, array) and for
// the name shapes that have to survive the lookup (a delimited identifier
// holding a dot).
var nationColTypes = map[string]parquet.TypeID{
	"n_name":       parquet.TypeString,
	"n_comment":    parquet.TypeString,
	"n_nationkey":  parquet.TypeInt64,
	"n_regionkey":  parquet.TypeInt64,
	"n_ratio":      parquet.TypeFloat64,
	"n_amount":     parquet.TypeDecimal,
	"n_embedding":  parquet.TypeVector,
	"n_tags":       parquet.TypeArray,
	"id.orig_h":    parquet.TypeIPv4,
	"n_createdate": parquet.TypeDate,
}

// With the input's column types in hand a bare column reference DECIDES, which
// is what retires the family in #333: nothing in COALESCE(n_name, n_comment)
// decided a type, so coalesce's numeric fallback stood, the projection got a
// Float64 vector, and every string write was dropped for the integer 0.
//
// The numeric half is the constraint that ruled out simply flipping those
// declarations to String, so it is pinned here at the same time: a numeric
// column through the same functions stays numeric, and becomes MORE precise
// (Int64/Float64 rather than the blanket Float64 guess) rather than less.
func TestNodeDeclaredTypeFromColumnTypes(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		want  parquet.TypeID
		wantC expr.Confidence
	}{
		// The broken shapes from the issue. Every one of these was
		// (Float64, Guessed) — a string column typed numeric.
		{"nullif over a string column", "NULLIF(n_name, 'ALGERIA')",
			parquet.TypeString, expr.Decided},
		{"single-argument coalesce", "COALESCE(n_name)",
			parquet.TypeString, expr.Decided},
		{"coalesce over two string columns", "COALESCE(n_name, n_comment)",
			parquet.TypeString, expr.Decided},
		{"greatest over two string columns", "GREATEST(n_name, n_comment)",
			parquet.TypeString, expr.Decided},
		{"least over two string columns", "LEAST(n_name, n_comment)",
			parquet.TypeString, expr.Decided},
		// ifnull was the tell: it alone declared RetSameAsArg(TypeString),
		// so it alone was already right. It must stay right.
		{"ifnull, the control that already worked", "IFNULL(n_name, n_comment)",
			parquet.TypeString, expr.Decided},

		// The numeric constraint. NULLIF(int_col, 1) was the reason #331 was
		// solved with a confidence signal instead of by editing declarations.
		{"nullif over an int column stays numeric", "NULLIF(n_nationkey, 1)",
			parquet.TypeInt64, expr.Decided},
		{"coalesce over an int column stays numeric", "COALESCE(n_nationkey, 0)",
			parquet.TypeInt64, expr.Decided},
		{"greatest over two int columns stays numeric", "GREATEST(n_nationkey, n_regionkey)",
			parquet.TypeInt64, expr.Decided},
		{"a float column keeps its float", "COALESCE(n_ratio, 0)",
			parquet.TypeFloat64, expr.Decided},
		{"a float column beside an int column", "GREATEST(n_ratio, n_nationkey)",
			parquet.TypeFloat64, expr.Decided},
		// A temporal column keeps its own type rather than being flattened
		// to a number: DATE and TIMESTAMP have their own vector storage, and
		// what the value path writes round-trips only through it.
		{"a date column keeps its date", "COALESCE(n_createdate, n_createdate)",
			parquet.TypeDate, expr.Decided},

		// Mixed types: the first argument that decides wins, which is
		// DuckDB's answer for COALESCE(int_col, 'text') — INTEGER, the
		// string literal cast to it.
		{"mixed int column and string literal follows the column",
			"COALESCE(n_nationkey, 'text')", parquet.TypeInt64, expr.Decided},
		{"mixed string column and int literal follows the column",
			"COALESCE(n_name, 0)", parquet.TypeString, expr.Decided},

		// #331's nesting still resolves, now without needing the literal.
		{"nested nullif over a string column", "COALESCE(NULLIF(n_name, 'ALGERIA'), n_comment)",
			parquet.TypeString, expr.Decided},
		{"nested nullif over an int column", "COALESCE(NULLIF(n_nationkey, 0), n_regionkey)",
			parquet.TypeInt64, expr.Decided},

		// A column the input does not carry is not a column here. So is an
		// aggregate output and a synthetic sort/group key: none of them are
		// in the map, and a wrong confident answer would be propagated as
		// fact by #331's machinery.
		{"an unknown column decides nothing", "COALESCE(no_such_col, n_missing)",
			parquet.TypeFloat64, expr.Guessed},
		{"an aggregate output column decides nothing", `COALESCE("sum(l_quantity)")`,
			parquet.TypeFloat64, expr.Guessed},
		{"a synthetic sort key decides nothing", "COALESCE(__sortkey_0)",
			parquet.TypeFloat64, expr.Guessed},
		{"a synthetic group key decides nothing", "COALESCE(__gb_expr_0)",
			parquet.TypeFloat64, expr.Guessed},

		// Parameterized types carry more than a TypeID, and the map carries
		// only the TypeID — declaring them would build an output vector
		// without the scale/dimension/element type it needs.
		{"a decimal column declines", "COALESCE(n_amount, n_amount)",
			parquet.TypeFloat64, expr.Guessed},
		{"a vector column declines", "COALESCE(n_embedding)",
			parquet.TypeFloat64, expr.Guessed},
		{"an array column declines", "COALESCE(n_tags)",
			parquet.TypeFloat64, expr.Guessed},

		// A delimited identifier that contains a dot is one name, not a
		// qualified reference, and must not be split on the way to the map.
		{"a delimited identifier with a dot resolves whole", `COALESCE("id.orig_h")`,
			parquet.TypeIPv4, expr.Decided},
		// A genuinely qualified reference matches on its bare column.
		{"a qualified reference matches the bare name", "COALESCE(n.n_name)",
			parquet.TypeString, expr.Decided},

		// A bare column decides here; it is inferProjectionTypeCols that
		// withholds the catalog from a projection that is only a copy, since
		// exec.Project types that output from the column it copies. See
		// TestInferProjectionTypeColsWithholdsTheCatalogFromACopy.
		{"a bare column decides once the catalog is in hand", "n_name",
			parquet.TypeString, expr.Decided},
	}
	for _, tc := range tests {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("%s: parse %q: %v", tc.name, tc.sql, err)
		}
		got, c := nodeDeclaredType(node, nationColTypes)
		if got != tc.want || c != tc.wantC {
			t.Errorf("%s\n  %s\n  declared (%s, %s), want (%s, %s)",
				tc.name, tc.sql, got, c, tc.want, tc.wantC)
		}
	}
}

// inferProjectionTypeCols is what the planner actually calls, and it withholds
// the catalog from a projection that is a bare column copy: exec.Project types
// that output from the input column, which sees renames and derived inputs the
// catalog cannot. Everything computed gets the catalog.
func TestInferProjectionTypeColsWithholdsTheCatalogFromACopy(t *testing.T) {
	tests := []struct {
		sql  string
		want parquet.TypeID
	}{
		// Bare copies: the caller's fallback, as before this change.
		{"n_nationkey", parquet.TypeString},
		{"(n_nationkey)", parquet.TypeString},
		{"n_name", parquet.TypeString},
		// Computed: the catalog decides.
		{"COALESCE(n_name, n_comment)", parquet.TypeString},
		{"NULLIF(n_nationkey, 1)", parquet.TypeInt64},
		{"UPPER(n_name)", parquet.TypeString},
	}
	for _, tc := range tests {
		node, err := plansql.ParseExpression(tc.sql)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.sql, err)
		}
		if got := inferProjectionTypeCols(node, parquet.TypeString, nil, nationColTypes); got != tc.want {
			t.Errorf("%s: projection typed %s, want %s", tc.sql, got, tc.want)
		}
	}
}

// inputColTypes describes a node's OUTPUT, so it has to stop at every node
// that can rebind a name. Descending past one would answer with a scan's type
// for a value that arrives as something else — the same silent-drop corruption
// #333 is about, pointing the other way.
func TestInputColTypesStopsAtRebindingNodes(t *testing.T) {
	scan := func(cols map[string]parquet.TypeID) *logical.Node {
		return &logical.Node{Type: logical.NodeScan, TableName: "nation", ScanColTypes: cols}
	}
	nation := map[string]parquet.TypeID{"n_name": parquet.TypeString, "n_nationkey": parquet.TypeInt64}
	region := map[string]parquet.TypeID{"r_name": parquet.TypeString, "r_regionkey": parquet.TypeInt64}
	// Same name, different type on each side of a join.
	conflicting := map[string]parquet.TypeID{"n_name": parquet.TypeInt64}
	wrap := func(t logical.NodeType, child *logical.Node) *logical.Node {
		return &logical.Node{Type: t, Children: []*logical.Node{child}}
	}
	join := func(l, r *logical.Node) *logical.Node {
		return &logical.Node{Type: logical.NodeJoin, Children: []*logical.Node{l, r}}
	}

	tests := []struct {
		name string
		node *logical.Node
		want map[string]parquet.TypeID
	}{
		{"a scan answers its catalog types", scan(nation), nation},
		{"a filter is shape-preserving", wrap(logical.NodeFilter, scan(nation)), nation},
		{"a sort is shape-preserving", wrap(logical.NodeSort, scan(nation)), nation},
		{"a limit is shape-preserving", wrap(logical.NodeLimit, scan(nation)), nation},
		{"a distinct is shape-preserving", wrap(logical.NodeDistinct, scan(nation)), nation},
		{"a project can rebind a name", wrap(logical.NodeProject, scan(nation)), nil},
		{"an aggregate replaces the schema", wrap(logical.NodeAggregate, scan(nation)), nil},
		{"a window adds columns of its own", wrap(logical.NodeWindow, scan(nation)), nil},
		{"a join merges both sides", join(scan(nation), scan(region)), map[string]parquet.TypeID{
			"n_name": parquet.TypeString, "n_nationkey": parquet.TypeInt64,
			"r_name": parquet.TypeString, "r_regionkey": parquet.TypeInt64,
		}},
		{"a join drops the names its sides disagree on", join(scan(nation), scan(conflicting)),
			map[string]parquet.TypeID{"n_nationkey": parquet.TypeInt64}},
		{"an unannotated scan makes the whole answer unknown", scan(nil), nil},
		{"one unannotated side of a join is enough", join(scan(nation), scan(nil)), nil},
		{"a project below a join is enough", join(scan(nation), wrap(logical.NodeProject, scan(region))), nil},
		{"nil", nil, nil},
	}
	for _, tc := range tests {
		got := inputColTypes(tc.node)
		if len(got) != len(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
			continue
		}
		for c, want := range tc.want {
			if got[c] != want {
				t.Errorf("%s: column %q typed %s, want %s", tc.name, c, got[c], want)
			}
		}
	}
}

// windowSpecOutputType is the window operator's half of the same problem
// (#345). exec.Window allocates batch.NewVector(OutputType) and had no runtime
// correction, so a value function declared float64 over a string column
// dropped every write for the integer 0:
//
//	SELECT n_name, LAG(n_name) OVER (ORDER BY n_nationkey) FROM nation
//	-- NULL, 0, 0, 0 ...
//
// The rank family is genuinely input-independent and stays on the name list.
// The value functions resolve from the column they copy out of, and DECLINE —
// keeping the float64 fallback — wherever the column cannot be resolved,
// because a confidently wrong declaration is worse than the guess it replaces.
func TestWindowSpecOutputType(t *testing.T) {
	nation := map[string]parquet.TypeID{
		"n_name":      parquet.TypeString,
		"n_nationkey": parquet.TypeInt32,
		"n_founded":   parquet.TypeDate,
		"n_seen_at":   parquet.TypeTimestamp,
		"n_rate":      parquet.TypeDecimal,
		"n_tags":      parquet.TypeArray,
	}
	scan := func(cols map[string]parquet.TypeID) *logical.Node {
		return &logical.Node{Type: logical.NodeScan, TableName: "nation", ScanColTypes: cols}
	}
	// A Window node over the given child, as buildWindow receives it.
	win := func(child *logical.Node) *logical.Node {
		return &logical.Node{Type: logical.NodeWindow, Children: []*logical.Node{child}}
	}

	tests := []struct {
		name  string
		node  *logical.Node
		fn    string
		input string
		want  parquet.TypeID
	}{
		// The value functions: the input column IS the answer.
		{"lag over a string column", win(scan(nation)), "lag", "n_name", parquet.TypeString},
		{"lead over a string column", win(scan(nation)), "lead", "n_name", parquet.TypeString},
		{"first_value over a string column", win(scan(nation)), "first_value", "n_name", parquet.TypeString},
		{"last_value over a string column", win(scan(nation)), "last_value", "n_name", parquet.TypeString},
		{"nth_value over a string column", win(scan(nation)), "nth_value", "n_name, 2", parquet.TypeString},
		{"a DATE column keeps its rendering", win(scan(nation)), "first_value", "n_founded", parquet.TypeDate},
		{"a TIMESTAMP column keeps its rendering", win(scan(nation)), "lag", "n_seen_at", parquet.TypeTimestamp},
		{"a narrow int stays narrow", win(scan(nation)), "lag", "n_nationkey", parquet.TypeInt32},
		// LAG's offset and default ride in the same string; the column is
		// everything up to the first comma, as buildWindow splits it.
		{"lag with an offset and a default", win(scan(nation)), "lag", "n_name, 2, 'NONE'", parquet.TypeString},
		{"a qualified reference resolves to its bare name", win(scan(nation)), "lag", "n1.n_name", parquet.TypeString},
		{"the function name is matched case-insensitively", win(scan(nation)), "FIRST_VALUE", "n_name", parquet.TypeString},

		// The rank family: input-independent, so the name list still answers.
		{"row_number", win(scan(nation)), "row_number", "", parquet.TypeInt64},
		{"rank", win(scan(nation)), "rank", "", parquet.TypeInt64},
		{"dense_rank", win(scan(nation)), "dense_rank", "", parquet.TypeInt64},
		{"ntile", win(scan(nation)), "ntile", "4", parquet.TypeInt64},
		{"percent_rank", win(scan(nation)), "percent_rank", "", parquet.TypeFloat64},
		{"cume_dist", win(scan(nation)), "cume_dist", "", parquet.TypeFloat64},
		{"count", win(scan(nation)), "count", "n_name", parquet.TypeInt64},
		{"sum finalizes to float64 whatever it summed", win(scan(nation)), "sum", "n_nationkey", parquet.TypeFloat64},

		// Declines. Each keeps the pre-#345 float64, which is what every
		// caller's fallback already handles.
		{"a computed argument decides nothing", win(scan(nation)), "first_value", "UPPER(n_name)", parquet.TypeFloat64},
		{"a column no scan carries", win(scan(nation)), "lag", "not_a_column", parquet.TypeFloat64},
		{"an unannotated scan", win(scan(nil)), "lag", "n_name", parquet.TypeFloat64},
		{"a project can rebind the name", win(&logical.Node{Type: logical.NodeProject, Children: []*logical.Node{scan(nation)}}),
			"lag", "n_name", parquet.TypeFloat64},
		{"DECIMAL carries no scale in the catalog map", win(scan(nation)), "lag", "n_rate", parquet.TypeFloat64},
		{"ARRAY carries no element type", win(scan(nation)), "lag", "n_tags", parquet.TypeFloat64},
		{"an empty argument", win(scan(nation)), "lag", "", parquet.TypeFloat64},
		{"a window with no child", &logical.Node{Type: logical.NodeWindow}, "lag", "n_name", parquet.TypeFloat64},
	}
	for _, tc := range tests {
		got := windowSpecOutputType(tc.node, logical.WindowExpr{Func: tc.fn, InputCol: tc.input, OutputCol: "w"})
		if got != tc.want {
			t.Errorf("%s: %s(%s) declared %s, want %s", tc.name, tc.fn, tc.input, got, tc.want)
		}
	}
}
