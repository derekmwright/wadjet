package test

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/ingest"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
)

// Issue #341 at the query layer. The unit tests in internal/engine/expr pin the
// compiler's verdict; these pin what a user actually gets back, which is where
// the bug did its damage:
//
//	SELECT no_such_function(n_name) FROM nation   → a column of NULLs
//	SELECT array_agg(n_name) FROM nation          → 25 rows of NULL, not 1
//
// The aggregate case is the reason this file exists separately from the unit
// tests: a name nothing recognizes as an aggregate never triggers grouping, so
// the result's SHAPE was wrong, not just its values. Only an end-to-end query
// can observe that.

func unknownFuncDB(t *testing.T) (context.Context, *wadjet.DB) {
	t.Helper()
	ctx := context.Background()
	store := objstore.NewMemStore()
	db, err := wadjet.Open(ctx, wadjet.Config{Store: store, Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	schema := parquet.Schema{
		Columns: []parquet.Column{
			{Name: "n_name", Type: parquet.TypeString},
			{Name: "p_retailprice", Type: parquet.TypeFloat64},
			{Name: "d", Type: parquet.TypeDate},
		},
	}
	if err := db.CreateTable(ctx, "nation", schema, nil); err != nil {
		t.Fatal(err)
	}
	rows := []map[string]any{
		{"n_name": "ALGERIA", "p_retailprice": 900.5, "d": "1996-01-10"},
		{"n_name": "ARGENTINA", "p_retailprice": 902.5, "d": "1996-01-10"},
	}
	ing := db.NewIngester("nation", schema, nil, ingest.Config{MaxBufferRows: 10})
	if err := ing.Ingest(ctx, rows); err != nil {
		t.Fatal(err)
	}
	if err := ing.FlushAll(ctx); err != nil {
		t.Fatal(err)
	}
	return ctx, db
}

// TestUnknownFunctionErrors: the query must fail, and the message must name the
// function. "Unknown function" is the single most actionable error a SQL engine
// can return, and it is worthless if it does not say which one.
func TestUnknownFunctionErrors(t *testing.T) {
	ctx, db := unknownFuncDB(t)
	cases := []struct {
		name, sql, wantIn string
	}{
		{
			name:   "scalar in projection",
			sql:    `SELECT no_such_function(n_name) FROM nation`,
			wantIn: "no_such_function",
		},
		{
			name:   "scalar in WHERE",
			sql:    `SELECT n_name FROM nation WHERE no_such_predicate(n_name)`,
			wantIn: "no_such_predicate",
		},
		{
			name:   "scalar nested in arithmetic",
			sql:    `SELECT no_such_function(p_retailprice) + 1 FROM nation`,
			wantIn: "no_such_function",
		},
		{
			name:   "scalar in GROUP BY",
			sql:    `SELECT count(*) FROM nation GROUP BY no_such_function(n_name)`,
			wantIn: "no_such_function",
		},
		{
			name:   "unknown name in aggregate position",
			sql:    `SELECT no_such_agg(p_retailprice) FROM nation GROUP BY n_name`,
			wantIn: "no_such_agg",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			res, err := db.Query(ctx, c.sql)
			if err == nil {
				t.Fatalf("expected an error, got %d rows: %v", len(res.Rows), res.Rows)
			}
			if !strings.Contains(err.Error(), c.wantIn) {
				t.Errorf("error must name %q, got %q", c.wantIn, err.Error())
			}
		})
	}
}

// TestUnknownAggregateErrors covers the shape bug specifically. Before the fix
// each of these returned one NULL row PER INPUT ROW rather than a single
// aggregated row, so a caller checking only values would still have been
// reading the wrong number of them.
func TestUnknownAggregateErrorsAtQueryLayer(t *testing.T) {
	ctx, db := unknownFuncDB(t)
	for _, sql := range []string{
		`SELECT array_agg(n_name) FROM nation`,
		`SELECT count_if(n_name = 'ALGERIA') FROM nation`,
	} {
		res, err := db.Query(ctx, sql)
		if err == nil {
			t.Errorf("%s: expected an error, got %d rows: %v", sql, len(res.Rows), res.Rows)
			continue
		}
		if !strings.Contains(err.Error(), "aggregate") {
			t.Errorf("%s: error should identify the name as an aggregate, got %q", sql, err.Error())
		}
	}
}

// TestIssueFunctionsAnswerCorrectly pins the three functions the issue tabulates
// as returning NULL, with the values it says they owe. Registering them was a
// choice — the error is the fix and the functions are a follow-up — but each is
// a spelling of something the engine already had, so leaving them to error
// would have been the worse of the two.
func TestIssueFunctionsAnswerCorrectly(t *testing.T) {
	ctx, db := unknownFuncDB(t)
	cases := []struct {
		sql  string
		want float64
	}{
		{`SELECT DATE_PART('year', DATE '1996-01-10') AS v FROM nation LIMIT 1`, 1996},
		{`SELECT date_part('year', d) AS v FROM nation LIMIT 1`, 1996},
		{`SELECT ascii(n_name) AS v FROM nation LIMIT 1`, 65},
		{`SELECT ceiling(p_retailprice) AS v FROM nation LIMIT 1`, 901},
		{`SELECT CEILING(p_retailprice) AS v FROM nation LIMIT 1`, 901},
	}
	for _, c := range cases {
		res, err := db.Query(ctx, c.sql)
		if err != nil {
			t.Errorf("%s: %v", c.sql, err)
			continue
		}
		if len(res.Rows) != 1 {
			t.Errorf("%s: got %d rows, want 1", c.sql, len(res.Rows))
			continue
		}
		got, ok := toNumber(res.Rows[0]["v"])
		if !ok || got != c.want {
			t.Errorf("%s: got %v (%T), want %v",
				c.sql, res.Rows[0]["v"], res.Rows[0]["v"], c.want)
		}
	}
}

// TestKnownFunctionsUnaffected: the check must not cost a working query. Mixed
// case and alias spellings resolve through the same registry lookup the error
// path consults, so they are the cases most likely to break if that lookup is
// ever made stricter than case-insensitive.
func TestKnownFunctionsUnaffected(t *testing.T) {
	ctx, db := unknownFuncDB(t)
	for _, sql := range []string{
		`SELECT upper(n_name) AS v FROM nation`,
		`SELECT UPPER(n_name) AS v FROM nation`,
		`SELECT UpPeR(n_name) AS v FROM nation`,
		`SELECT substr(n_name, 1, 3) AS v FROM nation`,
		`SELECT substring(n_name, 1, 3) AS v FROM nation`,
		`SELECT length(n_name) AS v FROM nation`,
		`SELECT len(n_name) AS v FROM nation`,
		`SELECT ceil(p_retailprice) AS v FROM nation`,
		`SELECT count(*) AS v FROM nation`,
		`SELECT sum(p_retailprice) AS v FROM nation`,
		`SELECT n_name FROM nation WHERE upper(n_name) = 'ALGERIA'`,
		`SELECT count(*) AS v FROM nation GROUP BY substr(n_name, 1, 1)`,
	} {
		if _, err := db.Query(ctx, sql); err != nil {
			t.Errorf("%s: must succeed, got %v", sql, err)
		}
	}
}
