package physical

import (
	"context"
	"fmt"
	"strings"
	"testing"

	plansql "github.com/derekmwright/wadjet/internal/planner/sql"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// fakeCatalog is a minimal tableColumnSource for binder tests. A missing
// table answers the same sentinel the real catalog does (a confirmed miss);
// set `unreachable` to model a transport failure instead.
type fakeCatalog struct {
	tables      map[string][]string
	unreachable bool
}

func (f *fakeCatalog) GetTable(_ context.Context, name string) (*catalog.TableMeta, error) {
	if f.unreachable {
		return nil, fmt.Errorf("kv get: connection refused")
	}
	cols, ok := f.tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("table %q %w", name, catalog.ErrTableNotFound)
	}
	schema := parquet.Schema{}
	for _, c := range cols {
		col := parquet.Column{Name: c}
		// `attrs` is this fixture's ROW CONTAINER — the dotted-access cases
		// are about a FIELD PATH — and it has to SAY so. A column whose type
		// nobody sets reads as the zero TypeID, which is TypeBool, and the
		// binder now refuses field notation on a qualifier it can prove is
		// not composite (PostgreSQL's 42809). An untyped stub schema is not a
		// proof of anything, and letting one stand in for a container made
		// this fixture assert the opposite of what it says.
		// `amount` carries a real SCALAR type, so the fixture can drive the
		// refusal that says field notation does not apply to one — PostgreSQL's
		// 42809, whose message names the type.
		if strings.EqualFold(c, "amount") {
			col.Type = parquet.TypeDecimal
			col.Precision, col.Scale = 18, 4
		}
		if strings.EqualFold(c, "attrs") {
			col.Type = parquet.TypeRow
			col.Fields = []parquet.Column{
				{Name: "score", Type: parquet.TypeInt64, Nullable: true},
				{Name: "label", Type: parquet.TypeString, Nullable: true},
			}
		}
		schema.Columns = append(schema.Columns, col)
	}
	return &catalog.TableMeta{Name: name, Schema: schema}, nil
}

func mustExtract(t *testing.T, sql string) *plansql.SelectInfo {
	t.Helper()
	parsed, err := plansql.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	info, err := plansql.ExtractSelect(parsed)
	if err != nil {
		t.Fatalf("extract %q: %v", sql, err)
	}
	return info
}

func TestValidateColumns(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{
		"events": {"id", "ts", "attrs", "region", "amount"},
		"other":  {"eid", "val"},
	}}

	tests := []struct {
		name    string
		sql     string
		wantErr bool
	}{
		// --- Issue #147 core targets ---
		{"where typo", "SELECT id FROM events WHERE nosuchcol > 90", true},
		{"select typo (legacy projection path)", "SELECT nosuchcol FROM events", true},
		{"dotted ROW access resolves", "SELECT attrs.score FROM events", false},
		{"where valid", "SELECT id FROM events WHERE id > 90", false},

		// --- qualified references ---
		{"qualified typo on known table", "SELECT events.nosuchcol FROM events", true},
		{"qualified valid", "SELECT events.id FROM events", false},
		{"alias qualified valid", "SELECT e.id FROM events e", false},
		{"alias qualified typo", "SELECT e.nosuchcol FROM events e", true},

		// --- clauses ---
		{"group by typo", "SELECT region FROM events GROUP BY nope", true},
		{"group by valid", "SELECT region FROM events GROUP BY region", false},
		{"group by alias", "SELECT id AS x FROM events GROUP BY x", false},
		{"group by ordinal", "SELECT id, count(*) FROM events GROUP BY 1", false},
		{"order by typo", "SELECT id FROM events ORDER BY nope", true},
		{"order by alias", "SELECT id AS x FROM events ORDER BY x", false},
		{"order by table col not selected", "SELECT id FROM events ORDER BY ts", false},
		{"having typo", "SELECT region, count(*) c FROM events GROUP BY region HAVING nope > 5", true},
		{"having valid", "SELECT region, count(*) c FROM events GROUP BY region HAVING count(*) > 5", false},
		{"agg arg typo", "SELECT sum(nosuchcol) FROM events", true},
		{"agg arg valid", "SELECT sum(amount) FROM events", false},

		// --- joins / multi-table ---
		{"join bare cols valid", "SELECT id, val FROM events JOIN other ON id = eid", false},
		{"join qualified valid", "SELECT a.id, b.val FROM events a JOIN other b ON a.id = b.eid", false},
		{"join bare typo", "SELECT id, badcol FROM events a JOIN other b ON a.id = b.eid", true},
		{"join condition typo", "SELECT a.id FROM events a JOIN other b ON a.id = b.nope", true},
		{"join condition valid", "SELECT a.id FROM events a JOIN other b ON a.id = b.eid", false},
		{"join condition subquery typo", "SELECT a.id FROM events a JOIN other b ON a.id = b.eid AND a.amount > (SELECT max(nope) FROM other)", true},

		// --- unknown table (#367: a confirmed catalog miss is 42P01, no
		// longer an open scope; the transport-failure escape hatch is
		// TestValidateUnreachableCatalogStaysOpen) ---
		{"unregistered table errors", "SELECT anything FROM mystery_table", true},
		{"unregistered table qualified errors", "SELECT m.anything FROM mystery_table m", true},

		// --- CTEs ---
		{"cte output valid", "WITH c AS (SELECT id FROM events) SELECT id FROM c", false},
		{"cte output typo", "WITH c AS (SELECT id FROM events) SELECT nosuchcol FROM c", true},
		{"cte body typo", "WITH c AS (SELECT nosuchcol FROM events) SELECT id FROM c", true},
		{"cte explicit columns", "WITH c(x) AS (SELECT id FROM events) SELECT x FROM c", false},

		// --- derived tables ---
		{"derived output valid", "SELECT id FROM (SELECT id FROM events) d", false},
		{"derived output typo", "SELECT nosuchcol FROM (SELECT id FROM events) d", true},
		{"derived body typo", "SELECT id FROM (SELECT nosuchcol FROM events) d", true},
		{"derived star is open", "SELECT nosuchcol FROM (SELECT * FROM events) d", false},

		// --- subqueries / correlation ---
		{"correlated outer ref ok", "SELECT id FROM events e WHERE EXISTS (SELECT 1 FROM other o WHERE o.eid = e.id)", false},
		{"subquery internal typo", "SELECT id FROM events WHERE id IN (SELECT nosuchcol FROM other)", true},

		// --- star / table-less ---
		{"select star", "SELECT * FROM events", false},
		{"select star plus valid", "SELECT *, region FROM events", false},
		{"table-less select literal", "SELECT 1 + 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := mustExtract(t, tt.sql)
			err := validateColumns(context.Background(), cat, info)
			if tt.wantErr && err == nil {
				t.Errorf("expected error for %q, got nil", tt.sql)
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error for %q: %v", tt.sql, err)
			}
		})
	}
}

func TestValidateColumnsErrorNamesColumn(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{"events": {"id", "ts"}}}
	info := mustExtract(t, "SELECT id FROM events WHERE typocol > 1")
	err := validateColumns(context.Background(), cat, info)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "typocol") {
		t.Errorf("error should name the column, got: %v", err)
	}
}

// TestValidateRejections pins the plan-time refusals of #367 and #380: each
// statement PostgreSQL refuses gets an error carrying PostgreSQL's SQLSTATE
// and naming the offender — never a silent answer. The lookalike rows pin the
// legitimate shapes each refusal must NOT catch.
func TestValidateRejections(t *testing.T) {
	cat := &fakeCatalog{tables: map[string][]string{
		"events": {"id", "ts", "attrs", "region", "amount"},
		"other":  {"eid", "val"},
		"dup":    {"id", "score"},
		"zeek":   {"id.orig_h", "uid"},
	}}

	tests := []struct {
		name      string
		sql       string
		wantState string // "" = must validate cleanly
		wantIn    string // substring the error must contain (the offender)
	}{
		// --- #367: unknown table → 42P01 undefined_table ---
		{"unknown table", "SELECT * FROM no_such_table_here", "42P01", `"no_such_table_here"`},
		{"unknown table in join", "SELECT id FROM events JOIN nope ON id = x", "42P01", `"nope"`},
		{"unknown table in subquery", "SELECT id FROM events WHERE id IN (SELECT x FROM nope)", "42P01", `"nope"`},
		{"unknown table in cte body", "WITH c AS (SELECT x FROM nope) SELECT 1 FROM c", "42P01", `"nope"`},

		// --- #380: qualifier naming no FROM entry → 42P01 missing FROM-clause entry ---
		{"undefined alias in where", "SELECT t0.id FROM events t0 WHERE t1.eid BETWEEN 10 AND 150", "42P01", `"t1"`},
		{"undefined alias in select", "SELECT t1.id FROM events t0", "42P01", `"t1"`},
		{"table name qualifier when aliased", "SELECT events.id FROM events e", "42P01", `"events"`},

		// #380 lookalikes that must keep validating:
		{"delimited dotted name", `SELECT "id.orig_h" FROM zeek`, "", ""},
		{"row field path", "SELECT attrs.score FROM events", "", ""},
		// --- round-4 review P1: field notation on a qualifier that is
		// PROVABLY not composite is 42809, not one NULL per row ---
		{"field notation on a scalar", "SELECT amount.x FROM events", "42809", "not a composite type"},
		{"field notation on a scalar in where", "SELECT id FROM events WHERE amount.x > 1", "42809", "numeric"},
		// …and the container beside it still resolves, which is what says the
		// refusal reads the DECLARATION rather than the spelling.
		{"row field path beside it", "SELECT attrs.score, amount FROM events", "", ""},
		{"row field in where", "SELECT id FROM events WHERE attrs.score > 1", "", ""},
		{"correlated outer alias", "SELECT id FROM events e WHERE EXISTS (SELECT 1 FROM other o WHERE o.eid = e.id)", "", ""},
		{"correlated outer alias two deep", "SELECT id FROM events e WHERE EXISTS (SELECT 1 FROM other o WHERE EXISTS (SELECT 1 FROM dup d WHERE d.id = e.id AND d.score = o.val))", "", ""},
		{"lateral sees left alias", "SELECT e.id FROM events e JOIN LATERAL (SELECT o.val FROM other o WHERE o.eid = e.id) l ON 1 = 1", "", ""},

		// --- #367: ambiguous unqualified column → 42702 ---
		{"ambiguous across join", "SELECT id FROM events a JOIN dup b ON a.id = b.id", "42702", `"id"`},
		{"ambiguous self join", "SELECT id FROM events a JOIN events b ON a.id = b.id", "42702", `"id"`},
		{"ambiguous in where", "SELECT a.id FROM events a JOIN dup b ON a.id = b.id WHERE id > 1", "42702", `"id"`},
		{"ambiguous in order by", "SELECT a.id AS k FROM events a JOIN dup b ON a.id = b.id ORDER BY id", "42702", `"id"`},

		// 42702 lookalikes:
		{"qualified resolves ambiguity", "SELECT a.id, b.id FROM events a JOIN dup b ON a.id = b.id", "", ""},
		{"unique bare col across join", "SELECT region, score FROM events a JOIN dup b ON a.id = b.id", "", ""},
		{"output alias shadows ambiguous input", "SELECT a.id AS id FROM events a JOIN dup b ON a.id = b.id ORDER BY id", "", ""},
		{"inner shadows outer silently", "SELECT id FROM events e WHERE EXISTS (SELECT id FROM dup)", "", ""},

		// --- #367: bare column beside aggregate, no GROUP BY → 42803 ---
		{"bare column beside aggregate", "SELECT region, count(*) FROM events", "42803", `"region"`},
		{"bare column in expression beside aggregate", "SELECT region || 'x', count(*) FROM events", "42803", `"region"`},
		{"qualified column beside aggregate", "SELECT e.region, max(e.amount) FROM events e", "42803", `"e.region"`},

		// 42803 lookalikes:
		{"grouped column beside aggregate", "SELECT region, count(*) FROM events GROUP BY region", "", ""},
		{"group by ordinal", "SELECT region, count(*) FROM events GROUP BY 1", "", ""},
		{"literal beside aggregate", "SELECT 1 + 1, count(*) FROM events", "", ""},
		{"window beside bare column", "SELECT region, count(*) OVER () FROM events", "", ""},
		{"aggregate only", "SELECT count(*), max(amount) - min(amount) FROM events", "", ""},
		{"scalar subquery beside aggregate", "SELECT (SELECT max(val) FROM other), count(*) FROM events", "", ""},
		{"outer ref beside aggregate in subquery", "SELECT id FROM events e WHERE amount > (SELECT max(o.val) FROM other o WHERE o.eid = e.id)", "", ""},

		// --- undefined column now carries 42703 ---
		{"unknown column carries 42703", "SELECT no_such_column FROM events", "42703", `"no_such_column"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := mustExtract(t, tt.sql)
			err := validateColumns(context.Background(), cat, info)
			if tt.wantState == "" {
				if err != nil {
					t.Fatalf("expected %q to validate, got: %v", tt.sql, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("expected a %s rejection for %q, got nil — the statement would be answered silently", tt.wantState, tt.sql)
			}
			if got := sqlerr.StateOf(err); got != tt.wantState {
				t.Errorf("SQLSTATE = %q, want %s (err: %v)", got, tt.wantState, err)
			}
			if tt.wantIn != "" && !strings.Contains(err.Error(), tt.wantIn) {
				t.Errorf("error must name the offender %s, got: %v", tt.wantIn, err)
			}
		})
	}
}

// TestValidateUnreachableCatalogStaysOpen pins the boundary of the 42P01
// rejection: when the catalog cannot be REACHED, the table's existence is
// unknown and the binder must stay conservative rather than refuse a query
// that may be perfectly valid.
func TestValidateUnreachableCatalogStaysOpen(t *testing.T) {
	cat := &fakeCatalog{unreachable: true}
	info := mustExtract(t, "SELECT anything FROM events")
	if err := validateColumns(context.Background(), cat, info); err != nil {
		t.Fatalf("a transport failure must not reject the query, got: %v", err)
	}
}
