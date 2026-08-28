package physical

import (
	"context"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/catalog"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// typedCatalog is a tableColumnSource that carries a declared parquet.TypeID
// per column — the fakeCatalog in validate_test.go builds untyped columns, and
// the plan-time literal refusal turns on the DECLARED type.
type typedCatalog struct {
	tables map[string][]parquet.Column
}

func (c *typedCatalog) GetTable(_ context.Context, name string) (*catalog.TableMeta, error) {
	cols, ok := c.tables[strings.ToLower(name)]
	if !ok {
		return nil, catalog.ErrTableNotFound
	}
	return &catalog.TableMeta{Name: name, Schema: parquet.Schema{Columns: cols}}, nil
}

func netCatalog() *typedCatalog {
	return &typedCatalog{tables: map[string][]parquet.Column{
		"net": {
			{Name: "id", Type: parquet.TypeInt64},
			{Name: "c", Type: parquet.TypeCIDR},
			{Name: "v6", Type: parquet.TypeIPv6},
			{Name: "v4", Type: parquet.TypeIPv4},
			{Name: "m", Type: parquet.TypeMAC},
			{Name: "u", Type: parquet.TypeUUID},
			{Name: "amt", Type: parquet.TypeDecimal, Precision: 18, Scale: 2},
			{Name: "name", Type: parquet.TypeString},
		},
	}}
}

// TestValidateLiteralTypeRefusal covers the #579 mechanism: colScope now
// carries each column's parquet.TypeID (not a bare isDecimal bool), and
// refuseLiteralForType dispatches per type. Today only DECIMAL has a rule.
//
// It pins the DECIMAL half byte-for-byte — the pre-existing plan-time refusal
// (#517) must keep firing through the widened mechanism at both the direct
// comparison and the boxed sites (simple CASE, GREATEST/LEAST, IN) — and pins
// the no-over-reach half: a valid DECIMAL literal, a string column, and a
// column-vs-column comparison must all still validate.
func TestValidateLiteralTypeRefusal(t *testing.T) {
	cat := netCatalog()

	tests := []struct {
		name      string
		sql       string
		wantState string // "" = must validate cleanly
		wantIn    string // substring the error must contain
	}{
		// --- DECIMAL still refuses, through the TypeID mechanism. ---
		{"decimal eq bad literal", "SELECT count(*) FROM net WHERE amt = 'abc'", "22P02", "numeric"},
		{"decimal parenthesized column", "SELECT count(*) FROM net WHERE (amt) = 'abc'", "22P02", "numeric"},
		{"decimal simple case bad literal", "SELECT CASE amt WHEN 'abc' THEN 1 ELSE 0 END FROM net", "22P02", "numeric"},
		{"decimal greatest bad literal", "SELECT GREATEST(amt, 'abc') FROM net", "22P02", "numeric"},
		{"decimal in bad literal", "SELECT count(*) FROM net WHERE amt IN ('abc', 'x')", "22P02", "numeric"},

		// --- No over-reach: valid literals and non-literal comparisons work. ---
		{"decimal eq valid", "SELECT count(*) FROM net WHERE amt = '12.75'", "", ""},
		{"decimal eq large numeric", "SELECT count(*) FROM net WHERE amt = '1e400'", "", ""},
		{"string column any literal", "SELECT count(*) FROM net WHERE name = 'zzz'", "", ""},
		{"decimal eq column no literal", "SELECT count(*) FROM net WHERE amt = id", "", ""},
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
				t.Errorf("error must mention %q, got: %v", tt.wantIn, err)
			}
		})
	}
}

// TestPlanTimeNeverRefusesPGValidNetworkLiteral is the guard against the
// PG-superset regression the first cut of #579 introduced: wadjet's network
// literal parsers are STRICTER than PostgreSQL's input grammar, so refusing a
// network literal at plan time on those parsers raised 22P02 for input
// PostgreSQL ACCEPTS — abbreviated cidr/inet, alternate macaddr notations, and
// brace/no-dash/uppercase UUIDs — and did so net-new at the boxed sites
// (GREATEST/LEAST, simple CASE, IN, IS DISTINCT FROM) that had no refusal
// before. ADR-0012 item 1 forbids ever refusing what PostgreSQL accepts.
//
// Until #627 widens those parsers to a SUPERSET of PostgreSQL's grammar, the
// network types carry NO plan-time literal refusal. Every literal below is
// valid in PostgreSQL and must validate cleanly at EVERY site; a future
// network arm in refuseLiteralForType that reintroduces the strictness fails
// here before it can reach a user.
func TestPlanTimeNeverRefusesPGValidNetworkLiteral(t *testing.T) {
	cat := netCatalog()

	// column + a PG-valid literal that wadjet's own network parser rejects.
	boundary := []struct{ col, lit string }{
		// Abbreviated cidr/inet — PostgreSQL infers the octets/mask.
		{"c", "192.168"},
		{"c", "10"},
		{"c", "192.168.1"},
		{"c", "10/8"},
		{"c", "192.168/16"},
		{"v4", "192.168"},
		{"v6", "::ffff:1.2.3.4"},
		// macaddr notations PostgreSQL accepts.
		{"m", "08002b:010203"},
		{"m", "08002b-010203"},
		{"m", "0800-2b01-0203"},
		{"m", "08-00-2b-01-02-03"},
		// UUID brace / no-dash / uppercase forms PostgreSQL accepts.
		{"u", "{a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11}"},
		{"u", "a0eebc999c0b4ef8bb6d6bb9bd380a11"},
		{"u", "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11"},
	}

	// Each site is a template with two positional slots: the column, then the
	// quoted literal. It must validate cleanly for every boundary literal.
	sites := []struct{ name, tmpl string }{
		{"where_eq", "SELECT count(*) FROM net WHERE @c = @l"},
		{"where_is_distinct_from", "SELECT count(*) FROM net WHERE @c IS DISTINCT FROM @l"},
		{"where_in", "SELECT count(*) FROM net WHERE @c IN (@l)"},
		{"simple_case", "SELECT CASE @c WHEN @l THEN 1 ELSE 0 END FROM net"},
		{"greatest", "SELECT GREATEST(@c, @l) FROM net"},
		{"least", "SELECT LEAST(@c, @l) FROM net"},
	}

	for _, b := range boundary {
		for _, s := range sites {
			t.Run(s.name+"_"+b.col+"_"+b.lit, func(t *testing.T) {
				sql := strings.NewReplacer("@c", b.col, "@l", "'"+b.lit+"'").Replace(s.tmpl)
				info := mustExtract(t, sql)
				if err := validateColumns(context.Background(), cat, info); err != nil {
					t.Fatalf("plan-time refused PG-valid literal %q at %q: %v", b.lit, sql, err)
				}
			})
		}
	}
}
