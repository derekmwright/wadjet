package wadjet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/scan"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Row-group pruning must never change an answer. It is an optimization whose
// entire contract is "skip a row group that provably holds no matching row",
// and the row groups it skips are invisible: the query returns fewer rows and
// nothing says a decision was made.
//
// Three columns broke that contract, and all three broke it the same way
// (#442, and #438 which is the same defect seen through a DECIMAL): the prune
// layer compares two `any` values by their Go KIND, the footer's bounds are in
// the FILE's representation and the literal is in the ENGINE's, and for
// DECIMAL, IPV6 and UUID those are different things that land in the same
// kind. `c_ipv6 >= '2001:db8::5'` returned 0 rows against 4914 matching ones,
// because '2' (0x32) sorts above every byte of a 2001:db8:: address.
//
// This is the audit as a gate rather than as prose: every flat type, every
// comparison operator, a literal taken from a row the fixture actually holds,
// answered with pruning on and with pruning off. It is the shape no other gate
// has — the two-path suite and the DuckDB fingerprint both run the same prune
// on both sides and agree on the wrong answer.
func TestTypeMatrixPruningNeverChangesTheAnswer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping type-matrix gate under -short — the dedicated CI step runs it without -short")
	}
	ctx := context.Background()
	db := tmOpen(t)

	// A row in the middle of the fixture, so a literal taken from it splits
	// the row groups rather than falling outside every bound.
	const probeRow = 1500
	rows := typematrix.Data(typematrix.Rows)
	if len(rows) <= probeRow {
		t.Fatalf("fixture has %d rows, need more than %d", len(rows), probeRow)
	}
	// Each column NULLs on its own stride, so the probe row is chosen per
	// column: the first row at or after probeRow that actually holds a value.
	valueFor := func(name string) any {
		for i := probeRow; i < len(rows); i++ {
			if v, ok := rows[i][name]; ok && v != nil {
				return v
			}
		}
		return nil
	}

	prevStats := scan.StatsPrune.Set(true)
	prevDict := scan.DictPrune.Set(true)
	t.Cleanup(func() {
		scan.StatsPrune.Set(prevStats)
		scan.DictPrune.Set(prevDict)
	})

	count := func(t *testing.T, sql string, prune bool) (int64, bool) {
		t.Helper()
		scan.StatsPrune.Set(prune)
		scan.DictPrune.Set(prune)
		res, err := tmRun(ctx, db, sql)
		if err != nil {
			return 0, false
		}
		if len(res.Rows) != 1 || len(res.Columns) != 1 {
			t.Fatalf("%s: want one row of one column, got %d rows and %d columns",
				sql, len(res.Rows), len(res.Columns))
		}
		n, ok := tmAsInt64(res.Rows[0][res.Columns[0]])
		if !ok {
			t.Fatalf("%s: COUNT(*) came back as %#v", sql, res.Rows[0][res.Columns[0]])
		}
		return n, true
	}

	checked, skipped := 0, 0
	for _, c := range typematrix.Columns() {
		if !c.Flat {
			continue // a container has no scalar bound to prune on
		}
		v := valueFor(c.Name)
		lit, ok := tmSQLLiteral(c.Type, v)
		if !ok {
			t.Fatalf("column %s: the fixture value %#v has no SQL literal form — "+
				"the sweep would silently skip the type", c.Name, v)
		}
		for _, op := range []string{"=", "<>", "<", "<=", ">", ">="} {
			sql := fmt.Sprintf("SELECT COUNT(*) AS n FROM %s WHERE %s %s %s",
				typematrix.Table, c.Name, op, lit)
			t.Run(c.Name+"_"+tmOpName(op), func(t *testing.T) {
				off, okOff := count(t, sql, false)
				if !okOff {
					skipped++
					t.Skipf("the engine does not answer this shape: %s", sql)
				}
				on, okOn := count(t, sql, true)
				if !okOn {
					t.Fatalf("pruning turned an answerable query into an error: %s", sql)
				}
				checked++
				if on != off {
					t.Errorf("PRUNING CHANGED THE ANSWER\n  SQL: %s\n  prune on  = %d\n  prune off = %d",
						sql, on, off)
				}
			})
		}
	}
	t.Logf("prune sweep: %d predicates compared, %d shapes unanswerable", checked, skipped)
	if checked == 0 {
		t.Fatal("no predicate was compared — the sweep proves nothing")
	}
}

// tmSQLLiteral renders a fixture value as the SQL literal a query would carry.
// A type with no rendering here is a FAILURE, not a skip: the point of the
// sweep is that no type is missing from it.
func tmSQLLiteral(typ parquet.TypeID, v any) (string, bool) {
	switch typ {
	case parquet.TypeBool:
		b, ok := v.(bool)
		return strconv.FormatBool(b), ok
	case parquet.TypeInt32, parquet.TypePort, parquet.TypeProtocol:
		switch tv := v.(type) {
		case int32:
			return strconv.FormatInt(int64(tv), 10), true
		case int64:
			return strconv.FormatInt(tv, 10), true
		}
	case parquet.TypeInt64, parquet.TypeTimestamp, parquet.TypeDuration:
		n, ok := v.(int64)
		return strconv.FormatInt(n, 10), ok
	case parquet.TypeFloat32:
		f, ok := v.(float32)
		return strconv.FormatFloat(float64(f), 'f', -1, 32), ok
	case parquet.TypeFloat64, parquet.TypeDecimal:
		f, ok := v.(float64)
		return strconv.FormatFloat(f, 'f', -1, 64), ok
	case parquet.TypeString, parquet.TypeIPv4, parquet.TypeIPv6,
		parquet.TypeCIDR, parquet.TypeMAC, parquet.TypeUUID, parquet.TypeDate:
		s, ok := v.(string)
		return "'" + strings.ReplaceAll(s, "'", "''") + "'", ok
	case parquet.TypeBytes:
		b, ok := v.([]byte)
		return "'" + strings.ReplaceAll(string(b), "'", "''") + "'", ok
	}
	return "", false
}

func tmOpName(op string) string {
	switch op {
	case "=":
		return "eq"
	case "<>":
		return "ne"
	case "<":
		return "lt"
	case "<=":
		return "le"
	case ">":
		return "gt"
	}
	return "ge"
}

func tmAsInt64(v any) (int64, bool) {
	switch tv := v.(type) {
	case int64:
		return tv, true
	case int32:
		return int64(tv), true
	case int:
		return int64(tv), true
	case float64:
		return int64(tv), true
	}
	return 0, false
}
