package coordinator

import (
	"context"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// The QUALIFIED-SIDE fixture (#706): two tables carrying the SAME column name
// at DIFFERENT (p,s), with values that differ per row and one that does not
// FIT the narrower declaration.
//
// It rides along in tmdTables() rather than standing up a cluster of its own,
// the way every other fixture in this package does. The existing cross-scale
// pair (`setopdecja` / `setopdecjb`) cannot stand in for it: both of its rows
// carry the SAME NUMBER at whichever scale the column declares, on purpose, so
// a reference resolved against the WRONG side's declaration renders exactly
// the same digits and the fixture cannot fail. That is the can't-fail shape
// the correctness protocol's method 2 names, and #706 is invisible to it.
//
// zzj's second row is 12345678.1234, which needs ten digits before the point
// is even considered: read under zzp's DECIMAL(9,2) it is 22003, which is the
// loud face of the defect. The other rows differ at the FOURTH decimal, which
// is the silent one — right digits under the wrong scale.
const (
	zzpTable = "zzp"
	zzjTable = "zzj"
)

func zzpSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d92", Type: parquet.TypeDecimal, Precision: 9, Scale: 2, Nullable: true},
	}}
}

func zzjSchema() parquet.Schema {
	return parquet.Schema{Columns: []parquet.Column{
		{Name: "id", Type: parquet.TypeInt64},
		{Name: "d92", Type: parquet.TypeDecimal, Precision: 18, Scale: 4, Nullable: true},
	}}
}

func zzpData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "d92": dbpDec(-350)},
		{"id": int64(2), "d92": dbpDec(0)},
		{"id": int64(3), "d92": dbpDec(1275)},
	}
}

func zzjData() []map[string]any {
	return []map[string]any{
		{"id": int64(1), "d92": dbpDec(11111)},
		{"id": int64(2), "d92": dbpDec(123456781234)},
		{"id": int64(3), "d92": dbpDec(33333)},
	}
}

// A QUALIFIED reference to a name BOTH join sides carry resolves through the
// side its qualifier names, on three arms against PostgreSQL 17 (#706).
//
// `zzp.d92` is DECIMAL(9,2) and `zzj.d92` is DECIMAL(18,4). The single-process
// path took the VALUE through the qualified spelling — `expr.ResolveColumnRef`
// keeps the qualifier — and the DECLARATION through the BARE name, which is
// the first `d92` in the joined stream and therefore the OTHER arm's column.
// One output described by two sides:
//
//	SELECT z.d92 FROM zzp t JOIN zzj z ON t.id = z.id
//	-- PostgreSQL 1.1111 · 12345678.1234 · 3.3333
//	-- single    22003: 1.1111 does not fit a DECIMAL at scale 2
//
//	SELECT t.d92 FROM zzj z JOIN zzp t ON t.id = z.id
//	-- PostgreSQL -3.50 · 0.00 · 12.75 · single  -3.5000 · 0.0000 · 12.7500
//
// The loud face and the silent one are one defect seen from either end, which
// is why the fixture carries a value that does NOT FIT the narrower scale
// beside two that merely render differently: a fixture with only the second
// kind cannot tell a wrong declaration from a right one at a glance, and one
// where both arms hold the SAME number cannot tell anything at all — the
// cross-scale pair already in this package is exactly that.
//
// The DAG arms were correct throughout — `QualifyAllBuildCols` renames a build
// arm's columns, so the two never share a bare name there — and are asserted
// rather than skipped, so a repair that breaks them fails here.
func TestQualifiedReferenceResolvesThroughItsOwnSideThreeArms(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up two embedded NATS clusters")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	t.Cleanup(cancel)
	arms := dajArms(t, ctx)

	for _, tc := range []struct {
		name, sql string
		cols      []string
		want      string
	}{
		{
			name: "build-side-reference-narrow-arm-first",
			sql:  "SELECT z.d92 AS d FROM zzp t JOIN zzj z ON t.id = z.id ORDER BY z.id",
			cols: []string{"d"},
			want: "3 rows: 1.1111;12345678.1234;3.3333;",
		},
		{
			name: "probe-side-reference-wide-arm-first",
			sql:  "SELECT t.d92 AS d FROM zzj z JOIN zzp t ON t.id = z.id ORDER BY t.id",
			cols: []string{"d"},
			want: "3 rows: -3.50;0.00;12.75;",
		},
		{
			name: "probe-side-reference-narrow-arm-first",
			sql:  "SELECT t.d92 AS d FROM zzp t JOIN zzj z ON t.id = z.id ORDER BY t.id",
			cols: []string{"d"},
			want: "3 rows: -3.50;0.00;12.75;",
		},
		{
			name: "build-side-reference-wide-arm-first",
			sql:  "SELECT z.d92 AS d FROM zzj z JOIN zzp t ON t.id = z.id ORDER BY z.id",
			cols: []string{"d"},
			want: "3 rows: 1.1111;12345678.1234;3.3333;",
		},
		{
			// BOTH projected at once, so neither can borrow the other's
			// declaration without the pair disagreeing.
			name: "both-sides-projected",
			sql:  "SELECT t.d92 AS td, z.d92 AS zd FROM zzp t JOIN zzj z ON t.id = z.id ORDER BY t.id",
			cols: []string{"td", "zd"},
			want: "3 rows: -3.50|1.1111;0.00|12345678.1234;12.75|3.3333;",
		},
		{
			// A THIRD relation in the join, which the filing reports as
			// flipping the answer back — order dependence is the tell of a
			// resolution reading position rather than the qualifier.
			name: "with-a-third-relation-joined",
			sql: "SELECT z.d92 AS d FROM zzp t JOIN zzj z ON t.id = z.id " +
				"JOIN " + dbpTable + " u ON u.id = t.id ORDER BY z.id",
			cols: []string{"d"},
			want: "3 rows: 1.1111;12345678.1234;3.3333;",
		},
		{
			// The DERIVED spelling of the other arm: `d92 * 2` republishes
			// the name at a scale the base table does not have.
			name: "the-other-arm-is-a-derived-table",
			sql: "SELECT t.d92 AS d FROM zzp t JOIN (SELECT id, d92 * 2 AS d92 FROM zzp) q " +
				"ON t.id = q.id ORDER BY t.id",
			cols: []string{"d"},
			want: "3 rows: -3.50;0.00;12.75;",
		},
		{
			// An AGGREGATE over the qualified reference, which reads the
			// declaration through a different consumer.
			name: "aggregate-over-the-build-sides-column",
			sql:  "SELECT SUM(z.d92) AS s FROM zzp t JOIN zzj z ON t.id = z.id",
			cols: []string{"s"},
			want: "1 rows: 12345682.5678;",
		},
		{
			// The ZERO-ROW variant: nothing arrives, so the answer is
			// described from the plan alone.
			name: "zero-rows",
			sql:  "SELECT t.d92 AS d FROM zzp t JOIN zzj z ON t.id = z.id WHERE t.id < 0",
			cols: []string{"d"},
			want: "0 rows: ",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			for _, arm := range arms {
				res, err := arm.run(tc.sql)
				if err != nil {
					t.Fatalf("%s arm refused a query PostgreSQL 17 answers: %v\n  SQL: %s",
						arm.name, err, tc.sql)
				}
				if got := dajDigest(res, tc.cols); got != tc.want {
					t.Errorf("%s arm answered\n  %s\nPostgreSQL 17 answers\n  %s\n"+
						" — a qualified reference was declared by the OTHER arm\n  SQL: %s",
						arm.name, got, tc.want, tc.sql)
				}
			}
		})
	}
}
