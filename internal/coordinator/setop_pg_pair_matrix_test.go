package coordinator

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// Every ORDERED PAIR of the type matrix, against PostgreSQL's verdict (#648,
// round 2).
//
// The no-common-type rule was a hand-written list of exempted pairs once, and
// a hand-written list is checked by a hand-written gate — which is how twenty-
// one shapes PostgreSQL ANSWERS became a plan-time 42804 with every cell of
// that gate green. This one is GENERATED from both ends: the columns come from
// `typematrix.Columns()`, so a type added to the matrix is added here, and the
// expectation comes from `setOpPGPairVerdict`, 324 cells measured on live
// PostgreSQL 17.11 (see that file's header for the script and the schema).
//
// Per PAIR the gate asserts a DISPOSITION, in three classes:
//
//	PostgreSQL ANSWERS and wadjet carries it   -> every arm answers
//	PostgreSQL REFUSES                          -> every arm refuses with
//	                                               "types X and Y cannot be
//	                                               matched" and, on the
//	                                               single-process path, 42804
//	PostgreSQL ANSWERS and wadjet has no
//	  CARRIER for the two arms' files           -> every arm refuses with the
//	                                               carrier message, and NEVER
//	                                               with PostgreSQL's SQLSTATE
//
// The third class is exactly DATE ∪ TIMESTAMP and the IPv4/IPv6/CIDR family,
// and it is listed here rather than derived, so a pair that leaves or joins it
// fails this test.
func TestEverySetOperationTypePairTakesPostgresVerdict(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: this gate stands up an embedded NATS cluster")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)

	single := tmdStandalone(t, ctx)
	infra := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infra, nil)
	coord := tmdCoordinator(t, ctx, infra)

	flat := make([]typematrix.Col, 0, 18)
	for _, c := range typematrix.Columns() {
		if c.Flat {
			flat = append(flat, c)
		}
	}
	if len(flat)*len(flat) != len(setOpPGPairVerdict) {
		t.Fatalf("the fixture has %d flat types (%d ordered pairs) and the measured table has %d "+
			"cells — regenerate it with scratchpad genmatrix.sh against live PostgreSQL rather "+
			"than editing either by hand", len(flat), len(flat)*len(flat), len(setOpPGPairVerdict))
	}

	for _, a := range flat {
		for _, b := range flat {
			key := a.Name + "|" + b.Name
			want, ok := setOpPGPairVerdict[key]
			if !ok {
				t.Errorf("no measured PostgreSQL verdict for %s", key)
				continue
			}
			t.Run(key, func(t *testing.T) {
				sql := fmt.Sprintf(
					"SELECT %s AS v FROM typemx WHERE id < 3 UNION ALL SELECT %s FROM typemx WHERE id < 3",
					a.Name, b.Name)
				class := setOpPairClass(a.Type, b.Type, want)
				for _, arm := range []struct {
					name string
					run  func(string) (*oracle.Result, error)
					co   *Coordinator
				}{
					{"single", func(q string) (*oracle.Result, error) { return tmdRunSingle(ctx, single, q) }, nil},
					{"dag", func(q string) (*oracle.Result, error) { return tmdRunDAG(ctx, coord, q) }, coord},
				} {
					var before a2Routes
					if arm.co != nil {
						before = a2ReadRoutes(arm.co)
					}
					res, err := arm.run(sql)
					if arm.co != nil {
						a2CheckRoutes(t, arm.name, before, a2ReadRoutes(arm.co), a2Routes{}, sql)
					}
					switch class {
					case setOpPairAnswers:
						if err != nil {
							t.Errorf("%s arm REFUSED a pair PostgreSQL resolves as %s: %v\n  SQL: %s",
								arm.name, want, err, sql)
							continue
						}
						if len(res.Rows) != 6 {
							t.Errorf("%s arm returned %d rows, want 6\n  SQL: %s",
								arm.name, len(res.Rows), sql)
						}
					case setOpPairRefused:
						if err == nil {
							t.Errorf("%s arm ANSWERED %d rows for a pair PostgreSQL refuses\n  SQL: %s",
								arm.name, len(res.Rows), sql)
							continue
						}
						if !strings.Contains(err.Error(), "cannot be matched") {
							t.Errorf("%s arm refused with %q, want PostgreSQL's \"types X and Y "+
								"cannot be matched\"\n  SQL: %s", arm.name, err.Error(), sql)
						}
					case setOpPairNoCarrier:
						if err == nil {
							t.Errorf("%s arm ANSWERED a pair this engine has no carrier for — if "+
								"the carrier now exists, move the pair out of setOpPairNoCarrierPairs "+
								"and assert the values\n  SQL: %s", arm.name, sql)
							continue
						}
						if !strings.Contains(err.Error(), "no common carrier") {
							t.Errorf("%s arm refused with %q, want the carrier message\n  SQL: %s",
								arm.name, err.Error(), sql)
						}
						if strings.Contains(err.Error(), "cannot be matched") {
							t.Errorf("%s arm claims PostgreSQL refuses a pair it resolves as %s\n"+
								"  SQL: %s", arm.name, want, sql)
						}
					}
				}
			})
		}
	}
}

type setOpPairDisposition int

const (
	setOpPairAnswers setOpPairDisposition = iota
	setOpPairRefused
	setOpPairNoCarrier
)

// setOpPairNoCarrierPairs is the third class, LISTED rather than derived: the
// pairs PostgreSQL resolves and this engine cannot concatenate. A pair that
// leaves it (because a DATE → TIMESTAMP promotion or an inet-family carrier
// landed) or joins it fails the matrix above, which is the point.
var setOpPairNoCarrierPairs = map[[2]parquet.TypeID]bool{
	{parquet.TypeDate, parquet.TypeTimestamp}: true,
	{parquet.TypeTimestamp, parquet.TypeDate}: true,
	{parquet.TypeIPv4, parquet.TypeIPv6}:      true,
	{parquet.TypeIPv6, parquet.TypeIPv4}:      true,
	{parquet.TypeIPv4, parquet.TypeCIDR}:      true,
	{parquet.TypeCIDR, parquet.TypeIPv4}:      true,
	{parquet.TypeIPv6, parquet.TypeCIDR}:      true,
	{parquet.TypeCIDR, parquet.TypeIPv6}:      true,
	// A wire-declared integer meeting DECIMAL or REAL: PostgreSQL resolves
	// numeric and real, and this engine has no coercion that moves a PORT,
	// PROTOCOL or DURATION vector into either. Against BIGINT and DOUBLE
	// PRECISION it does, which is why those pairs are not here.
	{parquet.TypePort, parquet.TypeDecimal}:     true,
	{parquet.TypeDecimal, parquet.TypePort}:     true,
	{parquet.TypePort, parquet.TypeFloat32}:     true,
	{parquet.TypeFloat32, parquet.TypePort}:     true,
	{parquet.TypeProtocol, parquet.TypeDecimal}: true,
	{parquet.TypeDecimal, parquet.TypeProtocol}: true,
	{parquet.TypeProtocol, parquet.TypeFloat32}: true,
	{parquet.TypeFloat32, parquet.TypeProtocol}: true,
	{parquet.TypeDuration, parquet.TypeDecimal}: true,
	{parquet.TypeDecimal, parquet.TypeDuration}: true,
	{parquet.TypeDuration, parquet.TypeFloat32}: true,
	{parquet.TypeFloat32, parquet.TypeDuration}: true,
}

func setOpPairClass(a, b parquet.TypeID, pgVerdict string) setOpPairDisposition {
	if pgVerdict == "ERR" {
		return setOpPairRefused
	}
	if setOpPairNoCarrierPairs[[2]parquet.TypeID{a, b}] {
		return setOpPairNoCarrier
	}
	return setOpPairAnswers
}
