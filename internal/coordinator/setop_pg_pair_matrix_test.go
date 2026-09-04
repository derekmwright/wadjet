package coordinator

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/oracle"
	"github.com/derekmwright/wadjet/internal/oracle/typematrix"
	"github.com/derekmwright/wadjet/internal/planner/physical"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/parquet"
	"github.com/derekmwright/wadjet/wadjet"
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
// The third class is `setOpPairNoCarrierPairs`, listed here rather than derived
// so a pair that leaves or joins it fails this test — and checked against the
// planner's own computed set by TestTheCarrierGapListIsTheCodes, so the list
// and the code cannot drift apart the way this comment once had.
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
	infraB := tmdInfra(t, ctx)
	tmdWriteTables(t, ctx, infraB, nil)
	coordB := tmdCoordinator(t, ctx, infraB, func(c *Config) { c.BroadcastBytesOverride = 1 })

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
				// The DECLARED type of the union's column, per arm. A value
				// comparison cannot see it, and a wrong one is a wrong OID on
				// the wire for a right value — the divergence the oracle's
				// wire arm exists for. Collected here and compared across the
				// arms below.
				decls := map[string]string{}
				for _, arm := range []struct {
					name string
					run  func(string) (string, *oracle.Result, error)
					co   *Coordinator
				}{
					{"single", func(q string) (string, *oracle.Result, error) {
						return tmdRunSingleDecl(ctx, single, q)
					}, nil},
					{"dag", func(q string) (string, *oracle.Result, error) {
						return tmdRunDAGDecl(ctx, coord, q)
					}, coord},
					{"dag-shuffled", func(q string) (string, *oracle.Result, error) {
						return tmdRunDAGDecl(ctx, coordB, q)
					}, coordB},
				} {
					rows := func(q string) (*oracle.Result, error) {
						_, r, err := arm.run(q)
						return r, err
					}
					var before a2Routes
					if arm.co != nil {
						before = a2ReadRoutes(arm.co)
					}
					decl, res, err := arm.run(sql)
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
						decls[arm.name] = decl
						// The VALUES, not the row count. A count cannot see a
						// column materialised in the FIRST arm's carrier —
						// `c_port ∪ 4000000000` came back as -294967296 with
						// six rows — which is exactly what hid the wrap.
						//
						// The reference is each arm's OWN spelling, run alone
						// and concatenated: UNION ALL is that multiset, moved
						// into the common type. Where the common type is a
						// FLOAT the move is LOSSY by design (real ∪ bigint is
						// real in PostgreSQL too), so the reference is put
						// through the same width before comparing.
						refA := setOpPairRefRows(t, rows, a.Name, want)
						refB := setOpPairRefRows(t, rows, b.Name, want)
						ref := append(append([]string(nil), refA...), refB...)
						sort.Strings(ref)
						got := setOpCanonRows(res)
						if strings.Join(got, " ") != strings.Join(ref, " ") {
							t.Errorf("%s arm's union is not its two arms' values\n  got  %v\n"+
								"  want %v (each arm alone, at the common type %s)\n  SQL: %s",
								arm.name, got, ref, want, sql)
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
						// What the CLIENT is told. PostgreSQL answers this
						// query, so the refusal is feature_not_supported and
						// not an internal error; unclassified, sqlerr.StateOf
						// answers "" and the pgwire door sends XX000, which
						// tells a driver the server broke.
						if arm.co == nil {
							if got := sqlerr.StateOf(err); got != "0A000" {
								t.Errorf("the carrier refusal carries SQLSTATE %q, want 0A000 "+
									"(feature_not_supported)\n  SQL: %s", got, sql)
							}
						}
					}
				}
				// One query, one declared type. Two doors that agree on every
				// VALUE and disagree on the type publish the same rows under
				// two different OIDs, which a row comparison cannot see: it is
				// how the leftmost-unknown-literal split survived a review
				// round with every cell of this matrix green.
				for _, arm := range []string{"dag", "dag-shuffled"} {
					if got, ok := decls[arm]; ok && got != decls["single"] {
						t.Errorf("the %s arm DECLARES %s where the single-process path declares %s "+
							"(PostgreSQL: %s)\n  SQL: %s", arm, got, decls["single"], want, sql)
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
	// A wire-declared integer meeting DECIMAL: PostgreSQL resolves numeric, and
	// this engine has no coercion that moves a PORT, PROTOCOL or DURATION
	// vector there — the DECIMAL rung reads an INT32/INT64 unscaled carrier.
	// The INTEGER and FLOAT rungs DO carry them, which is why only DECIMAL is
	// here.
	{parquet.TypePort, parquet.TypeDecimal}:     true,
	{parquet.TypeDecimal, parquet.TypePort}:     true,
	{parquet.TypeProtocol, parquet.TypeDecimal}: true,
	{parquet.TypeDecimal, parquet.TypeProtocol}: true,
	{parquet.TypeDuration, parquet.TypeDecimal}: true,
	{parquet.TypeDecimal, parquet.TypeDuration}: true,
}

// tmdRunSingleDecl / tmdRunDAGDecl are tmdRunSingle / tmdRunDAG with the
// result column's DECLARED type beside the rows — `v:DECIMAL(9,2)`, the same
// spelling on both doors, so the two are comparable as strings.
//
// A matrix that compares only VALUES cannot see a right value under a wrong
// OID, and that is not hypothetical here: the leftmost-unknown-literal split
// (round-3 blocker) had the single-process path publishing OID 25 for a column
// the DAG published as numeric, with every one of these 324 cells green.
func tmdRunSingleDecl(ctx context.Context, db *wadjet.DB, sql string) (
	decl string, res *oracle.Result, err error,
) {
	defer func() {
		if r := recover(); r != nil {
			decl, res, err = "", nil, fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := db.Query(ctx, sql)
	if qerr != nil {
		return "", nil, qerr
	}
	if len(out.ColumnMetas) > 0 {
		m := out.ColumnMetas[0]
		decl = sudDecl(m.Name, m.TypeID, m.Precision, m.Scale)
	}
	return decl, &oracle.Result{Columns: out.Columns, Rows: out.Rows}, nil
}

func tmdRunDAGDecl(ctx context.Context, coord *Coordinator, sql string) (
	decl string, res *oracle.Result, err error,
) {
	defer func() {
		if r := recover(); r != nil {
			decl, res, err = "", nil, fmt.Errorf("PANIC: %v", r)
		}
	}()
	out, qerr := coord.ExecuteSQL(ctx, sql)
	if qerr != nil {
		return "", nil, qerr
	}
	if out.Error != "" {
		return "", nil, fmt.Errorf("%s", out.Error)
	}
	// Before Rows(): materializing the result detaches the batches the schema
	// would otherwise be read from.
	if schema := out.OutputSchema(); len(schema) > 0 {
		decl = sudDecl(schema[0].Name, schema[0].Type, schema[0].Precision, schema[0].Scale)
	}
	rows, rerr := out.Rows()
	if rerr != nil {
		return "", nil, fmt.Errorf("materializing distributed rows: %w", rerr)
	}
	return decl, &oracle.Result{Columns: out.Columns, Rows: rows}, nil
}

// setOpPairRefRows is one arm's own values, alone, rendered at the COMMON type
// the pair resolves to. `pgVerdict` is PostgreSQL's name for that type, from
// the measured table, so the width the reference is narrowed to is not read
// from the code under test.
func setOpPairRefRows(t *testing.T, run func(string) (*oracle.Result, error),
	col, pgVerdict string,
) []string {
	t.Helper()
	// A FLOAT common type is LOSSY by design — `real ∪ bigint` is real in
	// PostgreSQL too, and a real widened to double precision renders its own
	// float32 value at float64 precision (0.1 becomes 0.10000000149011612,
	// ADR-0012 item 12's own example). So the reference is CAST to the common
	// type, which is an independent spelling of "this arm's values at that
	// width" and not the union under test.
	expr := col
	switch pgVerdict {
	case "real":
		expr = fmt.Sprintf("CAST(%s AS REAL)", col)
	case "double precision":
		expr = fmt.Sprintf("CAST(%s AS DOUBLE)", col)
	}
	res, err := run(fmt.Sprintf("SELECT %s AS v FROM typemx WHERE id < 3", expr))
	if err != nil {
		t.Fatalf("the reference spelling for %s refused: %v", expr, err)
	}
	out := setOpCanonRows(res)
	sort.Strings(out)
	return out
}

// TestTheCarrierGapListIsTheCodes asserts that the list this file states and
// the set the planner actually refuses are the same set. The round-2 review
// found ADR-0012 and docs/sql-reference.md naming two carrier pairs where the
// code refused twenty, and this file's own header naming a different set again
// from the map eight lines below it — a list nobody checks drifts from the code
// the moment the code moves.
func TestTheCarrierGapListIsTheCodes(t *testing.T) {
	code := map[[2]parquet.TypeID]bool{}
	for _, p := range physical.SetOpCarrierGapPairs() {
		code[p] = true
	}
	for p := range setOpPairNoCarrierPairs {
		if !code[p] {
			t.Errorf("%s ∪ %s is listed here and the planner does not refuse it", p[0], p[1])
		}
	}
	for p := range code {
		if !setOpPairNoCarrierPairs[p] {
			t.Errorf("the planner refuses %s ∪ %s and this list does not say so — add it, and "+
				"add it to ADR-0012 item 12's carrier list too", p[0], p[1])
		}
	}
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
