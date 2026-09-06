package server

import (
	"reflect"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	wadjetv1 "github.com/derekmwright/wadjet/gen/wadjet/v1"
)

// The gRPC DOOR CENSUS: every RPC the service exposes, times every identity,
// with the code the door answers and the side effect it leaves.
//
// A per-RPC authorization rule is only as good as the enumeration behind it —
// #934 and #935 were two methods nobody had walked, on a door whose only
// authorization surface was an interceptor that authenticates. This test is
// that enumeration, so a method added later is either listed here or visibly
// missing from a list that names every RPC in the service.
//
// It runs on BOTH provider shapes, and it PINS the cells that are still wrong
// today rather than omitting them. A pin that starts disagreeing fails, and
// deleting it is the fix's proof:
//
//   - pinSelectDeny: a SELECT the policy engine denies at the TABLE level comes back
//     Internal, not PermissionDenied, because internal/auth/plan_enforce.go
//     returns that refusal with no SQLSTATE on ANY door (unlike its DML twin,
//     which carries 42501 and does map here). Fixing it is an internal/auth
//     change; grpcQueryError needs no edit when it lands.
//   - pinLegacyDML: with a provider built WITHOUT an ABAC evaluator
//     (auth.NewProvider(authn, authz, nil, nil)), EnforcePlanPolicies and
//     EnforceDMLPolicies both return early, so the reader reads and writes a
//     table its role does not list. The doors that ship always migrate `roles:`
//     to ABAC (cmd/wadjet buildProviderFromConfig), so a YAML-configured server
//     is not exposed — but a provider built in process is, and METADATA is
//     decided in both shapes while the DATA is not.
//
// The three async RPCs answer Unavailable here because this rig is standalone
// (no coordinator). They are recorded as measured; SEC3's query-ownership arc
// changes them.
type grpcCensusCell struct {
	rpc string
	key string // "" = no credential
	// The two shapes, always both spelled out: a cell that differed silently
	// between them is how the no-evaluator gap hid.
	wantABAC   codes.Code
	wantLegacy codes.Code
	pin        string // non-empty: a known-wrong cell, with what makes it right
	// after asserts the SIDE EFFECT. A refusal that already dropped the table
	// satisfies any code-only assertion.
	after func(t *testing.T, rig grpcAuthzRig)
}

// The pin texts. Each names the FIX that makes its cell right, not the pin's
// own label: the whole contract is that a failing run tells the person reading
// it what just landed, and "PIN-B" tells them to go find a doc comment
// (round-1 review P5).
const (
	pinSelectDeny = "SEC1, two fixes reach this cell: (abac shape) " +
		"internal/auth/plan_enforce.go's SELECT table refusal carries no SQLSTATE — when it " +
		"returns sqlerr 42501, as its DML twin already does, grpcQueryError maps it and this " +
		"cell becomes PermissionDenied; (legacy shape) " + pinLegacyDML
	pinLegacyDML = "SEC1: a provider with no ABAC evaluator returns early from " +
		"EnforcePlanPolicies/EnforceDMLPolicies; when the no-evaluator provider enforces the " +
		"legacy role rule on SELECT and DML, this cell becomes PermissionDenied on the legacy shape"
)

func grpcCensus() []grpcCensusCell {
	const (
		OK     = codes.OK
		Unauth = codes.Unauthenticated
		Denied = codes.PermissionDenied
		Unavl  = codes.Unavailable
		Intern = codes.Internal
	)
	tableGone := func(name string) func(*testing.T, grpcAuthzRig) {
		return func(t *testing.T, rig grpcAuthzRig) {
			if grpcAuthzHasTable(t, rig.cat, name) {
				t.Errorf("side effect: %q still exists after an accepted drop", name)
			}
		}
	}
	tableThere := func(name string) func(*testing.T, grpcAuthzRig) {
		return func(t *testing.T, rig grpcAuthzRig) {
			if !grpcAuthzHasTable(t, rig.cat, name) {
				t.Errorf("side effect: %q is gone after a refusal", name)
			}
		}
	}
	noSuchTable := func(name string) func(*testing.T, grpcAuthzRig) {
		return func(t *testing.T, rig grpcAuthzRig) {
			if grpcAuthzHasTable(t, rig.cat, name) {
				t.Errorf("side effect: %q was created on a refused call", name)
			}
		}
	}
	created := func(name string) func(*testing.T, grpcAuthzRig) {
		return func(t *testing.T, rig grpcAuthzRig) {
			if !grpcAuthzHasTable(t, rig.cat, name) {
				t.Errorf("side effect: %q was not created on an accepted call", name)
			}
		}
	}

	var cells []grpcCensusCell
	// Health bypasses authentication by design (grpc.go), for every identity
	// including none — a liveness probe has no credentials.
	for _, key := range []string{"", "reader-key", "writer-key", "admin-key"} {
		cells = append(cells, grpcCensusCell{"Health/Check", key, OK, OK, "", nil})
	}
	// No credential: the interceptor refuses before any method body runs, and
	// nothing is touched.
	for _, rpc := range []string{
		"Query/select-allowed", "Query/select-secret", "Query/delete-allowed",
		"QueryStream/select-allowed", "QueryStream/select-secret",
		"SubmitQuery", "GetQueryStatus", "CancelQuery",
		"ListTables", "DescribeTable/allowed", "DescribeTable/secret",
	} {
		cells = append(cells, grpcCensusCell{rpc, "", Unauth, Unauth, "", nil})
	}
	cells = append(cells, []grpcCensusCell{
		{"CreateTable", "", Unauth, Unauth, "", noSuchTable("census_new")},
		{"DropTable/protected", "", Unauth, Unauth, "", tableThere("protected")},

		// --- reader: read on its own table, nothing else -------------------
		{"Query/select-allowed", "reader-key", OK, OK, "", nil},
		{"Query/select-secret", "reader-key", Intern, OK, pinSelectDeny, nil},
		{"Query/delete-allowed", "reader-key", Denied, OK, pinLegacyDML, nil},
		{"QueryStream/select-allowed", "reader-key", OK, OK, "", nil},
		{"QueryStream/select-secret", "reader-key", Intern, OK, pinSelectDeny, nil},
		{"SubmitQuery", "reader-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"GetQueryStatus", "reader-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"CancelQuery", "reader-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"ListTables", "reader-key", OK, OK, "", nil},
		{"DescribeTable/allowed", "reader-key", OK, OK, "", nil},
		{"DescribeTable/secret", "reader-key", Denied, Denied, "", nil},
		{"CreateTable", "reader-key", Denied, Denied, "", noSuchTable("census_new")},
		{"DropTable/protected", "reader-key", Denied, Denied, "", tableThere("protected")},

		// --- writer: read and write everywhere ----------------------------
		{"Query/select-allowed", "writer-key", OK, OK, "", nil},
		{"Query/select-secret", "writer-key", OK, OK, "", nil},
		{"Query/delete-allowed", "writer-key", OK, OK, "", nil},
		{"QueryStream/select-allowed", "writer-key", OK, OK, "", nil},
		{"QueryStream/select-secret", "writer-key", OK, OK, "", nil},
		{"SubmitQuery", "writer-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"GetQueryStatus", "writer-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"CancelQuery", "writer-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"ListTables", "writer-key", OK, OK, "", nil},
		{"DescribeTable/allowed", "writer-key", OK, OK, "", nil},
		{"DescribeTable/secret", "writer-key", OK, OK, "", nil},
		{"CreateTable", "writer-key", OK, OK, "", created("census_new")},
		{"DropTable/protected", "writer-key", OK, OK, "", tableGone("protected")},

		// --- admin ---------------------------------------------------------
		{"Query/select-secret", "admin-key", OK, OK, "", nil},
		{"QueryStream/select-secret", "admin-key", OK, OK, "", nil},
		{"SubmitQuery", "admin-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"GetQueryStatus", "admin-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"CancelQuery", "admin-key", Unavl, Unavl, "SEC3 changes these", nil},
		{"ListTables", "admin-key", OK, OK, "", nil},
		{"DescribeTable/secret", "admin-key", OK, OK, "", nil},
		{"CreateTable", "admin-key", OK, OK, "", created("census_new")},
		{"DropTable/protected", "admin-key", OK, OK, "", tableGone("protected")},
	}...)
	return cells
}

// grpcCensusInvoke performs one cell's RPC. Every cell goes over the wire —
// calling a handler method directly would test the method, not the door.
func grpcCensusInvoke(t *testing.T, rig grpcAuthzRig, rpc, key string) error {
	t.Helper()
	ctx := grpcAuthzCtx(key)
	switch rpc {
	case "Health/Check":
		_, err := rig.health.Check(ctx, &healthpb.HealthCheckRequest{})
		return err
	case "Query/select-allowed":
		_, err := rig.client.Query(ctx, &wadjetv1.QueryRequest{Sql: "SELECT * FROM allowed"})
		return err
	case "Query/select-secret":
		_, err := rig.client.Query(ctx, &wadjetv1.QueryRequest{Sql: "SELECT * FROM secret"})
		return err
	case "Query/delete-allowed":
		_, err := rig.client.Query(ctx, &wadjetv1.QueryRequest{Sql: "DELETE FROM allowed WHERE id = 1"})
		return err
	case "QueryStream/select-allowed", "QueryStream/select-secret":
		table := "allowed"
		if rpc == "QueryStream/select-secret" {
			table = "secret"
		}
		stream, err := rig.client.QueryStream(ctx, &wadjetv1.QueryRequest{Sql: "SELECT * FROM " + table})
		if err != nil {
			return err
		}
		_, err = stream.Recv()
		return err
	case "SubmitQuery":
		_, err := rig.client.SubmitQuery(ctx, &wadjetv1.QueryRequest{Sql: "SELECT 1"})
		return err
	case "GetQueryStatus":
		_, err := rig.client.GetQueryStatus(ctx, &wadjetv1.GetQueryStatusRequest{QueryId: "q-census"})
		return err
	case "CancelQuery":
		_, err := rig.client.CancelQuery(ctx, &wadjetv1.CancelQueryRequest{QueryId: "q-census"})
		return err
	case "ListTables":
		_, err := rig.client.ListTables(ctx, &wadjetv1.ListTablesRequest{})
		return err
	case "DescribeTable/allowed":
		_, err := rig.client.DescribeTable(ctx, &wadjetv1.DescribeTableRequest{TableName: "allowed"})
		return err
	case "DescribeTable/secret":
		_, err := rig.client.DescribeTable(ctx, &wadjetv1.DescribeTableRequest{TableName: "secret"})
		return err
	case "CreateTable":
		_, err := rig.client.CreateTable(ctx, &wadjetv1.CreateTableRequest{
			Name:    "census_new",
			Columns: []*wadjetv1.ColumnDef{{Name: "id", Type: "BIGINT"}},
		})
		return err
	case "DropTable/protected":
		_, err := rig.client.DropTable(ctx, &wadjetv1.DropTableRequest{Name: "protected"})
		return err
	}
	t.Fatalf("census: no invoke for RPC %q", rpc)
	return nil
}

func TestGRPCDoorCensus(t *testing.T) {
	for _, shape := range []string{grpcAuthzLegacy, grpcAuthzABAC} {
		for _, cell := range grpcCensus() {
			want := cell.wantABAC
			if shape == grpcAuthzLegacy {
				want = cell.wantLegacy
			}
			id := cell.key
			if id == "" {
				id = "no-credential"
			}
			t.Run(shape+"/"+cell.rpc+"/"+id, func(t *testing.T) {
				// A rig per cell: the mutating cells change the catalog, and a
				// census whose rows depend on the order they ran in is not a
				// census.
				rig := grpcAuthzUp(t, shape, grpcAuthzConfig())
				err := grpcCensusInvoke(t, rig, cell.rpc, cell.key)
				if got := grpcAuthzCode(err); got != want {
					msg := "census: %s / %s / %s = %v, want %v (err %v)"
					if cell.pin != "" {
						msg += "\n  this cell is PINNED: " + cell.pin +
							"\n  if the fix landed, update the census — do not widen it"
					}
					t.Errorf(msg, shape, cell.rpc, id, got, want, err)
				}
				if cell.after != nil {
					cell.after(t, rig)
				}
			})
		}
	}
}

// TestGRPCDoorCensusCoversEveryRPC keeps the census honest: every method the
// service exposes must appear in it. A method added to the service without a
// census row fails here rather than shipping unauthorized and unnoticed, which
// is precisely how #934 and #935 arrived.
func TestGRPCDoorCensusCoversEveryRPC(t *testing.T) {
	// The service's methods come from the GENERATED client interface, not from
	// a list somebody keeps up to date: an RPC added to the proto appears here
	// on the next build, with no census row, and this fails.
	iface := reflect.TypeOf((*wadjetv1.WadjetServiceClient)(nil)).Elem()

	covered := map[string]bool{}
	for _, cell := range grpcCensus() {
		name, _, _ := strings.Cut(cell.rpc, "/")
		covered[name] = true
	}
	// Health is not part of WadjetService — it is the standard health service
	// the server also registers, and the one method that bypasses
	// authentication, so the census must walk it too.
	if !covered["Health"] {
		t.Error("the census does not walk the health service, whose authentication bypass is a door")
	}
	delete(covered, "Health")

	exposed := map[string]bool{}
	for i := 0; i < iface.NumMethod(); i++ {
		m := iface.Method(i).Name
		exposed[m] = true
		if !covered[m] {
			t.Errorf("RPC %q has no census row: every method on this door is walked, or the door "+
				"has an entry nobody audited — which is how #934 and #935 arrived", m)
		}
	}
	for name := range covered {
		if !exposed[name] {
			t.Errorf("the census walks %q, which WadjetServiceClient does not expose", name)
		}
	}
	if len(exposed) == 0 {
		t.Fatal("reflected no methods off WadjetServiceClient: the enumeration is broken, not empty")
	}
}
