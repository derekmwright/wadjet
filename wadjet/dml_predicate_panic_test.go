package wadjet

import (
	"context"
	"testing"

	"github.com/derekmwright/wadjet/internal/engine/exec"
	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// dmlFatalPredicates are the fatal-evaluation classes a DML WHERE can reach:
// each raises a FatalEvalPanic rather than returning a value, and each must
// come back as PostgreSQL's SQLSTATE for it (ADR-0019).
var dmlFatalPredicates = []struct {
	Name  string
	Where string
	State string
}{
	{"division by zero", "1/0 = 1", "22012"},
	{"modulo by zero", "1 % 0 = 1", "22012"},
	{"invalid cast to bool", "CAST('abc' AS BOOL)", "22P02"},
	{"invalid cast to int", "CAST('abc' AS INT) = 1", "22P02"},
}

// A DML WHERE that cannot be evaluated fails the STATEMENT with a SQLSTATE and
// changes nothing — on every door. The embedded door survived only because
// DB.Execute's own boundary caught the panic several frames up; the HTTP door
// had no boundary at all and answered with a dropped connection (#677).
//
// The counter is the half that keeps this honest: a FatalEvalPanic is the
// DESIGNED class and must not be counted as a recovered engine bug, so a fix
// that "worked" by turning these into XX000 QueryPanics would fail here.
func TestDMLPredicatePanicIsAStatementErrorNotAnOutage(t *testing.T) {
	for _, tc := range dmlFatalPredicates {
		for _, stmt := range []string{
			"DELETE FROM pp WHERE " + tc.Where,
			"UPDATE pp SET n = 2 WHERE " + tc.Where,
		} {
			t.Run(tc.Name+" / "+stmt[:6], func(t *testing.T) {
				ctx := context.Background()
				db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
				if err != nil {
					t.Fatal(err)
				}
				defer db.Close()
				if _, err := db.Query(ctx, "CREATE TABLE pp (id INT64, n INT64)"); err != nil {
					t.Fatal(err)
				}
				if _, err := db.Execute(ctx, "INSERT INTO pp VALUES (1, 1)"); err != nil {
					t.Fatal(err)
				}

				before := exec.QueryPanicsRecovered()
				_, err = db.Execute(ctx, stmt)
				if err == nil {
					t.Fatalf("%q succeeded; want %s", stmt, tc.State)
				}
				if got := sqlerr.StateOf(err); got != tc.State {
					t.Errorf("%q: SQLSTATE %q, want %q (err: %v)", stmt, got, tc.State, err)
				}
				if d := exec.QueryPanicsRecovered() - before; d != 0 {
					t.Errorf("%q counted %d recovered panic(s); a fatal EVAL is the designed class "+
						"and must keep its own SQLSTATE, not become an XX000 engine bug", stmt, d)
				}

				// The statement changed nothing, and the engine still works.
				q, err := db.Query(ctx, "SELECT id, n FROM pp")
				if err != nil {
					t.Fatalf("the engine is unusable after a refused DML: %v", err)
				}
				if len(q.Rows) != 1 || q.Rows[0]["n"].(int64) != 1 {
					t.Errorf("row set after a refused %q is %v, want one row with n=1", stmt, q.Rows)
				}
			})
		}
	}
}
