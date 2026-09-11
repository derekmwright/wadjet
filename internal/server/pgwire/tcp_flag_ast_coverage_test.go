package pgwire

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/testutil/tcpflagcases"
)

// Promotes the recursive used/unused cases in a2r7rev_extra_test.go.txt,
// and enumerates the expression/query-block walk over empty input.
func TestTCPFlagASTCoverage(t *testing.T) {
	_, srv := setupRealDB(t)
	conn := connectPgconn(t, srv.Addr())
	for _, tc := range tcpflagcases.Cases {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/f%d", tc.Name, format), func(t *testing.T) {
				r := conn.ExecParams(context.Background(), tc.SQL, nil, nil, nil, []int16{format}).Read()
				want := tcpflagcases.State(tc.Name, "wire")
				if want == "" && strings.HasPrefix(tc.Name, "merge_") && (r.Err != nil || r.CommandTag.String() != "MERGE 0") {
					t.Errorf("MERGE residual changed: %s %v", r.CommandTag, r.Err)
				}
				if want == "" && !strings.HasPrefix(tc.Name, "merge_") && (r.Err != nil || len(r.Rows) != 0) {
					t.Errorf("empty SELECT residual changed: %q %v", r.Rows, r.Err)
				}
				if pgErrCode(r.Err) != want || (want == "22023" && (r.Err == nil || !strings.Contains(r.Err.Error(), "BOGUS"))) {
					t.Errorf("SQL=%s rows=%q error=%v state=%s; want %s", tc.SQL, r.Rows, r.Err, pgErrCode(r.Err), want)
				}
			})
		}
	}

	for _, tc := range tcpflagcases.RecursiveControls {
		for _, format := range []int16{0, 1} {
			t.Run(fmt.Sprintf("%s/f%d", tc.Name, format), func(t *testing.T) {
				r := conn.ExecParams(context.Background(), tc.SQL, nil, nil, nil, []int16{format}).Read()
				if r.Err != nil || len(r.Rows) != 0 {
					t.Fatalf("valid recursive name: rows=%q error=%v", r.Rows, r.Err)
				}
			})
		}
	}
}
