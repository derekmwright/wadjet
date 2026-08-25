package kernel

import (
	"fmt"
	"testing"

	"github.com/derekmwright/wadjet/internal/storage/parquet"
)

// TestCidrStatsSortKeyMatchesKernel is the gate parquet's own copy of this
// key names in its doc (#523): internal/storage/parquet cannot import this
// package — kernel imports batch, which imports parquet, and since #523
// kernel imports parquet directly too — so CidrSortKey is DUPLICATED there
// as parquet.CidrStatsSortKey, and the two must agree byte for byte or
// row-group pruning compares a writer's bound against a reader's literal in
// two different orders.
//
// That divergence would be a WRONG ANSWER, not a lost optimization: the
// writer picks which row wins a row group's min/max using its copy, the
// reader re-keys that row's text using the same copy, and the filter kernel
// keys the literal using this one. A key that disagreed anywhere in the four
// steps PostgreSQL's inet order takes (family, common bits under the smaller
// mask, mask length, full address) would prune away row groups holding rows
// the filter keeps.
//
// The fixture is the same one CidrSortKey's own semantics gate uses —
// pgInetOrder, derived verbatim from `SELECT t FROM v ORDER BY t::inet, t`
// in a postgres:17-alpine container — so this pins the copy against
// PostgreSQL's answer and not merely against whatever the two happen to
// share.
//
// This test lives here rather than in internal/storage/parquet because the
// import graph only allows it in one direction: this package can see both
// implementations, and that one can see only its own.
func TestCidrStatsSortKeyMatchesKernel(t *testing.T) {
	if len(pgInetOrder) == 0 {
		t.Fatal("pgInetOrder is empty — the fixture this gate stands on is gone")
	}
	for _, s := range pgInetOrder {
		want, wantOK := CidrSortKey(s)
		got, gotOK := parquet.CidrStatsSortKey(s)
		if wantOK != gotOK {
			t.Errorf("%s: kernel.CidrSortKey ok=%v, parquet.CidrStatsSortKey ok=%v — "+
				"the two copies disagree about whether the value is an address at all", s, wantOK, gotOK)
			continue
		}
		if got != want {
			t.Errorf("%s: parquet.CidrStatsSortKey = %x, kernel.CidrSortKey = %x — "+
				"the duplicated key has drifted; a writer's bound and a reader's literal "+
				"are now in different orders", s, got, want)
		}
	}

	// The refusals must match too: parquet's writer latches cidrKeyFailed on
	// a false, which suppresses CidrStatsOrderKey for the whole file, while
	// this package's false makes the query an error (22P02). One copy
	// accepting what the other refuses would put a file's promise and the
	// query's meaning out of step.
	for _, s := range []string{"", "not-an-address", "10.0.0.0/33", "10.0.0.0/-1", "::/129", "10.0.0"} {
		_, wantOK := CidrSortKey(s)
		_, gotOK := parquet.CidrStatsSortKey(s)
		if wantOK != gotOK {
			t.Errorf("%q: kernel.CidrSortKey ok=%v, parquet.CidrStatsSortKey ok=%v", s, wantOK, gotOK)
		}
	}

	// A host-bearing prefix at several mask lengths, in both families: the
	// shape whose host bits the FIRST CidrSortKey threw away (#492), so a
	// copy that reverted to keying only the masked network would pass the
	// canonical fixture above and fail here.
	for _, addr := range []string{"10.0.0.1", "192.168.188.190", "2001:db8::9", "::ffff:10.0.0.2"} {
		for _, mask := range []int{0, 8, 16, 24, 32, 64, 128} {
			s := fmt.Sprintf("%s/%d", addr, mask)
			want, wantOK := CidrSortKey(s)
			got, gotOK := parquet.CidrStatsSortKey(s)
			if wantOK != gotOK || got != want {
				t.Errorf("%s: parquet.CidrStatsSortKey = %x (ok=%v), kernel.CidrSortKey = %x (ok=%v)",
					s, got, gotOK, want, wantOK)
			}
		}
	}
}
