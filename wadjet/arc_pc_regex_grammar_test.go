// SPDX-License-Identifier: MIT

package wadjet

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"testing"

	"github.com/derekmwright/wadjet/internal/sqlerr"
	"github.com/derekmwright/wadjet/internal/storage/objstore"
)

// TestArcPCRegexOperatorsAnswerPostgreSQLsGrammar runs the pattern-match
// operators over PostgreSQL's documented ARE grammar (§9.7.3): 1,255 cells —
// every accepted form and every rejected one, bracket expressions and ranges
// (mixed-case endpoints included), class escapes, quantifiers and bounds
// (malformed and unfinished ones), anchors under quantifiers, constraints,
// metasyntax, embedded options, case controls and NULL — each answered by
// PostgreSQL 17.11 under the C collation (testdata/pc_regex_cells.json,
// recorded by arc PC's round-1 review, measure.jsonl).
//
// A cell answers PostgreSQL's true/false/NULL, and a pattern PostgreSQL
// rejects is its 2201B. The one other disposition is 0A000, and only for the
// ARE constructs RE2 has no form for — a back reference, lookaround, \m \M,
// the word-edge and collating bracket classes, and the b e n p w x embedded
// options — never for a pattern PostgreSQL rejects.
func TestArcPCRegexOperatorsAnswerPostgreSQLsGrammar(t *testing.T) {
	raw, err := os.ReadFile("testdata/pc_regex_cells.json")
	if err != nil {
		t.Fatal(err)
	}
	var cells []struct{ SQL, PG string }
	if err := json.Unmarshal(raw, &cells); err != nil {
		t.Fatal(err)
	}
	refusable := regexp.MustCompile(`\\[1-9]|\(\?[=!<]|\\[mM]|\[\[[:.=][<>]?|\[\[[.=]|\(\?[a-z]*[benpwx]`)
	ctx := context.Background()
	db, err := Open(ctx, Config{Store: objstore.NewMemStore(), Bucket: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	answered, refused := 0, 0
	for _, c := range cells {
		got := "NULL"
		res, qerr := db.Query(ctx, c.SQL)
		switch {
		case qerr != nil:
			got = "E:" + sqlerr.StateOf(qerr)
		case len(res.Rows) == 1:
			if v := res.Rows[0]["v"]; v != nil {
				got = map[bool]string{true: "t", false: "f"}[v == true]
			}
		}
		if got == c.PG {
			answered++
			continue
		}
		if got == "E:0A000" && c.PG != "E:2201B" && refusable.MatchString(c.SQL) {
			refused++
			continue
		}
		t.Errorf("%s\n  wadjet %s, PostgreSQL 17.11 %s", c.SQL, got, c.PG)
	}
	if len(cells) < 1200 || answered < 1100 {
		t.Fatalf("%d cells, %d agreeing: the table stopped reaching the operator", len(cells), answered)
	}
	fmt.Printf("%d of %d cells agree with PostgreSQL 17.11; %d refused 0A000 as documented\n",
		answered, len(cells), refused)
}
