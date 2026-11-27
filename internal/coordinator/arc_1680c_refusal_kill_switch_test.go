// SPDX-License-Identifier: AGPL-3.0-only

package coordinator

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/derekmwright/wadjet/internal/optswitch"
)

// TestArc1680cRefusalsUnderEveryKillSwitch: every refusal stage A owns (an
// A refusal and every not-parsed cell of the interval corpus) is decided
// before any optimization, so turning any one kill switch off changes
// neither whether the statement is refused nor its SQLSTATE nor its
// identity, on the single and the DAG arm (#1680c; docs/testing/GATES.md,
// the kill-switch invariance of a refusal).
func TestArc1680cRefusalsUnderEveryKillSwitch(t *testing.T) {
	if testing.Short() {
		t.Skip("-short: every kill switch over the corpus's refusals")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	t.Cleanup(cancel)
	writer, arms := iv80ArmsFixture(t, ctx)
	for _, sec := range iv80Sections() {
		for _, c := range sec.cells() {
			if c.ddl && !c.write {
				writer(c.sql)
			}
		}
	}
	var picked []iv80Arm
	for _, a := range arms {
		if a.name == "single" || a.name == "dag" {
			picked = append(picked, a)
		}
	}
	type cell struct{ key, sql string }
	var cells []cell
	for _, sec := range iv80Sections() {
		man, _ := iv80ReadTSV(t, iv80Path(sec.stem, "_manifest.tsv"))
		for _, c := range sec.cells() {
			if c.pgOnly || c.ddl || c.write || iv80CellStyle(c) != "" {
				continue
			}
			m, ok := man[c.name]
			if !ok {
				continue
			}
			f := strings.Split(m[0], "\t")
			if len(f) < 2 || !(f[0] == "A" && strings.HasPrefix(f[1], "refusal ") || strings.HasPrefix(f[1], "not-parsed ")) {
				continue
			}
			cells = append(cells, cell{sec.stem + ":" + c.name, c.sql})
		}
	}
	if len(cells) < 300 {
		t.Fatalf("only %d refusal cells", len(cells))
	}
	type key struct{ arm, cell string }
	base := map[key]string{}
	for _, a := range picked {
		for _, c := range cells {
			base[key{a.name, c.key}] = iv80EngineText(a.run(c.sql))
		}
	}
	moved := 0
	for _, tog := range optswitch.All() {
		prev := tog.Set(false)
		for _, a := range picked {
			for _, c := range cells {
				got := iv80EngineText(a.run(c.sql))
				want := base[key{a.name, c.key}]
				gc, _ := iv80ErrCode(got)
				wc, _ := iv80ErrCode(want)
				if strings.HasPrefix(got, "ERR ") != strings.HasPrefix(want, "ERR ") || gc != wc || iv80ErrIdentity(got) != iv80ErrIdentity(want) {
					moved++
					if moved <= 20 {
						t.Errorf("%s off, %s on %s:\n  with it  %s\n  without  %s", tog.Name, c.key, a.name, want, got)
					}
				}
			}
		}
		tog.Set(prev)
	}
	t.Logf("%d refusal cells x %d kill switches x %d arms; %d moved", len(cells), len(optswitch.All()), len(picked), moved)
}
