// SPDX-License-Identifier: MIT

package batch

import (
	"bufio"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// decimalMoveSite is a line that MOVES a DECIMAL carrier from one vector to
// another: it writes a DecimalData.Data element or range and reads another
// vector's DecimalData.Data in the same statement, or assigns a whole
// DecimalData. Every such move must move the value's display scale with it
// (ADR-0024 §1 as amended; arc PS RISKS R1: a move that copies the carrier
// alone prints the column's scale where the value has its own, which is
// today's text and so never louder).
var decimalMoveSite = regexp.MustCompile(
	`DecimalData\.Data\[[^]]*\] *=[^=].*DecimalData\.Data|` +
		`copy\(.*DecimalData\.Data.*DecimalData\.Data|` +
		`DecimalData *= *[A-Za-z_.]*\.DecimalData|` +
		`DecimalData\.Data *= *append\(.*DecimalData\.Data\[`)

// decimalMoveAllowed is every line the pattern matches outside the helpers,
// by file, with the count and the reason the display scale still moves. A
// new move site fails the census until it goes through
// DecimalColumn.CopyRow / CopyRange / Gather / AppendRow, or is listed here
// with the statement that carries its codes.
var decimalMoveAllowed = map[string]struct {
	n      int
	reason string
}{
	"internal/engine/batch/view.go": {3,
		"Flatten: the NULL-aware carrier loops (2) are followed by GatherCodes over the same indices; " +
			"`v.DecimalData = flat.DecimalData` hands over the flattened column, codes included"},
	"internal/engine/exec/sort.go": {2,
		"gatherVector's NULL-aware carrier loop; GatherDScaleCodes over srcRows follows it"},
	"internal/engine/exec/window.go": {1,
		"windowGatherVector's NULL-aware carrier loop; GatherDScaleCodes over perm follows it"},
}

// TestDecimalCarrierMovesCarryTheDisplayScale is the census RISKS R1 asks
// for. At eb76eb97 it counts 21 move lines in 11 files, none of which
// carried a display scale (there was none to carry); each now goes through a
// DecimalColumn helper or is listed above with the statement that moves its
// codes.
func TestDecimalCarrierMovesCarryTheDisplayScale(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	got := map[string][]string{}
	for _, dir := range []string{"internal", "wadjet", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if strings.HasPrefix(d.Name(), ".") || d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			defer f.Close()
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			sc := bufio.NewScanner(f)
			sc.Buffer(make([]byte, 1<<20), 1<<20)
			for ln := 1; sc.Scan(); ln++ {
				line := strings.TrimSpace(sc.Text())
				if strings.HasPrefix(line, "//") {
					continue
				}
				if decimalMoveSite.MatchString(line) {
					got[rel] = append(got[rel], line)
				}
			}
			return sc.Err()
		})
		if err != nil {
			t.Fatalf("walking %s: %v", dir, err)
		}
	}
	files := make([]string, 0, len(got))
	for f := range got {
		files = append(files, f)
	}
	sort.Strings(files)
	for _, f := range files {
		want, ok := decimalMoveAllowed[f]
		if !ok {
			t.Errorf("%s moves a DECIMAL carrier without its display scale (%d lines):\n  %s",
				f, len(got[f]), strings.Join(got[f], "\n  "))
			continue
		}
		if len(got[f]) != want.n {
			t.Errorf("%s: %d carrier-move lines, the census lists %d (%s):\n  %s",
				f, len(got[f]), want.n, want.reason, strings.Join(got[f], "\n  "))
		}
	}
	for f, want := range decimalMoveAllowed {
		if _, ok := got[f]; !ok {
			t.Errorf("%s: the census lists %d carrier-move lines and finds none; drop the entry", f, want.n)
		}
	}
}
