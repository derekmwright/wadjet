package expr

import (
	"fmt"
	"math"
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/derekmwright/wadjet/internal/oracle/semvergen"
)

// EVERY GENERATED BOUND, AT THE TOP OF THE DOMAIN (#967).
//
// `semver_range.go` closes a band by raising ONE component by one, and it does
// that at SIXTEEN sites. A component is accepted up to int64's maximum, so
// every one of those sites can be handed a component that has nowhere to go,
// and `+1` there is a negative number: the `<` half then drops every row and
// the `>=` half admits every row — the wrong boolean in both directions, with
// no refusal and no NULL to show for it.
//
// This is the SEAM enumerated once rather than one position at a time: the
// table below holds a row for every site, named by the function and branch the
// raise lives in, with the range spelling that reaches it, the desugaring it
// must render, and probe versions on both sides of the bound. A site missing
// from this table is a site with no cell, which is why the count is asserted.
//
// THE PROBES ARE NOT DECORATION EITHER. For a MINOR or PATCH raise the
// saturated bound must still BOUND: `1.9223372036854775807.x` admits
// `1.M.max` and refuses `2.0.0`. A fix that answered the overflow by dropping
// the upper bound would pass a rendering check and fail those rows, so each
// such site carries the row above its band.
const (
	svMax  = "9223372036854775807"
	svMaxV = svMax + "." + svMax + "." + svMax
)

type svBoundProbe struct {
	ver  string
	want bool
}

type svBoundSite struct {
	site   string // the function and branch whose raise this row covers
	rng    string
	expand string
	probes []svBoundProbe
}

func svBoundSites() []svBoundSite {
	return []svBoundSite{
		{"semverHyphenRange hi.xMinor (major+1)", "1.2.3 - " + svMax,
			">=1.2.3 <=" + svMaxV,
			[]svBoundProbe{{svMax + ".0.0", true}, {svMaxV, true}, {"1.2.3", true}, {"1.2.2", false}}},
		{"semverHyphenRange hi.xPatch (minor+1)", "1.2.3 - 1." + svMax,
			">=1.2.3 <=1." + svMax + "." + svMax,
			[]svBoundProbe{{"1." + svMax + "." + svMax, true}, {"1.2.3", true}, {"2.0.0", false}}},
		{"desugarSemverComparator > p.xMinor (major+1)", ">" + svMax + ".x",
			">" + svMaxV,
			[]svBoundProbe{{"1.0.0", false}, {svMax + ".0.0", false}, {svMaxV, false}}},
		{"desugarSemverComparator > p.xPatch (minor+1)", ">1." + svMax + ".x",
			">1." + svMax + "." + svMax,
			[]svBoundProbe{{"2.0.0", true}, {"1." + svMax + "." + svMax, false}, {"1." + svMax + ".0", false}, {"1.0.0", false}}},
		{"desugarSemverComparator <= p.xMinor (major+1)", "<=" + svMax + ".x",
			"<=" + svMaxV,
			[]svBoundProbe{{"1.0.0", true}, {svMaxV, true}, {"0.0.0", true}}},
		{"desugarSemverComparator <= p.xPatch (minor+1)", "<=1." + svMax + ".x",
			"<=1." + svMax + "." + svMax,
			[]svBoundProbe{{"1." + svMax + "." + svMax, true}, {"1.0.0", true}, {"2.0.0", false}}},
		{"desugarSemverComparator X-range p.xMinor (major+1)", svMax + ".x",
			">=" + svMax + ".0.0 <=" + svMaxV,
			[]svBoundProbe{{svMax + ".0.0", true}, {svMax + ".9.9", true}, {svMaxV, true}, {"1.0.0", false}}},
		{"desugarSemverComparator X-range p.xPatch (minor+1)", "1." + svMax + ".x",
			">=1." + svMax + ".0 <=1." + svMax + "." + svMax,
			[]svBoundProbe{{"1." + svMax + ".0", true}, {"1." + svMax + "." + svMax, true}, {"2.0.0", false}, {"1.0.0", false}}},
		{"semverCaret p.xMinor (major+1)", "^" + svMax + ".x",
			">=" + svMax + ".0.0 <=" + svMaxV,
			[]svBoundProbe{{svMax + ".0.0", true}, {svMaxV, true}, {"1.0.0", false}}},
		{"semverCaret p.xPatch major==0 (minor+1)", "^0." + svMax + ".x",
			">=0." + svMax + ".0 <=0." + svMax + "." + svMax,
			[]svBoundProbe{{"0." + svMax + ".0", true}, {"0." + svMax + "." + svMax, true}, {"1.0.0", false}}},
		{"semverCaret p.xPatch major!=0 (major+1)", "^" + svMax + ".2.x",
			">=" + svMax + ".2.0 <=" + svMaxV,
			[]svBoundProbe{{svMax + ".2.0", true}, {svMax + ".9.9", true}, {svMax + ".1.9", false}, {"1.0.0", false}}},
		{"semverCaret exact 0.0.p (patch+1)", "^0.0." + svMax,
			">=0.0." + svMax + " <=0.0." + svMax,
			[]svBoundProbe{{"0.0." + svMax, true}, {"0.0.0", false}, {"0.1.0", false}}},
		{"semverCaret exact 0.m.p (minor+1)", "^0." + svMax + ".0",
			">=0." + svMax + ".0 <=0." + svMax + "." + svMax,
			[]svBoundProbe{{"0." + svMax + ".0", true}, {"0." + svMax + "." + svMax, true}, {"1.0.0", false}}},
		{"semverCaret exact major!=0 (major+1)", "^" + svMax + ".0.0",
			">=" + svMax + ".0.0 <=" + svMaxV,
			[]svBoundProbe{{svMax + ".0.0", true}, {svMax + ".9.9", true}, {"1.0.0", false}, {svMax + ".0.0-beta", false}}},
		{"semverTilde p.xMinor (major+1)", "~" + svMax + ".x",
			">=" + svMax + ".0.0 <=" + svMaxV,
			[]svBoundProbe{{svMax + ".0.0", true}, {svMaxV, true}, {"1.0.0", false}}},
		{"semverTilde exact (minor+1)", "~1." + svMax + ".0",
			">=1." + svMax + ".0 <=1." + svMax + "." + svMax,
			[]svBoundProbe{{"1." + svMax + ".0", true}, {"1." + svMax + ".1", true}, {"1." + svMax + "." + svMax, true}, {"2.0.0", false}, {"1.0.0", false}}},
	}
}

// svRaiseSites is how many `+1` sites semver_range.go has. The table above
// must carry one row for each, and TestTheBoundTableCoversEveryRaiseSite reads
// the source to hold that true as the file changes.
const svRaiseSites = 16

func TestEveryGeneratedBoundSaturatesAtTheAcceptanceBound(t *testing.T) {
	sites := svBoundSites()
	if len(sites) != svRaiseSites {
		t.Fatalf("the table holds %d sites, want %d", len(sites), svRaiseSites)
	}
	for _, s := range sites {
		t.Run(strings.NewReplacer(" ", "_", "(", "", ")", "", "+", "plus", "=", "eq", "!", "not", ".", "_").Replace(s.site), func(t *testing.T) {
			r, err := ParseSemverRange("semver_satisfies", s.rng)
			if err != nil {
				t.Fatalf("%q was refused: %v", s.rng, err)
			}
			if got := r.String(); got != s.expand {
				t.Errorf("%q desugars to\n  %s\nwant\n  %s", s.rng, got, s.expand)
			}
			for _, c := range r[0] {
				if c.ver.major < 0 || c.ver.minor < 0 || c.ver.patch < 0 {
					t.Errorf("%q generated a NEGATIVE component: %s", s.rng, c.String())
				}
			}
			for _, p := range s.probes {
				v, ok := parseSemver(p.ver)
				if !ok {
					t.Fatalf("%q is not a version", p.ver)
				}
				if got := semverSatisfies(v, r); got != p.want {
					t.Errorf("semver_satisfies(%q, %q) = %v, want %v (desugars to %s)",
						p.ver, s.rng, got, p.want, r.String())
				}
			}
		})
	}
}

// TestTheBoundTableCoversEveryRaiseSite reads semver_range.go and says two
// things about the seam: every call to a bound constructor has a row in the
// table above, and NOTHING raises a version component anywhere else.
//
// The second half is written against the SPELLINGS a raise can take, because
// the round-2 review measured two it used to miss: a raise spread over two
// statements (`nm := hi.minor` then `nm++`) and a raise on a line carrying a
// trailing `//` comment, which an earlier version skipped wholesale. Comments
// are therefore STRIPPED rather than skipped, and an identifier that ever
// takes a component's value is tracked, so `nm++` is a raise and the loop
// index's `i++` is not. Both of those still WRAP, so the corpus sweep below
// catches them too — what this closes is the case where a new site is spelled
// past the scan AND saturates wrongly rather than wrapping.
func TestTheBoundTableCoversEveryRaiseSite(t *testing.T) {
	src := stripGoLineComments(readSemverRangeSource(t))
	calls := 0
	for _, name := range []string{"semverUpperMajor(", "semverUpperMinor(", "semverUpperPatch(", "semverLowerMajor(", "semverLowerMinor("} {
		calls += strings.Count(src, name) - strings.Count(src, "func "+name)
	}
	if calls != svRaiseSites {
		t.Errorf("semver_range.go has %d bound-constructor call sites, and the table covers %d; "+
			"a desugaring that closes a band has to have a cell at the acceptance bound",
			calls, svRaiseSites)
	}

	// Every identifier that ever takes a component's value, so a raise on a
	// COPY of one is a raise.
	lines := strings.Split(src, "\n")
	alias := map[string]bool{"major": true, "minor": true, "patch": true}
	takes := regexp.MustCompile(`\b([A-Za-z_][A-Za-z0-9_]*)\s*:?=\s*[^=]*\b(?:major|minor|patch)\b`)
	for _, line := range lines {
		if m := takes.FindStringSubmatch(line); m != nil {
			alias[m[1]] = true
		}
	}
	raise := regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.]*)\s*(?:\+\+|\+\s*1\b)`)
	// The five constructors are where the raise is SUPPOSED to be, and they
	// are skipped by brace depth rather than by a line pattern: their own
	// bodies hold the only guarded `+1` in the file.
	depth := 0
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if depth == 0 && (strings.HasPrefix(trimmed, "func semverUpper") || strings.HasPrefix(trimmed, "func semverLower")) {
			depth = strings.Count(line, "{") - strings.Count(line, "}")
			continue
		}
		if depth > 0 {
			depth += strings.Count(line, "{") - strings.Count(line, "}")
			continue
		}
		for _, m := range raise.FindAllStringSubmatch(line, -1) {
			name := m[1]
			if i := strings.LastIndexByte(name, '.'); i >= 0 {
				name = name[i+1:]
			}
			if !alias[name] {
				continue // an index, a counter, an offset — not a component
			}
			t.Errorf("a version component is raised outside the five bound constructors, "+
				"so it has no cell at the acceptance bound and can wrap: %s", trimmed)
		}
	}
}

// stripGoLineComments removes `//` comments so a raise cannot hide behind one.
//
// It is naive on purpose — the file it reads has no `//` inside a string
// literal, and asserting that is cheaper than a Go parser for a gate whose
// whole job is to read six spellings out of one file.
func stripGoLineComments(src string) string {
	var b strings.Builder
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

// NO RANGE BUILT OUT OF A CORPUS VERSION RENDERS A NEGATIVE COMPONENT.
//
// The site table names the sixteen positions; this is the same claim asked
// blind, over every operator form applied to the shipped corpus — whose
// components are drawn from a list ENDING in int64's maximum, which is how the
// wrap was reachable in the first place. It is also the shape the next
// arithmetic edge would show up in.
func TestNoDesugaringOverTheCorpusRendersANegativeComponent(t *testing.T) {
	versions := append(semvergen.Corpus(967, 2000), semvergen.Edge()...)
	versions = append(versions,
		svMax+".0.0", "0."+svMax+".0", "0.0."+svMax, svMaxV,
		svMax+"."+svMax+".0", "1."+svMax+"."+svMax)
	forms := []string{"^%s", "~%s", ">%s", ">=%s", "<%s", "<=%s", "=%s", "%s", "0.0.1 - %s", "%s - " + svMaxV}
	checked, refused := 0, 0
	for _, v := range versions {
		core := v
		if i := strings.IndexAny(core, "-+"); i >= 0 {
			core = core[:i]
		}
		parts := strings.Split(core, ".")
		spellings := []string{core}
		if len(parts) == 3 {
			spellings = append(spellings, parts[0]+"."+parts[1]+".x", parts[0]+".x", parts[0], parts[0]+"."+parts[1])
		}
		for _, sp := range spellings {
			for _, f := range forms {
				rng := fmt.Sprintf(f, sp)
				r, err := ParseSemverRange("semver_satisfies", rng)
				if err != nil {
					refused++
					continue
				}
				checked++
				for _, set := range r {
					for _, c := range set {
						if c.ver.major < 0 || c.ver.minor < 0 || c.ver.patch < 0 {
							t.Fatalf("%q desugars to %s, which carries a negative component",
								rng, r.String())
						}
						if c.ver.major == math.MinInt64 || c.ver.minor == math.MinInt64 || c.ver.patch == math.MinInt64 {
							t.Fatalf("%q desugars to %s, which wrapped", rng, r.String())
						}
					}
				}
			}
		}
	}
	if checked < 10000 {
		t.Errorf("only %d ranges were checked (%d refused); this sweep is meant to be wide",
			checked, refused)
	}
	t.Logf("%d generated ranges rendered, %d refused, no negative component", checked, refused)
}

// readSemverRangeSource reads the desugaring file by NAME. It walks no
// directory on purpose: a gate that walks the tree sees a git worktree's
// second copy of the source and its verdict then depends on whether anybody
// has one open.
func readSemverRangeSource(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("semver_range.go")
	if err != nil {
		t.Fatalf("reading the desugaring source: %v", err)
	}
	return string(b)
}
