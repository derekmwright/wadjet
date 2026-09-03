package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGithubSlug(t *testing.T) {
	cases := []struct {
		name string
		text string
		want string
	}{
		{"plain", "Getting Started", "getting-started"},
		{"code-span-flag", "`serve` Command", "serve-command"},
		{"numbered", "1. Run scenarios", "1-run-scenarios"},
		{"parens", "Common Table Expressions (CTEs)", "common-table-expressions-ctes"},
		{"bold", "Two-level group index **conversion**", "two-level-group-index-conversion"},
		{"italic", "What this does *not* change", "what-this-does-not-change"},
		{
			"code-span-with-internal-punctuation",
			"`walkStages` per-node behavior (`plan.go:2919`)",
			"walkstages-per-node-behavior-plango2919",
		},
		{
			"flag-name-in-parens-preserves-hyphens",
			"Async scratch purge (`--async-scratch-purge`)",
			"async-scratch-purge---async-scratch-purge",
		},
		{
			"wildcard-inside-code-span-not-treated-as-emphasis",
			"Delete `PickAggregateShuffleCandidate*` and threshold vars",
			"delete-pickaggregateshufflecandidate-and-threshold-vars",
		},
		{"underscore-word-kept", "docs/README.md", "docsreadmemd"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := githubSlug(tc.text)
			if got != tc.want {
				t.Errorf("githubSlug(%q) = %q, want %q", tc.text, got, tc.want)
			}
		})
	}
}

func TestHeadingSlugsDuplicatesGetNumberedSuffixes(t *testing.T) {
	content := "# Overview\n\n## Setup\n\nSome text.\n\n## Setup\n\nMore text.\n\n## Setup\n"
	got := headingSlugs(content)
	want := []string{"overview", "setup", "setup-1", "setup-2"}
	if !equalSlices(got, want) {
		t.Errorf("headingSlugs() = %v, want %v", got, want)
	}
}

func TestHeadingSlugsSkipsFencedCode(t *testing.T) {
	content := "# Real Heading\n\n```bash\n# Not a heading\n## Also not a heading\n```\n\n## Second Real Heading\n"
	got := headingSlugs(content)
	want := []string{"real-heading", "second-real-heading"}
	if !equalSlices(got, want) {
		t.Errorf("headingSlugs() = %v, want %v", got, want)
	}
}

func TestStripFencesPreservesLineCount(t *testing.T) {
	content := "line1\n```go\ncode line\n```\nline5\n"
	lines := stripFences(content)
	origLines := len(splitLines(content))
	if len(lines) != origLines {
		t.Fatalf("stripFences changed line count: got %d, want %d", len(lines), origLines)
	}
	if lines[0] != "line1" || lines[4] != "line5" {
		t.Errorf("stripFences altered non-fenced lines: %#v", lines)
	}
	if lines[1] != "" || lines[2] != "" || lines[3] != "" {
		t.Errorf("stripFences left fenced content in place: %#v", lines)
	}
}

func TestExtractLinkTargetsAndSplitFragment(t *testing.T) {
	line := "See [a](foo.md#bar) and [b](https://example.com/x) and [c](#local) and ![img](pic.png)."
	targets := extractLinkTargets(line)
	want := []string{"foo.md#bar", "https://example.com/x", "#local", "pic.png"}
	if !equalSlices(targets, want) {
		t.Fatalf("extractLinkTargets() = %v, want %v", targets, want)
	}

	pathPart, anchorPart, hasAnchor := splitFragment("foo.md#bar")
	if pathPart != "foo.md" || anchorPart != "bar" || !hasAnchor {
		t.Errorf("splitFragment(foo.md#bar) = (%q, %q, %v)", pathPart, anchorPart, hasAnchor)
	}
	pathPart, _, hasAnchor = splitFragment("foo.md")
	if pathPart != "foo.md" || hasAnchor {
		t.Errorf("splitFragment(foo.md) unexpectedly has an anchor")
	}
	pathPart, anchorPart, hasAnchor = splitFragment("#local")
	if pathPart != "" || anchorPart != "local" || !hasAnchor {
		t.Errorf("splitFragment(#local) = (%q, %q, %v)", pathPart, anchorPart, hasAnchor)
	}

	if !isExternal("https://example.com/x") || isExternal("foo.md") || isExternal("") {
		t.Errorf("isExternal misclassified a target")
	}
}

// TestCheckCatchesBrokenLinksAndAnchors builds a small synthetic doc tree
// (so this test never depends on, or rots with, the state of the real
// docs/) and asserts every violation class the gate promises: a missing
// file, a missing heading anchor, and a docs/*.md page unreachable from
// docs/README.md. It also asserts the things that must NOT be flagged:
// external URLs, directory links, and a same-file anchor.
func TestCheckCatchesBrokenLinksAndAnchors(t *testing.T) {
	root := t.TempDir()
	mustWriteFile(t, filepath.Join(root, "docs", "README.md"), `# Docs

- [Reachable](reachable.md)
`)
	mustWriteFile(t, filepath.Join(root, "docs", "reachable.md"), `# Reachable

## Section One

Links: [ok anchor](#section-one), [missing file](nope.md), [missing anchor](reachable.md#no-such-heading).
`)
	mustWriteFile(t, filepath.Join(root, "docs", "orphan.md"), `# Orphan

Nobody links here.
`)
	mustWriteFile(t, filepath.Join(root, "docs", "design", "note.md"), `# Design Note

External: [anthropic](https://www.anthropic.com). Directory: [design dir](.).
`)

	violations, err := Check(root)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}

	assertContains(t, violations, "broken link", `"nope.md"`)
	assertContains(t, violations, "broken anchor", `"no-such-heading"`)
	assertContains(t, violations, "orphan.md is not reachable from docs/README.md", "")

	// Exactly those three: the external URL, the directory link, and the
	// valid same-file anchor must not add a fourth.
	if len(violations) != 3 {
		t.Errorf("got %d violations, want exactly 3:\n%s", len(violations), strings.Join(violations, "\n"))
	}
}

func TestCheckOnLiveTree(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "docs", "README.md")); err != nil {
		t.Skipf("repo root not found at %s (unexpected working directory for `go test`): %v", root, err)
	}
	violations, err := Check(root)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("docs tree has %d link/anchor/reachability violation(s):\n%s",
			len(violations), strings.Join(violations, "\n"))
	}
}

func mustWriteFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func assertContains(t *testing.T, violations []string, kind, substr string) {
	t.Helper()
	for _, v := range violations {
		if strings.Contains(v, kind) && (substr == "" || strings.Contains(v, substr)) {
			return
		}
	}
	t.Errorf("expected a %q violation containing %s, got:\n%s", kind, substr, strings.Join(violations, "\n"))
}

func splitLines(s string) []string {
	return strings.Split(s, "\n")
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
