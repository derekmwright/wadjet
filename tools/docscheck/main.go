// Command docscheck resolves every relative link and #anchor markdown
// documentation makes against the tree, and checks that every top-level
// docs/*.md page is reachable by following links starting at
// docs/README.md. See `task docs:check`.
//
// Scope: README.md, CONTRIBUTING.md, and every *.md file under docs/
// (recursively). Anchors are validated against GitHub's heading-slug
// rules (lowercase, strip punctuation, spaces become hyphens, duplicate
// headings get -1/-2/... suffixes) applied to the heading's rendered
// text — inline code spans and emphasis markers are stripped the way
// GitHub's renderer would strip them before slugging.
package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func main() {
	root := "."
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	violations, err := Check(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "docscheck:", err)
		os.Exit(2)
	}
	if len(violations) == 0 {
		fmt.Println("docscheck: OK")
		return
	}
	for _, v := range violations {
		fmt.Println(v)
	}
	fmt.Fprintf(os.Stderr, "docscheck: %d violation(s)\n", len(violations))
	os.Exit(1)
}

// Check runs every gate against the tree rooted at root and returns one
// formatted line per violation (empty when the tree is clean).
func Check(root string) ([]string, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolving root: %w", err)
	}

	files, err := docFiles(absRoot)
	if err != nil {
		return nil, fmt.Errorf("collecting doc files: %w", err)
	}

	contents := make(map[string]string, len(files))
	headings := make(map[string]map[string]bool, len(files))
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("reading %s: %w", f, err)
		}
		content := string(b)
		contents[f] = content
		set := make(map[string]bool)
		for _, s := range headingSlugs(content) {
			set[s] = true
		}
		headings[f] = set
	}

	var violations []string
	reachEdges := make(map[string]map[string]bool) // doc file -> set of .md files it links to

	for _, f := range files {
		lines := stripFences(contents[f])
		for lineNo, line := range lines {
			for _, target := range extractLinkTargets(line) {
				pathPart, anchorPart, hasAnchor := splitFragment(target)
				if isExternal(pathPart) {
					continue
				}
				if pathPart == "" && !hasAnchor {
					continue // empty link target; nothing to resolve
				}
				resolved := f
				if pathPart != "" {
					resolved = filepath.Clean(filepath.Join(filepath.Dir(f), pathPart))
				}
				info, statErr := os.Stat(resolved)
				if statErr != nil {
					violations = append(violations, fmt.Sprintf(
						"%s:%d: broken link %q (resolves to %s, which does not exist)",
						rel(absRoot, f), lineNo+1, target, rel(absRoot, resolved)))
					continue
				}
				if info.IsDir() {
					// Directory link (e.g. "../design/"). GitHub renders a
					// listing; no heading anchor applies.
					continue
				}
				if strings.HasSuffix(strings.ToLower(resolved), ".md") {
					if reachEdges[f] == nil {
						reachEdges[f] = make(map[string]bool)
					}
					reachEdges[f][resolved] = true
				}
				if hasAnchor {
					if !strings.HasSuffix(strings.ToLower(resolved), ".md") {
						// Anchors on non-Markdown targets (line ranges on
						// source files, etc.) are not heading anchors.
						continue
					}
					set, ok := headings[resolved]
					if !ok {
						// resolved is a .md file outside the checked set
						// (shouldn't happen: docFiles covers every .md
						// under docs/ plus README.md/CONTRIBUTING.md).
						continue
					}
					if !set[anchorPart] {
						violations = append(violations, fmt.Sprintf(
							"%s:%d: broken anchor %q in link %q (no heading in %s slugs to #%s)",
							rel(absRoot, f), lineNo+1, anchorPart, target, rel(absRoot, resolved), anchorPart))
					}
				}
			}
		}
	}

	violations = append(violations, checkReachability(absRoot, files, reachEdges)...)

	sort.Strings(violations)
	return violations, nil
}

// docFiles returns README.md and CONTRIBUTING.md at root, plus every
// *.md file under root/docs. Directory names starting with "." or "_"
// are skipped — a git worktree nested under .claude/worktrees/ is a
// full second copy of the tree, and walking it a second time makes the
// gate's verdict depend on whether anyone happens to have a worktree
// open (CLAUDE.md, "Test patterns").
func docFiles(root string) ([]string, error) {
	var files []string
	for _, name := range []string{"README.md", "CONTRIBUTING.md"} {
		p := filepath.Join(root, name)
		if _, err := os.Stat(p); err == nil {
			files = append(files, p)
		}
	}
	docsDir := filepath.Join(root, "docs")
	err := filepath.WalkDir(docsDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if d.IsDir() {
			if path != docsDir && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_")) {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(name, ".md") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(files)
	return files, nil
}

// checkReachability asserts every direct child docs/*.md file (not the
// ADR/design/benchmarks/internals/testing/positioning/archive
// subdirectories, which have their own index pages) is reachable from
// docs/README.md by following links transitively.
func checkReachability(root string, files []string, edges map[string]map[string]bool) []string {
	docsDir := filepath.Join(root, "docs")
	start := filepath.Join(docsDir, "README.md")
	if _, err := os.Stat(start); err != nil {
		return []string{"docs/README.md does not exist: reachability gate cannot run"}
	}

	visited := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for next := range edges[cur] {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}

	var violations []string
	for _, f := range files {
		dir := filepath.Dir(f)
		if dir != docsDir {
			continue // not a direct child of docs/
		}
		if filepath.Base(f) == "README.md" {
			continue
		}
		if !strings.HasSuffix(f, ".md") {
			continue
		}
		if !visited[f] {
			violations = append(violations, fmt.Sprintf(
				"%s is not reachable from docs/README.md", rel(root, f)))
		}
	}
	return violations
}

func rel(root, path string) string {
	r, err := filepath.Rel(root, path)
	if err != nil {
		return path
	}
	return filepath.ToSlash(r)
}

// --- Fenced-code-block-aware line scanning ---

// stripFences returns content split into lines, with every line inside a
// ``` or ~~~ fenced code block (including the fence markers themselves)
// replaced with an empty string, so line numbers are preserved while
// code-block content is excluded from both heading and link extraction.
func stripFences(content string) []string {
	lines := strings.Split(content, "\n")
	out := make([]string, len(lines))
	inFence := false
	var marker string
	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !inFence {
			if m := fenceMarker(trimmed); m != "" {
				inFence = true
				marker = m
				continue // fence-open line is blank
			}
			out[i] = line
			continue
		}
		// inFence
		if strings.HasPrefix(trimmed, marker) {
			inFence = false
		}
		// blank either way: the closing marker line, or content inside the fence
	}
	return out
}

func fenceMarker(trimmed string) string {
	if strings.HasPrefix(trimmed, "```") {
		return "```"
	}
	if strings.HasPrefix(trimmed, "~~~") {
		return "~~~"
	}
	return ""
}

// --- Link extraction ---

var linkTargetRe = regexp.MustCompile(`\[[^\[\]]*\]\(([^()]+)\)`)

// extractLinkTargets returns the raw (still percent-un-decoded) target of
// every Markdown link on the line, in order.
func extractLinkTargets(line string) []string {
	matches := linkTargetRe.FindAllStringSubmatch(line, -1)
	if matches == nil {
		return nil
	}
	targets := make([]string, 0, len(matches))
	for _, m := range matches {
		targets = append(targets, strings.TrimSpace(m[1]))
	}
	return targets
}

var externalSchemeRe = regexp.MustCompile(`(?i)^[a-z][a-z0-9+.-]*:`)

// isExternal reports whether a link path (the part before any #anchor)
// is an absolute URL/URI (http, https, mailto, tel, ...) rather than a
// path relative to the repository tree. An empty path (a same-file
// "#anchor" link) is not external.
func isExternal(pathPart string) bool {
	if pathPart == "" {
		return false
	}
	return externalSchemeRe.MatchString(pathPart)
}

// splitFragment splits a link target into its path and #anchor parts.
// hasAnchor distinguishes "path" (no anchor) from "path#" (an explicit,
// empty anchor — treated as present so a stray trailing "#" is still
// checked rather than silently ignored).
func splitFragment(target string) (pathPart, anchorPart string, hasAnchor bool) {
	idx := strings.IndexByte(target, '#')
	if idx == -1 {
		return target, "", false
	}
	return target[:idx], target[idx+1:], true
}

// --- Heading extraction + GitHub anchor slugging ---

var headingLineRe = regexp.MustCompile(`^(#{1,6})\s+(.*?)\s*#*\s*$`)

// headingSlugs returns the GitHub anchor slug of every ATX heading in
// content, in document order, with duplicate slugs suffixed -1, -2, ...
// exactly as GitHub's own slugger does.
func headingSlugs(content string) []string {
	lines := stripFences(content)
	occurrences := make(map[string]int)
	var slugs []string
	for _, line := range lines {
		m := headingLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		base := githubSlug(m[2])
		if base == "" {
			continue
		}
		slugs = append(slugs, nextSlug(base, occurrences))
	}
	return slugs
}

// nextSlug reproduces github-slugger's disambiguation: the first
// occurrence of a slug is unsuffixed; each later occurrence appends
// "-N", where N is chosen (and recorded) so it never collides with a
// heading that already produced that exact suffixed slug.
func nextSlug(base string, occurrences map[string]int) string {
	if _, seen := occurrences[base]; !seen {
		occurrences[base] = 0
		return base
	}
	for {
		occurrences[base]++
		candidate := base + "-" + strconv.Itoa(occurrences[base])
		if _, seen := occurrences[candidate]; !seen {
			occurrences[candidate] = 0
			return candidate
		}
	}
}

var (
	codeSpanRe        = regexp.MustCompile("`+([^`]*)`+")
	mdLinkTextRe      = regexp.MustCompile(`\[([^\[\]]*)\]\([^()]*\)`)
	nonSlugCharRe     = regexp.MustCompile(`[^\p{L}\p{N}_\- ]`)
	codePlaceholderRe = regexp.MustCompile("\x00(\\d+)\x00")
)

// githubSlug converts raw heading markdown text into the anchor GitHub
// assigns it: inline code spans and Markdown links are reduced to their
// rendered text (backticks/brackets are formatting, not content), bold
// /italic asterisks are stripped, the result is lowercased, everything
// that isn't a Unicode letter, digit, underscore, hyphen or space is
// removed, and spaces become hyphens.
func githubSlug(headingText string) string {
	var codeContents []string
	protected := codeSpanRe.ReplaceAllStringFunc(headingText, func(m string) string {
		sub := codeSpanRe.FindStringSubmatch(m)
		codeContents = append(codeContents, sub[1])
		return "\x00" + strconv.Itoa(len(codeContents)-1) + "\x00"
	})
	protected = mdLinkTextRe.ReplaceAllString(protected, "$1")
	protected = strings.ReplaceAll(protected, "*", "")
	protected = codePlaceholderRe.ReplaceAllStringFunc(protected, func(m string) string {
		sub := codePlaceholderRe.FindStringSubmatch(m)
		idx, _ := strconv.Atoi(sub[1])
		return codeContents[idx]
	})

	lower := strings.ToLower(protected)
	stripped := nonSlugCharRe.ReplaceAllString(lower, "")
	return strings.ReplaceAll(stripped, " ", "-")
}
