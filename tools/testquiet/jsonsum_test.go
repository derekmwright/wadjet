// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

// TestSummarizeRecordedOnePassOneFail feeds Summarize recorded `go test
// -json` output — testdata/recorded_one_pass_one_fail.json, captured with
// `go test -json ./passpkg/... ./failpkg/...` against two small fixture
// packages, one passing (sample/passpkg) and one with a single failing
// test (sample/failpkg) — and asserts the quiet summary names both
// packages with the right status, shows the failing test's name and its
// buffered output, never shows the passing test, and returns exit 1.
func TestSummarizeRecordedOnePassOneFail(t *testing.T) {
	data, err := os.ReadFile("testdata/recorded_one_pass_one_fail.json")
	if err != nil {
		t.Fatalf("reading fixture: %v", err)
	}

	var buf bytes.Buffer
	exit := Summarize(bytes.NewReader(data), &buf)
	out := buf.String()

	if exit != 1 {
		t.Fatalf("exit = %d, want 1 (one of the two packages failed)", exit)
	}
	if !strings.Contains(out, "PASS sample/passpkg") {
		t.Errorf("missing passing-package line; got:\n%s", out)
	}
	if !strings.Contains(out, "FAIL sample/failpkg") {
		t.Errorf("missing failing-package line; got:\n%s", out)
	}
	if !strings.Contains(out, "--- FAIL: TestBoom  (sample/failpkg)") {
		t.Errorf("missing failing test's own header; got:\n%s", out)
	}
	if !strings.Contains(out, "kaboom") {
		t.Errorf("failing test's buffered output (the t.Fatal message) is missing; got:\n%s", out)
	}
	if strings.Contains(out, "TestOK") {
		t.Errorf("a passing test's name must never appear in the quiet summary; got:\n%s", out)
	}
}

func TestSummarizeAllPass(t *testing.T) {
	const events = `
{"Action":"start","Package":"p"}
{"Action":"run","Package":"p","Test":"TestA"}
{"Action":"output","Package":"p","Test":"TestA","Output":"--- PASS: TestA (0.00s)\n"}
{"Action":"pass","Package":"p","Test":"TestA","Elapsed":0}
{"Action":"pass","Package":"p","Elapsed":0.01}
`
	var buf bytes.Buffer
	exit := Summarize(strings.NewReader(events), &buf)
	if exit != 0 {
		t.Fatalf("exit = %d, want 0", exit)
	}
	if strings.Contains(buf.String(), "FAIL") {
		t.Errorf("no failure in the input; output must not mention FAIL: %s", buf.String())
	}
}

// TestSummarizeBuildFailure exercises the ImportPath-keyed build-fail
// shape (captured separately: a package that fails to compile reports
// build-output/build-fail events under ImportPath="<pkg>.test", not the
// Package field the rest of the stream uses).
func TestSummarizeBuildFailure(t *testing.T) {
	const events = `
{"ImportPath":"sample/broken.test","Action":"build-output","Output":"# sample/broken\n"}
{"ImportPath":"sample/broken.test","Action":"build-output","Output":"broken/broken_test.go:3:23: expected ')', found '{'\n"}
{"ImportPath":"sample/broken.test","Action":"build-fail"}
{"Action":"start","Package":"sample/broken"}
{"Action":"output","Package":"sample/broken","Output":"FAIL\tsample/broken [setup failed]\n"}
{"Action":"fail","Package":"sample/broken","Elapsed":0,"FailedBuild":"sample/broken.test"}
`
	var buf bytes.Buffer
	exit := Summarize(strings.NewReader(events), &buf)
	out := buf.String()
	if exit != 1 {
		t.Fatalf("exit = %d, want 1", exit)
	}
	if !strings.Contains(out, "FAIL sample/broken") {
		t.Errorf("missing failing package line; got:\n%s", out)
	}
	if !strings.Contains(out, "--- FAIL: [build error]  (sample/broken)") {
		t.Errorf("missing build-error entry; got:\n%s", out)
	}
	if !strings.Contains(out, "expected ')', found '{'") {
		t.Errorf("missing compiler diagnostic; got:\n%s", out)
	}
}

func TestTail(t *testing.T) {
	lines := make([]string, 50)
	for i := range lines {
		lines[i] = string(rune('a' + i%26))
	}
	got := tail(lines, 40)
	if len(got) != 40 {
		t.Fatalf("len = %d, want 40", len(got))
	}
	if got[0] != lines[10] {
		t.Errorf("tail did not keep the last 40 elements")
	}

	short := []string{"x", "y"}
	if got := tail(short, 40); len(got) != 2 {
		t.Errorf("tail of a short slice should return it unchanged, got %v", got)
	}
}
