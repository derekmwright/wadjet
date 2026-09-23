// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"strings"
	"testing"
)

// TestFilterTextLogOnePassOneFail mirrors the recorded-JSON gate but for
// the plain-text shape battery_run.sh's lanes actually write: `go test
// ./passpkg/... ./failpkg/...` (no -v, no -json) — only a failing test's
// own "--- FAIL" block prints, each package prints its summary line once.
func TestFilterTextLogOnePassOneFail(t *testing.T) {
	const log = `--- FAIL: TestBoom (0.00s)
    f_test.go:6: setting up
    f_test.go:7: kaboom
FAIL
FAIL	sample/failpkg	0.003s
ok  	sample/passpkg	0.004s
`
	var buf bytes.Buffer
	exit := FilterTextLog(strings.NewReader(log), &buf)
	out := buf.String()

	if exit != 1 {
		t.Fatalf("exit = %d, want 1", exit)
	}
	if !strings.Contains(out, "FAIL sample/failpkg") {
		t.Errorf("missing failing-package line; got:\n%s", out)
	}
	if !strings.Contains(out, "ok   sample/passpkg") {
		t.Errorf("missing passing-package line; got:\n%s", out)
	}
	if !strings.Contains(out, "--- FAIL: TestBoom  (sample/failpkg)") {
		t.Errorf("missing failing test's own header; got:\n%s", out)
	}
	if !strings.Contains(out, "kaboom") {
		t.Errorf("failing test's buffered output is missing; got:\n%s", out)
	}
}

func TestFilterTextLogAllPass(t *testing.T) {
	const log = "ok  \tsample/passpkg\t0.004s\n?   \tsample/notests\t[no test files]\n"
	var buf bytes.Buffer
	exit := FilterTextLog(strings.NewReader(log), &buf)
	if exit != 0 {
		t.Fatalf("exit = %d, want 0", exit)
	}
	if strings.Contains(buf.String(), "FAIL") {
		t.Errorf("no failure in the input; output must not mention FAIL: %s", buf.String())
	}
}

// TestFilterTextLogTruncated exercises a log cut off mid-run (the process
// was killed): a "--- FAIL" block opens and never gets a package summary
// line. There is no live process to ask, so this must still report a
// failure rather than silently returning 0.
func TestFilterTextLogTruncated(t *testing.T) {
	const log = `--- FAIL: TestBoom (0.00s)
    f_test.go:7: kaboom
`
	var buf bytes.Buffer
	exit := FilterTextLog(strings.NewReader(log), &buf)
	if exit != 1 {
		t.Fatalf("exit = %d, want 1 (truncated log with an open FAIL block)", exit)
	}
}

// TestFilterTextLogToleratesVerbose exercises a -v log fed into the same
// filter: "--- PASS"/"--- SKIP" blocks must be recognized and discarded,
// same as a passing/skipped test in the JSON path.
func TestFilterTextLogToleratesVerbose(t *testing.T) {
	const log = `=== RUN   TestOK
--- PASS: TestOK (0.00s)
=== RUN   TestBoom
--- FAIL: TestBoom (0.00s)
    f_test.go:7: kaboom
FAIL
FAIL	sample/failpkg	0.003s
`
	var buf bytes.Buffer
	exit := FilterTextLog(strings.NewReader(log), &buf)
	out := buf.String()
	if exit != 1 {
		t.Fatalf("exit = %d, want 1", exit)
	}
	if strings.Contains(out, "TestOK") {
		t.Errorf("a passing test's name must never appear in the quiet summary; got:\n%s", out)
	}
	if !strings.Contains(out, "--- FAIL: TestBoom  (sample/failpkg)") {
		t.Errorf("missing failing test's own header; got:\n%s", out)
	}
}
