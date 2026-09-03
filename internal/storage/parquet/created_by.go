package parquet

import (
	"runtime/debug"
	"strings"
	"sync"
)

// CreatedBy is the string this package stamps into every file's footer
// `created_by`, in the format's own convention:
//
//	wadjet version 0.18.22 (build 8b693f30c1)
//
// The convention is `<library> version <semver>` optionally followed by
// `(build <hash>)`. parquet-mr and pyarrow both parse it, and both USE it —
// each keys reader-side workarounds for known writer bugs off the writer's
// version, which is the whole reason a version belongs here.
//
// Until #456 this was the constant "wadjet (native writer)", so no reader —
// ours or anyone's — could tell which wadjet wrote a file. That has already
// cost one migration its audit: ADR-0018's two compatibility notes (the
// pre-#409 grouping damage and the pre-#429 `DECIMAL(p > 18)` files) both have
// to tell an operator to find affected tables by INGEST DATE against a release
// date, because the file cannot answer the question itself and the damage is
// invisible in the bytes. A file written from here on can be enumerated
// instead, and a reader-side workaround for a future such defect becomes
// possible at all rather than forcing a re-ingest.
//
// It is computed ONCE, from runtime/debug.ReadBuildInfo, and never
// hand-maintained: there is no version constant in this repo to forget to bump
// and no -ldflags for a release to remember to pass. A test that pinned the
// exact string would need updating every release, so
// TestCreatedByCarriesAParsableVersion asserts the SHAPE.
//
// What that reports depends on how the binary was built, and all three answers
// are honest rather than tidy:
//
//	installed at a tag   wadjet version 0.18.22 (build 8b693f30c1de)
//	built from a commit  wadjet version 0.18.22-0.20260903172650-fd679ae9e742 (build fd679ae9e742)
//	go test              wadjet version 0.0.0-devel
//
// The middle one is Go's pseudo-version and it is deliberately not trimmed to
// its base tag: that build is NOT v0.18.22, it is a commit after it, and a
// migration keying on a version has to be able to tell those apart. `go test`
// disables VCS stamping, which is why the third has no build hash — and why a
// file written by a test fixture is identifiable as one.
func CreatedBy() string { return createdByOnce() }

var createdByOnce = sync.OnceValue(buildCreatedBy)

// createdByLibrary is the library name in the stamp. It is the name of the
// thing that WROTE the file, so it does not track the binary's name.
const createdByLibrary = "wadjet"

// createdByDevelVersion is what a build with no module version reports —
// `go build` in a checkout, and every `go test` binary, where
// debug.ReadBuildInfo gives Main.Version as "(devel)" or "". Spelling it as a
// valid semver pre-release keeps the field parsable by the Apache convention's
// readers rather than handing them "(devel)", which their `version <semver>`
// grammar does not accept. The build hash beside it is what identifies such a
// build, and Go stamps that from VCS by default.
const createdByDevelVersion = "0.0.0-devel"

func buildCreatedBy() string {
	version, build := createdByDevelVersion, ""
	if bi, ok := debug.ReadBuildInfo(); ok && bi != nil {
		if v := normalizeModuleVersion(bi.Main.Version); v != "" {
			version = v
		}
		build = vcsBuildID(bi.Settings)
	}
	if build == "" {
		return createdByLibrary + " version " + version
	}
	return createdByLibrary + " version " + version + " (build " + build + ")"
}

// normalizeModuleVersion turns a Go module version into the semver the
// convention wants, or "" when the build carries none. "(devel)" is what a
// build outside the module proxy reports and it is not a version.
func normalizeModuleVersion(v string) string {
	v = strings.TrimSpace(v)
	if v == "" || v == "(devel)" {
		return ""
	}
	return strings.TrimPrefix(v, "v")
}

// vcsBuildID is the commit the binary was built from, with "-dirty" appended
// when the tree had uncommitted changes — because a file written by a modified
// build must not claim to have been written by the commit it was modified
// from, which is exactly the claim a migration would trust. Go records both
// settings by default for a build inside a repository.
func vcsBuildID(settings []debug.BuildSetting) string {
	rev, dirty := "", false
	for _, s := range settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return ""
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		return rev + "-dirty"
	}
	return rev
}

// CreatedByInfo is a parsed `created_by`. Ok is false for a string that does
// not follow the convention — which every file written before #456 is, and
// every file from a writer that spells its own field differently.
type CreatedByInfo struct {
	Library string // "wadjet", "parquet-mr", "parquet-cpp-arrow", …
	Version string // semver, with no leading "v"
	Build   string // the "(build …)" hash, empty when the writer stamped none
	Ok      bool
}

// ParseCreatedBy reads the Apache `created_by` convention,
// `<library> version <semver> (build <hash>)`, so a migration can ask a file
// which writer produced it instead of asking the catalog when it was ingested.
//
// A string that does not follow the convention returns Ok=false with Library
// set to the whole string, because that is still the most useful thing a
// caller can report about it — "wadjet (native writer)", the pre-#456 stamp,
// lands there.
func ParseCreatedBy(s string) CreatedByInfo {
	s = strings.TrimSpace(s)
	lib, rest, found := strings.Cut(s, " version ")
	if !found || lib == "" || rest == "" {
		return CreatedByInfo{Library: s}
	}
	version, build := rest, ""
	if i := strings.Index(rest, " (build "); i >= 0 {
		version = rest[:i]
		if b := strings.TrimSuffix(rest[i+len(" (build "):], ")"); b != rest[i+len(" (build "):] {
			build = b
		}
	}
	version = strings.TrimSpace(version)
	if version == "" {
		return CreatedByInfo{Library: s}
	}
	return CreatedByInfo{Library: lib, Version: version, Build: build, Ok: true}
}

// CreatedBy returns the writer identification from this file's footer, exactly
// as it was stamped. See ParseCreatedBy for the convention it follows.
func (f *FileReader) CreatedBy() string {
	if f == nil || f.meta == nil {
		return ""
	}
	return f.meta.CreatedBy
}

// CreatedBy returns the writer identification from this file's footer.
func (r *Reader) CreatedBy() string {
	if r == nil {
		return ""
	}
	return r.fr.CreatedBy()
}
