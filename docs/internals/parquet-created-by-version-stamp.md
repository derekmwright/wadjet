# Parquet created by version stamp

Source: internal/storage/parquet/created_by.go — func CreatedBy() string { return createdByOnce() }, moved 2026-09-11 (#1026)

CreatedBy is the string this package stamps into every file's footer
`created_by`, in the format's own convention:

	wadjet version 0.18.22 (build 8b693f30c1)

The convention is `<library> version <semver>` optionally followed by
`(build <hash>)`. parquet-mr and pyarrow both parse it, and both USE it —
each keys reader-side workarounds for known writer bugs off the writer's
version, which is the whole reason a version belongs here.

Until #456 this was the constant "wadjet (native writer)", so no reader —
ours or anyone's — could tell which wadjet wrote a file. That has already
cost one migration its audit: ADR-0018's two compatibility notes (the
pre-#409 grouping damage and the pre-#429 `DECIMAL(p > 18)` files) both have
to tell an operator to find affected tables by INGEST DATE against a release
date, because the file cannot answer the question itself and the damage is
invisible in the bytes. A file written from here on can be enumerated
instead, and a reader-side workaround for a future such defect becomes
possible at all rather than forcing a re-ingest.

It is computed ONCE, from runtime/debug.ReadBuildInfo, and never
hand-maintained: there is no version constant in this repo to forget to bump
and no -ldflags for a release to remember to pass. A test that pinned the
exact string would need updating every release, so
TestCreatedByCarriesAParsableVersion asserts the SHAPE.

What that reports depends on how the binary was built, and all three answers
are honest rather than tidy:

	installed at a tag   wadjet version 0.18.22 (build 8b693f30c1de)
	built from a commit  wadjet version 0.18.22-0.20260903172650-fd679ae9e742 (build fd679ae9e742)
	go test              wadjet version 0.0.0-devel

The middle one is Go's pseudo-version and it is deliberately not trimmed to
its base tag: that build is NOT v0.18.22, it is a commit after it, and a
migration keying on a version has to be able to tell those apart. `go test`
disables VCS stamping, which is why the third has no build hash — and why a
file written by a test fixture is identifiable as one.
