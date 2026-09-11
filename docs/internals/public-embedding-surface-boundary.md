# Public embedding surface boundary

Source: wadjet/embed.go — type Schema = parquet.Schema, moved 2026-09-11 (#1026)

This file is the whole public surface an OUT-OF-TREE module needs to run the
program in docs/getting-started.md — open a database over local disk or an
S3-compatible store, declare a table, ingest rows, query it — and nothing
else (#805).

Before it, `go get github.com/derekmwright/wadjet/wadjet` succeeded and the
first line of the guide's program did not compile: Config.Store is
objstore.Store, CreateTable takes parquet.Schema and NewIngester takes
ingest.Config, all three under internal/, which Go forbids another module
from importing (`use of internal package … not allowed`). The engine was
embeddable only from inside its own repository.

The fix is deliberately the SMALLEST one that makes the guide true:

  - The three types the guide names are ALIASES. An alias is a second name
    for one type, not a copy and not a wrapper: wadjet.Schema IS
    parquet.Schema, so db.CreateTable takes it unchanged, every in-repo
    caller is untouched, and there is no conversion to keep in step.
  - The stores are CONSTRUCTORS returning objstore.Store, the interface
    Config.Store takes. A caller assigns the result and never names the
    type, which is what the internal rule actually forbids.
  - Nothing else is exported. Config.MetaKV (a persistent catalog) and
    Config.AuthProvider still name internal types with no public
    constructor, so a persistent catalog and in-process ABAC remain
    in-repo-only; docs/embedding.md says so.

The claim that this is enough is not an argument, it is a build:
test/embed/ is a separate module with its own go.mod that imports only
github.com/derekmwright/wadjet/wadjet, and
TestTheGuidesProgramBuildsAndRunsOutOfTree compiles and runs it.
