# Objstore object key component rule

Source: internal/storage/objstore/object_key.go — func ValidateObjectKey(key string) error {, moved 2026-09-11 (#1026)
Superseded: The guarantee here is lexical containment; symlink traversal needs filesystem-level protection. filepath.VolumeName recognizes volume paths according to the host platform.

ValidateObjectKey is the ONE rule for what an object key may be, and every
store asks it, so a key one store accepts is a key they all accept.

The rule: a key is one or more "/"-separated components; no component is
empty, "." or ".."; no byte is NUL; the key is neither absolute nor a
Windows volume path and carries no backslash. That is exactly the set for
which `filepath.Join(root, bucket, filepath.FromSlash(key))` is guaranteed
to stay under `root/bucket`.

It exists because FileStore had no such rule and filepath.Join CLEANS its
result: `filepath.Join(root, bucket, "../../../tmp/x")` is a path outside
the root, with no error and no ".." left to see. The keys this store is
handed are built from user-controlled names — a table's data lives under
`tables/<name>/` (partition.TablePrefix) and a table name is a SQL
identifier, which a DOUBLE-QUOTED spelling takes verbatim — so
`CREATE TABLE "../../../tmp/x"` was an arbitrary file write anywhere the
process could reach, on the supported `storage.type: file` deployment
(CodeQL go/path-injection alerts #23, #24 and #25: Get's open, Put's temp
file, and Put's rename into place).

The catalog refuses such a NAME as well, at CREATE, which is where a person
can be told what is wrong. This is the layer that has to be right anyway:
the catalog is one of several callers, and a store cannot know what its keys
were made of.
