# Archived design docs

Point-in-time plans, specs, and research notes from past workstreams
(March–April 2026), kept for design lineage: why the distribution-property
IR, native-DAG execution, the harness, and the spill machinery took the
shapes they have. Nothing here is maintained. Every document opens with an ARCHIVED banner:
it is a superseded design note and does not describe the current code.
The directory is `docs/_archive/` (leading underscore) so filesystem-walking
gates skip it, and it is listed in the repository's `.ignore` so ripgrep and
editor search skip it by default (`rg --no-ignore` reaches it). Current, maintained documentation lives
in `docs/internals/` (file-anchored code maps) and `docs/design/` (active
design memos).
