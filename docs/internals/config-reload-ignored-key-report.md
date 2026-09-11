# Config reload ignored key report

Source: internal/config/manager.go — func (m *Manager) ReloadWithReport(path string) ([]string, error) {, moved 2026-09-11 (#1026)

ReloadWithReport reads the config file, applies it, and returns the
registry keys THE FILE SETS that no subscriber consumes — the ones Apply
preserved.

Apply preserving them is right: the running process is not going to
re-read a startup-only key, and the manager must report the running
configuration. But saying nothing about the part that was ignored is how
an operator edits `worker.max_concurrent`, sees "reloaded", and believes
it took effect. The PUT path answers 409 naming such a key; a file reload
cannot refuse (the file legitimately carries startup-only keys for the
NEXT start), so it reports instead.

The report is FileKeys ∩ !HotReloadable, and it has to be. Diffing the
running config against Load(path) instead names keys the file never
mentions: the running config's default tier is the FLAG's default, while
Load merges over DefaultConfig(), and decision 2 of ADR-0029 exists
precisely because those two differ — DefaultConfig() sets
storage.access_key to "minioadmin" where --access-key defaults to "",
and worker.cache_bytes to 256 MiB where --cache-bytes defaults to 0. That
diff reported three keys on every reload of any file in any deployment
before this, plus one per key taken from a flag or the environment, so
the config-file WATCHER emitted the warning on every legitimate auth
edit and the one true positive arrived buried in sixteen false ones.
