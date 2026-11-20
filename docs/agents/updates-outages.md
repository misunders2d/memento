# Client updates and outages

Detail for [AGENTS.md](../../AGENTS.md). Caveman-compressed; every rule here is binding.

## Explicit native updates and outages

Use `memento agent update HARNESS` with same custom `--config`/`--native-config-dir` as installation. Validates entire private-service code bundle before replacing files; preserves config/client UID/role/outbox/native settings/timers; never restarts active sessions or discovers model. Publication atomic per file, standalone runner last; after interruption rerun when connected to finish. No rollback journal, automatic updater or per-message update request. Load via native reload or fresh session; if hook/MCP definitions change use `agent add`, keeping human trust prompts.

Additional Codex async recall definition: after updating run `memento agent add codex` with same config + native config location, then human reviews it in `/hooks`. Updating files alone neither installs nor trusts that new definition. Pi/OMP need extension reload or fresh session to load new model-request callback.

Ordinary automatic recall also carries server's client-bundle fingerprint. Updated clients show one operational notice while different bundle offered, including headless Pi/OMP request context. No polling or automatic installation; notice not saved as memory or cached recall. Old clients need one explicit update before recognizing notices. Unknown development build or failed recall never claims update available. Fingerprint identifies bundle, not version ordering, not proof interrupted update completed; keep normal rerun + reload steps.

Offline capture stays queued until another native lifecycle hook or explicit upload retries. Idle client: no retry daemon. Bounded automatic recall warns, continues without fresh context (same-query cache usable five minutes, with warning); explicit saves fail closed, not queued as mutations.
