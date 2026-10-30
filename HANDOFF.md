# Handoff: code review, 2026-10-30

Point-in-time review of `main` at `60060f7`. Linear remains the source of truth for design, status and acceptance ([Personal Clone](https://linear.app/mellanni/project/personal-clone-d5c1389b6508)); this file only records review findings for whoever picks up the work. Nothing below has been fixed.

## Scope and verification

- Branches: `main` fast-forwarded `7acf17d..60060f7`. The only other remote branch, `rust-core-comparison-20261025`, is fully merged into `main`.
- Ran: `uv run pytest` on `test_worker.py`, `test_work_api.py`, `test_runner.py`, `test_bridge.py` (253 passed); `cargo test --locked -p memento-core -p memento-domain` (96 passed, 0 failed); `cargo clippy` on the same crates (exit 0, style warnings only).
- Not run: `server`, `ann`, `media`, `model` crates (need the pinned native SDK; host toolchain was rustc 1.99, pinned is 1.98), the full Python suite, anything against the live service.
- Findings 1 and 2 come from reading the code; neither was reproduced.

## New findings

### 1. Poisoned writer mutex causes a silent write outage (high)

The writer is a `std::sync::Mutex<Connection>`; once poisoned, every write returns `Unavailable("Evidence writer requires recovery")` until restart (`crates/core/src/store.rs:240-244`). There is no `panic = "abort"` profile, and there are about 749 `unwrap`/`expect` calls in non-test source, many on model-supplied JSON inside write closures (for example `crates/domain/src/consolidation_apply.rs:680`). `/health` probes a separate SQLite connection (`crates/core/src/compact.rs:53-60`, called from `crates/server/src/lib.rs:404-406`), so health stays 200, the Docker healthcheck stays green and `restart: unless-stopped` never fires.

Fix: report `writer.is_poisoned()` from health, or recover with `PoisonError::into_inner` (the dropped transaction has already rolled back) and log once.

### 2. One failed lease renewal kills the whole worker run (high)

`Leases._run` marks a job lost after a single failed `/work/renew` (`src/memento/runner.py:554-565`); `tick` then raises `worker lease lost before completion` (`src/memento/worker.py:123-127`). Renewal goes through `store.write` (`crates/domain/src/jobs.rs:165-167`) and so waits behind the same writer mutex as owner `compact` (held for the whole VACUUM by design) and large consolidation commits. One renewal that exceeds its `min(15, lease/8)` timeout throws away minutes of in-flight model work.

Fix: treat only an explicit `renewed:false`, or failure close to the real lease expiry, as lost.

### 3. Harden admission on the REST routes (security)

The Host/Origin admission check (`crates/server/src/mcp.rs:40-76`) is applied only to `/mcp/` and `/ui`. The other REST routes, including all mutating ones (`crates/server/src/lib.rs:364-386`), do not run it, and `json_body` accepts a request with no `Content-Type` (`crates/server/src/lib.rs:291-304`). The viewer design already identified the unauthenticated mutating API as the main risk but fenced only `/ui`.

Fix: require `application/json` on JSON POST bodies (reject a missing header) and apply the existing `admit` check to every route as middleware. Shipped clients already send `application/json` (`src/memento/runner.py:286`, `src/memento/bridge.py:112`, `src/memento/native_worker.ts:25`); verify the container healthcheck and tailnet clients still pass afterwards.

### 4. Automatic recall injects noise (baseline clause 1)

Observed in the reviewing session itself:

- Harness-generated Claude Code `<task-notification>` messages fire the recall hook as if they were human prompts. Only `[HERDR AGENT MESSAGE]` is exempt (`src/memento/bridge.py:942`, `:1055`).
- The anchor tokenizer splits on `/` (`crates/server/src/anchors.rs:8-19`), so a path such as `.../projects/memento` anchored the entity "Projects", and the word "Claude" anchored the Claude tool entity.
- An unrelated image media reference was injected for a code-review request (a live instance of MEL-703).

Fix: exempt or cache-reuse harness notifications the same way as Herdr messages, and strip path/URL spans before anchor matching. The second change touches the owner's "no name-length or language rules" decision, so it needs owner approval.

### 5. Model-initiated releases are recorded as service rejections (medium)

The adapter always releases with `error: "service_rejected"` (`src/memento/native_worker.ts:99`) and the server persists it on the job (`crates/domain/src/processing.rs:597`). A model voluntarily giving work back (the observed Terra claim → release pattern) therefore appears as a service rejection in status diagnostics. The client also cools the job for 900 s while the server backoff is 30 s.

Fix: add a `model_released` release category and align or document the two backoffs.

### 6. No CI (medium)

There is no `.github/` directory; all verification is manual agent runs, and the known ANN flake is not tracked anywhere automatic. Minimal CI: pytest, `cargo test` for `memento-core` and `memento-domain` (no native SDK required), and clippy correctness lints as hard failures.

### 7. Rust formatting density blocks review (medium)

In `crates/`, 555 lines exceed 200 characters, 82 exceed 500 and 8 exceed 1000. `consolidation_apply.rs:680` runs about ten fence checks on one line. Diffs on such code are effectively unreviewable for humans and agents. Fix: enforce `rustfmt` in CI and reformat incrementally.

### 8. Documentation size (low-medium)

`AGENTS.md` is about 77 KB (roughly 19k tokens loaded into every agent turn in this repo) and `README.md` about 88 KB. `AGENTS.md` duplicates design and status even though it states that Linear is the source of truth. Suggestion: a one-screen rules file with pointers, and the operating runbook moved to `docs/operations.md`.

### 9. Two implementations to maintain (owner decision)

Alongside the Rust service (about 44k lines), the repo still carries the Python service path and about 22.5k lines of Python tests. This is a recurring maintenance cost to budget for, not necessarily something to remove.

## Known issues confirmed still present

- **Submit/abort race (MEL-715).** When a lease is lost but the job is already done, the host sends `abort` (`src/memento/worker.py:123-136`) while the native `submit` may still be in flight. `submit` commits and then claims the next work in the same request (`crates/domain/src/processing.rs:186-198`), so the aborted client never sees that next lease; it is not released in `finally` and stays idle until expiry. Fix: when the lost job is already done, do not abort; let the pending tool result settle.
- **Comparison allocator keys on `type + first word`** (`crates/domain/src/consolidation_context.rs:681-690`), whereas `AGENTS.md` says candidates sharing a *discovered subject* share comparison slots. This explains the collision of many candidates starting with "In". Deliberately deferred until the 16/36/64 batch-size comparison.
- **Tracked in Linear:** MEL-703 (image relevance), MEL-704 (status and deep-recall latency), MEL-705 (Codex spool backlog and conflicts), MEL-689 (late recall), MEL-669 (owner `compact` not yet run on live data), MEL-712 (legacy replacement-chain recovery not run), MEL-707 (loop audit), the ANN test flake, and the excluded instruction-only legacy candidates.
- **Capacity is the main strategic risk.** Native model invocation dominates processing time, and the candidate and indexing backlogs were not converging during catch-up (see the MEL-715 capacity checkpoint). Batch sizing and bounded parallelism, as recommended in Memory 2, matter more than any of the fixes above.

## Suggested order

1. Findings 1 and 2: small, contained.
2. Submit/abort race.
3. Finding 3 (admission hardening).
4. Finding 4 (recall hygiene), after the owner decision on anchors.
5. CI with `rustfmt` and clippy.
6. `AGENTS.md` split.
7. Allocator subject key, after the batch-size comparison.

Nobody is currently assigned to any of these.
