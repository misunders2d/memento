# memento

Agent memory system for Sergey's always-on personal assistant (Personal Clone).

**Linear is the source of truth.** Repo holds code only; do not duplicate design or status here.

Owner-controlled **Memento: immutable top-level requirement** in [Personal Clone project description](https://linear.app/mellanni/project/personal-clone-d5c1389b6508) governs implementation + acceptance. Only explicit owner amendment changes that baseline; implementation limits do not weaken it.

- Project: [Personal Clone](https://linear.app/mellanni/project/personal-clone-d5c1389b6508)
- Design record: [Memory 2](https://linear.app/mellanni/document/memory-2-4d1f54cbf034)
- Origin issue: [MEL-657](https://linear.app/mellanni/issue/MEL-657)

**Completion housekeeping:** after material implementation or deployment changes, reconcile Linear project overview, latest status update + affected issues with verified result; add dated implementation note to design record when its architecture or operating assumptions change. Check GitHub README + project `AGENTS.md` for affected setup, migration, recovery, development instructions. Preserve owner-controlled baseline + historical decisions; distinguish shipped code, verified deployment, acceptance evidence, open limitations. Commit/push authorized source-document changes; verify remote result before reporting handover complete. Update blocked or outside authorized scope: report exact gap + next action, never leave stale docs unmentioned.

**Clean coding handover:** finish authorized integration + local rollout, commit/push `main`, remove completed task branches + worktrees before reporting done. Preserve unmerged work before removing its branch; never discard unknown changes. Leave repo clean: no completed-task checkout left for owner to finish.

**Reuse before building:** before designing or replacing pipeline/worker machinery, inspect maintained existing solutions via primary docs + code. Record reusable components + gaps against owner-controlled baseline in Linear; justify custom machinery only where existing solutions do not fit. Measure complete processing work, including existing-memory maintenance, retries, historical catch-up. Intermediate queue counts alone do not establish daily capacity or overnight completion.

**Workflow and API first:** translate owner's intended workflow into observable acceptance before tuning implementation. Prefer existing Memento HTTP/MCP interface + shared processing contract over separate agent-specific engines, brokers or duplicated state. Pi, Claude, Codex, OMP, AGY = clients of that contract; qualify automatic native execution separately from API compatibility. Add adapter or abstraction only for demonstrated missing capability. Separate measured startup, model, token, commit costs; persistent process alone does not prove lower cost or higher capacity.

**Stop expanding after acceptance:** once agreed implementation + acceptance checks pass, finish authorized rollout + housekeeping. Add check only for concrete unresolved failure or new material risk. Say deployment started only after its command is running; distinguish build, switch, verified runtime. Repeated deployment promises are not progress.

**Background handover:** before reporting worker started, verify committed unit of requested kind + identify its actual service owner. Reap task-owned hung tests; do not confuse them with production work. Report historical admission exclusions explicitly. Owner-authorized `history release-candidates` admits already extracted older candidates without raw-history replay.

**Native agent context:** retain native reasoning + tool history inside unfinished work. Compact completed evidence only after verified commit/release boundary. Current packet + last receipt does not preserve enough state to recover from repeated tool errors; verify real multi-step workflow before claiming persistence works.

## Hard gates (always apply)

Short reminders of rules stated in the detail docs. The detail doc holds the exact wording, conditions and exceptions; read it before acting in that area.

- **Contabo:** do not deploy, test, profile or diagnose Memento on Contabo without new explicit user authorization.
- **Human/operator steps, not agent commands:** fresh Compose installation, `sudo`, `sudo loginctl enable-linger <agent-user>`, and restarting the container after a panic or poisoned writer. Other rollout needs authorization (see **Clean coding handover**, **Stop expanding after acceptance**).
- **Owner socket = service owner/human, never the agent:** review `list|approve|reject`, `forget`, `inventory`/`merge`, `revisions`/`recover-revisions` (never on a real database), and the owner-only `backup`, `compact`, `export`, `history park`, `memento-restore`. No approval field or owner impersonation exists for agents.
- **Isolation:** agent must not share the service UID; no sudo or Docker-daemon access when human-review isolation matters.
- **Network:** never expose this unauthenticated API publicly. Loopback listener + tailnet Serve only; keep MCP host protection on.
- **One writer:** one service process per data directory. Never point the Python comparison service at a native database. No in-place conversion, no lowering the DB header, never two instances accepting writes. Keep `data/` on local Linux storage (not NFS/SMB/FUSE).
- **Acceptance runs:** `tests/memory_acceptance.py` never targets production; it needs a launcher-owned fresh instance. Tests use synthetic text, not secrets. Keep reports private.
- **Do not restart** the service during startup index builds or `compact`.
- **Processing:** one server processing config/timer. Never restart a stopped unscheduled catch-up without owner authorization. Codex server processing stays fail-closed and AGY server processing stays refused; do not claim Codex extraction acceptance.
- **Native trust:** setup never grants native trust, hook or MCP permissions (Codex `/hooks`); never grant them or force an agent continuation. The human approves.
- **Rollout order:** service first, then processing runner, then `memento agent update` on clients. Say deployment started only after its command is running.
- **Secrets/Drive:** never read private credential files into the conversation, print secret values, use ambient Google credentials or broaden scopes beyond `drive.file`. Never create a replacement OAuth client or Drive folder during recovery; never omit the known `--folder-id`. Initial secret provisioning belongs to the service owner; an agent may only use explicitly authorized credential-manager injection, otherwise pause for owner approval.
- **Evidence:** explicit saves need nonempty verbatim quotes; never invent evidence. Forget and derived-memory retirement keep raw history. Agents cannot forget data or approve sensitive changes.
- **Identities/history:** do not copy histories/config identities across devices and claim cross-device deduplication. Do not delete native histories or import processing sessions as human evidence.
- **Website:** deploy only `website/`, never the repository root; never point `memento.bezosapp.uk` at the private memory API.
- **Config:** keep code defaults generic; per-machine values go in the deployment's private configuration.
- **Claims:** a timer firing, queue counts, healthy `/health` or installed files prove nothing; verify a committed unit and actual capture, extraction and recall separately.

## Detail docs (read the matching one BEFORE acting; they are binding, not optional)

| Read before working on | Doc |
| --- | --- |
| running/installing/configuring the service, Compose limits, schema header, model threads, indexing/chunk behavior, shutdown, tailnet publication, Rust/Python dev checks | [docs/agents/service.md](docs/agents/service.md) |
| anything under `website/`, Vercel, Cloudflare DNS, Google branding/OAuth client for the public pages | [docs/agents/website.md](docs/agents/website.md) |
| connecting a harness (`/connect`, `memento agent add`), setting up the one processing host/timer, Codex/AGY limits | [docs/agents/client-setup.md](docs/agents/client-setup.md) |
| Pi worker, `/work` API, claims/leases, extraction windows, consolidation batches/policies (`consolidate-v10`..`v15`), duplicate/identity checks, repairs (`absorb`/`supersede`/`retire`), `POST /work/maintenance` groups, existing-evidence references, automatic `person` memories, legacy Codex instruction exclusion, extraction handles, rejections, first-test sequence, legacy runner, `work_timing` | [docs/agents/processing.md](docs/agents/processing.md) |
| automatic/step recall timing and budgets, context packing, `full_text`, entity anchors, decay, period recall (`memory_since`/`memory_until`), `recall_timing`, `explain` | [docs/agents/recall.md](docs/agents/recall.md) |
| the 8 MCP tools, `remember`/`supersede`/`reconcile`/`history`, relation names, loop responsibility/close/decline/due dates, manual loop housekeeping, quote flush | [docs/agents/memory-tools-loops.md](docs/agents/memory-tools-loops.md) |
| inferred technical lessons, `technical_evidence`, lesson subject links, behavioral-suggestion offer receipts | [docs/agents/learning.md](docs/agents/learning.md) |
| `import-history`/`upload`, historical pool (`history park`), raw offload/compression, `/status` fields, startup index builds, FTS index, `/health` | [docs/agents/history-import-storage.md](docs/agents/history-import-storage.md) |
| `memento media save|status|get`, captured attachment registry, media recall | [docs/agents/media.md](docs/agents/media.md) |
| `memento agent update`, Codex async definition, bundle-fingerprint notice, offline capture | [docs/agents/updates-outages.md](docs/agents/updates-outages.md) |
| Drive connect/reconnect/cold storage, owner socket commands (`backup`, review, `forget`, `inventory`/`merge`, `revisions`, `compact`), `memento-restore` | [docs/agents/drive-admin-recovery.md](docs/agents/drive-admin-recovery.md) |
| `export`, archive restore, `drive-binding.json`, fresh Compose recovery, moving hosts | [docs/agents/export-migration.md](docs/agents/export-migration.md) |

## Local development

Installation, Drive/native-agent polish and production acceptance run locally. Use isolated local clients + private TLS to exercise remote-client behavior. Do not deploy, test, profile or diagnose Memento on Contabo without new explicit user authorization; these operational instructions do not grant it.
