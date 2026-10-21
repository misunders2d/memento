# memento

Agent memory system for Sergey's always-on personal assistant (Personal Clone).

**Linear is the source of truth.** This repo holds code only; do not duplicate design or status here.

- Project: [Personal Clone](https://linear.app/mellanni/project/personal-clone-d5c1389b6508)
- Design record: [Memory 2](https://linear.app/mellanni/document/memory-2-4d1f54cbf034)
- Origin issue: [MEL-657](https://linear.app/mellanni/issue/MEL-657)

## Local development

Installation, Drive/native-agent polish and production acceptance run locally. Use isolated local clients and private TLS to exercise remote-client behavior. Do not deploy, test, profile or diagnose Memento on Contabo without new explicit user authorization; these operational instructions do not grant it.

### Service (local Linux storage only)

Python >=3.12; one service process per data directory. For a non-container development service: `uv sync --extra media --extra embedding --extra drive`, then `MEMENTO_EMBEDDINGS=1 uv run memento --data-dir ./data`. Tests: `uv run pytest -q`. API contracts: `http://127.0.0.1:8765/docs`.

Fresh Compose installation (human/operator steps, not agent commands):
```sh
sudo install -d -o 10001 -g 10001 -m 0700 ./data
docker compose up --build -d
curl --fail http://127.0.0.1:8765/health
```
Wait for healthy real-model startup; first download needs network, disk space and any Hugging Face model access/license acceptance required by the pinned `google/embeddinggemma-2` revision in `retrieval.py`. Cache persists under `/data/models`. Do not substitute disabled embeddings for acceptance. Compose uses dedicated UID/GID 10001, read-only root, 2 CPUs, 5 GiB RAM, 256 PIDs, dropped capabilities and `/tmp` tmpfs. Keep `data/` on local Linux storage (not NFS/SMB/FUSE); the agent must not share the service UID. Overrides: `MEMENTO_UID`, `MEMENTO_GID`, `MEMENTO_AGENT_UID`, `MEMENTO_DATA_DIR`, `MEMENTO_PORT`; optional `.env` or `MEMENTO_ENV_FILE`.

The default uses one CPU thread per inference lane: foreground recall can run alongside one background encoder without waiting for a whole long document or oversubscribing the two-CPU quota. Mutable model preprocessing remains serialized; float32 precision, prompts, revision and dimensions are unchanged. Keep `OMP_NUM_THREADS=1` under this resource budget; raising it can cause automatic recall to miss its deadline during indexing.

Explicit text is tokenized literally, including model control-token spellings; image/audio preprocessing remains native. A one-time text-encoding upgrade re-encodes only affected legacy text vectors without changing raw history, vector IDs or ordinary text/media vectors. It makes affected failed units due again; only successful normal indexing resolves those failure records. Passage chunking uses the same literal-token mode under the shared preprocessing lock.

Shutdown finishes the current indexing unit, commits completed vectors and leaves the rest pending instead of draining a whole batch before exit. Restart resumes that unfinished work.

Publication stays loopback-only; never expose this unauthenticated API publicly. On the server, set `MEMENTO_ALLOWED_HOSTS=<machine>.<tailnet>.ts.net` in `.env`, recreate the memory container with `docker compose up -d` to apply the environment, then run `tailscale serve --bg 8765` (tailnet HTTPS enabled). Serve proxies to the unchanged loopback listener; tailnet ACLs must restrict access to your devices. MCP host protection stays enabled; `/mcp/` is the endpoint. On each laptop, ask the native agent to connect using `https://<machine>.<tailnet>.ts.net/connect`; that private guide is the canonical client bootstrap instruction. The agent must have neither sudo nor Docker-daemon access when human-review isolation matters.

### Public information website (static Vercel hosting)

`website/` contains only the public product, privacy and terms pages, not a memory dashboard or Google sign-in. Import this repository into Vercel with **Root Directory `website`**, **Framework Preset Other**, and **Output Directory `.`**. Its checked-in `vercel.json` disables installation and build commands and sets response headers and canonical trailing slashes. There are no dependencies, environment variables, functions, API rewrites or server containers. Do not deploy the repository root.

For a local routing preview with a current Vercel CLI, without linking a project or deploying:
```sh
vercel dev --local --cwd website --listen 127.0.0.1:18765
```

Add `memento.bezosapp.uk` to the Vercel project's production domains. In Cloudflare's `bezosapp.uk` zone, add the `memento` **DNS-only CNAME using the exact project-specific target Vercel provides**. Keep the existing nameservers and other records. Keep the production pages publicly readable, without a Vercel authentication challenge, so Google can fetch the policy URLs.

No Contabo changes are needed for this website: do not add a Cloudflare Tunnel route, run an extra website container, open public ports, or point this hostname at the private memory API. The memory service remains private and independent.

After the pages are publicly reachable, use these Google Auth Platform branding values in the existing project:
- Application home page: `https://memento.bezosapp.uk/`
- Privacy policy: `https://memento.bezosapp.uk/privacy/`
- Terms of service: `https://memento.bezosapp.uk/terms/`
- Authorized domain: `bezosapp.uk`

Before requesting Google brand verification, verify ownership of `bezosapp.uk` in Google Search Console using a Google account associated with this project as Owner or Editor. If needed, add Google's exact verification TXT record in Cloudflare without changing other DNS records. Personal-use apps can be exempt from review; domain verification and branding approval are not implied by hosting the pages. See [Google's authorized-domain requirements](https://developers.google.com/identity/protocols/oauth2/production-readiness/brand-verification#authorized-domains).

Keep the existing **Desktop app** OAuth client and loopback callback; this site needs no Web client or public OAuth callback. Publishing the External app **In production** avoids the seven-day Testing refresh-token limitation. If the connection was authorized in Testing, have the human re-consent after publishing and install the private connection as described below. Public pages, project administration and publishing do not themselves grant `drive.file` access; native Google consent remains an explicit human step.

### Native capture and processing

For ordinary agent connection, give the agent the private service's `/connect` URL: it downloads the stdlib runner, exposes it as `memento`, and runs `memento agent add HARNESS --url SERVICE_ORIGIN` using the existing native login. The command reuses the shipped installer, with no new daemon, registry or per-message work. The human does not run commands or edit config files; mandatory native trust/permission prompts remain explicit approvals. A matching default config keeps its role/identity; other harnesses use `${XDG_CONFIG_HOME:-$HOME/.config}/memento/HARNESS/runner.json`. Reuse `--config PATH` for an existing custom location, including later commands, or pass `drive-connect --url URL` directly. Client setup needs no timezone or model API key.

The following commands set up the **one designated processing host**, as the ordinary host agent user, **not** service UID/root. Install/login to the chosen native CLI normally first; processing uses its own default model/login, never a separate paid model API key. Download the stdlib-only runner from the trusted local service:
```sh
URL=http://127.0.0.1:8765
install -d -m 0700 "$HOME/.local/share/memento"
curl --fail "$URL/runner" -o "$HOME/.local/share/memento/runner.py"
python3 "$HOME/.local/share/memento/runner.py" setup --agent pi --timezone Europe/Berlin --url "$URL"
```
Choose `--agent pi`, `omp`, `claude` or `codex`; server processing requires an explicit IANA timezone (replace `Europe/Berlin`). Setup is prompt-free, installs the selected native hooks/extensions and MCP definitions, and writes private `${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json` plus sibling `bridge.py`. Server setup installs/activates `~/.config/systemd/user/memento-runner.{service,timer}`: boot +2 minutes, then every 15 minutes after completion. For unattended headless processing, the human/operator must run `sudo loginctl enable-linger <agent-user>` so the user manager and timer survive logout. Use one server processing config/timer; `memento agent add HARNESS` installs other harnesses without model discovery, generation or another processing timer. Reinstallation preserves the config's stable client identity. Optional `--config PATH` selects a separate private config; use it consistently below. `--no-activate` installs the processor without starting the user timer.

`memento agent add HARNESS` never creates a server processor or demotes an existing server config; it only reinstalls that harness's integration. Keep the single server's `setup --agent HARNESS --timezone ZONE` flow. A failed native reinstall restores the previous runner config rather than silently changing its service URL or mode.

Restart native sessions after installation. **Codex needs human `/hooks` trust and MCP permissions; setup never grants them. Codex server processing stays fail-closed: tested 0.163.0 retains `functions.exec`/`wait` declarations and undeclared `apply_patch` routing, so read-only/approval policy is not tools-zero. Codex capture/import/recall/MCP client interfaces remain available after native consent; do not claim Codex extraction acceptance.** Choose Pi/OMP/Claude for the single server processor; their native safety-flag checks enforce tool-free processing and leave failed work pending.

Pi/OMP automatic recall distinguishes an unavailable service from a healthy empty result: it warns once per outage and clears that warning state after a successful request. A failed same-query request may use context cached for up to five minutes, but still warns; it is not proof that the connection is healthy.

First-test sequence (use synthetic text, not secrets):
1. In a real native conversation state a distinctive durable fact. Confirm raw capture via `GET /episodes/search?q=...`, then fetch `GET /episodes/{id}` for exact text/source/conversation/time.
2. Compact using the harness's native compaction action, or leave the conversation inactive for 30 minutes. Compaction marks a captured sequence boundary; inactivity processes through the latest sequence. Client compaction only nudges the server; the server timer/tick drains due work. Server-side compaction also drains immediately.
3. On the processing host run:
   ```sh
   python3 "$HOME/.local/share/memento/runner.py" --config "${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json" tick --consolidate-now
   ```
   This single first-test flag bypasses only the consolidation night window, not extraction eligibility, and does not alter the schedule. Normal consolidation runs within 02:00–07:00 in the configured timezone. Inspect `journalctl --user -u memento-runner.service` and candidate `history(id)` for exact quotes, source spans, requested/reported model and known usage; do not infer success from a timer firing.
   Tick prints exact backlog counts for pending extraction conversations, pending jobs and leased jobs—not estimated message counts. Each extraction call packs at most 64 windows and 48,000 projected source characters; long user text continues with exact overlap, while tool output is projected to 2,000 characters without changing raw history.
   Extraction admission alternates between the oldest unfinished captured evidence and the newest eligible conversation. This lets a finite imported prefix progress while recent conversations also receive processing time. The order survives restart in existing job records; compaction/inactivity eligibility and retry identities stay unchanged.
   During a long drain, a failed job can be retried after a 15-minute wait following its batch, while unrelated work continues. Tick still exits nonzero if any attempt failed, even if a later retry succeeded.
   Failed attempts emit JSON diagnostics to stderr/the processor journal with job ID, work kind, error category and available HTTP status/allowlisted rejection code or safe cause. They exclude memory text, model output, URLs and arbitrary server error messages; use the receipt to distinguish failures rather than infer a cause from backlog counts.
4. In both the originating conversation and a second fresh native session call MCP `recall` for the fact; candidates must be visible as unconfirmed before promotion. Follow with `history(id)` to verify original evidence. Require actual capture/extraction/recall evidence separately for each harness, not just installed files.
5. For Pi/OMP automatic injection, ask a fresh native session about the synthetic fact without mentioning its answer or asking it to call a memory tool. Verify the answer cites the same memory ID, the evidence resolves to the original conversation, and injected context is not added to persisted raw history. Check capture, extraction, explicit MCP recall and automatic injection separately.

Committed citation usage and query feedback follow promotion, correction and reviewed merges atomically, without separate assertion versions or changes to raw evidence.
The six MCP tools are `recall`, `remember`, `link`, `supersede`, `history`, `loop`. Explicit saves require nonempty verbatim quotes (zero-based `occurrence` for repeats), never invented evidence; native hooks resolve current-conversation provenance. Mutations require stable `command_id`, `actor`, `why`; reuse a command ID only for an identical retry. `remember`/replacement scopes are `["personal"]`, `["professional"]` or both. `supersede` distinguishes correction, supported refinement and world-time change (the latter requires aware `valid_at`); `history` preserves versions/diffs/reasons/evidence. `recall(query, effort="cheap|normal|deep", scope=...)` is semantic; omit query for recent raw activity, or use `since` (aware ISO time)/`source` for literal raw evidence filtering (raw evidence has no inferred scope). Exact source text/artifacts and time/page offsets are retained. `loop` supports `list|open|close|later|remind`; only explicit `remind` sets a reminder time, with no proactive scheduler.
Native and MCP tools treat empty optional strings, including `recall.effort`, as omitted; required text/reasons/quotes and nonempty invalid dates still fail validation. HTTP commands remain strict.

During historical uploads, quote-only native saves and quoted media saves flush the matching eligible message projections without draining unrelated capture. Evidence with an explicit episode ID needs no local flush. If a generic tool wrapper bypasses native pre-tool hooks, set `CFG` to the existing harness configuration and run `python3 "$(dirname "$CFG")/bridge.py" flush-conversation --config "$CFG" --agent HARNESS --conversation-id ID --quote 'Exact source quote'`, repeating `--quote` as needed, then retry the identical save command. The result identifies acknowledged episodes and unmatched quote indices; an unmatched local quote may already be on the server, whose normal evidence validation still applies. Embedded tool-result blocks that lack a derivable message key require ordinary upload. No-quote `flush-conversation` retains its whole-conversation behavior.

Automatic context uses a shared bounded budget for facts and conditional behavioral suggestions; an empty result is valid. Generalized suggestions use the existing `experience` type and consolidation schedule, retain exact supporting evidence, and distinguish inferred lessons from explicit user preferences. Current instructions take precedence. Lasting relevant corrections can revise or retire a suggestion through the existing history; no per-turn model call or separate learning service is involved. Broad explicit recall remains available.

Known complete legacy Codex `AGENTS.md` instruction envelopes, with an optional environment-context suffix, are retained as raw history but excluded from extraction and behavioral authority. Existing memories supported only by those envelopes remain explicitly searchable and are omitted from automatic context. The server handles this without re-importing histories or updating clients; genuine prose outside the exact recognized form remains eligible.

### Import histories on each origin device

Finish the first-test sequence above before importing history.

After client/server setup there, keep that device's private config and stable generated `client_uid`; rerunning setup preserves it. Select actual local history files/directories explicitly (no automatic discovery). Run all applicable commands, replacing the example paths:
```sh
CFG="${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json"
BRIDGE="$(dirname "$CFG")/bridge.py"
python3 "$BRIDGE" import-history /path/to/pi/jsonl-history --config "$CFG" --agent pi
python3 "$BRIDGE" import-history /path/to/omp/jsonl-history --config "$CFG" --agent omp
python3 "$BRIDGE" import-history /path/to/claude/jsonl-history --config "$CFG" --agent claude
python3 "$BRIDGE" import-history /path/to/codex/jsonl-history --config "$CFG" --agent codex
python3 "$BRIDGE" upload --config "$CFG"
```
Only complete JSONL records are queued; directories recurse over `*.jsonl`. Repeating import with the same origin identity/event content is idempotent; uploads retry durably. Do not copy histories/config identities across devices and claim cross-device deduplication.

Explicit `upload` drains until empty or a transport error, prints JSON `pending`/`conflicts` counts, and exits nonzero if either remains. Native background upload retains its bounded runtime. Large attachments on slow connections may require explicit `memento upload`; failed binary retries do not block later text uploads.

Failed uploads include a sanitized `error` category, HTTP status and local outbox ID when available; never infer timeout from pending counts or healthy `/health` alone. Preserve that result and the queued data, address the cause, then retry the same upload without re-importing. Immutable conflicts need investigation rather than repeated retries.

Explicit text upload requests use the normal 15-second foreground command allowance. Background text requests retain their two-second timeout and bounded hook runtime; changing the foreground allowance does not add automatic retries.

The supported frontdoor is `memento import-history PATH --agent HARNESS`, followed by `memento upload --agent HARNESS` and `memento status --agent HARNESS`; keep `--config PATH` for a custom installation. A matching default config is reused, otherwise the harness config is selected. Referenced attachments require explicit `--artifact-root DIR`; OMP blob references use its native blob directory. Native attachment import supports up to 32 MiB per object and reports missing/unsupported material. Raw records remain lossless; opaque attachment bytes are excluded from projected text processing. Ordinary hooks reconcile only the current transcript; explicit imports/uploads use persisted file-change fingerprints for registered histories.

Import returns selected conversation IDs and queues existing compaction markers after their captured records, making that history eligible for extraction while live conversations continue. After upload reaches zero pending/conflicts, retain their `extraction.progress` boundaries from status. A later `cursor >=` the retained `compact_through` proves prefix extraction; later capture, compaction or a partial offset beyond that prefix does not invalidate it. Indexing and candidate disposition require separate evidence; consolidation retains its night window. No import registry or second processing pipeline is created.

New skipped native records offload exact opaque runs of at least 64 KiB into the existing artifact store. Explicit source/history reads reconstruct and checksum the original outside the writer lock; unavailable/corrupt source bytes fail explicitly. Offloaded raw JSON cannot be assertion evidence: cite its message projection with exact quotes and offsets. Existing inline evidence remains supported. Physical storage does not change event IDs, logical metadata or retry equality. Existing inline rows are not rewritten; `raw_storage.inline_raw_bytes` reports remaining inline raw representation, including small new records. These source artifacts are promptly eligible for Drive; database exports include their bytes only while they lack a verified remote copy.

Status distinguishes local queue state, capture, indexing, extraction, consolidation candidates/jobs, media and Drive availability. Native-source counts are under `service`; explicit artifact-save counts are under `artifacts_service`. Counts use one read-only database snapshot so history scans do not hold the capture lock; query CPU/I/O cost still grows with stored history. Zero consolidation jobs does not mean all candidates were promoted, and counts alone do not prove the processor is online.

The first startup without the status covering index builds it transactionally from existing records; large histories can delay readiness. Keep free data-volume space for the index and its WAL, and account for the temporary sort in Compose's `/tmp` tmpfs under the service memory limit. Clients spool while startup completes. Later starts reuse the index; backups and exports include its size. Older non-container SQLite versions retain exact counts but may not optimize indexed expressions as effectively. Capture metadata remains validated JSON; malformed legacy metadata fails the build rather than being silently omitted.

Watch for `Building episode status index` and `Episode status index build completed in N seconds` in the service logs. Health can fail and Docker can mark the container unhealthy until completion. Do not restart during the build: interruption rolls back the transaction and the next start repeats it.

`artifact_storage` is explicitly global even in source-filtered status: it counts registered, local-only and verified-remote objects/bytes, not cache residency, eligibility or queued work. The existing Drive worker drains full bounded batches immediately and checks shutdown between batches; idle and failed work retain existing waits/backoff.

Use `memento media save FILE --agent HARNESS --scope personal|professional`, `memento media status JOB_ID --agent HARNESS`, and `memento media get SHA256 --agent HARNESS --output NEW_FILE`. Save streams up to the existing 1 GiB blob limit; retain its job ID and stable command ID for identical retries. An exact native `--quote TEXT --conversation-id ID` supplies conversational evidence; otherwise the command records honest artifact-identity evidence, not a fabricated user statement. Retrieval verifies the checksum and refuses overwrite. MCP `remember` accepts uploaded media references and evidence, never inline binary.

### Explicit native updates and outages

Use `memento agent update HARNESS` with the same custom `--config`/`--native-config-dir` used for installation. It validates the entire private-service code bundle before replacing files, preserves config/client UID/role/outbox/native settings/timers and never restarts active sessions or discovers a model. Publication is atomic per file, standalone runner last; after interruption rerun when connected to finish. There is no rollback journal, automatic updater or per-message update request. Load through native reload or a fresh session; use `agent add` if hook/MCP definitions change, retaining human trust prompts.

Ordinary automatic recall also carries the server's client-bundle fingerprint. Updated clients show one operational notice while a different bundle is offered, including in headless Pi/OMP request context. This adds no polling or automatic installation, and the notice is not saved as memory or cached recall. Old clients need one explicit update before they can recognize notices. An unknown development build or failed recall does not claim an update is available. The fingerprint identifies a bundle, not version ordering or proof that an interrupted update completed; retain the normal rerun and reload steps.

Offline capture stays queued until another native lifecycle hook or explicit upload retries it. An idle client has no retry daemon. Bounded automatic recall warns and continues without fresh context (same-query cache may be used for five minutes with a warning); explicit saves fail closed and are not queued as mutations.


### Drive, human administration and recovery

Drive is optional and integral when enabled: install `--extra drive` outside Docker (image includes it). Human prerequisite: enable Google Drive API in a dedicated project, create a **Desktop app** OAuth client, and explicitly consent to only `drive.file`. For an External consent screen set publishing status **In production**: Testing refresh tokens expire after seven days. Do not use ambient Google credentials or broaden scopes. First-time operator setup creates a dedicated app-accessible folder; normal reconnection verifies the exact account and reuses that folder. Manually created folders are not automatically app-accessible.

**Installing agents:** native setup, `/health`, the public website and Google branding are separate from Drive authorization. First run `drive-connect`: an already-connected service or an existing private bootstrap needs no new credential lookup. Only unavailable authorization requires provisioning on the **service host**, not the browser laptop. Do not claim a requested Drive connection is ready until the helper reports verified activation. Reuse the original Desktop client from the human's authorized credential store before asking them to supply files or repeat Google Console setup; an Infisical deployment may use `MEMENTO_GOOGLE_DESKTOP_CLIENT_JSON`. If its CLI is missing, install it and surface only the necessary human login. Obtain the original account/folder from existing binding metadata; ask only for missing metadata. Never invent a new client/folder, use ambient Google authentication, or print secret values. `/connect` provides the same workflow for agents without this repository.

**One-link connection:** retain the original Desktop client, account and existing app-created folder. A human/operator provisions that binding once, using a private original client JSON readable by the service UID and running as that UID:
```sh
uv run --extra drive memento-drive configure --client-json /private/original-desktop-client.json --account YOU@example.com --folder-id EXISTING_FOLDER_ID --data-dir /private/memento-data
```
This creates owner-private `DATA_DIR/drive-client.json`, not a Drive grant. Never create a replacement client or folder during recovery. Existing private connection and bootstrap bindings must agree. The service owner controls initial secret provisioning. An installing agent may use explicitly authorized credential-manager injection into the existing owner provisioning helper, without exposing values or moving token files; otherwise pause for owner approval. Never read private credential files into the conversation, share the service UID, or elevate privileges.

For normal connection, the agent on the user's browser device runs the downloaded runner against the private service:
```sh
python3 "$HOME/.local/share/memento/runner.py" drive-connect --url https://<machine>.<tailnet>.ts.net
```
Use the actual private service URL. Without `--url`, the runner uses the selected `--config PATH` (default `runner.json`); `/connect` clients must reuse their harness-specific config path. The explicit URL also works before runner setup. The agent gives the human the printed official Google link. Open it on that same device, choose the intended account and approve `drive.file`. The stdlib runner handles the Desktop client's loopback callback and relays the one-time code over private HTTPS (HTTP is accepted only for loopback development). No SSH forwarding, user token-file transfer, chat switching, Web client or public callback is needed.

The service alone exchanges the code, verifies the exact account and owned/untrashed existing app-accessible folder, atomically saves mode-0600 `DATA_DIR/drive-oauth.json`, and activates cold storage without a restart. Success is reported only after activation. Pending links expire after ten minutes and cannot be replayed; denied consent, wrong accounts, broader grants and inaccessible folders never install a connection or create a folder. A definitive revoked/expired grant leaves the service available for reconnection while `/health` reports `503 Drive reauthorization required`; malformed private config, wrong folder/account and unrelated startup failures are not suppressed. Native agent admission remains unchanged.

An already-connected service exits successfully without another consent link. Cancelling the pending helper with Ctrl-C makes a best-effort state-bound cancellation so a new attempt need not wait for expiry. The helper reports rejected, expired, pending/conflicting and unavailable authorization without echoing provider response bodies or credentials; cancellation never replaces the original account/client/folder binding.

If the human explicitly requests fresh consent while already connected, run the same command with `--reconnect`. This keeps the working grant active until the service verifies and installs the replacement for the exact original binding. Denial, expiry or failed verification does not disconnect the existing grant. Closing the retired local HTTP client does not revoke the old Google refresh token.

The standalone operator `memento-drive connect` remains available for first-time folder creation and offline recovery. For recovery always provide `--folder-id EXISTING_FOLDER_ID` if the previous connection file was lost; a conflicting explicit ID is rejected before consent, and reconnect retains the original client/account/folder. Without a previous connection or explicit folder ID it is first-time setup and creates a folder, so never omit the known ID during recovery.

When Drive is connected, retained media and other payloads at least 10 MiB are promptly eligible for verified remote storage, regardless of pinning or importance. Other unprotected artifacts remain eligible after 90 days (`MEMENTO_COLD_MAX_IMPORTANCE`, `MEMENTO_COLD_AFTER_DAYS`, `MEMENTO_COLD_MIN_BYTES`). Pending media work and active readers protect local bytes; upload failures retain them. Fetched cold copies use a 1 GiB / one-hour cache, checked every minute (`MEMENTO_COLD_CACHE_BYTES`, `MEMENTO_COLD_CACHE_TTL_SECONDS`); in-use copies can temporarily exceed the bound.

No automatic database backup or remote forget journals. Request a manual verified Drive backup through the owner-only admin socket:
```sh
docker compose exec memory /app/.venv/bin/memento-admin --data-dir /data backup
```
Run review `list|approve|reject` and `forget ID --why REASON` as service owner/root, never the agent; decisions require human terminal confirmation. Forget removes active memory and suppresses re-extraction but retains raw history/artifacts/backups; old backups can restore forgotten content.

Recovery on a separate host, as intended service owner, with a human-provided private connection:
```sh
uv run --extra drive memento-restore --connection /private/drive-oauth.json
uv run --extra drive memento-restore --connection /private/drive-oauth.json --backup-id ID --destination /private/empty-recovery
```
The first lists verified backups; the second restores database plus available hot artifacts to an empty private directory. Incomplete snapshots are refused unless explicitly adding `--allow-incomplete`; inspect the gap manifest. Keep the original Desktop OAuth client securely off-box; after disk loss re-consent that same client to recover app-scoped access. Drive copies are plaintext, retained versions consume quota, and local data/credential ownership must be restored before starting Compose.

### Database export and host migration

Owner-only `memento-admin --data-dir DIR export --output FILE` exports a consistent database, its **existing Drive references, and only locally stored payloads without a verified remote copy** into one private plaintext archive. Never download or repack Drive-resident files for export. Inclusion follows `remote_id IS NULL` in the snapshot, not hot/cold tier or cache presence; missing/corrupt required local bytes abort. It uses the existing owner Unix socket and human terminal confirmation, never an HTTP/MCP export route. Output parent must be private; output is `0600` and never overwritten. Use `/data/exports` or another persistent data-volume directory, not `/tmp` tmpfs. Copy out with `docker compose cp`, verify checksums, then remove only the verified in-volume copy.

The `memento export` and `memento restore` frontdoors delegate to these existing owner/export and restore engines. They preserve the same ownership, confirmation and empty-destination requirements.

`memento-restore --archive FILE --destination EMPTY` restores the database without an active Drive grant. Version-2 archives retain remote IDs and the original non-secret account/folder/public-client binding; restore prints the connection identity. Referenced Drive files become readable after reconnecting that original binding. Included local-only payloads remain hot; referenced payloads become cold, without downloading them. Manifest checksums, exact local-only payload coverage, SQLite integrity and foreign keys are validated in private staging before atomic publication; nonempty/live/symlink targets and unsafe members are rejected. Existing host transfer/failure/backup records are reset, not graph/raw/version/vector/audit history. Credentials, native configs and models are excluded. Version-1 self-contained archives retain their previous restore behavior: all payloads local/hot and old remote IDs cleared.

Preserve the restored owner-private `drive-binding.json`: it records only the expected account, folder and public client ID. Do not substitute another binding or infer the instance directory from an external credential-file path. Configure/one-link and startup use their existing explicit data directory; standalone `memento-drive connect` must receive `--data-dir RESTORED_DIR` for a restored instance. Export must refuse remote references without original binding metadata; it must not recover them by downloading Drive files.

Ctrl+C exits the admin client but does not cancel its server export. Normal completion/failure cleans staging; a service kill can leave `.memento-export-*` in the output directory. Remove only the exact task-owned staging after its producer is stopped, never the live database or Drive objects.

For fresh Compose recovery, **import before first startup**, as service UID, into `/data/instance` on the empty data volume using `docker compose run --rm --no-deps --entrypoint /app/.venv/bin/memento-restore -v /private/memento.tar:/import/memento.tar:ro memory --archive /import/memento.tar --destination /data/instance`. The archive must be privately readable by UID 10001. Then use a persistent Compose override mounting `./data/instance:/data` before `up`; the README contains the exact override. Do not restore over a started instance or atomically rename the mounted `/data` root.

Migration is a point-in-time move, not sync: before the final export disable the old host's Memento Serve route, stop its processor timer and wait for any running tick, and close its native sessions to stop loopback uploads. Remote clients spool durably. Keep the old service running for owner export, then stop it before enabling the new instance/repointing clients; never accept writes on both. Retain client identities so queued captures upload to the new host, configure one processor, and separately reconnect optional Drive with the original client/account/folder. Another-server deployment still requires explicit authorization.
