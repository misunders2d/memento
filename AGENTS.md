# memento

Agent memory system for Sergey's always-on personal assistant (Personal Clone).

**Linear is the source of truth.** This repo holds code only; do not duplicate design or status here.

- Project: [Personal Clone](https://linear.app/mellanni/project/personal-clone-d5c1389b6508)
- Design record: [Memory 2](https://linear.app/mellanni/document/memory-2-4d1f54cbf034)
- Origin issue: [MEL-657](https://linear.app/mellanni/issue/MEL-657)

## Local development

### Service (local Linux storage only)

Python >=3.12; one service process per data directory. For a non-container development service: `uv sync --extra media --extra embedding --extra drive`, then `MEMENTO_EMBEDDINGS=1 uv run memento --data-dir ./data`. Tests: `uv run pytest -q`. API contracts: `http://127.0.0.1:8765/docs`.

Fresh Compose installation (human/operator steps, not agent commands):
```sh
sudo install -d -o 10001 -g 10001 -m 0700 ./data
docker compose up --build -d
curl --fail http://127.0.0.1:8765/health
```
Wait for healthy real-model startup; first download needs network, disk space and any Hugging Face model access/license acceptance required by the pinned `google/embeddinggemma-2` revision in `retrieval.py`. Cache persists under `/data/models`. Do not substitute disabled embeddings for acceptance. Compose uses dedicated UID/GID 10001, read-only root, 2 CPUs, 5 GiB RAM, 256 PIDs, dropped capabilities and `/tmp` tmpfs. Keep `data/` on local Linux storage (not NFS/SMB/FUSE); the agent must not share the service UID. Overrides: `MEMENTO_UID`, `MEMENTO_GID`, `MEMENTO_AGENT_UID`, `MEMENTO_DATA_DIR`, `MEMENTO_PORT`; optional `.env` or `MEMENTO_ENV_FILE`.

Publication stays loopback-only; never expose this unauthenticated API publicly. On the server, set `MEMENTO_ALLOWED_HOSTS=<machine>.<tailnet>.ts.net` in `.env`, restart the memory service, then run `tailscale serve --bg 8765` (tailnet HTTPS enabled). Serve proxies to the unchanged loopback listener; tailnet ACLs must restrict access to your devices. MCP host protection stays enabled; `/mcp/` is the endpoint. On each laptop, download the runner from `https://<machine>.<tailnet>.ts.net/runner` as below, then run `python3 "$HOME/.local/share/memento/runner.py" setup --client --agent pi --timezone Europe/Berlin --url https://<machine>.<tailnet>.ts.net` (choose its actual harness/timezone). The agent must have neither sudo nor Docker-daemon access when human-review isolation matters.

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

Run as the ordinary host agent user, **not** service UID/root. Install/login to the chosen native CLI normally first; processing uses its own default model/login, never a separate paid model API key. Download the stdlib-only runner from the trusted local service:
```sh
URL=http://127.0.0.1:8765
install -d -m 0700 "$HOME/.local/share/memento"
curl --fail "$URL/runner" -o "$HOME/.local/share/memento/runner.py"
python3 "$HOME/.local/share/memento/runner.py" setup --agent pi --timezone Europe/Berlin --url "$URL"
```
Choose `--agent pi`, `omp`, `claude` or `codex`; an explicit IANA timezone is required (replace `Europe/Berlin`). Setup is prompt-free, installs the selected native hooks/extensions and MCP definitions, and writes private `${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json` plus sibling `bridge.py`. Server setup installs/activates `~/.config/systemd/user/memento-runner.{service,timer}`: boot +2 minutes, then every 15 minutes after completion. For unattended headless processing, the human/operator must run `sudo loginctl enable-linger <agent-user>` so the user manager and timer survive logout. Use one server processing config/timer; other devices run the same setup with `--client`: capture/compaction only, no model discovery, generation or processing timer. Optional `--config PATH` selects a separate private config; use it consistently below. `--no-activate` installs without starting the timer.

Restart native sessions after installation. **Codex needs human `/hooks` trust and MCP permissions; setup never grants them. Codex server processing stays fail-closed: tested 0.163.0 retains `functions.exec`/`wait` declarations and undeclared `apply_patch` routing, so read-only/approval policy is not tools-zero. Codex capture/import/recall/MCP client interfaces remain available after native consent; do not claim Codex extraction acceptance.** Choose Pi/OMP/Claude for the single server processor; their native safety-flag checks enforce tool-free processing and leave failed work pending.

First-test sequence (use synthetic text, not secrets):
1. In a real native conversation state a distinctive durable fact. Confirm raw capture via `GET /episodes/search?q=...`, then fetch `GET /episodes/{id}` for exact text/source/conversation/time.
2. Compact using the harness's native compaction action, or leave the conversation inactive for 30 minutes. Compaction marks a captured sequence boundary; inactivity processes through the latest sequence. Client compaction only nudges the server; the server timer/tick drains due work. Server-side compaction also drains immediately.
3. On the processing host run:
   ```sh
   python3 "$HOME/.local/share/memento/runner.py" --config "${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json" tick --consolidate-now
   ```
   This single first-test flag bypasses only the consolidation night window, not extraction eligibility, and does not alter the schedule. Normal consolidation runs within 02:00–07:00 in the configured timezone. Inspect `journalctl --user -u memento-runner.service` and candidate `history(id)` for exact quotes, source spans, requested/reported model and known usage; do not infer success from a timer firing.
   Tick prints exact backlog counts for pending extraction conversations, pending jobs and leased jobs—not estimated message counts. Each extraction call packs at most 64 windows and 48,000 projected source characters; long user text continues with exact overlap, while tool output is projected to 2,000 characters without changing raw history.
4. In both the originating conversation and a second fresh native session call MCP `recall` for the fact; candidates must be visible as unconfirmed before promotion. Follow with `history(id)` to verify original evidence. Require actual capture/extraction/recall evidence separately for each harness, not just installed files.

Committed citation usage and query feedback follow promotion, correction and reviewed merges atomically, without separate assertion versions or changes to raw evidence.
The six MCP tools are `recall`, `remember`, `link`, `supersede`, `history`, `loop`. Explicit saves require nonempty verbatim quotes (zero-based `occurrence` for repeats), never invented evidence; native hooks resolve current-conversation provenance. Mutations require stable `command_id`, `actor`, `why`; reuse a command ID only for an identical retry. `remember`/replacement scopes are `["personal"]`, `["professional"]` or both. `supersede` distinguishes correction, supported refinement and world-time change (the latter requires aware `valid_at`); `history` preserves versions/diffs/reasons/evidence. `recall(query, effort="cheap|normal|deep", scope=...)` is semantic; omit query for recent raw activity, or use `since` (aware ISO time)/`source` for literal raw evidence filtering (raw evidence has no inferred scope). Exact source text/artifacts and time/page offsets are retained. `loop` supports `list|open|close|later|remind`; only explicit `remind` sets a reminder time, with no proactive scheduler.
Native and MCP tools treat empty optional strings, including `recall.effort`, as omitted; required text/reasons/quotes and nonempty invalid dates still fail validation. HTTP commands remain strict.

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

Explicit `upload` drains until empty or a transport error, prints JSON `pending`/`conflicts` counts, and exits nonzero if either remains. Native background upload retains its bounded runtime.

### Drive, human administration and recovery

Drive is optional and integral when enabled: install `--extra drive` outside Docker (image includes it). Human prerequisite: enable Google Drive API in a dedicated project, create a **Desktop app** OAuth client, and explicitly consent to only `drive.file`. For an External consent screen set publishing status **In production**: Testing refresh tokens expire after seven days. Do not use ambient Google credentials or broaden scopes. The connection verifies the exact account and creates its own dedicated folder; manually created folders are not automatically app-accessible.

**Reconnect/recovery:** retain the original Desktop client, account and app-created folder. An existing private connection at the exact `--output` path preserves its folder binding; an explicit different `--folder-id` is rejected before consent. If the connection file was lost, append `--folder-id EXISTING_FOLDER_ID` to the command below. After fresh human `drive.file` consent, the connector verifies the account and that exact app-accessible, owned, untrashed folder before saving; it never creates a replacement in this mode. Without a previous connection or `--folder-id`, this is a first-time setup and creates a new folder. Do not omit the known folder ID during recovery.

Headless server recipe: on the laptop keep `ssh -N -L 8766:127.0.0.1:8766 SERVER` running. In another terminal on SERVER:
```sh
uv run --extra drive memento-drive connect --client-json /private/desktop-client.json --account YOU@example.com --output /private/drive-oauth.json --no-browser --port 8766
```
Open the printed official consent URL in the laptop browser and approve interactively; the browser callback reaches SERVER's loopback through that tunnel. Success saves a mode-0600 connection containing the verified account/folder, not three environment settings. As a human operator install it at `./data/drive-oauth.json`, mode 0600, owner 10001:10001, then restart the service. Do not read/copy credentials as an agent. The service reads only `DATA_DIR/drive-oauth.json`; absent means disabled. Default cold archive eligibility: unpinned, importance <=0.75, age >=90 days, size >=10 MiB (`MEMENTO_COLD_MAX_IMPORTANCE`, `MEMENTO_COLD_AFTER_DAYS`, `MEMENTO_COLD_MIN_BYTES`).

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
