# Memento

### Your assistant should remember more than the last chat.

Memento gives your native AI agents a shared, private memory—with receipts. Stop re-explaining your preferences, projects and decisions every time you start a new conversation. Memento captures the conversation, extracts evidence-backed memories, and automatically brings relevant context into later prompts.

**Remember the useful things. Keep the original evidence. Change your mind without losing the history.**

- **Works where you already work.** Native integrations for Pi, OMP, Claude Code and Codex; no replacement chat app.
- **Recall without a ritual.** Relevant memories are injected automatically. MCP tools handle explicit saves, corrections, history and open loops.
- **Evidence, not unexplained assertions.** Memories retain source quotes and offsets. Candidates are visibly unconfirmed before promotion; corrections keep version history.
- **One memory across your devices.** A private central service, with capture clients on your other machines. Each origin keeps its own stable identity.
- **Your storage, your boundaries.** Local Linux storage and local embeddings. Optional Google Drive cold storage and manual backups use a dedicated app-accessible folder, not access to your whole Drive.

Extraction and consolidation use your **existing native CLI login and default model**, not a new paid model API key. Those calls still consume model usage and follow your provider's terms; “local storage” does not mean no conversation content ever reaches a model provider.

[Product website](https://memento.bezosapp.uk/) · [Privacy](https://memento.bezosapp.uk/privacy/) · [Terms](https://memento.bezosapp.uk/terms/)

## How it fits together

![Memento architecture: native capture and automatic recall, separate service identity, evidence-backed candidates, nightly consolidation and optional Drive storage](docs/images/architecture.svg)

Raw history and inferred memory are separate. Extraction runs after native compaction or 30 minutes of conversation inactivity. Supported candidates are available to recall before promotion. Consolidation runs during the configured **02:00–07:00 local-time window**; it is not an exact 02:00 cron job.

This is a private memory backend, not a public dashboard, a Google sign-in website, a whole-Drive indexer or a guarantee that every model inference is true. The public website is a separate static product/policy site.

## Installation map

1. Run **one persistent service** on local Linux storage.
2. Set up **one server processor** using Pi, OMP or Claude Code with an existing native login.
3. Set up other harnesses/devices as **capture clients**.
4. Start new native sessions and verify capture → candidate → fresh-session recall.
5. If wanted, connect Google Drive through explicit human consent and verify activation.

> **Already installed?** Reuse the existing data directory or volume, Compose overrides, private configs and native logins. Re-running setup preserves the generated client identity. Do not initialize a fresh database over an existing deployment, copy identities between devices, or use `docker compose down -v` as an upgrade command.
>
> **Installing agent?** Execute the permitted setup and verification yourself. Surface only genuine human steps: native login/trust, service-owner permissions and Google consent. Never request secret values in chat or treat a healthy API as proof of native capture or Drive authorization.

## 1. Run the service

### Requirements

- Linux with Docker Engine and Compose, or Python **3.12+** and [uv](https://docs.astral.sh/uv/).
- Dedicated persistent **local Linux storage**. Do not use NFS, SMB or FUSE for the live database.
- Space and network access for the pinned full `google/embeddinggemma-2` model; any required model access/license acceptance is a human prerequisite.
- A separate ordinary-user native agent account. One service process per data directory.

### Recommended: Docker Compose

Clone the repository, then enter it:

```sh
git clone https://github.com/misunders2d/memento.git
cd memento
```

For a **fresh installation only**, the human/operator prepares the default bind mount with the service's ownership. These are operator commands, not permission for an agent to acquire sudo or Docker-daemon access:

```sh
sudo install -d -o 10001 -g 10001 -m 0700 ./data
docker compose up --build -d
curl --fail http://127.0.0.1:8765/health
```

Wait for healthy real-model startup; the first download can take time. Inspect `docker compose logs memory` if startup fails. Model cache persists under `/data/models`. **Do not disable embeddings to declare installation successful.**

Default deployment properties:

| Setting | Default |
| --- | --- |
| Published listener | `127.0.0.1:8765` only |
| Persistent data | `./data` → `/data` |
| Service UID/GID | `10001:10001` |
| Agent UID used for separation checks | `1000`; set the actual agent UID if different |
| Limits | 2 CPUs, 5 GiB RAM, 256 PIDs |
| Hardening | Read-only root, `/tmp` tmpfs, all capabilities dropped, no new privileges |
| Restart policy | `unless-stopped` |

Overrides: `MEMENTO_UID`, `MEMENTO_GID`, `MEMENTO_AGENT_UID`, `MEMENTO_DATA_DIR`, `MEMENTO_PORT`; optional `.env` or `MEMENTO_ENV_FILE`. Preserve existing values and mount configuration during upgrades. Enable Docker startup on the host according to its normal administration policy.

**Human-review isolation matters:** the agent must not share the service UID, have sudo, or control the Docker daemon. Docker-daemon access can defeat this boundary. Container hardening is not a substitute for separating those privileges.

### Non-container development

```sh
uv sync --extra media --extra embedding --extra drive
MEMENTO_EMBEDDINGS=1 uv run memento --data-dir ./data
```

Run under the intended service identity with private storage, not inside the ordinary agent's identity when testing human-review isolation. This foreground development command does not install a boot service or reproduce Compose's resource limits.

API contracts: `http://127.0.0.1:8765/docs`. Agent-readable installation guide: `http://127.0.0.1:8765/connect`.

## 2. Install the native server processor

Use the **ordinary host agent user**, never root or the service UID. Install your chosen native CLI and complete its normal login first. Memento uses that CLI's own default model/login.

Download the stdlib-only runner from your trusted private service:

```sh
URL=http://127.0.0.1:8765
install -d -m 0700 "$HOME/.local/share/memento"
curl --fail "$URL/runner" -o "$HOME/.local/share/memento/runner.py"
chmod 600 "$HOME/.local/share/memento/runner.py"
python3 "$HOME/.local/share/memento/runner.py" setup \
  --agent pi --timezone Europe/Berlin --url "$URL"
```

Replace the timezone with the actual **IANA timezone**. Choose `--agent pi`, `omp` or `claude` for the single server processor. Setup is prompt-free; it installs the native capture/recall hooks or extensions, MCP definitions, private `runner.json` and sibling `bridge.py`, plus the processing service/timer. It does not log into a model provider or authorize Google Drive.

Default config: `${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json`. Use `--config PATH` consistently when choosing another location. `--no-activate` installs without starting the timer.

The user timer starts roughly two minutes after boot and then **15 minutes after each completed run**. For unattended processing after logout, the human/operator enables lingering:

```sh
sudo loginctl enable-linger YOUR_AGENT_USER
```

Keep one server processing config/timer. Do not silently convert it into a client when installing another harness. A sleeping or powered-off host cannot process memory; keep it awake for an overnight acceptance check.

### Codex limitation

Codex capture/import/recall/MCP client integration is available after **human `/hooks` trust and MCP permissions**. Setup never grants those permissions. Codex **server extraction is fail-closed**: the tested native CLI still exposed tool routing under its read-only/approval flags, so it did not meet tools-zero processing requirements. Use Pi, OMP or Claude Code as the processor; do not bypass a native refusal or claim Codex extraction acceptance.

## 3. Add other harnesses or devices as clients

A client captures conversations and nudges the server after compaction. It does **not** discover a processing model, generate memories or install another processing timer.

For OMP on the same host, keep a separate private config:

```sh
python3 "$HOME/.local/share/memento/runner.py" \
  --config "$HOME/.config/memento/omp/runner.json" setup \
  --client --agent omp --timezone Europe/Berlin --url "$URL"
```

Use the matching harness (`pi`, `omp`, `claude` or `codex`) on each device. Keep each device's own config and stable generated `client_uid`; do not copy configs or histories to pretend they came from another origin.

**Start new native sessions after installation.** Do not silently restart an active user-owned agent. The existing native profile, model selection and login remain the source of truth.

### Private access from another machine

Never expose this unauthenticated HTTP/MCP service publicly. On the server, set the actual tailnet hostname in `.env`, recreate the memory container to apply that environment, and have the authorized operator publish it through Tailscale Serve:

```sh
# .env: MEMENTO_ALLOWED_HOSTS=YOUR_MACHINE.YOUR_TAILNET.ts.net
docker compose up -d
tailscale serve --bg 8765
```

Tailnet HTTPS must be enabled and ACLs must restrict access to your devices. The API's published listener remains loopback-only. MCP host protection stays enabled; the endpoint is `/mcp/`.

On a client device, download `/runner` from `https://YOUR_MACHINE.YOUR_TAILNET.ts.net`, then run the client setup above with that HTTPS origin and its real timezone. Do not disable TLS verification or use a public reverse proxy as a shortcut.

## 4. Prove memory actually works

Use synthetic text, not secrets. Installed files, a running timer or a convincing answer alone are not proof.

1. In a real native conversation say a distinctive fact, for example: **“My personal NorthWindow project uses accent code BLUE7294.”**
2. Verify raw capture. Find the episode, then fetch it and check the **exact original text, source, conversation and timestamp**:

   ```sh
   curl --get --data-urlencode 'q=NorthWindow' "$URL/episodes/search"
   curl "$URL/episodes/EPISODE_ID"
   ```

3. Use native compaction, or leave the conversation inactive for **30 minutes**. Compaction records a captured sequence boundary. Server-side compaction can drain immediately; a client nudges the server and the processing timer drains due work. Inspect the actual processing journal, not just the timer state:

   ```sh
   systemctl --user status memento-runner.timer
   journalctl --user -u memento-runner.service
   ```

4. Inspect semantic recall and history. Before promotion, the memory should be visible as an **unconfirmed candidate**, with verbatim quotes and exact source spans. Check the reported/requested model and any known usage in history:

   ```sh
   curl --get --data-urlencode 'q=NorthWindow' "$URL/recall"
   curl "$URL/history/MEMORY_ID"
   ```

5. In a **fresh native session**, ask which accent code you chose for NorthWindow, without including the code or memory ID. Require the correct answer **with its memory citation**, automatically supplied without a memory-tool call. Verify that captured raw user text is still only the original question—not the injected context. Also exercise MCP `recall` and `history` explicitly to inspect the evidence.
6. Verify Pi and OMP independently if installing both. Keep one designated server processor. Do not infer one harness works because another does.

For a one-time first-test promotion outside the night window:

```sh
python3 "$HOME/.local/share/memento/runner.py" \
  --config "${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json" tick --consolidate-now
```

That flag bypasses **only the consolidation night window**, not extraction eligibility, and does not change the schedule. It is **not proof of normal 2 a.m. execution**.

### Verify the normal overnight schedule

Leave an evidence-backed candidate pending before 02:00 in the server config's timezone. Keep the host awake and the timer enabled. Observe the normal service journal and memory history after the first completed in-window tick, without running a forced tick or modifying the clock.

The window is **02:00–07:00**, not exactly 02:00. The first in-window claim freezes that night's eligible-candidate boundary; later candidates wait for another night. Processing duration shifts the next tick. Give pending extraction/consolidation time to finish before calling a miss a failure. There is no daytime catch-up for a missed night window.

## 5. Optional Google Drive connection

Drive stores eligible cold artifacts and supports **manual owner-requested backups**. It is not required for local capture/recall and does not grant access to unrelated Drive files.

### Normal agent-assisted connection or reconnection

**Try the installed connection first**, on the human's browser device:

```sh
python3 "$HOME/.local/share/memento/runner.py" drive-connect --url "$URL"
```

With no explicit URL, use the correct harness's `--config PATH`; the default is `runner.json`. An already-connected service exits successfully without another link or credential lookup. If a Google link is printed, keep the helper running while the human opens that official link **on the same device**, selects the intended account and explicitly approves `drive.file`.

The runner receives the Desktop client's loopback callback. The service alone exchanges the code, verifies the original account and owned, untrashed, app-accessible folder, saves a private connection and activates it. **Only verified activation and a successful helper exit mean connected.** No Web OAuth client, public callback, SSH forwarding or token-file transfer is needed for this normal flow. HTTP is accepted only for loopback development; remote use requires private HTTPS.

If authorization is unavailable, that error is **not a consent link**. Provision the original binding as below, then retry. Cancelling a pending helper makes a best-effort cancellation; links expire after ten minutes and cannot be replayed. Wrong accounts, inaccessible folders or broader grants do not install a connection.

### Service-owner prerequisites and original binding

These are separate from native setup, a healthy API, the public website and Google branding:

- A dedicated Google project with Drive API enabled.
- The **original Desktop app OAuth client**, not a new Web client.
- The exact intended account and original app-created folder for reconnection.
- Human consent to **only `drive.file`**. For an External app, **In production** avoids Testing's seven-day refresh-token limitation. Re-consent after publishing if the old grant was issued in Testing.

On the **service host**, the service owner provisions the original binding, under the service identity:

```sh
uv run --extra drive memento-drive configure \
  --client-json /private/original-desktop-client.json \
  --account YOU@example.com --folder-id EXISTING_FOLDER_ID \
  --data-dir /private/memento-data
```

For Compose, run that same provisioning helper inside the existing container, where its default identity owns `/data`:

```sh
docker compose exec -T memory /app/.venv/bin/memento-drive configure \
  --client-json /tmp/original-desktop-client.json \
  --account YOU@example.com --folder-id EXISTING_FOLDER_ID --data-dir /data
```

The private temporary client file must already be provisioned inside the container, mode `0600` and readable only by its service identity, through the owner's approved secret-manager injection. The owner removes that temporary source afterwards. This does not authorize an agent to read credentials, copy token files, use a different UID or grant itself Docker access. The helper used by this workflow is the same `configure_client` provisioning path as the CLI.

Use the actual mounted data directory. This produces owner-private `drive-client.json`, **not** a Drive grant. Existing bootstrap and connection bindings must agree.

Installing agents should reuse the human's authorized credential store before requesting files or repeating Google Console setup. An Infisical deployment may use `MEMENTO_GOOGLE_DESKTOP_CLIENT_JSON`; install its CLI if necessary and surface only the required human login. Reuse existing account/folder metadata and ask only for genuinely missing metadata. Explicitly authorized secret-manager injection may feed the existing owner provisioning helper privately; otherwise pause for owner approval. **Never print secret values, read token files into chat, share the service identity or elevate privileges.**

### Truly first-time setup and offline recovery

The normal one-link workflow deliberately requires an existing folder binding. For a genuinely new installation with no app-created folder, the human/operator uses the standalone connector:

```sh
uv run --extra drive memento-drive connect \
  --client-json /private/desktop-client.json --account YOU@example.com \
  --output /private/drive-oauth.json
```

It creates the dedicated folder only for first-time setup. The human/operator installs the resulting connection as `DATA_DIR/drive-oauth.json`, mode `0600`, owned by the service UID/GID, then restarts the service. An agent must not copy tokens to accomplish this. For a headless operator flow, use `--no-browser --port 8766` and a human-maintained `ssh -N -L 8766:127.0.0.1:8766 SERVER` tunnel.

**Recovery must reuse the original client, account and folder.** If the private connection file was lost, add `--folder-id EXISTING_FOLDER_ID` to the connector command. Without a previous connection or explicit folder ID, it is first-time setup and creates a new folder—never omit a known ID during recovery. Manually created folders are not automatically accessible under `drive.file`.

A definitive revoked/expired grant leaves the service available for reconnection while `/health` reports `503 Drive reauthorization required`. Wrong bindings, malformed private configuration and unrelated startup failures are not silently ignored. Do not revoke a working grant merely to demonstrate that a command runs; persisted-connection reload is not fresh human re-consent.

## Import existing conversation history

Finish the native first-test sequence **before** importing history. Import on each origin device using that device's private config and stable identity. Select actual local paths explicitly; there is no automatic history discovery.

```sh
CFG="${XDG_CONFIG_HOME:-$HOME/.config}/memento/runner.json"
BRIDGE="$(dirname "$CFG")/bridge.py"
python3 "$BRIDGE" import-history /path/to/history --config "$CFG" --agent pi
python3 "$BRIDGE" upload --config "$CFG"
```

Use `--agent omp`, `claude` or `codex` and the matching harness config for other histories. Directories recurse over `*.jsonl`; only complete JSONL records are queued. Repeating an import with the same origin identity/event content is idempotent. Explicit upload drains until empty or transport failure, reports JSON `pending`/`conflicts` counts and exits nonzero if either remains. Native background upload has a bounded runtime and retries durably.

## MCP and memory semantics

The six MCP tools are:

| Tool | Purpose |
| --- | --- |
| `recall` | Semantic memory search, or recent/literal raw evidence |
| `remember` | Explicit evidence-backed save |
| `link` | Connect memories |
| `supersede` | Correct, refine or record a world-time change |
| `history` | Inspect versions, differences, reasons and evidence |
| `loop` | List/open/close/defer open loops or set an explicit reminder time |

`recall(query, effort="normal", scope=...)` performs semantic retrieval; effort can be `cheap`, `normal` or `deep`. Omit the query for recent raw activity; use aware ISO `since` or `source` for literal raw evidence filtering. Raw history has no inferred personal/professional scope.

Explicit saves require **nonempty verbatim quotes**, with zero-based `occurrence` for repeats; native hooks resolve current-conversation provenance. Mutations require a stable `command_id`, `actor` and `why`. Reuse a command ID only for an identical retry. Save/replacement scopes are `["personal"]`, `["professional"]` or both. World-time changes require an aware `valid_at`. Native/MCP empty optional strings are treated as omitted; required text and invalid nonempty dates still fail validation. HTTP commands remain strict.

Only explicit `loop` action `remind` sets a reminder time. There is **no proactive reminder scheduler**. Promotion, correction and reviewed merges preserve committed citation usage and query feedback without rewriting original evidence.

## Administration, backup and recovery

Human decisions use the **owner-only admin socket**, not an agent-accessible admin HTTP API. Run as service owner/root; review decisions and forget require human terminal confirmation:

```sh
docker compose exec memory /app/.venv/bin/memento-admin --data-dir /data list
docker compose exec memory /app/.venv/bin/memento-admin --data-dir /data backup
```

`approve PROPOSAL_ID --why REASON`, `reject PROPOSAL_ID --why REASON` and `forget ID --why REASON` are operator actions. Forget removes active memory and suppresses re-extraction; **it does not erase raw history, artifacts or existing backups**. Restoring an old backup can restore forgotten content.

There is **no automatic database backup**. The backup command requests and verifies a manual Drive snapshot. Cold archiving defaults to unpinned artifacts, importance ≤0.75, age ≥90 days and size ≥10 MiB; overrides are `MEMENTO_COLD_MAX_IMPORTANCE`, `MEMENTO_COLD_AFTER_DAYS`, `MEMENTO_COLD_MIN_BYTES`. A newly connected installation may have no eligible cold data.

On a separate recovery host, the intended service owner uses a human-provided private connection:

```sh
uv run --extra drive memento-restore --connection /private/drive-oauth.json
uv run --extra drive memento-restore --connection /private/drive-oauth.json \
  --backup-id BACKUP_ID --destination /private/empty-recovery
```

The first command lists verified backups. The second restores the database and available hot artifacts to an **empty private directory**. Incomplete snapshots are refused unless the operator deliberately supplies `--allow-incomplete`; inspect the gap manifest. Keep the original Desktop client securely off-box so the human can re-consent after disk loss. Restore correct data/credential ownership before starting Compose.

Drive copies are **plaintext**, and retained versions consume quota. Protect local disks, private configs, model-provider accounts and backups according to your threat model.

## Troubleshooting

| Observation | Check |
| --- | --- |
| API is healthy, but no memory appears | Verify actual native capture first; then compaction/30-minute inactivity, the single processor's journal, and candidate history. |
| A candidate is not durable yet | Check the configured timezone, real night window, candidate boundary and review/deferral decision. Do not force a tick to claim normal schedule acceptance. |
| Timer ran, but extraction did not finish | Inspect the native CLI's existing login/default model and safety-flag refusal. Failed work remains pending; do not bypass tools-zero requirements. |
| Processing exits 1, but the journal shows only backlog counts | Before recreating the container, inspect `docker compose logs --since 30m memory` for a backend traceback. Preserve pending work and original evidence; do not force promotion or bypass validation. |
| Native setup worked, but Drive did not connect | Run `drive-connect`; only verified activation proves a grant. Missing bootstrap requires service-owner provisioning, not a replacement OAuth client. |
| Remote MCP/HTTP is rejected | Check the exact allowed tailnet hostname, HTTPS and ACLs. Do not disable host protection or expose the API publicly. |
| Reinstalling another harness | Keep the server config; select a distinct private client config and retain its existing identity. |

## Development and project records

```sh
uv run --extra media --extra embedding --extra drive pytest -q
```

Key implementation areas: `api.py`/`mcp_server.py` (interfaces), `runner.py`/`bridge.py`/`native_*.ts` (native integration), `processing.py`/`extraction.py`/`consolidation.py` (processing), `store.py`/`graph.py`/`retrieval.py` (evidence and retrieval), and `drive_*.py`/`cold_*.py` (optional storage).

Linear is the source of truth for design and status: [Personal Clone](https://linear.app/mellanni/project/personal-clone-d5c1389b6508) · [Memory 2](https://linear.app/mellanni/document/memory-2-4d1f54cbf034) · [origin MEL-657](https://linear.app/mellanni/issue/MEL-657). Repository operator details are in [AGENTS.md](AGENTS.md); third-party notices are in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
