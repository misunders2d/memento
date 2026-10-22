import json
import base64
import hashlib
import asyncio
import os
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator, model_validator

from .store import ConflictError, Store
from .commands import Link, Loop, MediaInput, Remember, Supersede
from .mcp_server import create_mcp, recall_memories, submit_media
from .graph import Graph
from .blobs import BlobStore
from .artifacts import Artifacts
from .jobs import LostLease
from .raw_text import RawTextUnavailable
from .viewer import install as install_viewer


MAX_CAPTURE_BYTES = 1024 * 1024
MAX_BLOB_BYTES = 1024 * 1024 * 1024


def client_release_assets():
    """Fingerprint source assets before stamping the served bridge identity."""
    files = {name: Path(__file__).with_name(name).read_bytes() for name in
             ('runner.py', 'bridge.py', 'native_pi.ts', 'native_omp.ts', 'native_shared.ts')}
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in files.items()}
    revision = hashlib.sha256(json.dumps(hashes, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    marker = b'CLIENT_REVISION = None'
    if files['bridge.py'].count(marker) != 1:
        raise RuntimeError('Client bridge revision marker is missing or ambiguous')
    files['bridge.py'] = files['bridge.py'].replace(marker, ('CLIENT_REVISION = ' + repr(revision)).encode())
    release = {'format': 1, 'revision': revision, 'files': {
        name: {'content': base64.b64encode(content).decode('ascii'),
               'sha256': hashlib.sha256(content).hexdigest()} for name, content in files.items()}}
    return revision, files['bridge.py'], json.dumps(release)


class BlobResponse(StreamingResponse):
    """Release the already-open reader even if the body never starts."""

    def __init__(self, content, reader, **kwargs):
        self.reader = reader
        super().__init__(content, **kwargs)

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            self.reader.__exit__(None, None, None)


class BodyLimit:
    """Bound raw bytes before JSON parsing, including chunked requests."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope['type'] != 'http':
            return await self.app(scope, receive, send)
        maximum = MAX_BLOB_BYTES if scope['path'] == '/blobs' else MAX_CAPTURE_BYTES
        for name, value in scope['headers']:
            if name == b'content-length':
                if not value.isdigit():
                    return await Response(status_code=400)(scope, receive, send)
                if int(value) > maximum:
                    return await Response(status_code=413)(scope, receive, send)
        received = 0

        async def bounded_receive():
            nonlocal received
            message = await receive()
            received += len(message.get('body', b''))
            if received > maximum:
                raise HTTPException(413, 'Request body exceeds the endpoint size limit')
            return message

        await self.app(scope, bounded_receive, send)


class EpisodeInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)

    source: str = Field(min_length=1, max_length=256)
    event_id: str = Field(min_length=1, max_length=512)
    conversation_id: str = Field(min_length=1, max_length=512)
    kind: str = Field(min_length=1, max_length=128)
    text: str
    occurred_at: Annotated[AwareDatetime, Field(strict=False)] | None = None
    metadata: dict[str, JsonValue] = Field(default_factory=dict)

    @field_validator('source', 'event_id', 'conversation_id', 'kind', 'text')
    @classmethod
    def valid_unicode(cls, value: str) -> str:
        # SQLite UTF-8 cannot represent lone UTF-16 surrogates. Reject, never replace.
        try:
            value.encode('utf-8')
        except UnicodeEncodeError:
            raise ValueError('Text must be valid Unicode') from None
        return value

    @field_validator('metadata')
    @classmethod
    def valid_metadata(cls, value: dict) -> dict:
        try:
            json.dumps(value, ensure_ascii=False, allow_nan=False).encode('utf-8')
        except (UnicodeEncodeError, ValueError):
            raise ValueError('Metadata must be finite, valid Unicode JSON') from None
        return value

    def evidence(self) -> dict:
        result = self.model_dump(mode='json')
        if self.occurred_at is not None:
            result['occurred_at'] = self.occurred_at.isoformat()
        return result


class WorkInput(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)

    def arguments(self):
        return self.model_dump(mode='json')


class CompactionInput(WorkInput):
    source: str = Field(min_length=1, max_length=256)
    conversation_id: str = Field(min_length=1, max_length=512)


class ClaimInput(WorkInput):
    agent: str = Field(min_length=1, max_length=64)
    timezone: str = Field(min_length=1, max_length=128)
    compaction: CompactionInput | None = None
    exclude: list[str] = Field(default_factory=list)
    consolidate_now: bool = False

    def arguments(self):
        values = super().arguments()
        values['timezone_name'] = values.pop('timezone')
        return values


class LeaseInput(WorkInput):
    id: str = Field(min_length=1, max_length=128)
    owner: str = Field(min_length=1, max_length=128)


class CompleteInput(LeaseInput):
    agent: str = Field(min_length=1, max_length=64)
    output: dict[str, JsonValue]
    model: str | None = None
    usage: dict[str, JsonValue] | None = None


class ReleaseInput(LeaseInput):
    error: Literal['cli_failed', 'invalid_output', 'service_rejected']


class CitationInput(WorkInput):
    event_id: str = Field(min_length=1, max_length=512)
    ids: list[str]
    query: str | None = Field(default=None, max_length=4096)
    shown: list[str] = Field(default_factory=list)


class DriveBeginInput(WorkInput):
    callback_port: int = Field(ge=1, le=65535)
    reconnect: bool = False


class DriveFinishInput(WorkInput):
    state: str = Field(min_length=16, max_length=512)
    code: str | None = Field(default=None, min_length=1, max_length=4096)
    error: str | None = Field(default=None, min_length=1, max_length=256)

    @model_validator(mode='after')
    def one_result(self):
        if (self.code is None) == (self.error is None):
            raise ValueError('Provide exactly one authorization code or denial')
        return self


def create_app(data_dir: Path) -> FastAPI:
    # One immutable bundle per service process; native turns never read/hash files.
    client_revision, served_bridge, served_release = client_release_assets()

    @asynccontextmanager
    async def lifespan(app):
        app.state.store = Store(data_dir)
        retrieval = None
        admin = None
        backend = None
        cold_runtime = None
        worker = None
        drive_authorization = None
        app.state.cold = None
        app.state.cold_runtime = None
        app.state.drive_reauthorization_required = False
        try:
            app.state.graph = Graph(app.state.store)
            from .jobs import Queue, Worker
            app.state.jobs = Queue(app.state.graph)
            from .processing import Processing
            app.state.processing = Processing(app.state.graph)
            from .admin import serve_admin
            admin = await serve_admin(app.state.graph, data_dir, app.state)
            app.state.blobs = app.state.store.blobs
            app.state.artifacts = app.state.store.artifacts
            from .cold_manager import ColdManager
            from .cold_runtime import ColdRuntime
            from .drive_auth import DriveAuthorization, DriveAuthorizationError, requires_reauthorization
            from .drive_connect import _publish
            drive = data_dir / 'drive-oauth.json'

            def prepare_drive(verified_backend):
                manager = ColdManager(app.state.artifacts, verified_backend, data_dir,
                    max_importance=float(os.environ.get('MEMENTO_COLD_MAX_IMPORTANCE', '0.75')))
                runtime = ColdRuntime(manager,
                    after_days=int(os.environ.get('MEMENTO_COLD_AFTER_DAYS', '90')),
                    min_bytes=int(os.environ.get('MEMENTO_COLD_MIN_BYTES', str(10 * 1024 * 1024))),
                    cache_bytes=int(os.environ.get('MEMENTO_COLD_CACHE_BYTES', str(1024 * 1024 * 1024))),
                    cache_ttl_seconds=int(os.environ.get('MEMENTO_COLD_CACHE_TTL_SECONDS', '3600')))
                return manager, runtime

            def install_drive(verified_backend, config, reconnect):
                nonlocal backend, cold_runtime
                if app.state.cold is not None:
                    if not reconnect:
                        raise DriveAuthorizationError(409, 'Drive is already connected')
                    manager = app.state.cold
                    with manager._mutex:
                        _publish(drive, config)
                        previous, manager.backend = manager.backend, verified_backend
                        backend = verified_backend
                    # The verified replacement is active; retiring the old HTTP
                    # client must not turn success into a closed new connection.
                    try:
                        previous.close()
                    except Exception as error:
                        logging.getLogger(__name__).warning(
                            'Retired Drive connection cleanup failed (%s)', type(error).__name__)
                    app.state.drive_reauthorization_required = False
                    return
                manager, runtime = prepare_drive(verified_backend)
                _publish(drive, config)
                app.state.cold, app.state.cold_runtime = manager, runtime
                try:
                    runtime.start()
                except BaseException:
                    app.state.cold = app.state.cold_runtime = None
                    raise
                backend, cold_runtime = verified_backend, runtime
                app.state.drive_reauthorization_required = False

            if drive.exists() or drive.is_symlink():
                from .cold import DriveColdStore
                try:
                    backend = await asyncio.to_thread(DriveColdStore.from_file, drive, data_dir=data_dir)
                except Exception as error:
                    if not requires_reauthorization(error):
                        raise
                    app.state.drive_reauthorization_required = True
                else:
                    app.state.cold, cold_runtime = prepare_drive(backend)
                    app.state.cold_runtime = cold_runtime
            app.state.media = None
            enabled = os.environ.get('MEMENTO_EMBEDDINGS', '0')
            if enabled not in ('0', '1'):
                raise RuntimeError('MEMENTO_EMBEDDINGS must be 0 or 1')
            handlers = {}
            if enabled == '1':
                from .retrieval import Retrieval
                retrieval = await asyncio.to_thread(Retrieval, app.state.graph)
                from .ingest import MediaIngest
                app.state.media = MediaIngest(retrieval, app.state.artifacts)
                def process_media(job):
                    if app.state.cold is not None:
                        app.state.cold.fetch(job.payload['sha256'])
                    return app.state.media.ingest(**dict(job.payload, command_id='job/' + job.id), lease=job)
                handlers['media'] = process_media
                retrieval.start()
            app.state.retrieval = retrieval
            worker = Worker(app.state.jobs, handlers, maintenance=app.state.store.compress_raw_step)
            worker.start()
            if cold_runtime is not None:
                cold_runtime.start()
            drive_authorization = DriveAuthorization(data_dir, install_drive)
            app.state.drive_authorization = drive_authorization
            async with mcp_app.lifespan(app):
                yield
        finally:
            if drive_authorization is not None:
                await asyncio.to_thread(drive_authorization.close)
            if worker is not None:
                await asyncio.to_thread(worker.close)
            if admin is not None:
                server, path, connections = admin
                server.close()
                await server.wait_closed()
                if connections:
                    await asyncio.gather(*tuple(connections))
                path.unlink(missing_ok=True)
            if cold_runtime is not None:
                await asyncio.to_thread(cold_runtime.close)
            if backend is not None:
                await asyncio.to_thread(backend.close)
            if retrieval is not None:
                await asyncio.to_thread(retrieval.close)
            app.state.store.close()

    app = FastAPI(title='Memento evidence service', lifespan=lifespan)
    app.add_middleware(BodyLimit)
    allowed_hosts = [item.strip() for item in os.environ.get('MEMENTO_ALLOWED_HOSTS', '').split(',') if item.strip()]
    mcp_app = create_mcp(app.state).http_app(path='/', stateless_http=True,
        host_origin_protection=True, allowed_hosts=allowed_hosts or None)
    @app.api_route('/mcp', methods=['GET', 'POST', 'DELETE'], include_in_schema=False)
    def mcp_redirect(request: Request):
        # Relative redirects preserve private HTTPS without trusting proxy headers.
        location = '/mcp/'
        if request.url.query:
            location += '?' + request.url.query
        return Response(status_code=307, headers={'Location': location})

    app.mount('/mcp', mcp_app)
    install_viewer(app, allowed_hosts)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, error):
        # Do not echo raw evidence in errors. Invalid Unicode/NaN inputs cannot
        # safely pass through the default JSON serializer either.
        detail = [{key: item[key] for key in ('loc', 'msg', 'type')}
                  for item in error.errors()]
        return Response(json.dumps({'detail': detail}, ensure_ascii=True),
                        status_code=422, media_type='application/json')

    @app.exception_handler(RawTextUnavailable)
    async def missing_raw_source(request, error):
        return Response(json.dumps({'detail': str(error)}), status_code=503, media_type='application/json')

    @app.get('/health')
    def health():
        app.state.store.health()
        if app.state.retrieval is not None:
            try:
                app.state.retrieval.health()
            except RuntimeError as error:
                raise HTTPException(503, str(error)) from error
        if app.state.drive_reauthorization_required:
            raise HTTPException(503, 'Drive reauthorization required')
        if app.state.cold_runtime is not None and app.state.cold_runtime.last_error is not None:
            cold = app.state.cold_runtime
            raise HTTPException(503, dict(error='Cold archival incomplete',
                archive_error=cold.last_error, archive_failure_count=cold.sweep_failure_count))
        return {'status': 'ok', 'processing': 'host_runner'}

    @app.get('/status')
    def status(source: Annotated[str | None, Query(min_length=1, max_length=256)] = None):
        from .status import operational_status
        return operational_status(app.state, source)

    async def drive_request(request, operation, arguments):
        # Only the local runner relays OAuth results. Browsers never POST these
        # endpoints directly, including a hostile page targeting loopback.
        if request.headers.get('origin') is not None:
            raise HTTPException(403, 'Use the Memento runner for Drive authorization')
        from .drive_auth import DriveAuthorizationError
        try:
            return await asyncio.to_thread(operation, **arguments)
        except DriveAuthorizationError as error:
            raise HTTPException(error.status_code, str(error)) from None
        except Exception:
            # Provider exceptions can contain authorization codes or tokens.
            raise HTTPException(502, 'Drive authorization failed; no connection is confirmed') from None

    @app.post('/drive/connect/begin')
    async def begin_drive(value: DriveBeginInput, request: Request, response: Response):
        response.headers['Cache-Control'] = 'no-store'
        if app.state.cold is not None and not value.reconnect:
            raise HTTPException(409, 'Drive is already connected')
        return await drive_request(request, app.state.drive_authorization.begin, value.arguments())

    @app.post('/drive/connect/finish')
    async def finish_drive(value: DriveFinishInput, request: Request, response: Response):
        response.headers['Cache-Control'] = 'no-store'
        return await drive_request(request, app.state.drive_authorization.finish, value.arguments())

    @app.post('/episodes', status_code=201)
    def capture(episode: EpisodeInput, response: Response):
        try:
            stored, created = app.state.store.capture(episode.evidence())
        except ConflictError as error:
            raise HTTPException(409, str(error)) from error
        if not created:
            response.status_code = 200
        return {'id': stored['id']}

    @app.get('/episodes/search')
    def search(q: Annotated[str | None, Query(max_length=4096)] = None,
               limit: Annotated[int, Query(ge=1, le=100)] = 20,
               conversation_id: str | None = None, source: str | None = None,
               since: Annotated[AwareDatetime | None, Query()] = None):
        try:
            return {'episodes': app.state.store.search(q, limit, conversation_id, source,
                since=since.isoformat() if since is not None else None)}
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    @app.get('/episodes/{episode_id}')
    def get(episode_id: str):
        episode = app.state.store.get(episode_id)
        if episode is None:
            raise HTTPException(404, 'Episode not found')
        return episode

    def graph_command(method, command):
        try:
            return method(**command.arguments())
        except (ConflictError, LostLease) as error:
            raise HTTPException(409, str(error)) from error
        except (ValueError, UnicodeEncodeError) as error:
            raise HTTPException(422, str(error)) from error

    @app.post('/remember')
    def remember(command: Remember | MediaInput, response: Response):
        if isinstance(command, MediaInput):
            response.status_code = 202
            return ingest_media(command)
        return graph_command(app.state.graph.remember, command)

    @app.post('/link')
    def link(command: Link):
        return graph_command(app.state.graph.link, command)

    @app.post('/supersede')
    def supersede(command: Supersede):
        return graph_command(app.state.graph.supersede, command)

    @app.post('/loop')
    def loop(command: Loop):
        return graph_command(app.state.graph.loop, command)

    @app.get('/history/{node_id}')
    def history(node_id: str):
        """Read full assertion lineage, immutable versions, word diffs and exact evidence."""
        versions = app.state.graph.history(node_id)
        if not versions:
            raise HTTPException(404, 'Node not found')
        return {'versions': versions}

    @app.post('/blobs', status_code=201)
    async def upload_blob(request: Request):
        info = await app.state.blobs.put_stream(request.stream())
        try:
            return await asyncio.to_thread(app.state.artifacts.register, info,
                request.headers.get('content-type', 'application/octet-stream'))
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    @app.get('/blobs/{sha256}')
    def get_blob(sha256: str):
        reader = None
        try:
            info = app.state.artifacts.get(sha256)
            if info is None:
                raise HTTPException(404, 'Blob not found')
            if app.state.cold is not None:
                reader = app.state.cold.reader(sha256)
                source = reader.__enter__()
            else:
                path = app.state.blobs.verify(sha256)
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except FileNotFoundError:
            raise HTTPException(404, 'Blob not found') from None
        if reader is not None:
            async def content():
                while chunk := await asyncio.to_thread(source.read, 1024 * 1024):
                    yield chunk
            return BlobResponse(content(), reader, media_type=info['mime'],
                headers={'Content-Length': str(info['size']),
                         'Content-Disposition': f'attachment; filename="{sha256}"'})
        return FileResponse(path, media_type=info['mime'] if info else 'application/octet-stream',
                            filename=sha256)

    @app.get('/recall')
    def recall(q: Annotated[str, Query(max_length=4096)] = '',
               effort: str = 'normal',
               limit: Annotated[int, Query(ge=1, le=100)] = 10,
               scope: Literal['personal', 'professional'] | None = None,
               since: Annotated[AwareDatetime | None, Query()] = None,
               source: str | None = None, automatic: bool = False,
               context: Annotated[str, Query(max_length=4096)] = ''):
        if q.strip() and since is None and source is None and app.state.retrieval is None:
            raise HTTPException(503, 'Native embedding runtime is disabled')
        try:
            return {'client_revision': client_revision, 'memories': recall_memories(app.state, q, effort, limit, scope=scope,
                since=since.isoformat() if since is not None else None, source=source,
                automatic=automatic, context=context)}
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    @app.post('/media', status_code=202)
    def ingest_media(command: MediaInput):
        try:
            return submit_media(app.state, command)
        except (ConflictError, LostLease) as error:
            raise HTTPException(409, str(error)) from error
        except RuntimeError as error:
            raise HTTPException(503, str(error)) from error
        except LookupError as error:
            raise HTTPException(404, str(error)) from error
        except (ValueError, UnicodeEncodeError) as error:
            raise HTTPException(422, str(error)) from error

    @app.get('/jobs/{job_id}')
    def get_job(job_id: str):
        result = app.state.jobs.get(job_id)
        if result is None:
            raise HTTPException(404, 'Job not found')
        return result

    @app.get('/connect')
    def agent_connection():
        return Response(
            """Memento: connect this native agent to the private service.

The agent performs installation; do not ask the human to run commands, edit
configuration, move credentials, or change chats. Use the private service origin
from which you fetched this guide, not the public product website.

1. Identify your native harness: pi, omp, claude, or codex. Keep its existing
   account, login, model selection, unrelated hooks and MCP definitions.
2. Download /runner into ~/.local/share/memento/runner.py (private parent,
   executable only by this user). It needs only Python 3's stdlib. Expose it as
   ~/.local/bin/memento with a symlink to that same runner, not a second copy;
   the processing timer must keep using the same file after upgrades. Never
   replace an unrelated existing command. An installed Memento package also
   supports the agent command. Use the full executable path if not on PATH.
3. Run: memento agent add HARNESS --url SERVICE_ORIGIN
   Substitute pi, omp, claude or codex and this private service origin.
   This installs only that harness's capture, automatic recall and memory tools.
   A matching default runner.json keeps its role and identity; otherwise the
   config is ${XDG_CONFIG_HOME:-$HOME/.config}/memento/HARNESS/runner.json.
   Reinstallation reuses that config. Use --config PATH for an existing custom
   location. No timezone, model discovery, new login or extra timer is needed.
   It never creates a server processor or demotes an existing one.
   Without the command link, use:
   python3 ~/.local/share/memento/runner.py agent add HARNESS --url SERVICE_ORIGIN
4. Load the installed native integration using the harness's supported reload
   or a fresh native session. Native trust/permission prompts remain human
   decisions: surface just that approval if required; never grant it yourself
   or evade an owner-session admission guard.
5. Verify the six native tools (recall, remember, link, supersede, history, loop),
   call recall, and verify a new native exchange was captured. Installation
   alone is not a connected result. Report any actual permission/reload blocker
   precisely instead of claiming success.

Durable client updates are explicit, never automatic:
Updated clients can report a different available client bundle once through an
existing recall request. The notice never installs code or restarts a session.
Older clients need an explicit update first before they can display notices.
Run: memento agent update HARNESS
Use the same --config PATH and --native-config-dir PATH if installation used
custom locations. Update reads the exact existing private service URL; it has no
--url option and never retargets the installation. It preserves private config,
client identity, client/server role, outbox/cursors, native settings and timers.
The whole runner/bridge/extension bundle is validated before any replacement.
Each file is published atomically, with the standalone runner last. This is not
a single whole-installation transaction: rerun update after an interruption to
finish the cutover. A failed download leaves installed code unchanged.
The existing standalone runner path and its command symlink stay the same.
Update never discovers models, changes logins, activates timers or restarts an
active session. Use supported native reload or a fresh session to load new code;
Codex hook trust remains a human decision. With no command link, use:
python3 ~/.local/share/memento/runner.py agent update HARNESS
If a release changes hook or MCP definitions, rerun agent add for that harness.

Google Drive is optional and service-wide, not a separate grant for each agent.
Native setup, a healthy API, the public website and Google branding do not mean
Drive is connected. Complete these steps when the human requests Drive:
1. On the human's browser device run runner.py drive-connect --url SERVICE_ORIGIN.
   An already-connected service needs no new credential lookup.
   For explicitly requested fresh consent while connected, add --reconnect;
   the existing grant stays active until its same-binding replacement verifies.
   If a Google link is printed, keep the helper running, let the human approve
   drive.file, and await verified success. Only an unavailable-authorization error sends
   you to the provisioning steps below; never present that error as a link.
2. On the SERVICE HOST, the service owner provisions the original Desktop
   client, intended account and existing app-created folder with:
   memento-drive configure --client-json PRIVATE_CLIENT_PATH --account EMAIL
   --folder-id EXISTING_FOLDER_ID --data-dir DATA_DIR
   Look in the human's authorized credential store first; an Infisical
   deployment may use MEMENTO_GOOGLE_DESKTOP_CLIENT_JSON. Install its CLI if
   missing and surface only its required human login. Reuse original binding
   metadata; ask only for metadata not available. Do not repeat Google Console
   setup or create a replacement client or folder.
3. Use explicitly authorized credential-manager injection into the owner
   helper; never display secret values, move token files, share the service
   identity or elevate privileges. If owner approval is missing, pause.
   After provisioning, retry step 1 on the browser device. Only verified
   service activation and a successful helper exit mean Drive is connected.
""",
            media_type='text/plain', headers={'Cache-Control': 'no-store'})

    @app.get('/runner')
    def runner_source():
        return FileResponse(Path(__file__).with_name('runner.py'), media_type='text/x-python',
                            filename='memento-runner.py')

    @app.get('/bridge')
    def bridge_source():
        return Response(served_bridge, media_type='text/x-python', headers={
            'Cache-Control': 'no-store', 'Content-Disposition': 'attachment; filename="memento-bridge.py"'})

    @app.get('/client-release')
    def client_release():
        # Same private service/TLS boundary as /runner; no credentials or data.
        return Response(served_release, media_type='application/json',
                        headers={'Cache-Control': 'no-store'})

    @app.get('/integrations/{agent}')
    def native_extension(agent: Literal['pi', 'omp', 'shared']):
        return FileResponse(Path(__file__).with_name('native_' + agent + '.ts'),
                            media_type='text/plain', filename='native_' + agent + '.ts')

    @app.post('/work/compaction')
    def compact(command: CompactionInput):
        return graph_command(app.state.processing.compact, command)

    @app.post('/work/claim')
    def claim_work(command: ClaimInput):
        return graph_command(app.state.processing.claim, command)

    @app.post('/work/renew')
    def renew_work(command: LeaseInput):
        return graph_command(app.state.processing.renew, command)

    @app.post('/work/complete')
    def complete_work(command: CompleteInput):
        return graph_command(app.state.processing.complete, command)

    @app.post('/work/release')
    def release_work(command: ReleaseInput):
        return graph_command(app.state.processing.release, command)

    @app.post('/usage')
    def record_citations(command: CitationInput):
        from .importance import cite
        return graph_command(lambda **values: cite(app.state.graph, **values), command)

    return app
