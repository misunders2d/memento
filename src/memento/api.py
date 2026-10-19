import json
import asyncio
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import FileResponse
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, field_validator

from .store import ConflictError, Store
from .commands import Link, Loop, MediaInput, Remember, Supersede
from .mcp_server import create_mcp, recall_memories
from .graph import Graph
from .blobs import BlobStore
from .artifacts import Artifacts
from .jobs import LostLease


MAX_CAPTURE_BYTES = 1024 * 1024
MAX_BLOB_BYTES = 1024 * 1024 * 1024


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


def create_app(data_dir: Path) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app):
        app.state.store = Store(data_dir)
        retrieval = None
        admin = None
        backend = None
        cold_runtime = None
        worker = None
        app.state.cold = None
        app.state.cold_runtime = None
        try:
            app.state.graph = Graph(app.state.store)
            from .jobs import Queue, Worker
            app.state.jobs = Queue(app.state.graph)
            from .processing import Processing
            app.state.processing = Processing(app.state.graph)
            from .admin import serve_admin
            admin = await serve_admin(app.state.graph, data_dir, app.state)
            app.state.blobs = BlobStore(data_dir / 'blobs')
            app.state.artifacts = Artifacts(app.state.store, app.state.blobs)
            drive = data_dir / 'drive-oauth.json'
            if drive.exists() or drive.is_symlink():
                from .cold import DriveColdStore
                from .cold_manager import ColdManager
                from .cold_runtime import ColdRuntime
                backend = await asyncio.to_thread(DriveColdStore.from_file, drive)
                app.state.cold = ColdManager(app.state.artifacts, backend, data_dir,
                    max_importance=float(os.environ.get('MEMENTO_COLD_MAX_IMPORTANCE', '0.75')))
                cold_runtime = ColdRuntime(app.state.cold,
                    after_days=int(os.environ.get('MEMENTO_COLD_AFTER_DAYS', '90')),
                    min_bytes=int(os.environ.get('MEMENTO_COLD_MIN_BYTES', str(10 * 1024 * 1024))))
                app.state.cold_runtime = cold_runtime
            app.state.media = None
            enabled = os.environ.get('MEMENTO_EMBEDDINGS', '0')
            if enabled not in ('0', '1'):
                raise RuntimeError('MEMENTO_EMBEDDINGS must be 0 or 1')
            if enabled == '1':
                from .retrieval import Retrieval
                retrieval = await asyncio.to_thread(Retrieval, app.state.graph)
                from .ingest import MediaIngest
                app.state.media = MediaIngest(retrieval, app.state.artifacts)
                def process_media(job):
                    if app.state.cold is not None:
                        app.state.cold.fetch(job.payload['sha256'])
                    return app.state.media.ingest(**dict(job.payload, command_id='job/' + job.id), lease=job)
                worker = Worker(app.state.jobs, {'media': process_media})
                worker.start()
                retrieval.start()
            app.state.retrieval = retrieval
            if cold_runtime is not None:
                cold_runtime.start()
            async with mcp_app.lifespan(app):
                yield
        finally:
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
    app.mount('/mcp', mcp_app)

    @app.exception_handler(RequestValidationError)
    async def validation_error(request, error):
        # Do not echo raw evidence in errors. Invalid Unicode/NaN inputs cannot
        # safely pass through the default JSON serializer either.
        detail = [{key: item[key] for key in ('loc', 'msg', 'type')}
                  for item in error.errors()]
        return Response(json.dumps({'detail': detail}, ensure_ascii=True),
                        status_code=422, media_type='application/json')

    @app.get('/health')
    def health():
        app.state.store.health()
        if app.state.retrieval is not None:
            try:
                app.state.retrieval.health()
            except RuntimeError as error:
                raise HTTPException(503, str(error)) from error
        if app.state.cold_runtime is not None and app.state.cold_runtime.last_error is not None:
            cold = app.state.cold_runtime
            raise HTTPException(503, dict(error='Cold archival incomplete',
                archive_error=cold.last_error, archive_failure_count=cold.sweep_failure_count))
        return {'status': 'ok', 'processing': 'host_runner'}

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
    def remember(command: Remember):
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
        try:
            path = (app.state.cold.fetch(sha256) if app.state.cold is not None
                    else app.state.blobs.verify(sha256))
        except ValueError as error:
            raise HTTPException(422, str(error)) from error
        except FileNotFoundError:
            raise HTTPException(404, 'Blob not found') from None
        info = app.state.artifacts.get(sha256)
        return FileResponse(path, media_type=info['mime'] if info else 'application/octet-stream',
                            filename=sha256)

    @app.get('/recall')
    def recall(q: Annotated[str, Query(max_length=4096)] = '',
               effort: str = 'normal',
               limit: Annotated[int, Query(ge=1, le=100)] = 10,
               scope: Literal['personal', 'professional'] | None = None,
               since: Annotated[AwareDatetime | None, Query()] = None,
               source: str | None = None):
        if q.strip() and since is None and source is None and app.state.retrieval is None:
            raise HTTPException(503, 'Native embedding runtime is disabled')
        try:
            return {'memories': recall_memories(app.state, q, effort, limit, scope=scope,
                since=since.isoformat() if since is not None else None, source=source)}
        except ValueError as error:
            raise HTTPException(422, str(error)) from error

    @app.post('/media', status_code=202)
    def ingest_media(command: MediaInput):
        if app.state.media is None:
            raise HTTPException(503, 'Native embedding runtime is disabled')
        try:
            if app.state.artifacts.get(command.sha256) is None:
                raise HTTPException(404, 'Source blob not found')
            return graph_command(app.state.jobs.submit_media, command)
        except FileNotFoundError:
            raise HTTPException(404, 'Source blob not found') from None

    @app.get('/jobs/{job_id}')
    def get_job(job_id: str):
        result = app.state.jobs.get(job_id)
        if result is None:
            raise HTTPException(404, 'Job not found')
        return result

    @app.get('/runner')
    def runner_source():
        return FileResponse(Path(__file__).with_name('runner.py'), media_type='text/x-python',
                            filename='memento-runner.py')

    @app.get('/bridge')
    def bridge_source():
        return FileResponse(Path(__file__).with_name('bridge.py'), media_type='text/x-python',
                            filename='memento-bridge.py')

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
