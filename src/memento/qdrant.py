"""Private, disposable Qdrant child and its small REST boundary.

SQLite owns the evidence and original float32 vectors. This module never opens
SQLite and never accepts a remote endpoint. A configured failed child is an
explicit search outage, not an empty result or an exact-search fallback.
"""

import hashlib
import fcntl
import json
import logging
import math
import os
from pathlib import Path
import secrets
import shutil
import socket
import stat
import subprocess
import sys
import time

import httpx

VERSION = '1.20.0'
BINARY_SHA256 = '12eb59dbd9401b5e4b6bef8021e98ec5893276eed27f97dc02372719a22753d9'
COLLECTION = 'memento_vectors'
CHECKPOINT = 'ffffffff-ffff-ffff-ffff-ffffffffffff'
FIELDS = dict(kind='keyword', type='keyword', current='bool', ordinary='bool', automatic='bool',
              historical='bool', parked_candidate='bool', entity='bool', personal='bool', professional='bool')


class SearchUnavailable(RuntimeError):
    """Fixed messages only; child output, vectors and service URLs stay private."""


class Qdrant:
    def __init__(self, binary, directory, dimensions):
        self.directory = Path(directory).absolute()
        self.dimensions = dimensions
        self.process = None
        self.client = None
        self._lease = None
        binary = Path(binary)
        self.binary = binary
        if not binary.is_absolute() or not binary.is_file():
            raise SearchUnavailable('Configured search binary is missing')
        with binary.open('rb') as stream:
            signature = hashlib.file_digest(stream, 'sha256').hexdigest()
        if signature != BINARY_SHA256:
            raise SearchUnavailable('Configured search binary differs from the pinned release')
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        info = self.directory.stat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise SearchUnavailable('Search directory must be private and owned by the service')
        with socket.socket() as reservation:
            reservation.bind(('127.0.0.1', 0))
            port = reservation.getsockname()[1]
        key = secrets.token_hex(32)
        config = {'log_level': 'WARN', 'telemetry_disabled': True,
            'storage': {'storage_path': str(self.directory / 'storage'),
                'snapshots_path': str(self.directory / 'snapshots'),
                'performance': {'max_search_threads': 2, 'max_optimization_threads': 1}},
            'service': {'host': '127.0.0.1', 'http_port': port, 'grpc_port': None,
                'max_workers': 2, 'enable_cors': False, 'api_key': key,
                'enable_snapshot_url_recovery': False}, 'cluster': {'enabled': False}}
        path = self.directory / 'config.yaml'
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, 'w') as stream:
            json.dump(config, stream)  # JSON is valid YAML; no YAML dependency.
        environment = {'PATH': os.environ.get('PATH', '/usr/bin:/bin'), 'LANG': 'C.UTF-8'}
        if os.environ.get('LD_LIBRARY_PATH'):
            environment['LD_LIBRARY_PATH'] = os.environ['LD_LIBRARY_PATH']
        storage = self.directory / 'storage'
        recoverable = storage.is_dir() and not storage.is_symlink()
        previous = None
        try:
            # The child inherits this OS lease. A new parent cannot mistake an
            # old child still flushing after parent death for corrupt storage.
            self._lease = (self.directory / 'child.lock').open('a')
            os.fchmod(self._lease.fileno(), 0o600)
            deadline = time.monotonic() + 5
            while True:
                try:
                    fcntl.flock(self._lease, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    break
                except BlockingIOError:
                    if time.monotonic() >= deadline:
                        raise SearchUnavailable('Previous search child has not finished shutdown') from None
                    time.sleep(.05)
            for attempt in range(2):
                try:
                    self._start_child(binary, path, environment, port, key)
                except (SearchUnavailable, OSError):
                    self._stop_child()
                    if attempt == 0 and recoverable:
                        previous = self.directory / ('unreadable-' + secrets.token_hex(8))
                        storage.replace(previous)
                        continue
                    if previous is not None:
                        if storage.exists():
                            shutil.rmtree(storage)
                        previous.replace(storage)
                    raise SearchUnavailable('Search child could not reopen its private index') from None
                if previous is not None:
                    logging.getLogger(__name__).warning('Search storage could not reopen; rebuilding the disposable index from SQLite')
                    shutil.rmtree(previous)
                return
        except BaseException:
            self.close()
            raise

    def _start_child(self, binary, path, environment, port, key):
        # Set parent-death signaling in a short stdlib child, then exec the real
        # binary in that same PID. Avoid preexec_fn after model threads exist.
        launch = ("import ctypes,os,signal,sys\n"
                  "parent=int(sys.argv[1])\n"
                  "if os.getppid()!=parent: os._exit(70)\n"
                  "if ctypes.CDLL(None,use_errno=True).prctl(1,signal.SIGTERM,0,0,0)!=0: os._exit(70)\n"
                  "if os.getppid()!=parent: os._exit(70)\n"
                  "os.execv(sys.argv[2],sys.argv[2:])\n")
        self.process = subprocess.Popen([sys.executable, '-c', launch, str(os.getpid()), str(binary), '--config-path', str(path)],
            cwd=self.directory, env=environment, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL, pass_fds=(self._lease.fileno(),))
        self.client = httpx.Client(base_url=f'http://127.0.0.1:{port}', trust_env=False,
            follow_redirects=False, headers={'api-key': key})
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            self._alive()
            # Never contact another process during the ephemeral-port handoff.
            if self._owns_listener(port):
                result = self.request('GET', '/', deadline=deadline)
                if result.get('version') != VERSION:
                    raise SearchUnavailable('Search child version differs from the pinned release')
                return
            time.sleep(.05)
        raise SearchUnavailable('Search child startup deadline exceeded')

    def _alive(self):
        if self.process is None or self.process.poll() is not None:
            raise SearchUnavailable('Search child is unavailable')

    @property
    def running(self):
        return self.process is not None and self.process.poll() is None

    def reopen(self):
        """Existing synchronizer owns retries; this is one bounded local reopen."""
        binary, directory, dimensions = self.binary, self.directory, self.dimensions
        self.close()
        logging.getLogger(__name__).warning('Search child exited; reopening its private derived index')
        self.__init__(binary, directory, dimensions)

    def _owns_listener(self, port):
        try:
            handles = set()
            for entry in Path(f'/proc/{self.process.pid}/fd').iterdir():
                try:
                    target = entry.readlink().as_posix()
                except OSError:
                    continue
                if target.startswith('socket:['):
                    handles.add(target[8:-1])
            for line in Path(f'/proc/{self.process.pid}/net/tcp').read_text().splitlines()[1:]:
                row = line.split()
                if row[1] == f'0100007F:{port:04X}' and row[3] == '0A' and row[9] in handles:
                    return True
        except OSError:
            pass
        return False

    def request(self, method, path, body=None, *, deadline, missing=False):
        self._alive()
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SearchUnavailable('Search synchronization or request deadline exceeded')
        try:
            response = self.client.request(method, path, json=body, timeout=min(2, remaining))
            if missing and response.status_code == 404:
                return None
            response.raise_for_status()
            value = response.json()
            if not isinstance(value, dict):
                raise ValueError
            if time.monotonic() > deadline:
                raise SearchUnavailable('Search synchronization or request deadline exceeded')
            return value
        except httpx.HTTPStatusError as error:
            raise SearchUnavailable(f'Search request rejected (HTTP {error.response.status_code}); the derived index remains pending') from None
        except httpx.TimeoutException:
            raise SearchUnavailable('Search request timed out; the derived index remains pending') from None
        except httpx.RequestError:
            raise SearchUnavailable('Search connection failed; the derived index remains pending') from None
        except ValueError:
            raise SearchUnavailable('Search returned invalid JSON; the derived index remains pending') from None

    def info(self, *, deadline):
        value = self.request('GET', '/collections/' + COLLECTION, deadline=deadline, missing=True)
        if value is not None and not isinstance(value.get('result'), dict):
            raise SearchUnavailable('Search returned invalid collection metadata')
        return value['result'] if value else None

    def configured(self, info):
        try:
            config = info['config']
            vectors = config['params']['vectors']
            hnsw = config['hnsw_config']
            return (vectors['size'] == self.dimensions and vectors['distance'] == 'Cosine'
                and vectors.get('datatype') == 'float32' and vectors['on_disk'] is True
                and config['params']['shard_number'] == 1 and config['params']['on_disk_payload'] is True
                and config.get('quantization_config') is None
                and all(hnsw[key] == value for key, value in dict(m=16, ef_construct=200,
                    full_scan_threshold=10000, max_indexing_threads=2, on_disk=True).items())
                and config['optimizer_config']['indexing_threshold'] == 1
                and config['optimizer_config']['max_optimization_threads'] == 1
                and config['optimizer_config']['default_segment_number'] == 2
                and all(info['payload_schema'][name]['data_type'] == schema for name, schema in FIELDS.items()))
        except (KeyError, TypeError):
            return False

    def reset(self, *, deadline):
        if self.info(deadline=deadline) is not None:
            self.request('DELETE', '/collections/' + COLLECTION, deadline=deadline)
        self.request('PUT', '/collections/' + COLLECTION, {
            'vectors': {'size': self.dimensions, 'distance': 'Cosine', 'datatype': 'float32', 'on_disk': True},
            'shard_number': 1, 'on_disk_payload': True,
            'hnsw_config': {'m': 16, 'ef_construct': 200, 'full_scan_threshold': 10000,
                            'max_indexing_threads': 2, 'on_disk': True},
            'optimizers_config': {'indexing_threshold': 0, 'max_optimization_threads': 1,
                                  'default_segment_number': 2}}, deadline=deadline)
        # Payload indices precede vectors, matching the measured filter-aware graph.
        for name, schema in FIELDS.items():
            self.request('PUT', '/collections/' + COLLECTION + '/index?wait=true&ordering=strong',
                         {'field_name': name, 'field_schema': schema}, deadline=deadline)

    def finish_build(self, *, deadline):
        self.request('PATCH', '/collections/' + COLLECTION,
                     {'optimizers_config': {'indexing_threshold': 1, 'max_optimization_threads': 1}}, deadline=deadline)

    def upsert(self, points, *, deadline):
        if points:
            value = self.request('PUT', '/collections/' + COLLECTION + '/points?wait=true&ordering=strong',
                                 {'points': points}, deadline=deadline)
            if not isinstance(value.get('result'), dict) or value['result'].get('status') != 'completed':
                raise SearchUnavailable('Search mutation was not confirmed complete')

    def delete(self, ids, *, deadline):
        if ids:
            value = self.request('POST', '/collections/' + COLLECTION + '/points/delete?wait=true&ordering=strong',
                                 {'points': ids}, deadline=deadline)
            if not isinstance(value.get('result'), dict) or value['result'].get('status') != 'completed':
                raise SearchUnavailable('Search mutation was not confirmed complete')

    def checkpoint(self, *, deadline):
        value = self.request('GET', '/collections/' + COLLECTION + '/points/' + CHECKPOINT,
                             deadline=deadline, missing=True)
        try:
            return value['result']['payload'].get('checkpoint') if value else None
        except (KeyError, TypeError, AttributeError):
            raise SearchUnavailable('Search returned invalid checkpoint metadata') from None

    def commit_checkpoint(self, checkpoint, *, deadline):
        self.upsert([{'id': CHECKPOINT, 'vector': [1.] + [0.] * (self.dimensions - 1),
                      'payload': {'current': False, 'checkpoint': checkpoint}}], deadline=deadline)

    def search(self, vector, limit, equals, *, deadline):
        result = self.request('POST', '/collections/' + COLLECTION + '/points/query', {
            'query': vector.tolist(), 'limit': limit,
            'filter': {'must': [{'key': key, 'match': {'value': value}} for key, value in equals.items()]},
            'params': {'hnsw_ef': 2048, 'exact': False}, 'with_payload': False, 'with_vector': False}, deadline=deadline)
        try:
            points = result['result']['points']
            if not isinstance(points, list) or len(points) > limit:
                raise ValueError
            rows = []
            for point in points:
                identifier, score = point['id'], point['score']
                if isinstance(identifier, bool) or not isinstance(identifier, int) or identifier < 0:
                    raise ValueError
                if isinstance(score, bool) or not isinstance(score, (float, int)) or not math.isfinite(score):
                    raise ValueError
                rows.append({'seq': identifier, 'cosine': float(score)})
            if len({row['seq'] for row in rows}) != len(rows):
                raise ValueError
            return rows
        except (KeyError, TypeError, ValueError):
            raise SearchUnavailable('Search returned an invalid result') from None

    def _stop_child(self):
        if self.client is not None:
            self.client.close()
            self.client = None
        if self.process is not None:
            if self.process.poll() is None:
                self.process.terminate()
                try:
                    self.process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    self.process.kill()
            self.process.wait(timeout=3)
            self.process = None

    def close(self):
        self._stop_child()
        if self._lease is not None:
            self._lease.close()
            self._lease = None
