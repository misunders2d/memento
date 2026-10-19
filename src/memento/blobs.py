"""Immutable, crash-safe local content-addressed artifacts."""

import asyncio
import hashlib
import os
import re
import tempfile
from pathlib import Path
from typing import BinaryIO


class BlobStore:
    def __init__(self, root: Path):
        self.root = root
        root.mkdir(parents=True, exist_ok=True, mode=0o700)

    def path(self, digest: str) -> Path:
        if not re.fullmatch(r'[0-9a-f]{64}', digest):
            raise ValueError('Expected a lowercase SHA-256 digest')
        return self.root / digest[:2] / digest[2:]

    def put(self, source: BinaryIO) -> dict:
        digest = hashlib.sha256()
        size = 0
        fd, name = tempfile.mkstemp(prefix='.ingest-', dir=self.root)
        temporary = Path(name)
        try:
            with os.fdopen(fd, 'wb') as target:
                while chunk := source.read(1024 * 1024):
                    digest.update(chunk)
                    size += len(chunk)
                    target.write(chunk)
                target.flush()
                os.fsync(target.fileno())
            return self._publish(temporary, digest.hexdigest(), size)
        finally:
            temporary.unlink(missing_ok=True)

    def _publish(self, temporary: Path, sha256: str, size: int) -> dict:
        destination = self.path(sha256)
        destination.parent.mkdir(mode=0o700, exist_ok=True)
        try:
            os.link(temporary, destination)
        except FileExistsError:
            try:
                self.verify(sha256)
            except OSError:
                # The staged bytes hash to this address; restoring damaged
                # bytes repairs the same artifact, never a different version.
                os.replace(temporary, destination)
        self._sync(destination.parent)
        self._sync(self.root)
        return {'sha256': sha256, 'size': size, 'tier': 'hot'}

    async def put_stream(self, chunks) -> dict:
        """Consume an asynchronous byte stream with bounded memory."""
        digest = hashlib.sha256()
        size = 0
        fd, name = tempfile.mkstemp(prefix='.ingest-', dir=self.root)
        temporary = Path(name)
        try:
            with os.fdopen(fd, 'wb') as target:
                async for chunk in chunks:
                    digest.update(chunk)
                    size += len(chunk)
                    await asyncio.to_thread(target.write, chunk)
                await asyncio.to_thread(target.flush)
                await asyncio.to_thread(os.fsync, target.fileno())
            return await asyncio.to_thread(self._publish, temporary, digest.hexdigest(), size)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _sync(path: Path):
        fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def verify(self, digest: str) -> Path:
        path = self.path(digest)
        with path.open('rb') as source:
            actual = hashlib.file_digest(source, 'sha256').hexdigest()
        if actual != digest:
            raise OSError(f'Corrupt blob: {digest}')
        return path

    def open(self, digest: str) -> BinaryIO:
        return self.verify(digest).open('rb')
