"""Personal Drive cold artifacts using Google's official media transport."""

import hashlib
import os
import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Protocol

FIELDS = 'id,size,md5Checksum,sha256Checksum'
CHUNK = 8 * 1024 * 1024


@dataclass(frozen=True)
class ColdObject:
    id: str
    size: int
    sha256: str
    md5: str


class ColdStore(Protocol):
    def upload(self, path: Path, name: str, mime: str, checkpoint: dict | None,
               save_checkpoint: Callable[[dict], None]) -> ColdObject: ...
    def download(self, object_id: str, destination: Path, sha256: str) -> None: ...
    def list_backups(self) -> list[dict]: ...


def _hashes(path):
    sha, md5 = hashlib.sha256(), hashlib.md5(usedforsecurity=False)
    size = 0
    with Path(path).open('rb') as source:
        while chunk := source.read(1024 * 1024):
            sha.update(chunk)
            md5.update(chunk)
            size += len(chunk)
    return sha.hexdigest(), md5.hexdigest(), size


class DriveColdStore:
    def __init__(self, folder_id: str, account: str, *, service):
        self.folder_id = self._id(folder_id)
        if not account:
            raise ValueError('Explicit personal Drive account is required')
        self.service = service
        self._uploads = {}
        self._verify_account(service, account)
        self._verify_folder(service, self.folder_id)

    @classmethod
    def from_file(cls, path: Path):
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build
        from .drive_connect import SCOPES, load_connection
        config = load_connection(path)
        credentials = Credentials.from_authorized_user_info(config, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials, cache_discovery=False)
        try:
            return cls(config['memento']['folder_id'], config['memento']['account'], service=service)
        except BaseException:
            service.close()
            raise

    @staticmethod
    def _verify_account(service, account):
        about = service.about().get(fields='user(emailAddress)').execute()
        if about.get('user', {}).get('emailAddress', '').casefold() != account.casefold():
            raise ValueError('Drive OAuth account differs from the explicitly configured personal account')

    @staticmethod
    def _verify_folder(service, folder_id):
        folder = service.files().get(fileId=folder_id, fields='id,mimeType,trashed,ownedByMe').execute()
        if (folder.get('id') != folder_id or folder.get('mimeType') != 'application/vnd.google-apps.folder'
                or folder.get('trashed') or not folder.get('ownedByMe')):
            raise ValueError('Cold storage requires an owned, untrashed dedicated Drive folder')

    @staticmethod
    def _id(value):
        if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z0-9_-]+', value):
            raise ValueError('Invalid Drive object id')
        return value

    def _download_to(self, object_id, target):
        from googleapiclient.http import MediaIoBaseDownload
        download = MediaIoBaseDownload(target, self.service.files().get_media(fileId=object_id), chunksize=CHUNK)
        done = False
        while not done:
            _, done = download.next_chunk()

    def _verified(self, object_id, expected_sha, expected_md5, size):
        object_id = self._id(object_id)
        metadata = self.service.files().get(fileId=object_id, fields=FIELDS).execute()
        if metadata.get('id') != object_id:
            raise ValueError('Drive verification returned a different object')
        if int(metadata.get('size', -1)) != size:
            raise ValueError('Drive upload size mismatch; hot artifact retained')
        if metadata.get('md5Checksum') != expected_md5:
            raise ValueError('Drive upload MD5 mismatch; hot artifact retained')
        remote_sha = metadata.get('sha256Checksum')
        if remote_sha is None:
            sha = hashlib.sha256()
            with tempfile.TemporaryFile() as target:
                self._download_to(object_id, target)
                target.seek(0)
                while block := target.read(1024 * 1024):
                    sha.update(block)
            remote_sha = sha.hexdigest()
        if remote_sha != expected_sha:
            raise ValueError('Drive upload SHA-256 mismatch; hot artifact retained')
        return ColdObject(object_id, size, expected_sha, expected_md5)

    def list_backups(self):
        """Discover self-verifying snapshots without relying on the lost local DB."""
        result, token = [], None
        pattern = re.compile(r'memory-(?:incomplete-(?P<gaps>[1-9]\d{0,9})-)?(?P<id>\d{8}T\d{6}-[a-f0-9]{12})-(?P<sha>[a-f0-9]{64})\.sqlite')
        while True:
            page = self.service.files().list(
                q=f"'{self.folder_id}' in parents and trashed=false and name contains 'memory-'",
                fields='nextPageToken,files(id,name,size,sha256Checksum)', pageSize=1000, pageToken=token).execute()
            for item in page.get('files', []):
                match = pattern.fullmatch(item.get('name', ''))
                if match is None:
                    continue
                digest = match['sha']
                if item.get('sha256Checksum', digest) != digest:
                    raise ValueError('Remote backup name and checksum disagree')
                gap_count = int(match['gaps'] or 0)
                result.append(dict(id=match['id'], remote_id=self._id(item['id']), sha256=digest,
                                   size=int(item['size']), gap_count=gap_count, complete=gap_count == 0))
            token = page.get('nextPageToken')
            if not token:
                return sorted(result, key=lambda item: item['id'], reverse=True)

    def upload(self, path, name, mime, checkpoint, save_checkpoint):
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaFileUpload
        sha, md5, size = _hashes(path)
        checkpoint = dict(checkpoint or {})
        if checkpoint and (checkpoint.get('sha256') != sha or checkpoint.get('size') != size):
            raise ValueError('Upload checkpoint differs from immutable artifact')
        if checkpoint.get('object_id'):
            return self._verified(checkpoint['object_id'], sha, md5, size)
        media = MediaFileUpload(str(path), mimetype=mime, chunksize=CHUNK, resumable=True)
        try:
            request = self._uploads.get(sha)
            if request is None:
                # Public SDK serialization does not restore recovery state.
                # Across process restarts copy afresh; never trust stale progress.
                request = self.service.files().create(body={'name': name, 'parents': [self.folder_id],
                    'appProperties': {'sha256': sha}}, media_body=media, fields=FIELDS)
                self._uploads[sha] = request
            else:
                # Keep SDK recovery state, but no open handles between attempts.
                request.resumable = media
            save_checkpoint(dict(sha256=sha, size=size))
            response = None
            while response is None:
                _, response = request.next_chunk()
            self._uploads.pop(sha)
            obj = self._verified(response['id'], sha, md5, size)
            save_checkpoint(dict(object_id=obj.id, sha256=sha, size=size))
            return obj
        except HttpError as error:
            if error.resp.status in (404, 410):
                self._uploads.pop(sha, None)
            raise
        finally:
            media.stream().close()

    def download(self, object_id, destination, sha256):
        object_id = self._id(object_id)
        destination = Path(destination)
        fd, name = tempfile.mkstemp(prefix='.cold-', dir=destination.parent)
        temporary = Path(name)
        try:
            with os.fdopen(fd, 'w+b') as target:
                self._download_to(object_id, target)
                target.flush()
                os.fsync(target.fileno())
            if _hashes(temporary)[0] != sha256:
                raise ValueError('Cold artifact SHA-256 mismatch')
            os.replace(temporary, destination)
            fd = os.open(destination.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        finally:
            temporary.unlink(missing_ok=True)

    def close(self):
        for request in self._uploads.values():
            request.resumable.stream().close()
        self._uploads.clear()
        self.service.close()
