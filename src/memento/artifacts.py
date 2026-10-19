"""Blob location metadata lives in the same database as graph references."""

import re
from datetime import datetime, timezone


class Artifacts:
    def __init__(self, store, blobs):
        self.store = store
        self.blobs = blobs

    def register(self, info, mime='application/octet-stream'):
        mime = mime.split(';', 1)[0].strip().lower()
        if not re.fullmatch(r'[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+', mime):
            raise ValueError('Invalid MIME type')
        self.blobs.path(info['sha256'])
        with self.store._mutex:
            db = self.store._db
            old = db.execute('SELECT * FROM blobs WHERE sha256=?', (info['sha256'],)).fetchone()
            if old is not None:
                if old['size'] != info['size']:
                    raise ValueError('Artifact size differs from its immutable reference')
                return dict(old)
            db.execute('INSERT INTO blobs VALUES (?,?,?,?,?,?)',
                (info['sha256'], info['size'], mime, 'hot', None,
                 datetime.now(timezone.utc).isoformat()))
            return dict(db.execute('SELECT * FROM blobs WHERE sha256=?', (info['sha256'],)).fetchone())

    def get(self, sha256):
        self.blobs.path(sha256)
        with self.store._mutex:
            row = self.store._db.execute('SELECT * FROM blobs WHERE sha256=?', (sha256,)).fetchone()
            return dict(row) if row is not None else None
