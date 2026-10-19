"""Index original artifacts and native media units without caption substitution."""

import io
import json
import mimetypes
import wave
from pathlib import Path
from uuid import uuid4

import numpy as np
from av.error import FFmpegError

from .embedder import TEXT_BATCH, embed_audio, embed_images, embed_texts
from .extract import SkipFile, doc_text, pdf_pages, prepare_image
from .media import iter_audio_windows, keyframes
from .store import ConflictError
from .text_chunks import chunks


class MediaIngest:
    def __init__(self, retrieval, artifacts):
        self.retrieval = retrieval
        self.graph = retrieval.graph
        self.artifacts = artifacts
        self.blobs = artifacts.blobs

    def _save(self, content, mime):
        info = self.blobs.put(content)
        self.artifacts.register(info, mime)
        return info['sha256']

    def _image(self, image):
        content = io.BytesIO()
        image.convert('RGB').save(content, 'JPEG', quality=90)
        content.seek(0)
        sha = self._save(content, 'image/jpeg')
        with self.retrieval._inference:
            vector = embed_images(self.retrieval.model, [image], 70)[0]
        return sha, vector

    def _audio(self, path):
        for start, end, samples in iter_audio_windows(path):
            with self.retrieval._inference:
                vector = embed_audio(self.retrieval.model, [samples])[0]
            content = io.BytesIO()
            with wave.open(content, 'wb') as target:
                target.setparams((1, 2, 16000, 0, 'NONE', 'not compressed'))
                target.writeframes((np.clip(samples, -1, 1) * 32767).astype('<i2').tobytes())
            content.seek(0)
            sha = self._save(content, 'audio/wav')
            yield dict(modality='audio', text='', artifact_sha256=sha,
                       start_s=start, end_s=end, page=None, vector=vector)

    def _text(self, text, sha256, page):
        source = self._save(io.BytesIO(text.encode('utf-8')), 'text/plain')
        with self.retrieval._inference:
            passages = chunks(text, self.retrieval.model.tokenizer)
        for offset in range(0, len(passages), TEXT_BATCH):
            batch = passages[offset:offset + TEXT_BATCH]
            with self.retrieval._inference:
                vectors = embed_texts(self.retrieval.model,
                    ['title: none | text: ' + passage.text for passage in batch])
            for passage, vector in zip(batch, vectors, strict=True):
                yield dict(modality='text', text=passage.text, artifact_sha256=sha256,
                           start_s=None, end_s=None, page=page, vector=vector,
                           char_start=passage.start, char_end=passage.end, source_sha256=source)

    def _prepare(self, path, sha256, kind, filename):
        if kind == 'image':
            image, _, _ = prepare_image(path, 1024, min_side=1)
            with self.retrieval._inference:
                vector = embed_images(self.retrieval.model, [image], 70)[0]
            yield dict(modality='image', text='', artifact_sha256=sha256,
                       start_s=None, end_s=None, page=None, vector=vector)
        elif kind == 'video':
            for start, end, image in keyframes(path):
                artifact, vector = self._image(image)
                yield dict(modality='image', text='', artifact_sha256=artifact,
                           start_s=start, end_s=end, page=None, vector=vector)
            yield from self._audio(path)
        elif kind == 'audio':
            yield from self._audio(path)
        elif kind == 'document':
            if Path(filename).suffix.lower() == '.pdf':
                pages = pdf_pages(path)
                for page in pages.pages:
                    if page.image is not None:
                        artifact, vector = self._image(page.image)
                        yield dict(modality='image', text=page.text, artifact_sha256=artifact,
                                   start_s=None, end_s=None, page=page.number, vector=vector)
                    else:
                        yield from self._text(page.text, sha256, page.number)
            else:
                yield from self._text(doc_text(path, Path(filename).suffix), sha256, None)
        else:
            raise ValueError('Media kind must be image, audio, video or document')

    def ingest(self, *, command_id, actor, why, sha256, kind, filename, evidence, scope, lease=None):
        scope = self.graph._scope('media', scope)
        request = dict(op='media_ingest', sha256=sha256, kind=kind,
                       filename=filename, evidence=evidence, scope=scope)
        serialized = json.dumps(dict(actor=actor, why=why, command=request),
                                sort_keys=True, ensure_ascii=False, allow_nan=False)
        with self.graph.store._mutex:
            prior = self.graph.store._db.execute(
                'SELECT request,result FROM graph_commits WHERE command_id=?', (command_id,)).fetchone()
            if prior:
                if prior['request'] != serialized:
                    raise ConflictError('Command identifier already contains a different command')
                return json.loads(prior['result'])
        # Blob durability precedes every graph/vector reference, including previews.
        path = self.blobs.verify(sha256)
        self.artifacts.register(dict(sha256=sha256, size=path.stat().st_size),
                                mimetypes.guess_type(filename)[0] or 'application/octet-stream')
        try:
            prepared = list(self._prepare(path, sha256, kind, filename))
        except (SkipFile, FFmpegError) as error:
            raise ValueError(f'Artifact cannot be decoded: {error}') from error
        if not prepared:
            raise ValueError('No indexable media units; original artifact remains stored')
        def apply(commit):
            if lease is not None:
                lease.check()
            node = self.graph._insert_node(commit, dict(type='media', text=filename, evidence=evidence, scope=scope))
            db = self.graph.store._db
            db.execute('INSERT INTO media_files VALUES (?,?,?,?,?)',
                       (node['id'], sha256, filename, kind, commit))
            segments = []
            for item in prepared:
                segment_id = uuid4().hex[:16]
                db.execute('''INSERT INTO segments
                    (id,node_id,modality,text,artifact_sha256,start_s,end_s,page,char_start,char_end,source_sha256)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)''',
                    (segment_id, node['id'], item['modality'], item['text'],
                     item['artifact_sha256'], item['start_s'], item['end_s'], item['page'],
                     item.get('char_start'), item.get('char_end'), item.get('source_sha256')))
                self.retrieval._put(segment_id, 'segment', 1, item['vector'])
                segments.append(dict(id=segment_id, start_s=item['start_s'],
                    end_s=item['end_s'], page=item['page'],
                    char_start=item.get('char_start'), char_end=item.get('char_end'),
                    source_text_url=('/blobs/' + item['source_sha256']) if item.get('source_sha256') else None))
            return dict(id=node['id'], blob_url='/blobs/' + sha256, segments=segments)
        return self.graph._command(command_id, actor, why, request, apply)
