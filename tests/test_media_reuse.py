"""Complete native media vectors can be reused without weakening scope or provenance."""

import io
import json
from types import SimpleNamespace

import numpy as np
import pytest

from memento.graph import Graph
from memento.ingest import MediaIngest
from memento.retrieval import Retrieval
from memento.store import ConflictError, Store


@pytest.fixture
def media(tmp_path, monkeypatch):
    store = Store(tmp_path)
    retrieval = Retrieval(Graph(store), model=SimpleNamespace(tokenizer=lambda text, **kw: {}))
    result = MediaIngest(retrieval, store.artifacts)
    result.prepared_calls = []

    def prepare(path, sha256, kind, filename):
        result.prepared_calls.append((sha256, kind, filename))
        for page in (1, 2):
            vector = np.zeros(768, np.float32)
            vector[page - 1] = 1
            yield dict(modality='image', text='', artifact_sha256=sha256,
                       start_s=None, end_s=None, page=page, vector=vector)

    monkeypatch.setattr(result, '_prepare', prepare)
    yield result
    store.close()


def source(media, event='source'):
    store = media.graph.store
    info = store.blobs.put(io.BytesIO(b'original-media-bytes'))
    store.artifacts.register(info, 'image/png')
    sha256 = info['sha256']
    text = 'Captured attachment ' + sha256
    episode, _ = store.capture(dict(source='native:fixture:pi', event_id=event,
        conversation_id=event, kind='user', text=text, occurred_at=None, metadata={}))
    db = store._db
    db.execute('''INSERT OR IGNORE INTO captured_media
        (sha256,kind,filename,state) VALUES (?,'image','attachment.png','ready')''', (sha256,))
    db.execute('INSERT OR IGNORE INTO captured_media_refs VALUES (?,?,?,?,?)',
               (sha256, episode['id'], text.index(sha256), len(text), sha256))
    return sha256, dict(episode_id=episode['id'], quote=sha256)


def save(media, sha256, evidence, command_id, *, captured=False, **changes):
    command = dict(command_id=command_id, actor='fixture', why='Save the source artifact',
        sha256=sha256, kind='image', filename='attachment.png',
        evidence=[] if captured else [evidence], scope=[] if captured else ['professional'],
        captured=captured)
    command.update(changes)
    return media.ingest(**command)


def vectors(media, node_id):
    return [row['embedding'] for row in media.graph.store._db.execute('''
        SELECT v.embedding FROM segments s JOIN vector_items i ON i.target=s.id AND i.kind='segment'
        JOIN memory_vectors v ON v.rowid=i.seq WHERE s.node_id=? ORDER BY s.page''', (node_id,))]


@pytest.mark.parametrize('captured_first', [True, False])
def test_complete_media_reuse_preserves_independent_scope_and_command_identity(media, captured_first):
    sha256, evidence = source(media)
    first = save(media, sha256, evidence, 'first', captured=captured_first)
    second = save(media, sha256, evidence, 'second', captured=not captured_first, filename='renamed.PNG')
    assert first['id'] != second['id']
    assert len(media.prepared_calls) == 1
    assert vectors(media, first['id']) == vectors(media, second['id'])
    assert len(vectors(media, first['id'])) == 2
    db = media.graph.store._db
    for result, captured in ((first, captured_first), (second, not captured_first)):
        node = media.graph._node(result['id'])
        assert json.loads(node['scope']) == ([] if captured else ['professional'])
        assert db.execute('SELECT count(*) FROM graph_evidence WHERE target=?', (result['id'],)).fetchone()[0] == 1
        assert db.execute('SELECT identity FROM media_representations WHERE node_id=?', (result['id'],)).fetchone()
    captured_node = first['id'] if captured_first else second['id']
    assert dict(db.execute('SELECT node_id,state FROM captured_media WHERE sha256=?', (sha256,)).fetchone()) == {
        'node_id': captured_node, 'state': 'indexed'}
    assert save(media, sha256, evidence, 'first', captured=captured_first) == first
    with pytest.raises(ConflictError, match='different command'):
        save(media, sha256, evidence, 'first', captured=captured_first, filename='different.png')
    assert len(media.prepared_calls) == 1


def test_complete_legacy_media_is_reused_without_reencoding(media):
    sha256, evidence = source(media)
    first = save(media, sha256, evidence, 'legacy')
    db = media.graph.store._db
    db.execute('DELETE FROM media_representations WHERE node_id=?', (first['id'],))
    second = save(media, sha256, evidence, 'captured', captured=True)
    assert len(media.prepared_calls) == 1
    assert vectors(media, first['id']) == vectors(media, second['id'])
    assert db.execute('SELECT count(*) FROM media_representations').fetchone()[0] == 2


def test_future_decoder_does_not_assume_markerless_media_is_compatible(media, monkeypatch):
    sha256, evidence = source(media)
    first = save(media, sha256, evidence, 'legacy')
    media.graph.store._db.execute('DELETE FROM media_representations WHERE node_id=?', (first['id'],))
    monkeypatch.setattr('memento.ingest.MEDIA_DECODER_VERSION', 2)
    save(media, sha256, evidence, 'new-decoder', captured=True)
    assert len(media.prepared_calls) == 2


@pytest.mark.parametrize('invalidate', ['missing', 'inactive', 'identity'])
def test_incomplete_or_incompatible_representation_is_not_reused(media, invalidate):
    sha256, evidence = source(media)
    first = save(media, sha256, evidence, 'first')
    db = media.graph.store._db
    vector = db.execute('''SELECT i.seq FROM segments s JOIN vector_items i ON i.target=s.id
        WHERE s.node_id=? LIMIT 1''', (first['id'],)).fetchone()[0]
    if invalidate == 'missing':
        db.execute('DELETE FROM memory_vectors WHERE rowid=?', (vector,))
    elif invalidate == 'inactive':
        db.execute('UPDATE memory_vectors SET current=0 WHERE rowid=?', (vector,))
    else:
        db.execute("UPDATE media_representations SET identity='other-decoder' WHERE node_id=?", (first['id'],))
    second = save(media, sha256, evidence, 'captured', captured=True)
    assert len(media.prepared_calls) == 2
    assert len(vectors(media, second['id'])) == 2


def test_document_decoder_suffix_is_part_of_compatibility(media):
    sha256, evidence = source(media)
    save(media, sha256, evidence, 'pdf', kind='document', filename='source.pdf')
    save(media, sha256, evidence, 'txt', kind='document', filename='source.txt')
    assert len(media.prepared_calls) == 2


def test_public_empty_scope_cannot_enter_the_internal_capture_path(media):
    sha256, evidence = source(media)
    with pytest.raises(ValueError, match='scope requires'):
        save(media, sha256, evidence, 'public', scope=[])
    assert media.prepared_calls == []
    result = save(media, sha256, evidence, 'captured', captured=True)
    assert json.loads(media.graph._node(result['id'])['scope']) == []


@pytest.mark.parametrize('stage', ['before', 'during'])
def test_captured_source_is_checked_before_encoding_and_again_at_commit(media, monkeypatch, stage):
    sha256, evidence = source(media)
    db = media.graph.store._db

    def exclude():
        db.execute("INSERT INTO forgotten_sources VALUES ('native:fixture:pi','source','fixture','fixture')")

    if stage == 'before':
        exclude()
    else:
        prepare = media._prepare

        def raced(*args):
            prepared = list(prepare(*args))
            exclude()
            return iter(prepared)

        monkeypatch.setattr(media, '_prepare', raced)
    with pytest.raises(ConflictError, match='active source'):
        save(media, sha256, evidence, 'captured', captured=True)
    assert len(media.prepared_calls) == (0 if stage == 'before' else 1)
    for table in ('media_files', 'segments', 'media_representations', 'graph_commits'):
        assert db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0
    assert db.execute('SELECT node_id FROM captured_media WHERE sha256=?', (sha256,)).fetchone()[0] is None


def test_forgotten_captured_target_is_not_recreated_from_another_occurrence(media):
    sha256, evidence = source(media)
    first = save(media, sha256, evidence, 'first', captured=True)
    preview = media.graph._forget_preview(first['id'])
    media.graph._forget(command_id='forget', actor='human', why='Exclude this media',
                        target=first['id'], token=preview['token'])
    _, newer = source(media, 'newer')
    with pytest.raises(ConflictError, match='active source'):
        save(media, sha256, newer, 'later', captured=True)
    assert len(media.prepared_calls) == 1
    assert media.graph.store._db.execute('SELECT count(*) FROM media_files').fetchone()[0] == 1
    # A retry returns its old receipt without publishing another target.
    assert save(media, sha256, evidence, 'first', captured=True) == first


def test_failed_preparation_does_not_publish_a_completion_marker(media, monkeypatch):
    sha256, evidence = source(media)

    def failed(*args):
        raise ValueError('No decodable units')

    monkeypatch.setattr(media, '_prepare', failed)
    with pytest.raises(ValueError, match='No decodable'):
        save(media, sha256, evidence, 'failure')
    db = media.graph.store._db
    assert db.execute('SELECT count(*) FROM media_representations').fetchone()[0] == 0
    assert db.execute('SELECT count(*) FROM media_files').fetchone()[0] == 0
