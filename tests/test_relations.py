"""Open predicates keep source, direction and the existing authority boundaries."""

import pytest
from pydantic import ValidationError

from memento.commands import Link
from memento.graph import Graph
from memento.relations import RELATION_EXAMPLES
from memento.store import Store


TEXT = 'Rhea is the parent of Sol. Sol is the sibling of Nova.'


@pytest.fixture
def family(tmp_path):
    store = Store(tmp_path)
    episode, _ = store.capture(dict(source='synthetic', event_id='family',
        conversation_id='family', kind='user', text=TEXT, occurred_at=None, metadata={}))
    graph = Graph(store)
    evidence = [dict(episode_id=episode['id'], quote=TEXT)]
    nodes = {name: graph.remember(command_id='person-' + name, actor='fixture',
        why='Explicit synthetic person', type='person', text=name, scope=['personal'],
        evidence=evidence)['id'] for name in ('Rhea', 'Sol', 'Nova')}
    yield graph, episode['id'], evidence, nodes
    store.close()


def test_open_predicates_keep_evidence_direction_and_retry_identity(family):
    graph, episode, evidence, nodes = family
    for name, rel, target in [('Rhea', 'parent_of', 'Sol'), ('Sol', 'sibling_of', 'Nova')]:
        command = Link(command_id=rel, actor='fixture', why='Explicit synthetic relation',
                       a=nodes[name], rel=rel, b=nodes[target], evidence=evidence)
        result = graph.link(**command.arguments())
        assert graph.link(**command.arguments()) == result
        row = graph.history(result['id'])[0]
        assert (row['src'], row['rel'], row['dst']) == (nodes[name], rel, nodes[target])
        assert row['evidence'][0]['episode_id'] == episode
        assert row['evidence'][0]['quote'] == TEXT
    assert not graph.pending_reviews()


@pytest.mark.parametrize('rel', ['', 'has sister', 'Parent_of', 'parent-of',
                                 'supersedes', 'a' * 65, 'parent_of\n'])
def test_malformed_or_reserved_predicates_are_rejected_without_mutation(family, rel):
    graph, _, evidence, nodes = family
    arguments = dict(command_id='invalid', actor='fixture', why='Must fail atomically',
                     a=nodes['Rhea'], rel=rel, b=nodes['Sol'], evidence=evidence)
    with pytest.raises(ValidationError):
        Link(**arguments)
    before = graph.store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0]
    with pytest.raises(ValueError):
        graph.link(**arguments)
    assert graph.store._db.execute('SELECT count(*) FROM graph_commits').fetchone()[0] == before


def test_public_relation_schema_offers_examples_without_an_enum():
    schema = Link.model_json_schema()['properties']['rel']
    assert 'enum' not in schema
    assert schema['examples'] == list(RELATION_EXAMPLES)
    assert schema['not'] == {'const': 'supersedes'}
