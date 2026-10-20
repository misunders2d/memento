"""User-visible conditional guidance must survive the shared injection budget."""

import pytest

from memento import bridge


@pytest.fixture
def spool(tmp_path):
    config = {'uid': 1000, 'client_uid': 'synthetic-device', 'url': 'http://127.0.0.1:1', 'preset': 'codex'}
    path = tmp_path / 'runner.json'
    path.write_bytes(bridge.encode(config))
    result = bridge.Spool(path, config)
    try:
        yield result
    finally:
        result.close()


def suggestion(**changes):
    behavior = {
        'applicability': 'When installing a private service using an available supported installer.',
        'conduct': 'Use the shipped installer instead of asking the human to edit configuration.',
        'exceptions': ['Keep native trust and Google consent explicit.',
                       'Follow a current user request to edit configuration manually.'],
        'rationale': 'Repeated manual editing caused avoidable installation failures.',
        'authority': 'inferred',
        **changes,
    }
    return {'id': 'behavior-id', 'text': 'Always automate every installation step.',
            'category': 'behavioral_suggestion', 'tier': 'confirmed', 'authority': behavior['authority'],
            'behavior': behavior}


def test_full_conditions_and_exceptions_survive_summary_and_hook_provenance(spool, monkeypatch):
    memory = suggestion(rationale='Long supporting rationale. ' * 200)
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory]})
    direct = bridge.passive(spool, 's', 'Connect another native harness.')
    hooked = bridge.hook(spool, 'codex', {'session_id': 's', 'hook_event_name': 'UserPromptSubmit',
                                        'prompt': 'Connect another native harness.'})
    context = hooked['hookSpecificOutput']['additionalContext']
    for rendered in (direct['context'], context):
        assert memory['behavior']['applicability'] in rendered
        assert memory['behavior']['conduct'] in rendered
        assert all(exception in rendered for exception in memory['behavior']['exceptions'])
        assert memory['text'] not in rendered
        assert 'Long supporting rationale.' not in rendered
        assert '[inferred]' in rendered and 'current instructions take priority' in rendered
        assert len(rendered.encode('utf-8')) <= bridge.CONTEXT_BYTES
    assert direct['shown'] == ['behavior-id']


def test_user_authority_candidate_and_rationale_remain_distinct(spool, monkeypatch):
    memory = suggestion(authority='explicit_user')
    memory['tier'] = 'candidate'
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory]})
    result = bridge.passive(spool, 's', 'Install the service.')
    assert '[explicit user guidance]' in result['context']
    assert '[unconfirmed candidate]' in result['context']
    assert memory['behavior']['rationale'] in result['context']
    assert all(exception in result['context'] for exception in memory['behavior']['exceptions'])


def test_a_suggestion_that_cannot_fit_all_exceptions_is_omitted_without_losing_facts(spool, monkeypatch):
    memory = suggestion(exceptions=['Do not automate human approval. ' + '猫' * 500])
    fact = {'id': 'fact-id', 'text': 'The service uses the existing native login.', 'category': 'memory'}
    monkeypatch.setattr(bridge, 'http', lambda *_args, **_kwargs: {'memories': [memory, fact]})
    result = bridge.passive(spool, 's', 'Install the service.')
    assert result['shown'] == ['fact-id']
    assert '[memory:behavior-id]' not in result['context']
    assert memory['behavior']['conduct'] not in result['context']
    assert fact['text'] in result['context']
    assert len(result['context'].encode('utf-8')) <= bridge.CONTEXT_BYTES
