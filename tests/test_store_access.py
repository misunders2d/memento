import os

import pytest

from memento.store import Store


@pytest.mark.parametrize('mode', [0o750, 0o755, 0o770, 0o777])
def test_shared_data_directory_is_refused(tmp_path, mode):
    tmp_path.chmod(mode)
    with pytest.raises(RuntimeError, match='private'):
        Store(tmp_path)
    assert not (tmp_path / 'memory.sqlite').exists()


def test_service_cannot_share_configured_agent_identity(tmp_path, monkeypatch):
    monkeypatch.setenv('MEMENTO_AGENT_UID', str(os.getuid()))
    with pytest.raises(RuntimeError, match='distinct'):
        Store(tmp_path)
    assert not (tmp_path / 'memory.sqlite').exists()
