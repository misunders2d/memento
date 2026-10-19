import pytest

from memento.text_chunks import chunks


def byte_tokenizer(text, **kwargs):
    return {'offset_mapping': [(i, i + 1) for i, char in enumerate(text) for _ in char.encode('utf-8')]}


def test_document_chunks_preserve_original_unicode_and_whitespace():
    text = '  Первая\t  строка 😀\r\n\n\nвторая\u00a0строка — café!  ' * 20
    passages = chunks(text, byte_tokenizer, size=64, overlap=10)
    covered = set()
    for passage in passages:
        assert passage.text == text[passage.start:passage.end]
        assert 0 <= passage.start < passage.end <= len(text)
        assert passage.end - passage.start <= 64
        assert len(byte_tokenizer(passage.text)['offset_mapping']) <= 64
        covered.update(range(passage.start, passage.end))
    assert all(index in covered for index, char in enumerate(text) if not char.isspace())
    assert any('\t  ' in passage.text for passage in passages)
    assert any('\r\n\n\n' in passage.text for passage in passages)


@pytest.mark.parametrize('text', ['', ' \t\r\n\u00a0'])
def test_whitespace_has_no_document_assertions(text):
    assert chunks(text, byte_tokenizer) == []


def test_invalid_overlap_cannot_hang_document_worker():
    with pytest.raises(ValueError):
        chunks('evidence', byte_tokenizer, size=10, overlap=10)


def test_dense_multibyte_passages_keep_the_tail_within_the_token_budget():
    text = '{"id":"a0123456789abcdef","note":"😀 Кириллица café"}\n' * 200 + 'FINAL EVIDENCE'
    passages = chunks(text, byte_tokenizer)
    covered = set()
    for passage in passages:
        assert passage.text == text[passage.start:passage.end]
        assert len(byte_tokenizer(passage.text)['offset_mapping']) <= 128
        covered.update(range(passage.start, passage.end))
    assert all(i in covered for i, char in enumerate(text) if not char.isspace())
    assert passages[-1].text.endswith('FINAL EVIDENCE')


def test_a_character_larger_than_the_token_budget_fails_instead_of_looping():
    with pytest.raises(ValueError, match='Unicode character'):
        chunks('😀', byte_tokenizer, size=2, overlap=0)
