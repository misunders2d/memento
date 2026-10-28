"""Open predicate vocabulary; examples are guidance, never an allowlist."""

import re

RELATION_PATTERN = r'[a-z][a-z0-9_]{0,63}'
RELATION_EXAMPLES = ('belongs_to', 'follows_up_on', 'works_in', 'decided_by',
                     'mentions', 'part_of', 'caused_by', 'owned_by', 'related_to')


def valid_relation(value):
    """Retain identifier bounds and the atomic-supersession authority boundary."""
    return (isinstance(value, str) and value != 'supersedes'
            and re.fullmatch(RELATION_PATTERN, value) is not None)
