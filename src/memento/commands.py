"""Wire contracts for explicit evidence-backed graph commands."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator

from .relations import RELATION_EXAMPLES, RELATION_PATTERN, valid_relation

Scope = Annotated[list[Literal['personal', 'professional']], Field(min_length=1, max_length=2)]


class Evidence(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    episode_id: str | None = Field(default=None, min_length=1, max_length=64)
    quote: str = Field(min_length=1)
    occurrence: int | None = Field(default=None, ge=0)


class Command(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    command_id: str = Field(min_length=1, max_length=512)
    actor: str = Field(min_length=1, max_length=256)
    why: str = Field(min_length=1)
    evidence: list[Evidence] = Field(min_length=1, max_length=100)
    source: str | None = Field(default=None, min_length=1, max_length=256)
    conversation_id: str | None = Field(default=None, min_length=1, max_length=512)

    def arguments(self):
        return self.model_dump(mode='json', exclude_none=True)


class Remember(Command):
    type: str = Field(pattern=r'^[a-z][a-z0-9_]{0,63}$')
    text: str = Field(min_length=1)
    scope: Scope
    valid_from: Annotated[AwareDatetime, Field(strict=False)] | None = None
    valid_to: Annotated[AwareDatetime, Field(strict=False)] | None = None


class Link(Command):
    a: str = Field(min_length=1, max_length=64)
    rel: str = Field(pattern='^' + RELATION_PATTERN + '$',
        description='Evidence-backed directional predicate in lowercase snake_case. '
                    'Examples are suggestions, not an allowlist; supersedes is reserved for atomic supersession.',
        json_schema_extra={'examples': list(RELATION_EXAMPLES), 'not': {'const': 'supersedes'}})
    b: str = Field(min_length=1, max_length=64)

    @field_validator('rel')
    @classmethod
    def relation_identifier(cls, value):
        if not valid_relation(value):
            raise ValueError('Invalid directional relation')
        return value


class Replacement(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True)
    type: str = Field(min_length=1, max_length=64)
    text: str = Field(min_length=1)
    scope: Scope


class Supersede(Command):
    old: str = Field(min_length=1, max_length=64)
    new: Replacement
    kind: Literal['correction', 'change', 'refinement'] = 'correction'
    valid_at: Annotated[AwareDatetime, Field(strict=False)] | None = None


class Reconcile(Command):
    survivor: str = Field(min_length=1, max_length=64)
    absorbed: list[Annotated[str, Field(min_length=1, max_length=64)]] = Field(min_length=1, max_length=8)
    expected_versions: dict[Annotated[str, Field(min_length=1, max_length=64)],
                            Annotated[int, Field(ge=1)]] = Field(min_length=2, max_length=9)


class MediaInput(Command):
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    kind: Literal['image', 'audio', 'video', 'document']
    filename: str = Field(min_length=1, max_length=256)
    scope: Scope


class Loop(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)
    action: Literal['list', 'open', 'close', 'later', 'remind', 'assign', 'decline']
    id: str | None = Field(default=None, min_length=1, max_length=64)
    days: float = Field(default=7, gt=0)
    due_at: Annotated[AwareDatetime, Field(strict=False)] | None = None
    due_before: Annotated[AwareDatetime, Field(strict=False)] | None = None
    scope: Literal['personal', 'professional'] | None = None
    limit: int = Field(default=20, ge=1, le=100)
    command_id: str | None = Field(default=None, min_length=1, max_length=512)
    actor: str | None = Field(default=None, min_length=1, max_length=256)
    why: str | None = Field(default=None, min_length=1)
    responsibility: Literal['owner', 'other', 'unknown'] | None = None
    evidence: list[Evidence] = Field(default_factory=list, max_length=8)

    def arguments(self):
        return self.model_dump(mode='json', exclude_none=True)
