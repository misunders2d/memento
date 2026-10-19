"""Wire contracts for explicit evidence-backed graph commands."""

from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

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
    rel: Literal['belongs_to', 'follows_up_on', 'works_in', 'decided_by',
                 'mentions', 'part_of', 'caused_by', 'owned_by', 'related_to']
    b: str = Field(min_length=1, max_length=64)


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


class MediaInput(Command):
    sha256: str = Field(pattern=r'^[0-9a-f]{64}$')
    kind: Literal['image', 'audio', 'video', 'document']
    filename: str = Field(min_length=1, max_length=256)
    scope: Scope


class Loop(BaseModel):
    model_config = ConfigDict(extra='forbid', strict=True, allow_inf_nan=False)
    action: Literal['list', 'open', 'close', 'later', 'remind']
    id: str | None = Field(default=None, min_length=1, max_length=64)
    days: float = Field(default=7, gt=0)
    due_at: Annotated[AwareDatetime, Field(strict=False)] | None = None
    due_before: Annotated[AwareDatetime, Field(strict=False)] | None = None
    scope: Literal['personal', 'professional'] | None = None
    limit: int = Field(default=20, ge=1, le=100)
    command_id: str | None = Field(default=None, min_length=1, max_length=512)
    actor: str | None = Field(default=None, min_length=1, max_length=256)
    why: str | None = Field(default=None, min_length=1)

    def arguments(self):
        return self.model_dump(mode='json', exclude_none=True)
