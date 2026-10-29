"""MCP shares the HTTP service's sole writer; no approval tool is exposed."""

from typing import Annotated, Literal
import json
from pydantic import AwareDatetime, BeforeValidator
from fastmcp import FastMCP
from fastmcp.tools.base import ToolResult

from .processing import WORK_DESCRIPTION, processing_view
from .commands import Evidence, Link, Loop, MediaInput, Remember, Supersede, Reconcile


def _empty_as_none(value):
    return None if value == '' else value


def _omit_empty_fields(value):
    if isinstance(value, dict):
        return {key: _omit_empty_fields(item) for key, item in value.items() if item != ''}
    if isinstance(value, list):
        return [_omit_empty_fields(item) for item in value]
    return value


# Omission sentinels are a tool-boundary concern; HTTP commands stay strict.
_OptionalDate = Annotated[AwareDatetime | Literal[''] | None, BeforeValidator(_empty_as_none)]
_OptionalString = Annotated[str | None, BeforeValidator(_empty_as_none)]
_OptionalScope = Annotated[Literal['personal', 'professional', ''] | None, BeforeValidator(_empty_as_none)]
_OptionalResponsibility = Annotated[Literal['owner', 'other', 'unknown', ''] | None, BeforeValidator(_empty_as_none)]


def recall_memories(state, query='', effort='normal', limit=10, *, scope=None,
                    since=None, source=None, automatic=False, context=''):
    """Keep ordinary semantic recall separate from bounded raw activity search."""
    if effort not in ('cheap', 'normal', 'deep') or not 1 <= limit <= 100 or scope not in (None, 'personal', 'professional'):
        raise ValueError('Invalid effort, limit or scope')
    if len(query) > 4096 or '\x00' in query:
        raise ValueError('Recall text must be at most 4096 characters without NUL')
    if query.strip() and since is None and source is None:
        if state.retrieval is None:
            raise ValueError('Native embedding runtime is disabled')
        if automatic or context:
            return state.retrieval.recall(query, effort, limit, scope=scope,
                                          automatic=automatic, context=context)
        return state.retrieval.recall(query, effort, limit, scope=scope)
    # Raw evidence has no personal/professional labels. Do not infer a label
    # from an assertion that happens to cite one part of a conversation.
    if scope is not None:
        return []
    return [dict(row, type='raw_episode', tier='raw', state='raw', confirmed=False,
                 text=row['excerpt'], score=0.0, source_url='/episodes/' + row['id'],
                 match='keyword' if query.strip() else 'activity')
            for row in state.store.search(query, limit, source=source, since=since)]


def submit_media(state, command):
    """Share the same checked reference-only save between HTTP and MCP."""
    if state.media is None:
        raise RuntimeError('Native embedding runtime is disabled')
    if state.artifacts.get(command.sha256) is None:
        raise LookupError('Source blob not found')
    return state.jobs.submit_media(**command.arguments())


def create_mcp(state):
    mcp = FastMCP('Memento', instructions=(
        'Routine capture and passive recall belong to the harness. '
        'Use recall(deep) when passive context is insufficient; cite memory ids. '
        'Remember only explicit saves. Every assertion cites an exact source episode quote. '
        'Correct beliefs with one atomic supersede; history explains when, why, and evidence. '
        'Reuse command_id only when retrying the identical command. '
        'Tools cannot approve sensitive changes or forget data.'
    ))

    @mcp.tool
    def recall(query: str = '', effort: _OptionalString = 'normal', limit: int = 10,
               scope: _OptionalScope = None,
               since: _OptionalDate = None, source: _OptionalString = None) -> ToolResult:
        """Recall memories, or omit query for recent activity; since/source filter raw evidence.

        When choosing how to carry out a task and automatic context is insufficient, consult relevant prior experiences.
        """
        memories = recall_memories(state, query, effort or 'normal', limit, scope=scope,
            since=since.isoformat() if since is not None else None, source=source)
        lines = [f"{item['id']} | {item['type']} | "
                 f"{' '.join(item['text'].split())[:240]} | {item['score']:.3f}"
                 for item in memories]
        return ToolResult(content='\n'.join(lines) or 'No matching memories.',
                          structured_content={'memories': memories})

    @mcp.tool
    def remember(command: Annotated[Remember | MediaInput, BeforeValidator(_omit_empty_fields)]) -> dict:
        """Save an exact-quote assertion or queue an uploaded media reference; never send base64 bytes."""
        if isinstance(command, MediaInput):
            return submit_media(state, command)
        return state.graph.remember(**command.arguments())

    @mcp.tool
    def link(command: Annotated[Link, BeforeValidator(_omit_empty_fields)]) -> dict:
        """Add an evidence-backed directional relation from a to b."""
        return state.graph.link(**command.arguments())

    @mcp.tool
    def supersede(command: Annotated[Supersede, BeforeValidator(_omit_empty_fields)]) -> dict:
        """Atomically correct an old assertion with inline replacement and reason."""
        return state.graph.supersede(**command.arguments())

    @mcp.tool
    def reconcile(command: Annotated[Reconcile, BeforeValidator(_omit_empty_fields)]) -> dict:
        """On explicit user request, combine proven duplicates into survivor; absorb at most eight IDs.

        Read history first. Supply every member's current expected_versions and exact original evidence
        for every member. Names or similarity alone do not prove identity; explain sameness in why.
        Preserves evidence, history, relations and loop choices. Pins, sensitive types or different
        scopes return a human review proposal; this tool cannot approve it. Stale versions fail atomically.
        """
        return state.graph.reconcile(**command.arguments())

    @mcp.tool
    def history(id: str) -> list[dict]:
        """Read full supersession lineage, immutable versions, word diffs, reasons and exact evidence."""
        versions = state.graph.history(id)
        if not versions:
            raise ValueError('Node not found')
        return versions

    @mcp.tool
    def loop(action: Literal['list', 'open', 'close', 'later', 'remind', 'assign', 'decline'],
             id: _OptionalString = None, days: float = 7,
             due_at: _OptionalDate = None, due_before: _OptionalDate = None,
             scope: _OptionalScope = None,
             command_id: _OptionalString = None, actor: _OptionalString = None,
             why: _OptionalString = None, limit: int = 20,
             responsibility: _OptionalResponsibility = None,
             evidence: list[Evidence] | None = None) -> dict:
        """List/filter loops or adjust markers. assign labels responsibility; decline retracts an automatic false marker. Evidence-bearing close checks original chronology; only remind sets a reminder."""
        command = Loop(action=action, id=id, days=days, due_at=due_at,
                       due_before=due_before, scope=scope, command_id=command_id,
                       actor=actor, why=why, limit=limit, responsibility=responsibility, evidence=evidence or [])
        arguments = command.arguments()
        if action == 'list':
            arguments.pop('id', None)
            arguments.pop('days', None)
        return state.graph.loop(**arguments)

    @mcp.tool(description=WORK_DESCRIPTION)
    def work(action: Literal['pool', 'claim', 'read', 'renew', 'complete', 'submit', 'release', 'status'],
             command: dict) -> ToolResult:
        """Process explicitly authorized memory work through the shared pool/lease/validation API.

        pool pages the eligible manifest; claim returns bounded evidence and schemas;
        submit accepts {complete: <completion>, next?: <claim>} to commit and continue.
        Renew leases independently during generation; status reconciles uncertain outcomes.
        No raw database access, arbitrary URLs, approval or forgetting is exposed.
        """
        return ToolResult(content=json.dumps(processing_view(state.processing.work(action=action, command=command)),ensure_ascii=False))

    return mcp
