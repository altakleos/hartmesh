"""Copy files from explicit conversation references through the interactive host."""

import json
import unicodedata
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Annotated, Any, Literal, Self

from langchain.tools import tool
from langchain_core.messages import ToolMessage
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.constants import CONVERSATION_READER_CONTEXT_KEY
from deerflow.files.store import StoreError, normalize_relative_path
from deerflow.tools.types import Runtime
from deerflow.uploads.manager import normalize_filename, upload_virtual_path
from deerflow.utils.thread_id import ThreadId, validate_thread_id

IMPORTED_FILES_KEY = "imported_files"
_IMPORT = "import_conversation_file"


def file_tool_json(value: dict) -> str:
    """Keep source text literal and separate from model-facing context tags."""
    return json.dumps(value, ensure_ascii=False).replace("<", "\\u003c")


def _safe_leaf(name: str) -> bool:
    return not any(unicodedata.category(char) == "Cc" for char in name) and normalize_filename(name) == name


class ImportReceipt(BaseModel):
    """Server-observed result of one completed copy to personal chat uploads."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    kind: Literal["conversation"]
    source_title: str | None
    filename: str
    size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    virtual_path: str

    @model_validator(mode="after")
    def exact_upload(self) -> Self:
        if not _safe_leaf(self.filename) or self.virtual_path != upload_virtual_path(self.filename):
            raise ValueError("Receipt must identify the saved upload")
        return self


def imported_files_of(message: ToolMessage | Mapping) -> list[ImportReceipt]:
    """Read a supported successful import receipt, including from a stored message."""
    if isinstance(message, ToolMessage):
        message = message.model_dump(include={"type", "name", "status", "additional_kwargs"})
    if not isinstance(message, Mapping) or message.get("type") != "tool" or message.get("name") != _IMPORT or message.get("status", "success") != "success":
        return []
    kwargs = message.get("additional_kwargs")
    raw = kwargs.get(IMPORTED_FILES_KEY) if isinstance(kwargs, Mapping) else None
    if not isinstance(raw, list) or len(raw) != 1:
        return []
    try:
        return [ImportReceipt.model_validate(raw[0])]
    except ValidationError:
        return []


@dataclass(frozen=True)
class ConversationFileAccess:
    """Interactive host capabilities, bound to one destination and its references."""

    read: Callable[..., Awaitable[str]]
    list_files: Callable[..., Awaitable[dict | None]]
    import_file: Callable[..., Awaitable[ImportReceipt | None]]

    async def __call__(self, **page: Any) -> str:
        return await self.read(**page)


def conversation_file_access(context) -> ConversationFileAccess | None:
    if not isinstance(context, dict) or context.get("is_subagent") or context.get(AGENT_EXECUTION_CONTEXT_KEY) is not None:
        return None
    access = context.get(CONVERSATION_READER_CONTEXT_KEY)
    return access if isinstance(access, ConversationFileAccess) else None


_UNAVAILABLE = json.dumps({"error": "That file isn't available from here."})


@tool(parse_docstring=True)
async def list_conversation_files(
    thread_id: ThreadId,
    runtime: Runtime,
    before_created_at: Annotated[str, Field(max_length=64)] | None = None,
    before_run_id: Annotated[str, Field(min_length=1, max_length=256)] | None = None,
    offset: Annotated[int, Field(ge=0, strict=True)] = 0,
) -> str:
    """List files presented by answers in a conversation explicitly referenced for this turn.

    Use each available file's run_id and path with import_conversation_file to copy it.
    When has_more is true, pass next_before_created_at, next_before_run_id and next_offset back as the cursor, even for an empty page.

    Args:
        thread_id: ID of a conversation referenced for this turn.
        runtime: Injected host access for this turn.
        before_created_at: Timestamp cursor from the preceding page.
        before_run_id: Run ID cursor from the same page.
        offset: File offset within that run from the same page (zero for a new search).
    """
    access = conversation_file_access(runtime.context if runtime else None)
    if access is None:
        return _UNAVAILABLE
    try:
        validate_thread_id(thread_id)
    except ValueError:
        return _UNAVAILABLE
    if (before_created_at is None) != (before_run_id is None) or before_created_at == "" or before_run_id == "" or type(offset) is not int or offset < 0 or (offset and before_run_id is None):
        return json.dumps({"error": "Pass both cursor fields from the same page, or omit both."})
    result = await access.list_files(thread_id=thread_id, before_created_at=before_created_at, before_run_id=before_run_id, offset=offset)
    return file_tool_json(result) if result is not None else _UNAVAILABLE


@tool(parse_docstring=True)
async def import_conversation_file(
    thread_id: ThreadId,
    run_id: Annotated[str, Field(min_length=1, max_length=256)],
    path: Annotated[str, Field(min_length=1, max_length=4096)],
    runtime: Runtime,
) -> ToolMessage | str:
    """Copy a presented file from a referenced conversation into this chat's attachments.

    Use list_conversation_files to find the source. The returned virtual_path names the
    actual copy, including a changed filename when an attachment already occupies the name.

    Args:
        thread_id: ID of a conversation referenced for this turn.
        run_id: Source answer's run ID from list_conversation_files.
        path: Exact presented output path from that answer.
        runtime: Injected host access for this turn.
    """
    access = conversation_file_access(runtime.context if runtime else None)
    if access is None or not runtime.tool_call_id or not 1 <= len(run_id) <= 256 or len(path) > 4096:
        return _UNAVAILABLE
    try:
        validate_thread_id(thread_id)
        prefix = "/mnt/user-data/outputs/"
        relative = path.removeprefix(prefix)
        if not path.startswith(prefix) or normalize_relative_path(relative) != relative or not _safe_leaf(relative.rsplit("/", 1)[-1]):
            return _UNAVAILABLE
    except (ValueError, StoreError):
        return _UNAVAILABLE
    receipt = await access.import_file(thread_id=thread_id, run_id=run_id, path=path)
    if not isinstance(receipt, ImportReceipt):
        return _UNAVAILABLE
    result = {IMPORTED_FILES_KEY: [receipt.model_dump()]}
    return ToolMessage(content=file_tool_json(result), name=_IMPORT, tool_call_id=runtime.tool_call_id, additional_kwargs=result)
