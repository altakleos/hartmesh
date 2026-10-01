from pathlib import Path
from typing import Annotated

from langchain.tools import InjectedToolCallId, tool
from langchain_core.messages import ToolMessage
from langgraph.config import get_config
from langgraph.types import Command

from deerflow.config.paths import VIRTUAL_PATH_PREFIX, get_paths
from deerflow.runtime.presented_files import PRESENTED_FILES_KEY
from deerflow.runtime.user_context import resolve_runtime_user_id
from deerflow.tools.types import Runtime

OUTPUTS_VIRTUAL_PREFIX = f"{VIRTUAL_PATH_PREFIX}/outputs"


def _get_thread_id(runtime: Runtime) -> str | None:
    """Resolve the current thread id from runtime context or RunnableConfig."""
    thread_id = runtime.context.get("thread_id") if runtime.context else None
    if thread_id:
        return thread_id

    runtime_config = getattr(runtime, "config", None) or {}
    thread_id = runtime_config.get("configurable", {}).get("thread_id")
    if thread_id:
        return thread_id

    try:
        return get_config().get("configurable", {}).get("thread_id")
    except RuntimeError:
        return None


def resolve_presented_filepath(
    runtime: Runtime,
    filepath: str,
) -> tuple[str, Path]:
    """Resolve a presented file path to its virtual form and its host path.

    Accepts either:
    - A virtual sandbox path such as `/mnt/user-data/outputs/report.md`
    - A host-side thread outputs path such as
      `/app/backend/.deer-flow/threads/<thread>/user-data/outputs/report.md`

    Returns:
        The normalized virtual path and the resolved host path it names.

    Raises:
        ValueError: If runtime metadata is missing or the path is outside the
            current thread's outputs directory.
    """
    if runtime.state is None:
        raise ValueError("Thread runtime state is not available")

    thread_id = _get_thread_id(runtime)
    if not thread_id:
        raise ValueError("Thread ID is not available in runtime context or runtime config")

    thread_data = runtime.state.get("thread_data") or {}
    outputs_path = thread_data.get("outputs_path")
    if not outputs_path:
        raise ValueError("Thread outputs path is not available in runtime state")

    outputs_dir = Path(outputs_path).resolve()
    stripped = filepath.lstrip("/")
    virtual_prefix = VIRTUAL_PATH_PREFIX.lstrip("/")

    if stripped == virtual_prefix or stripped.startswith(virtual_prefix + "/"):
        try:
            actual_path = get_paths().resolve_virtual_path(thread_id, filepath, user_id=resolve_runtime_user_id(runtime))
        except TypeError:
            actual_path = get_paths().resolve_virtual_path(thread_id, filepath)
    else:
        actual_path = Path(filepath).expanduser().resolve()

    try:
        relative_path = actual_path.relative_to(outputs_dir)
    except ValueError as exc:
        raise ValueError(f"Only files in {OUTPUTS_VIRTUAL_PREFIX} can be presented: {filepath}") from exc

    return f"{OUTPUTS_VIRTUAL_PREFIX}/{relative_path.as_posix()}", actual_path


def _normalize_presented_filepath(
    runtime: Runtime,
    filepath: str,
) -> str:
    """Normalize a presented file path to the `/mnt/user-data/outputs/*` contract."""
    virtual_path, _actual_path = resolve_presented_filepath(runtime, filepath)
    return virtual_path


@tool("present_files", parse_docstring=True)
def present_file_tool(
    runtime: Runtime,
    filepaths: list[str],
    tool_call_id: Annotated[str, InjectedToolCallId],
) -> Command:
    """Make files visible to the user for viewing and rendering in the client interface.

    When to use the present_files tool:

    - Making any file available for the user to view, download, or interact with
    - Presenting multiple related files at once
    - After creating files that should be presented to the user, when the call that created them did not already present them

    When NOT to use the present_files tool:
    - When you only need to read file contents for your own processing
    - For temporary or intermediate files not meant for user viewing
    - For files a tool result already named under "Presented to the user": they are delivered, and calling this tool would attach them a second time

    Notes:
    - Call this tool after creating files and moving them to the `/mnt/user-data/outputs` directory, unless the call that wrote them already presented them (the `bash` tool's `present` argument does this in the same call).
    - This tool can be safely called in parallel with other tools. State updates are handled by a reducer to prevent conflicts.

    Args:
        filepaths: List of absolute file paths to present to the user. **Only** files in `/mnt/user-data/outputs` can be presented.
    """
    try:
        normalized_paths = [_normalize_presented_filepath(runtime, filepath) for filepath in filepaths]
    except ValueError as exc:
        return Command(
            update={"messages": [ToolMessage(f"Error: {exc}", tool_call_id=tool_call_id)]},
        )

    # The merge_artifacts reducer will handle merging and deduplication. The
    # tag on the message is what the delivery journal, the IM channels and the
    # browser read as "this result presented these files".
    return Command(
        update={
            "artifacts": normalized_paths,
            "messages": [
                ToolMessage(
                    "Successfully presented files",
                    tool_call_id=tool_call_id,
                    additional_kwargs={PRESENTED_FILES_KEY: list(dict.fromkeys(normalized_paths))},
                )
            ],
        },
    )
