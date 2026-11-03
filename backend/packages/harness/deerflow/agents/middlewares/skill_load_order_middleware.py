"""Hold skill work until the skill's instructions, and its first command, have run.

HartMesh-owned replacement for the skill-ordering half of the former
``SkillToolPolicyMiddleware``. Upstream (#6935) made ``allowed-tools``
compatibility metadata and deleted that middleware; the two ordering rules
below are not about ``allowed-tools`` and stay.
"""

from __future__ import annotations

import asyncio
import logging
import posixpath
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from deerflow.agents.middlewares.message_utils import is_genuine_user_message
from deerflow.agents.middlewares.skill_context import extract_skills, is_skill_file, skill_name_from_path, skill_read_target
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY, stamp_declared_error_meta
from deerflow.config.summarization_config import DEFAULT_SKILL_FILE_READ_TOOL_NAMES
from deerflow.constants import DEFAULT_SKILLS_CONTAINER_PATH
from deerflow.runtime.secret_context import read_slash_skill_source_paths
from deerflow.sandbox.tool_metadata import is_sandbox_tool
from deerflow.skills.first_command import FirstCommand, runs_first_command
from deerflow.skills.storage import get_or_new_skill_storage, get_or_new_user_skill_storage
from deerflow.skills.types import Skill
from deerflow.subagents.status_contract import make_subagent_additional_kwargs

if TYPE_CHECKING:
    from deerflow.config.app_config import AppConfig
    from deerflow.skills.storage.skill_storage import SkillStorage

logger = logging.getLogger(__name__)

_TOOL_SEARCH_NAME = "tool_search"
_TASK_TOOL_NAME = "task"
# Metadata-only discovery returns no work, so it may run beside a skill load.
_DISCOVERY_TOOL_NAMES = frozenset({"describe_skill", _TOOL_SEARCH_NAME})


class SkillLoadOrderMiddleware(AgentMiddleware[AgentState]):
    """Run a skill's work only once its instructions (and first command) can govern it.

    A skill governs from the message that loads it, not the one after. Tool
    calls run in parallel, so a call chosen in the same assistant message as
    a skill's first ``SKILL.md`` read was selected before those instructions
    could reach the model. Such a call is not run; its result says why and
    names the instructions, so the next selection is made with them in hand.
    Reads of skill material and metadata-only discovery in that message run,
    and a skill already read in the conversation (including one summarization
    compacted into ``skill_context``) or slash-activated holds nothing back.

    A skill may also declare the command its work starts with
    (``first-command``). In the turn that first loads such a skill, the
    sandbox runs nothing but that command, and other skills' ``SKILL.md``
    reads, until the command has run or the model has answered; a sandbox
    call chosen before then is not run, and its result shows the command.
    A subagent's chain (``first_command_order=False``) holds no such order:
    it sees only its own messages, not a run the lead has already made.

    It never filters the tool set or ``tool_search`` results: tool authority
    belongs to agent assembly and execution authorization.
    """

    def __init__(
        self,
        *,
        available_skills: set[str] | None = None,
        app_config: AppConfig | None = None,
        user_id: str | None = None,
        slash_source_owner_token: str,
        first_command_order: bool = True,
    ) -> None:
        super().__init__()
        if not isinstance(slash_source_owner_token, str) or not slash_source_owner_token:
            raise ValueError("slash_source_owner_token must be a non-empty string")
        self._available_skills = set(available_skills) if available_skills is not None else None
        self._app_config = app_config
        self._user_id = user_id
        self._slash_source_owner_token = slash_source_owner_token
        self._first_command_order = first_command_order
        # The same definition of "a skill was read" that DurableContextMiddleware
        # captures into skill_context: a configured read tool on a path under
        # the configured skills root.
        if app_config is None:
            self._skills_root = posixpath.normpath(DEFAULT_SKILLS_CONTAINER_PATH)
            self._skill_read_tool_names = frozenset(DEFAULT_SKILL_FILE_READ_TOOL_NAMES)
        else:
            self._skills_root = posixpath.normpath(app_config.skills.container_path or DEFAULT_SKILLS_CONTAINER_PATH)
            self._skill_read_tool_names = frozenset(app_config.summarization.skill_file_read_tool_names)

    def _storage(self) -> SkillStorage:
        if self._user_id is not None:
            return get_or_new_user_skill_storage(self._user_id, app_config=self._app_config)
        if self._app_config is not None:
            return get_or_new_skill_storage(app_config=self._app_config)
        return get_or_new_skill_storage()

    @staticmethod
    def _runtime_context(request: ToolCallRequest) -> dict | None:
        context = getattr(getattr(request, "runtime", None), "context", None)
        return context if isinstance(context, dict) else None

    def _skill_registry(self, request: ToolCallRequest | None) -> dict[str, Skill] | None:
        """Every skill this run may use, by its normalized ``SKILL.md`` container path.

        ``None`` when the registry cannot be read. Blocking: sync handlers or a worker thread only.
        """
        try:
            from deerflow.skills.container_registry import build_container_path_registry

            return build_container_path_registry(self._storage())
        except Exception:
            logger.exception("Failed to load skills for the first-command order")
            return None

    def _skill_read(self, tool_call: Mapping) -> str | None:
        return skill_read_target(dict(tool_call), skills_root=self._skills_root, read_tool_names=self._skill_read_tool_names)

    def _before_instructions(self, tool_call: Mapping) -> bool:
        """Whether *tool_call* acts before a skill loaded beside it can govern it.

        Reads of skill material bring the instructions in, and discovery
        returns metadata and runs nothing; everything else acts.
        """
        return self._skill_read(tool_call) is None and tool_call.get("name") not in _DISCOVERY_TOOL_NAMES

    def _in_hand(self, earlier: list, state: object, context: dict | None) -> set[str]:
        """The ``SKILL.md`` paths whose instructions reached the model before *earlier* ended."""
        in_hand = {entry["path"] for entry in extract_skills(earlier, skills_root=self._skills_root, read_tool_names=self._skill_read_tool_names)}
        # A read that summarization compacted away still governed the calls
        # chosen after it; its skill_context reference is what asks for the re-read.
        captured = _state_value(state, "skill_context")
        in_hand.update(posixpath.normpath(entry["path"]) for entry in captured or () if isinstance(entry, Mapping) and isinstance(entry.get("path"), str))
        in_hand.update(posixpath.normpath(path) for path in read_slash_skill_source_paths(context, owner_token=self._slash_source_owner_token))
        return in_hand

    def _unread_loads(self, tool_calls: list, earlier: list, state: object, context: dict | None) -> list[str]:
        """The skills whose first ``SKILL.md`` read is among *tool_calls*."""
        loads = [path for call in tool_calls if (path := self._skill_read(call)) is not None and is_skill_file(path)]
        if not loads:
            return []
        in_hand = self._in_hand(earlier, state, context)
        return list(dict.fromkeys(path for path in loads if path not in in_hand))

    @staticmethod
    def _locate(request: ToolCallRequest) -> tuple[list, int] | None:
        """The conversation and the index of the assistant message that chose this call."""
        call_id = request.tool_call.get("id")
        messages = _state_value(getattr(request, "state", None), "messages")
        if not call_id or not isinstance(messages, list):
            return None
        for index in range(len(messages) - 1, -1, -1):
            message = messages[index]
            if isinstance(message, AIMessage) and any(call.get("id") == call_id for call in message.tool_calls or []):
                return messages, index
        return None

    def _loaded_beside(self, request: ToolCallRequest) -> tuple[list, list[str]]:
        """The conversation before this call's message, and the skills first loaded in that message."""
        if not self._before_instructions(request.tool_call):
            return [], []
        located = self._locate(request)
        if located is None:
            return [], []
        messages, index = located
        return messages[:index], self._unread_loads(messages[index].tool_calls, messages[:index], getattr(request, "state", None), self._runtime_context(request))

    def _chosen_before_instructions(self, request: ToolCallRequest, earlier: list, unread: list[str], registry: dict[str, Skill] | None) -> ToolMessage:
        """Refuse a call selected alongside a skill's first load; see the class docstring."""
        named = ", ".join(f"the {skill_name_from_path(path)} skill's instructions ({path})" for path in unread)
        name = str(request.tool_call.get("name") or "")
        content = f"Not run: this call was chosen in the same message that loads {named}, before they could shape it. Choose the next step from those instructions once that read has returned them."
        # The order the load starts, when the skill declares a first command
        # that has not already run in this conversation.
        ordered = self._first_command_order and not _state_value(getattr(request, "state", None), "summary_text")
        loads = [(path, len(earlier)) for path in unread] if ordered else []
        orders = [(skill, directory, loaded_at) for skill, directory, loaded_at in self._orders(loads, registry) if not _first_command_settled(earlier, loaded_at, directory, skill.first_command)]
        if orders:
            content = f"{content} {_first_command_order(orders)}"
        message = ToolMessage(content=content, tool_call_id=str(request.tool_call.get("id")), name=name, status="error")
        if name == _TASK_TOOL_NAME:
            # A delegation that never started still needs a final status, or
            # the subtask it announced is shown as running for good.
            message.additional_kwargs = make_subagent_additional_kwargs("failed", error=content)
        return stamp_declared_error_meta(message, "not_run")

    def _awaiting_first_command(self, request: ToolCallRequest) -> tuple[list, int, list[tuple[str, int]]] | None:
        """The skills first loaded in this turn, for a sandbox call that could have to wait.

        Returns the conversation, the index of this call's message and each
        such skill's ``SKILL.md`` path with the index of its read's result, or
        ``None`` when nothing could order this call. Reads no registry, so it
        is cheap on every call.
        """
        if not self._first_command_order or not is_sandbox_tool(getattr(request, "tool", None)):
            return None
        target = self._skill_read(request.tool_call)
        if target is not None and is_skill_file(target):
            # Loading a skill is how its instructions arrive, and never acts on the work.
            return None
        state = getattr(request, "state", None)
        if _state_value(state, "summary_text"):
            # Compaction hides earlier loads and runs of the command, so a
            # read here is not known to be the skill's first.
            return None
        located = self._locate(request)
        if located is None:
            return None
        messages, index = located
        turn_start = next((position + 1 for position in range(index - 1, -1, -1) if is_genuine_user_message(messages[position])), 0)
        loads: dict[str, int] = {}
        for entry in extract_skills(messages[:index], skills_root=self._skills_root, read_tool_names=self._skill_read_tool_names):
            loads.setdefault(entry["path"], entry["loaded_at"])
        in_hand = self._in_hand([], None, self._runtime_context(request))
        pending = [(path, loaded_at) for path, loaded_at in loads.items() if loaded_at >= turn_start and path not in in_hand]
        if not pending:
            return None
        return messages, index, pending

    def _before_first_command(self, request: ToolCallRequest, awaiting: tuple[list, int, list[tuple[str, int]]], registry: dict[str, Skill] | None) -> ToolMessage | None:
        """Refuse sandbox work chosen before the first command of a skill first loaded in this turn.

        A skill's ``first-command`` is what its work starts with, and the
        command itself reads what it needs. Between the skill's first load and
        a run of that command, the sandbox runs nothing else; once it has run,
        whatever its outcome, or the model has answered, everything runs as
        before. A run of any waiting skill's command goes ahead, so two such
        skills cannot hold each other.
        """
        messages, index, pending = awaiting
        orders = self._orders(pending, registry)
        if any(_runs(request.tool_call, directory, skill.first_command) for skill, directory, _loaded_at in orders):
            return None
        waiting = [(skill, directory, loaded_at) for skill, directory, loaded_at in orders if not _first_command_settled(messages[:index], loaded_at, directory, skill.first_command)]
        if not waiting:
            return None
        content = f"Not run: {_first_command_order(waiting)}"
        name = str(request.tool_call.get("name") or "")
        return stamp_declared_error_meta(ToolMessage(content=content, tool_call_id=str(request.tool_call.get("id")), name=name, status="error"), "not_run")

    def _orders(self, loads: list[tuple[str, int]], registry: dict[str, Skill] | None) -> list[tuple[Skill, str, int]]:
        """Each loaded skill that this run may use and that declares a first command, with its directory."""
        if registry is None:
            return []
        orders = []
        for path, loaded_at in loads:
            skill = registry.get(path)
            if skill is not None and skill.first_command is not None and skill.enabled and (self._available_skills is None or skill.name in self._available_skills):
                orders.append((skill, posixpath.dirname(path), loaded_at))
        return orders

    def _held(self, request: ToolCallRequest, registry: Callable[[], dict[str, Skill] | None]) -> ToolMessage | None:
        earlier, unread = self._loaded_beside(request)
        if unread:
            return self._chosen_before_instructions(request, earlier, unread, registry())
        awaiting = self._awaiting_first_command(request)
        if awaiting is None:
            return None
        return self._before_first_command(request, awaiting, registry())

    async def _aheld(self, request: ToolCallRequest) -> ToolMessage | None:
        _earlier, unread = self._loaded_beside(request)
        if not unread and self._awaiting_first_command(request) is None:
            return None
        # The registry scan and the reading of earlier commands stay off the event loop.
        return await asyncio.to_thread(self._held, request, lambda: self._skill_registry(request))

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        refused = self._held(request, lambda: self._skill_registry(request))
        if refused is not None:
            return refused
        return handler(request)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        refused = await self._aheld(request)
        if refused is not None:
            return refused
        return await handler(request)


def _state_value(state: object, key: str) -> object:
    return state.get(key) if isinstance(state, Mapping) else getattr(state, key, None)


def _runs(tool_call: Mapping, directory: str, first_command: FirstCommand) -> bool:
    command = (tool_call.get("args") or {}).get("command") if isinstance(tool_call.get("args"), Mapping) else None
    return isinstance(command, str) and runs_first_command(command, directory=directory, first_command=first_command)


def _first_command_settled(earlier: list, loaded_at: int, directory: str, first_command: FirstCommand) -> bool:
    """Whether the order a skill's first command sets is over, for a call chosen after *earlier*.

    It is over once a run of the command has a result that is not a hold-back
    of its own (a build that failed has run), at any point in the
    conversation: a skill first read now may have been run before, from a
    slash command. It is also over once the model has answered without
    calling anything after the load.
    """
    results = {message.tool_call_id: message for message in earlier if isinstance(message, ToolMessage)}
    for position, message in enumerate(earlier):
        if not isinstance(message, AIMessage):
            continue
        if not message.tool_calls:
            if position > loaded_at:
                return True
            continue
        for call in message.tool_calls:
            result = results.get(call.get("id"))
            if result is not None and _runs(call, directory, first_command) and not _not_run(result):
                return True
    return False


def _not_run(result: ToolMessage) -> bool:
    meta = (result.additional_kwargs or {}).get(TOOL_META_KEY)
    return isinstance(meta, Mapping) and meta.get("error_type") == "not_run"


def _first_command_order(orders: list[tuple[Skill, str, int]]) -> str:
    """What a skill's first command is and how to run it, for a call that did not run before it."""
    starts = []
    for skill, directory, _loaded_at in orders:
        first_command = skill.first_command
        example = " ".join((f'SKILL_DIR="{directory}"; python "${{SKILL_DIR:?}}/{first_command.script}"', *first_command.arguments))
        starts.append(
            f"The {skill.name} skill's work starts with `{first_command}` in its directory, and that command reads what it needs. "
            f"Run it first, the way the skill's instructions show, for example `{example}` followed by the arguments they give."
        )
    return " ".join(starts) + " Until it has run in this turn, nothing else runs in the sandbox. If it cannot run yet because something it needs is missing, ask the person or answer instead."
