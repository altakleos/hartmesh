"""Apply skill ``allowed-tools`` only to skills active in lead-agent context."""

from __future__ import annotations

import asyncio
import json
import logging
import posixpath
import secrets
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, override

from langchain.agents import AgentState
from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import ModelCallResult, ModelRequest, ModelResponse
from langchain_core.messages import AIMessage, ToolMessage
from langgraph.prebuilt.tool_node import ToolCallRequest
from langgraph.types import Command

from deerflow.agents.middlewares.message_utils import is_genuine_user_message
from deerflow.agents.middlewares.skill_context import extract_skills, is_skill_file, skill_name_from_path, skill_read_target
from deerflow.agents.middlewares.tool_result_meta import TOOL_META_KEY, stamp_declared_error_meta
from deerflow.config.summarization_config import DEFAULT_SKILL_FILE_READ_TOOL_NAMES
from deerflow.constants import DEFAULT_SKILLS_CONTAINER_PATH
from deerflow.runtime.secret_context import SKILL_TOOL_POLICY_DECISION_CONTEXT_KEY, read_slash_skill_source_paths
from deerflow.sandbox.tool_metadata import is_sandbox_tool
from deerflow.skills.first_command import FirstCommand, runs_first_command
from deerflow.skills.storage import get_or_new_skill_storage, get_or_new_user_skill_storage
from deerflow.skills.tool_policy import ALWAYS_AVAILABLE_BUILTIN_TOOL_NAMES, allowed_tool_names_for_skills
from deerflow.skills.types import Skill
from deerflow.subagents.status_contract import make_subagent_additional_kwargs

if TYPE_CHECKING:
    from deerflow.config.app_config import AppConfig
    from deerflow.skills.storage.skill_storage import SkillStorage

logger = logging.getLogger(__name__)

_POLICY_DECISION_VERSION = 2
_POLICY_SOURCE_PASSIVE = "passive"
_POLICY_SOURCE_SLASH = "slash"
_POLICY_SOURCE_SKILL_CONTEXT = "skill_context"
_POLICY_SOURCES = frozenset({_POLICY_SOURCE_PASSIVE, _POLICY_SOURCE_SLASH, _POLICY_SOURCE_SKILL_CONTEXT})
_MISSING_POLICY_DECISION = object()
# Async-prepass registry load already attempted and failed: the async hook must
# not let the worker-side filter silently retry storage — a successful retry
# would resolve skills whose names are absent from the (empty) decision map and
# fall back to the synchronous provider API from the thread, which for a
# loop-affine provider turns a denial into a fail-open allow. Tri-state marker
# in the style of _MISSING_POLICY_DECISION.
# Registry argument handed down the policy-resolution call chain: a loaded
# snapshot (dict), the load-failure marker, or None ("load it here" — the
# sync-path behavior).
type _RegistryArg = dict[str, Skill] | object | None

_REGISTRY_LOAD_FAILED = object()
_TOOL_SEARCH_NAME = "tool_search"
_TASK_TOOL_NAME = "task"
# Discovery returns catalog metadata and runs nothing, so it cannot act on a
# skill's subject before the skill's instructions arrive.
_DISCOVERY_TOOL_NAMES = frozenset({"describe_skill", _TOOL_SEARCH_NAME})

type _PolicySignature = tuple[str, tuple[str, ...]]


class SkillToolPolicyMiddleware(AgentMiddleware[AgentState]):
    """Restrict agent tools to declarations from slash/in-context skills.

    Merely enabling a skill makes it discoverable; it does not activate its
    authority policy. A skill becomes policy-active when the user slash-activates
    it for the run or after the model loads it into ``skill_context``. Explicit
    slash activation dominates for the rest of that run: passively reading a
    second skill cannot widen the slash skill's authority.

    A skill governs from the message that loads it, not the one after. Tool
    calls run in parallel, so a call chosen in the same assistant message as
    a skill's first ``SKILL.md`` read was selected before those instructions
    could reach the model, and before its ``allowed-tools`` became active.
    Such a call is not run; its result says why and names the instructions,
    so the next selection is made with them in hand. Reads of skill material
    and metadata-only discovery in that message run, and a skill already read
    in the conversation (including one summarization compacted into
    ``skill_context``) or slash-activated holds nothing back.

    A skill may also declare the command its work starts with
    (``first-command``). In the turn that first loads such a skill, the
    sandbox runs nothing but that command, and other skills' ``SKILL.md``
    reads, until the command has run or the model has answered; a sandbox
    call chosen before then is not run, and its result shows the command.
    A subagent's chain (``first_command_order=False``) holds no such order:
    it sees only its own messages, not a run the lead has already made.
    """

    def __init__(
        self,
        *,
        available_skills: set[str] | None = None,
        app_config: AppConfig | None = None,
        user_id: str | None = None,
        slash_source_owner_token: str,
        skill_authorization=None,
        first_command_order: bool = True,
    ) -> None:
        super().__init__()
        if not isinstance(slash_source_owner_token, str) or not slash_source_owner_token:
            raise ValueError("slash_source_owner_token must be a non-empty string")
        self._available_skills = set(available_skills) if available_skills is not None else None
        self._app_config = app_config
        self._user_id = user_id
        self._slash_source_owner_token = slash_source_owner_token
        self._skill_authorization = skill_authorization
        self._first_command_order = first_command_order
        self._decision_owner_token = secrets.token_urlsafe(24)
        # The same definition of "a skill was read" that DurableContextMiddleware
        # captures into skill_context: a configured read tool on a path under
        # the configured skills root.
        if app_config is None:
            self._skills_root = posixpath.normpath(DEFAULT_SKILLS_CONTAINER_PATH)
            self._skill_read_tool_names = frozenset(DEFAULT_SKILL_FILE_READ_TOOL_NAMES)
        else:
            self._skills_root = posixpath.normpath(app_config.skills.container_path or DEFAULT_SKILLS_CONTAINER_PATH)
            self._skill_read_tool_names = frozenset(app_config.summarization.skill_file_read_tool_names)

    def _activation_allowed(self, skill_name: str, *, activation_decisions: dict[str, bool] | None = None) -> bool:
        """Action-scoped ``skill:activate`` decision for one skill name.

        Persisted ``skill_context`` entries were authorized when they were
        stamped, but the policy may have changed since — every entry is
        re-authorized before its allowed-tools declaration is applied.
        *activation_decisions* carries decisions precomputed on the event loop
        via ``aauthorize()`` (async hooks); names absent from the map fall back
        to the synchronous check, which is the correct API for the sync hooks.
        """
        if self._skill_authorization is None:
            return True
        if activation_decisions is not None and skill_name in activation_decisions:
            return activation_decisions[skill_name]
        from deerflow.authz.skill_filter import skill_activation_allowed

        return skill_activation_allowed(self._skill_authorization, skill_name)

    async def _collect_activation_decisions(self, names) -> dict[str, bool] | None:
        """Precompute ``skill:activate`` decisions on the event loop (async hooks).

        The policy resolution itself runs in a worker thread (storage reads),
        but the provider decision belongs on the loop with ``aauthorize()``.
        *names* must already be canonical (``Skill.name``) — the caller
        resolves the policy paths through the live registry off-loop first,
        because the decision consumers (``_active_skills_for_paths``) check
        the registry skill's declared name. Authorizing a path-derived name
        here would miss the map and fall back to the synchronous provider
        call from the worker thread (wrong API for loop-affine providers).
        """
        if self._skill_authorization is None:
            return None
        candidates = {name for name in names if isinstance(name, str) and name}
        if not candidates:
            return None
        from deerflow.authz.skill_filter import skill_activation_allowed_async

        decisions: dict[str, bool] = {}
        for name in sorted(candidates):
            decisions[name] = await skill_activation_allowed_async(self._skill_authorization, name)
        return decisions

    def _resolve_policy_registry(self, paths: tuple[str, ...]) -> tuple[dict[str, Skill] | object, list[str]]:
        """Load the path-keyed registry and canonicalize the policy paths.

        Blocking (skill-tree read): worker thread only. Returns the registry,
        the canonical ``Skill.name`` for every resolvable path, or — when the
        load fails — the ``_REGISTRY_LOAD_FAILED`` marker with no names. The
        marker preserves the failure: the async caller hands it down so
        ``_active_skills_for_paths`` applies its fail-closed treatment instead
        of retrying storage (a transient-failure retry that succeeds would
        resolve skills missing from the decision map and fall back to the
        synchronous provider API). Unresolvable paths yield no name:
        ``_active_skills_for_paths`` skips them before the activation check,
        so no decision is needed for them.
        """
        try:
            from deerflow.skills.container_registry import build_container_path_registry

            registry = build_container_path_registry(self._storage())
        except Exception:
            logger.exception("Failed to load active skills for allowed-tools policy")
            return _REGISTRY_LOAD_FAILED, []
        canonical: list[str] = []
        for path in paths:
            skill = registry.get(posixpath.normpath(path))
            if skill is not None:
                canonical.append(skill.name)
        return registry, canonical

    def _storage(self) -> SkillStorage:
        if self._user_id is not None:
            return get_or_new_user_skill_storage(self._user_id, app_config=self._app_config)
        if self._app_config is not None:
            return get_or_new_skill_storage(app_config=self._app_config)
        return get_or_new_skill_storage()

    @staticmethod
    def _skill_context_entries(request: ModelRequest | ToolCallRequest) -> list:
        """Persisted ``skill_context`` entries from agent state (possibly empty)."""
        state = getattr(request, "state", None)
        if state is None:
            state = {}
        if isinstance(state, Mapping):
            entries = state.get("skill_context") or []
        elif hasattr(state, "skill_context"):
            entries = getattr(state, "skill_context") or []
        else:
            logger.warning("Unsupported agent state shape for skill tool policy: %s", type(state).__name__)
            entries = []
        if not isinstance(entries, (list, tuple)):
            logger.warning("Invalid skill_context shape for skill tool policy: %s", type(entries).__name__)
            entries = []
        return list(entries)

    def _active_policy(self, request: ModelRequest | ToolCallRequest) -> _PolicySignature:
        context = getattr(getattr(request, "runtime", None), "context", None)
        slash_paths = read_slash_skill_source_paths(context, owner_token=self._slash_source_owner_token)
        if slash_paths:
            return _POLICY_SOURCE_SLASH, slash_paths

        paths: list[str] = []
        for entry in self._skill_context_entries(request):
            if isinstance(entry, dict) and isinstance(entry.get("path"), str):
                paths.append(entry["path"])
        if paths:
            return _POLICY_SOURCE_SKILL_CONTEXT, tuple(paths)
        return _POLICY_SOURCE_PASSIVE, ()

    def _active_skills_for_paths(
        self,
        paths: tuple[str, ...],
        *,
        activation_decisions: dict[str, bool] | None = None,
        registry: _RegistryArg = None,
    ) -> tuple[list[Skill], bool]:
        if not paths:
            return [], False

        if registry is _REGISTRY_LOAD_FAILED:
            # The async prepass already attempted the load and failed. Do not
            # retry here: a successful retry would resolve skills absent from
            # the prepass-built decision map and silently fall back to the
            # synchronous provider API from this worker thread. Preserve the
            # failure with the same treatment as a first-load failure below.
            return [], True
        if registry is None:
            try:
                from deerflow.skills.container_registry import build_container_path_registry

                registry = build_container_path_registry(self._storage())
            except Exception:
                logger.exception("Failed to load active skills for allowed-tools policy")
                # A real active reference exists but cannot be authorized. Signal a
                # policy failure so callers retain only framework-safe tools.
                return [], True

        active: list[Skill] = []
        seen: set[str] = set()
        for path in paths:
            skill = registry.get(posixpath.normpath(path))
            if skill is None:
                logger.warning("Active skill path could not be resolved for allowed-tools policy: %s", path)
                continue
            if not skill.enabled:
                logger.warning("Active skill is disabled for allowed-tools policy: %s", path)
                continue
            if self._available_skills is not None and skill.name not in self._available_skills:
                logger.warning("Active skill is outside the agent allowlist for allowed-tools policy: %s", path)
                continue
            # Phase 3: skill_context persists across runs — an entry stamped
            # under an earlier policy must not keep applying its allowed-tools
            # declaration after the provider starts denying activation (same
            # skip-and-continue treatment as a disabled or unallowlisted skill).
            if not self._activation_allowed(skill.name, activation_decisions=activation_decisions):
                logger.info("Active skill is denied by the skill:activate policy for allowed-tools policy: %s", path)
                continue
            if skill.name in seen:
                continue
            seen.add(skill.name)
            active.append(skill)
        if not active:
            logger.warning("No active skill references could be authorized for allowed-tools policy; failing closed")
            return [], True
        return active, False

    def _allowed_names_for_paths(
        self,
        paths: tuple[str, ...],
        *,
        activation_decisions: dict[str, bool] | None = None,
        registry: _RegistryArg = None,
    ) -> set[str] | None:
        active_skills, policy_failed = self._active_skills_for_paths(paths, activation_decisions=activation_decisions, registry=registry)
        if policy_failed:
            return set(ALWAYS_AVAILABLE_BUILTIN_TOOL_NAMES)
        allowed = allowed_tool_names_for_skills(active_skills)
        if allowed is None:
            return None
        return allowed | set(ALWAYS_AVAILABLE_BUILTIN_TOOL_NAMES)

    @staticmethod
    def _runtime_context(request: ModelRequest | ToolCallRequest) -> dict | None:
        context = getattr(getattr(request, "runtime", None), "context", None)
        return context if isinstance(context, dict) else None

    def _store_policy_decision(self, request: ModelRequest, policy: _PolicySignature, allowed: set[str] | None) -> None:
        context = self._runtime_context(request)
        if context is not None:
            source, paths = policy
            context[SKILL_TOOL_POLICY_DECISION_CONTEXT_KEY] = {
                "version": _POLICY_DECISION_VERSION,
                "owner_token": self._decision_owner_token,
                "source": source,
                "active_paths": list(paths),
                "allowed_names": None if allowed is None else sorted(allowed),
            }

    def _read_policy_decision(self, context: dict | None, policy: _PolicySignature) -> set[str] | None | object:
        if context is None:
            return _MISSING_POLICY_DECISION
        decision = context.get(SKILL_TOOL_POLICY_DECISION_CONTEXT_KEY)
        if not isinstance(decision, dict):
            return _MISSING_POLICY_DECISION
        if type(decision.get("version")) is not int or decision["version"] != _POLICY_DECISION_VERSION:
            return _MISSING_POLICY_DECISION
        if not isinstance(decision.get("owner_token"), str) or decision["owner_token"] != self._decision_owner_token:
            return _MISSING_POLICY_DECISION
        source, paths = policy
        stored_source = decision.get("source")
        if not isinstance(stored_source, str) or stored_source not in _POLICY_SOURCES or stored_source != source:
            return _MISSING_POLICY_DECISION
        stored_paths = decision.get("active_paths")
        if not isinstance(stored_paths, list) or not all(isinstance(path, str) for path in stored_paths) or tuple(stored_paths) != paths:
            return _MISSING_POLICY_DECISION
        allowed = decision.get("allowed_names")
        if allowed is None:
            return None
        if not isinstance(allowed, list) or not all(isinstance(name, str) for name in allowed):
            return _MISSING_POLICY_DECISION
        return set(allowed)

    def _allowed_names(
        self,
        request: ModelRequest | ToolCallRequest,
        *,
        policy: _PolicySignature | None = None,
        activation_decisions: dict[str, bool] | None = None,
        registry: _RegistryArg = None,
    ) -> set[str] | None:
        resolved_policy = self._active_policy(request) if policy is None else policy
        _, paths = resolved_policy
        context = self._runtime_context(request)
        decision = self._read_policy_decision(context, resolved_policy)
        if decision is not _MISSING_POLICY_DECISION:
            return decision
        return self._allowed_names_for_paths(paths, activation_decisions=activation_decisions, registry=registry)

    def _filter_model_request(
        self,
        request: ModelRequest,
        *,
        policy: _PolicySignature | None = None,
        refresh_decision: bool = False,
        activation_decisions: dict[str, bool] | None = None,
        registry: _RegistryArg = None,
    ) -> ModelRequest:
        resolved_policy = self._active_policy(request) if policy is None else policy
        _, paths = resolved_policy
        if refresh_decision:
            allowed = self._allowed_names_for_paths(paths, activation_decisions=activation_decisions, registry=registry)
        else:
            allowed = self._allowed_names(request, policy=resolved_policy, activation_decisions=activation_decisions, registry=registry)
        if refresh_decision:
            self._store_policy_decision(request, resolved_policy, allowed)
        if allowed is None:
            return request
        tools = [tool for tool in request.tools if getattr(tool, "name", None) in allowed]
        if len(tools) < len(request.tools):
            logger.debug("Skill policy filtered %d lead tool schema(s)", len(request.tools) - len(tools))
        return request.override(tools=tools)

    def _blocked_tool_message(
        self,
        request: ToolCallRequest,
        *,
        allowed: set[str] | None,
    ) -> ToolMessage | None:
        name = str(request.tool_call.get("name") or "")
        if allowed is None or not name or name in allowed:
            return None
        return ToolMessage(
            content=f"Error: Tool '{name}' is not allowed by the active skill policy.",
            tool_call_id=str(request.tool_call.get("id") or "missing_tool_call_id"),
            name=name,
            status="error",
        )

    def _skill_registry(self, request: ModelRequest | ToolCallRequest | None) -> dict[str, Skill] | None:
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

    @staticmethod
    def _tool_search_policy_error(request: ToolCallRequest) -> ToolMessage:
        return ToolMessage(
            content="Error: tool_search returned a result that could not be validated against the active skill policy.",
            tool_call_id=str(request.tool_call.get("id") or "missing_tool_call_id"),
            name=_TOOL_SEARCH_NAME,
            status="error",
        )

    def _filter_tool_search_result(
        self,
        request: ToolCallRequest,
        result: ToolMessage | Command,
        *,
        allowed: set[str] | None,
    ) -> ToolMessage | Command:
        """Remove denied schemas and promotions from tool_search output.

        Keeping tool_search available is safe only if it cannot return a full
        schema for a tool removed by the active policy. Deferred filtering still
        controls when an allowed schema becomes model-visible; this method keeps
        the discovery result itself within the same authorization boundary.
        """
        name = str(request.tool_call.get("name") or "")
        if name != _TOOL_SEARCH_NAME or allowed is None:
            return result
        if not isinstance(result, Command) or not isinstance(result.update, dict):
            logger.warning("Active-policy tool_search returned an unsupported result shape")
            return self._tool_search_policy_error(request)

        promoted = result.update.get("promoted")
        messages = result.update.get("messages")
        if not isinstance(promoted, dict) or not isinstance(messages, list) or len(messages) != 1:
            logger.warning("Active-policy tool_search command omitted promoted/messages updates")
            return self._tool_search_policy_error(request)
        raw_names = promoted.get("names")
        if not isinstance(raw_names, list) or not all(isinstance(item, str) for item in raw_names):
            logger.warning("Active-policy tool_search returned malformed promoted names")
            return self._tool_search_policy_error(request)

        permitted_names = [item for item in raw_names if item in allowed]
        sanitized_messages: list[ToolMessage] = []
        for message in messages:
            if not isinstance(message, ToolMessage) or message.name != _TOOL_SEARCH_NAME:
                logger.warning("Active-policy tool_search returned an unexpected message shape")
                return self._tool_search_policy_error(request)
            content = message.content
            if raw_names:
                try:
                    schemas = json.loads(content) if isinstance(content, str) else None
                except json.JSONDecodeError:
                    schemas = None
                if not isinstance(schemas, list):
                    logger.warning("Active-policy tool_search returned schemas that could not be filtered")
                    return self._tool_search_policy_error(request)
                filtered_schemas = [schema for schema in schemas if isinstance(schema, dict) and (schema.get("name") in permitted_names or (isinstance(schema.get("function"), dict) and schema["function"].get("name") in permitted_names))]
                content = json.dumps(filtered_schemas, indent=2, ensure_ascii=False) if filtered_schemas else "No tools found matching the active skill policy."
            sanitized_messages.append(message.model_copy(update={"content": content}))

        sanitized_update = dict(result.update)
        sanitized_update["promoted"] = {**promoted, "names": permitted_names}
        sanitized_update["messages"] = sanitized_messages
        return Command(
            graph=result.graph,
            update=sanitized_update,
            resume=result.resume,
            goto=result.goto,
        )

    @override
    def wrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], ModelResponse],
    ) -> ModelCallResult:
        policy = self._active_policy(request)
        return handler(self._filter_model_request(request, policy=policy, refresh_decision=True))

    @override
    async def awrap_model_call(
        self,
        request: ModelRequest,
        handler: Callable[[ModelRequest], Awaitable[ModelResponse]],
    ) -> ModelCallResult:
        policy = self._active_policy(request)
        _, paths = policy
        if not paths:
            self._store_policy_decision(request, policy, None)
            return await handler(request)
        # Three phases, each on the right executor: the registry lookup
        # (skill-tree read) and the policy filter run in a worker thread,
        # while the skill:activate re-authorization decisions are awaited on
        # the event loop (aauthorize) for the *canonical* skill names the
        # registry resolved — keying the map by path-derived names would miss
        # and fall back to the sync provider API from the thread (wrong API
        # for loop-affine providers). The loaded registry is reused by the
        # filter so the whole hook costs one skill-tree scan.
        registry, canonical_names = await asyncio.to_thread(self._resolve_policy_registry, paths)
        activation_decisions = await self._collect_activation_decisions(canonical_names)
        filtered = await asyncio.to_thread(
            self._filter_model_request,
            request,
            policy=policy,
            refresh_decision=True,
            activation_decisions=activation_decisions,
            registry=registry,
        )
        return await handler(filtered)

    @override
    def wrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], ToolMessage | Command],
    ) -> ToolMessage | Command:
        refused = self._held(request, lambda: self._skill_registry(request))
        if refused is not None:
            return refused
        policy = self._active_policy(request)
        if not policy[1]:
            return handler(request)
        allowed = self._allowed_names(request, policy=policy)
        blocked = self._blocked_tool_message(request, allowed=allowed)
        if blocked is not None:
            return blocked
        return self._filter_tool_search_result(request, handler(request), allowed=allowed)

    @override
    async def awrap_tool_call(
        self,
        request: ToolCallRequest,
        handler: Callable[[ToolCallRequest], Awaitable[ToolMessage | Command]],
    ) -> ToolMessage | Command:
        refused = await self._aheld(request)
        if refused is not None:
            return refused
        policy = self._active_policy(request)
        if not policy[1]:
            return await handler(request)
        # Reuse the current model step's cached decision when present — only a
        # cache miss pays the three-phase resolution below (mirrors the sync
        # wrap_tool_call, which reads the same cache inside _allowed_names).
        cached = self._read_policy_decision(self._runtime_context(request), policy)
        if cached is not _MISSING_POLICY_DECISION:
            allowed = cached
        else:
            # Same three-phase shape as awrap_model_call: canonical names
            # resolved off-loop, decisions awaited on the loop, blocking
            # policy resolution in the thread with the registry reused.
            registry, canonical_names = await asyncio.to_thread(self._resolve_policy_registry, policy[1])
            activation_decisions = await self._collect_activation_decisions(canonical_names)
            allowed = await asyncio.to_thread(
                self._allowed_names,
                request,
                policy=policy,
                activation_decisions=activation_decisions,
                registry=registry,
            )
        blocked = self._blocked_tool_message(request, allowed=allowed)
        if blocked is not None:
            return blocked
        return self._filter_tool_search_result(request, await handler(request), allowed=allowed)


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
