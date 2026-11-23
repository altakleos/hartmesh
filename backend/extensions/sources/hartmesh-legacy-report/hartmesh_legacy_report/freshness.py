"""A bounded next-turn check for the last report handed over in this chat."""

import hashlib
import json
import os
import re
from dataclasses import dataclass, replace
from pathlib import Path

from deerflow_extension_api.storage import StorageConflict, StorageUnavailable, StorageUnsupported
from langchain.agents.middleware import AgentMiddleware
from langchain_core.messages import AIMessage, ToolMessage

from deerflow.agent_instances.conversations import AGENT_EXECUTION_CONTEXT_KEY
from deerflow.agent_instances.runtime import current_environment
from deerflow.config.app_config import peek_loaded_app_config
from deerflow.config.paths import Paths, get_paths
from deerflow.files.store import normalize_relative_path, open_regular_source
from deerflow.runtime.presented_files import PRESENTED_FILES_KEY
from deerflow.runtime.user_context import get_effective_user_id
from deerflow.sandbox.lease import sandbox_command_scope
from deerflow.sandbox.sandbox_provider import get_initialized_sandbox_provider
from deerflow.spaces.chat_bridge import get_chat_storage_host
from deerflow.spaces.chat_paths import ChatStoragePaths, ThreadMounts
from deerflow.utils.file_io import await_drained, run_file_io
from deerflow.utils.messages import is_real_user_message

_EVIDENCE = "hartmesh_report_sources_v1"
_OUTPUTS = "/mnt/user-data/outputs/"
_AREAS = {"files": "My files", "shared": "Shared"}
_REPORT_LIMIT = 16 * 1024 * 1024
_INPUT_LIMIT = 100 * 1024 * 1024
_MAX_SOURCES = 10
_SKIPPED = (OSError, ValueError, StorageConflict, StorageUnavailable, StorageUnsupported)


def _fingerprint(value):
    return hashlib.sha256(json.dumps(value, separators=(",", ":")).encode()).hexdigest()


def _revision(fd):
    st = os.fstat(fd)
    return st.st_size, _fingerprint((st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns))


@dataclass
class _Mounts:
    paths: Paths
    owner: str
    thread: str
    native: ThreadMounts | None
    local: bool = False

    def scope(self, area):
        if self.native is not None:
            signature = self.paths.routing_signature()
            if not self.native.user or (area == "shared" and not self.native.company):
                return None
            identity = signature if area == "shared" else signature[:2]
        else:
            identity = str(self.paths.base_dir)
        return _fingerprint((self.owner, self.thread, area, identity))

    def root(self, area):
        if area == "files":
            return self.paths.user_files_dir(self.owner)
        if area == "shared":
            return self.paths.shared_dir()
        return self.paths.sandbox_outputs_dir(self.thread, user_id=self.owner)

    def open(self, area, relative):
        target = self.root(area) / relative
        if self.native is None:
            return open_regular_source(target)
        volume = self.native.company if area == "shared" else self.native.user
        with volume.filesystem() as filesystem:
            return filesystem.open_regular(target.relative_to(volume.data_path).as_posix(), follow_symlinks=False)


async def _admit(thread):
    owner = get_effective_user_id()
    paths = get_paths()
    host = get_chat_storage_host()
    if isinstance(paths, ChatStoragePaths):
        if host is None or not paths.has_personal_files:
            return None
        current = await host.prepare_async(owner, thread)
        if current is None or current.mounts.user_id != owner or current.mounts.thread_id != thread or current.paths.routing_signature()[:2] != paths.routing_signature()[:2]:
            return None
        mounts = current.mounts
        if paths.routing_signature()[2] != current.paths.routing_signature()[2]:
            mounts = replace(mounts, company=None)
        return _Mounts(current.paths, owner, thread, mounts)
    config = peek_loaded_app_config()
    if host is not None or getattr(getattr(config, "storage_spaces", None), "enabled", False) is True:
        return None
    provider = get_initialized_sandbox_provider()
    if provider is None or not provider.uses_personal_and_shared_mounts:
        return None
    from deerflow.sandbox.local.local_sandbox_provider import LocalSandboxProvider

    return _Mounts(paths, owner, thread, None, local=isinstance(provider, LocalSandboxProvider))


def _presentation(message):
    if not isinstance(message, (ToolMessage, AIMessage)) or not message.id:
        return None
    paths = message.additional_kwargs.get(PRESENTED_FILES_KEY)
    if not isinstance(paths, list) or len(paths) > 64:
        return None
    for path in paths:
        if not isinstance(path, str) or len(path) > 2048 or not path.startswith(_OUTPUTS + "reports/"):
            continue
        relative = path[len(_OUTPUTS) :]
        if not relative.endswith((".view.json", ".report.json")):
            continue
        try:
            if normalize_relative_path(relative) != relative:
                continue
        except ValueError:
            continue
        member = Path(relative)
        if member.parent.parent.name == "drafts" and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,79}", member.parent.name):
            return member
    return None


class _ReportTooLarge(ValueError):
    pass


def _read(mounts, member, limit):
    with os.fdopen(mounts.open("outputs", member), "rb") as f:
        size, revision = _revision(f.fileno())
        if size > limit:
            raise _ReportTooLarge
        raw = f.read(size + 1)
        if len(raw) != size or _revision(f.fileno())[1] != revision:
            raise ValueError("Report changed during capture")
        return raw


def _live_path(mounts, source, row):
    if not isinstance(source, str) or len(source) > 4096 or not isinstance(row, dict):
        return None
    digest = row.get("sha256")
    if not isinstance(digest, str) or not re.fullmatch(r"[a-f0-9]{64}", digest):
        return None
    sheet = row.get("sheet")
    if "::" in source:
        if not isinstance(sheet, str) or not sheet or not source.endswith("::" + sheet):
            return None
        source = source[: -(len(sheet) + 2)]
    path = Path(source)
    if row.get("name") != path.name or path.suffix.lower() not in (".csv", ".txt", ".tsv", ".xlsx", ".xlsm", ".xls"):
        return None
    for area in _AREAS:
        scope = mounts.scope(area)
        if not scope:
            continue
        # The local sandbox rewrites virtual command arguments to host paths.
        # Match its admitted root lexically; never resolve a report-supplied path.
        prefixes = [f"/mnt/user-data/{area}/"]
        if mounts.local:
            prefixes.append(str(mounts.root(area)) + "/")
        for prefix in prefixes:
            if source.startswith(prefix):
                relative = source[len(prefix) :]
                if relative.isprintable() and normalize_relative_path(relative) == relative:
                    return {"area": area, "path": relative, "digest": digest, "scope": scope}
    return None


def _sources(mounts, member):
    base = member.name.rsplit(".", 2)[0]
    manifest = json.loads(_read(mounts, member.with_name("renders.json"), 64 * 1024))
    if manifest["version"] != 2 or manifest["bundle"] != member.parent.name or manifest["base"] != base or member.name not in manifest["sha256"]:
        return [], False
    canonical = member.with_name(base + ".report.json")
    if member != canonical and hashlib.sha256(_read(mounts, member, 1024 * 1024)).hexdigest() != manifest["sha256"][member.name]:
        return [], False
    # New view handoffs bind the exact view and the compact source metadata to
    # the same canonical digest. Old bundles and source-only handoffs verify
    # the canonical member itself; the row payload is never a live input.
    evidence_name = "input-sources.json"
    fresh_inputs = False
    if member == canonical or evidence_name not in manifest["sha256"]:
        canonical_bytes = _read(mounts, canonical, _REPORT_LIMIT)
        if hashlib.sha256(canonical_bytes).hexdigest() != manifest["sha256"][canonical.name]:
            return [], False
    if evidence_name in manifest["sha256"]:
        raw = _read(mounts, member.with_name(evidence_name), 128 * 1024)
        if hashlib.sha256(raw).hexdigest() != manifest["sha256"][evidence_name]:
            return [], False
        proof = json.loads(raw)
        if proof["version"] != 1 or proof["report_sha256"] != manifest["sha256"][canonical.name]:
            return [], False
        fresh_inputs = proof.get("fresh_inputs") is True
        sources, inputs = proof["sources"], proof["inputs"]
    else:
        report = json.loads(canonical_bytes)
        meta = report["meta"]
        if report["version"] != 1 or manifest["draft"] != meta["draft"]:
            return [], False
        sources, inputs = meta["build"]["sources"], meta["inputs"]
    if not isinstance(sources, list) or not isinstance(inputs, list) or len(inputs) != len(sources):
        return [], False
    rows = []
    for path, row in zip(sources[:_MAX_SOURCES], inputs[:_MAX_SOURCES], strict=True):
        try:
            source = _live_path(mounts, path, row)
        except ValueError:
            continue
        if source and source not in rows:
            rows.append(source)
    return rows, fresh_inputs


def _native_sources(sources, fresh_inputs, messages, index, member):
    # A re-presentation or retained-facts render did not open a live source.
    # Only its existing handoff can identify the source's original Space.
    produced = isinstance(messages[index], ToolMessage) and messages[index].name == "bash" and fresh_inputs
    for previous in reversed(messages[:index]):
        previous_member = _presentation(previous)
        if previous_member is None or previous_member.parent.parent != member.parent.parent or previous_member.name.rsplit(".", 2)[0] != member.name.rsplit(".", 2)[0]:
            continue
        if produced and previous_member.parent != member.parent:
            break
        prior = previous.additional_kwargs.get(_EVIDENCE)
        if not isinstance(prior, list):
            return []
        return [row for row in sources if any(all(old.get(key) == row[key] for key in ("area", "path", "scope", "digest")) for old in prior if isinstance(old, dict))]
    return sources if produced else []


def _compare(mounts, sources):
    budget = _INPUT_LIMIT
    changed, unchecked = [], []
    for source in sources:
        area = source["area"]
        if area not in _AREAS or source["scope"] != mounts.scope(area):
            continue
        name = source["path"]
        try:
            if normalize_relative_path(name) != name:
                continue
            with os.fdopen(mounts.open(area, name), "rb") as f:
                size, revision = _revision(f.fileno())
                if source.get("revision") == revision:
                    if source.get("changed"):
                        changed.append(source)
                    continue
                if size > budget:
                    unchecked.append(source)
                    continue
                budget -= size
                h = hashlib.sha256()
                remaining = size
                while remaining and (chunk := f.read(min(remaining, 1 << 20))):
                    remaining -= len(chunk)
                    h.update(chunk)
                if remaining or _revision(f.fileno())[1] != revision:
                    continue
                with os.fdopen(mounts.open(area, name), "rb") as latest:
                    if _revision(latest.fileno())[1] != revision:
                        continue
                source["revision"] = revision
                source["changed"] = h.hexdigest() != source["digest"]
                if source["changed"]:
                    changed.append(source)
        except _SKIPPED:
            continue
    return changed, unchecked


def _reply(final, notice):
    content = final.content
    if isinstance(content, str):
        if not content.endswith(notice):
            content += "\n\n" + notice
    elif not content or content[-1] != {"type": "text", "text": notice}:
        content = [*content, {"type": "text", "text": notice}]
    return final.model_copy(update={"content": content})


def _quoted_path(source, *, chinese=False):
    # CommonMark code spans keep a filename from adding links or markup.
    areas = {"files": "我的文件", "shared": "共享"} if chinese else _AREAS
    text = areas[source["area"]] + "/" + source["path"]
    fence = "`" * (1 + max((len(match) for match in re.findall(r"`+", text)), default=0))
    return f"{fence} {text} {fence}"


class ReportFreshnessMiddleware(AgentMiddleware):
    def after_agent(self, state, runtime):
        # Synchronous embedded runs have no Gateway file-admission lifecycle.
        return None

    async def aafter_agent(self, state, runtime):
        context = runtime.context or {}
        thread = context.get("thread_id")
        if not thread or context.get("non_interactive") or context.get(AGENT_EXECUTION_CONTEXT_KEY) is not None or current_environment() is not None or sandbox_command_scope(context):
            return None
        messages = state.get("messages", [])[-128:]
        if not messages or not isinstance(final := messages[-1], AIMessage) or not final.id or final.tool_calls:
            return None
        human = next((i for i in range(len(messages) - 1, -1, -1) if is_real_user_message(messages[i])), None)
        if human is None:
            return None
        for i in range(len(messages) - 1, -1, -1):
            tagged = messages[i]
            member = _presentation(tagged)
            if member is not None:
                break
        else:
            return None
        capture = i > human
        evidence = tagged.additional_kwargs.get(_EVIDENCE)
        bounded_handoff = isinstance(evidence, dict) and "oversized" in evidence
        if not capture and not bounded_handoff and (not isinstance(evidence, list) or not evidence or len(evidence) > _MAX_SOURCES):
            return None
        try:
            mounts = await _admit(thread)
            if mounts is None:
                return None
            chinese = context.get("locale") == "zh-CN"
            if capture or bounded_handoff:
                scope = mounts.scope("outputs")
                if bounded_handoff and not capture and evidence.get("oversized") != scope:
                    return None
                try:
                    sources, fresh_inputs = await await_drained(run_file_io(_sources, mounts, member))
                    if mounts.native is not None:
                        sources = _native_sources(sources, fresh_inputs, messages, i, member)
                except _ReportTooLarge:
                    current = await _admit(thread)
                    if current is None or current.scope("outputs") != scope:
                        return None
                    notice = "此报告草稿过大，无法自动核对其来源信息。你可以让我明确检查此草稿。" if chinese else "This draft is too large to verify its source information automatically. Ask me to check the draft explicitly."
                    tagged_update = tagged.model_copy(update={"additional_kwargs": {**tagged.additional_kwargs, _EVIDENCE: {"oversized": scope}}})
                    updates = {tagged.id: tagged_update} if capture else {}
                    updates[final.id] = _reply(updates.get(final.id, final), notice)
                    return {"messages": list(updates.values())}
                if not sources:
                    return None
                mounts = await _admit(thread)
                if mounts is None:
                    return None
            else:
                sources = [dict(row) for row in evidence]
            changed, unchecked = await await_drained(run_file_io(_compare, mounts, sources))
            current = await _admit(thread)
            if current is None:
                return None
            sources = [row for row in sources if row["scope"] == current.scope(row["area"])]
            changed = [row for row in changed if row in sources]
            unchecked = [row for row in unchecked if row in sources]
        except (*_SKIPPED, KeyError, TypeError, RecursionError):
            return None

        updates = {}
        if capture and sources:
            updates[tagged.id] = tagged.model_copy(update={"additional_kwargs": {**tagged.additional_kwargs, _EVIDENCE: sources}})
        notices = []
        if changed:
            names = ", ".join(_quoted_path(row, chinese=chinese) for row in changed)
            notices.append(
                f"报告生成后，以下源文件已更改：{names}。如要使用更新后的数据，请让我重新生成报告。" if chinese else f"Source files changed since this report was built: {names}. To use the updated data, ask me to rebuild the report."
            )
        if unchecked:
            names = ", ".join(_quoted_path(row, chinese=chinese) for row in unchecked)
            notices.append(f"由于本轮的文件大小限制，无法比较 {names}。你可以让我明确检查此草稿。" if chinese else f"I could not compare {names} within this turn’s file-size limit. Ask me to check the draft explicitly.")
        if notices:
            notice = "\n\n".join(notices)
            updates[final.id] = _reply(updates.get(final.id, final), notice)
        return {"messages": list(updates.values())} if updates else None
