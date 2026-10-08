"""Capture approved public references; private/integration packages never fall back."""

import base64
import hashlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass

from deerflow.agent_instances.contract import AgentDenied
from deerflow.skills.review.models import PackageLimits, normalize_relative_path
from deerflow.skills.review.readers import LocalDirectoryReader
from deerflow.skills.types import SkillCategory

_public_only = ContextVar("instance_public_skills", default=False)


@contextmanager
def public_skill_scope():
    token = _public_only.set(True)
    try:
        yield
    finally:
        _public_only.reset(token)


def public_skill_scope_enabled():
    return _public_only.get()


class PublicSkillStorage:
    """Read-only view for activation/discovery, including late package changes."""

    def __init__(self, storage):
        self.storage = storage

    def load_skills(self, *, enabled_only=False):
        return [skill for skill in self.storage.load_skills(enabled_only=enabled_only) if skill.category == SkillCategory.PUBLIC]

    def get_skills_root_path(self):
        return self.storage.get_skills_root_path()

    def get_container_root(self):
        return self.storage.get_container_root()

    def validate_skill_file_path(self, path):
        if not any(path == skill.skill_file for skill in self.load_skills()):
            raise AgentDenied("Only current public skill references are admitted")
        return self.storage.validate_skill_file_path(path)


@dataclass(frozen=True)
class PublicSkillCapture:
    revision: str
    names: frozenset[str]
    files: tuple[tuple[str, bytes], ...]


def _selected(definition, app_config):
    from deerflow.agents.lead_agent.prompt import get_enabled_skills_for_config

    if definition.config.get("mcp_plugins"):
        raise AgentDenied("Instance MCP credentials require an explicit capability adapter")
    requested = definition.config.get("skills")
    if requested == []:
        return []
    available = [skill for skill in get_enabled_skills_for_config(app_config, user_id=None) if skill.category == SkillCategory.PUBLIC]
    if requested is not None:
        names = {skill.name for skill in available}
        if set(requested) - names:
            raise AgentDenied("Instance skill references must be currently enabled public packages")
        available = [skill for skill in available if skill.name in requested]
    return available


def public_skill_names(definition, app_config):
    return {skill.name for skill in _selected(definition, app_config)}


def capture_public_skills(definition, app_config):
    selected = _selected(definition, app_config)
    files, manifest = [], []
    total = 0
    limits = PackageLimits(max_files=4096, max_file_bytes=8 << 20, max_total_bytes=64 << 20)
    for skill in selected:
        if skill.skill_dir.is_symlink() or skill.skill_file.is_symlink():
            raise AgentDenied("Public skill references cannot import linked host files")
        snapshot = LocalDirectoryReader(skill.skill_dir, limits=limits).read()
        if snapshot["reader_errors"] or snapshot["truncated"]:
            raise AgentDenied("The public skill package cannot be captured completely")
        package = normalize_relative_path(skill.relative_path.as_posix())
        for entry in snapshot["files"]:
            if entry["kind"] not in {"text", "binary"}:
                raise AgentDenied("Public skill captures do not follow links")
            path = "public/" + package + "/" + normalize_relative_path(entry["path"])
            data = entry["content"].encode("utf-8") if entry["kind"] == "text" else base64.b64decode(entry["content_base64"], validate=True)
            if hashlib.sha256(data).hexdigest() != entry["sha256"]:
                raise AgentDenied("The public package bytes do not match their capture")
            total += len(data)
            if total > limits.max_total_bytes or len(files) >= limits.max_files:
                raise AgentDenied("Instance public packages exceed their aggregate capture budget")
            files.append((path, data))
            manifest.append((path, entry["sha256"]))
    files.sort(key=lambda entry: entry[0])
    if len({path for path, _ in files}) != len(files):
        raise AgentDenied("Public package references overlap")
    revision = hashlib.sha256(json.dumps(sorted(manifest), separators=(",", ":"), ensure_ascii=False).encode("utf-8")).hexdigest()
    return PublicSkillCapture(revision, frozenset(skill.name for skill in selected), tuple(files))
