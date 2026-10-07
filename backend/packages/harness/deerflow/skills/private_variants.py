"""Registered baseline snapshots and scanned, owner-private copies."""

from __future__ import annotations

import asyncio
import re
import tempfile
import zipfile
from pathlib import Path

import yaml

from deerflow.config.extensions_config import ExtensionsConfig
from deerflow.skills.export import SkillExportError, build_skill_export, export_manifest
from deerflow.skills.frontmatter import split_skill_markdown
from deerflow.skills.installer import safe_extract_skill_archive
from deerflow.skills.parser import parse_skill_file
from deerflow.skills.types import SkillCategory
from deerflow.utils.file_io import await_drained

MAX_BASELINE_SOURCES = 256


def _sources(storage):
    seen = set()
    config = ExtensionsConfig.from_file()
    for category, root, path in storage._iter_skill_files():
        if category == SkillCategory.CUSTOM:
            continue
        skill = parse_skill_file(path, category=category, relative_path=path.parent.relative_to(root))
        if skill is None:
            continue
        identifier = f"{category.value}:{skill.relative_path.as_posix()}"
        if identifier in seen:
            raise ValueError("Ambiguous provider skill source.")
        seen.add(identifier)
        if len(seen) > MAX_BASELINE_SOURCES:
            raise ValueError("Too many provider skill sources.")
        yield skill, {"source_id": identifier, "name": skill.name, "category": category.value, "enabled": config.is_skill_enabled(skill.name, category.value)}


def provider_skill_sources(storage):
    """Source identity survives effective-name shadowing; host paths stay private."""
    return [metadata for _, metadata in _sources(storage)]


class _SourceStorage:
    def __init__(self, storage, skill):
        self.storage = storage
        self.skill = skill

    def __getattr__(self, name):
        return getattr(self.storage, name)

    def get_custom_skill_dir(self, name):
        if name != self.skill.name or self.skill.skill_dir.name != name:
            raise SkillExportError(422, "skill_export_unsupported", "Source package directory must match its declared name.")
        return self.skill.skill_dir


def _selected_source(storage, source_id):
    for skill, metadata in _sources(storage):
        if metadata["source_id"] == source_id:
            return _SourceStorage(storage, skill), metadata
    raise SkillExportError(404, "skill_not_found", "Provider skill source not found.")


def source_manifest(storage, source_id, cancel_event=None):
    source, metadata = _selected_source(storage, source_id)
    return {**export_manifest(source, metadata["name"], cancel_event), **metadata}


async def clone_provider_skill(storage, source_id, *, expected_revision, name=None, allow_baseline_override=False):
    """Drain an admitted clone and never alter its source package or old variant."""
    return await await_drained(_clone_provider_skill(storage, source_id, expected_revision=expected_revision, name=name, allow_baseline_override=allow_baseline_override))


async def _clone_provider_skill(storage, source_id, *, expected_revision, name, allow_baseline_override):
    source, metadata = await asyncio.to_thread(_selected_source, storage, source_id)
    if metadata["enabled"] is not True:
        raise ValueError("The provider has disabled this baseline skill.")
    target_name = storage.validate_skill_name(name or metadata["name"][:56].rstrip("-") + "-private")
    await asyncio.to_thread(storage.require_new_private_name, target_name, allow_baseline_override=allow_baseline_override)
    archive = await asyncio.to_thread(build_skill_export, source, metadata["name"], expected_revision)
    temporary = await asyncio.to_thread(tempfile.TemporaryDirectory, prefix="hartmesh-skill-clone-")
    try:
        container_root = storage.get_container_root()
        rewritten = await asyncio.to_thread(_renamed_archive, archive.file, Path(temporary.name), metadata["name"], target_name, source.skill.get_container_path(container_root), f"{container_root}/custom/{target_name}")
        origin = {"source_id": source_id, "source_name": metadata["name"], "source_category": metadata["category"], "revision": expected_revision}

        def source_check():
            current = source_manifest(storage, source_id)
            if current["revision"] != expected_revision or current["enabled"] is not True:
                raise SkillExportError(409, "skill_changed", "Provider skill changed; refresh the clone preview.")

        return await storage.ainstall_skill_from_archive(rewritten, allow_baseline_override=allow_baseline_override, origin=origin, source_check=source_check)
    finally:
        archive.close()
        await asyncio.to_thread(temporary.cleanup)


def _renamed_archive(file, root, source_name, target_name, source_path, target_path):
    with zipfile.ZipFile(file) as source:
        safe_extract_skill_archive(source, root)
    package = root / source_name
    path = package / "SKILL.md"
    parts, error = split_skill_markdown(path.read_text(encoding="utf-8"))
    if error or parts is None:
        raise ValueError("Source skill has invalid frontmatter.")
    parts.metadata["name"] = target_name
    # Relocate only this package's canonical instruction references. Resource
    # contents can contain examples or quoted paths and retain their bytes.
    body = re.sub(re.escape(source_path) + r"(?=/|[\s`\"'<>)]|$)", lambda _: target_path, parts.body)
    path.write_text("---\n" + yaml.safe_dump(parts.metadata, allow_unicode=True, sort_keys=False) + "---\n" + body, encoding="utf-8")
    archive = root / "clone.skill"
    with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as output:
        for child in sorted(package.rglob("*")):
            output.write(child, Path(target_name) / child.relative_to(package))
    return archive
