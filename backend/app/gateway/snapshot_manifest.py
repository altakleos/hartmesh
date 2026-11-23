"""The stable on-disk representation of a published conversation."""

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from app.gateway.transcript import is_visible, transcript_stem, visible_text

MIB = 1024 * 1024
FILE_LIMIT = 20 * MIB
TOTAL_LIMIT = 100 * MIB
MANIFEST_LIMIT = 10 * MIB
MANIFEST_SOURCE = "/conversation-snapshot/manifest"
TRANSCRIPT_SOURCE = "/conversation-snapshot/transcript"
Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class SnapshotLimit(ValueError):
    """An identified publication limit for the sharing dialog."""


class SnapshotModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, populate_by_name=True)


class SnapshotMember(SnapshotModel):
    path: str = Field(min_length=1, max_length=4096)
    size: int = Field(ge=0)
    sha256: Digest
    also_named: list[str] = Field(default_factory=list, max_length=64)


class SnapshotMessage(SnapshotModel):
    type: Literal["human", "ai"]
    content: str


class SnapshotAgent(SnapshotModel):
    kind: Literal["assistant", "employee"]
    id: str = Field(min_length=1, max_length=256)
    name: str = Field(min_length=1, max_length=256)


class SnapshotManifest(SnapshotModel):
    kind: Literal["conversation_snapshot"] = "conversation_snapshot"
    version: Literal[1] = 1
    title: str
    with_agent: SnapshotAgent | None = Field(None, alias="with")
    messages: list[SnapshotMessage] = Field(max_length=10000)
    transcript: SnapshotMember
    files: list[SnapshotMember] = Field(max_length=64)

    @model_validator(mode="after")
    def check_members(self):
        if self.transcript.path != "transcript.md" or self.transcript.size > MANIFEST_LIMIT or self.transcript.also_named:
            raise ValueError("Invalid snapshot transcript")
        seen = set()
        total = 0
        for member in self.files:
            path = PurePosixPath(member.path)
            if len(path.parts) < 2 or path.parts[0] != "files" or path.as_posix() != member.path or any(part.startswith(".") or "\\" in part or "\0" in part for part in path.parts):
                raise ValueError("Invalid snapshot file name")
            names = [path.name, *member.also_named]
            for name in names:
                if not name or name.startswith(".") or "/" in name or "\\" in name or "\0" in name or (path.parent, name) in seen:
                    raise ValueError("Invalid or duplicate snapshot name")
                seen.add((path.parent, name))
            if member.size > FILE_LIMIT:
                raise ValueError("Snapshot file is too large")
            total += member.size
        if total > TOTAL_LIMIT or len(seen) > 64:
            raise ValueError("Snapshot exceeds the file limit")
        return self


@dataclass(frozen=True)
class FrozenManifest:
    document: bytes
    transcript: bytes
    digest: str
    folder: str
    manifest: SnapshotManifest


def digest_of_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def parse_manifest(data: bytes) -> SnapshotManifest:
    if len(data) > MANIFEST_LIMIT:
        raise ValueError("Snapshot manifest is too large")
    value = json.loads(data)
    if not isinstance(value, dict) or value.get("kind") != "conversation_snapshot" or type(value.get("version")) is not int or value.get("version") != 1:
        raise ValueError("Unknown snapshot format")
    return SnapshotManifest.model_validate(value)


def freeze_manifest(title: str, messages: Iterable[dict], files: Iterable[SnapshotMember], *, with_agent: SnapshotAgent | None = None) -> FrozenManifest:
    rows = []
    for message in messages:
        if message.get("type") in {"human", "ai"} and is_visible(message) and (text := visible_text(message)):
            rows.append(SnapshotMessage(type=message["type"], content=text))
            if len(rows) > 10000:
                raise SnapshotLimit("snapshot-message-count")
    lines = [f"# {title}", ""]
    for row in rows:
        heading = "## 🧑 User" if row.type == "human" else "## 🤖 Assistant"
        lines.extend([heading, "", row.content, "", "---", ""])
    transcript = ("\n".join(lines).rstrip() + "\n").encode()
    if len(transcript) > MANIFEST_LIMIT:
        raise SnapshotLimit("snapshot-transcript-limit")
    grouped = {}
    for item in sorted(files, key=lambda item: item.path):
        path = PurePosixPath(item.path)
        key = (path.parent, item.sha256)
        if key in grouped:
            grouped[key].also_named.append(path.name)
        else:
            grouped[key] = item.model_copy(deep=True)
    manifest = SnapshotManifest(title=title, with_agent=with_agent, messages=rows, transcript=SnapshotMember(path="transcript.md", size=len(transcript), sha256=digest_of_bytes(transcript)), files=list(grouped.values()))
    document = json.dumps(manifest.model_dump(by_alias=True, exclude_none=True), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    if len(document) > MANIFEST_LIMIT:
        raise SnapshotLimit("snapshot-manifest-limit")
    digest = digest_of_bytes(document)
    stem = transcript_stem(title)[:80]
    suffix = " · " + digest[:8]
    stem = stem.encode()[: 255 - len(suffix.encode())].decode(errors="ignore").rstrip()
    return FrozenManifest(document, transcript, digest, f"Conversations/{stem}{suffix}", manifest)


def member_record(manifest: dict, member: SnapshotMember, record: dict | None) -> bool:
    return bool(
        record
        and record.get("removed_at") is None
        and record["path"] == manifest["path"].rsplit("/", 1)[0] + "/" + member.path
        and record["size"] == member.size
        and record["sha256"] == member.sha256
        and datetime.fromisoformat(record["published_at"]) <= datetime.fromisoformat(manifest["published_at"])
        and (member.path != "transcript.md" or record.get("from_path") == TRANSCRIPT_SOURCE)
    )


def generated_name(source: str) -> str | None:
    return {MANIFEST_SOURCE: "conversation.json", TRANSCRIPT_SOURCE: "transcript.md"}.get(source)
