"""Bounded streaming reads of internal Open Packaging Convention parts."""

import io
import posixpath
import struct
import zipfile
from collections import deque
from xml.parsers import expat

MAX_MEMBERS = 5000
MAX_PART = 32 * 1024 * 1024
MAX_TOTAL = 64 * 1024 * 1024
CHUNK = 64 * 1024
MAX_DEPTH = 128
MAX_TEXT = 32 * 1024

_NAMESPACES = {"http://schemas.openxmlformats.org/package/2006/relationships": "pkg"}
for transitional, strict, prefix in (
    ("spreadsheetml/2006/main", "spreadsheetml/main", "x"),
    ("wordprocessingml/2006/main", "wordprocessingml/main", "w"),
    ("presentationml/2006/main", "presentationml/main", "p"),
    ("drawingml/2006/main", "drawingml/main", "a"),
    ("officeDocument/2006/relationships", "officeDocument/relationships", "r"),
):
    _NAMESPACES["http://schemas.openxmlformats.org/" + transitional] = prefix
    _NAMESPACES["http://purl.oclc.org/ooxml/" + strict] = prefix


def short_name(name):
    namespace, sep, local = name.partition("}")
    if not sep:
        return name
    return _NAMESPACES.get(namespace, "?") + ":" + local


def _check_directory(source):
    # The end record identifies split archives and bounds directory allocation
    # before ZipFile constructs a ZipInfo object for every member.
    if len(source) > 16 * 1024 * 1024:
        raise ValueError("Office source exceeds the preview budget")
    offset = source.rfind(b"PK\x05\x06", max(0, len(source) - 65557))
    if offset < 0 or offset + 22 > len(source):
        raise ValueError("Missing ZIP directory")
    disk, directory_disk, local_entries, count, length, start, comment = struct.unpack_from("<4H2IH", source, offset + 4)
    if disk or directory_disk or count != local_entries or count > MAX_MEMBERS or offset + 22 + comment != len(source) or start + length != offset:
        raise ValueError("Unsupported ZIP directory")
    # Check the actual count as well as the self-reported count.
    cursor = start
    for _ in range(count):
        if source[cursor : cursor + 4] != b"PK\x01\x02" or cursor + 46 > offset:
            raise ValueError("Invalid ZIP directory")
        name, extra, note, member_disk = struct.unpack_from("<4H", source, cursor + 28)
        if member_disk:
            raise ValueError("Multi-disk archives are unsupported")
        cursor += 46 + name + extra + note
    if cursor != offset:
        raise ValueError("Inconsistent ZIP directory")


class OfficePackage:
    def __init__(self, source):
        _check_directory(source)
        self.zip = zipfile.ZipFile(io.BytesIO(source))
        self.total = 0
        self.notices = set()
        names = set()
        for info in self.zip.infolist():
            if info.flag_bits & 1 or info.filename in names:
                raise ValueError("Encrypted or ambiguous archive")
            if info.compress_type not in (zipfile.ZIP_STORED, zipfile.ZIP_DEFLATED):
                raise ValueError("Unsupported Office compression")
            names.add(info.filename)

    def events(self, part):
        """Yield normalized start/text/end events without retaining a document tree."""
        parser = expat.ParserCreate(namespace_separator="}")
        events = deque()
        depth = 0
        text_size = 0

        def start(name, attrs):
            nonlocal depth, text_size
            depth += 1
            text_size = 0
            if depth > MAX_DEPTH:
                raise ValueError("XML depth exceeds the preview budget")
            events.append(("start", short_name(name), {short_name(key): value for key, value in attrs.items()}))

        def end(name):
            nonlocal depth, text_size
            depth -= 1
            text_size = 0
            events.append(("end", short_name(name), None))

        def characters(content):
            nonlocal text_size
            encoded = content.encode("utf-8")
            keep = min(len(encoded), MAX_TEXT - text_size)
            if keep != len(encoded):
                self.notices.add("Long text was shortened.")
            if keep:
                events.append(("text", "", encoded[:keep].decode("utf-8", errors="ignore")))
                text_size += keep

        def refuse(*_):
            raise ValueError("Document types and entities are unsupported")

        parser.StartDoctypeDeclHandler = parser.EntityDeclHandler = parser.ExternalEntityRefHandler = refuse
        parser.StartElementHandler, parser.EndElementHandler, parser.CharacterDataHandler = start, end, characters
        size = 0
        with self.zip.open(part) as stream:
            while True:
                chunk = stream.read(min(CHUNK, MAX_PART - size + 1, MAX_TOTAL - self.total + 1))
                size += len(chunk)
                self.total += len(chunk)
                if size > MAX_PART or self.total > MAX_TOTAL:
                    raise ValueError("Expanded document exceeds the preview budget")
                parser.Parse(chunk, not chunk)
                while events:
                    yield events.popleft()
                if not chunk:
                    break

    def relationships(self, part):
        directory, name = posixpath.split(part)
        rels = posixpath.join(directory, "_rels", name + ".rels")
        if rels not in self.zip.namelist():
            return {}
        relationships = {}
        for event, tag, attrs in self.events(rels):
            if event != "start" or tag != "pkg:Relationship":
                continue
            kind, _, local = attrs.get("Type", "").rpartition("/")
            if _NAMESPACES.get(kind) != "r":
                continue
            if attrs.get("TargetMode", "Internal") != "Internal":
                continue
            target = attrs["Target"]
            if not target or any(char in target for char in ("\\", ":", "?", "#")):
                continue
            path = posixpath.normpath(posixpath.join(directory, target)).lstrip("/")
            if path in {".", ".."} or path.startswith("../"):
                continue
            key = attrs["Id"]
            if key in relationships or len(relationships) >= MAX_MEMBERS:
                raise ValueError("Invalid Office relationships")
            relationships[key] = (local, path)
        return relationships

    def main_part(self):
        parts = [path for kind, path in self.relationships("").values() if kind == "officeDocument"]
        if len(parts) != 1:
            raise ValueError("Missing or ambiguous Office document")
        return parts[0]
