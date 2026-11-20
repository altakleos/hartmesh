"""Private request identity and administrator-visible provenance for an imported file."""

import re
from dataclasses import dataclass

MAX_NAME_SUFFIX = 99


def numbered_paths(path):
    parent, _, name = path.rpartition("/")
    stem, dot, extension = name.rpartition(".")
    stem, extension = (stem, dot + extension) if dot and stem else (name, "")
    if len(name.encode("utf-8")) <= 255:
        yield path
    for number in range(2, MAX_NAME_SUFFIX + 1):
        leaf = f"{stem} ({number}){extension}"
        if len(leaf.encode("utf-8")) <= 255:
            yield f"{parent}/{leaf}" if parent else leaf


def visible_source(source):
    if source == {"kind": "own_conversation"}:
        return source
    if isinstance(source, dict) and set(source) == {"kind", "publication_id"} and source["kind"] == "shared_publication" and isinstance(source["publication_id"], str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", source["publication_id"]):
        return source
    return None


@dataclass(frozen=True)
class FileImport:
    binding: str
    publication_id: str | None = None

    def request_fields(self, path):
        source = {"kind": "own_conversation"} if self.publication_id is None else {"kind": "shared_publication", "publication_id": self.publication_id}
        if not isinstance(self.binding, str) or not re.fullmatch(r"[0-9a-f]{64}", self.binding) or visible_source(source) is None:
            raise ValueError("Invalid imported file identity")
        return {"source": source, "import_binding": self.binding, "requested_path": path}

    @staticmethod
    def same_request(stored, request):
        if not isinstance(stored, dict) or not isinstance(request.get("requested_path"), str):
            return False
        chosen = stored.get("path")
        return chosen in numbered_paths(request["requested_path"]) and stored == {**request, "path": chosen}
