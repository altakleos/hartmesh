"""Construct passive views that fit the host's byte, block and table budgets."""

import json

MAX_BLOCKS = 64
MAX_CELLS = 5000
MAX_BYTES = 1024 * 1024
MAX_PARAGRAPHS = 16


def encode(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode()


class View:
    def __init__(self, title, notices):
        self.document = {"format": "hartmesh.artifact-view", "version": 1, "title": title, "blocks": [], "exports": []}
        self.notices = notices
        self.bytes_left = MAX_BYTES - len(encode(self.document)) - 8192
        self.cells_left = MAX_CELLS

    def text(self, value, limit=4096):
        if len(value) <= limit:
            return value
        self.notices.add("Long text was shortened.")
        return value[:limit]

    def add(self, block):
        if len(self.document["blocks"]) >= MAX_BLOCKS - 1 or self.bytes_left <= 0:
            self.notices.add("Additional content was left out.")
            return False
        size = len(encode(block)) + 1
        if size > self.bytes_left:
            self.notices.add("Additional content was left out.")
            key = {"table": "rows", "text": "paragraphs", "list": "items"}[block["type"]]
            items = block[key]
            lo, hi = 0, len(items)
            while lo < hi:
                take = (lo + hi + 1) // 2
                if len(encode({**block, key: items[:take]})) + 1 <= self.bytes_left:
                    lo = take
                else:
                    hi = take - 1
            if not lo:
                self.bytes_left = 0
                return False
            block[key] = items[:lo]
            size = self.bytes_left
        self.document["blocks"].append(block)
        self.bytes_left -= size
        return True

    def table(self, rows, heading=None, *, labels=None):
        width = max((len(row) for row in rows), default=0)
        if labels is not None:
            width = max(width, len(labels))
        width = min(12, width)
        if not width or self.cells_left < width:
            self.notices.add("Additional table content was left out.")
            return False
        count = min(200, self.cells_left // width - 1, len(rows))
        if count < len(rows):
            self.notices.add("Additional table rows were left out.")
        labels = labels if labels is not None else [chr(65 + i) for i in range(width)]
        block = {
            "type": "table",
            "columns": [{"label": self.text((labels[i] if i < len(labels) else "") or f"Column {i + 1}", 256)} for i in range(width)],
            "rows": [[self.text(value) for value in (row + [""] * width)[:width]] for row in rows[:count]],
        }
        if heading is not None:
            block["heading"] = self.text(heading, 256)
        if self.add(block):
            self.cells_left -= width * (len(block["rows"]) + 1)
            return True
        return False

    def finish(self):
        if self.notices:
            text = " ".join(sorted(self.notices)) + " Download the original file for the complete content."
            self.document["blocks"].append({"type": "notice", "tone": "neutral", "text": text})
        return encode(self.document)
