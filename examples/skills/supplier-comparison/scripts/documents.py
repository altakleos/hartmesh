"""Bounded ordinary document writers; skill-result-acceptance-fixture.

This file ships inside this package. It imports no harness or report modules.
"""

from __future__ import annotations

import json
import tempfile
import textwrap
import uuid
import zipfile
from pathlib import Path
from xml.sax.saxutils import escape

MAX_INPUT_BYTES = 256 * 1024
MAX_OUTPUT_BYTES = 16 * 1024 * 1024
MAX_VIEW_BYTES = 1024 * 1024


def text(value, field, limit=2048):
    if not isinstance(value, str) or not value.strip() or len(value) > limit or any(ord(char) < 32 and char not in "\n\t" for char in value):
        raise ValueError(f"Invalid {field}.")
    try:
        value.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError(f"Invalid {field} Unicode.") from error
    return value


def text_list(value, field, maximum=24, *, optional=False):
    if optional and value is None:
        return []
    if not isinstance(value, list) or not (0 if optional else 1) <= len(value) <= maximum:
        raise ValueError(f"Invalid {field}.")
    return [text(item, field) for item in value]


def read_input(path):
    with Path(path).open("rb") as source:
        raw = source.read(MAX_INPUT_BYTES + 1)
    if len(raw) > MAX_INPUT_BYTES:
        raise ValueError("Input exceeds 256 KiB.")

    def pairs(items):
        result = {}
        for name, value in items:
            if name in result:
                raise ValueError("Duplicate input key.")
            result[name] = value
        return result

    def invalid_constant(_):
        raise ValueError("Non-finite input number.")

    data = json.loads(raw.decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid_constant)
    if not isinstance(data, dict):
        raise ValueError("Input must be an object.")
    return raw, data


def pdf(path, paragraphs):
    """Portable Courier/WinAnsi PDF, with complete bounded pagination."""
    lines = []
    for paragraph in paragraphs:
        if "\x7f" in paragraph:
            raise ValueError("Portable PDF font cannot represent the supplied DEL control.")
        if "\u00ad" in paragraph:
            raise ValueError("Portable PDF font cannot represent the supplied discretionary soft hyphen.")
        paragraph.encode("cp1252")  # Refuse unsupported glyphs before writing.
        for line in paragraph.split("\n"):
            lines.extend(
                textwrap.wrap(
                    line.expandtabs(4),
                    width=90,
                    replace_whitespace=False,
                    drop_whitespace=False,
                )
                or [""]
            )
        lines.append("")
    pages = [lines[index : index + 64] for index in range(0, len(lines), 64)] or [[]]
    if len(pages) > 8:
        raise ValueError("PDF exceeds its eight-page limit.")
    objects = [
        b"<< /Type /Catalog /Pages 2 0 R >>",
        b"",
        b"<< /Type /Font /Subtype /Type1 /BaseFont /Courier /Encoding /WinAnsiEncoding >>",
    ]
    kids = []
    for page in pages:
        page_id, stream_id = len(objects) + 1, len(objects) + 2
        kids.append(f"{page_id} 0 R")
        content = bytearray(b"BT /F1 8 Tf 11 TL 40 752 Td\n")
        for line in page:
            encoded = line.encode("cp1252").replace(b"\\", b"\\\\").replace(b"(", b"\\(").replace(b")", b"\\)")
            content.extend(b"(" + encoded + b") Tj T*\n")
        content.extend(b"ET\n")
        objects.extend(
            [
                f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /Font << /F1 3 0 R >> >> /Contents {stream_id} 0 R >>".encode(),
                f"<< /Length {len(content)} >>\nstream\n".encode() + content + b"endstream",
            ]
        )
    objects[1] = f"<< /Type /Pages /Count {len(pages)} /Kids [{' '.join(kids)}] >>".encode()
    output = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0]
    for index, item in enumerate(objects, 1):
        offsets.append(len(output))
        output.extend(f"{index} 0 obj\n".encode() + item + b"\nendobj\n")
    xref = len(output)
    output.extend(f"xref\n0 {len(offsets)}\n0000000000 65535 f \n".encode())
    for offset in offsets[1:]:
        output.extend(f"{offset:010d} 00000 n \n".encode())
    output.extend(f"trailer\n<< /Size {len(offsets)} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode())
    if len(output) > 2 * 1024 * 1024:
        raise ValueError("PDF exceeds 2 MiB.")
    Path(path).write_bytes(output)


def docx(path, paragraphs):
    """Small valid OOXML document with literal text and explicit line breaks."""
    if any(char in "\ufffe\uffff" for paragraph in paragraphs for char in paragraph):
        raise ValueError("DOCX cannot represent supplied XML-incompatible text.")
    body = []
    for paragraph in paragraphs:
        runs = "<w:br/>".join(f'<w:t xml:space="preserve">{escape(line)}</w:t>' for line in paragraph.split("\n"))
        body.append(f"<w:p><w:r>{runs}</w:r></w:p>")
    document = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main"><w:body>'
        + "".join(body)
        + '<w:sectPr><w:pgSz w:w="12240" w:h="15840"/></w:sectPr></w:body></w:document>'
    )
    content_types = (
        '<?xml version="1.0"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/word/document.xml" ContentType="application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml"/>'
        "</Types>"
    )
    relationships = (
        '<?xml version="1.0"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="word/document.xml"/>'
        "</Relationships>"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, content in (
            ("[Content_Types].xml", content_types),
            ("_rels/.rels", relationships),
            ("word/document.xml", document),
        ):
            archive.writestr(name, content.encode("utf-8"))


def workbook(path, rows):
    from openpyxl import Workbook

    output = Workbook()
    sheet = output.active
    sheet.title = "Quoted terms"
    for row in rows:
        sheet.append(row)
        for cell in sheet[sheet.max_row]:
            if isinstance(cell.value, str):
                cell.data_type = "s"  # Exact source text, never formulas.
    output.save(path)


def publish(output_root, stem, raw, view, writers):
    """Only complete bundles become visible; individual format failure is local."""
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    warnings, exports = [], []
    identifier = f"{stem}-{uuid.uuid4().hex[:12]}"
    with tempfile.TemporaryDirectory(prefix=".skill-result-", dir=output_root) as temporary:
        staged = Path(temporary) / "bundle"
        staged.mkdir()
        source_name = stem + ".json"
        (staged / source_name).write_bytes(raw)
        for filename, label, writer in writers:
            target = staged / filename
            try:
                writer(target)
                if target.stat().st_size > MAX_OUTPUT_BYTES:
                    raise ValueError("Export exceeds 16 MiB.")
                exports.append({"path": filename, "label": label})
            except Exception as error:
                target.unlink(missing_ok=True)
                kind = "PDF" if filename.endswith(".pdf") else filename.rsplit(".", 1)[-1].upper()
                reason = (
                    "portable PDF font does not support this text"
                    if isinstance(error, UnicodeEncodeError)
                    else "required writer dependency is unavailable"
                    if isinstance(error, ImportError)
                    else str(error)
                    if isinstance(error, ValueError)
                    else "writer failure"
                )
                warning = f"{kind} export unavailable: {reason}. Exact source and other successful exports remain available."
                warnings.append(warning)
                view["blocks"].append(
                    {
                        "type": "notice",
                        "tone": "warning",
                        "text": warning,
                        "attribution": "Document skill",
                    }
                )
        view.update(
            format="hartmesh.artifact-view",
            version=1,
            primary_source={"path": source_name},
            exports=exports,
        )
        view_name = stem + ".view.json"
        try:
            encoded = json.dumps(view, ensure_ascii=False, allow_nan=False, indent=2).encode("utf-8")
            if len(encoded) > MAX_VIEW_BYTES:
                raise ValueError("View exceeds 1 MiB.")
            (staged / view_name).write_bytes(encoded)
        except Exception:
            (staged / view_name).unlink(missing_ok=True)
            warnings.append("Rich view unavailable; open the ordinary source and successful exports.")
        files = ([view_name] if (staged / view_name).exists() else []) + [item["path"] for item in exports] + [source_name]
        staged.rename(output_root / identifier)
    return {"files": [f"{identifier}/{name}" for name in files], "warnings": warnings}
