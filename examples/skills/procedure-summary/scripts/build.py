"""Procedure summary producer; skill-result-acceptance-fixture."""

from __future__ import annotations

import argparse
import json

from documents import docx, pdf, publish, read_input, text, text_list
from working_data import read, validate


def build(input_path, output, settings_path=None, include_caveats=None):
    raw, source = read_input(input_path)
    snapshot = read(settings_path) if settings_path else None
    choices = snapshot["settings"] if snapshot else {"version": 1}
    if include_caveats is not None:
        choices = {**choices, "include_caveats": include_caveats}
    validate(choices)
    title = text(choices.get("heading", source.get("title", "Procedure summary")), "title", 256)
    scope = text(source.get("scope"), "scope")
    steps = text_list(source.get("steps"), "steps", 24)
    caveats = text_list(source.get("caveats"), "caveats", 16, optional=True)
    notice = "Summary of supplied instructions only. Consult the original procedure before operation; missing requirements have not been inferred."
    blocks = [
        {"type": "text", "heading": "Scope", "paragraphs": [scope]},
        {"type": "list", "heading": "Sequence", "ordered": True, "items": steps},
        {
            "type": "notice",
            "tone": "neutral",
            "text": notice,
            "attribution": "Procedure summary skill",
        },
    ]
    if caveats and choices.get("include_caveats", True):
        blocks.append({"type": "list", "heading": "Supplied caveats", "items": caveats})
    view = {"title": title, "blocks": blocks}
    paragraphs = [
        title,
        scope,
        *[f"{index}. {step}" for index, step in enumerate(steps, 1)],
        notice,
        *(caveats if choices.get("include_caveats", True) else ["Supplied caveats remain in the retained source; omitted from this handout by the requested presentation choice."]),
    ]
    result = publish(
        output,
        "procedure",
        raw,
        view,
        [
            ("procedure.pdf", "Printable handout", lambda path: pdf(path, paragraphs)),
            ("procedure.docx", "Editable handout", lambda path: docx(path, paragraphs)),
        ],
    )
    return {**result, "settings_used": snapshot, "effective": choices}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--settings")
    parser.add_argument("--include-caveats", choices=("yes", "no"))
    arguments = parser.parse_args()
    try:
        result = build(arguments.input, arguments.output, arguments.settings, None if arguments.include_caveats is None else arguments.include_caveats == "yes")
    except (OSError, ValueError, TypeError, RecursionError) as error:
        parser.exit(1, f"Cannot build procedure summary: {error}\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
