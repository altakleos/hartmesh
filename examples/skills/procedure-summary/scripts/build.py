"""Procedure summary producer; skill-result-acceptance-fixture."""

from __future__ import annotations

import argparse
import json

from documents import docx, pdf, publish, read_input, text, text_list


def build(input_path, output):
    raw, source = read_input(input_path)
    title = text(source.get("title", "Procedure summary"), "title", 256)
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
    if caveats:
        blocks.append({"type": "list", "heading": "Supplied caveats", "items": caveats})
    view = {"title": title, "blocks": blocks}
    paragraphs = [
        title,
        scope,
        *[f"{index}. {step}" for index, step in enumerate(steps, 1)],
        notice,
        *caveats,
    ]
    return publish(
        output,
        "procedure",
        raw,
        view,
        [
            ("procedure.pdf", "Printable handout", lambda path: pdf(path, paragraphs)),
            ("procedure.docx", "Editable handout", lambda path: docx(path, paragraphs)),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    arguments = parser.parse_args()
    try:
        result = build(arguments.input, arguments.output)
    except (OSError, ValueError, TypeError, RecursionError) as error:
        parser.exit(1, f"Cannot build procedure summary: {error}\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
