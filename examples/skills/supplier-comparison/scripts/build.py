"""Supplier comparison producer; skill-result-acceptance-fixture."""

from __future__ import annotations

import argparse
import json
import re
from decimal import ROUND_HALF_UP, Decimal

from documents import pdf, publish, read_input, text, text_list, workbook
from working_data import read, validate


def amount(value, field, scale, maximum):
    if not isinstance(value, str) or len(value) > 20 or not re.fullmatch(rf"[0-9]+(?:\.[0-9]{{1,{scale}}})?", value):
        raise ValueError(f"Invalid {field}.")
    result = Decimal(value)
    if result > maximum or result < 0 or field == "quantity" and result == 0:
        raise ValueError(f"Invalid {field}.")
    return result


def build(input_path, output, settings_path=None, show_delivery=None):
    raw, source = read_input(input_path)
    snapshot = read(settings_path) if settings_path else None
    choices = snapshot["settings"] if snapshot else {"version": 1}
    if show_delivery is not None:
        choices = {**choices, "show_delivery": show_delivery}
    validate(choices)
    title = text(choices.get("project", source.get("title", "Supplier comparison")), "title", 256)
    suppliers = source.get("suppliers")
    if not isinstance(suppliers, list) or not 1 <= len(suppliers) <= 50:
        raise ValueError("Invalid suppliers: supply 1 to 50 quotations.")
    rows, notes = [], []
    for supplier in suppliers:
        if not isinstance(supplier, dict):
            raise ValueError("Invalid supplier.")
        name = text(supplier.get("name"), "supplier name", 256)
        quantity = amount(supplier.get("quantity"), "quantity", 3, Decimal("1000000"))
        price = amount(supplier.get("unit_price"), "unit_price", 2, Decimal("1000000000"))
        currency = supplier.get("currency")
        if not isinstance(currency, str) or not re.fullmatch(r"[A-Z]{3}", currency):
            raise ValueError("Invalid currency.")
        delivery = text(supplier.get("delivery", "Not supplied"), "delivery", 512)
        caveats = text_list(supplier.get("caveats"), "quotation caveats", 16, optional=True)
        total = (quantity * price).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        rows.append([name, str(quantity), f"{price:.2f}", f"{total:.2f}", currency, delivery])
        notes.extend(f"{name}: {item}" for item in caveats)
    caveat = "Quoted line totals only, rounded to cents using half-up rounding. Taxes, freight and other unsupplied costs are excluded. Different currencies are not ranked or converted."
    columns = [
        "Supplier",
        "Quantity",
        "Unit price",
        "Quoted total",
        "Currency",
        "Delivery",
    ]
    if not choices.get("show_delivery", True):
        rows = [row[:-1] for row in rows]
        columns = columns[:-1]
        caveat += " Delivery omitted from this presentation; supplied terms remain in the retained source."
    blocks = [
        {
            "type": "text",
            "heading": "Scope",
            "paragraphs": [f"Comparison of {len(rows)} supplied quotations."],
        },
        {
            "type": "table",
            "heading": "Quoted terms",
            "columns": [{"label": item} for item in columns],
            "rows": rows,
        },
        {
            "type": "notice",
            "tone": "warning",
            "text": caveat,
            "attribution": "Supplier comparison skill",
        },
    ]
    for index in range(0, len(notes), 32):
        blocks.append(
            {
                "type": "list",
                "heading": "Supplied caveats",
                "items": notes[index : index + 32],
            }
        )
    view = {
        "title": title,
        "destination": {"collection": "Comparisons"},
        "blocks": blocks,
    }
    paragraphs = [title, caveat, *[" | ".join(row) for row in rows], *notes]
    workbook_rows = [[title], columns, *rows, [caveat], *[[item] for item in notes]]
    result = publish(
        output,
        "comparison",
        raw,
        view,
        [
            ("comparison.pdf", "Decision brief", lambda path: pdf(path, paragraphs)),
            (
                "comparison.xlsx",
                "Quoted terms workbook",
                lambda path: workbook(path, workbook_rows),
            ),
        ],
    )
    return {**result, "settings_used": snapshot, "effective": choices}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--settings")
    parser.add_argument("--show-delivery", choices=("yes", "no"))
    arguments = parser.parse_args()
    try:
        result = build(arguments.input, arguments.output, arguments.settings, None if arguments.show_delivery is None else arguments.show_delivery == "yes")
    except (OSError, ValueError, TypeError, RecursionError) as error:
        parser.exit(1, f"Cannot build comparison: {error}\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
