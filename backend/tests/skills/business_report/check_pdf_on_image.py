#!/usr/bin/env python3
"""Assert a rendered report PDF on the sandbox image (used by the smoke workflow).

Runs inside the built image, where PyMuPDF is available, after report.py has
built and rendered the small fixture. Exits non-zero with a reason when the PDF
is not what the skill promises: selectable text, the title on page one, a
plausible page count, and the sans-serif font the stylesheet asks for.
"""

from __future__ import annotations

import sys

import fitz


def main(argv: list[str]) -> int:
    if len(argv) != 2:
        sys.stderr.write("usage: check_pdf_on_image.py <report.pdf>\n")
        return 2
    document = fitz.open(argv[1])
    first_page = document[0].get_text()
    fonts = sorted({font[3] for page in document for font in page.get_fonts()})
    problems = []
    if not 2 <= document.page_count <= 8:
        problems.append(f"page count {document.page_count} is outside 2 to 8")
    if "Business Review" not in first_page:
        problems.append("page one does not carry the title")
    if "Totals match your file" not in "".join(page.get_text() for page in document):
        problems.append("the checks line is missing")
    if not any("DejaVu-Sans" in font for font in fonts) or any("Serif" in font for font in fonts):
        problems.append(f"unexpected fonts {fonts}")
    if problems:
        sys.stderr.write("PDF check failed: " + "; ".join(problems) + "\n")
        return 1
    print(f"pdf ok: {document.page_count} pages, fonts {fonts}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
