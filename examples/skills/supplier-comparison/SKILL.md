---
name: supplier-comparison
description: Compare supplied structured quotations with exact decimal line totals, explicit caveats, a workbook, and an optional printable brief.
allowed-tools: Read Bash present_files
compatibility: Python 3.12 or later; XLSX export uses openpyxl.
---

# Supplier comparison

This ordinary package is a skill-result-acceptance-fixture and a reusable example.
It imports no HartMesh or report-specific helpers. Keep quotations and caveats
as supplied; ask the user to resolve ambiguous values before preparing input.
Do not invent taxes, freight, delivery promises, exchange rates or recommendations.

1. Read the user's UTF-8 JSON input. It contains an optional `title` and 1–50
   `suppliers`. Each quotation supplies `name`, `quantity`, `unit_price` and
   three-letter uppercase `currency`; `delivery` and `caveats` are optional.
   Quantities are positive decimal strings with at most three fractional digits
   and at most 1,000,000. Prices are nonnegative decimal strings with at most
   two fractional digits and at most 1,000,000,000. Never coerce booleans or
   approximate floats. Input is limited to 256 KiB; duplicate keys are refused.
2. Resolve `scripts/build.py` relative to the canonical directory containing
   the SKILL.md you loaded. Its [document writers](scripts/documents.py) are
   packaged alongside it. Run:

       python3 -B <skill-directory>/scripts/build.py --input <uploaded-json-path> --output /mnt/user-data/outputs/skill-results
   The program validates the complete input before publishing a new result.
3. Read its JSON `files` and `warnings`. Each file is relative to the requested
   output directory. Present every listed file using `present_files`, with the
   view first when available. Never present a filename that the script omitted.

Line totals use decimal arithmetic and half-up cent rounding. Comparisons keep
input order and never rank different currencies. The workbook stores supplied
labels and caveats as literal strings, including formula-like beginnings.
The JSON source retains its original UTF-8 bytes. Portable PDFs use Courier
and WinAnsi, at most eight complete pages. Unsupported glyphs or writer limits
omit only that export with a visible notice; valid source/workbook/view files
remain available. A failed rich-view write retains ordinary outputs. No format
is a platform verification of the supplied commercial facts.
