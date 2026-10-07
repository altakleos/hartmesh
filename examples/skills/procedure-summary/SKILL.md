---
name: procedure-summary
description: Present supplied procedure scope, ordered steps, and caveats with an editable handout and an optional printable summary.
allowed-tools: Read Bash present_files
compatibility: Python 3.12 or later; standard-library document writers.
---

# Procedure summary

This ordinary package is a skill-result-acceptance-fixture and a reusable example.
It imports no HartMesh or report-specific helpers. Summarize only supplied
instructions. Ask about missing or contradictory requirements; do not fill them
with guessed safety procedures, intervals, responsibilities or authorization.

1. Prepare UTF-8 JSON containing an optional `title`, required `scope`, 1–24
   `steps`, and up to 16 optional `caveats`. Preserve supplied wording and order.
   Each scope, step and caveat is limited to 2,048 characters; the title to 256.
   Input is limited to 256 KiB; duplicate keys and invalid controls are refused.
2. Resolve `scripts/build.py` relative to the canonical directory containing
   the SKILL.md you loaded. Its [document writers](scripts/documents.py) are
   packaged alongside it. Run:

       python3 -B <skill-directory>/scripts/build.py --input <uploaded-json-path> --output /mnt/user-data/outputs/skill-results
3. Read the JSON `files` and `warnings`; paths are relative to that output
   directory. Present every listed file using `present_files`, with the view
   first when available. Never advertise an omitted or failed export.

The editable DOCX preserves XML-compatible UTF-8 text, metacharacters, spaces and line
breaks. Its package contains no macros or external relationships. The original
JSON bytes remain available. Portable PDF uses Courier/WinAnsi and at most
eight complete pages; unsupported glyphs or limits omit only PDF, with a visible
notice. A rich-view serialization failure retains ordinary files. This is a
summary of supplied material, not a certification or substitute for the full
procedure.
