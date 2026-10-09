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


## Retained working choices and tracked Work

Use only the host-qualified workspace scope. For a persistent AI employee,
`workflow-data/supplier-comparison/settings.json` under Home survives conversations.
The path is a consumer convention, not a permission boundary; current Home
permissions apply. In ordinary chat, keep the file with that conversation and
use explicit export/import for reuse. Save only an explicitly requested lasting
choice; clarify personal versus shared AI employee scope when ambiguous.

Run `python3 -B <skill-directory>/scripts/working_data.py read <settings-path>`.
It reports `missing` without creating a file, or returns settings, revision and
SHA-256. The version1 object supports `show_delivery` (boolean) and `project`
(nonempty plain text, at most256 characters). Unknown keys/types/versions,
duplicate keys, invalid UTF-8, over64 KiB and excessive nesting are rejected.

For an explicit lasting choice, prepare a v1 JSON payload, then run:

    python3 -B <skill-directory>/scripts/working_data.py save <settings-path> --from <payload-json> --expected <read-revision-or-missing> --operation <unique-intent-id>

`patch` takes only changed fields, replacing each whole value; null removes one
field. `reset` takes no payload and keeps an empty v1 document with a new revision.
`validate` checks an existing file. Managed writes add `_mutation` with a distinct
revision, operation ID and exact-request hash. Keep the same ID and request for
an uncertain retry; an intervening mutation or changed request is a conflict.
A post-replacement flush failure can mean published but uncertain. Reconcile the
exact intent before any new mutation. All cooperating writers use the stable
adjacent `.lock`; arbitrary native edits/deletion are outside this guarantee.
Reset affects future choices only, not output history, conversations or memory.

Build with `--settings <settings-path>`. A one-off `--show-delivery` yes/no overrides
`show_delivery` for this output only, preserving saved bytes/revision. Defaults apply
first, saved settings second, explicit invocation last. The returned
`settings_used` and `effective` identify the snapshot and choices; keep them with
Work evidence when needed. Omitted presentation fields remain in the source.

In an admitted tracked Work run, use available `read_work_context` and
`report_work` for progress, relevant blockers and outcomes. Ask for missing facts
or conflicting instructions through the generic human-input request capability;
never infer a manager decision from prose. Preserve intermediate files, assess
factual responses after explicit resumption, and register suggestions under the
adopted policy. A new derived Work item requires separate activation. Producing
a file does not imply human acceptance or an external action. Without those
admitted tools, offer ordinary output and explain the missing capability.
This optional example is not a mandatory production skill; private installation
does not make it available to a bound instance's public-only skill capture.
