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


## Retained working choices and tracked Work

Use only the host-qualified workspace scope. For a persistent AI employee,
`workflow-data/procedure-summary/settings.json` under Home survives conversations.
The path is a consumer convention, not a permission boundary; current Home
permissions apply. In ordinary chat, keep the file with that conversation and
use explicit export/import for reuse. Save only an explicitly requested lasting
choice; clarify personal versus shared AI employee scope when ambiguous.

Run `python3 -B <skill-directory>/scripts/working_data.py read <settings-path>`.
It reports `missing` without creating a file, or returns settings, revision and
SHA-256. The version1 object supports `include_caveats` (boolean) and `heading`
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

Build with `--settings <settings-path>`. A one-off `--include-caveats` yes/no overrides
`include_caveats` for this output only, preserving saved bytes/revision. Defaults apply
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
