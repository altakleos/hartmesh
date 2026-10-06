# Business report publication

The script runs on Python 3.10 and shipped document libraries. Install nothing,
call no network service or shell, and never change input files. Read/write text
with explicit UTF-8. Tests live in `backend/tests/skills/business_report/`;
test loaders must suppress bytecode inside this package.

Commands stage complete JSON/chart/check/render bundles under the report root's
reserved `.cache/business-report/` directory. Snapshot current state under a
brief filesystem lock, generate unlocked, then compare the exact source identity
under the commit lock before atomically renaming a unique `drafts/<bundle-id>/`
directory and advancing basename-scoped current state. Flush files and directories
before committing the pointer. Print success paths only after publication.

These commands retain published bundles and never modify their members. Reject
publication roots and explicit exports inside any existing bundle. Hashes detect
outside edits before reuse; an explicit build creates fresh members from inputs.
Legacy filename-only manifests cannot establish coherence, so their formats must
be rendered again. Only verified formats may be copied into a default render's
new bundle. Checks write diagnostics outside published bundles.

The optional `--bundle-id` makes same-call `present` paths predictable. Keep
current state with the drafts when moving a report. A crash after rename may
leave a complete unadopted bundle; never claim a multi-file replacement is atomic.
Report JSON and database schemas are unchanged by this directory layout.

The skill owns `schemas/report.schema.json` and passive view serialization in
`scripts/business_report_view.py`. Reuse `format_value` and `cell_format`, including
row overrides and footer formats; never calculate new facts in presentation.
Emit the view after successful renders/carry and before hashing the staged bundle.
Keep it in member hashes, outside the genuine-format carry list. Only actual staged
formats become explicit exports. View limits produce a truthful concise omission
or ordinary-file fallback; successful formats survive view failures. Interruption
still preserves the previous current draft. Present the view once with exports;
retain the canonical report and charts without duplicate rich handovers.

Compute quality checks before finalizing default action wording. A warning puts
review of Checks first, ahead of business suggestions, within the three-bullet
limit; no fallback may claim the checks need no action. Keep clean reports
useful and preserve numeric calculations and authored-prose validation.
