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

`business_report_settings.py` owns bounded v1 preferences validation and the
stable adjacent lock, exact expected revision, operation identity, atomic
replacement and uncertain-outcome reconciliation. Do not move business fields
into core. Legacy valid v1 reads are byte-preserving; managed writes add optional
`_mutation` metadata and reset retains a new revision. Reporting build validates
all fields before applying any, supports temporary field replacement, and retains
`preferences-used.json` as a hashed supporting bundle member. Prose/render copy
that exact verified snapshot. Report/view schemas are unchanged. The first-command
declaration admits report.py so explicit settings-only requests need no build.

`business_report_product.py` owns the strict offline shared identity projection.
Capture once before each build/prose/render/export, then pass the immutable result
to every renderer. Only HTML and derived PDF add product/provider attribution;
report company and tenant presentation keep their existing owners. Store the
canonical projection as a hashed supporting member; never carry HTML/PDF across
a changed shared identity. Fixed fields come only from the operator environment
and its read-only digest-bound input. No Gateway imports, network or shell calls.
The pure `urllib.parse.urlsplit` parser is allowed; it performs no I/O. Shipped
Pillow validates/decodes projected PNGs. See the
[input/activation contract](../../../docs/OFFLINE_REPORT_PRODUCT_IDENTITY.md).

`business_report_presentation.py` captures the existing strict tenant projection
and product binding once; default product envelopes bind without adding provider
attribution. Legacy capture sanitizes bytes once with the existing name policy.
Keep current Workspace context separate from authored/legacy company labels.
Retain exact effective palette and relative asset as hashed supporting members;
verify semantic binding before publication/reuse, not only outer member hashes.

`business_report_charts.py` owns bounded retained draw values. Reuse pixels only
with compatible validated data/primary; redraw changed primary from those values,
never from source exports or changed profiles. Missing legacy draw data refuses
presentation-affected rendering; an explicit XLSX export needs no chart pixels.
Prose/render retain historical preference bytes and never apply current files.
HTML/PDF carry needs full presentation equality; DOCX needs tenant/palette
compatibility; XLSX carries unchanged facts. Keep previews neutral and actual
export lists truthful. Existing schemas, source fences and publication uncertainty
remain. See [the full contract](../../../docs/OFFLINE_REPORT_PRESENTATION.md).
