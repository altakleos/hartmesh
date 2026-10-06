---
name: business-report
description: Use this skill when the user uploads a tabular business export (CSV, XLSX or XLS of jobs, orders, invoices, appointments or sales) and wants a monthly, quarterly or yearly business review, management report or summary they can download, share or send. Produces one report with KPIs, tables, locally drawn charts and a plain-words checks line, rendered as PDF, Word (DOCX) and Excel (XLSX) with the same numbers in each.
first-command: scripts/report.py build
---

# Business Report Skill

## Overview

One script turns an export into a report draft: `report.json` plus PNG charts, renders that one document to HTML, PDF, DOCX and XLSX, then adds a bounded passive `*.view.json` presentation. Every number in every render comes from `report.json`, so the formats agree by construction. The script runs on the libraries the sandbox image ships, installs nothing, calls no network service (the PDF printer refuses every URL that is not inline data) and never modifies an input file.

The first call for a report is the `build` in Step 1. It reads the export itself and prints what it found, so neither the upload, its sheets nor this directory needs looking at first: the scripts it runs are the ones named below. The runtime holds to that order (the `first-command` line above): the first time this file is read in a conversation, nothing else runs in the sandbox until a build has run, whatever its outcome, or until you answer without a tool.

**Script paths.** `$SKILL_DIR` is this skill's own directory — the one holding this `SKILL.md`, which `describe_skill` reports as `Directory` (`Location` is the file inside it). Assign it in every command that runs one of these scripts, as a statement of its own ahead of the script, the way each example below does (`SKILL_DIR="<Directory>"; python …`). Written in front of the script without the `;`, it is not set yet when bash expands that command's own words: the guard stops the command, and without the guard the path is `/scripts/…`. Where a skill is mounted differs between deployments, so no absolute path can be written here.

```
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" build   <files…> [--period 2026-08] --out REPORTDIR --bundle-id draft-1 [--render pdf,docx,xlsx]
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" show    REPORTFILE
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" prose   REPORTFILE --from prose.json --bundle-id draft-2 [--render pdf,docx,xlsx]
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" render  REPORTFILE --to pdf,docx,xlsx --bundle-id formats-1
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" checks  REPORTFILE <files…>
```

**A whole report is one run** — `build … --render pdf,docx,xlsx` — and a
change to the words is one more. Each run
costs the user a wait, so do not spend one where the previous run already
answered: `build` and `prose` both print the report's figures, checks and
notes, and `--render pdf,docx,xlsx` (or `--render all`) writes every format in
one run.

**The `present` argument is the handover.** Every run that writes files the
user should have — `build` or `prose` with `--render`, and `render` — is one
`bash` call whose `present` argument lists those files: the passive view first, then
each render. The raw report remains beside the view as supporting data; do not
present both rich entries for the same draft. The names are known before the run, because the report takes its
name from the last segment of `--out`: `--out …/2026-08-business-review`
with `--bundle-id draft-1` writes
`drafts/draft-1/2026-08-business-review.view.json` inside that directory,
with `2026-08-business-review.report.json` retained as the canonical source,
and `--render pdf,docx,xlsx` writes `2026-08-business-review.pdf`, `.docx`
and `.xlsx` beside it. Choose a unique `--bundle-id` in every writing run:
1 to 80 lowercase letters, digits or hyphens. An existing name is refused.
Without that option the CLI generates an ID and prints the actual paths;
use the explicit option for same-call `present` paths. The tool attaches each listed file the run wrote and
tells you so ("Presented to the user: …"); do not call `present_files` for
those files, and do not list the directory to see that they exist. Name only
the view and its renders: never `checks.json`, `renders.json` or anything
under `charts/` — the report carries what matters in them, and a file named
under `present` is delivered whether or not the user wants it. Never call
`render` once per format, never put renders in the background (`&` returns before the
files exist, and its output never reaches you), and never write your own Python to
find out what a run did — the run that made the draft already printed it.

The view is created after successful formats inside the staged bundle and included in its member hashes. Oversized content becomes a concise preview with an explicit omission notice and complete exports. If view creation fails, valid formats are still published and the script says the preview is unavailable. Keep already-presented exports; do not present them twice. If no file was delivered, use the canonical report and formats printed by the successful run in one `present_files` call. Never claim that a failed publication delivered files. Supporting source, checks and charts remain in the bundle, and automatic runtime delivery is unchanged.

Exit codes: `0` done; `1` a problem the user must hear about (stderr says what), including a withheld report or a period with no rows; `2` a command line the script refused, when stderr names the mistake (correct it and run again; there is nothing to tell the user), otherwise this sandbox is not the image the skill is built for (do not install anything; tell the user); `3` one decision is needed before building (stderr carries the question and the candidates).

## Workflow

### Step 1: Build

Build first. The script reads the file, picks the column roles, settles the
date order and the currency itself, and stops with exit `3` and one question
on stderr when a role is genuinely ambiguous or missing -- and that question
carries the file's columns, so it answers itself. Reading the file before
building buys nothing the build does not print: a run that succeeds prints the
rows, the period, the currency, every table and the columns no role claimed.
A period the user did not name is not a reason to read the file either: leave
`--period` off and the build covers the month holding most of the rows and
says so in its checks, for the user to correct in one sentence.

A period the user did name goes straight to `--period`, without checking the
file for it first. If the files have no rows in it, the build stops before it
writes anything (exit `1`) and its one line on stderr is what a check would
have found: the dates the files cover and the rows in each of the latest
months; tell the user which months the file does cover. If the files cover
only part of it, the build runs and its checks name the months they do not
reach. Either way a check first costs a call and finds nothing more.

That is one `bash` call, and `present` sits beside `command` in it, never
inside the command line (`report.py` refuses `--present`, runs nothing, and
says so):

```json
{"command": "SKILL_DIR=\"<Directory>\"; python \"${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py\" build /mnt/user-data/uploads/export.xlsx --period 2026-08 --out /mnt/user-data/outputs/reports/2026-08-business-review --bundle-id draft-1 --render pdf,docx,xlsx",
 "present": [
  "/mnt/user-data/outputs/reports/2026-08-business-review/drafts/draft-1/2026-08-business-review.view.json",
  "/mnt/user-data/outputs/reports/2026-08-business-review/drafts/draft-1/2026-08-business-review.pdf",
  "/mnt/user-data/outputs/reports/2026-08-business-review/drafts/draft-1/2026-08-business-review.docx",
  "/mnt/user-data/outputs/reports/2026-08-business-review/drafts/draft-1/2026-08-business-review.xlsx"
]}
```

Builds `<name>.report.json` (named after the `--out` directory; `--name` overrides), `<name>.view.json`, `charts/*.png`, `checks.json` and `renders.json` (the bundle identity and hashes of its members) in a new draft directory, `--out/drafts/<bundle-id>/`. Use `/mnt/user-data/outputs/reports/<period>-<slug>/` as the report root. Every requested format must finish before the complete directory becomes visible. A renderer failure or interruption preserves the preceding complete draft. Old bundles are retained unchanged by these commands; removing old directories is an explicit storage cleanup decision that also removes their historical download links. A second build becomes the next draft. Earlier months in the same file, or in extra files passed alongside, feed the comparison with the previous period and the same period last year. Periods: `2026-08`, `2026-Q3`, `2026`, or `2026-08-01..2026-08-15`; leave `--period` off when the user named none. A period with no rows is an error that names the dates the files cover and the rows in each of the latest months, for the user to hear; build another period only when the user picks one.

Options: `--exclude category=Warranty` (repeatable; a role or an exact column name, matched case-insensitively), `--company "Name"`, `--title "..."`, `--currency EUR`, `--short` for a one-sentence summary, `--prefs preferences.json`, `--profile <name>` (a tenant profile from `/mnt/tenant/report-profiles/` wins over the skill's `profiles/`).

The output then prints the report itself — the KPIs, every table, the `Checks:` line, a `Not included:` line and the `Inputs:` line — which is what `show` prints, so Step 2 needs no second run. Repeat the checks to the user in plain words, and the not-included items when there are any (the line says "nothing" when every section is there; do not read that out). Never claim a check the line does not show. If the build exits `1` with "Report withheld", the totals did not reconcile: say so, show the check text, and do not render anything.

`--render pdf,docx,xlsx` renders in the same run and is the normal first report: the build already carries a factual summary and actions computed from the data, so the user has all three files after one run. Leave it off only when you are going to write your own summary in Step 2 in the same turn, because the prose step publishes new text and formats in a separate bundle; a build without `--render` still names the view alone under `present`, with no invented exports.

### Step 2: Write the summary and the actions, then verify them

The build already printed the figures. Write one summary paragraph and up to three actions from those figures only, then hand them to the script together with the formats to render:

```bash
cat > /tmp/prose.json <<'JSON'
{"summary": ["August was the strongest month since March: $52,310.40 across 178 jobs, 6.4% above July."],
 "actions": ["Collect the $4,120.00 still unpaid across 21 jobs.", "Book the two technicians below 20 jobs onto the September installs."]}
JSON
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" prose /mnt/user-data/outputs/reports/2026-08-business-review/drafts/draft-1/2026-08-business-review.report.json --from /tmp/prose.json --render pdf,docx,xlsx --bundle-id draft-2
```

again with `present` naming the view and the three renders under `drafts/draft-2/`, the new paths for this run. That one run makes the next draft without recomputing anything, renders all three formats and prints the same figures-checks-inputs digest the build printed — summary and actions included, as the report now carries them. So there is nothing to look up afterwards and Step 3 is already done. Run `show` only for a report built in an earlier turn, whose figures are no longer in front of you. The script compares every number in your text with the figures in the report (KPIs, tables, checks, the periods named); a sentence with a number that matches none is dropped and the checks line says so. It does not judge the claim around a number, so get the direction words (above, below, up, down) right yourself, and do not cite a figure from a single row, because the check will drop it. If nothing survives, the built text stays and the output says so. Skipping this step is fine: the build already carries a factual summary and actions computed from the data. A rebuild replaces any written text with the computed text; run `prose` again after a rebuild if the text still applies.

### Step 3: Render

Only for a report whose last run did not render — a `build` or `prose` run
without `--render`, or a draft from an earlier turn. One run makes every
format in a new draft directory and copies the report beside them, so in a later turn the report
preview and its new renders are named together under `present`.

```bash
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" render <report.json> --to pdf,docx,xlsx --bundle-id formats-1
```

The new source JSON, view and renders land together in `drafts/formats-1/` under the report root; name the view and the rendered formats under `present`, leaving the source as supporting data. The renders are `<name>.pdf`, `.docx` and `.xlsx`; `--to html` also works but HTML is the sheet the PDF is printed from, not a format the user is offered. Rendering reads only `report.json` and the pictures inside the report directory (and the tenant bundle); it never rebuilds. The DOCX has real headings and tables so the user can edit it; the XLSX has a Summary sheet whose revenue, count and average are live formulas over the Rows sheet, one sheet per table with live `SUM` totals and a live ratio for average columns, and the cleaned rows.

### Step 4: Answer

The files you named under `present` are already with the user: the tool result names them under "Presented to the user". Do not call `present_files` for those and do not run `ls` to check they exist. A `Not attached:` line from the tool names a file it did not deliver and why (it does not exist, or this run did not write it) — if the run printed an error, say what it printed and do not claim a delivery; a `Note:` line from the script names a file that sits beside the report but was not written by it. A result with no "Presented to the user" line handed nothing over: hand over the view (or the canonical report when the preview is unavailable) and the renders the run printed in one `present_files` call, without listing the directory first. In the web workspace `<name>.view.json` shows the skill-formatted figures, charts, checks and explicitly listed downloads. The supporting `<name>.report.json` remains the canonical source. Historical source-only reports use the installed compatibility plugin; on a chat platform the view is an ordinary file. Show the KPI strip, the checks line and any `Not included` items in your reply. Say which file the report used and when it was uploaded (the `Inputs:` line of the digest `build`, `prose` and `show` print). Then ask one short question about what to change, for example whether any rows should be excluded or a note added.

## Changing a report

- A change to the data (exclude rows, another period, another mapping) is a new `build` into the same `--out` directory with `--render pdf,docx,xlsx`; it becomes the next draft with its own renders. Written text from `prose` is replaced by the computed text; run `prose` again if it still applies.
- A change to the words (shorter summary, different actions) is one run: `prose … --render pdf,docx,xlsx --bundle-id <new-id>` with `present` naming the new view and renders in that bundle. Nothing is recomputed. A run without `--render` publishes the new JSON without formats; the previous bundle and its formats remain together. Neighboring files the script did not write stay untouched. Use the current report path printed by the previous successful run: stale sources and outside edits are refused, rather than silently overwriting a newer draft.
- Say what changed and the draft number, for example "Done. Two warranty jobs removed (2 jobs, $0.00). Draft 2."
- Never modify the uploaded file. Never edit `report.json` by hand; the script owns it.

`checks <report.json> [inputs...]` verifies that saved draft against its recorded
input hashes and independently recomputed figures, including tables and cleaned
rows. It also rechecks the numbers in the saved prose. Changed or missing inputs,
altered figures, or unverifiable build choices return exit `1` and a failed
diagnostic `checks.json` at the report root; repeat the printed failure to the user. The command never rewrites
the published bundle, report or renders. Keep the root's `.cache/business-report/` state with its draft directories when moving a report; abandoned private staging directories can be removed when no report command is running. Use an explicit `build` to incorporate new inputs.
New reports retain their effective profile, per-file mappings, comparisons and
customer limit for this check. Older reports without those choices use the
available profile and default preferences; if they cannot be reproduced, rebuild
with the intended choices before claiming they are verified.

## Saving and preferences

The report directory under `/mnt/user-data/outputs/reports/` is what the user downloads or shares; there is no other place to save it in this deployment, so "save" means it is already there, and you say so.

`preferences.json` holds choices the user wants applied to every report: brand colours, exclusions, summary length, comparisons, charts, currency:

```json
{"version": 1,
 "brand": {"primary": "#0a6b3d"},
 "exclusions": [{"role": "category", "equals": "Warranty"}],
 "summary_length": "short",
 "comparisons": ["previous_period", "same_period_last_year"]}
```

Write it at `/mnt/user-data/outputs/reports/preferences.json` only when the user states a lasting preference ("always drop warranty jobs", "use our green from now on"); a one-off request changes only this report. When the preference comes with the request that first reads this file, create `reports/` and write the file in the same `bash` command as the build, ahead of it, since nothing else runs in the sandbox before the build. Pass it to every build in this conversation with `--prefs`; `meta.preferences_applied` in `report.json` lists what applied. The file lives with this conversation's outputs, so tell the user in one sentence that the preference applies to reports in this conversation and that they can download the file and upload it next time to apply it again. Do not promise it will be remembered on its own.

## Branding

When `/mnt/tenant/brand.json` exists the script picks up the company name, logo and colours by itself (`--tenant DIR` points elsewhere). The file looks like `{"company_name": "Example Services Co.", "logo": "logo.png", "colors": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}}`; the logo is a PNG or JPEG next to it. The rules are the workspace header's: a missing or unsupported logo degrades to the name alone, and a name that is blank, longer than 80 characters, or not one plain line is no name (the report is still built). `--company` overrides the name for one report.

## What the checks mean

- `totals_reconcile`: the report total against an independent sum of the amount column and against each table's total. A failed reconciliation withholds the report.
- `rows_used`: rows read, rows outside the period, rows without a usable date, rows excluded. Unusable dates produce a warning, since those rows do not contribute to the report.
- `date_order`: order is inferred from the entire date column. Each source with ambiguous dates (both parts at 12 or below) or conflicting day/month and month/day evidence is named in a warning; the assumed interpretation remains visible. Valid alternative formats are parsed without discarding dates already resolved.
- `duplicate_ids`, `unmapped_rows` (blank person or category, listed as Unassigned or Uncategorized), `unparsed_amounts` (unreadable amounts count as zero; an amount column with no readable value at all is called out), `currency` (stated or assumed), `exclusions`, `external_links` (workbook links are not checked), `prose_numbers` after Step 2.

Every check is labelled "checked by the report script"; that is what it is.

## When an export does not load cleanly

`inspect` is the tool for these. It is not a step on the way to a report:
everything it returns about a file that builds is in what the build prints.

```bash
SKILL_DIR="<Directory>"; python "${SKILL_DIR:?assign SKILL_DIR first, as its own statement}/scripts/report.py" inspect /mnt/user-data/uploads/export.xlsx
```

Per file and sheet it gives the columns with their type and samples, the suggested roles with a confidence, `ambiguous` roles with their candidates, `missing` required roles, the months present, a `period_suggestion` (the busiest month), `date_order`, a `currency` guess and a ready-made `question` covering every open role at once. When `question` is set, ask exactly that, once, even when it names two roles; put every answer in one mapping file, `{"date": "Completed On", "amount": "Invoice Total"}`, and pass `--mapping` to the build. A column you name explicitly displaces any role the script guessed for it; `{"id": null}` clears a role. A workbook sheet is addressed as `export.xlsx::Sheet name`, never by sheet name alone.

- Exit `3` with a sheet question: pass `file.xlsx::Sheet`.
- Exit `3` for a role: its `columns` list is what the file holds. When one of them clearly carries a role the script did not match (a `Treatment` column where the profile expects a service), map it and build again; the section then takes its heading from that column name. Do not ask the user about it, and do not read the file to see it.
- A build that succeeded but named columns under `Unused columns:` read the same way: map one when it clearly holds a role the report is missing.
- A header row that is not the first row needs nothing from you: an export that opens with a company name and a blank line is read from the row that names the columns. "no date and amount column" therefore means the file really has none in the first ten rows -- say what the file does have (`inspect` lists it) and ask which column carries the date and the amount.
- Amounts in a format the script cannot read count as zero and appear in `unparsed_amounts`, which quotes a few of them as the file wrote them: repeat those to the user, because revenue moved. For ordinary amounts, the decimal separator is decided once per column from the values ("1.234,56" reads as European; "1,234.56" as US). Scientific notation accepts an ungrouped mantissa with either decimal separator.
- Conflicting currencies across input files, amount headers/values, or explicit Currency / Currency Code columns stop the build before any totals are written. Supply inputs expressed in one currency. `--currency` assigns a unit to unlabeled amounts; it never converts money or overrides conflicting currency evidence.
- Numeric values in `report.json` and Excel retain source and aggregate precision. Displayed monetary values are rounded to cents; a displayed row sum can differ from the displayed total when source amounts contain fractions of a cent. Excel formulas use the underlying values, so recalculation agrees with their saved results.
- Timestamps that carry a time zone offset are converted to UTC before the period is applied.
- A `.xls` that is an HTML or CSV export in disguise: save it under the right extension in the workspace first.

## Package layout

- [scripts/business_report_publish.py](scripts/business_report_publish.py): stages complete directories, verifies member hashes and serializes current-draft publication with a filesystem lock. Atomic rename protects readers and process interruption; files and directories are flushed before committing the current pointer. A failure after rename may leave a complete unadopted bundle, never a partially published one. Platforms without filesystem locking refuse publication.
- [schemas/report.schema.json](schemas/report.schema.json): the skill-owned version 1 report model; its logical schema identifier and data shape are unchanged.
- [scripts/business_report_view.py](scripts/business_report_view.py): formats passive view v1 from the same Python formatting helpers and authored checks. It calculates no new facts.
- [scripts/report.py](scripts/report.py): the command surface, reading, mapping, metrics, checks and prose; it imports [scripts/business_report_common.py](scripts/business_report_common.py) (constants, formatting, brand) and [scripts/business_report_render.py](scripts/business_report_render.py) (HTML, PDF, DOCX, XLSX, charts) from its own directory.
- [profiles/services-generic.json](profiles/services-generic.json): the default profile (column aliases, vocabulary, status groups, section order). A tenant profile of the same name in `/mnt/tenant/report-profiles/` replaces it.
- [templates/report.html.j2](templates/report.html.j2) and [templates/report.css](templates/report.css): the HTML the PDF is printed from; brand colours arrive as CSS variables.

## Notes

- Fixtures for this skill's tests live in the repository under `backend/tests/skills/business_report/`; the script itself ships no sample data.
- Charts are PNGs drawn locally with matplotlib in the brand colour; nothing about the data leaves the sandbox to draw them. Tables list at most 25 entries and charts at most 12 bars, with the rest folded into one "Other" row or bar whose figures keep the totals exact.
- The summary the build writes is factual and derived from the figures; the model's own summary replaces it only through `prose`, so every number in a render has been compared with the report's figures.
