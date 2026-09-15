---
name: business-report
description: Use this skill when the user uploads a tabular business export (CSV, XLSX or XLS of jobs, orders, invoices, appointments or sales) and wants a monthly, quarterly or yearly business review, management report or summary they can download, share or send. Produces one report with KPIs, tables, locally drawn charts and a plain-words checks line, rendered as PDF, Word (DOCX) and Excel (XLSX) with the same numbers in each.
---

# Business Report Skill

## Overview

One script turns an export into a report draft: `report.json` plus PNG charts, then renders that one document to HTML, PDF, DOCX and XLSX. Every number in every render comes from `report.json`, so the formats agree by construction. The script runs on the libraries the sandbox image ships, installs nothing, calls no network service and never modifies an input file.

```
python /mnt/skills/public/business-report/scripts/report.py inspect <files…>
python /mnt/skills/public/business-report/scripts/report.py build   <files…> --period 2026-08 --out DIR
python /mnt/skills/public/business-report/scripts/report.py prose   DIR/<name>.report.json --from prose.json
python /mnt/skills/public/business-report/scripts/report.py render  DIR/<name>.report.json --to pdf|docx|xlsx|html
python /mnt/skills/public/business-report/scripts/report.py checks  DIR/<name>.report.json <files…>
```

Exit codes: `0` done; `1` a problem the user must hear about (stderr says what), including a withheld report; `2` this sandbox is not the image the skill is built for (do not install anything; tell the user); `3` one decision is needed before building (stderr carries the question and the candidates).

## Workflow

### Step 1: Inspect, and ask at most one question

```bash
python /mnt/skills/public/business-report/scripts/report.py inspect /mnt/user-data/uploads/export.xlsx
```

The JSON output gives, per file and sheet: the columns with their type and samples, the suggested column roles (`date`, `amount`, `id`, `customer`, `category`, `person`, `status`, `source`, `quantity`, `location`) with a confidence, `ambiguous` roles with their candidate columns, `missing` required roles, the months present, a `period_suggestion`, a `currency` guess and a ready-made `question` when one is due.

Tell the user in one sentence what you found: rows, date range, the people or categories seen, the currency. Then:

- If `question` is null, do not ask for confirmation; go on to build the suggested period unless the user named a different one.
- If `question` is set, ask exactly that (which column is the date, which is the amount, or which sheet is the export). Put the answer in a mapping file, `{"date": "Completed On"}`, and pass `--mapping` to build. A workbook sheet is addressed as `export.xlsx::Sheet name`, never by sheet name alone.
- Ask about the currency only when the check says it is assumed and the user's context makes another currency likely.

### Step 2: Build

```bash
python /mnt/skills/public/business-report/scripts/report.py build \
  /mnt/user-data/uploads/export.xlsx \
  --period 2026-08 \
  --out /mnt/user-data/outputs/reports/2026-08-business-review
```

Builds `<period>-<title>.report.json`, `charts/*.png` and `checks.json` in `--out`. Use `/mnt/user-data/outputs/reports/<period>-<slug>/` as the directory, one directory per report; a second build into the same directory becomes the next draft. Earlier months in the same file, or in extra files passed alongside, feed the comparison with the previous period and the same period last year. Periods: `2026-08`, `2026-Q3`, `2026`, or `2026-08-01..2026-08-15`.

Options: `--exclude category=Warranty` (repeatable; a role or an exact column name, matched case-insensitively), `--company "Name"`, `--title "..."`, `--currency EUR`, `--short` for a one-sentence summary, `--prefs preferences.json`, `--profile <name>` (a tenant profile from `/mnt/tenant/report-profiles/` wins over the skill's `profiles/`).

The output ends with a `Checks:` line and, when the data cannot support a section, a `Not included:` line. Repeat both to the user in plain words; never claim a check the line does not show. If the build exits `1` with "Report withheld", the totals did not reconcile: say so, show the check text, and do not render anything.

### Step 3: Write the summary and the actions, then verify them

Read `report.json` (KPIs, tables, notes). Write one summary paragraph and up to three actions from those figures only, then hand them to the script, which removes any sentence with a number that is not in the report:

```bash
cat > /tmp/prose.json <<'JSON'
{"summary": ["August was the strongest month since March: $52,310 across 178 jobs, 6.4% above July."],
 "actions": ["Collect the $4,120 still unpaid across 21 jobs.", "Book the two technicians below 20 jobs onto the September installs."]}
JSON
python /mnt/skills/public/business-report/scripts/report.py prose /mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.report.json --from /tmp/prose.json
```

This makes the next draft without recomputing anything. If the output says numbers were removed, tell the user which sentences were dropped and why. Skipping this step is fine: the build already carries a factual summary and actions computed from the data.

### Step 4: Render

```bash
python /mnt/skills/public/business-report/scripts/report.py render <report.json> --to pdf
python /mnt/skills/public/business-report/scripts/report.py render <report.json> --to docx
python /mnt/skills/public/business-report/scripts/report.py render <report.json> --to xlsx
```

Renders land next to the report as `<name>.pdf`, `.docx`, `.xlsx` or `.html`. Rendering reads only `report.json`; it never rebuilds. The DOCX has real headings and tables so the user can edit it; the XLSX has a Summary sheet whose revenue, count and average are live formulas over the Rows sheet, one sheet per table with `SUM` totals, and the cleaned rows.

### Step 5: Present

Show the KPI strip, the checks line and the `Not included` items, then offer the files with `present_files`. Say which file the report used and when it was uploaded (both are in `report.json` under `meta.inputs`). Then ask one short question about what to change, for example whether any rows should be excluded or a note added.

## Changing a report

- A change to the data (exclude rows, another period, another mapping) is a new `build` into the same `--out` directory; it becomes the next draft and the renders must be run again.
- A change to the words (shorter summary, different actions) is `prose`; nothing is recomputed.
- Say what changed and the draft number, for example "Done. Two warranty jobs removed (2 jobs, $0). Draft 2."
- Never modify the uploaded file. Never edit `report.json` by hand; the script owns it.

## Saving and preferences

"Save" means copying the report directory to `/mnt/user-data/files/reports/<period>-<slug>/` when that directory exists on this deployment; otherwise the files stay in `/mnt/user-data/outputs/reports/` and you tell the user that is where they are.

`preferences.json` holds lasting choices: brand colours, exclusions, summary length, comparisons, charts, currency:

```json
{"version": 1,
 "brand": {"primary": "#0a6b3d"},
 "exclusions": [{"role": "category", "equals": "Warranty"}],
 "summary_length": "short",
 "comparisons": ["previous_period", "same_period_last_year"]}
```

Write it only when the user states a lasting preference ("always drop warranty jobs", "use our green from now on"); a one-off request changes only this report. When you write it, say so in one sentence. It lives at `/mnt/user-data/files/reports/preferences.json` when that directory exists, otherwise at `/mnt/user-data/outputs/reports/preferences.json`. Pass it to every build with `--prefs`; `meta.preferences_applied` in `report.json` lists what applied.

## Branding

When `/mnt/tenant/brand.json` exists the script picks up the company name, logo and colours by itself (`--tenant DIR` points elsewhere). A missing logo degrades to the name alone. `--company` overrides the name for one report.

## What the checks mean

- `totals_reconcile`: the report total against an independent sum of the amount column and against each table's total. The only check that withholds the report.
- `rows_used`: rows read, rows outside the period, rows without a usable date, rows excluded.
- `duplicate_ids`, `unmapped_rows` (blank person or category, listed as Unassigned or Uncategorized), `unparsed_amounts` (count as zero), `currency` (stated or assumed), `exclusions`, `external_links` (workbook links are not checked), `prose_numbers` after Step 3.

Every check is labelled "checked by the report script"; that is what it is.

## When an export does not load cleanly

- Exit `3` with a sheet question: pass `file.xlsx::Sheet`.
- "no date and amount column": the header row is not the first row (`Unnamed: N` columns are the sign). Read the sheet with pandas using `header=<row>`, write a CSV into `/mnt/user-data/workspace/`, and build from that; say so.
- Amounts in a format the script cannot read count as zero and appear in `unparsed_amounts`; look at the samples in `inspect` and tell the user which values were unreadable.
- A `.xls` that is an HTML or CSV export in disguise: save it under the right extension in the workspace first.

## Package layout

- [scripts/report.py](scripts/report.py): the whole command surface; no other script is needed.
- [profiles/services-generic.json](profiles/services-generic.json): the default profile (column aliases, vocabulary, status groups, section order). A tenant profile of the same name in `/mnt/tenant/report-profiles/` replaces it.
- [templates/report.html.j2](templates/report.html.j2) and [templates/report.css](templates/report.css): the HTML the PDF is printed from; brand colours arrive as CSS variables.

## Notes

- Fixtures for this skill's tests live in the repository under `backend/tests/skills/business_report/`; the script itself ships no sample data.
- Charts are PNGs drawn locally with matplotlib in the brand colour; nothing about the data leaves the sandbox to draw them.
- The summary the build writes is factual and derived from the figures; the model's own summary replaces it only through `prose`, so no unverified number reaches a render.
