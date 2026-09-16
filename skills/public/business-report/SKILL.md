---
name: business-report
description: Use this skill when the user uploads a tabular business export (CSV, XLSX or XLS of jobs, orders, invoices, appointments or sales) and wants a monthly, quarterly or yearly business review, management report or summary they can download, share or send. Produces one report with KPIs, tables, locally drawn charts and a plain-words checks line, rendered as PDF, Word (DOCX) and Excel (XLSX) with the same numbers in each.
---

# Business Report Skill

## Overview

One script turns an export into a report draft: `report.json` plus PNG charts, then renders that one document to HTML, PDF, DOCX and XLSX. Every number in every render comes from `report.json`, so the formats agree by construction. The script runs on the libraries the sandbox image ships, installs nothing, calls no network service (the PDF printer refuses every URL that is not inline data) and never modifies an input file.

**Script paths.** `$SKILL_DIR` is this skill's own directory — the one holding this `SKILL.md`, which `describe_skill` reports as `Directory` (`Location` is the file inside it). Set `SKILL_DIR` to that directory at the start of each command that runs one of these scripts. Where a skill is mounted differs between deployments, so no absolute path can be written here.

```
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" inspect <files…>
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" build   <files…> --period 2026-08 --out DIR [--render pdf,docx,xlsx]
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" show    DIR/<name>.report.json
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" prose   DIR/<name>.report.json --from prose.json [--render pdf,docx,xlsx]
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" render  DIR/<name>.report.json --to pdf,docx,xlsx
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" checks  DIR/<name>.report.json <files…>
```

**A whole report is three runs**, and a change to the words is one. Each run
costs the user a wait, so do not spend one where the previous run already
answered: `build` and `prose` both print the report's figures, checks and
notes; `--render pdf,docx,xlsx` (or `--render all`) writes every format in one
run; and `build`, `prose` and `render` each end with a `Present:` line
followed by the files to hand over, one absolute path per line, the report
first. Never call `render` once
per format, never put renders in the background (`&` drops the shell variables
the next command needs), and never write your own Python to find out what a run
did — the run that made the draft already printed it.

Exit codes: `0` done; `1` a problem the user must hear about (stderr says what), including a withheld report or a period with no rows; `2` this sandbox is not the image the skill is built for (do not install anything; tell the user); `3` one decision is needed before building (stderr carries the question and the candidates).

## Workflow

### Step 1: Inspect, and ask at most one question

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" inspect /mnt/user-data/uploads/export.xlsx
```

The JSON output gives, per file and sheet: the columns with their type and samples, the suggested column roles (`date`, `amount`, `id`, `customer`, `category`, `person`, `status`, `source`, `quantity`, `location`) with a confidence, `ambiguous` roles with their candidate columns, `missing` required roles, the months present, a `period_suggestion`, `date_order` (how day and month were read), a `currency` guess and a ready-made `question` that covers every open role at once.

Tell the user in one sentence what you found: rows, date range, the people or categories seen, the currency. Then:

- If `question` is null, do not ask for confirmation; go on to build the suggested period unless the user named a different one.
- If `question` is set, ask exactly that, once, even when it names two roles. Put every answer in one mapping file, `{"date": "Completed On", "amount": "Invoice Total"}`, and pass `--mapping` to build. A column you name explicitly displaces any role the script had guessed for it; `{"id": null}` clears a role. A workbook sheet is addressed as `export.xlsx::Sheet name`, never by sheet name alone.
- If `inspect` shows a column that clearly holds a role the script did not match (a `Treatment` column when the profile expects `Service`), map it the same way; the section then takes its heading from that column name. Do not ask about it.
- Ask about the currency only when the check says it is assumed and the user's context makes another currency likely. Ask about the date order only when the `date_order` check appears and the user's context makes day/month likely.

### Step 2: Build

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" build \
  /mnt/user-data/uploads/export.xlsx \
  --period 2026-08 \
  --out /mnt/user-data/outputs/reports/2026-08-business-review
```

Builds `<period>-<title>.report.json`, `charts/*.png`, `checks.json` and `renders.json` (which renders belong to this draft) in `--out`. Use `/mnt/user-data/outputs/reports/<period>-<slug>/` as the directory, one directory per report; a second build into the same directory becomes the next draft, removes its own renders of the previous draft (they no longer match) and says so. Earlier months in the same file, or in extra files passed alongside, feed the comparison with the previous period and the same period last year. Periods: `2026-08`, `2026-Q3`, `2026`, or `2026-08-01..2026-08-15`. A period with no rows is an error that names the dates the files do cover; build a period the files have.

Options: `--exclude category=Warranty` (repeatable; a role or an exact column name, matched case-insensitively), `--company "Name"`, `--title "..."`, `--currency EUR`, `--short` for a one-sentence summary, `--prefs preferences.json`, `--profile <name>` (a tenant profile from `/mnt/tenant/report-profiles/` wins over the skill's `profiles/`).

The output then prints the report itself — the KPIs, every table, the `Checks:` line, a `Not included:` line and the `Inputs:` line — which is what `show` prints, so Step 3 needs no second run. Repeat the checks to the user in plain words, and the not-included items when there are any (the line says "nothing" when every section is there; do not read that out). Never claim a check the line does not show. If the build exits `1` with "Report withheld", the totals did not reconcile: say so, show the check text, and do not render anything.

`--render pdf,docx,xlsx` renders in the same run. Use it when you are not writing your own summary; otherwise leave it off here and render from `prose` in Step 3, because the prose step replaces the text and every render made before it.

### Step 3: Write the summary and the actions, then verify them

The build already printed the figures. Write one summary paragraph and up to three actions from those figures only, then hand them to the script together with the formats to render:

```bash
cat > /tmp/prose.json <<'JSON'
{"summary": ["August was the strongest month since March: $52,310.40 across 178 jobs, 6.4% above July."],
 "actions": ["Collect the $4,120.00 still unpaid across 21 jobs.", "Book the two technicians below 20 jobs onto the September installs."]}
JSON
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" prose /mnt/user-data/outputs/reports/2026-08-business-review/2026-08-business-review.report.json --from /tmp/prose.json --render pdf,docx,xlsx
```

That one run makes the next draft without recomputing anything, renders all three formats, prints the same figures-checks-inputs digest the build printed — summary and actions included, as the report now carries them — and ends with the `Present:` line. So there is nothing to look up afterwards and Step 4 is already done. Run `show` only for a report built in an earlier turn, whose figures are no longer in front of you. The script compares every number in your text with the figures in the report (KPIs, tables, checks, the periods named); a sentence with a number that matches none is dropped and the checks line says so. It does not judge the claim around a number, so get the direction words (above, below, up, down) right yourself, and do not cite a figure from a single row, because the check will drop it. If nothing survives, the built text stays and the output says so. Skipping this step is fine: the build already carries a factual summary and actions computed from the data. A rebuild replaces any written text with the computed text; run `prose` again after a rebuild if the text still applies.

### Step 4: Render

Only for a report whose last run did not render — a `build` or `prose` run
without `--render`, or a draft from an earlier turn. One run makes every
format:

```bash
python "${SKILL_DIR:?set it to this skill's directory}/scripts/report.py" render <report.json> --to pdf,docx,xlsx
```

Renders land next to the report as `<name>.pdf`, `.docx` and `.xlsx`; `--to html` also works but HTML is the sheet the PDF is printed from, not a format the user is offered. Rendering reads only `report.json` and the pictures inside the report directory (and the tenant bundle); it never rebuilds. The DOCX has real headings and tables so the user can edit it; the XLSX has a Summary sheet whose revenue, count and average are live formulas over the Rows sheet, one sheet per table with live `SUM` totals and a live ratio for average columns, and the cleaned rows.

### Step 5: Present

Show the KPI strip, the checks line and any `Not included` items, then offer the files with `present_files` in one call — exactly the paths listed under the last run's `Present:` line, in that order, one per line, `<name>.report.json` first and the renders after it. A `Not presented:` line means a file beside the report was not written by it; leave it out and say so if it matters. In the web workspace that file is drawn as the report itself — the figures, the charts, the checks line and a download button for the PDF, Word and Excel renders listed with it — so leaving it out reduces the report to a list of attachments; on a chat platform it is simply one more file. Do not present `checks.json`, `renders.json` or anything under `charts/`; the report carries what matters in them. Say which file the report used and when it was uploaded (the `Inputs:` line of the digest `build`, `prose` and `show` print). Then ask one short question about what to change, for example whether any rows should be excluded or a note added.

## Changing a report

- A change to the data (exclude rows, another period, another mapping) is a new `build` into the same `--out` directory with `--render pdf,docx,xlsx`; it becomes the next draft and replaces the renders of the previous one. Written text from `prose` is replaced by the computed text; run `prose` again if it still applies.
- A change to the words (shorter summary, different actions) is one run: `prose … --render pdf,docx,xlsx`. Nothing is recomputed, the three formats are rewritten, and the `Present:` line names what to hand over. A `prose` run without `--render` deletes the renders it made earlier, because they still say what the previous draft said; a file it did not write is left alone and reported on a `Not presented:` line instead.
- Say what changed and the draft number, for example "Done. Two warranty jobs removed (2 jobs, $0.00). Draft 2."
- Never modify the uploaded file. Never edit `report.json` by hand; the script owns it.

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

Write it at `/mnt/user-data/outputs/reports/preferences.json` only when the user states a lasting preference ("always drop warranty jobs", "use our green from now on"); a one-off request changes only this report. Pass it to every build in this conversation with `--prefs`; `meta.preferences_applied` in `report.json` lists what applied. The file lives with this conversation's outputs, so tell the user in one sentence that the preference applies to reports in this conversation and that they can download the file and upload it next time to apply it again. Do not promise it will be remembered on its own.

## Branding

When `/mnt/tenant/brand.json` exists the script picks up the company name, logo and colours by itself (`--tenant DIR` points elsewhere). The file looks like `{"company_name": "Example Services Co.", "logo": "logo.png", "colors": {"primary": "#0a6b3d", "secondary": "#9ccdb4"}}`; the logo is a PNG or JPEG next to it, and a missing or unsupported logo degrades to the name alone. `--company` overrides the name for one report.

## What the checks mean

- `totals_reconcile`: the report total against an independent sum of the amount column and against each table's total. The only check that withholds the report.
- `rows_used`: rows read, rows outside the period, rows without a usable date, rows excluded.
- `date_order`: only when day and month could not be told apart (every value has both parts at 12 or below); says which order was assumed.
- `duplicate_ids`, `unmapped_rows` (blank person or category, listed as Unassigned or Uncategorized), `unparsed_amounts` (unreadable amounts count as zero; an amount column with no readable value at all is called out), `currency` (stated or assumed), `exclusions`, `external_links` (workbook links are not checked), `prose_numbers` after Step 3.

Every check is labelled "checked by the report script"; that is what it is.

## When an export does not load cleanly

- Exit `3` with a sheet question: pass `file.xlsx::Sheet`.
- "no date and amount column": the header row is not the first row (`Unnamed: N` columns are the sign). Read the sheet with pandas using `header=<row>`, write a CSV into `/mnt/user-data/workspace/`, and build from that; say so.
- Amounts in a format the script cannot read count as zero and appear in `unparsed_amounts`; look at the samples in `inspect` and tell the user which values were unreadable. The decimal separator is decided once per column from the values ("1.234,56" reads as European; "1,234.56" as US).
- Timestamps that carry a time zone offset are converted to UTC before the period is applied.
- A `.xls` that is an HTML or CSV export in disguise: save it under the right extension in the workspace first.

## Package layout

- [scripts/report.py](scripts/report.py): the command surface, reading, mapping, metrics, checks and prose; it imports [scripts/business_report_common.py](scripts/business_report_common.py) (constants, formatting, brand) and [scripts/business_report_render.py](scripts/business_report_render.py) (HTML, PDF, DOCX, XLSX, charts) from its own directory.
- [profiles/services-generic.json](profiles/services-generic.json): the default profile (column aliases, vocabulary, status groups, section order). A tenant profile of the same name in `/mnt/tenant/report-profiles/` replaces it.
- [templates/report.html.j2](templates/report.html.j2) and [templates/report.css](templates/report.css): the HTML the PDF is printed from; brand colours arrive as CSS variables.

## Notes

- Fixtures for this skill's tests live in the repository under `backend/tests/skills/business_report/`; the script itself ships no sample data.
- Charts are PNGs drawn locally with matplotlib in the brand colour; nothing about the data leaves the sandbox to draw them. Tables list at most 25 entries and charts at most 12 bars, with the rest folded into one "Other" row or bar whose figures keep the totals exact.
- The summary the build writes is factual and derived from the figures; the model's own summary replaces it only through `prose`, so every number in a render has been compared with the report's figures.
