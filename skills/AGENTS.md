# AGENTS.md — skills

Agent skills. `public/` is committed and reviewed; `custom/` is gitignored.
Managed integration packs live outside this tree at
`.deer-flow/integrations/skills/{provider}/`.

## A command surface is a latency budget

A skill's `SKILL.md` is read by a model that pays a full round trip for every
command it runs, and the user waits through each one. The tenant-class `.17`
trace spent 20 calls on one report and 13 on a one-sentence revision; the
avoidable ones were commands the doc asked for twice over, and probes for
answers an earlier run had computed but not printed. So:

- **One intention, one run.** A step must not exist to read back what the
  previous step already knew. `build` prints the figures `show` prints;
  `build` and `prose` take `--render pdf,docx,xlsx` and write every format in
  one process; `prose` prints the text it just wrote.
- **A run that produces files names them.** End with a `Present:` line and the
  paths under it, one per line — a joined line cannot be split back apart when
  a directory the caller chose has a space in it. The old doc did state the
  rule in prose and the model still dropped the `report.json` the workspace
  draws the report from: a rule to apply is not a list to copy.
- **Never document a command per output.** Three render lines in a doc become
  three calls, or one backgrounded line whose `&` drops the shell variables the
  next command needs. Both happened.
- **A step that invalidates its outputs must remove them — and only its own.**
  `prose` bumps the draft, so the renders beside it no longer agree with
  `report.json`; it deletes them unless it replaces them. What it may delete is
  what the skill recorded writing (`renders.json`), never what the file name
  suggests: the same directory can hold a bundle the user re-uploaded, and
  their files are the one thing in a thread that cannot be regenerated. Write
  the replacements first, then remove the leftovers, so a failure mid-run
  destroys nothing.

- **Nothing read from user data may start a line.** A spreadsheet cell reaches
  the digest, the digest reaches the model, and the model is told to act on
  whole lines of it. Strip control characters before printing, or a cell forges
  any line the doc teaches.

## Paths, review and tests

- No absolute skill path in any skill text: address scripts through
  `${SKILL_DIR:?…}`, which `describe_skill` reports as each skill's
  `Directory`. The rule, why the guarded form specifically, and the test that
  pins it are in
  [the harness skills guide](../backend/packages/harness/deerflow/skills/AGENTS.md).
- Changed public skills are reviewed in CI by
  `scripts/review_changed_public_skills.py`; waivers live in
  `.github/skill-review-waivers.v1.json` and never waive blockers.
- Tests for a public skill's scripts live in `backend/tests/skills/<name>/` so
  `make test` and the CI shards run them; see
  [backend/AGENTS.md](../backend/AGENTS.md). The sandbox smoke workflow then
  runs the same commands on the real image, so keep the workflow's invocation
  identical to the one `SKILL.md` asks for.
