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
  one process; `prose` prints the text it just wrote; `build` asks the one
  question `inspect` would have surfaced, so the doc sends the model to
  `build` first when the period is known.
- **The call that makes the files hands them over.** A run that writes
  deliverables is one `bash` call whose `present` argument names them; the
  tool attaches each named file the call wrote (inside this conversation's
  outputs, a regular file, modified after the call started) and the result
  says so, so the model has no second call to make and nothing to copy from
  the output. For that to work the paths must be knowable before the run:
  name outputs deterministically from what the caller chose (the report skill
  names the report after its `--out` directory) and document the names in
  `SKILL.md`. Nothing is parsed from a script's output — a script prints for
  the reader, and no line of it is a contract. The `.18` tenant-class traces
  are why: the model dropped `report.json` from its own `present_files` call
  on both fresh reports, and made no call at all on either revision, having
  re-run `prose --render pdf,docx,xlsx` over the same paths as the turn
  before. A rule to apply is not a list to copy, and a list to copy is still
  a judgement.
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
