# Standalone skill result qualification

The runnable [examples](../examples/skills/README.md) exercise the public passive
view contract. They own their calculations, wording and document generation.
Neither imports the report producer or requires a domain branch in the host.

`scripts/docker_acceptance.py` builds the generic production images, then runs
`frontend-hm/tests/e2e-docker-acceptance/skill-results.spec.ts`. The journey starts
with an empty catalog and installs both archives through real owner JWT/CSRF
APIs. Native static and model-assisted scans remain enabled. The synthetic model
returns deterministic fixture decisions and requests actual `read_file`, `bash`
and `present_files` calls. It does not evaluate live-model judgment or commercial
facts. The static scanner flags OOXML namespace `http:` identifiers; those are
inert document metadata and the writers perform no network requests.

The browser verifies the actual tool transcript, source bytes, table totals,
steps, caveats, custom labels, downloaded export hashes, selected-file Save,
Share and Undo. It also exercises malformed-view and plain-PDF ordinary controls,
owner isolation, and an inert Shared archive before explicit recipient installation.
Gateway, graph, files, frontend and authentication use production implementations;
there are no browser API mocks or acceptance bypass routes.

Run each persistence profile with a dedicated disk-backed temporary directory:

```bash
qualification_tmp=$(mktemp -d /tmp/hartmesh-skill-results.XXXXXXXX)
TMPDIR="$qualification_tmp" python3 scripts/docker_acceptance.py --stores both --artifacts "$qualification_tmp/evidence"
```

Stage new source files first. Paired source mode builds once, retains its unique
images through both profiles, verifies the final source/harness hashes, and cleans
up after both runs. Check `pair-result.json` and both profiles' `result.json`
source-tree/harness hashes and inspected/running image IDs. Independent cached
builds can still produce different image IDs; a common Git label alone is insufficient. The browser receipts
record actual presented paths, tool calls and file SHA-256s. CI preserves logs,
screenshots and receipts for both profiles. Source-mode evidence does not satisfy
the separate published-candidate admission policy or publish any images.

Local sandbox bash is explicitly enabled only within the disposable acceptance
Gateway container. This proves functionality. Read-only isolation is separately
checked with the existing AIO image: mount `supplier-comparison/` at
`/mnt/skills/public/supplier-comparison`, a copied `procedure-summary/` package
at `/mnt/skills/custom/private-procedure`, `inputs/` at `/mnt/inputs`, and
`backend/tests/skills/skill_results/` at `/mnt/qualification`, all read-only.
Mount a dedicated writable output directory at `/mnt/user-data/outputs`. Run
`python3 -B /mnt/qualification/check_on_image.py` as runtime user `1000:1000`,
with no network, all capabilities dropped and no new privileges. Record the
actual image ID and confirm host skill bytes remain unchanged afterward.

That check refuses attempted skill writes, runs both portable producers, and
opens their files with independent PDF, DOCX and XLSX readers already in AIO.
It requires no package installation. Projection tests verify complete copies
with distinct inodes; provisioner tests preserve read-only skill category mounts
and writable user data. These contract tests do not claim a live Kubernetes
deployment was tested. No schema or release version changes are required.
