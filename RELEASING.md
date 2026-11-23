# Releasing HartMesh

Use the HartMesh procedure below for this repository. The
[pinned upstream release guide](https://github.com/bytedance/deer-flow/blob/67db3d883c38264e2a188d9aaad44f7a7b55015d/RELEASING.md)
describes DeerFlow's separate publishing workflows.

## HartMesh distribution releases

HartMesh is a distribution of DeerFlow. A release ships the single-VM profile
under `deploy/compose/` and five
container images, including the separately deployed provisioner. The Helm
chart in this tree is upstream's, unchanged and not qualified against this
build; a release tag here does not publish it.

### Version

A HartMesh release is `X.Y.Z+hartmesh.N`: `X.Y.Z` is the upstream version the
build is based on and `N` increases with every release. These five sources must
agree:

| Source | Version field |
| --- | --- |
| `backend/pyproject.toml` | Root project `version` |
| `backend/packages/harness/pyproject.toml` | `deerflow-harness` `version` |
| `backend/uv.lock` | Root `deer-flow` package version (uv's normalized spelling) |
| `frontend-hm/package.json` | `version` |
| `deploy/helm/deer-flow/Chart.yaml` | Both `version` and `appVersion` |

The pinned upstream `frontend/package.json` is excluded from HartMesh version
bumps. `bash scripts/bump_version.sh <version>` updates the product sources and
refreshes the lockfile; `bash scripts/verify_versions.sh <version>` checks all
five. The publishing workflows stop when a source disagrees.

For `2.1.0+hartmesh.1` the spellings are: git tag `v2.1.0+hartmesh.1`,
container image tag `v2.1.0-hartmesh.1`, and `sha-<first seven characters of
the commit>` for lookup by commit. `scripts/release_tag_spellings.sh` is the
one implementation; workflows call it. The images are
`ghcr.io/<owner>/<repo>-backend`, `-frontend`, `-provisioner`, `-sandbox` and
`-sandbox-network-proxy`. `ghcr.io/<owner>/<repo>-sandbox-base` is a
cache of the sandbox's upstream base image, not something to deploy.

### Procedure

The compose profile must reference its images by digest in the tagged tree,
so a release builds its images before the tag exists.

Every release's `CHANGELOG.md` entry must include a `### Schema changes`
section comparing its database schema with the previous HartMesh release.
Describe added, changed or removed tables and columns, migration revisions,
and the upgrade behavior or required operator action. If the database schema
is unchanged, state explicitly: "No database schema changes since v<previous
release>." Changes to a manifest or configuration format should be described
separately from the database schema.

Before committing the release version, rename `## [Unreleased]` to the chosen
version, keep its schema section, and start a new Unreleased section. Preview
the notes with `python3 scripts/release_notes.py <version>`. The manifest
workflow requires a nonempty schema section and publishes that release's
changelog entry as the GitHub Release notes; rerunning it updates those notes
from the same tagged source.

1. **Choose the version** and write it to every source:
   ```bash
   bash scripts/bump_version.sh 2.1.0+hartmesh.1
   bash scripts/verify_versions.sh 2.1.0+hartmesh.1
   ```
2. **Commit and push** that change (a branch is fine; the candidate build
   reads the ref it is dispatched on).
3. **Build the candidate images** from that commit:
   ```bash
   gh workflow run container.yaml --repo altakleos/hartmesh --ref <that branch> -f version=2.1.0+hartmesh.1
   ```
   A dispatch builds all five images under the release's tag spelling and
   never reuses a pinned digest, whatever `deploy/compose/images.txt` carries.
   It refuses a version whose Git tag or GitHub Release already exists;
   lookup failures also stop publication. Use the canonical `X.Y.Z+hartmesh.N`
   spelling without a leading `v`. Publication is serialized per version.
   Wait for every job to succeed.
4. **Pin the compose profile** to the digests that build published, and
   commit the pins:
   ```bash
   scripts/pin_compose_images.py --release 2.1.0+hartmesh.1
   scripts/pin_compose_images.py --check
   git add deploy/compose
   git commit -m "release: pin compose profile for v2.1.0+hartmesh.1"
   ```
   `--release` first points the four profile lines for this repository's
   images at this release's candidate tags and then resolves every tag to a
   digest, in `deploy/compose/images.txt`, `compose.yaml` and `config.yaml`
   together. Without it the script refuses while any of those lines is a tag:
   between releases the tree carries the previous release's pins, and pinning
   them again would ship the previous release under the new name.
5. **Test the exact candidate application images**, after pushing the pin
   commit to that branch:
   ```bash
   gh workflow run docker-acceptance.yml --repo altakleos/hartmesh --ref <that branch> \
     -f backend_image=ghcr.io/altakleos/hartmesh-backend@sha256:<backend-digest> \
     -f frontend_image=ghcr.io/altakleos/hartmesh-frontend@sha256:<frontend-digest>
   ```
   Wait for the whole run to complete successfully, including both SQLite and
   PostgreSQL/Redis. With `crane` and authenticated `gh` available, provide
   `GH_TOKEN` with repository Actions, packages and attestations read access,
   then record that run:
   ```bash
   python3 scripts/release_artifacts.py record-acceptance \
     --repository altakleos/hartmesh --version 2.1.0+hartmesh.1 --run-id <run-id>
   git add deploy/compose/acceptance.json
   git commit -m "release: record candidate image acceptance"
   git push origin <that branch>
   ```
   Record creation verifies all five candidates and committed Compose pins,
   then authenticates the completed run and both attempt-specific result
   artifacts through GitHub. Each result must show the exact backend/frontend
   digests, matching inspected/running image IDs, the same workflow/harness
   inputs, and the fixture declarations and identities. The record (schema 1)
   binds the version, run, attempt and both archive SHA-256 values. An arbitrary
   local `result.json` or an uncommitted record cannot admit a release.

   If either store fails, fix the cause and rerun **all jobs**, or dispatch a
   new complete run. A partial-job rerun cannot combine evidence from different
   attempts. Regenerate and commit the record before tagging. Changes to the
   workflow, runner, browser test directory, fixtures or test dependency controls
   require fresh acceptance; changes to image build inputs also require fresh
   candidates and pins.
   Tenant-VM qualification belongs to the separate infra/consumer workflow;
   it is not a prerequisite for cutting a HartMesh release and is never performed
   by the release session. Automated image acceptance above remains mandatory.
   If a consumer separately supplies tenant-class results for the pinned bundle,
   preserve the
   native and complete 27-case bundle JUnit reports, the measured guest facts,
   the eight manually verified steps and six timings, and the bundle's image
   list. An optional `tenant_class` receipt can be recorded with:
   ```bash
   python3 scripts/tenant_class_receipt.py \
     --record deploy/compose/acceptance.json --version 2.1.0+hartmesh.1 \
     --native-junit <native.xml> --acceptance-junit <acceptance.xml> \
     --steps <steps.json> --guest <guest.json> --images deploy/compose/images.txt
   git add deploy/compose/acceptance.json
   git commit -m "release: record tenant-class qualification"
   ```
   The receipt tool derives runtime and mount-propagation evidence from the
   two required acceptance cases, refuses skipped or incomplete reports and
   preserves the five authenticated image-acceptance fields. Its ten checks
   comprise eight manually verified steps and those two derived results. Required
   CI also runs all 27 bundle cases, but its scripted model and runc alias cannot
   attest the tenant VM class or gVisor. Release admission permits the receipt to
   be absent and strictly validates it when supplied; never generate a human
   qualification claim from CI results.
   Refreshing image admission replaces its five fields and removes an older
   tenant receipt. A consumer must qualify the new pinned bundle separately
   before a replacement tenant receipt can be attached.

6. **Tag and push** the commit containing the verified record:
   ```bash
   git tag v2.1.0+hartmesh.1
   git push origin v2.1.0+hartmesh.1
   ```
   Before any image is re-tagged, the container workflow verifies all five
   candidates, including the provisioner. Each immutable digest must carry
   verified build provenance for this repository's container workflow and a
   source revision whose tracked build inputs match the tagged source. The
   candidate's recorded input fingerprint must match too. The four Compose
   pins must resolve to those candidate digests. The same preflight re-verifies
   the committed acceptance record, authenticated run, artifact hashes and
   exact-image results before publishing aliases for the release commit. Missing, failed,
   expired or mismatched evidence stops publication; there is no rebuild fallback.

   Candidates built before these guards lack the required input evidence and
   cannot be adopted. Build new candidates under an unused version. Once a
   version has a Git tag or GitHub Release, source corrections require a new
   version rather than rebuilding or moving the existing release tag. An
   unchanged tag's publication can be retried after a transient failure while
   its acceptance artifacts remain available. Candidate result artifacts request
   90-day retention, subject to repository retention policy and earlier deletion.
   A tag is immutable: if its artifacts expire or its recorded run is superseded
   by another attempt, an incomplete release needs a new version and verified
   record. Refresh the record before tagging; never move a tag to refresh it.

   Root release notes and Compose pins may change after the candidate build.
   Changes to image source trees, shipped skills, Dockerfiles, or build controls
   require another candidate build before tagging. Build-input checks include
   documentation inside those source trees conservatively. Adoption preserves
   the candidate bytes and their original build revision, and adds the final
   commit's `sha-` lookup tag.
7. **Record the release**, once the five image jobs have succeeded:
   ```bash
   gh workflow run release-manifest.yaml --repo altakleos/hartmesh -f version=2.1.0+hartmesh.1
   ```
   It checks out the tag, resolves each image, checks every line of
   `deploy/compose/images.txt` against what was published, repeats the candidate
   provenance, source and acceptance checks, and requires both release and
   final-commit image tags to resolve to those verified digests. It attaches
   `release-manifest.json` and the separate `release-acceptance.json` record
   to the GitHub Release. The manifest retains schema 4 and
   lists the five images by repository, tag and digest, and the Compose
   profile's `images.txt` with its SHA-256. Verify a downloaded copy offline:
   ```bash
   python3 scripts/verify_release_manifest.py release-manifest.json
   ```

A package GHCR creates is private until its visibility is changed in the
package's settings; do that once for each of the five images.

### Docker acceptance

With Docker, the frontend test dependencies, and Playwright Chromium already
available, run from the repository root:

```bash
python3 scripts/docker_acceptance.py --artifacts /tmp/hartmesh-docker-acceptance
```

To test already-built immutable application images with disposable PostgreSQL
and Redis, use the same journey without rebuilding either application image:

```bash
python3 scripts/docker_acceptance.py \
  --backend-image ghcr.io/altakleos/hartmesh-backend@sha256:<backend-digest> \
  --frontend-image ghcr.io/altakleos/hartmesh-frontend@sha256:<frontend-digest> \
  --stores postgres-redis --artifacts /tmp/hartmesh-image-acceptance
```

Both image arguments are required together and must contain full SHA-256
digests. Missing images are pulled by those digests. Omitting them retains
source builds; `--stores` also works with that default. SQLite remains the CLI
default. CI runs both store modes; workflow dispatch accepts the same optional
image pair. Use a new artifact directory for each run.

The runner builds the production backend and `frontend-hm` Dockerfiles from
tracked working-tree files. It starts a uniquely named Compose project with
nginx, the Gateway, the frontend, and a scripted OpenAI-compatible model on an
internal network. Nginx also joins a separate ingress network and is the only
published service, on a dynamically assigned loopback port. Configuration and
accounts are synthetic; sandbox tools run inside the disposable Gateway
container.

The browser journey covers registration, rejected and successful sign-in,
upload, streamed tools, downloads, Files and Shared, persisted history, account
export/download/discard, model errors, confirmed deletion and logout against
real application services. Logs, browser failure evidence and `result.json` go
to the selected directory. The summary records the store mode, harness commit
and harness fingerprint, inspected/running application image IDs/digests, actual
fixture image identities and configured references. Results use schema 2 and
record workflow/run/attempt context when executed in GitHub Actions. Each
application container's image ID must match its
recorded image. OCI source revision labels are recorded declarations; this runner
does not verify build attestations. Source mode separately records the build
commit and tracked working-tree fingerprint; image mode never labels the harness
commit as the supplied image's source.

Cleanup removes only this run's containers, volumes, networks and source-built
image tags, including on failed setup or interruption. Supplied images and shared
fixture images are preserved. `.github/workflows/docker-acceptance.yml` runs both
store modes in CI.

This exercises disposable application/store integration with local sandbox
execution. It does not qualify a full deployment profile, real model quality,
AIO/gVisor isolation, OIDC, VM infrastructure or Kubernetes; those remain separate
consumer qualification. It creates no VM image.

### Preview measurements on candidate images

For a release changing installed previews, dispatch the following workflow after
the candidate image build, while the branch still points at the version commit
used for that build. Pass the two immutable digests from the successful build.

```bash
gh workflow run preview-measurement.yml --repo altakleos/hartmesh --ref <candidate-branch> \
  -f backend_image=ghcr.io/altakleos/hartmesh-backend@sha256:<backend-digest> \
  -f frontend_image=ghcr.io/altakleos/hartmesh-frontend@sha256:<frontend-digest>
```

The workflow verifies the signed build provenance, release version and build
inputs of both digests. It matches their source revisions to its checkout and
their IDs to the running containers, then exercises signed-in previews on the
disposable Docker stack under the distribution Gateway limits. It retains
the per-trial timings, Gateway memory and privilege observations, rendered
Office fixtures and original download hashes, browser output, startup logs and
`preview-result.json` in the attempt's measurement artifact. Check the
observation file even when a browser check fails. Record the run, attempt,
commit, digests, measured values and any outstanding criteria in the bundle
README before choosing which presentations to enable. A missing criterion
keeps a new presentation disabled by default. The ordinary Docker acceptance
workflow still qualifies both store profiles after pinning.

Rebuild and remeasure if an image input changes. Repeat the measurement if
its harness changes. A bundle-only evidence or presentation configuration
commit can reuse the measured images when all image and harness inputs match.

### Employee conversation sharing

When a release changes conversation sharing, run the mounted snapshot qualification
on the release branch after committing the signed image pins. The workflow verifies
both pinned product images and signs in through the public browser. It also starts
the earlier Gateway against the same store to check that it can read an employee
snapshot and recover an interrupted publication.

```sh
gh workflow run compose-instances.yml --repo altakleos/hartmesh --ref <release-branch> \
  -f backend_image=<backend-repository>@sha256:<digest> -f verify_shared_snapshots=true
```

Check both `snapshot-publications.json` and `agent-sharing.json` in the run’s
artifact. The latter records the browser’s first employee run, the copied file,
and the outcome observed by the earlier reader.

### Employee conversation jobs

For changes to employee jobs, dispatch `compose-instances.yml` with
`verify_agent_jobs=true` and `connections=skip`. Leave the backend image
unset for a source rehearsal. For release qualification, supply the signed
backend digest on the release pin commit. The workflow verifies both pinned
application images and enables batches on its disposable installation.

Check `employee-jobs.xml` and `employee-jobs.json` for overlapping employee
and personal checks, a third person's completed chat, Gateway memory, stored
token counts, and final-turn connection calls. Check the interrupted final
reply, the replacement run's newly granted connection, and the switched-off
control. The scripted provider reports fixed token usage. Tenant performance
and provider pricing require separate measurements.

### Conversation title search measurements

Pull requests changing the title search measurement or its search handler build
the production images from tracked source and exercise a disposable PostgreSQL
stack. For release delivery, dispatch the same journey on the candidate
build commit after signed image verification is available:

```sh
gh workflow run title-search-measurement.yml --repo altakleos/hartmesh --ref <candidate-branch> \
  -f backend_image=<backend-repository>@sha256:<digest> \
  -f frontend_image=<frontend-repository>@sha256:<digest>
```

The signed-in reader has 2,000 conversations. The browser opens an older
matching title, and the runner measures 60 search requests through nginx after
warmup. Record the nearest-rank p95, the 300 ms limit, the actual container
limits, both running image digests, the source commit and workflow attempt
from `title-search-result.json` and `title-search-observations.json`. A miss
requires fixing and remeasuring the title path before the release. These
measurements use hosted runner hardware. Continue normal two-store
qualification after pinning the measured images.

### Message search measurement

Run the dedicated hosted workflow on the release candidate build commit before
enabling the message-search package in the distributed configuration.

```sh
gh workflow run message-search-measurement.yml --repo altakleos/hartmesh --ref <candidate-branch> \
  -f backend_image=<backend-repository>@sha256:<digest> \
  -f frontend_image=<frontend-repository>@sha256:<digest>
```

The rehearsal uses the same signed applications for the off control, enabled
measurement and off replay. It loads 800,000 visible messages across 20 users.
The heaviest reader owns 2,000 conversations and 400,000 messages. Headless
Chromium signs in, opens an older result and follows it to its conversation.
PostgreSQL and Gateway use the distribution's memory, swap, CPU and process
limits. The PostgreSQL extension probe rolls back its transaction.

Record the workflow attempt, source, both running image identities, archive
digest, observations and thresholds in the Compose release guide. The
eight gates are message and title p95 (60 ingress requests each after warmup),
whole-user catch-up, median chat time during catch-up, median added deletion
time for 500 messages, stored bytes including indexes versus visible UTF-8
bytes, both constraint timeouts and retries, and peak PostgreSQL cgroup memory.
The receipt records raw observations. Timeouts, incomplete history coverage
and absent measurements cannot establish a pass. The separate runner's disk
and CPU differ from a tenant installation.

Only `message-search-observations.json` with `decision: pass` on a fully
verified candidate permits the default configuration change. A completed
measurement can record `miss`; leave the package disabled in that release.
For an unverified result, correct the rehearsal or retain the disabled default.
Repeat the measurement on fresh candidates after any change to an image input.
Continue ordinary release acceptance separately.

### The sandbox base image

`docker/sandbox/Dockerfile` builds on upstream's all-in-one sandbox image,
pinned by digest. The release build reads that base from this repository's
own `-sandbox-base` cache so that a release does not depend on a third-party
registry. To move the base: mirror the new digest with
`gh workflow run sandbox-image-mirror.yaml --repo altakleos/hartmesh -f source=<registry>/<image>@sha256:<digest> -f version=<next version>`,
then change the digest in `docker/sandbox/Dockerfile`, the sandbox entry of
`.github/workflows/container.yaml`, and the restricted-profile base in
`.github/workflows/sandbox-image-smoke.yml`. Run the sandbox smoke workflow
before merging.

### Merging upstream

Three things in this tree follow upstream by hand, each checked by a test or a
gate so that a merge cannot leave one behind:

- **`frontend/`** is upstream's application and is never edited here; the
  product's own is `frontend-hm/`. After a merge, record the upstream commit
  the merged `frontend/` is a copy of, and commit the result with the merge:
  ```bash
  python3 scripts/verify_frontend_isolation.py --pin <upstream remote>/main
  ```
- **Database migrations.** Preserve every published revision's identity,
  `down_revision` and `depends_on`. Never insert a new prerequisite behind a
  published head: Alembic treats it as already applied and silently skips its DDL
  when upgrading that head, even if fresh installation and single-head checks pass.
  Append imported upstream DDL after the current HartMesh head. Reconcile revision
  ID collisions (IDs must fit 32 characters), imported dependencies and ordering
  before publication; record the upstream-to-HartMesh mapping in the new revision.
  A branched import needs a reviewed merge/forward migration plan, not automatic
  reparenting of published revisions. Update the current-head assertions in
  `backend/tests/test_migration_chain_head.py`, retain its historical ancestry
  fixtures unchanged, and add an upgrade fixture for each newly published head.
  Verify both fresh bootstrap and upgrades from supported published heads with
  existing rows, on SQLite and PostgreSQL. Describe schema changes and upgrade
  actions in release notes, or explicitly state that there are none.
- **Version sources.** When upstream changes its version, the five sources
  must agree again: `scripts/verify_versions.sh` says which one does not.

## Company connection qualification

Run the mounted company-connection rehearsal before enabling employee connections
and again against the final release images after changing the bundle default.
Dispatch on the release branch at the commit used to build both images.
The workflow verifies both signatures, source revisions, and running image IDs.

```bash
gh workflow run compose-instances.yml --repo altakleos/hartmesh --ref <candidate-branch> \
  -f connections=explicit \
  -f backend_image=ghcr.io/altakleos/hartmesh-backend@sha256:<backend-digest> \
  -f frontend_image=ghcr.io/altakleos/hartmesh-frontend@sha256:<frontend-digest>
```

Use `connections=bundle-default` on the final candidate. The initial run
supplies an explicit feature option in its disposable template. The final
run starts with the shipped default, then exercises the operator's off/on
switch. Both use a mounted employee Home, a synthetic model and three isolated
HTTPS fixtures. A temporary CA is added to each disposable Gateway's trusted
roots. Two service addresses appear in its private endpoint list.

Read the workflow attempt's `connections.json` and JUnit results for browser,
assignment, revocation, and rollback outcomes. The rehearsal opens prior test
data with the published .66, .61, and .60 bundles. Production rollback for
this feature targets .66. Keep the ordinary two-store Docker acceptance record
and the ordinary mounted qualification for the final release as separate gates.
