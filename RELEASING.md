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
build is based on and `N` increases with every release. These four sources must
agree:

| Source | Version field |
| --- | --- |
| `backend/pyproject.toml` | Root project `version` |
| `backend/uv.lock` | Root `deer-flow` package version (uv's normalized spelling) |
| `frontend-hm/package.json` | `version` |
| `deploy/helm/deer-flow/Chart.yaml` | Both `version` and `appVersion` |

The pinned upstream `frontend/package.json` is excluded from HartMesh version
bumps. `bash scripts/bump_version.sh <version>` updates the product sources and
refreshes the lockfile; `bash scripts/verify_versions.sh <version>` checks all
four. The publishing workflows stop when a source disagrees.

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
5. **Tag and push** the pin commit:
   ```bash
   git tag v2.1.0+hartmesh.1
   git push origin v2.1.0+hartmesh.1
   ```
   Before any image is re-tagged, the container workflow verifies all five
   candidates, including the provisioner. Each immutable digest must carry
   verified build provenance for this repository's container workflow and a
   source revision whose tracked build inputs match the tagged source. The
   candidate's recorded input fingerprint must match too. The four Compose
   pins must resolve to those candidate digests. Missing or unverifiable
   evidence stops publication; there is no rebuild fallback.

   Candidates built before these guards lack the required input evidence and
   cannot be adopted. Build new candidates under an unused version. Once a
   version has a Git tag or GitHub Release, source corrections require a new
   version rather than rebuilding or moving the existing release tag. An
   unchanged tag's publication can be retried after a transient failure.

   Root release notes and Compose pins may change after the candidate build.
   Changes to image source trees, shipped skills, Dockerfiles, or build controls
   require another candidate build before tagging. Build-input checks include
   documentation inside those source trees conservatively. Adoption preserves
   the candidate bytes and their original build revision, and adds the final
   commit's `sha-` lookup tag.
6. **Record the release**, once the five image jobs have succeeded:
   ```bash
   gh workflow run release-manifest.yaml --repo altakleos/hartmesh -f version=2.1.0+hartmesh.1
   ```
   It checks out the tag, resolves each image, checks every line of
   `deploy/compose/images.txt` against what was published, repeats the candidate
   provenance and source checks, and requires both release and final-commit
   image tags to resolve to those verified digests. It attaches
   `release-manifest.json` to the GitHub Release. The manifest (schema 4)
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
and fixture fingerprint, inspected application image IDs/digests, and actual
fixture image identities. Each application container's image ID must match its
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
- **Version sources.** When upstream changes its version, the four sources
  must agree again: `scripts/verify_versions.sh` says which one does not.
