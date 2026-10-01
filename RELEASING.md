# Releasing DeerFlow

DeerFlow releases are **tag-driven**: pushing a `v*` git tag triggers the
publishing workflows. There is no separate release script that bumps versions —
the maintainer bumps the version sources, updates the changelog, commits, and
tags. The helper scripts below keep the version sources in lockstep, and CI
gates the release on them agreeing with the tag.

## Version sources

A release version must appear, identically, in four places:

| File                                   | Field                |
| -------------------------------------- | -------------------- |
| `backend/pyproject.toml`               | `version = "X.Y.Z"`  |
| `frontend/package.json`                | `"version": "X.Y.Z"` |
| `deploy/helm/deer-flow/Chart.yaml`     | `version: X.Y.Z`     |
| `deploy/helm/deer-flow/Chart.yaml`     | `appVersion: "X.Y.Z"`|

Plus the git tag `vX.Y.Z` itself, which is the canonical release identifier.

Container images are tagged from the git tag (not from these files), and the
Helm chart version is validated against the tag — so if any source lags the
tag, the release is blocked (see [Version gate](#version-gate)).

The frontend's in-app About page (Settings ▸ About) is a *derived* consumer, not
a fifth source: it reads `frontend/package.json`'s version at build time, so it
tracks the table above automatically with no bump needed. Nightly builds override
it with the chart's nightly string (`<base>-nightly.<YYYYMMDD>-<short_sha>`) via
the `APP_VERSION` build-arg in `nightly.yaml`, so a nightly image's About page
distinguishes it from a release.

## Helper scripts

- `scripts/bump_version.sh <version>` — set all four fields at once, refresh
  `backend/uv.lock`, then self-verify. Tolerates a leading `v` (e.g. `v2.1.0`).
  Needs `uv` on `PATH` (`backend/uv.lock` pins the root package version too, and
  lint CI runs `uv lock --check`); the script fails before editing anything when
  `uv` is missing.
  ```bash
  scripts/bump_version.sh 2.1.0
  ```
- `scripts/verify_versions.sh [version]` — check that all sources agree. With
  no argument it requires mutual equality; with an argument it requires every
  source to equal it. Exits non-zero on mismatch. Run it locally before tagging
  to catch drift early:
  ```bash
  scripts/verify_versions.sh 2.1.0
  ```

## Release procedure

1. **Bump the version** across all sources (this refreshes
   `backend/uv.lock` too):
   ```bash
   scripts/bump_version.sh 2.1.0
   ```
2. **Update `CHANGELOG.md`**: rename the `## [Unreleased]` section to
   `## [2.1.0] — YYYY-MM-DD` (note the em dash `—`), and add a link reference
   at the bottom of the file:
   ```
   [2.1.0]: https://github.com/bytedance/deer-flow/releases/tag/v2.1.0
   ```
   Start a fresh `## [Unreleased]` section above it for the next cycle.
3. **Commit** the version + changelog changes:
   ```bash
   git add -A
   git commit -m "release: v2.1.0"
   ```
4. **Tag and push**:
   ```bash
   git tag v2.1.0
   git push origin v2.1.0
   ```
   Pushing the tag triggers the publishing workflows (below).

### Release candidates

Release-candidate tags must include the same prerelease suffix in all four
version fields. For example, before tagging `v2.1.0-rc0`, run
`bash scripts/bump_version.sh 2.1.0-rc0` and run
`bash scripts/verify_versions.sh 2.1.0-rc0` from the repository root. Commit
the version and lockfile changes before creating the tag. Python lockfiles
normalize this version to `2.1.0rc0` — `bump_version.sh` leaves the normalizing
to `uv lock` — while the source version fields checked by the release gate
retain `2.1.0-rc0`. Re-running a failed workflow on an unchanged tag does not
pick up a later version-fix commit.

## What CI publishes on a `v*` tag

- `.github/workflows/container.yaml` — builds and pushes `backend`,
  `frontend`, and `provisioner` images to `ghcr.io`, tagged with the release
  version (and `latest` on the default branch).
- `.github/workflows/chart.yaml` — packages the Helm chart and pushes it as an
  OCI artifact to `ghcr.io`. Users install with:
  ```bash
  helm install deer-flow oci://ghcr.io/<owner>/charts/deer-flow --version 2.1.0
  ```

## Nightly builds

`.github/workflows/nightly.yaml` runs on a schedule (and `workflow_dispatch`)
to publish the same three images plus the chart from unreleased `main`. It is
**not** gated by the version check (there is no `v*` tag) and it does **not**
touch the `latest` tag, which stays pinned to the last `v*` release. Every job
is gated on `github.repository == 'bytedance/deer-flow'`, so it only runs on
the upstream repo - a scheduled run or manual dispatch on a fork skips all jobs.

Artifacts (under the running repo's owner, where `<date>` is `YYYYMMDD`):

- Images: `ghcr.io/<owner>/deer-flow-{backend,frontend,provisioner}:nightly`
  (rolling, overwritten each run) and `:nightly-<date>` (pinned to a day, but
  mutable within it - a same-day re-dispatch overwrites it). For a truly
  immutable pin, use `:sha-<short>`.
- Chart: `oci://ghcr.io/<owner>/charts/deer-flow`, version `<base>-nightly.<date>-<sha>`
  (e.g. `2.1.0-nightly.20260710-77a3652`). The short SHA makes each dispatch's
  chart version unique, so a same-day re-dispatch re-publishes cleanly (OCI
  chart versions are immutable and otherwise can't be overwritten). The
  packaged chart defaults `image.registry=ghcr.io/<owner>` and
  `image.tag=nightly`, so installing it pulls the matching nightly images with
  no values overrides:
  ```bash
  helm install deer-flow oci://ghcr.io/<owner>/charts/deer-flow \
    --version 2.1.0-nightly.20260710-77a3652
  ```

The chart version is patched in-workflow only - `Chart.yaml` and `values.yaml`
in the repo are never modified.

## lark-cli sandbox images

The two optional Lark sandbox runtime images — `lark-cli-init` (Pattern A) and
`lark-cli-broker` (Pattern B) — are **not** part of the `v*` release. They track
the upstream `larksuite/cli` version, so they publish independently via
`.github/workflows/lark-cli-images.yaml`:

- Trigger with `workflow_dispatch` (a `lark_cli_version` input, e.g. `v1.0.65`)
  or by pushing a `lark-cli-v*` tag (the version is read from after the prefix).
- Builds multi-arch (`linux/amd64,linux/arm64`) and pushes
  `ghcr.io/<owner>/deer-flow-{lark-cli-init,lark-cli-broker}:<lark-cli-version>`.
- Gated on `github.repository == 'bytedance/deer-flow'`; not tied to the
  `verify-versions` gate (its version is the lark-cli release, not the DeerFlow
  release), and it never touches `latest`.

Both features stay opt-in: the provisioner ignores them until
`LARK_CLI_INIT_IMAGE` / `LARK_CLI_BROKER_IMAGE` point at a published tag.

## Version gate

Both publishing workflows call `.github/workflows/verify-versions.yml` as their
first job. It runs `scripts/verify_versions.sh` against the tag (minus the
`v`). If any of the four version sources doesn't match the tag, the verify job
fails and **all** publish jobs are skipped — no images, no chart.

The gate covers those four fields. `backend/uv.lock` is refreshed to the same
version by `scripts/bump_version.sh` and checked by `uv lock --check` in lint CI,
which fails on a stale lock.

When it fails, the job annotation names the offending file and suggests the
fix:

```
::error::frontend/package.json is '2.0.0' but expected '2.1.0'.
Tip: run scripts/bump_version.sh 2.1.0 to align all sources.
```

## Pre-releases (RCs)

Pre-release tags like `v2.1.0-rc1` are valid `v*` tags and trigger the same
workflows. The version sources must equal the full pre-release string
(`2.1.0-rc1`) — the gate compares exact strings. Use the same procedure with
the rc version:

```bash
scripts/bump_version.sh 2.1.0-rc1
# update CHANGELOG, commit, tag v2.1.0-rc1, push
```

## Recovering from a failed gate

If the gate failed because a source was forgotten:

1. Run `scripts/bump_version.sh <version>` to align the sources.
2. Amend or add a follow-up commit.
3. Delete and re-create the tag, then push it:
   ```bash
   git tag -d v2.1.0
   git tag v2.1.0
   git push origin :refs/tags/v2.1.0
   git push origin v2.1.0
   ```

Re-pushing the tag re-triggers the workflows. Because the gate blocks **all**
artifacts when it fails, nothing was published under the bad tag, so re-tagging
is safe — no images or chart were pushed to overwrite.

## Post-release

Optionally draft a **GitHub Release** from the tag, pasting the corresponding
`CHANGELOG.md` section as the release notes. The changelog link references
point at these release URLs.

For the 2.1.0 chart release (the first chart release), pre-`charts/` nightly
builds remain at the legacy bare `ghcr.io/<owner>/deer-flow` package. That
package receives no new versions after 2.1.0; delete it or revoke its
visibility once nothing still pulls from it.

## HartMesh distribution releases

Everything above is upstream's release process and is kept as upstream wrote
it. This section is what this repository does instead. HartMesh is a
distribution of DeerFlow: upstream's `main` is merged in regularly, and what a
release ships is the single-VM profile under `deploy/compose/` with the five
container images it runs. The Helm chart in this tree is upstream's, unchanged
and not qualified against this build; a release tag here does not publish it.

### Version

A HartMesh release is `X.Y.Z+hartmesh.N`: `X.Y.Z` is the upstream version the
build is based on and `N` increases with every release. The same string goes
in the four version sources (`deploy/helm/deer-flow/Chart.yaml`,
`backend/pyproject.toml`, `backend/uv.lock`, `frontend-hm/package.json`);
`scripts/bump_version.sh` writes all of them and `scripts/verify_versions.sh`
checks them. Between releases the tree carries upstream's own version.

For `2.1.0+hartmesh.1` the spellings are: git tag `v2.1.0+hartmesh.1`,
container image tag `v2.1.0-hartmesh.1`, and `sha-<first seven characters of
the commit>` for lookup by commit. `scripts/release_tag_spellings.sh` is the
one implementation; workflows call it. The images are
`ghcr.io/<owner>/<repo>-backend`, `-frontend`, `-provisioner`, `-sandbox` and
`-sandbox-network-proxy`. `ghcr.io/<owner>/<repo>-sandbox-base` is a private
cache of the sandbox's upstream base image, not something to deploy.

### Procedure

The compose profile must reference its images by digest in the tagged tree,
so a release builds its images before the tag exists.

1. **Choose the version** and write it to every source:
   ```bash
   scripts/bump_version.sh 2.1.0+hartmesh.1
   scripts/verify_versions.sh 2.1.0+hartmesh.1
   ```
2. **Commit and push** that change (a branch is fine; the candidate build
   reads the ref it is dispatched on).
3. **Build the candidate images** from that commit:
   ```bash
   gh workflow run container.yaml --ref <that branch> -f version=2.1.0+hartmesh.1
   ```
   A dispatch builds all five images under the release's tag spelling and
   never reuses a pinned digest, whatever `deploy/compose/images.txt` carries.
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
   The container workflow does not rebuild an image the profile pins. It
   re-tags the pinned digest unchanged, after checking that the release tag
   already resolves to it, which only this version's candidate build can have
   arranged.
6. **Record the release**, once the five image jobs have succeeded:
   ```bash
   gh workflow run release-manifest.yaml -f version=2.1.0+hartmesh.1
   ```
   It checks out the tag, resolves each image, checks every line of
   `deploy/compose/images.txt` against what was published, and attaches
   `release-manifest.json` to the GitHub Release. The manifest (schema 4)
   lists the five images by repository, tag and digest, and the compose
   profile's `images.txt` with its SHA-256. Verify a downloaded copy offline:
   ```bash
   python3 scripts/verify_release_manifest.py release-manifest.json
   ```

A package GHCR creates is private until its visibility is changed in the
package's settings; do that once for each of the five images.

### The sandbox base image

`docker/sandbox/Dockerfile` builds on upstream's all-in-one sandbox image,
pinned by digest. The release build reads that base from this repository's
own `-sandbox-base` cache so that a release does not depend on a third-party
registry. To move the base: mirror the new digest with
`gh workflow run sandbox-image-mirror.yaml -f source=<registry>/<image>@sha256:<digest> -f version=<next version>`,
then change the digest in both `docker/sandbox/Dockerfile` and the sandbox
entry of `.github/workflows/container.yaml`, and run the sandbox smoke
workflow before merging.

### Merging upstream

Three things in this tree follow upstream by hand, each checked by a test or a
gate so that a merge cannot leave one behind:

- **`frontend/`** is upstream's application and is never edited here; the
  product's own is `frontend-hm/`. After a merge, record the upstream commit
  the merged `frontend/` is a copy of, and commit the result with the merge:
  ```bash
  python3 scripts/verify_frontend_isolation.py --pin <upstream remote>/main
  ```
- **Database migrations.** This distribution's revisions follow upstream's
  newest one in a single chain. When upstream adds a revision, point the first
  distribution revision's `down_revision` at it and update
  `backend/tests/test_migration_chain_head.py`, which names both ends.
- **Version sources.** When upstream changes its version, the four sources
  must agree again: `scripts/verify_versions.sh` says which one does not.
