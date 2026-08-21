# Releasing DeerFlow

DeerFlow releases are **tag-driven**: pushing a `v*` git tag triggers the
publishing workflows. There is no separate release script that bumps versions —
the maintainer bumps the version sources, updates the changelog, commits, and
tags. The helper scripts below keep the version sources in lockstep, and CI
gates the release on them agreeing with the tag.

## Fork releases

HartMesh releases use `X.Y.Z+hartmesh.N`: `X.Y.Z` is the upstream version base
and `N` is the monotonically increasing HartMesh patch-set number on that base.
The build-metadata suffix keeps the fork's identity explicit without claiming a
new upstream version. The exact string is valid in the Python, npm, and Helm
version sources; registries derive two transport-safe spellings from it.

For `2.1.0+hartmesh.1`, the published spellings are:

| Use | Spelling |
| --- | --- |
| Git tag | `v2.1.0+hartmesh.1` |
| Helm chart version / `helm --version` | `2.1.0+hartmesh.1` |
| Chart OCI tag | `2.1.0_hartmesh.1` (Helm maps build metadata for OCI storage) |
| Container image tag | `v2.1.0-hartmesh.1` (the metadata action sanitizes the git tag) |
| Commit lookup image tag | `sha-<first-7-commit-characters>` |

The shared implementation of the two registry spellings is
`scripts/release_tag_spellings.sh`; release workflows must call it instead of
reimplementing the substitutions. Published image repositories are
`ghcr.io/<owner>/<repo>-backend`, `-frontend`, and `-provisioner`. The chart is
`oci://ghcr.io/<owner>/charts/deer-flow`. The optional sandbox mirror is
`ghcr.io/<owner>/<repo>-sandbox`.

## Version sources

A release version must appear, identically, in five fields:

| File                                   | Field                |
| -------------------------------------- | -------------------- |
| `backend/pyproject.toml`               | `version = "X.Y.Z"`  |
| `backend/uv.lock`                      | root `deer-flow` package `version = "X.Y.Z"` |
| `frontend/package.json`                | `"version": "X.Y.Z"` |
| `deploy/helm/deer-flow/Chart.yaml`     | `version: X.Y.Z`     |
| `deploy/helm/deer-flow/Chart.yaml`     | `appVersion: "X.Y.Z"`|

Plus the git tag `v<version>` itself, which is the canonical release identifier.

Container images are tagged from the git tag (not from these files), and the
Helm chart version is validated against the tag — so if any source lags the
tag, the release is blocked (see [Version gate](#version-gate)).

The frontend's in-app About page (Settings ▸ About) is a *derived* consumer, not
an additional source: it reads `frontend/package.json`'s version at build time, so it
tracks the table above automatically with no bump needed. Nightly builds override
it with the chart's nightly string (`<base>-nightly.<YYYYMMDD>-<short_sha>`) via
the `APP_VERSION` build-arg in `nightly.yaml`, so a nightly image's About page
distinguishes it from a release.

## Helper scripts

- `scripts/bump_version.sh <version>` — set all five fields at once, running
  `uv lock` to update the root package entry before self-verification. The
  helper requires `uv` and tolerates a leading `v` (e.g.
  `v2.1.0+hartmesh.1`).
  ```bash
  scripts/bump_version.sh 2.1.0+hartmesh.1
  ```
- `scripts/verify_versions.sh [version]` — check that all sources agree. With
  no argument it requires mutual equality; with an argument it requires every
  source to equal it. Exits non-zero on mismatch. Run it locally before tagging
  to catch drift early:
  ```bash
  scripts/verify_versions.sh 2.1.0+hartmesh.1
  ```

## Fork release procedure

1. **Choose the fork version.** Increment `N` for every attempted release once
   that chart version has been published. Do not reuse an upstream-only version.
2. **Bump the version** across all five fields:
   ```bash
   scripts/bump_version.sh 2.1.0+hartmesh.1
   ```
3. **Update `CHANGELOG.md`** by adding a fork release section **above** the
   upstream `## [Unreleased]` block. Leave that upstream block and its link
   reference untouched. List the fork PRs included in this cut:
   ```
   ## [2.1.0+hartmesh.1] — YYYY-MM-DD

   - hartmesh#123 — concise change summary
   ```
   Add a matching link reference for the fork release using this repository's
   owner and name:
   ```
   [2.1.0+hartmesh.1]: https://github.com/<owner>/<repo>/releases/tag/v2.1.0+hartmesh.1
   ```
4. **Verify and commit** the version + changelog changes:
   ```bash
   scripts/verify_versions.sh 2.1.0+hartmesh.1
   git add -A
   git commit -m "release: v2.1.0+hartmesh.1"
   ```
5. **Tag and push**:
   ```bash
   git tag v2.1.0+hartmesh.1
   git push origin v2.1.0+hartmesh.1
   ```
   Pushing the tag triggers the publishing workflows below. Wait for both the
   chart and all three container jobs to succeed before recording the release
   identity.
6. **Mirror and record identities.** Follow
   [Manual release workflows](#manual-release-workflows), then perform the
   first-publish visibility checks if these packages are new.

## Durable runtime qualification evidence

The administrator deployment report is not remote attestation. With no reference it reports
`status: unqualified` and `trust: none_declared`. When an operator configures a bounded
Kubernetes qualification reference, v1 retains `status: qualified` for wire compatibility
but reports `trust: operator_asserted`. That state proves only that the operator declared an
artifact digest.

For a release gate, obtain the evidence artifact through an independently controlled path or
artifact store, then run `backend/scripts/verify_qualification_evidence.py`. Supply the
declared report digest and independently expected qualification ID, image digest, chart
version/digest, rendered configuration digest, Alembic head, scope, namespace, and every
required scenario. Only exit zero with `status: verified` and
`trust: external_evidence_verified` is exact-artifact evidence. A missing artifact, a default
Kubernetes test skip, or an operator-asserted reference is an unpassed release gate. The full
offline command is in the [Helm deployment guide](deploy/helm/deer-flow/README.md#deployment-identity-and-qualification).

The verifier performs no network fetch and does not validate signatures. Its current proof is
the canonical artifact SHA-256 plus exact subject and complete passing-scenario match.

## What CI publishes on a `v*` tag

- `.github/workflows/container.yaml` — builds and pushes `backend`,
  `frontend`, and `provisioner` images to `ghcr.io`, tagged with the release
  tag's registry-safe image spelling and `sha-<short-commit>`.
- `.github/workflows/chart.yaml` — packages the Helm chart and pushes it as an
  OCI artifact to `ghcr.io`. Users install with:
  ```bash
  helm install deer-flow oci://ghcr.io/<owner>/charts/deer-flow \
    --version 2.1.0+hartmesh.1
  ```

## Manual release workflows

After selecting an exact upstream sandbox digest, dispatch the mirror workflow.
It rejects floating sources, copies by digest, verifies the destination digest,
and prints the reference to use for `sandbox.sandboxImage` and the release
manifest:

```bash
gh workflow run sandbox-image-mirror.yaml \
  -f 'source=<source-registry>/<sandbox-image>@sha256:<64-lowercase-hex>' \
  -f version=2.1.0+hartmesh.1
```

Copy the verified `ghcr.io/<owner>/<repo>-sandbox@sha256:...` reference from the
mirror summary. After the tag-triggered chart and all three image jobs succeed,
dispatch the manifest workflow and pass that reference through its optional
`sandbox` input:

```bash
gh workflow run release-manifest.yaml \
  -f version=2.1.0+hartmesh.1 \
  -f 'sandbox=ghcr.io/<owner>/<repo>-sandbox@sha256:<64-lowercase-hex>'
```

The manifest workflow checks out the exact tag, cross-checks each release image
against its `sha-` tag when present, resolves the chart, creates the GitHub
Release if needed, and attaches `release-manifest.json` as both a workflow
artifact and a release asset. A missing `sha-` tag is recorded as
`revision_check: tag-not-found`; a resolved digest mismatch remains fatal. When
`sandbox` is supplied, the workflow verifies its digest and adds
`"sandbox": {"repository": "...", "digest": "sha256:..."}`. Omitting the input
omits that key from the schema-1 manifest.

Do not dispatch the manifest while a publish job is pending or failed: a
missing image or a tag/digest mismatch intentionally fails the workflow. The
sandbox must be mirrored first so its verified reference can be included in the
manifest.

### First-publish GHCR visibility

GHCR creates each package as private on first publish, and package visibility
cannot be changed by these workflows or a GHCR API. In the GitHub Packages UI,
open package settings and change visibility to **Public** for all five
deployment packages:

- `<repo>-backend`
- `<repo>-frontend`
- `<repo>-provisioner`
- `<repo>-sandbox`
- `charts/deer-flow`

Log out of GHCR (or use a clean shell with no registry credentials) and verify
each image tag plus the chart OCI tag. Every command must succeed without
authentication:

```bash
crane manifest <ref> >/dev/null
```

For example, check the three image references from `release-manifest.json`, the
sandbox mirror tag from its workflow summary, and
`ghcr.io/<owner>/charts/deer-flow:2.1.0_hartmesh.1`. A successful authenticated
pull is not evidence that visibility was changed.

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
`v`). If any of the five version fields doesn't match the tag, the verify job
fails and **all** publish jobs are skipped — no images, no chart.

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

## Release failure handling

| Failure | Recovery |
| --- | --- |
| Version gate fails | Nothing was published. Fix the sources with `scripts/bump_version.sh`, commit, delete and recreate the tag on the fixed commit, then push it again. |
| A container job fails after the gate | Re-run the failed job on the **same workflow run**. Image tags are mutable, so the missing image can be completed without changing the release identity. |
| The chart version was published | The chart version is immutable. Never move or re-push that tag; increment the `hartmesh.N` patch-set number and make a new release. |

Deleting and recreating a tag is safe only when the version gate failed before
any artifact was published. If the chart job might have succeeded, inspect the
package first and use a new `N`.

## Post-release

The `release-manifest` workflow creates the **GitHub Release** if it is absent
and attaches the resolved identity. Expand its notes from the corresponding
`CHANGELOG.md` section after the workflow succeeds; keep the manifest asset
attached unchanged.

For the 2.1.0 chart release (the first chart release), pre-`charts/` nightly
builds remain at the legacy bare `ghcr.io/<owner>/deer-flow` package. That
package receives no new versions after 2.1.0; delete it or revoke its
visibility once nothing still pulls from it.
