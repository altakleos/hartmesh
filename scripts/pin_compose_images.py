#!/usr/bin/env python3
"""Pin the compose profile's image references to digests before a release tag.

The golden VM image pre-pulls exactly the lines of deploy/compose/images.txt
(one ``<repository>@sha256:<64 hex>`` per line) and the estate's stated
property is that a first start pulls nothing. That holds only if the running
stack references the same strings, so this script keeps three files in
lockstep:

* ``deploy/compose/images.txt``     - the reference list (input and output)
* ``deploy/compose/compose.yaml``   - every ``image:`` value
* ``deploy/compose/config.yaml``    - ``sandbox.image`` and ``network.proxy_image``

``pin --release X.Y.Z+hartmesh.N`` first rewrites every fork image line (a
component ``container.yaml`` builds, ``ghcr.io/<owner>/<repo>-<component>``)
to that release's image tag spelling, whatever the line said before, then
resolves every tag-form line to its registry digest, rewrites the matching
references in the two YAML files to ``repo@sha256:...``, writes images.txt
from the same strings, verifies, and prints each fork resolution so the
session cutting the release can compare it with the candidate build. Between
cuts the tree carries the previous release's digest pins (the pin commit is
the last thing a release changes), so ``--release`` is what moves every fork
line to the new release before anything is resolved. A tag-form fork line
without ``--release`` is refused: resolving it would pin whatever the tag
names today and the adopt step on the tag would re-tag that as the new
release with every check green. Third-party lines (``postgres``, ``redis``,
``nginx``) are resolved as written; to bump one, put the new tag form in all
three files in place of the old digest string and run pin. ``--check``
verifies only. Either mode exits non-zero while any
reference still carries a tag or the three files disagree, so a release cut
cannot proceed past it.

Resolution uses ``crane digest`` when crane is installed, otherwise
``docker buildx imagetools inspect``; both return the manifest-list digest,
which is what ``docker pull repo@sha256:...`` expects.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

PINNED_REFERENCE = re.compile(r"\A(?P<repository>[a-z0-9./_-]+)@sha256:(?P<digest>[0-9a-f]{64})\Z")
TAGGED_REFERENCE = re.compile(r"\A(?P<repository>[a-z0-9./_-]+):(?P<tag>[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})\Z")
DIGEST = re.compile(r"\Asha256:[0-9a-f]{64}\Z")
RELEASE_VERSION = re.compile(r"\Av?(?P<version>[0-9]+\.[0-9]+\.[0-9]+\+hartmesh\.[0-9]+)\Z")
# The components .github/workflows/container.yaml builds; a fork image is
# ``ghcr.io/<owner>/<repo>-<component>`` for exactly one of these.
FORK_COMPONENTS = ("backend", "frontend", "provisioner", "sandbox", "sandbox-network-proxy")
FORK_REPOSITORY = re.compile(r"\Aghcr\.io/[a-z0-9._-]+/[a-z0-9._-]+-(?:" + "|".join(re.escape(component) for component in FORK_COMPONENTS) + r")\Z")
_YAML_IMAGE_LINE = re.compile(r"^(?P<prefix>\s*(?:-\s*)?(?:image|proxy_image):\s*)(?P<reference>\S+)(?P<suffix>\s*(?:#.*)?)$")

Resolver = Callable[[str], str]


class PinError(ValueError):
    """A refusal that must stop the release cut."""


@dataclass(frozen=True)
class Resolution:
    """One tag-form reference and the pinned reference it resolved to."""

    reference: str
    pinned: str


@dataclass(frozen=True)
class PinResult:
    references: list[str]
    resolutions: tuple[Resolution, ...]


@dataclass(frozen=True)
class ProfileFiles:
    images: Path
    compose: Path
    config: Path

    @classmethod
    def under(cls, profile_dir: Path) -> ProfileFiles:
        return cls(images=profile_dir / "images.txt", compose=profile_dir / "compose.yaml", config=profile_dir / "config.yaml")


def read_references(images_path: Path) -> list[str]:
    lines = [line.strip() for line in images_path.read_text(encoding="utf-8").splitlines()]
    references = [line for line in lines if line]
    if not references or len(references) != len(set(references)):
        raise PinError(f"{images_path} must list each image reference exactly once")
    for reference in references:
        if PINNED_REFERENCE.fullmatch(reference) is None and TAGGED_REFERENCE.fullmatch(reference) is None:
            raise PinError(f"{images_path}: {reference!r} is neither repo:tag nor repo@sha256:<digest>")
    return references


def yaml_references(path: Path) -> list[str]:
    """Return every image reference the YAML file names, in file order."""

    found: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        match = _YAML_IMAGE_LINE.match(line)
        if match is not None:
            found.append(match.group("reference"))
    return found


def rewrite_yaml(path: Path, mapping: dict[str, str]) -> None:
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    rewritten: list[str] = []
    for line in lines:
        match = _YAML_IMAGE_LINE.match(line.rstrip("\n"))
        if match is not None and match.group("reference") in mapping:
            line = f"{match.group('prefix')}{mapping[match.group('reference')]}{match.group('suffix')}\n"
        rewritten.append(line)
    path.write_text("".join(rewritten), encoding="utf-8")


def repository_of(reference: str) -> str:
    pinned = PINNED_REFERENCE.fullmatch(reference)
    if pinned is not None:
        return pinned.group("repository")
    tagged = TAGGED_REFERENCE.fullmatch(reference)
    if tagged is None:
        raise PinError(f"{reference!r} is not an image reference")
    return tagged.group("repository")


def is_fork_image(repository: str) -> bool:
    """True for a repository container.yaml publishes under the release tag."""

    return FORK_REPOSITORY.fullmatch(repository) is not None


def release_image_tag(release: str) -> str:
    """The image tag spelling of a fork release, as scripts/release_tag_spellings.sh prints it."""

    match = RELEASE_VERSION.fullmatch(release)
    if match is None:
        raise PinError(f"{release!r} is not a fork release version (X.Y.Z+hartmesh.N, leading v tolerated)")
    return "v" + match.group("version").replace("+", "-")


def verify(files: ProfileFiles) -> list[str]:
    """Return the pinned references when the three files agree, else raise."""

    references = read_references(files.images)
    tagged = [reference for reference in references if PINNED_REFERENCE.fullmatch(reference) is None]
    if tagged:
        raise PinError(f"{files.images} still carries tag-form references: {tagged}")
    expected = set(references)
    for path in (files.compose, files.config):
        for reference in yaml_references(path):
            if reference not in expected:
                raise PinError(f"{path} references {reference!r}, which is not a line of {files.images}")
    used = set(yaml_references(files.compose)) | set(yaml_references(files.config))
    unused = sorted(expected - used)
    if unused:
        raise PinError(f"{files.images} lists references the profile does not use: {unused}")
    return references


def pin(files: ProfileFiles, resolve: Resolver, *, release: str | None = None) -> PinResult:
    """Resolve tag-form references, rewrite the profile, write images.txt, verify.

    With ``release``, every fork image line is first pointed at that release's
    image tag, so the digests pinned are the candidate build's and never the
    placeholders' previous release. Without it, a tag-form fork line is refused.
    """

    references = read_references(files.images)
    image_tag = release_image_tag(release) if release is not None else None
    targets: dict[str, str] = {}
    for reference in references:
        repository = repository_of(reference)
        if is_fork_image(repository):
            if image_tag is not None:
                targets[reference] = f"{repository}:{image_tag}"
            elif PINNED_REFERENCE.fullmatch(reference) is None:
                raise PinError(f"{files.images} carries the tag-form fork image line {reference!r}; pass --release <version> so the pins are that release's candidate build rather than the placeholder's previous release")
            else:
                targets[reference] = reference
        else:
            targets[reference] = reference
    mapping: dict[str, str] = {}
    resolutions: list[Resolution] = []
    for reference, target in targets.items():
        if PINNED_REFERENCE.fullmatch(target) is not None:
            continue
        digest = resolve(target)
        if DIGEST.fullmatch(digest) is None:
            raise PinError(f"resolver returned {digest!r} for {target}, not a sha256 digest")
        mapping[reference] = f"{repository_of(target)}@{digest}"
        resolutions.append(Resolution(reference=target, pinned=mapping[reference]))
    for path in (files.compose, files.config):
        rewrite_yaml(path, mapping)
    pinned = [mapping.get(reference, reference) for reference in references]
    files.images.write_text("".join(f"{reference}\n" for reference in pinned), encoding="utf-8")
    return PinResult(references=verify(files), resolutions=tuple(resolutions))


def _crane_digest(reference: str) -> str:
    result = subprocess.run(["crane", "digest", reference], capture_output=True, text=True, timeout=120, check=False)
    if result.returncode != 0:
        raise PinError(f"crane could not resolve {reference}: {result.stderr.strip()}")
    return result.stdout.strip()


def _buildx_digest(reference: str) -> str:
    result = subprocess.run(
        ["docker", "buildx", "imagetools", "inspect", reference, "--format", "{{json .Manifest.Digest}}"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if result.returncode != 0:
        raise PinError(f"docker buildx could not resolve {reference}: {result.stderr.strip()}")
    try:
        digest = json.loads(result.stdout.strip())
    except json.JSONDecodeError as exc:
        raise PinError(f"docker buildx returned no digest for {reference}") from exc
    return str(digest)


def default_resolver() -> Resolver:
    if shutil.which("crane"):
        return _crane_digest
    if shutil.which("docker"):
        return _buildx_digest
    raise PinError("neither crane nor docker is installed; nothing can resolve image digests")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--profile", type=Path, default=Path(__file__).resolve().parents[1] / "deploy" / "compose")
    parser.add_argument("--check", action="store_true", help="verify only; refuse any tag-form reference")
    parser.add_argument("--release", metavar="VERSION", help="the fork release being cut (X.Y.Z+hartmesh.N); required to pin tag-form fork image lines")
    args = parser.parse_args(argv)
    if args.check and args.release is not None:
        parser.error("--check verifies the tree as it is and takes no --release")
    files = ProfileFiles.under(args.profile)
    try:
        if args.check:
            references = verify(files)
        else:
            result = pin(files, default_resolver(), release=args.release)
            references = result.references
            for resolution in result.resolutions:
                if is_fork_image(repository_of(resolution.reference)):
                    print(f"pin_compose_images: resolved {resolution.reference} -> {resolution.pinned.rpartition('@')[2]}")
    except (PinError, OSError) as exc:
        print(f"pin_compose_images: {exc}", file=sys.stderr)
        return 1
    for reference in references:
        print(reference)
    print(f"pin_compose_images: {files.images} and the profile agree on {len(references)} digest-pinned references")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
