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

``pin`` resolves every tag-form line of images.txt to its registry digest,
rewrites the matching references in the two YAML files to ``repo@sha256:...``,
writes images.txt from the same strings, and then verifies. ``--check``
verifies only. Either mode exits non-zero while any reference still carries a
tag or the three files disagree, so a release cut cannot proceed past it.

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
_YAML_IMAGE_LINE = re.compile(r"^(?P<prefix>\s*(?:-\s*)?(?:image|proxy_image):\s*)(?P<reference>\S+)(?P<suffix>\s*(?:#.*)?)$")

Resolver = Callable[[str], str]


class PinError(ValueError):
    """A refusal that must stop the release cut."""


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


def pin(files: ProfileFiles, resolve: Resolver) -> list[str]:
    """Resolve tag-form references, rewrite the profile, write images.txt, verify."""

    references = read_references(files.images)
    mapping: dict[str, str] = {}
    for reference in references:
        if PINNED_REFERENCE.fullmatch(reference) is not None:
            continue
        digest = resolve(reference)
        if DIGEST.fullmatch(digest) is None:
            raise PinError(f"resolver returned {digest!r} for {reference}, not a sha256 digest")
        mapping[reference] = f"{repository_of(reference)}@{digest}"
    for path in (files.compose, files.config):
        rewrite_yaml(path, mapping)
    pinned = [mapping.get(reference, reference) for reference in references]
    files.images.write_text("".join(f"{reference}\n" for reference in pinned), encoding="utf-8")
    return verify(files)


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
    args = parser.parse_args(argv)
    files = ProfileFiles.under(args.profile)
    try:
        references = verify(files) if args.check else pin(files, default_resolver())
    except (PinError, OSError) as exc:
        print(f"pin_compose_images: {exc}", file=sys.stderr)
        return 1
    for reference in references:
        print(reference)
    print(f"pin_compose_images: {files.images} and the profile agree on {len(references)} digest-pinned references")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
