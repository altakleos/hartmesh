"""Finite synthetic operations, mounted only by the preview measurement."""

import signal
import time

from deerflow_extension_api import ArtifactPresentation, PluginContribution, extension


def project(source: bytes) -> bytes:
    if source == b"oversize":
        return b"x" * 65
    if source == b"invalid":
        raise ValueError("Expected a sample label")
    if source == b"interrupted":
        signal.raise_signal(signal.SIGKILL)
    if source == b"address-limit":
        import json
        import mmap
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        error = 0
        try:
            # Fixed reservation with no access permission. No pages are touched.
            with mmap.mmap(-1, 256 << 20, flags=mmap.MAP_PRIVATE | mmap.MAP_ANONYMOUS, prot=0):
                pass
        except OSError as exc:
            error = exc.errno
        return json.dumps({"soft": soft, "hard": hard, "errno": error}, separators=(",", ":")).encode()
    if source == b"deadline":
        time.sleep(11)
        return source
    time.sleep(2.5 if source.startswith(b"sample-") else 1)
    return source


@extension(api="0.2.5", name="sample-projector")
def install(registry, config):
    if not registry.plugin(
        PluginContribution(
            namespace="example.measurement",
            title="Sample projection",
            description="A controlled, finite preview for runner measurements.",
            enabled=True,
            api_version=2,
            artifacts=(
                ArtifactPresentation(
                    id="sample",
                    suffixes=(".sample",),
                    source_max_bytes=64,
                    preview_max_bytes=64,
                    project=project,
                    projection_marker="sample-v1",
                ),
            ),
        )
    ):
        raise ValueError("Sample presentation unavailable")
