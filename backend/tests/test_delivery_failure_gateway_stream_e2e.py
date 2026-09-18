"""A turn that produced a file and presented none of it delivers it anyway (DF22), and the fence still speaks when it cannot (DF13/DF14).

What runs for real: the same Gateway, route, worker, admission, graph and SSE
consumer as ``test_turn_phase_gateway_stream_e2e.py``, plus the real delivery
fence — the post-run outputs scan, the terminal ``error`` status, the advisory
frame the worker publishes before the stream's end marker, and the durable
verdict the run record then reports over HTTP.

What is synthetic: the produced artifact. The probe model emits no tool call,
so the test writes one file into the thread's outputs directory while the graph
is still streaming, after the pre-run snapshot has been taken. The worker's scan
cannot tell that file apart from one the agent wrote, which is the point: this
is a turn that produced an output and finished without presenting it, exactly
the shape two tenant-class turns took while the browser showed success.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import test_turn_phase_gateway_stream_e2e as e2e

ARTIFACT_NAME = "monthly-report.md"


@pytest.fixture(scope="module")
def delivery_gateway(tmp_path_factory: pytest.TempPathFactory) -> Iterator[e2e._Gateway]:
    with e2e.serve_gateway(tmp_path_factory.mktemp("delivery-failure-e2e")) as served:
        yield served


def _outputs_dir(home: Path, thread_id: str) -> Path | None:
    """This thread's outputs directory once the run has laid it down.

    The host path is user-scoped or legacy depending on the deployment, so it is
    resolved by search rather than constructed, and never fabricated: a
    directory this helper invented would sit outside the roots the worker scans
    and the turn would look like it simply passed the fence.
    """
    for candidate in home.rglob(f"threads/{thread_id}/user-data/outputs"):
        if candidate.is_dir():
            return candidate
    return None


def _produce_one_artifact_mid_turn(home: Path, thread_id: str) -> Any:
    """Write an output as soon as the turn is visibly under way, then stop."""
    written = {"done": False}

    def on_frame(observation: e2e._StreamObservation) -> None:
        # Every frame the client can see is already past the pre-run snapshot,
        # which the worker captures before it publishes ``metadata``. So write
        # on the earliest frame whose turn has laid down the outputs directory,
        # rather than on a chosen frame, and keep the whole rest of the turn as
        # margin before the post-run scan.
        if written["done"]:
            return
        outputs = _outputs_dir(home, thread_id)
        if outputs is None:
            return
        written["done"] = True
        (outputs / ARTIFACT_NAME).write_text(
            "# Monthly report\n\nRevenue is up.\n",
            encoding="utf-8",
        )

    return on_frame, written


def _frames_of(observed: e2e._StreamObservation, event: str) -> list[Any]:
    return [payload for name, payload in observed.frames if name == event]


def _verdicts(observed: e2e._StreamObservation) -> list[Any]:
    """Delivery verdict frames only: the advisory channel also carries ``turn_progress``."""
    return [payload for name, payload in observed.frames if name == "custom" and isinstance(payload, dict) and str(payload.get("type", "")).startswith("artifact_delivery_")]


def _verdict_index(observed: e2e._StreamObservation) -> int:
    return next(index for index, (name, payload) in enumerate(observed.frames) if name == "custom" and isinstance(payload, dict) and str(payload.get("type", "")).startswith("artifact_delivery_"))


ARTIFACT_PATH = f"/mnt/user-data/outputs/{ARTIFACT_NAME}"


def _tagged_paths(history: Any) -> list[str]:
    """Every path the turn's messages report as handed over, in order.

    The tag is the presentation, whichever message carries it: this is the
    same field the browser reads to draw the chips under an answer.
    """
    paths: list[str] = []

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            tagged = (node.get("additional_kwargs") or {}).get("presented_files")
            if isinstance(tagged, list):
                paths.extend(path for path in tagged if isinstance(path, str))
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(history)
    return list(dict.fromkeys(paths))


def test_a_turn_that_produced_a_file_and_presented_none_of_it_delivers_it_anyway(
    delivery_gateway: e2e._Gateway,
) -> None:
    """The DF22 shape, end to end: the run succeeds and the person gets the file.

    Before this, the same turn ended ``error`` with
    ``artifact_delivery_incomplete`` and the file reached the person only
    through a recovery notice. The runtime now hands over what the turn
    produced and nobody presented, so the fence has nothing to fail.
    """
    base = delivery_gateway.loopback_url
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        on_frame, written = _produce_one_artifact_mid_turn(delivery_gateway.tmp_home, thread_id)
        observed = e2e._observe_stream(client, base, thread_id, csrf, "probe:text please", on_frame=on_frame)
        run = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()
        history = client.post(f"{base}/api/threads/{thread_id}/history", json={"limit": 20}, headers={"X-CSRF-Token": csrf}).json()
        delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/delivery").json()

    assert written["done"], "the turn produced no artifact, so nothing was exercised"
    assert observed.text_frames >= 1, "the turn answered"
    assert "error" not in observed.events, observed.events
    assert observed.events[-1] == "end", observed.events

    assert run["status"] == "success", "a requested artifact that exists is not an error"
    assert run.get("stop_reason") is None, run.get("stop_reason")
    assert not _verdicts(observed), "nothing to correct, so no notice"
    assert delivery == {"available": False, "version": 1}, "and no recovery-only delivery"

    # The file is handed over where the person reads, not only in the panel.
    assert ARTIFACT_PATH in _tagged_paths(history), "the turn's messages never reported the file as delivered"


def test_the_fence_still_speaks_when_the_runtime_cannot_hand_the_file_over(
    delivery_gateway: e2e._Gateway,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DF13's live notice and DF14's durable one, on the path that still needs them.

    Runtime delivery is best effort: it scans the filesystem, and a scan can
    fail. When it does, the run is exactly the run the tenant class saw, and
    the verdict has to reach both a live reader and one who reloads, saying
    the same thing path by path. Failing the scan is how that path is reached
    here, because with the middleware working there is no other way to reach
    it.
    """
    from deerflow.agents.middlewares import runtime_delivery_middleware as module

    async def unavailable(*_args: Any, **_kwargs: Any) -> Any:
        raise OSError("outputs scan unavailable")

    monkeypatch.setattr(module, "capture_workspace_snapshot", unavailable)

    base = delivery_gateway.loopback_url
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        on_frame, written = _produce_one_artifact_mid_turn(delivery_gateway.tmp_home, thread_id)
        observed = e2e._observe_stream(client, base, thread_id, csrf, "probe:text please", on_frame=on_frame)
        run = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()
        runs = client.get(f"{base}/api/threads/{thread_id}/runs").json()
        flagged = [row for row in runs if row["stop_reason"] == "artifact_delivery_incomplete"]
        delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/delivery").json()

    assert written["done"], "the turn produced no artifact, so the fence was never exercised"
    assert "error" not in observed.events, observed.events
    assert _verdict_index(observed) < observed.events.index("end"), observed.events

    live = _verdicts(observed)[-1]
    assert live["type"] == "artifact_delivery_incomplete"
    assert live["run_id"] == observed.run_id
    assert live["undelivered_count"] == 1
    assert live["undelivered_paths"] == [ARTIFACT_PATH]

    # The durable half, on the exact calls a reloaded page makes.
    assert run["status"] == "error"
    assert run["stop_reason"] == "artifact_delivery_incomplete"
    assert [row["run_id"] for row in flagged] == [observed.run_id]
    assert delivery["available"] is True
    assert delivery["run_id"] == live["run_id"]
    assert delivery["undelivered_paths"] == live["undelivered_paths"]
    assert delivery["undelivered_count"] == live["undelivered_count"] == 1
    assert delivery["message"] == live["message"], "the correction does not change wording on reload"


def test_an_ordinary_turn_on_the_same_gateway_still_ends_clean(
    delivery_gateway: e2e._Gateway,
) -> None:
    """The fence only speaks for runs that produced something."""
    base = delivery_gateway.loopback_url
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        observed = e2e._observe_stream(client, base, thread_id, csrf, "probe:text please")
        delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/delivery").json()

    assert observed.events[-1] == "end", observed.events
    assert "error" not in observed.events, observed.events
    assert not _verdicts(observed), observed.frames
    # And the route says so rather than 404ing or inventing an empty notice:
    # this is the answer for almost every run, so it has to be an ordinary one.
    assert delivery == {"available": False, "version": 1}
