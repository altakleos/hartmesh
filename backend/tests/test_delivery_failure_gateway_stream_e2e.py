"""A browser watching a delivery failure sees a failure (hartmesh-tenancy/DF13).

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


def test_a_turn_that_never_presented_its_file_reports_the_failure_without_breaking_the_stream(
    delivery_gateway: e2e._Gateway,
) -> None:
    base = delivery_gateway.loopback_url
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        on_frame, written = _produce_one_artifact_mid_turn(delivery_gateway.tmp_home, thread_id)
        observed = e2e._observe_stream(client, base, thread_id, csrf, "probe:text please", on_frame=on_frame)
        run = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}").json()

    assert written["done"], "the turn produced no artifact, so the fence was never exercised"
    assert observed.text_frames >= 1, "the turn must have shown prose, or there is no contradiction to correct"
    assert "error" not in observed.events, observed.events
    assert observed.events.index("custom") < observed.events.index("end"), observed.events

    detail = _frames_of(observed, "custom")[-1]
    assert detail["type"] == "artifact_delivery_incomplete"
    assert detail["run_id"] == observed.run_id
    assert detail["undelivered_count"] == 1
    assert detail["undelivered_paths"] == [f"/mnt/user-data/outputs/{ARTIFACT_NAME}"]

    # The durable half, on the exact call a browser already makes on every
    # reconnect. This is the assertion that survives a reload, and it is what
    # infra asked for: the verdict outlives the connection that carried it.
    assert run["status"] == "error"
    assert run["stop_reason"] == "artifact_delivery_incomplete"


def test_the_notice_survives_the_connection_that_carried_it(
    delivery_gateway: e2e._Gateway,
) -> None:
    """A reader who reloads gets the same correction, with the same files.

    DF13 published the verdict and stopped: the frame is page-local state, so
    the tenant-class rerun found both failed turns keeping their stored ``error``
    and stop reason across a reload while the notice under the turn — and the
    way to the files it offered — was gone. This is the route that gives it
    back, and it must agree with the frame path by path (hartmesh-tenancy/DF14).
    """
    base = delivery_gateway.loopback_url
    with httpx.Client() as client:
        csrf, thread_id = e2e._register_and_create_thread(client, base)
        on_frame, written = _produce_one_artifact_mid_turn(delivery_gateway.tmp_home, thread_id)
        observed = e2e._observe_stream(client, base, thread_id, csrf, "probe:text please", on_frame=on_frame)
        # Exactly the calls a reloaded page makes: the thread's runs, then the
        # verdict for the one whose stop reason says there is one.
        runs = client.get(f"{base}/api/threads/{thread_id}/runs").json()
        flagged = [run for run in runs if run["stop_reason"] == "artifact_delivery_incomplete"]
        delivery = client.get(f"{base}/api/threads/{thread_id}/runs/{observed.run_id}/delivery").json()

    assert written["done"], "the turn produced no artifact, so the fence was never exercised"
    assert [run["run_id"] for run in flagged] == [observed.run_id]

    live = _frames_of(observed, "custom")[-1]
    assert delivery["available"] is True
    assert delivery["run_id"] == live["run_id"] == observed.run_id
    assert delivery["undelivered_paths"] == live["undelivered_paths"]
    assert delivery["undelivered_count"] == live["undelivered_count"] == 1
    # The same sentence, so the correction does not change wording on reload.
    assert delivery["message"] == live["message"]


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
    assert not _frames_of(observed, "custom"), observed.frames
    # And the route says so rather than 404ing or inventing an empty notice:
    # this is the answer for almost every run, so it has to be an ordinary one.
    assert delivery == {"available": False, "version": 1}
