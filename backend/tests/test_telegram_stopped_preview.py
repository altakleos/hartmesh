"""Opaque host handoff controls; actual SSE/transport provenance is separate."""

from copy import copy, deepcopy
from hashlib import sha256

import pytest


def issued(*, current=None):
    from app.telegram_bots.stopped_preview import StoppedSegment, _issue_stopped_preview

    identity = {"publication_id": "a" * 32, "scope": {"owner_user_id": "human", "thread_id": "thread"}}
    text = "Shown prefix"
    segment = StoppedSegment(
        span_id="model-call",
        message_id="model-message",
        sequence=1,
        text=text,
        text_sha256=sha256(text.encode()).hexdigest(),
        mode="edit",
        anchor_message_id=501,
        draft_id=None,
        part_index=0,
        part_id="shown-part",
        source_start=0,
        source_end=len(text),
    )
    return _issue_stopped_preview(publication_identity=identity, run_id="run", stop_id="original-stop", segments=(segment,), check_current=current or (lambda: True)), identity


def test_stopped_preview_take_is_single_use_but_last_fence_still_works():
    from app.telegram_bots.stopped_preview import check_stopped_preview, take_stopped_preview

    capability, identity = issued()
    data = take_stopped_preview(capability, publication_identity=identity, run_id="run")
    assert data.stop_id == "original-stop" and data.segments[0].anchor_message_id == 501
    assert data.segments[0].part_id == "shown-part" and data.segments[0].source_end == len("Shown prefix")
    check_stopped_preview(capability, publication_identity=identity, run_id="run")
    with pytest.raises(RuntimeError):
        take_stopped_preview(capability, publication_identity=identity, run_id="run")
    assert copy(capability) is deepcopy(capability) is capability


@pytest.mark.parametrize("change", ["retired", "different_run", "different_publication", "json"])
def test_stopped_handoff_cannot_rebind_or_revive(change):
    from app.telegram_bots.stopped_preview import check_stopped_preview, take_stopped_preview

    active = [True]
    capability, identity = issued(current=lambda: active[0])
    run_id = "run"
    if change == "retired":
        active[0] = False
    elif change == "different_run":
        run_id = "later-run"
    elif change == "different_publication":
        identity = {**identity, "publication_id": "b" * 32}
    else:
        capability = {"run_id": "run", "publication_id": "a" * 32}
    with pytest.raises(RuntimeError):
        take_stopped_preview(capability, publication_identity=identity, run_id=run_id)
    with pytest.raises(RuntimeError):
        check_stopped_preview(capability, publication_identity=identity, run_id=run_id)


def test_shown_capability_keeps_real_anchor_without_minting_stop_authority():
    from app.telegram_bots.stopped_preview import _issue_shown_preview, check_shown_preview, check_stopped_preview, take_shown_preview

    stopped, identity = issued()
    from app.telegram_bots.stopped_preview import take_stopped_preview

    data = take_stopped_preview(stopped, publication_identity=identity, run_id="run")
    shown = _issue_shown_preview(publication_identity=identity, run_id="run", segments=data.segments, check_current=lambda: True)
    assert take_shown_preview(shown, publication_identity=identity, run_id="run").segments == data.segments
    check_shown_preview(shown, publication_identity=identity, run_id="run")
    with pytest.raises(RuntimeError):
        check_stopped_preview(shown, publication_identity=identity, run_id="run")
