"""Mixed offers suppress document captions without fabricating answer content."""

import pytest

from app.telegram_bots.files import plan_output_files


def photo():
    return {"path": "/mnt/user-data/outputs/chart.png", "filename": "chart.png", "size": 123, "mime_type": "image/png", "state": "ready"}


def test_explicit_mixed_no_caption_preserves_normal_single_photo_recipe():
    plan = plan_output_files([photo()], answer_visible="", capacity=50_000_000, max_bytes=50_000_000, caption_allowed=False)
    assert plan.caption is False
    assert plan.parts == ({"kind": "photo", "files": [photo()], "unavailable": None},)
    assert plan_output_files([photo()], answer_visible="", capacity=50_000_000, max_bytes=50_000_000) == plan_output_files([photo()], answer_visible="", capacity=50_000_000, max_bytes=50_000_000, caption_allowed=True)
    assert plan_output_files([photo()], answer_visible="", capacity=50_000_000, max_bytes=50_000_000).caption is True


@pytest.mark.parametrize("value", [None, 0, 1, "false"])
def test_caption_mode_refuses_ambiguous_non_boolean(value):
    with pytest.raises(ValueError):
        plan_output_files([photo()], answer_visible="", capacity=50_000_000, max_bytes=50_000_000, caption_allowed=value)
