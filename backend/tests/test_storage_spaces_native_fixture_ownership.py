"""An ambiguous create must not permit deleting an owned fixture's backing."""

import subprocess
from types import SimpleNamespace

import pytest
from _storage_spaces_native_test_support import prepare_consumer_container


def test_create_effect_before_lost_acknowledgement_preserves_fixture_uncertainty(tmp_path):
    plan = SimpleNamespace(id="a" * 32, host_id="fixture", views=())
    effect = tmp_path / "created-container"
    containers, containment = [], {"confirmed": True}

    def create_then_error(arguments, **_kwargs):
        effect.write_text("task-owned stopped container", encoding="utf-8")
        raise subprocess.TimeoutExpired(arguments, 30)

    with pytest.raises(subprocess.TimeoutExpired):
        prepare_consumer_container(plan, "fixture-image", "pass", containers, containment, runner=create_then_error)
    assert effect.exists() and not containers
    assert containment["confirmed"] is False


@pytest.mark.parametrize("previously_confirmed", [True, False])
def test_exact_capture_keeps_owned_identity_and_never_clears_earlier_uncertainty(previously_confirmed):
    plan = SimpleNamespace(id="a" * 32, host_id="fixture", views=())
    containers, containment = [], {"confirmed": previously_confirmed}

    def acknowledged(arguments, **_kwargs):
        return subprocess.CompletedProcess(arguments, 0, stdout="c" * 64 + "\n")

    assert prepare_consumer_container(plan, "fixture-image", "pass", containers, containment, runner=acknowledged) == "c" * 64
    assert containers == [("c" * 64, plan.id)]
    assert containment["confirmed"] is previously_confirmed
