from __future__ import annotations

import pytest
from support import helm


def test_missing_helm_fails_in_ci(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(helm, "_HELM", None)
    monkeypatch.setenv("CI", "true")

    def unexpected_skip(reason: str) -> None:
        raise AssertionError(f"CI must fail instead of skip: {reason}")

    monkeypatch.setattr(helm.pytest, "skip", unexpected_skip)

    with pytest.raises(pytest.fail.Exception, match="helm is required in CI to verify rendered chart values"):
        helm.render_chart()


def test_missing_helm_skips_outside_ci(monkeypatch: pytest.MonkeyPatch) -> None:
    class SkipCalled(Exception):
        pass

    monkeypatch.setattr(helm, "_HELM", None)
    monkeypatch.delenv("CI", raising=False)

    def record_skip(reason: str) -> None:
        raise SkipCalled(reason)

    monkeypatch.setattr(helm.pytest, "skip", record_skip)

    with pytest.raises(SkipCalled, match="helm is required to verify rendered chart values"):
        helm.render_chart()
