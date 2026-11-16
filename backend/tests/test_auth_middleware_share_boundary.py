"""Unimplemented share routes cannot acquire an anonymous read exemption."""

import pytest

from app.gateway.auth_middleware import _is_public


@pytest.mark.parametrize("suffix", ["", "/artifacts/report.html"])
def test_share_tokens_are_not_anonymous_authority(suffix):
    assert not _is_public("/api/shares/" + "a" * 43 + suffix)
