"""Capture is an explicit creating request, never a metadata convention."""

import pytest
from pydantic import ValidationError

from app.gateway.run_models import RunCreateRequest


def test_creating_request_accepts_explicit_output_capture():
    body = RunCreateRequest.model_validate({"output_capture": {"version": 1}})
    assert body.output_capture.model_dump() == {"version": 1}


@pytest.mark.parametrize("version", [True, False, 1.0, "1", None, 0, 2])
def test_capture_version_is_a_required_literal_integer(version):
    with pytest.raises(ValidationError) as failure:
        RunCreateRequest.model_validate({"output_capture": {"version": version}})
    assert any(error["loc"] == ("output_capture", "version") for error in failure.value.errors())


@pytest.mark.parametrize("capture", [{}, {"version": 1, "extra": "not-allowed"}, True, "enabled"])
def test_capture_has_a_closed_typed_shape(capture):
    with pytest.raises(ValidationError):
        RunCreateRequest.model_validate({"output_capture": capture})


@pytest.mark.parametrize("data", [{}, {"output_capture": None}])
def test_ordinary_creating_request_does_not_request_capture(data):
    body = RunCreateRequest.model_validate(data)
    assert "output_capture" not in body.model_dump(exclude_none=True)


def test_metadata_cannot_request_output_capture():
    body = RunCreateRequest(metadata={"output_capture": {"version": 1}}, context={"output_capture": {"version": 1}})
    assert "output_capture" not in body.model_dump(exclude_none=True)
