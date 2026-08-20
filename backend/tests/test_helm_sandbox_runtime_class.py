"""Helm plumbing contract for the sandbox RuntimeClass selection."""

from __future__ import annotations

import pytest
from support.helm import deployment_env


@pytest.mark.parametrize(
    ("extra_args", "expected"),
    [
        ((), ""),
        (("--set", "sandbox.runtimeClassName=gvisor"), "gvisor"),
    ],
    ids=["cluster-default", "gvisor"],
)
def test_sandbox_runtime_class_value_renders_to_provisioner_env(
    extra_args: tuple[str, ...],
    expected: str,
) -> None:
    env = deployment_env("provisioner", *extra_args)

    assert env["SANDBOX_RUNTIME_CLASS"] == expected
