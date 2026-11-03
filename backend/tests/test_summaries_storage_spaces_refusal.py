"""Project document summaries cannot run on Storage Spaces (upstream sync, plan §5.2 k)."""

from types import SimpleNamespace

import pytest

from app.gateway.app import refuse_summaries_on_storage_spaces


def _config(*, spaces: bool, summaries: bool):
    return SimpleNamespace(storage_spaces=SimpleNamespace(enabled=spaces), projects=SimpleNamespace(summaries_enabled=summaries))


def test_summaries_on_storage_spaces_refuse_the_start():
    with pytest.raises(RuntimeError, match="projects.summaries_enabled is not supported with storage_spaces.enabled"):
        refuse_summaries_on_storage_spaces(_config(spaces=True, summaries=True))


@pytest.mark.parametrize(("spaces", "summaries"), [(True, False), (False, True), (False, False)])
def test_either_one_alone_starts(spaces, summaries):
    refuse_summaries_on_storage_spaces(_config(spaces=spaces, summaries=summaries))


def test_a_config_without_the_sections_starts():
    refuse_summaries_on_storage_spaces(SimpleNamespace())


def test_the_lifespan_calls_the_refusal_before_starting_summary_workers():
    """Kept in the startup path, not only importable."""
    import inspect

    from app.gateway import app as app_module

    source = inspect.getsource(app_module._gateway_lifespan)
    assert source.index("refuse_summaries_on_storage_spaces(startup_config)") < source.index("init_summary_generator(")
