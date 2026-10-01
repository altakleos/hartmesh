"""An autouse fixture that undoes what ``AppConfig.from_file`` leaves behind.

Loading a config file applies it to process-wide singletons that
``reset_app_config()`` does not put back (``_apply_singleton_configs`` writes
about a dozen of them). Tests that load the tenant compose profile therefore
leak its PostgreSQL checkpointer into whatever the shard schedules next, which
then fails resolving the host ``postgres``. Only the checkpointer singleton is
restored here: it is the one with reach outside the loading module. The leak
itself belongs to ``from_file``, not to its callers.

Import the fixture into a test module to arm it::

    from _config_singleton_guard import restore_config_singletons  # noqa: F401
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest


@pytest.fixture(autouse=True)
def restore_config_singletons() -> Iterator[None]:
    from deerflow.config.app_config import reset_app_config
    from deerflow.config.checkpointer_config import get_checkpointer_config, set_checkpointer_config

    previous = get_checkpointer_config()
    try:
        yield
    finally:
        set_checkpointer_config(previous)
        reset_app_config()
