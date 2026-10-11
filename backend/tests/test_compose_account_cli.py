"""Offline account creation shares bundle seat admission without Docker startup."""

from types import SimpleNamespace

import pytest
from test_local_add_user import _command, _local
from test_local_add_user import deployment as deployment

from deerflow.config.storage_spaces_config import StoragePlan, StorageSpacesConfig


@pytest.mark.parametrize("available", [False, True])
def test_offline_add_user_enforces_prepared_seats_without_runtime(deployment, capsys, monkeypatch, tmp_path, available):
    from app.gateway import storage_spaces as ss

    plan = StoragePlan(users=1, user_gib=1, ai_assistants_per_user=0, ai_assistant_gib=1, ai_employees=0, ai_employee_gib=1, company_gib=1)
    config = _local(open_=False).model_copy(update={"storage_spaces": StorageSpacesConfig(enabled=True, inventory_path=str(tmp_path / "inventory.json"), plan=plan)})
    deployment(config)

    def manifest(_path):
        if not available:
            raise FileNotFoundError("injected missing pool")
        return SimpleNamespace(volumes={"slot": SimpleNamespace(kind="user")})

    async def forbidden(*_args, **_kwargs):
        raise AssertionError("offline command must not initialize Docker storage")

    monkeypatch.setattr(ss.PreparedVolumeCatalog, "from_manifest", manifest)
    monkeypatch.setattr(ss, "initialize_storage_spaces", forbidden)
    if available:
        code, first, _ = _command(capsys, "--email", "first@example.com")
        assert code == 0, first
    code, refused, _ = _command(capsys, "--email", "second@example.com")
    assert code == 1 and refused["error"] == "no_seats_left"
    assert (refused["allowed"], refused["in_use"]) == ((1, 1) if available else (0, 0))
    assert "one_time_password" not in refused
