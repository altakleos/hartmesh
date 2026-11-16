"""Live human accounts for ingress SQL tests, with owned cache restoration."""

from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import text

from app.gateway import deps  # Gateway imports its dependencies before async lifespan work.
from app.gateway.auth.local_provider import LocalAuthProvider
from app.gateway.auth.repositories.sqlite import SQLiteUserRepository
from app.telegram_bots.authority import TelegramAuthority
from deerflow.config.app_config import AppConfig
from deerflow.persistence.user.access import DisabledIdentityRow, IdentityHoldRow, RoleLimitRow
from deerflow.persistence.user.model import UserRow


async def install_live_owner(db, monkeypatch, *, owner=None, email="ingress@example.com", provider=False, local_enabled=True):
    from app.telegram_bots import pages
    from deerflow.config import app_config

    async with db.engine.begin() as connection:
        if db.dialect == "sqlite":
            await connection.execute(text("BEGIN"))
        await connection.run_sync(lambda sync: UserRow.metadata.create_all(sync, tables=[UserRow.__table__, DisabledIdentityRow.__table__, IdentityHoldRow.__table__, RoleLimitRow.__table__]))
    owner = owner or str(uuid4())
    async with db.sessions.begin() as session:
        session.add(
            UserRow(
                id=owner, email=email, password_hash="unusable-fixture", system_role="user", oauth_provider="fixture" if provider else None, oauth_id=owner if provider else None, oauth_issuer="https://issuer.example" if provider else None
            )
        )
    users = SQLiteUserRepository(db.sessions)
    monkeypatch.setattr(deps, "_cached_repo", users)
    monkeypatch.setattr(deps, "_cached_local_provider", LocalAuthProvider(users))
    auth = {"local": {"enabled": local_enabled}}
    if not local_enabled:
        auth["oidc"] = {"enabled": True, "providers": {"fixture": {"display_name": "Fixture", "issuer": "https://issuer.example", "client_id": "fixture"}}}
    config = AppConfig.model_validate({"sandbox": {"use": "deerflow.sandbox.local:LocalSandboxProvider"}, "auth": auth})
    # Final route snapshots inspect the loaded cache as well as the getter.
    # Install this fixture's real config and restore every cache field afterward.
    for name in ("_app_config", "_app_config_path", "_app_config_mtime", "_app_config_signature", "_app_config_is_custom", "_app_config_source", "_app_config_extensions_source"):
        monkeypatch.setattr(app_config, name, getattr(app_config, name))
    app_config.set_app_config(config)
    monkeypatch.setattr(app_config, "get_app_config", lambda: config)
    # Pages retains an imported getter, including when loaded before this fixture.
    monkeypatch.setattr(pages, "get_app_config", lambda: config)
    authority = TelegramAuthority(links=db.links, connections=db.connections)
    return SimpleNamespace(id=owner, users=users, config=config, authority=authority)
