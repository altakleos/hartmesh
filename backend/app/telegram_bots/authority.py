"""Current account facts used by managed Telegram presentation."""


def get_local_provider():
    # Keep catalog and operator imports independent of Gateway startup.
    from app.gateway.deps import get_local_provider as provider

    return provider()


async def is_administrator(owner_user_id: str) -> bool:
    """Read the binding owner's effective administrator role at each use."""
    try:
        user = await get_local_provider().get_user(owner_user_id)
        return user is not None and user.system_role == "admin" and getattr(user, "role_limit", None) != "user"
    except Exception:
        # Missing or unreadable accounts confer no administrator presentation.
        return False
