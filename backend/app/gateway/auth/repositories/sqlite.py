"""SQLAlchemy-backed UserRepository implementation.

Uses the shared async session factory from
``deerflow.persistence.engine`` — the ``users`` table lives in the
same database as ``threads_meta``, ``runs``, ``run_events``, and
``feedback``.

Constructor takes the session factory directly (same pattern as the
other four repositories in ``deerflow.persistence.*``). Callers
construct this after ``init_engine_from_config()`` has run.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import case, delete, func, literal, select, text, tuple_, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.gateway.auth.models import User
from app.gateway.auth.repositories.base import UserNotFoundError, UserRepository
from deerflow.persistence.user.access import LIMIT_ROLES, ROLES, DisabledIdentityRow, IdentityHoldRow, RoleLimitRow, identity_lock_key, issuer_key, limited_role
from deerflow.persistence.user.model import OAUTH_IDENTITY_INDEX_NAME, UserRow

# ``email`` is ``mapped_column(unique=True, index=True)``, which SQLAlchemy
# (and 0001_baseline) realise as a single UNIQUE INDEX -- not a named UNIQUE
# constraint -- so a Postgres duplicate reports the index name here.
_EMAIL_UNIQUE_INDEX_NAME = "ix_users_email"


def _driver_constraint_name(exc: IntegrityError) -> str | None:
    """The violated constraint's name from the driver exception, or ``None``
    when the driver does not expose one.

    ``exc.orig`` is NOT the raw driver error. SQLAlchemy's asyncpg dialect
    re-raises a plain ``AsyncAdapt_asyncpg_dbapi.IntegrityError`` built from a
    rendered string and carrying only ``pgcode``/``sqlstate``
    (``sqlalchemy/dialects/postgresql/asyncpg.py::_handle_exception``); the
    real ``asyncpg.UniqueViolationError`` — the one with ``constraint_name`` —
    survives as ``exc.orig.__cause__`` (``raise translated_error from error``).
    aiosqlite exposes no constraint name at all. Check the wrapper, then its
    cause.
    """
    for obj in (exc.orig, getattr(exc.orig, "__cause__", None)):
        name = getattr(obj, "constraint_name", None)
        if name:
            return str(name)
    return None


def _is_oauth_identity_violation(exc: IntegrityError) -> bool:
    """Distinguish the ``idx_users_oauth_identity`` (oauth_provider, oauth_id)
    unique-index violation from any OTHER ``IntegrityError`` reaching this
    commit (a duplicate primary key, or a duplicate-email race that slipped
    past the pre-check above).

    Never uses the SQLAlchemy wrapper's ``str(exc)``: it embeds the full
    failed INSERT statement, whose column list names
    ``oauth_provider``/``oauth_id`` on every call regardless of which
    constraint fired, so a substring check on it misclassifies every
    commit-time ``IntegrityError`` on this table as an OAuth conflict
    (reproduced on SQLite: a duplicate ``id`` raised "OAuth account already
    linked: None/None").

    Postgres: match :data:`OAUTH_IDENTITY_INDEX_NAME` against the driver
    constraint name (see :func:`_driver_constraint_name`). SQLite: no
    structured name, only a message naming the columns (``"UNIQUE constraint
    failed: users.oauth_provider, users.oauth_id"``) — require BOTH oauth
    column names, not a bare "oauth" substring.
    """
    name = _driver_constraint_name(exc)
    if name is not None:
        return name == OAUTH_IDENTITY_INDEX_NAME
    message = str(exc.orig).lower()
    return "oauth_provider" in message and "oauth_id" in message


def _is_email_violation(exc: IntegrityError) -> bool:
    """True when ``exc`` is the ``ix_users_email`` uniqueness violation, using
    the same driver-exception inspection as
    :func:`_is_oauth_identity_violation` (never ``str(exc)``, whose INSERT text
    names every column)."""
    name = _driver_constraint_name(exc)
    if name is not None:
        return name == _EMAIL_UNIQUE_INDEX_NAME
    return "users.email" in str(exc.orig).lower()


def _is_uniqueness_violation(exc: IntegrityError) -> bool:
    """True for a unique-index / primary-key violation specifically, as
    opposed to a NOT NULL / CHECK / foreign-key ``IntegrityError`` on the same
    INSERT -- only the former means "a user like this already exists"."""
    sqlstate = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
    if sqlstate is not None:
        return sqlstate == "23505"  # unique_violation
    message = str(exc.orig).lower()
    return "unique constraint failed" in message or "primary key constraint failed" in message


def _violated_constraint(exc: IntegrityError) -> str | None:
    """Best-effort name of the constraint behind ``exc``, for a diagnostic that
    does not pin an unattributed violation on a specific column. Uses the
    driver constraint name where available, else the column(s) SQLite names in
    its message (``"UNIQUE constraint failed: users.id"``)."""
    name = _driver_constraint_name(exc)
    if name:
        return name
    marker = "constraint failed: "
    message = str(exc.orig)
    if marker in message:
        return message.split(marker, 1)[1].splitlines()[0].strip() or None
    return None


def _aware(value: datetime | None) -> datetime | None:
    """SQLite loses tzinfo on read; reattach UTC so timestamps compare reliably."""
    if value is None or value.tzinfo is not None:
        return value
    return value.replace(tzinfo=UTC)


def _normalize_email(email: str) -> str:
    """Canonicalise an email address for storage and lookup.

    An email identifies exactly one account regardless of the case the client
    sends. The two write paths would otherwise disagree: local registration
    normalises through ``EmailStr``, which lowercases only the *domain* and
    keeps the local-part case (``Victim@X.COM`` -> ``Victim@x.com``), while OIDC
    provisioning lowercases the whole address (``-> victim@x.com``). Combined
    with the previous case-sensitive lookup, ``Victim@x.com`` and
    ``victim@x.com`` resolved to two separate rows, defeating the invariant
    that a local account blocks an SSO login on the same email.

    Canonicalising to lowercase at every write site and matching
    case-insensitively on read closes that gap for new accounts while letting
    existing mixed-case rows keep resolving, without a destructive bulk rewrite.
    """
    return email.lower()


# Fixed 63-bit key for pg_advisory_xact_lock(bigint); scopes the first-admin
# claim without colliding with other advisory-lock users in this database.
_FIRST_ADMIN_LOCK_KEY = int.from_bytes(hashlib.sha256(b"deerflow:auth:first-admin-claim").digest()[:8], "big") & 0x7FFFFFFFFFFFFFFF


class SQLiteUserRepository(UserRepository):
    """Async user repository backed by the shared SQLAlchemy engine."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._sf = session_factory

    # ── Converters ────────────────────────────────────────────────────

    @staticmethod
    def _row_to_user(row: UserRow, disabled_at: datetime | None = None, role_limit: str | None = None) -> User:
        return User(
            id=UUID(row.id),
            email=row.email,
            password_hash=row.password_hash,
            # The role every reader sees: the stored one, held at the limit.
            # The column is kept at or below it too, but a write that carries
            # a role it read earlier back over the row (``update_user``) can
            # still land above it; this read is what makes that harmless.
            system_role=limited_role(row.system_role, role_limit),  # type: ignore[arg-type]
            # SQLite loses tzinfo on read; reattach UTC so downstream
            # code can compare timestamps reliably.
            created_at=_aware(row.created_at),
            oauth_provider=row.oauth_provider,
            oauth_id=row.oauth_id,
            oauth_issuer=row.oauth_issuer,
            needs_setup=row.needs_setup,
            token_version=row.token_version,
            last_sign_in_at=_aware(row.last_sign_in_at),
            email_released_from=row.email_released_from,
            disabled_at=_aware(disabled_at),
            role_limit=role_limit,
        )

    async def _disabled_at(self, session: AsyncSession, row: UserRow) -> datetime | None:
        """Whether the deployer turned this identity off: the one place the fact lives.

        Only a provider account has an identity to look up; a local account
        cannot be turned off this way and never matches. A provider account
        linked before the issuer was recorded (NULL, see 0039) matches on its
        subject alone: fail closed, since the row will adopt the configured
        issuer at its next sign-in and until then only the subject is known.
        """
        if not row.oauth_id or not row.oauth_provider:
            return None
        stmt = select(DisabledIdentityRow.disabled_at).where(DisabledIdentityRow.subject == row.oauth_id)
        if row.oauth_issuer:
            stmt = stmt.where(DisabledIdentityRow.issuer == issuer_key(row.oauth_issuer))
        return await session.scalar(stmt.limit(1))

    async def _role_limit(self, session: AsyncSession, row: UserRow) -> str | None:
        """The highest role the deployer lets this identity hold, or ``None``.

        Matched as :meth:`_disabled_at` matches, and failing closed the same
        way on a row whose issuer was never recorded.
        """
        if not row.oauth_id or not row.oauth_provider:
            return None
        stmt = select(RoleLimitRow.role).where(RoleLimitRow.subject == row.oauth_id)
        if row.oauth_issuer:
            stmt = stmt.where(RoleLimitRow.issuer == issuer_key(row.oauth_issuer))
        limits = (await session.scalars(stmt)).all()
        # Only an unrecorded issuer can match more than one; the lowest wins,
        # and one this code does not know counts lowest of all.
        return min(limits, key=lambda held: ROLES.index(held) if held in ROLES else -1) if limits else None

    async def _load(self, session: AsyncSession, row: UserRow | None) -> User | None:
        if row is None:
            return None
        return self._row_to_user(row, await self._disabled_at(session, row), await self._role_limit(session, row))

    @staticmethod
    def _user_to_row(user: User) -> UserRow:
        return UserRow(
            id=str(user.id),
            email=user.email,
            password_hash=user.password_hash,
            system_role=user.system_role,
            created_at=user.created_at,
            oauth_provider=user.oauth_provider,
            oauth_id=user.oauth_id,
            oauth_issuer=user.oauth_issuer,
            needs_setup=user.needs_setup,
            token_version=user.token_version,
            last_sign_in_at=user.last_sign_in_at,
            email_released_from=user.email_released_from,
        )

    # ── CRUD ──────────────────────────────────────────────────────────

    async def create_user(self, user: User) -> User:
        """Insert a new user. Raises ``ValueError`` on any uniqueness
        violation -- duplicate email, a duplicate (provider, oauth_id) pair
        for an OAuth-linked account, or a duplicate id -- with a message
        naming the specific conflict. Other ``IntegrityError``\\ s (NOT NULL,
        CHECK, foreign key) propagate unchanged.

        The email is canonicalised to lowercase before insert so the existing
        unique constraint enforces case-insensitive uniqueness for new rows and
        the returned ``User`` reflects the stored form.
        """
        async with self._sf() as session:
            await self._insert_user(session, user)
            await session.commit()
        return user

    async def _insert_user(self, session: AsyncSession, user: User) -> User:
        """Pre-check the email and flush *user*; the caller owns the transaction.

        Shared by :meth:`create_user` and :meth:`create_first_admin` so both
        report the same uniqueness conflicts as ``ValueError``.
        """
        user.email = _normalize_email(user.email)
        # A person demoted before their first sign-in is created at the
        # limit, and the account handed back says so. Read under the
        # identity's lock, and applied again after the flush below, so a
        # limit that commits between this read and the insert still holds.
        provider_identity = bool(user.oauth_provider and user.oauth_id)
        if provider_identity:
            if user.oauth_issuer:
                await self._hold_identity(session, user.oauth_issuer, str(user.oauth_id))
            probe = UserRow(oauth_provider=user.oauth_provider, oauth_id=user.oauth_id, oauth_issuer=user.oauth_issuer)
            user.role_limit = await self._role_limit(session, probe)
            user.system_role = limited_role(user.system_role, user.role_limit)  # type: ignore[assignment]
        row = self._user_to_row(user)
        # The unique constraint is case-sensitive, so it cannot catch a
        # canonical address colliding with a mixed-case legacy row.
        existing = select(UserRow.id).where(func.lower(UserRow.email) == user.email).limit(1)
        if await session.scalar(existing) is not None:
            raise ValueError(f"Email already registered: {user.email}")
        session.add(row)
        try:
            await session.flush()
            if provider_identity:
                await session.execute(update(UserRow).where(UserRow.id == row.id).values(system_role=self._role_within_limit(user.system_role, user.oauth_issuer)))
                stored = await session.scalar(select(UserRow.system_role).where(UserRow.id == row.id))
                if stored != user.system_role:
                    user.system_role = stored  # type: ignore[assignment]
                    user.role_limit = await self._role_limit(session, row)
        except IntegrityError as exc:
            await session.rollback()
            # The email pre-check above already ruled out an email
            # collision under normal (non-racing) conditions, so
            # IntegrityErrors reaching here are usually
            # idx_users_oauth_identity -- but not always (a duplicate
            # primary key, or an email collision that raced past the
            # pre-check). Attribute the failure to the constraint that
            # actually fired instead of assuming any one of them.
            if _is_oauth_identity_violation(exc):
                raise ValueError(f"OAuth account already linked: {user.oauth_provider}/{user.oauth_id}") from exc
            if _is_email_violation(exc):
                # A duplicate address that got past the pre-check: a
                # concurrent insert of the same email.
                raise ValueError(f"Email already registered: {user.email}") from exc
            if _is_uniqueness_violation(exc):
                # Some other unique index / primary key (in practice a
                # duplicate id). "Already exists" fits, but don't dress it
                # up as an email conflict for an address that isn't
                # registered.
                constraint = _violated_constraint(exc)
                raise ValueError(f"User already exists (constraint: {constraint})" if constraint else "User already exists") from exc
            # A NOT NULL / CHECK / foreign-key IntegrityError is not a
            # "user already exists" condition and not part of this
            # method's ValueError contract -- let it propagate.
            raise
        return user

    @staticmethod
    async def _serialize_first_admin_claim(session: AsyncSession) -> None:
        """Serialize concurrent first-admin claims before the count is read.

        SQLite takes the database write lock up front (``BEGIN IMMEDIATE``),
        the idiom the project repositories use for read-then-write
        transactions. Postgres has no row to lock — the table is empty on a
        first boot — so it takes a transaction-scoped advisory lock on a
        fixed key, the way the channel OAuth scope cap does.

        A dialect with neither strategy raises: this is the point that makes
        :meth:`create_first_admin` atomic, and falling through would leave a
        plain check-then-act that lets two first-boot requests both create an
        admin. The engine builds only these two dialects today, so the raise
        is a guard for a future backend, not a reachable path.
        """
        dialect = session.get_bind().dialect.name
        if dialect == "sqlite":
            await session.execute(text("BEGIN IMMEDIATE"))
        elif dialect == "postgresql":
            await session.execute(text("SELECT pg_advisory_xact_lock(:lock_key)"), {"lock_key": _FIRST_ADMIN_LOCK_KEY})
        else:
            raise RuntimeError(f"Cannot serialize the first-admin claim: no locking strategy for SQL dialect {dialect!r}")

    async def create_first_admin(self, user: User) -> User | None:
        """Insert *user* as the first admin, or return None if one already exists.

        The admin count and the insert share one transaction and writers are
        serialized first, so two concurrent first-boot requests cannot both
        read an empty system and both create an admin. ``None`` means the
        claim was lost — the caller reports "already initialized" — and the
        uniqueness conflicts of :meth:`create_user` still raise ``ValueError``.
        """
        async with self._sf() as session:
            await self._serialize_first_admin_claim(session)
            admin_count = await session.scalar(select(func.count()).select_from(UserRow).where(UserRow.system_role == "admin"))
            if admin_count:
                return None
            await self._insert_user(session, user)
            await session.commit()
        return user

    async def get_user_by_id(self, user_id: str) -> User | None:
        async with self._sf() as session:
            return await self._load(session, await session.get(UserRow, user_id))

    async def get_user_by_email(self, email: str) -> User | None:
        # Case-insensitive match: an account is keyed by its email regardless of
        # the case the caller supplies (see ``_normalize_email``). ``.first()``
        # with a deterministic ``created_at`` ordering resolves to the oldest
        # account instead of raising if a pre-fix database already holds two
        # rows differing only in case, so the fix never turns a legacy duplicate
        # pair into a 500. ``id`` is a secondary tiebreaker so the choice stays
        # deterministic even if two legacy rows share the same ``created_at``.
        stmt = select(UserRow).where(func.lower(UserRow.email) == _normalize_email(email)).order_by(UserRow.created_at, UserRow.id).limit(1)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return await self._load(session, result.scalars().first())

    async def update_user(self, user: User) -> User:
        async with self._sf() as session:
            row = await session.get(UserRow, str(user.id))
            if row is None:
                # Hard fail on concurrent delete: callers (reset_admin,
                # password change handlers, _ensure_admin_user) all
                # fetched the user just before this call, so a missing
                # row here means the row vanished underneath us. Silent
                # success would let the caller log "password reset" for
                # a row that no longer exists.
                raise UserNotFoundError(f"User {user.id} no longer exists")
            # Canonicalise the email only when it actually changes, comparing
            # case-insensitively against the stored value, then mirror the
            # persisted value back onto the returned object. Re-lowercasing an
            # *unchanged* legacy mixed-case email — e.g. a password-only update
            # on a pre-fix ``Victim@x.com`` row while a canonical ``victim@x.com``
            # row also exists — would rewrite it onto the other row's unique
            # email and raise IntegrityError, surfacing as a 500 on the
            # change-password / reset-admin paths that do not catch it. Guarding
            # against the *raw* stored value (``canonical != row.email``) is not
            # enough: the mixed-case row's canonical form still differs from its
            # own stored casing, so it would rewrite and collide anyway. A genuine
            # change still normalises, so the unique constraint keeps enforcing
            # case-insensitive uniqueness for updated rows; get_user_by_email
            # already resolves legacy mixed-case rows case-insensitively on read.
            canonical_email = _normalize_email(user.email)
            if canonical_email != _normalize_email(row.email):
                row.email = canonical_email
            user.email = row.email
            row.password_hash = user.password_hash
            row.system_role = user.system_role
            row.oauth_provider = user.oauth_provider
            row.oauth_id = user.oauth_id
            row.oauth_issuer = user.oauth_issuer
            row.needs_setup = user.needs_setup
            row.token_version = user.token_version
            row.last_sign_in_at = user.last_sign_in_at
            # Carried like every other field, and for the same reason the
            # sign-in write clears it: an account that changes its address
            # through here and kept the column set would read as released
            # while holding a real address, and ``release-email`` would then
            # refuse to give that address up.
            row.email_released_from = user.email_released_from
            await session.commit()
        return user

    async def rehash_password(self, user_id: str, *, expected_password_hash: str, expected_token_version: int, password_hash: str) -> User | None:
        stmt = update(UserRow).where(UserRow.id == user_id, UserRow.password_hash == expected_password_hash, UserRow.token_version == expected_token_version).values(password_hash=password_hash).returning(UserRow)
        async with self._sf() as session:
            row = (await session.execute(stmt)).scalar_one_or_none()
            # Materialize RETURNING before commit. A fresh SELECT afterwards
            # could give a stale password verification a reset's valid version.
            result = await self._load(session, row)
            await session.commit()
            return result

    async def replace_password(
        self,
        user_id: str,
        password_hash: str,
        *,
        expected_password_hash: str | None = None,
        expected_token_version: int | None = None,
        new_email: str | None = None,
        needs_setup: bool | None = None,
    ) -> User | None:
        stmt = update(UserRow).where(UserRow.id == user_id)
        if expected_password_hash is not None:
            stmt = stmt.where(UserRow.password_hash == expected_password_hash)
        if expected_token_version is not None:
            stmt = stmt.where(UserRow.token_version == expected_token_version)
        values = {"password_hash": password_hash, "token_version": UserRow.token_version + 1}
        if new_email is not None:
            canonical = _normalize_email(new_email)
            changed = func.lower(UserRow.email) != canonical
            values["email"] = case((changed, canonical), else_=UserRow.email)
            values["email_released_from"] = case((changed, None), else_=UserRow.email_released_from)
        if needs_setup is not None:
            values["needs_setup"] = needs_setup
        async with self._sf() as session:
            try:
                row = (await session.execute(stmt.values(**values).returning(UserRow))).scalar_one_or_none()
                result = await self._load(session, row)
                await session.commit()
            except IntegrityError as exc:
                await session.rollback()
                if _is_email_violation(exc):
                    raise ValueError("Email already registered") from exc
                raise
            return result

    async def count_users(self) -> int:
        stmt = select(func.count()).select_from(UserRow)
        async with self._sf() as session:
            return await session.scalar(stmt) or 0

    async def list_user_ids(self) -> list[str]:
        stmt = select(UserRow.id).order_by(UserRow.created_at, UserRow.id)
        async with self._sf() as session:
            result = await session.scalars(stmt)
            return list(result)

    async def count_admin_users(self) -> int:
        """Accounts whose role reads ``admin``: stored so, and held there by no limit."""
        stmt = select(UserRow).where(UserRow.system_role == "admin")
        async with self._sf() as session:
            rows = (await session.execute(stmt)).scalars().all()
            return sum([limited_role(row.system_role, await self._role_limit(session, row)) == "admin" for row in rows])

    async def get_user_by_oauth(self, provider: str, oauth_id: str) -> User | None:
        stmt = select(UserRow).where(UserRow.oauth_provider == provider, UserRow.oauth_id == oauth_id)
        async with self._sf() as session:
            result = await session.execute(stmt)
            return await self._load(session, result.scalar_one_or_none())

    async def get_user_by_identity(self, issuer: str, subject: str) -> User | None:
        """The account an identity provider's ``(issuer, subject)`` created, if any.

        Usually exactly one; see :meth:`list_users_by_identity` for when it is
        not, and for the order the first is picked in.
        """
        accounts = await self.list_users_by_identity(issuer, subject)
        return accounts[0] if accounts else None

    async def list_users_by_identity(self, issuer: str, subject: str) -> list[User]:
        """Every account an identity provider's ``(issuer, subject)`` created.

        The uniqueness the schema enforces is ``(oauth_provider, oauth_id)``,
        not ``(issuer, subject)``: two configured providers may point at one
        issuer, and then one person at that issuer has two accounts, one per
        provider name. A caller that acts on a single row has to say so rather
        than take whichever came back first.

        Rows that record this issuer come first; a row linked before the issuer
        was recorded (NULL) follows, because the deployer names the issuer the
        provider is configured for, which is the one that row will adopt at its
        next sign-in.
        """
        stmt = select(UserRow).where(UserRow.oauth_id == subject, UserRow.oauth_provider.is_not(None)).order_by(UserRow.created_at, UserRow.id)
        async with self._sf() as session:
            rows = (await session.execute(stmt)).scalars().all()
            recorded = [row for row in rows if row.oauth_issuer and issuer_key(row.oauth_issuer) == issuer_key(issuer)]
            adopting = [row for row in rows if not row.oauth_issuer]
            return [await self._load(session, row) for row in (*recorded, *adopting)]  # type: ignore[misc]

    async def end_sessions(self, user_id: str) -> bool:
        """Invalidate every session of the account: one atomic increment of ``token_version``.

        Atomic so a sign-in writing the row at the same moment cannot carry
        a stale version back over the bump.
        """
        async with self._sf() as session:
            result = await session.execute(update(UserRow).where(UserRow.id == user_id).values(token_version=UserRow.token_version + 1))
            await session.commit()
            return bool(result.rowcount)

    async def record_sign_in(self, user_id: str, *, system_role: str, oauth_issuer: str | None, last_sign_in_at: datetime, email: str | None = None) -> bool:
        """What a provider sign-in writes to an existing account, and nothing else.

        Returns whether the address followed: ``False`` when none was offered,
        and when one was but another account holds it by the time of the write.

        A targeted update rather than ``update_user``: the sign-in must not
        carry a ``token_version`` it read a moment ago back over an
        ``end_sessions`` that landed in between.

        ``email`` is written only when the caller decided the address follows
        this sign-in; omitted, the stored one is untouched. It is canonicalised
        here like every other write, so the unique index keeps enforcing
        case-insensitive uniqueness.

        Writing an address also clears ``email_released_from``, because that
        column *is* what "released" means: an account that holds a real
        address again is not one whose address is going spare, and it must be
        releasable again if the deployer later turns it off. Leaving the
        column set would make a release a once-per-account act and re-open the
        lock-out this exists to end.

        The role stored is the lower of ``system_role`` and the identity's
        role limit, read inside this statement rather than before it: a
        sign-in that read the claim before the deployer's limit committed
        stores the limit if the limit is there when the write runs.
        """
        stamp: dict[str, object] = {"system_role": self._role_within_limit(system_role, oauth_issuer), "oauth_issuer": oauth_issuer, "last_sign_in_at": last_sign_in_at}
        async with self._sf() as session:
            subject = await session.scalar(select(UserRow.oauth_id).where(UserRow.id == user_id))
            identity = (oauth_issuer, subject) if oauth_issuer and subject else None
            if identity is not None:
                await self._hold_identity(session, *identity)
            if email is not None:
                try:
                    await session.execute(update(UserRow).where(UserRow.id == user_id).values(**stamp, email=_normalize_email(email), email_released_from=None))
                    await session.commit()
                    return True
                except IntegrityError as exc:
                    if not _is_email_violation(exc):
                        raise
                    # Another account took the address between the caller's
                    # holder check and this write. Stamping the sign-in is not
                    # optional, and the address not following is the same
                    # outcome the caller already has a path for.
                    await session.rollback()
                    if identity is not None:
                        await self._hold_identity(session, *identity)
            await session.execute(update(UserRow).where(UserRow.id == user_id).values(**stamp))
            await session.commit()
            return False

    @staticmethod
    async def _hold_identity(session: AsyncSession, issuer: str, subject: str) -> None:
        """Serialise this transaction with every other write of the identity's role, until it ends.

        On PostgreSQL a statement reads other tables as of its own start, so a
        sign-in whose write began before a limit committed would store the
        claim's role after it; a transaction-scoped advisory lock taken by the
        limit, the lift, a sign-in and a first sign-in's insert orders them
        instead. SQLite has one writer at a time and needs nothing.
        """
        connection = await session.connection()
        if connection.dialect.name != "postgresql":
            return
        await session.execute(text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"), {"key": identity_lock_key(issuer, subject)})

    @staticmethod
    def _role_within_limit(role: str, oauth_issuer: str | None):
        """``role``, held at the limit of the row being written, as one SQL expression.

        Correlated to the updated row's subject, so it is evaluated in the
        write itself. A row with no recorded issuer matches on its subject
        alone, failing closed like every other read.
        """
        limit = select(func.min(RoleLimitRow.role)).where(RoleLimitRow.subject == UserRow.oauth_id)
        if oauth_issuer:
            limit = limit.where(RoleLimitRow.issuer == issuer_key(oauth_issuer))
        held = limit.scalar_subquery()
        # A limit this code does not know holds the lowest role, as every read does.
        return case((held.is_(None), literal(role)), *((held == ceiling, literal(limited_role(role, ceiling))) for ceiling in LIMIT_ROLES), else_=literal(ROLES[0]))

    async def release_email(self, user_id: str, *, replacement: str) -> str | None:
        """Give up the account's address, recording what it held. Returns that address.

        ``None`` when the account is gone or is already holding a released
        address, so the caller can say which of the two happened without a
        second read.

        The write is a compare-and-swap on the address it read: two runs at
        once release once, and a sign-in that changed the address in between
        cannot have the older one recorded as what was given up. An account
        that took a real address again at a later sign-in has the column
        cleared (:meth:`record_sign_in`), so it can be released again --
        a release is not a once-per-account act, or the lock-out this exists
        to end would simply come back.
        """
        async with self._sf() as session:
            row = await session.get(UserRow, user_id)
            if row is None or row.email_released_from is not None:
                return None
            held = row.email
            result = await session.execute(
                update(UserRow).where(UserRow.id == user_id, UserRow.email == held, UserRow.email_released_from.is_(None)).values(email=_normalize_email(replacement), email_released_from=held),
            )
            await session.commit()
            return held if result.rowcount else None

    async def list_users(self) -> list[User]:
        """Every account, oldest first, each with its derived disabled state."""
        async with self._sf() as session:
            rows = (await session.execute(select(UserRow).order_by(UserRow.created_at, UserRow.id))).scalars().all()
            return [await self._load(session, row) for row in rows]  # type: ignore[misc]

    # ── Identities the deployer turned off ────────────────────────────

    async def is_identity_disabled(self, issuer: str, subject: str) -> bool:
        """Read before an account is created or returned at sign-in."""
        stmt = select(DisabledIdentityRow.disabled_at).where(DisabledIdentityRow.issuer == issuer_key(issuer), DisabledIdentityRow.subject == subject)
        async with self._sf() as session:
            return await session.scalar(stmt) is not None

    async def disable_identity(self, issuer: str, subject: str) -> bool:
        """Record the refusal; returns False when it was already recorded (idempotent)."""
        key = issuer_key(issuer)
        async with self._sf() as session:
            existing = await session.get(DisabledIdentityRow, (key, subject))
            if existing is not None:
                return False
            session.add(DisabledIdentityRow(issuer=key, subject=subject, disabled_at=datetime.now(UTC)))
            try:
                await session.commit()
            except IntegrityError:
                # Lost a race with another disable of the same identity: the
                # refusal is recorded either way.
                await session.rollback()
                return False
            return True

    async def enable_identity(self, issuer: str, subject: str) -> bool:
        """Withdraw the refusal; returns False when there was none (idempotent)."""
        async with self._sf() as session:
            existing = await session.get(DisabledIdentityRow, (issuer_key(issuer), subject))
            if existing is None:
                return False
            await session.delete(existing)
            await session.commit()
            return True

    async def list_refused_user_ids(self) -> set[str]:
        """Every account a recorded refusal covers, in one read.

        The match :meth:`_disabled_at` makes for one account, made for all:
        subject, plus the issuer when one is recorded (a row whose issuer was
        never recorded matches on its subject alone, failing closed).
        """
        async with self._sf() as session:
            refused = {(row.issuer, row.subject) for row in (await session.execute(select(DisabledIdentityRow.issuer, DisabledIdentityRow.subject))).all()}
            if not refused:
                return set()
            subjects = {subject for _, subject in refused}
            stmt = select(UserRow.id, UserRow.oauth_id, UserRow.oauth_issuer).where(UserRow.oauth_id.in_(subjects), UserRow.oauth_provider.is_not(None))
            rows = (await session.execute(stmt)).all()
            return {str(user_id) for user_id, subject, issuer in rows if not issuer or (issuer_key(issuer), subject) in refused}

    async def list_disabled_identities(self) -> list[tuple[str, str, datetime]]:
        """Every identity turned off, whether or not an account exists for it."""
        stmt = select(DisabledIdentityRow).order_by(DisabledIdentityRow.disabled_at, DisabledIdentityRow.issuer, DisabledIdentityRow.subject)
        async with self._sf() as session:
            rows = (await session.execute(stmt)).scalars().all()
            return [(row.issuer, row.subject, _aware(row.disabled_at)) for row in rows]  # type: ignore[misc]

    # ── What turning an identity off held ─────────────────────────────

    async def record_holds(self, issuer: str, subject: str, targets: list[tuple[str, str, str]]) -> None:
        """Record ``(kind, target_id, user_id)`` as held for this identity; a target already recorded stays as it was."""
        if not targets:
            return
        key = issuer_key(issuer)
        async with self._sf() as session:
            existing = {(row.kind, row.target_id) for row in (await session.execute(select(IdentityHoldRow).where(IdentityHoldRow.issuer == key, IdentityHoldRow.subject == subject))).scalars()}
            now = datetime.now(UTC)
            for kind, target_id, user_id in dict.fromkeys(targets):
                if (kind, target_id) not in existing:
                    session.add(IdentityHoldRow(issuer=key, subject=subject, kind=kind, target_id=target_id, user_id=user_id, held_at=now))
                    existing.add((kind, target_id))
            try:
                await session.commit()
            except IntegrityError:
                # A concurrent disable of the same identity recorded it first.
                await session.rollback()
                await self.record_holds(issuer, subject, targets)

    async def list_holds(self, issuer: str, subject: str) -> list[tuple[str, str, str]]:
        """``(kind, target_id, user_id)`` for everything held for this identity, oldest first."""
        stmt = (
            select(IdentityHoldRow.kind, IdentityHoldRow.target_id, IdentityHoldRow.user_id)
            .where(IdentityHoldRow.issuer == issuer_key(issuer), IdentityHoldRow.subject == subject)
            .order_by(IdentityHoldRow.held_at, IdentityHoldRow.kind, IdentityHoldRow.target_id)
        )
        async with self._sf() as session:
            return [(str(kind), str(target_id), str(user_id)) for kind, target_id, user_id in (await session.execute(stmt)).all()]

    async def list_all_holds(self) -> list[tuple[str, str, str, str, str]]:
        """``(issuer, subject, kind, target_id, user_id)`` for every hold recorded, by identity."""
        stmt = select(IdentityHoldRow.issuer, IdentityHoldRow.subject, IdentityHoldRow.kind, IdentityHoldRow.target_id, IdentityHoldRow.user_id).order_by(
            IdentityHoldRow.issuer, IdentityHoldRow.subject, IdentityHoldRow.kind, IdentityHoldRow.target_id
        )
        async with self._sf() as session:
            return [tuple(str(value) for value in row) for row in (await session.execute(stmt)).all()]  # type: ignore[misc]

    async def discard_holds(self, issuer: str, subject: str, targets: list[tuple[str, str]] | None = None) -> int:
        """Forget ``(kind, target_id)`` from this identity's record, or the whole record; returns how many were forgotten."""
        stmt = delete(IdentityHoldRow).where(IdentityHoldRow.issuer == issuer_key(issuer), IdentityHoldRow.subject == subject)
        if targets is not None:
            if not targets:
                return 0
            stmt = stmt.where(tuple_(IdentityHoldRow.kind, IdentityHoldRow.target_id).in_(targets))
        async with self._sf() as session:
            result = await session.execute(stmt)
            await session.commit()
            return int(result.rowcount or 0)

    # ── Role limits the deployer holds ────────────────────────────────

    async def _covered_rows(self, session: AsyncSession, issuer: str, subject: str) -> list[UserRow]:
        """The rows :meth:`list_users_by_identity` returns, inside the caller's transaction."""
        stmt = select(UserRow).where(UserRow.oauth_id == subject, UserRow.oauth_provider.is_not(None))
        rows = (await session.execute(stmt)).scalars().all()
        return [row for row in rows if not row.oauth_issuer or issuer_key(row.oauth_issuer) == issuer_key(issuer)]

    async def _lower_to(self, session: AsyncSession, issuer: str, subject: str, role: str) -> int:
        """Bring every covered row's stored role down to ``role``; returns how many were above it."""
        ids = [row.id for row in await self._covered_rows(session, issuer, subject)]
        above = [held for held in ROLES if limited_role(held, role) != held]
        if not ids or not above:
            return 0
        result = await session.execute(update(UserRow).where(UserRow.id.in_(ids), UserRow.system_role.in_(above)).values(system_role=role))
        return int(result.rowcount or 0)

    async def role_limit_for(self, issuer: str, subject: str) -> tuple[str, datetime] | None:
        """The limit held for this identity and when it was set, or ``None``."""
        async with self._sf() as session:
            row = await session.get(RoleLimitRow, (issuer_key(issuer), subject))
            return None if row is None else (row.role, _aware(row.limited_at))  # type: ignore[return-value]

    async def limit_role(self, issuer: str, subject: str, role: str) -> tuple[bool, int, int]:
        """Hold the identity at ``role``, and lower every covered account's stored role in the same transaction.

        Returns whether the limit is new (or changed), how many stored roles
        were above it, and how many accounts' sessions it ended. Re-running it
        re-lowers: a role written past the limit is brought down again and
        counted. When either changed anything, every covered account's
        sessions end in that same transaction, so a command that dies after
        the commit cannot leave them open while every later pass reads no
        change.
        """
        if role not in LIMIT_ROLES:
            raise ValueError(f"a role limit holds an identity below its highest role; one of {', '.join(LIMIT_ROLES)}")
        key = issuer_key(issuer)
        async with self._sf() as session:
            await self._hold_identity(session, key, subject)
            existing = await session.get(RoleLimitRow, (key, subject))
            recorded = existing is None or existing.role != role
            if existing is None:
                session.add(RoleLimitRow(issuer=key, subject=subject, role=role, limited_at=datetime.now(UTC)))
            elif existing.role != role:
                existing.role = role
                existing.limited_at = datetime.now(UTC)
            try:
                await session.flush()
            except IntegrityError:
                # Lost a race with another limit of the same identity: the
                # limit is recorded either way; lower under it all the same.
                await session.rollback()
                recorded = False
                await self._hold_identity(session, key, subject)
            lowered = await self._lower_to(session, key, subject, role)
            ended = 0
            if recorded or lowered:
                ids = [row.id for row in await self._covered_rows(session, key, subject)]
                if ids:
                    result = await session.execute(update(UserRow).where(UserRow.id.in_(ids)).values(token_version=UserRow.token_version + 1))
                    ended = int(result.rowcount or 0)
            await session.commit()
            return recorded, lowered, ended

    async def lift_role_limit(self, issuer: str, subject: str) -> bool:
        """Withdraw the limit; returns False when there was none (idempotent).

        The stored role stays where the limit held it -- in the same
        transaction, so a role a racing sign-in wrote past the limit is not
        handed back by the lift. Only the next sign-in reads the claim again.
        """
        key = issuer_key(issuer)
        async with self._sf() as session:
            await self._hold_identity(session, key, subject)
            existing = await session.get(RoleLimitRow, (key, subject))
            if existing is None:
                return False
            await self._lower_to(session, key, subject, existing.role)
            await session.execute(delete(RoleLimitRow).where(RoleLimitRow.issuer == key, RoleLimitRow.subject == subject))
            await session.commit()
            return True

    async def list_role_limits(self) -> list[tuple[str, str, str, datetime]]:
        """Every limit held, whether or not an account exists for its identity."""
        stmt = select(RoleLimitRow).order_by(RoleLimitRow.limited_at, RoleLimitRow.issuer, RoleLimitRow.subject)
        async with self._sf() as session:
            rows = (await session.execute(stmt)).scalars().all()
            return [(row.issuer, row.subject, row.role, _aware(row.limited_at)) for row in rows]  # type: ignore[misc]
