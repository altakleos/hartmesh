"""Prepare an owner-scoped thread without publishing or executing an input."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

import httpx

from app.telegram_bots.authority import require
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.links import ThreadPreparation
from deerflow.runtime.keyed_lock import AsyncKeyedLockTable
from deerflow.utils.file_io import await_drained

_SCOPE = ("id", "owner_user_id", "bot_id", "agent_kind", "agent_ref", "connection_id", "telegram_user_id", "chat_id", "binding_generation", "token_revision")


class ThreadPreparationFailure(ValueError):
    """Closed local/acknowledged preparation failures for the turn owner."""

    def __init__(self, kind: str):
        if kind not in {"custom_agent_deleted", "files_full"}:
            raise ValueError("invalid-thread-preparation-failure")
        self.kind = kind
        super().__init__("custom-agent-deleted" if kind == "custom_agent_deleted" else "thread-files-full")


@dataclass
class _Attempt:
    link: object
    channel: object
    retired: bool = False


class ThreadCoordinator:
    def __init__(self, *, links, authority, client, channel, send_allowed, outbox, product_name: str):
        self.links, self.authority, self.client = links, authority, client
        self._channel, self._send_allowed = channel, send_allowed
        self.outbox, self.product_name = outbox, product_name
        self._locks = AsyncKeyedLockTable()
        self._operations: set[asyncio.Task] = set()
        self._stopping = False
        self._stopped = False
        self._stop_task = None

    def _current(self, attempt: _Attempt) -> bool:
        channel, link = attempt.channel, attempt.link
        return bool(
            not self._stopping
            and not attempt.retired
            and self._send_allowed() is True
            and channel is not None
            and self._channel(link.id) is channel
            and channel.application.running
            and all(getattr(channel.link, key) == getattr(link, key) for key in _SCOPE)
        )

    def _check(self, attempt: _Attempt) -> None:
        if not self._current(attempt):
            raise ValueError("thread-attempt-changed")

    async def _guard(self, attempt: _Attempt, *, deleted_target=False) -> None:
        self._check(attempt)
        link = attempt.link
        current = await await_drained(self.links.update_if_current(link.id, token_revision=link.token_revision))
        if current is None or current.state != "connected" or current.health not in ({"ok", "agent_unavailable"} if deleted_target else {"ok"}) or any(getattr(current, key) != getattr(link, key) for key in _SCOPE):
            raise ValueError("thread-attempt-changed")
        self._check(attempt)

    async def _permission(self, attempt: _Attempt, *actions: str) -> None:
        await await_drained(require(attempt.link.owner_user_id, *actions))
        await self._guard(attempt)

    @staticmethod
    def _validate(response, link, identity):
        if not isinstance(response, dict) or response.get("thread_id") != identity or response.get("owner_user_id") != link.owner_user_id or "agent_instance" not in response or response["agent_instance"] is not None:
            raise ValueError("invalid-thread-proof")
        metadata = response.get("metadata")
        if not isinstance(metadata, dict):
            raise ValueError("invalid-thread-target")
        selected, canonical = metadata.get("channel_agent_name"), metadata.get("agent_name")
        if selected == "lead_agent" and canonical is None:
            return "lead_agent", "lead_agent"
        if isinstance(selected, str) and selected and selected != "lead_agent" and canonical == selected:
            return "custom_agent", selected
        raise ValueError("invalid-thread-target")

    async def _authorize_preparation(self, attempt, preparation, *needed):
        owner = await self.authority.prepare_live_owner(attempt.link.owner_user_id)
        await self._permission(attempt, *needed)
        fresh = await await_drained(
            self.links.reserve_thread_preparation(
                attempt.link,
                preparation.origin,
                purpose=preparation.purpose,
                endpoint_id=self.client.endpoint_id,
                check_live_owner=owner,
                check_attempt_current=lambda: self._current(attempt),
                require_healthy=True,
                **({"owner_callback_id": preparation.owner_callback_id} if preparation.owner_callback_id is not None else {}),
            )
        )
        if any(getattr(fresh, key) != getattr(preparation, key) for key in ("purpose", "creation_id", "endpoint_id", "expected_mapping_id", "expected_thread_id", "selected_thread_id", "mapping_id")):
            raise ValueError("thread-attempt-changed")
        self._check(attempt)

    async def _dispatch(self, attempt, preparation, action):
        await self._permission(attempt, action)
        await self._target(attempt, preparation.origin, **({"owner_callback_id": preparation.owner_callback_id} if preparation.owner_callback_id is not None else {}))
        await self._authorize_preparation(attempt, preparation, action)

    async def _read(self, attempt, identity, preparation):
        await self._permission(attempt, "threads:read")
        response = await self.client.get_thread(attempt.link.owner_user_id, identity, before_dispatch=lambda: self._dispatch(attempt, preparation, "threads:read"))
        await self._permission(attempt, "threads:read")
        return self._validate(response, attempt.link, identity)

    async def _target(self, attempt, origin, *, owner_callback_id=None):
        link = attempt.link
        if link.agent_kind == "lead_agent" and link.agent_ref == "lead_agent":
            return
        if link.agent_kind != "custom_agent":
            raise ValueError("unsupported-thread-target")
        from deerflow.config.agents_config import AgentConfig, load_agent_config

        try:
            config = await await_drained(asyncio.to_thread(load_agent_config, link.agent_ref, user_id=link.owner_user_id))
        except FileNotFoundError:
            if owner_callback_id is not None or "prepublication_group" in origin.route_basis:
                # The controller/callback owns this new intent. Never attach a notice or
                # finish processing on its immutable original human tombstone.
                raise ThreadPreparationFailure("custom_agent_deleted") from None
            await self._guard(attempt, deleted_target=True)
            saved = await await_drained(self.links.set_health(link.id, "agent_unavailable", holds=True, expected_token_revision=link.token_revision, captured_link=link, check_attempt_current=lambda: self._current(attempt)))
            if saved is None:
                raise ValueError("thread-attempt-changed") from None
            await self._guard(attempt, deleted_target=True)
            from app.telegram_bots.pages import page_address

            url = await await_drained(asyncio.to_thread(page_address, "web.telegram", link))
            await self._guard(attempt, deleted_target=True)
            part = {"kind": "text", "source_ref": {"key": "custom_agent.deleted", "values": {"agent": link.agent_ref, "product": self.product_name}}}
            if url is not None:
                part["url_button"] = {"page": "web.telegram", "url": url}
            await await_drained(
                self.outbox.plan_status_notice(link, source_id=f"thread-deleted:{origin.chat_id}:{origin.message_id}", parts=[part], inbox_rows=[origin], finish_route=True, check_attempt_current=lambda: self._current(attempt))
            )
            raise ValueError("custom-agent-deleted") from None
        if not isinstance(config, AgentConfig) or config.name != link.agent_ref:
            raise ValueError("invalid-thread-target")
        await self._guard(attempt)

    async def _prepare(self, attempt, inbound, origin, purpose, *, owner_callback_id=None):
        link = attempt.link
        if inbound is not None and (
            inbound.channel_name != "telegram"
            or inbound.owner_user_id != link.owner_user_id
            or inbound.connection_id != link.connection_id
            or inbound.chat_id != str(link.chat_id)
            or inbound.user_id != str(link.telegram_user_id)
            or inbound.workspace_id != str(link.bot_id)
            or inbound.topic_id != (str(origin.message_thread_id) if origin.message_thread_id is not None else None)
        ):
            raise ValueError("invalid-thread-input-scope")
        key = (link.owner_user_id, link.id, link.connection_id, link.chat_id, link.binding_generation) + ((origin.message_thread_id,) if origin.message_thread_id is not None else ())
        async with self._locks.hold(key):
            repair = link.agent_kind == "custom_agent"
            await self._guard(attempt, deleted_target=repair)
            owner = await self.authority.prepare_live_owner(link.owner_user_id)
            await self._guard(attempt, deleted_target=repair)
            preparation = await await_drained(
                self.links.reserve_thread_preparation(
                    link,
                    origin,
                    purpose=purpose,
                    endpoint_id=self.client.endpoint_id,
                    check_live_owner=owner,
                    check_attempt_current=lambda: self._current(attempt),
                    **({"owner_callback_id": owner_callback_id} if owner_callback_id is not None else {}),
                )
            )
            await self._guard(attempt, deleted_target=repair)
            if preparation.endpoint_id != self.client.endpoint_id:
                raise ValueError("thread-endpoint-changed")
            origin = preparation.origin
            await self._target(attempt, origin, **({"owner_callback_id": owner_callback_id} if owner_callback_id is not None else {}))
            target = link.agent_kind, link.agent_ref
            if preparation.selected_thread_id is not None:
                if await self._read(attempt, preparation.selected_thread_id, preparation) != target:
                    raise ValueError("invalid-thread-target")
                await self._target(attempt, origin, **({"owner_callback_id": owner_callback_id} if owner_callback_id is not None else {}))
                await self._authorize_preparation(attempt, preparation, "threads:read")
                return preparation.selected_thread_id
            identity = None
            needed = {"threads:read"}
            if purpose == "ensure" and preparation.expected_thread_id is not None:
                try:
                    if await self._read(attempt, preparation.expected_thread_id, preparation) == target:
                        identity = preparation.expected_thread_id
                except httpx.HTTPStatusError as error:
                    # The conversation was deleted on the web, so this chat goes on in a new one.
                    if error.response.status_code != 404:
                        raise
            if identity is None:
                await self._permission(attempt, "threads:write")
                needed.add("threads:write")
                identity = preparation.creation_id
                metadata = {"channel_agent_name": link.agent_ref, "channel_source": {"type": "im_channel", "provider": "telegram", "chat_id": str(link.chat_id), "connection_id": link.connection_id}}
                if link.agent_kind == "custom_agent":
                    metadata["agent_name"] = link.agent_ref
                try:
                    response = await self.client.create_thread(link.owner_user_id, thread_id=identity, metadata=metadata, before_dispatch=lambda: self._dispatch(attempt, preparation, "threads:write"))
                except httpx.HTTPStatusError as error:
                    if error.response.status_code == 507:
                        raise ThreadPreparationFailure("files_full") from None
                    raise
                await self._guard(attempt)
                if not isinstance(response, dict) or response.get("thread_id") != identity or response.get("owner_user_id") != link.owner_user_id:
                    raise ValueError("invalid-thread-proof")
                if await self._read(attempt, identity, preparation) != target:
                    raise ValueError("invalid-thread-target")
            await self._target(attempt, origin, **({"owner_callback_id": owner_callback_id} if owner_callback_id is not None else {}))
            owner = await self.authority.prepare_live_owner(link.owner_user_id)
            await self._permission(attempt, *sorted(needed))
            if not await await_drained(self.links.commit_thread_mapping(link, preparation, thread_id=identity, endpoint_id=self.client.endpoint_id, check_live_owner=owner, check_attempt_current=lambda: self._current(attempt))):
                raise ValueError("thread-mapping-changed")
            return identity

    async def _reserve_new(self, attempt, origin):
        if not isinstance(origin, InboxRow) or origin.route_kind != "command" or origin.route_target_id != "new" or origin.message_thread_id is not None:
            raise ValueError("invalid-new-command")
        link = attempt.link
        key = (link.owner_user_id, link.id, link.connection_id, link.chat_id, link.binding_generation)
        async with self._locks.hold(key):
            await self._guard(attempt)
            owner = await self.authority.prepare_live_owner(link.owner_user_id)
            await self._guard(attempt)
            preparation = await await_drained(
                self.links.reserve_thread_preparation(link, origin, purpose="new", endpoint_id=self.client.endpoint_id, check_live_owner=owner, check_attempt_current=lambda: self._current(attempt), require_healthy=True)
            )
            self._check(attempt)
            if preparation.origin.route_complete or preparation.origin.state not in {"accepted", "reply_pending"}:
                raise ValueError("invalid-new-command")
            await self._target(attempt, preparation.origin)
            await self._guard(attempt)
            return preparation

    async def _callback_input(self, attempt, callback_id):
        from app.telegram_bots.inbox import InboxRepository

        await self._guard(attempt)
        owner = await self.authority.prepare_live_owner(attempt.link.owner_user_id)
        await self._guard(attempt)
        guard = self.links.publication_guard(attempt.link, check_live_owner=owner, check_attempt_current=lambda: self._current(attempt))
        stage = await InboxRepository(self.links.session_factory).read_callback_input(attempt.link, callback_id, guard=guard)
        self._check(attempt)
        return stage

    async def _prepare_callback(self, attempt, callback_id):
        stage = await self._callback_input(attempt, callback_id)
        if stage.route_complete or stage.state == "interrupted":
            raise ValueError("thread-attempt-changed")
        if stage.thread_id is None:
            return await self._prepare(attempt, None, stage.origins[0], "ensure", owner_callback_id=callback_id)

        async def authorize():
            await self._permission(attempt, "threads:read")
            fresh = await self._callback_input(attempt, callback_id)
            if fresh.thread_id != stage.thread_id or fresh.route_complete:
                raise ValueError("thread-attempt-changed")
            await self._target(attempt, fresh.origins[0], owner_callback_id=callback_id)
            self._check(attempt)

        await authorize()
        response = await self.client.get_thread(attempt.link.owner_user_id, stage.thread_id, before_dispatch=authorize)
        if self._validate(response, attempt.link, stage.thread_id) != (attempt.link.agent_kind, attempt.link.agent_ref):
            raise ValueError("invalid-thread-target")
        await authorize()
        return stage.thread_id

    async def _prepare_submission(self, attempt, stage):
        from app.telegram_bots.inbox import InboxRepository

        inbox = InboxRepository(self.links.session_factory)
        if stage.callback_id is not None or stage.parent_publication["scope"]["endpoint_id"] != self.client.endpoint_id:
            raise ValueError("thread-attempt-changed")

        async def authorize():
            await self._guard(attempt)
            await self._permission(attempt, "threads:read")
            owner = await self.authority.prepare_live_owner(attempt.link.owner_user_id)
            await self._guard(attempt)
            guard = self.links.publication_guard(attempt.link, check_live_owner=owner, check_attempt_current=lambda: self._current(attempt))
            fresh = await inbox.read_submission(attempt.link, stage, guard=guard)
            if fresh.thread_id != stage.thread_id or fresh.route_complete:
                raise ValueError("thread-attempt-changed")
            await self._target(attempt, fresh.origins[0])
            self._check(attempt)

        await authorize()
        response = await self.client.get_thread(attempt.link.owner_user_id, stage.thread_id, before_dispatch=authorize)
        if self._validate(response, attempt.link, stage.thread_id) != (attempt.link.agent_kind, attempt.link.agent_ref):
            raise ValueError("invalid-thread-target")
        await authorize()
        return stage.thread_id

    async def _run(self, link, inbound, origin, purpose, *, reserve_only=False, callback_id=None, submission=None):
        attempt = _Attempt(link, self._channel(link.id))
        self._check(attempt)
        work = (
            self._prepare_submission(attempt, submission)
            if submission is not None
            else self._prepare_callback(attempt, callback_id)
            if callback_id is not None
            else self._reserve_new(attempt, origin)
            if reserve_only
            else self._prepare(attempt, inbound, origin, purpose)
        )
        task = asyncio.create_task(work, name="telegram-thread-preparation")
        self._operations.add(task)
        task.add_done_callback(self._operations.discard)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            attempt.retired = True
            # Lock waiters can leave immediately. Every admitted SQL/offload/SDK
            # unit below already drains cancellation before releasing resources.
            task.cancel()
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not task.cancelled():
                task.exception()
            raise

    async def ensure_thread(self, link, inbound, *, origin) -> str:
        return await self._run(link, inbound, origin, "ensure")

    async def new_conversation(self, link, *, origin) -> str:
        return await self._run(link, None, origin, "new")

    async def reserve_new_conversation(self, link, *, origin) -> ThreadPreparation:
        """Pin a General /new offer without dispatching any thread request."""
        return await self._run(link, None, origin, "new", reserve_only=True)

    async def ensure_callback_thread(self, link, *, callback_id) -> str:
        return await self._run(link, None, None, "ensure", callback_id=callback_id)

    async def ensure_submission_thread(self, link, stage) -> str:
        return await self._run(link, None, None, "ensure", submission=stage)

    def begin_shutdown(self) -> None:
        self._stopping = True

    async def _finish_stop(self):
        if self._operations:
            await asyncio.gather(*tuple(self._operations), return_exceptions=True)
        self._stopped = True

    async def stop(self):
        self.begin_shutdown()
        if self._stop_task is None or self._stop_task.done() and not self._stopped:
            self._stop_task = asyncio.create_task(self._finish_stop(), name="telegram-thread-preparation-cleanup")
        await asyncio.shield(self._stop_task)


def _coordinator() -> ThreadCoordinator:
    from app.telegram_bots.supervisor import current

    supervisor = current()
    if supervisor is None or supervisor.threads is None:
        raise RuntimeError("thread-preparation-unavailable")
    return supervisor.threads


async def ensure_thread(link, inbound, *, origin) -> str:
    return await _coordinator().ensure_thread(link, inbound, origin=origin)


async def new_conversation(link, *, origin) -> str:
    return await _coordinator().new_conversation(link, origin=origin)
