"""Acknowledged quiet-hour prompts and atomic, replay-safe typed preferences."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta

from sqlalchemy import select, text

from app.telegram_bots.callbacks import CALLBACK_METADATA_KEYS, CallbackRefused, _arm_visible_prompt
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.message_settings import choose, parse_quiet, setting_changes
from app.telegram_bots.message_settings_custody import SettingsGroupGuard, require_settings_group
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_result_identity import digest
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def validate_prompt(item, prompt, *, acknowledged=False, arming=False):
    try:
        basis = item.payload["settings_prompt_basis"]
        original = item.payload["product_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "prompt_id", "key"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or basis["key"] not in {"settings.quiet_prompt", "settings.quiet_bad"}
            or item.dedupe_key != "notice:settings-hours:" + basis["prompt_id"]
            or item.class_ != "reply"
            or "settings_dispatch_conflict" in item.payload
            or len(item.parts) != 1
            or prompt is None
            or prompt.id != basis["prompt_id"]
            or prompt.kind != "prompt"
            or prompt.target_kind is not None
            or prompt.target_id is not None
            or prompt.payload.get("purpose") != "quiet_times"
            or (prompt.link_id, prompt.binding_generation, prompt.chat_id, prompt.message_thread_id) != (item.link_id, item.binding_generation, item.chat_id, item.message_thread_id)
            or prompt.payload.get("anchor_outbox_id") != item.id
            or type(prompt.payload.get("presentation_version")) is not int
            or prompt.payload.get("presentation_version") != 0
            or prompt.payload.get("presentation_pending") is not False
        ):
            raise ValueError()
        part = item.parts[0]
        recipe = {"kind": "text", "source_ref": {"key": basis["key"], "values": {}}, "force_reply": True}
        if not same_body(original["parts"], [recipe]) or any(not same_body(part.get(key), value) for key, value in recipe.items()) or type(part["presentation_version"]) is not int or part["presentation_version"] != 0:
            raise ValueError()
        if prompt.payload.get("prompt_visibility", {}).get("part_id") != part["part_id"]:
            raise ValueError()
        visibility = prompt.payload["prompt_visibility"]
        if set(visibility) != {"state", "part_id", "ttl_seconds"} or visibility["state"] not in {"pending", "visible"} or type(visibility["ttl_seconds"]) is not int or visibility["ttl_seconds"] <= 0:
            raise ValueError()
        if acknowledged:
            if part["state"] != "sent" or type(part["message_ids"]) is not list or len(part["message_ids"]) != 1 or type(part["message_ids"][0]) is not int or part["message_ids"][0] <= 0:
                raise ValueError()
            stamp = datetime.fromisoformat(part["sent_at"])
            if stamp.utcoffset() is None:
                raise ValueError()
            if arming:
                if visibility["state"] != "pending" or prompt.message_id is not None or prompt.expires_at is not None:
                    raise ValueError()
            else:
                from app.telegram_bots.notifications import aware

                if (
                    visibility["state"] != "visible"
                    or type(prompt.message_id) is not int
                    or prompt.message_id != part["message_ids"][0]
                    or aware(prompt.created_at) != stamp
                    or prompt.expires_at is None
                    or aware(prompt.expires_at) != stamp + timedelta(seconds=visibility["ttl_seconds"])
                ):
                    raise ValueError()
        return part
    except (KeyError, TypeError, ValueError, IndexError):
        raise ProductSourceRetired() from None


async def acknowledge_prompt(session, row, part):
    prompt = await session.get(TelegramCallbackRow, row.payload["settings_prompt_basis"]["prompt_id"], with_for_update=True, populate_existing=True)
    if validate_prompt(_item(row), prompt, acknowledged=True, arming=True) != part:
        raise ProductSourceRetired()
    _arm_visible_prompt(prompt, part)


class PromptDispatchProjection:
    def __init__(self, services, captured):
        self.services, self.captured = services, captured

    def extend(self, query):
        return (
            query.add_columns(TelegramOutboxRow, TelegramCallbackRow)
            .outerjoin(TelegramOutboxRow, TelegramOutboxRow.id == self.captured.item.id)
            .outerjoin(TelegramCallbackRow, TelegramCallbackRow.id == self.captured.item.payload["settings_prompt_basis"]["prompt_id"])
        )

    def check(self, facts):
        captured = self.captured
        binding, row, prompt = facts[0][0], facts[0][5], facts[0][6]
        current = self.services.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
        if current != captured:
            raise DeliveryClaimRetired()
        validate_prompt(current.item, prompt)
        if prompt.consumed_at is not None or "prompt_successor" in prompt.payload or prompt.payload["prompt_visibility"]["state"] != "pending":
            raise ProductSourceRetired()


async def authorize_prompt(services, captured):
    checker = await services.prepare_live_owner(captured.link.owner_user_id)
    channel = services.channel(captured.link.id)
    guard = services.links.publication_guard(captured.link, check_live_owner=checker, check_attempt_current=lambda: channel is not None and services.channel(captured.link.id) is channel and channel._callback_current(captured.link))
    async with services.outbox.sessions() as writer, writer.begin():
        await guard.check_projection(writer, PromptDispatchProjection(services, captured))


class PromptAnswerProjection:
    def __init__(self, parent, prompt, mark):
        self.parent_id, self.prompt_id = parent.id, prompt.id
        self.parent = deepcopy(_item(parent))
        self.prompt = self.capture(prompt)
        self.mark_id = (mark.link_id, mark.message_id, mark.chat_id)
        self.mark = deepcopy(InboxRow.from_row(mark))
        self.successor = None
        self.outcome = None

    @staticmethod
    def capture(prompt):
        from app.telegram_bots.callbacks import _row

        return _row(prompt)

    def extend(self, query):
        from sqlalchemy import and_

        return (
            query.add_columns(TelegramOutboxRow, TelegramCallbackRow, TelegramInboundMarkRow)
            .outerjoin(TelegramOutboxRow, TelegramOutboxRow.id == self.parent_id)
            .outerjoin(TelegramCallbackRow, TelegramCallbackRow.id == self.prompt_id)
            .outerjoin(TelegramInboundMarkRow, and_(TelegramInboundMarkRow.link_id == self.mark_id[0], TelegramInboundMarkRow.message_id == self.mark_id[1], TelegramInboundMarkRow.chat_id == self.mark_id[2]))
        )

    def check(self, facts):
        parent, prompt, mark = facts[0][5:8]
        from dataclasses import replace

        if parent is None or prompt is None or mark is None or _item(parent) != self.parent:
            raise CallbackRefused()
        current = self.capture(prompt)
        expected_payload = dict(self.prompt.payload)
        if self.successor is not None:
            expected_payload["prompt_successor"] = self.successor
        expected = replace(self.prompt, payload=expected_payload, consumed_at=current.consumed_at, closed_reason="replaced" if self.successor else "answered")
        frozen_input = ("link_id", "chat_id", "message_id", "update_id", "message_thread_id", "binding_generation", "accepted_at", "sanitized_input", "turn_id", "thread_id", "run_id", "operation_ids")
        current_mark = InboxRow.from_row(mark)
        if (
            current.consumed_at is None
            or current != expected
            or any(not same_body(getattr(current_mark, key), getattr(self.mark, key)) if key == "sanitized_input" else getattr(current_mark, key) != getattr(self.mark, key) for key in frozen_input)
        ):
            raise CallbackRefused()
        if (mark.route_kind, mark.route_target_id, mark.route_complete) != ("settings_input", self.prompt_id, True) or mark.route_basis != {"prompt_id": self.prompt_id} or self.outcome not in mark.delivery_ids:
            raise CallbackRefused()
        validate_prompt(_item(parent), prompt, acknowledged=True)


class ClosedSettingsGuard:
    def __init__(self, base, callbacks):
        self.base, self.callbacks, self.group, self.origin = base, callbacks, None, None
        self.captured_link = base.captured_link
        self.publication = None

    async def prepare(self):
        await self.base.prepare()

    def current(self):
        self.base.current()

    async def check(self, writer):
        if self.group is None:
            return await self.base.check(writer)
        await require_settings_group(writer, self.group, source_keys={"prompt.closed", "prompt.unavailable"}, publication=True)
        current = await self.base.check_projection(writer, ClosedSettingsProjection(self))
        self.callbacks._unexpired(self.group.selected)
        self.current()
        return current


class ClosedSettingsProjection:
    def __init__(self, guard):
        self.guard = guard

    def extend(self, query):
        from sqlalchemy import and_

        from app.telegram_bots.message_settings_custody import SettingsGroupProjection

        query = SettingsGroupProjection(self.guard.group).extend(query)
        origin = self.guard.origin
        return query.add_columns(TelegramInboundMarkRow).outerjoin(
            TelegramInboundMarkRow, and_(TelegramInboundMarkRow.link_id == origin.link_id, TelegramInboundMarkRow.chat_id == origin.chat_id, TelegramInboundMarkRow.message_id == origin.message_id)
        )

    def check(self, facts):
        from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY
        from app.telegram_bots.message_settings_custody import validate_settings_group

        guard, parent, mark = self.guard, facts[0][5], facts[0][7]
        controls = {row[6].id: row[6] for row in facts if row[6] is not None}
        validate_settings_group(parent, list(controls.values()), guard.group, source_keys={"prompt.closed", "prompt.unavailable"}, publication=True)
        original = guard.origin
        if mark is None or not same_body(mark.sanitized_input, original.sanitized_input) or (mark.binding_generation, mark.message_thread_id, mark.update_id) != (original.binding_generation, original.message_thread_id, original.update_id):
            raise CallbackRefused()
        basis = parent.payload.get("product_basis", {})
        if (
            basis.get("origins") != [[original.link_id, original.message_id, original.chat_id]]
            or basis.get("finish_route") is not False
            or mark.route_basis.get("human_input_ui", {}).get("held_prompt_id") != guard.group.selected.payload.get("prompt_id")
        ):
            raise CallbackRefused()
        if guard.publication is not None and not same_body(controls[guard.group.selected.id].payload.get(CALLBACK_PUBLICATION_KEY), guard.publication):
            raise CallbackRefused()


class SettingsPrompts:
    def __init__(self, services):
        self.services = services
        self.prompts, self.inbox = services.prompts, services.outbox.inbox
        from app.telegram_bots.human_input import HumanInputs

        self.closed = HumanInputs(services, source=object())

    async def _plan(self, writer, link, *, identity, key, guard, origin=None):
        existing = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:settings-hours:" + identity))
        if existing is not None:
            prompt = await writer.get(TelegramCallbackRow, identity)
            validate_prompt(_item(existing), prompt, acknowledged=existing.state == "sent")
            return existing.id
        topic = origin.message_thread_id if origin is not None else None
        await self.prompts.open(link, "quiet_times", {}, chat_id=link.chat_id, message_thread_id=topic, message_id=None, prompt_id=identity, guard=guard, session=writer, defer_visibility=True)
        outbox_id = await self.services.outbox.plan_product(
            link,
            source_id="settings-hours:" + identity,
            parts=[{"kind": "text", "source_ref": {"key": key, "values": {}}, "force_reply": True}],
            inbox_rows=[origin] if origin is not None else [],
            finish_route=origin is not None,
            message_thread_id=topic,
            session=writer,
        )
        parent = await writer.get(TelegramOutboxRow, outbox_id)
        parent.payload = {**parent.payload, "settings_prompt_basis": {"version": 1, "prompt_id": identity, "key": key}}
        prompt = await writer.get(TelegramCallbackRow, identity)
        prompt.payload = {**prompt.payload, "anchor_outbox_id": outbox_id, "presentation_version": 0, "presentation_pending": False, "prompt_visibility": {**prompt.payload["prompt_visibility"], "part_id": parent.parts[0]["part_id"]}}
        validate_prompt(_item(parent), prompt)
        return outbox_id

    async def open(self, channel, row, *, query_id):
        if type(query_id) is not str or not 1 <= len(query_id) <= 256:
            raise CallbackRefused()
        link = deepcopy(channel.link)
        guard = SettingsGroupGuard(self.services.commands._guard(channel, link), self.services.callbacks)
        identity = digest([link.id, link.binding_generation, row.id, query_id, "quiet_times"])[:32]

        async def perform():
            async with self.services.callbacks.writer(link, guard) as writer:
                group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                await require_settings_group(writer, group)
                guard.group = group
                selected = group.selected
                payload = {key: value for key, value in selected.payload.items() if key not in CALLBACK_METADATA_KEYS}
                if (selected.kind, selected.target_kind, selected.target_id) != ("settings_prompt", "link", link.id) or payload != {"field": "quiet"} or row.message_thread_id is not None:
                    raise CallbackRefused()
                return await self._plan(writer, link, identity=identity, key="settings.quiet_prompt", guard=guard)

        return await await_drained(perform())

    async def handle_text(self, channel, update, *, captured_link=None):
        link, message = deepcopy(captured_link or channel.link), update.message
        if (
            message is None
            or type(message.text) is not str
            or not message.text.strip()
            or message.text.lstrip().startswith("/")
            or message.photo
            or message.document
            or message.voice
            or message.audio
            or message.video
            or message.media_group_id is not None
            or message.forward_origin is not None
            or message.is_automatic_forward is True
            or any(message.api_kwargs.get(key) is not None for key in ("forward_from", "forward_from_chat", "forward_sender_name", "forward_date"))
        ):
            return False
        guard = self.services.commands._guard(channel, link)

        async def perform():
            async with self.services.callbacks.writer(link, guard) as writer:
                mark = await writer.get(TelegramInboundMarkRow, (link.id, message.message_id, message.chat_id))
                if mark is not None and mark.route_kind is not None:
                    return mark.route_kind == "settings_input"
                if mark is not None and type(mark.route_basis.get("settings_prompt_id")) is str:
                    prompt = await self.prompts.get(link, mark.route_basis["settings_prompt_id"], guard=guard, session=writer)
                elif message.reply_to_message is not None:
                    from app.telegram_bots.callbacks import _row, normalize_callback_topic

                    located = await writer.scalar(
                        select(TelegramCallbackRow)
                        .where(
                            TelegramCallbackRow.link_id == link.id,
                            TelegramCallbackRow.chat_id == message.chat_id,
                            TelegramCallbackRow.binding_generation == link.binding_generation,
                            TelegramCallbackRow.message_thread_id == normalize_callback_topic(message.message_thread_id),
                            TelegramCallbackRow.kind == "prompt",
                            TelegramCallbackRow.message_id == message.reply_to_message.message_id,
                        )
                        .order_by(TelegramCallbackRow.created_at.desc())
                        .limit(1)
                    )
                    prompt = _row(located) if located is not None else None
                else:
                    prompt = await self.prompts.current(link, message.chat_id, message.message_thread_id, guard=guard, session=writer)
                if prompt is None or prompt.payload.get("purpose") != "quiet_times" or not self.prompts.is_visible(prompt):
                    return False
            value = {"type": "text", "text": message.text}
            if message.reply_to_message is not None:
                value["reply_to_message_id"] = message.reply_to_message.message_id
            async with self.services.callbacks.writer(link, guard) as writer:
                origin, _ = await self.inbox.accept(link, update, sanitized_input=value, session=writer)
                mark = (await self.inbox._rows(writer, [origin]))[0]
                if mark.route_kind is not None:
                    return mark.route_kind == "settings_input" or mark.route_basis.get("human_input_ui") is not None
                if mark.route_basis.get("settings_prompt_id", prompt.id) != prompt.id:
                    raise CallbackRefused()
                mark.route_basis = {**mark.route_basis, "settings_prompt_id": prompt.id}
                await writer.flush()
                origin = InboxRow.from_row(mark)
            await self._apply(channel, link, origin, prompt.id, guard)
            return True

        return await await_drained(perform())

    async def _apply(self, channel, link, origin, prompt_id, guard):
        context = await self.services.message_settings.context(link)
        await guard.prepare()
        async with self.services.outbox.sessions() as writer, writer.begin():
            if writer.get_bind().dialect.name == "sqlite":
                await writer.execute(text("BEGIN IMMEDIATE"))
            settings = await writer.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == link.owner_user_id).with_for_update().execution_options(populate_existing=True))
            await writer.execute(select(TelegramBotLinkRow.id).where(TelegramBotLinkRow.owner_user_id == link.owner_user_id).order_by(TelegramBotLinkRow.id).with_for_update())
            await guard.check(writer)
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            if mark.route_kind is not None:
                if mark.route_kind != "settings_input" or mark.route_target_id != prompt_id:
                    raise CallbackRefused()
                return
            if InboxRow.from_row(mark) != origin:
                raise CallbackRefused()
            prompt = await writer.get(TelegramCallbackRow, prompt_id, with_for_update=True, populate_existing=True)
            current = await self.prompts.current(link, origin.chat_id, origin.message_thread_id, guard=guard, session=writer)
            if (
                settings is None
                or prompt is None
                or prompt.payload.get("purpose") != "quiet_times"
                or (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (link.id, origin.chat_id, link.binding_generation, origin.message_thread_id)
            ):
                raise CallbackRefused()
            if current is None or current.id != prompt_id or prompt.consumed_at is not None or not self.prompts.is_visible(current):
                original = await self.inbox.route(InboxRow.from_row(mark), kind="turn", target_id=link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[], session=writer)
                await self.closed._closed_notice(link, original, prompt, guard=guard, writer=writer)
                await writer.flush()
                await guard.check(writer)
                return
            parent = await writer.get(TelegramOutboxRow, prompt.payload.get("anchor_outbox_id"), with_for_update=True, populate_existing=True)
            validate_prompt(_item(parent), prompt, acknowledged=True)
            projection = PromptAnswerProjection(parent, prompt, mark)
            original = InboxRow.from_row(mark)
            value = original.sanitized_input["text"]
            original = await self.inbox.route(original, kind="settings_input", target_id=prompt_id, basis={"prompt_id": prompt_id}, operation_ids=[], session=writer)
            try:
                parse_quiet(value)
                body = choose(settings, "quiet", value)
            except ValueError:
                identity = digest([link.id, link.binding_generation, origin.chat_id, origin.message_id, "quiet-retry"])[:32]
                projection.successor = identity
                projection.outcome = await self._plan(writer, link, identity=identity, key="settings.quiet_bad", guard=guard, origin=original)
            else:
                identity = prompt_id
                await self.services.acts.write(link, act_id=identity, kind="settings", body=body, message_id=origin.message_id, session=writer)
                updated = await self.services.settings.update(link.owner_user_id, **setting_changes(body), session=writer)
                source = context.show(updated, link=link)
                outbox_id = await self.services.outbox.plan_product_keyboard(
                    link,
                    source_id="settings:" + identity,
                    parts=[{"kind": "text", "reply_to_message_id": origin.message_id, "source_ref": {"key": source["key"], "values": source["values"]}}],
                    keyboard=source["keyboard"],
                    callbacks=self.services.callbacks,
                    guard=guard,
                    inbox_rows=[original],
                    finish_route=True,
                    message_thread_id=original.message_thread_id,
                    session=writer,
                )
                projection.outcome = outbox_id
                act = await writer.get(TelegramActRow, identity)
                act.state, act.attempts, act.first_attempt_at, act.updated_at = "done", 1, datetime.now(UTC), datetime.now(UTC)
                act.result = {"outbox_id": outbox_id, "field": "quiet", "value": body["new"]}
                prompt.consumed_at, prompt.closed_reason = self.services.callbacks.clock(), "answered"
            await writer.flush()
            await guard.check_projection(writer, projection)

    async def execute_callback(self, channel, row, *, query_id=None, update_id=None):
        if row.kind == "settings_prompt":
            await self.open(channel, row, query_id=query_id)
            return True
        if row.kind not in {"prompt_reopen", "prompt_send_new", "prompt_discard"}:
            return False
        guard = self.services.commands._guard(channel, channel.link)
        prompt = await self.prompts.get(channel.link, row.payload.get("prompt_id"), guard=guard)
        if prompt.payload.get("purpose") != "quiet_times":
            return False
        link = deepcopy(channel.link)
        guard = ClosedSettingsGuard(guard, self.services.callbacks)
        stage = None
        async with self.services.callbacks.writer(link, guard) as writer:
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            selected = group.selected
            if selected.payload.get("prompt_id") != prompt.id:
                raise CallbackRefused()
            mark = await writer.get(TelegramInboundMarkRow, (link.id, selected.payload.get("message_id"), link.chat_id), with_for_update=True, populate_existing=True)
            if (
                mark is None
                or (mark.binding_generation, mark.message_thread_id) != (link.binding_generation, row.message_thread_id)
                or mark.route_basis.get("human_input_ui", {}).get("held_prompt_id") != prompt.id
                or mark.route_complete
                and not (selected.kind == "prompt_send_new" and "publication_owner" in selected.payload)
            ):
                raise CallbackRefused()
            original = InboxRow.from_row(mark)
            guard.group, guard.origin = group, original
            await guard.check(writer)
            if selected.kind == "prompt_send_new":
                stage = await self.inbox.stage_prompt_message(
                    link, callback_id=selected.id, update_id=update_id, chat_id=selected.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, callbacks=self.services.callbacks, guard=guard, session=writer
                )
                from app.telegram_bots.inbox import CALLBACK_PUBLICATION_KEY

                control = await writer.get(TelegramCallbackRow, selected.id)
                guard.publication = deepcopy(control.payload[CALLBACK_PUBLICATION_KEY])
                await self.prompts.close(link, link.chat_id, selected.message_thread_id, reason="navigation", guard=guard, session=writer)
            else:
                await self.services.callbacks.consume_group(group, session=writer)
            if selected.kind == "prompt_discard":
                await self.services.outbox.plan_product_keyboard(
                    link,
                    source_id="prompt-discard:" + selected.id,
                    parts=[{"kind": "edit", "source_ref": {"key": "prompt.discarded", "values": {}}, "target_message_id": selected.message_id}],
                    keyboard=[],
                    callbacks=self.services.callbacks,
                    guard=guard,
                    inbox_rows=[original],
                    finish_route=True,
                    message_thread_id=selected.message_thread_id,
                    session=writer,
                )
                return True
            if selected.kind == "prompt_reopen":
                from app.telegram_bots.producer import keyboard_cleanup_row

                writer.add(keyboard_cleanup_row(selected, owner_user_id=link.owner_user_id, now=self.services.outbox.clock()))
                await self.prompts.close(link, link.chat_id, row.message_thread_id, reason="navigation", guard=guard, session=writer)
                await self._plan(writer, link, identity=digest([row.id, "quiet-reopen"])[:32], key="settings.quiet_prompt", guard=guard, origin=original)
        if stage is not None:
            guard.current()
            await self.services.turns.accept_staged(channel, stage)
        return True

    async def recover(self, channel, link, entry, *, guard):
        if len(entry.origins) != 1:
            return False
        origin = entry.origins[0]
        identity = origin.route_basis.get("settings_prompt_id")
        if type(identity) is not str:
            return False
        async with self.services.callbacks.writer(link, guard) as writer:
            prompt = await writer.get(TelegramCallbackRow, identity, with_for_update=True, populate_existing=True)
            if prompt is None or prompt.payload.get("purpose") != "quiet_times":
                return False
            if (prompt.link_id, prompt.chat_id, prompt.binding_generation, prompt.message_thread_id) != (link.id, origin.chat_id, link.binding_generation, origin.message_thread_id):
                raise CallbackRefused()
            mark = (await self.inbox._rows(writer, [origin]))[0]
            self.inbox._current(link, mark)
            if mark.route_kind is not None:
                return mark.route_kind == "settings_input" or mark.route_basis.get("human_input_ui", {}).get("held_prompt_id") == identity
            if prompt.consumed_at is None:
                prompt.consumed_at, prompt.closed_reason = self.services.callbacks.clock(), "cancelled"
            saved = await self.inbox.route(InboxRow.from_row(mark), kind="turn", target_id=link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[], session=writer)
            await self.closed._closed_notice(link, saved, prompt, guard=guard, writer=writer)
        return True
