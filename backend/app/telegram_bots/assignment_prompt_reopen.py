"""A real closed-input choice owns a fresh prompt, never the held answer."""

from copy import deepcopy
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import JSON, and_, false, func, or_, select
from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_drafts import origin_facts
from app.telegram_bots.assignment_typing import shown_prompt
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import _validate_rows
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired, _item, keyboard_cleanup_row
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_closed_inputs import closed_parts
from app.telegram_bots.work_outcome_delivery import acknowledged_parts
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_origins import anchor_signature
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramCallbackRow, TelegramInboundMarkRow, TelegramOutboxRow


def notice_signature(parent):
    return digest([parent.id, parent.payload.get("presentation_version"), parent.payload.get("product_basis"), parent.payload.get("product_keyboard_basis"), parent.parts])


def stored_prompt_intent(item):
    families = {"assignment_prompt_intent", "management_prompt_intent"}.intersection(item.payload)
    if len(families) != 1:
        raise ProductSourceRetired()
    if "management_prompt_intent" in families:
        from app.telegram_bots.work_management_prompt_sources import prompt_intent
    else:
        from app.telegram_bots.assignment_prompt_sources import prompt_intent
    return prompt_intent(item)


def prompt_plans(owner, item):
    stored_prompt_intent(item)
    return owner.work.management_edits.prompts if "management_prompt_intent" in item.payload else owner.work.assignment_prompts


def prompt_permissions(projection):
    return projection.facts.permissions if hasattr(projection, "facts") else projection.frame.detail.source.permissions


def require_reopen(value):
    from app.telegram_bots.work_actions import thaw

    value = thaw(value)
    if (
        type(value) is not dict
        or set(value) != {"version", "input", "source_outbox_id", "source_prompt_id", "source_signature", "notice_id", "context_prompt_id", "callback_id", "notice_signature"}
        or type(value["version"]) is not int
        or value["version"] != 1
        or any(
            type(value[key]) is not str or len(value[key]) != size or any(char not in "0123456789abcdef" for char in value[key])
            for key, size in (("source_outbox_id", 32), ("source_prompt_id", 32), ("source_signature", 64), ("notice_id", 32), ("context_prompt_id", 32), ("callback_id", 32), ("notice_signature", 64))
        )
    ):
        raise ProductSourceRetired()
    origin = value["input"]
    if (
        type(origin) is not dict
        or set(origin) != {"link_id", "chat_id", "message_id", "update_id", "binding_generation", "message_thread_id", "input_signature"}
        or type(origin["link_id"]) is not str
        or len(origin["link_id"]) != 32
        or any(type(origin[key]) is not int or not 0 <= origin[key] < 2**63 for key in ("chat_id", "message_id", "update_id", "binding_generation"))
        or origin["message_thread_id"] is not None
        and (type(origin["message_thread_id"]) is not int or origin["message_thread_id"] <= 1)
        or type(origin["input_signature"]) is not str
        or len(origin["input_signature"]) != 64
        or any(char not in "0123456789abcdef" for char in origin["input_signature"])
    ):
        raise ProductSourceRetired()
    return value


class AssignmentReopenProof:
    def __init__(self, link, value, intent, *, completed, sqlite):
        self.link, self.value, self.intent = link, require_reopen(value), intent
        self.completed, self.sqlite = completed, sqlite

    def extend(self, query):
        self.column = len(query.column_descriptions)
        origin = self.value["input"]
        mark, parent, source = aliased(TelegramInboundMarkRow), aliased(TelegramOutboxRow), aliased(TelegramOutboxRow)
        current, cleanup = aliased(TelegramOutboxRow), aliased(TelegramOutboxRow)
        echo = aliased(TelegramOutboxRow)
        control = TelegramCallbackRow
        args = []
        for column in control.__table__.columns:
            args.extend((column.name, func.json(column) if self.sqlite and isinstance(column.type, JSON) else column))
        aggregate = func.json_group_array(func.json_object(*args)) if self.sqlite else func.json_agg(func.json_build_object(*args))
        group_id = select(control.group_id).where(control.id == self.value["callback_id"]).scalar_subquery()
        controls = select(aggregate).where(or_(control.group_id == group_id, control.id.in_((self.value["source_prompt_id"], self.value["context_prompt_id"])))).scalar_subquery()
        return (
            query.add_columns(mark, parent, source, controls, current, cleanup, echo)
            .outerjoin_from(TelegramBotLinkRow, mark, and_(mark.link_id == origin["link_id"], mark.chat_id == origin["chat_id"], mark.message_id == origin["message_id"]))
            .outerjoin_from(mark, parent, parent.id == self.value["notice_id"])
            .outerjoin_from(parent, source, source.id == self.value["source_outbox_id"])
            .outerjoin_from(source, current, current.id == self.completed if self.completed is not None else false())
            .outerjoin_from(current, cleanup, cleanup.id == current.payload["reopen_cleanup_id"].as_string())
            .outerjoin_from(mark, echo, echo.id == mark.route_basis["closed_voice_echo"]["outbox_id"].as_string())
        )

    def check(self, rows):
        from app.telegram_bots.work_request_source import _RequestProjection

        mark, parent, source, raw = rows[0][self.column : self.column + 4]
        controls = _RequestProjection.objects(raw)
        selected = next((row for row in controls if row.id == self.value["callback_id"]), None)
        prompt = next((row for row in controls if row.id == self.value["source_prompt_id"]), None)
        context = next((row for row in controls if row.id == self.value["context_prompt_id"]), None)
        if any(value is None for value in (mark, parent, source, selected, prompt, context)):
            raise ProductSourceRetired()
        scope = (self.link.id, self.link.chat_id, self.link.binding_generation, self.value["input"]["message_thread_id"])
        old = stored_prompt_intent(_item(source))
        inherited = ("purpose", "origin", "draft_id", "parent_id") if self.intent["purpose"] == "work_edit" else ("purpose", "origin", "draft_id", "edit_origin", "query_key")
        closed, typing = mark.route_basis.get("work_closed", {}), mark.route_basis.get("closed_typing", {})
        if (
            not same_body(origin_facts(InboxRow.from_row(mark)), self.value["input"])
            or (mark.link_id, mark.chat_id, mark.binding_generation, mark.message_thread_id) != scope
            or mark.state == "interrupted"
            or mark.route_kind != "work_card_hold"
            or mark.route_complete is not bool(self.completed)
            or mark.operation_ids
            or mark.thread_id is not None
            or mark.turn_id is not None
            or self.completed is not None
            and (self.completed not in mark.delivery_ids or mark.outbox_id != self.completed)
            or closed.get("notice_id") != parent.id
            or closed.get("source_outbox_id") != source.id
            or closed.get("prompt_id") != context.id
            or not same_body(typing, {"version": 1, "prompt_id": prompt.id, "source_outbox_id": source.id, "ambiguous": False})
            or old["prompt_id"] != prompt.id
            or not same_body(old, source.payload.get("work_news_basis", {}).get("source", {}).get("work", {}).get("intent"))
            or any(not same_body(old.get(key), self.intent[key]) for key in inherited)
            or anchor_signature(_item(source)) != self.value["source_signature"]
            or notice_signature(parent) != self.value["notice_signature"]
            or parent.state != "sent"
            or source.state != "sent"
            or parent.payload.get("product_basis", {}).get("origins") != [[self.link.id, mark.message_id, mark.chat_id]]
            or context.kind != "prompt"
            or context.payload.get("purpose") != "retired_presentation"
            or context.payload.get("anchor_outbox_id") != source.id
            or context.payload.get("closed_input") != {"message_id": mark.message_id, "update_id": mark.update_id}
            or selected.kind != "prompt_reopen"
            or selected.payload.get("prompt_id") != context.id
            or selected.payload.get("message_id") != mark.message_id
            or any((row.link_id, row.chat_id, row.binding_generation, row.message_thread_id) != scope for row in (parent, source, selected, prompt, context))
            or any(row.owner_user_id != self.link.owner_user_id for row in (parent, source))
            or source.instance_id != self.link.agent_ref
            or parent.instance_id is not None
        ):
            raise ProductSourceRetired()
        shown_prompt(_item(source), prompt, accepted_at=mark.accepted_at)
        group = [row for row in controls if row.group_id == selected.group_id]
        _validate_rows(parent, group)
        if any("publication_owner" in row.payload or (row.consumed_at is not None) is not bool(self.completed) or self.completed is not None and row.closed_reason != "work-preview" or row.message_id != selected.message_id for row in group):
            raise ProductSourceRetired()
        recipes = closed_parts(mark, available=True)
        self.echo = rows[0][self.column + 6]
        if "closed_voice_echo" in mark.route_basis:
            from app.telegram_bots.closed_voice_echo import qualify_echo

            qualify_echo(self.link, mark, self.echo)
        if not same_body(parent.payload.get("product_basis", {}).get("parts"), recipes) or len(parent.parts) != len(recipes):
            raise ProductSourceRetired()
        acknowledged_parts(parent, [{**recipe, "protect_content": parent.parts[index]["protect_content"], **({"stored_keyboard": True} if index == len(recipes) - 1 else {})} for index, recipe in enumerate(recipes)])
        self.selected = selected
        if self.completed is not None:
            from app.telegram_bots.work_request_source import cleanup_ready

            current, cleanup = rows[0][self.column + 4 : self.column + 6]
            if current is None or not same_body(stored_prompt_intent(_item(current)), self.intent) or cleanup is None or cleanup.payload.get("target_message_id") != selected.message_id:
                raise ProductSourceRetired()
            expected = SimpleNamespace(
                id=current.id,
                link_id=self.link.id,
                owner_user_id=self.link.owner_user_id,
                binding_generation=self.link.binding_generation,
                chat_id=self.link.chat_id,
                payload={"work_news_basis": {"cleanup_id": current.payload.get("reopen_cleanup_id")}},
            )
            if not cleanup_ready(expected, cleanup) and current.state == "sending":
                raise DeliveryRetry(5)


class ReopenAdmission:
    def __init__(self, source, proof):
        self.source, self.proof = source, proof

    def extend(self, query):
        return self.proof.extend(self.source.extend(query))

    def check_with_permissions(self, rows, held):
        self.source.check_with_permissions(rows, held)
        if "agents:write" not in held or not prompt_permissions(self.source) & 2:
            raise AgentDenied("The assignment is unavailable")
        self.proof.check(rows)


async def reopen_assignment(owner, link, selected, origin, *, guard):
    callbacks, outbox = owner.callbacks, owner.outbox
    prompt_id = digest(["assignment-prompt-reopen", link.id, link.binding_generation, selected.id])[:32]
    async with outbox.sessions() as reader:
        existing = (
            await reader.execute(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key.in_(("notice:assignment-prompt:" + prompt_id, "notice:management-prompt:" + prompt_id))))
        ).scalar_one_or_none()
    if existing is not None:
        item = _item(existing)
        plans, intent = prompt_plans(owner, item), stored_prompt_intent(item)
        if intent["version"] != 2 or intent["reopen"]["callback_id"] != selected.id or not same_body(intent["reopen"]["input"], origin_facts(origin)):
            raise ProductSourceRetired()
        current = await plans.reader.read(link, item, allow_closed=True)
        projection = plans.projection(plans.reader, link, item, current, clock=await plans.reader.prepare_clock(link, item), allow_closed=True)
        admission = await prepare_attention(plans.reader, link)
        async with callbacks.writer(link, guard) as writer:
            await admission(session=writer, projection=projection)
        if existing.state != "sent":
            await plans.materialize(link, existing.id, guard=guard)
        return existing.id
    captured = origin.route_basis["closed_typing"]
    async with outbox.sessions() as reader:
        source = await reader.get(TelegramOutboxRow, captured["source_outbox_id"])
        parent = await reader.get(TelegramOutboxRow, origin.route_basis["work_closed"]["notice_id"])
    if source is None or parent is None:
        return await owner.host.work_callbacks.closed.unavailable(link, selected, guard=guard)
    item = _item(source)
    plans = prompt_plans(owner, item)
    try:
        current = await plans.reader.read(link, item, allow_closed=True)
    except (AgentDenied, ProductSourceRetired):
        return await owner.host.work_callbacks.closed.unavailable(link, selected, guard=guard)
    intent = stored_prompt_intent(item)
    descriptor = require_reopen(
        {
            "version": 1,
            "input": origin_facts(origin),
            "source_outbox_id": source.id,
            "source_prompt_id": captured["prompt_id"],
            "source_signature": anchor_signature(item),
            "notice_id": parent.id,
            "context_prompt_id": origin.route_basis["work_closed"]["prompt_id"],
            "callback_id": selected.id,
            "notice_signature": notice_signature(parent),
        }
    )
    intent = {**deepcopy(intent), "version": 2, "prompt_id": prompt_id, "reopen": descriptor}
    proof = AssignmentReopenProof(link, descriptor, intent, completed=None, sqlite=outbox.sessions.kw["bind"].dialect.name == "sqlite")
    clock = await plans.reader.prepare_clock(link, item)
    projection = plans.projection(plans.reader, link, item, current, clock=clock, allow_closed=True)
    admission = await prepare_attention(plans.reader, link)
    composed = ReopenAdmission(projection, proof)
    try:
        await admission(projection=composed)
    except (AgentDenied, ProductSourceRetired):
        return await owner.host.work_callbacks.closed.unavailable(link, selected, guard=guard)
    async with callbacks.writer(link, guard) as writer:
        await admission(session=writer, projection=composed)
        group = await callbacks.lock_current(link, selected.id, chat_id=selected.chat_id, message_id=selected.message_id, message_thread_id=selected.message_thread_id, guard=guard, session=writer)
        mark = await writer.get(TelegramInboundMarkRow, (link.id, origin.message_id, origin.chat_id), with_for_update=True, populate_existing=True)
        now = outbox.clock()
        management = intent["purpose"] == "work_edit"
        family = "management_prompt_intent" if management else "assignment_prompt_intent"
        payload = {"presentation_version": 0, family: intent}
        row = TelegramOutboxRow(
            id=uuid4().hex,
            link_id=link.id,
            owner_user_id=link.owner_user_id,
            binding_generation=link.binding_generation,
            chat_id=link.chat_id,
            instance_id=link.agent_ref,
            work_id=source.work_id,
            message_thread_id=origin.message_thread_id,
            class_="reply",
            dedupe_key=("notice:management-prompt:" if management else "notice:assignment-prompt:") + prompt_id,
            payload=payload,
            parts=[],
            state="pending",
            silent=True,
            not_before=now,
            created_at=now,
        )
        writer.add(row)
        await writer.flush()
        cleanup = keyboard_cleanup_row(group.selected, owner_user_id=link.owner_user_id, now=now, dedupe_key="notice:typing-reopen-cleanup:" + selected.id)
        cleanup.payload = {**cleanup.payload, "work_request_parent_id": row.id}
        writer.add(cleanup)
        row.payload = {**row.payload, "reopen_cleanup_id": cleanup.id, **({"management_prompt_cleanup_id": cleanup.id} if management else {})}
        await writer.flush()
        if "voice_note" in mark.route_basis:
            await owner.host.acts.finish_voice_note(link, message_id=mark.message_id, duration=mark.route_basis["voice_note"]["original"]["voice"]["duration"], used_as="discarded", session=writer)
        await outbox.inbox.register_delivery([InboxRow.from_row(mark)], outbox_id=row.id, primary=True, finish=True, session=writer)
        await callbacks.consume_group(group, reason="work-preview", session=writer)
        proof.completed = row.id
        if management:
            from app.telegram_bots.work_management_prompt_sources import editor_from
            from app.telegram_bots.work_view_sources import detail_read

            target = await writer.get(TelegramOutboxRow, intent["parent_id"], with_for_update=True, populate_existing=True)
            target.payload = {**target.payload, "work_management_editor": editor_from(intent)}
            await writer.flush()
            await writer.refresh(row)
            fresh = plans.projection(plans.reader, link, _item(row), detail=detail_read(current.work["detail"]), opening=current.work["opening"], history=current.work["history"], dedupe_key=current.work["owner_key"], clock=clock)
            await admission(session=writer, projection=fresh)
        else:
            await admission(session=writer, projection=composed)
        identity = row.id
    await plans.materialize(link, identity, guard=guard)
    return identity
