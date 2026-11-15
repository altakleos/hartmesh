"""Only complete, owned execution plans finish a typed Stop input."""

from types import SimpleNamespace

from sqlalchemy import select

from app.telegram_bots.acts import _snapshot
from app.telegram_bots.inbox import InboxRow
from app.telegram_bots.producer import ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_command_stop_origins import CommandStopProof, command_basis, operation_id, selected_targets, source_marker
from app.telegram_bots.work_news_delivery import frozen_news
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramInboundMarkRow, TelegramOutboxRow


def delivery_scope(row, mark, *, owner_user_id, instance_id):
    return (row.link_id, row.owner_user_id, row.binding_generation, row.chat_id, row.message_thread_id, row.instance_id) == (mark.link_id, owner_user_id, mark.binding_generation, mark.chat_id, mark.message_thread_id, instance_id)


async def seal_execution_command(plans, link, act_id, *, guard):
    async with plans.outbox._writer(link.id) as (writer, _binding):
        if guard is not None:
            await guard.check(writer)
        journal = await writer.get(TelegramActRow, act_id, populate_existing=True)
        if journal is None or source_marker(journal) is None:
            return
        incoming = await CommandStopProof(link, _snapshot(journal)).read(writer)
        mark = await writer.get(TelegramInboundMarkRow, (incoming.link_id, incoming.message_id, incoming.chat_id), with_for_update=True, populate_existing=True)
        await complete_selected(plans, link, mark, writer, guard=guard)


async def complete_selected(plans, link, mark, writer, *, guard):
    basis = command_basis(mark)
    from app.telegram_bots.work_command_stop_answers import answer_receipt_delivery, selected_answer
    from app.telegram_bots.work_command_stop_origins import read_command_choice

    await read_command_choice(writer, link, basis)
    owned = []
    if selected_answer(basis):
        answer = await answer_receipt_delivery(writer, link, basis, mark)
        if answer is None:
            return
        owned.append(answer.id)
    for target in selected_targets(basis):
        identity = operation_id(basis["input"], target)
        act = await writer.get(TelegramActRow, identity, with_for_update=True, populate_existing=True)
        row = await writer.scalar(select(TelegramOutboxRow).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "notice:work-execution:" + identity).execution_options(populate_existing=True))
        if act is None or row is None:
            return
        proof = CommandStopProof(link, _snapshot(act))
        await proof.read(writer)
        if (
            not delivery_scope(row, mark, owner_user_id=link.owner_user_id, instance_id=basis["instance_id"])
            or row.work_id != target["work_id"]
            or not same_body(row.payload.get("work_execution_intent"), {"version": 1, "act_id": identity, "origin": proof.origin})
        ):
            raise ProductSourceRetired()
        fixed = await fixed_stop_delivery(writer, link, row, act, mark)
        if fixed is not None:
            owned.append(fixed.id)
            continue
        if not row.parts:
            return
        _frozen, source, _part = frozen_news(SimpleNamespace(link=link, item=_item(row), part=None))
        if source.key != "execution.stop" or source.work["act"]["id"] != identity or not same_body(source.work["act"]["body"], act.body):
            raise ProductSourceRetired()
        owned.append(row.id)
    for identity in owned:
        await plans.outbox.inbox.register_delivery([InboxRow.from_row(mark)], outbox_id=identity, finish=False, session=writer)
    await plans.outbox.inbox.finish_route(InboxRow.from_row(mark), session=writer)
    if guard is not None:
        await guard.check(writer)


async def fixed_stop_delivery(writer, link, canonical, act, mark):
    """A real minimal target reply can complete the original typed input."""
    from app.telegram_bots.work_execution_readiness import readiness_signature
    from app.telegram_bots.work_saved_refusals import WorkSavedRefusals
    from app.telegram_bots.work_stop_refusals import StopRefusalProof, permission_refusal, refused_stop
    from deerflow.persistence.agent_instances.work import AgentWorkAttemptRow, AgentWorkRow

    actual = await writer.get(TelegramActRow, act.id, with_for_update=True, populate_existing=True)
    if actual is None:
        raise ProductSourceRetired()
    captured = _snapshot(actual)
    if not refused_stop(captured):
        if permission_refusal(captured):
            raise ProductSourceRetired()
        return None
    proof = StopRefusalProof(captured)
    query = (
        select(AgentWorkAttemptRow, AgentWorkRow)
        .join(AgentWorkRow, AgentWorkRow.id == AgentWorkAttemptRow.work_id)
        .where(AgentWorkAttemptRow.id == captured.body["attempt_id"], AgentWorkRow.id == captured.work_id, AgentWorkRow.instance_id == link.agent_ref)
    )
    facts = (await writer.execute(proof.extend(query))).all()
    if not facts:
        raise ProductSourceRetired()
    proof.check(facts, captured)
    native = facts[0][0]
    if native.thread_id != captured.stop_dispatch["thread_id"] or captured.stop_dispatch["run_id"] is not None and native.run_id != captured.stop_dispatch["run_id"]:
        raise ProductSourceRetired()
    candidates = list(
        await writer.scalars(
            select(TelegramOutboxRow)
            .where(
                TelegramOutboxRow.id.in_(mark.delivery_ids),
                TelegramOutboxRow.link_id == link.id,
                TelegramOutboxRow.payload["work_saved_refusal"]["act_id"].as_string() == act.id,
                TelegramOutboxRow.payload["work_saved_refusal"]["canonical_id"].as_string() == canonical.id,
            )
            .order_by(TelegramOutboxRow.created_at, TelegramOutboxRow.id)
        )
    )
    for row in candidates:
        item = _item(row)
        intent = WorkSavedRefusals.intent(item)
        if "gesture" in intent or intent["key"] not in {"agent.paused", "agent.retired", "access.agent_off", "work.need_inspect"}:
            raise ProductSourceRetired()
        WorkSavedRefusals.validate_item(link, item, _item(canonical), act_id=act.id, signature=readiness_signature(captured, captured), key=intent["key"])
        return row
    return None
