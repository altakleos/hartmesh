"""Prepare ordinary-chat list commands without enabling their public dispatch."""

import asyncio

from app.telegram_bots.agent_list_authority import AgentListGuard
from app.telegram_bots.agent_list_source import read_list
from app.telegram_bots.agent_lists import list_parts, present_list
from app.telegram_bots.callbacks import CallbackRefused
from deerflow.utils.file_io import await_drained


async def handle_list(commands, channel, link, update, word, *, source_id):
    from app.telegram_bots.agent_list_authority import source_basis

    if word not in {"assistants", "employees"} or link.agent_kind != "lead_agent" or link.agent_ref != "lead_agent":
        raise CallbackRefused()
    host = commands.services
    base = commands._guard(channel, link)
    await base.prepare()
    inbox = host.outbox.inbox
    origin, _ = await inbox.accept(link, update, sanitized_input={"command": word})
    base.current()
    if origin.binding_generation != link.binding_generation:
        raise CallbackRefused()
    if origin.route_complete:
        return
    if origin.route_kind is None:

        async def current():
            base.current()

        source = await read_list(host, link, custody="personal" if word == "assistants" else "company", before_read=current)
        settings = await host.settings.get(link.owner_user_id)
        if settings is None:
            raise CallbackRefused()

        def prepare():
            presentation = present_list(source.snapshot, link=link, bots=source.bots, details=source.details, locale=settings.locale, product=host.product_name, administrator=source.administrator)
            parts = list_parts(presentation)
            for part in parts:
                part["reply_to_message_id"] = origin.message_id
            return {"list": source_basis(source), "parts": parts, "keyboard": presentation.keyboard}

        basis = await await_drained(asyncio.to_thread(prepare))
        base.current()
        origin = await inbox.route(origin, kind="command", target_id=word, basis=basis, operation_ids=[])
    if origin.route_kind != "command" or origin.route_target_id != word or not {"list", "parts", "keyboard"}.issubset(origin.route_basis):
        raise CallbackRefused()
    basis = origin.route_basis
    guard = AgentListGuard(host, base, basis["list"])
    if basis.get("human_input_ui") != {"command_closed": word}:
        await host.prompts.close(link, origin.chat_id, origin.message_thread_id, reason="command", shown_only=True)
    base.current()
    await host.outbox.plan_agent_list(link, source_id=source_id, parts=basis["parts"], keyboard=basis["keyboard"], callbacks=host.callbacks, guard=guard, inbox_rows=[origin], message_thread_id=origin.message_thread_id)
