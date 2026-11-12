"""Collect actual instance album members before choosing their input owner."""

import re
from dataclasses import dataclass

from sqlalchemy import select, tuple_

from app.telegram_bots.callbacks import CallbackRefused, normalize_callback_topic
from app.telegram_bots.inbox import InboxRow, _scope, chat_input_parts
from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow
from deerflow.utils.file_io import await_drained


@dataclass(frozen=True)
class AlbumCapture:
    work_owned: bool
    origins: tuple[InboxRow, ...]


def album_manifest(origin, link):
    """Only the closed stored decision can establish a prior cohort's owner."""
    album = origin.route_basis.get("album")
    keys = {"version", "id", "message_ids", "source_ids", "state", "root_message_id", "prior_message_ids"}
    if type(album) is not dict or set(album) != keys or type(album["version"]) is not int or album["version"] != 1 or type(album["id"]) is not str or not album["id"] or album["id"] != origin.sanitized_input.get("media_group_id"):
        raise CallbackRefused()
    members, prior, sources = album["message_ids"], album["prior_message_ids"], album["source_ids"]
    for values, limit in ((members, 64), (prior, 128)):
        if type(values) is not list or len(values) > limit or any(type(value) is not int or value <= 0 for value in values) or len(set(values)) != len(values):
            raise CallbackRefused()
    if not members or origin.message_id not in members or set(members).intersection(prior):
        raise CallbackRefused()
    if type(sources) is not list or len(sources) > 128 or any(type(value) is not str or re.fullmatch(r"[0-9a-f]{32}", value) is None for value in sources) or sources != sorted(set(sources)):
        raise CallbackRefused()
    state, root = album["state"], album["root_message_id"]
    if state == "ordinary":
        if root is not None or sources or origin.route_kind != "turn" or origin.route_target_id != link.agent_ref or "source_outbox_id" in origin.route_basis:
            raise CallbackRefused()
    elif state == "ready":
        if type(root) is not int or root not in members or len(sources) != 1 or prior or origin.route_basis.get("source_outbox_id") != sources[0]:
            raise CallbackRefused()
        if "work_album" in origin.route_basis:
            from app.telegram_bots.work_album_inputs import album_context

            context = album_context(origin)
            is_root = origin.message_id == root
            if context["root_message_id"] != root or context["member_ids"] != members or context["source_outbox_id"] != sources[0] or origin.route_target_id != context["request_id"]:
                raise CallbackRefused()
            if origin.route_kind != ("work_input" if is_root else "work_album_member") or origin.operation_ids != ([context["operation_id"]] if is_root else []):
                raise CallbackRefused()
        elif origin.route_kind != "work_file_album" or origin.route_target_id != sources[0]:
            raise CallbackRefused()
    elif state in {"ambiguous", "late"}:
        if root is not None or not sources or origin.route_kind != "work_file_album" or origin.route_target_id is not None or "source_outbox_id" in origin.route_basis or state == "late" and not prior:
            raise CallbackRefused()
    else:
        raise CallbackRefused()
    return album


class WorkFileAlbums:
    def __init__(self, files):
        self.files, self.inputs, self.services = files, files.inputs, files.services
        self.inbox = files.inbox

    async def submit(self, channel, origin):
        from app.telegram_bots.work_album_answers import WorkAlbumAnswers

        return await WorkAlbumAnswers(self.files).submit(channel, origin)

    async def accept(self, link, update, value, profile):
        """The first committed receipt already belongs to durable collection."""

        async def perform():
            async with self.inbox._transaction() as writer:
                origin, created = await self.inbox.accept(link, update, sanitized_input=value, session=writer)
                if origin.route_kind is None:
                    origin = await self.inbox.route(origin, kind="work_album_pending", target_id=link.agent_ref, basis={"input_profile": profile}, operation_ids=[], session=writer)
                return origin, created

        return await await_drained(perform())

    async def recover(self, phase, *, link_id=None):
        """Classify retained input metadata while ingress is still held back."""
        from app.telegram_bots.turns import _chat_line, _Guard

        async def perform():
            row, after = TelegramInboundMarkRow, None
            order = (row.link_id, row.message_id, row.chat_id)
            while phase():
                try:
                    async with self.inbox.session_factory() as reader:
                        query = select(row).where(row.route_kind == "work_album_pending", row.route_complete.is_(False))
                        if link_id is not None:
                            query = query.where(row.link_id == link_id)
                        if after is not None:
                            query = query.where(tuple_(*order) > after)
                        page = tuple(InboxRow.from_row(mark) for mark in await reader.scalars(query.order_by(*order).limit(32)))
                except Exception as error:
                    _chat_line("work_album_recovery_held", error=error)
                    return
                seen = set()
                for origin in page:
                    if not phase():
                        return
                    key = (origin.link_id, origin.chat_id, origin.binding_generation, origin.message_thread_id, origin.sanitized_input.get("media_group_id"))
                    if key in seen:
                        continue
                    seen.add(key)
                    try:
                        link = await self.services.links.get(origin.link_id)
                        channel = self.services.channel(origin.link_id)
                        if link is None or channel is None:
                            continue

                        def current():
                            return phase() and self.services.channel(link.id) is channel and channel._same_scope(link, channel.link) and channel.transport_revision == link.token_revision and not channel._stopping

                        if not current():
                            continue
                        guard = _Guard(self.services, channel, link, current, repair=True)
                        await self.capture(channel, link, (origin,), guard=guard)
                    except Exception as error:
                        _chat_line("work_album_recovery_held", error=error, link=origin.link_id)
                if len(page) < 32:
                    return
                last = page[-1]
                after = last.link_id, last.message_id, last.chat_id

        await await_drained(perform())

    async def _members(self, writer, link, first):
        row = TelegramInboundMarkRow
        marks = list(
            await writer.scalars(
                select(row)
                .where(
                    row.link_id == link.id,
                    row.chat_id == link.chat_id,
                    row.binding_generation == link.binding_generation,
                    row.message_thread_id == first.message_thread_id,
                    row.sanitized_input["media_group_id"].as_string() == first.sanitized_input["media_group_id"],
                )
                .order_by(row.update_id, row.message_id)
                .limit(129)
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        )
        if not marks or len(marks) > 128:
            raise CallbackRefused()
        return marks

    async def capture(self, channel, link, origins, *, guard=None):
        """Freeze the complete durable cohort before any member has a file effect."""

        async def perform():
            if not origins or link.agent_kind != "instance":
                raise CallbackRefused()
            album_id = origins[0].sanitized_input.get("media_group_id")
            if type(album_id) is not str or not album_id or len(origins) > 64 or len({origin.message_id for origin in origins}) != len(origins):
                raise CallbackRefused()
            actual_guard = guard if guard is not None else self.services.commands._guard(channel, link)
            async with self.services.callbacks.writer(link, actual_guard) as writer:
                marks = await self._members(writer, link, origins[0])
                by_id = {mark.message_id: mark for mark in marks}
                for origin in origins:
                    if origin.message_id not in by_id:
                        raise CallbackRefused()
                    self.inbox._publication_snapshot_matches(by_id[origin.message_id], origin)
                captured = tuple(InboxRow.from_row(mark) for mark in marks)
            pending = [mark for mark in captured if mark.route_kind == "work_album_pending"]
            prior = [mark for mark in captured if mark.route_kind != "work_album_pending"]
            if not pending or len(pending) > 64:
                raise CallbackRefused()
            scope = (link.id, link.chat_id, link.binding_generation, origins[0].message_thread_id)
            sources, quoted_roots, unknown = set(), [], False
            for origin in captured:
                self.inbox._current(link, origin)
                if (origin.link_id, origin.chat_id, origin.binding_generation, origin.message_thread_id) != scope or origin.sanitized_input.get("media_group_id") != album_id:
                    raise CallbackRefused()
                _text, files = chat_input_parts(origin, bot_id=link.bot_id)
                if len(files) != 1 or files[0]["message_id"] != origin.message_id or origin.route_basis.get("input_profile", {}).get("forwarded") is not False:
                    raise CallbackRefused()
                if origin in prior:
                    album = album_manifest(origin, link)
                    sources.update(album["source_ids"])
                    continue
                if origin.route_target_id != link.agent_ref or origin.route_complete or any(value is not None for value in (origin.turn_id, origin.thread_id, origin.run_id, origin.outbox_id)) or origin.operation_ids:
                    raise CallbackRefused()
                quoted = origin.sanitized_input.get("reply_to_message_id")
                if quoted is not None:
                    source = await self.inputs.cards.find(link, quoted, topic=normalize_callback_topic(origin.message_thread_id))
                    if source is None:
                        unknown = True
                    else:
                        sources.add(source)
                        quoted_roots.append(origin)
            prior_work = any(origin.route_kind == "work_file_album" or "work_album" in origin.route_basis for origin in prior)
            work_owned = bool(sources) or prior_work
            state, root = "ordinary", None
            if work_owned:
                if prior_work:
                    state = "late"
                elif len(sources) != 1 or unknown or prior or len({origin.sanitized_input.get("prompt_id") for origin in pending}) != 1:
                    state = "ambiguous"
                else:
                    state, root = "ready", min(quoted_roots, key=lambda row: (row.update_id, row.message_id)).message_id
            source_ids = sorted(sources)
            album = {"version": 1, "id": album_id, "message_ids": [origin.message_id for origin in pending], "source_ids": source_ids, "state": state, "root_message_id": root, "prior_message_ids": [origin.message_id for origin in prior]}
            async with self.services.callbacks.writer(link, actual_guard) as writer:
                marks = await self._members(writer, link, origins[0])
                by_id = {mark.message_id: mark for mark in marks}
                if set(by_id) != {origin.message_id for origin in captured}:
                    raise CallbackRefused()
                for origin in captured:
                    self.inbox._publication_snapshot_matches(by_id[origin.message_id], origin)
                changed = []
                for origin in pending:
                    mark = by_id[origin.message_id]
                    if mark.route_complete or any(value is not None for value in (mark.turn_id, mark.thread_id, mark.run_id, mark.outbox_id)):
                        raise CallbackRefused()
                    mark.route_kind = "work_file_album" if work_owned else "turn"
                    mark.route_target_id = source_ids[0] if state == "ready" else None if work_owned else link.agent_ref
                    mark.state = "reply_pending" if work_owned else mark.state
                    mark.route_basis = {**mark.route_basis, "album": album, **({"source_outbox_id": source_ids[0]} if state == "ready" else {})}
                    changed.append(mark)
                if not work_owned:
                    partitions = []
                    for mark in changed:
                        if partitions and _scope(partitions[-1][-1]) == _scope(mark):
                            partitions[-1].append(mark)
                        else:
                            partitions.append([mark])
                    for partition in partitions:
                        ordered = self.inbox._prepublication_members(partition)
                        for mark in ordered:
                            mark.grouped_message_ids = [other.message_id for other in ordered if other is not mark]
                await writer.flush()
                for mark in changed:
                    for source_id in source_ids:
                        await self.inbox.register_delivery([InboxRow.from_row(mark)], outbox_id=source_id, primary=False, finish=False, session=writer)
                return AlbumCapture(work_owned, tuple(InboxRow.from_row(mark) for mark in changed))

        return await await_drained(perform())
