"""Stop a person's summaries while retaining the actual acknowledged navigation."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from sqlalchemy import select, text
from sqlalchemy.orm import aliased

from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.callbacks import CallbackRefused, StoredButton, _validate_rows
from app.telegram_bots.links import prepare_publication_guard
from app.telegram_bots.message_settings import choose, setting_changes
from app.telegram_bots.notifications import aware
from app.telegram_bots.producer import DeliveryClaimRetired, ProductSourceRetired, _item
from app.telegram_bots.render import render
from app.telegram_bots.split import visible_text
from app.telegram_bots.summary_delivery import frozen_summary
from app.telegram_bots.summary_views import frozen_view
from app.telegram_bots.work_actions import same_body
from app.telegram_bots.work_digest_members import claim_of
from app.telegram_bots.work_digest_navigation import _same_item
from app.telegram_bots.work_digest_views import parent_proof
from app.telegram_bots.work_request_source import require_show
from deerflow.agent_instances.contract import AgentDenied
from deerflow.persistence.telegram_bots.model import TelegramActRow, TelegramBotLinkRow, TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def frozen_stop(captured, anchor):
    """Validate the complete edit against its acknowledged original message."""
    try:
        item, link = captured.item, captured.link
        basis = item.payload["summary_stop_basis"]
        if (
            type(basis) is not dict
            or set(basis) != {"version", "anchor_id", "anchor_proof", "callback_id", "locale"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or basis.get("locale") not in {"en-US", "zh-CN"}
            or any(type(basis[key]) is not str or len(basis[key]) != length or any(char not in "0123456789abcdef" for char in basis[key]) for key, length in (("anchor_id", 32), ("anchor_proof", 64), ("callback_id", 32)))
            or anchor.id != basis["anchor_id"]
            or parent_proof(anchor) != basis["anchor_proof"]
            or set(item.payload) != {"presentation_version", "summary_stop_basis"}
            or type(item.payload["presentation_version"]) is not int
            or item.payload["presentation_version"] != 0
            or item.class_ != "reply"
            or item.dedupe_key != "notice:settings:" + basis["callback_id"]
            or item.silent is not True
            or (item.link_id, item.owner_user_id, item.binding_generation, item.chat_id, item.instance_id) != (link.id, link.owner_user_id, link.binding_generation, link.chat_id, link.agent_ref)
            or any(value is not None for value in (item.work_id, item.request_id, item.event_id, item.message_thread_id))
            or len(item.parts) != 1
        ):
            raise ValueError()
        original = SimpleNamespace(link=link, item=anchor, part=None)
        if "summary_view_basis" in anchor.payload:
            summary, _ = frozen_view(original, acknowledged=True)
        else:
            summary, _ = frozen_summary(original, acknowledged=True)
            if summary["concealed"]:
                raise ValueError()
        expected = SummaryPreferences.recipe(SimpleNamespace(anchor=anchor, locale=basis["locale"]), item.id)
        part = item.parts[0]
        metadata = {
            "part_id",
            "ordinal",
            "presentation_version",
            "state",
            "message_ids",
            "dispatched_at",
            "sent_at",
            "plain_fallback",
            "url_button_fallback",
            "invalid_token_verification_pending",
            "callback_group_id",
            "summary_body_redacted",
        }
        if (
            part.get("callback_group_id") is not None
            or not same_body({key: value for key, value in part.items() if key not in metadata}, expected)
            or type(part.get("ordinal")) is not int
            or part["ordinal"] != 0
            or type(part.get("presentation_version")) is not int
            or part["presentation_version"] != 0
            or type(part.get("part_id")) is not str
            or len(part["part_id"]) != 32
            or captured.part is not None
            and not same_body(captured.part, part)
            or any(type(part[key]) is not bool for key in ("plain_fallback", "url_button_fallback", "summary_body_redacted") if key in part)
        ):
            raise ValueError()
        return anchor.payload["product_keyboard_basis"]
    except (KeyError, TypeError, ValueError):
        raise ProductSourceRetired() from None


@dataclass
class StopSnapshot:
    owner: object
    link: object
    anchor: object
    root: object
    protected: bool | None
    callback_id: str
    locale: str
    show_id: str | None
    stopped: bool = False

    async def authorize(self, *, session=None, child=None, captured=None):
        projection = StopProjection(self, child=child, captured=captured)
        if self.protected is not None:
            admission = await prepare_attention(self.owner.plans.reader, self.link)
            return await admission(session=session, projection=projection)
        services = self.owner.services
        live = await services.prepare_live_owner(self.link.owner_user_id)
        guard = services.links.publication_guard(self.link, check_live_owner=live, check_attempt_current=lambda: True)
        if session is not None:
            return await guard.check_projection(session, projection)
        async with self.owner.plans.outbox.sessions() as writer, writer.begin():
            return await guard.check_projection(writer, projection)


class StopProjection:
    def __init__(self, snapshot, *, child=None, captured=None):
        self.snapshot, self.child, self.captured = snapshot, child, captured

    def extend(self, query):
        self.column = len(query.column_descriptions)
        anchor, root, controls, shows, settings, child = aliased(TelegramOutboxRow), aliased(TelegramOutboxRow), aliased(TelegramCallbackRow), aliased(TelegramCallbackRow), TelegramPersonSettingsRow, aliased(TelegramOutboxRow)
        n = self.snapshot
        return (
            query.add_columns(anchor, root, controls, shows, settings, child, TelegramActRow)
            .join(anchor, anchor.id == n.anchor.id)
            .join(root, root.id == n.root.id)
            .outerjoin(controls, controls.group_id == n.anchor.parts[-1]["callback_group_id"])
            .outerjoin(shows, shows.id == n.show_id)
            .join(settings, settings.owner_user_id == anchor.owner_user_id)
            .outerjoin(child, child.id == (self.child.id if self.child else None))
            .outerjoin(TelegramActRow, TelegramActRow.id == n.callback_id)
        )

    def check(self, rows):
        return self.check_with_permissions(rows, frozenset())

    def check_with_permissions(self, rows, held):
        n = self.snapshot
        if n.protected is not None:
            instance, policy = rows[0][1], rows[0][3]
            if (instance.custody == "company" and policy == "protected") != n.protected:
                raise ProductSourceRetired()
        anchor, root, _, show, settings, child, act = rows[0][self.column : self.column + 7]
        if _item(anchor) != n.anchor or _item(root) != n.root or settings.locale != n.locale or n.stopped and settings.summary_enabled:
            raise ProductSourceRetired()
        for parent in (anchor, root):
            if parent.state != "sent" or claim_of(parent) != (None, None, None) or parent.message_id != parent.parts[-1]["message_ids"][0]:
                raise ProductSourceRetired()
        if n.show_id is not None:
            require_show(show, root)
        controls = {row[self.column + 2].id: row[self.column + 2] for row in rows if row[self.column + 2] is not None}
        try:
            _validate_rows(anchor, list(controls.values()))
            selected = controls.get(n.callback_id)
            if selected is None or (selected.kind, selected.target_kind, selected.target_id) != ("summary_off", "link", n.link.id):
                raise CallbackRefused()
            for control in controls.values():
                if control.message_id != anchor.message_id or aware(control.created_at) != aware(root.created_at):
                    raise CallbackRefused()
                n.owner.plans.callbacks._unexpired(control)
                if control.id == n.callback_id and n.stopped:
                    if control.consumed_at is None or control.closed_reason != "done":
                        raise CallbackRefused()
                elif control.consumed_at is not None:
                    raise CallbackRefused()
        except CallbackRefused:
            raise ProductSourceRetired() from None
        if n.stopped:
            try:
                if act is None:
                    raise CallbackRefused()
                body = n.owner.services.acts._act_scope(act, n.link, anchor.message_id)
                if (
                    act.state != "done"
                    or act.attempts != 1
                    or act.first_attempt_at is None
                    or body["field"] != "summary"
                    or body["new"]["enabled"] is not False
                    or type(act.result) is not dict
                    or set(act.result) != {"outbox_id", "field", "value"}
                    or act.result["field"] != "summary"
                    or not same_body(act.result["value"], body["new"])
                    or self.child is not None
                    and act.result["outbox_id"] != self.child.id
                ):
                    raise CallbackRefused()
            except (ValueError, CallbackRefused):
                raise ProductSourceRetired() from None
        if self.child is not None:
            if child is None or not _same_item(child, self.child):
                raise ProductSourceRetired()
            if self.captured is not None:
                c = self.captured
                latest = n.owner.plans.outbox._sending(rows[0][0], child, link_id=n.link.id, part_id=c.part["part_id"], claim_owner=c.claim_owner, claim_id=c.claim_id, token_revision=c.token_revision)
                if latest != c:
                    raise DeliveryClaimRetired()


class _StopGuard:
    def __init__(self, base, snapshot):
        self.base, self.snapshot, self.child = base, snapshot, None
        self.captured_link = base.captured_link

    async def prepare(self):
        await prepare_publication_guard(self.base)

    def current(self):
        self.base.current()

    async def check(self, writer):
        current = await self.base.check(writer)
        await self.snapshot.authorize(session=writer, child=self.child)
        self.current()
        return current


class SummaryPreferences:
    def __init__(self, plans, services):
        self.plans, self.services = plans, services

    async def read(self, link, anchor_id, callback_id, *, stopped=False, content=True, locale=None):
        async with self.plans.outbox.sessions() as reader:
            anchor = await reader.get(TelegramOutboxRow, anchor_id)
            if anchor is None:
                raise ProductSourceRetired()
            anchor = _item(anchor)
            captured = SimpleNamespace(link=link, item=anchor, part=None)
            show_id = None
            if "summary_view_basis" in anchor.payload:
                basis, _ = frozen_view(captured, acknowledged=True)
                root = await reader.get(TelegramOutboxRow, basis["parent_id"])
                if root is None or parent_proof(_item(root)) != basis["parent_proof"]:
                    raise ProductSourceRetired()
                root = _item(root)
                original, _ = frozen_summary(SimpleNamespace(link=link, item=root, part=None), acknowledged=True)
                summary, show_id = basis["summary"], basis["callback_id"]
                if not original["concealed"]:
                    raise ProductSourceRetired()
            else:
                summary, _ = frozen_summary(captured, acknowledged=True)
                if summary["concealed"]:
                    raise ProductSourceRetired()
                root = anchor
        # An acknowledged summary is historical. Fence its current read access
        # and protection without rebuilding or comparing mutable Work sections.
        protected = summary["protected"] if content else None
        if locale is None:
            settings = await self.services.settings.get(link.owner_user_id)
            if settings is None:
                raise ProductSourceRetired()
            locale = settings.locale
        snapshot = StopSnapshot(self, link, anchor, root, protected, callback_id, locale, show_id, stopped)
        await snapshot.authorize()
        return snapshot

    async def execute_callback(self, channel, row, **kwargs):
        if row.kind != "summary_off":
            return False

        async def perform():
            link = deepcopy(channel.link)
            snapshot = await self.read(link, row.payload.get("anchor_outbox_id"), row.id, content=False)
            guard = _StopGuard(self.services.commands._guard(channel, link), snapshot)
            await guard.prepare()
            async with self.plans.outbox.sessions() as writer, writer.begin():
                if writer.get_bind().dialect.name == "sqlite":
                    await writer.execute(text("BEGIN IMMEDIATE"))
                settings = await writer.scalar(select(TelegramPersonSettingsRow).where(TelegramPersonSettingsRow.owner_user_id == link.owner_user_id).with_for_update().execution_options(populate_existing=True))
                await writer.execute(select(TelegramBotLinkRow.id).where(TelegramBotLinkRow.owner_user_id == link.owner_user_id).order_by(TelegramBotLinkRow.id).with_for_update())
                group = await self.plans.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
                if group.anchor_outbox_id != snapshot.anchor.id:
                    raise CallbackRefused()
                body = choose(settings, "summary", "off")
                await self.services.acts.write(link, act_id=row.id, kind="settings", body=body, message_id=row.message_id, session=writer)
                await self.services.settings.update(link.owner_user_id, **setting_changes(body), session=writer)
                selected = await writer.get(TelegramCallbackRow, row.id)
                selected.consumed_at, selected.closed_reason = self.plans.callbacks.clock(), "done"
                snapshot.stopped = True
                identity = uuid4().hex
                basis = {"version": 1, "anchor_id": snapshot.anchor.id, "anchor_proof": parent_proof(snapshot.anchor), "callback_id": row.id, "locale": snapshot.locale}
                part = self.recipe(snapshot, identity)
                child = TelegramOutboxRow(
                    id=identity,
                    link_id=link.id,
                    owner_user_id=link.owner_user_id,
                    binding_generation=link.binding_generation,
                    chat_id=link.chat_id,
                    instance_id=link.agent_ref,
                    dedupe_key="notice:settings:" + row.id,
                    class_="reply",
                    payload={"presentation_version": 0, "summary_stop_basis": basis},
                    parts=self.plans.outbox._parts([part], 0),
                    state="pending",
                    silent=True,
                    not_before=self.plans.outbox.clock(),
                    created_at=self.plans.outbox.clock(),
                )
                writer.add(child)
                act = await writer.get(TelegramActRow, row.id)
                act.state, act.attempts, act.first_attempt_at, act.updated_at = "done", 1, datetime.now(UTC), datetime.now(UTC)
                act.result = {"outbox_id": identity, "field": "summary", "value": body["new"]}
                await writer.flush()
                guard.child = _item(child)
                await guard.check(writer)

        await await_drained(perform())
        return True

    @staticmethod
    def recipe(snapshot, identity):
        original = snapshot.anchor.parts[-1]
        html = original["html"] + "\n" + render("summary.off", snapshot.locale)
        return {
            "kind": "edit",
            "source_ref": {"key": "summary.stopped", "values": {"outbox_id": identity}},
            "html": html,
            "visible_text": visible_text(html),
            "target_message_id": original["message_ids"][0],
            "target_protect_content": original["protect_content"],
            "protect_content": original["protect_content"],
            "silent": True,
        }

    async def prepare(self, captured):
        basis = captured.item.payload.get("summary_stop_basis")
        if (
            type(basis) is not dict
            or set(basis) != {"version", "anchor_id", "anchor_proof", "callback_id", "locale"}
            or type(basis["version"]) is not int
            or basis["version"] != 1
            or basis.get("locale") not in {"en-US", "zh-CN"}
            or captured.item.class_ != "reply"
            or captured.item.dedupe_key != "notice:settings:" + basis["callback_id"]
            or captured.item.silent is not True
            or len(captured.item.parts) != 1
        ):
            raise ProductSourceRetired()
        try:
            snapshot = await self.read(captured.link, basis["anchor_id"], basis["callback_id"], stopped=True, content=not captured.part.get("summary_body_redacted", False), locale=basis["locale"])
        except AgentDenied:
            await self.redact(captured)
            raise DeliveryClaimRetired() from None
        frozen_stop(captured, snapshot.anchor)
        return snapshot

    async def read_part(self, captured):
        snapshot = await self.prepare(captured)
        if captured.part.get("summary_body_redacted"):
            html = render("summary.off", snapshot.locale)
            return html, visible_text(html)
        return captured.part["html"], captured.part["visible_text"]

    async def keyboard(self, captured):
        snapshot = await self.prepare(captured)
        if captured.part.get("summary_body_redacted"):
            return ()
        async with self.plans.outbox.sessions() as reader:
            rows = list(await reader.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == snapshot.anchor.parts[-1]["callback_group_id"])))
        descriptors, positions = _validate_rows(snapshot.anchor, rows)
        return tuple(
            tuple(
                StoredButton(spec["label"], url=spec["url"], label_values=spec.get("label_values")) if "url" in spec else StoredButton(spec["label"], callback_data="h1:" + positions[line, column].id, label_values=spec.get("label_values"))
                for column, spec in enumerate(buttons)
                if spec.get("kind") != "summary_off"
            )
            for line, buttons in enumerate(descriptors)
            if any(spec.get("kind") != "summary_off" for spec in buttons)
        )

    async def authorize(self, captured, *, silent, now):
        if silent is not True:
            raise ProductSourceRetired()
        snapshot = await self.prepare(captured)
        try:
            await snapshot.authorize(child=captured.item, captured=captured)
        except AgentDenied:
            await self.redact(captured)
            raise DeliveryClaimRetired() from None

    async def redact(self, captured):
        """Persist a one-way, source-free fallback before another SDK admission."""
        snapshot = await self.read(captured.link, captured.item.payload["summary_stop_basis"]["anchor_id"], captured.item.payload["summary_stop_basis"]["callback_id"], stopped=True, content=False)
        frozen_stop(captured, snapshot.anchor)
        async with self.plans.outbox._writer(captured.link.id) as (writer, binding):
            row = await writer.get(TelegramOutboxRow, captured.item.id, with_for_update=True)
            current = self.plans.outbox._sending(binding, row, link_id=captured.link.id, part_id=captured.part["part_id"], claim_owner=captured.claim_owner, claim_id=captured.claim_id, token_revision=captured.token_revision)
            if current != captured:
                raise DeliveryClaimRetired()
            await snapshot.authorize(session=writer, child=captured.item, captured=captured)
            parts = deepcopy(row.parts)
            parts[0].update(summary_body_redacted=True, state="pending")
            row.parts, row.state, row.not_before = parts, "pending", self.plans.outbox.clock()
            row.claim_owner = row.claim_id = row.claimed_at = None


async def acknowledge_stop(writer, row):
    """An edit receipt cannot change the original summary's message identity."""
    from app.telegram_bots.acts import Acts
    from app.telegram_bots.links import BotLink

    link = await writer.get(TelegramBotLinkRow, row.link_id, populate_existing=True)
    basis = row.payload["summary_stop_basis"]
    anchor = await writer.get(TelegramOutboxRow, basis["anchor_id"], populate_existing=True)
    if link is None or anchor is None:
        raise ProductSourceRetired()
    current, parent = BotLink.from_row(link), _item(anchor)
    part = row.parts[0]
    frozen_stop(SimpleNamespace(link=current, item=_item(row), part=part), parent)
    if (
        part["state"] != "sent"
        or type(part["message_ids"]) is not list
        or len(part["message_ids"]) != 1
        or type(part["message_ids"][0]) is not int
        or part["message_ids"] != [part["target_message_id"]]
        or datetime.fromisoformat(part["sent_at"]).utcoffset() is None
    ):
        raise ProductSourceRetired()
    controls = list(await writer.scalars(select(TelegramCallbackRow).where(TelegramCallbackRow.group_id == parent.parts[-1]["callback_group_id"]).execution_options(populate_existing=True)))
    _validate_rows(anchor, controls)
    selected = next((control for control in controls if control.id == basis["callback_id"]), None)
    if selected is None or selected.kind != "summary_off" or selected.consumed_at is None or selected.closed_reason != "done":
        raise ProductSourceRetired()
    original_time = datetime.fromisoformat(parent.payload["summary_view_basis"]["summary"]["as_of"]) if "summary_view_basis" in parent.payload else anchor.created_at
    for control in controls:
        if type(control.message_id) is not int or control.message_id != part["target_message_id"] or aware(control.created_at) != aware(original_time):
            # Show controls retain the root's original timestamp.
            raise ProductSourceRetired()
    if "summary_view_basis" in parent.payload:
        view = parent.payload["summary_view_basis"]
        root = await writer.get(TelegramOutboxRow, view["parent_id"], populate_existing=True)
        shown = await writer.get(TelegramCallbackRow, view["callback_id"], populate_existing=True)
        if root is None or parent_proof(_item(root)) != view["parent_proof"]:
            raise ProductSourceRetired()
        require_show(shown, root)
    act = await writer.get(TelegramActRow, selected.id, populate_existing=True)
    if act is None:
        raise ProductSourceRetired()
    body = Acts._act_scope(act, current, part["target_message_id"])
    if (
        act.state != "done"
        or act.attempts != 1
        or act.first_attempt_at is None
        or body["field"] != "summary"
        or body["new"]["enabled"] is not False
        or not same_body(act.result, {"outbox_id": row.id, "field": "summary", "value": body["new"]})
    ):
        raise ProductSourceRetired()
