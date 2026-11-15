"""Private saved-edit prompts use the same complete deferred ACK barrier."""

from dataclasses import asdict

from sqlalchemy import false
from sqlalchemy.orm import aliased

from app.telegram_bots.assignment_sources import private_facts
from app.telegram_bots.attention_authority import prepare_attention
from app.telegram_bots.producer import DeliveryRetry, ProductSourceRetired, _item
from app.telegram_bots.work_actions import same_body, thaw
from app.telegram_bots.work_management_edit_sources import ManagementEditFrame
from app.telegram_bots.work_news_sources import WorkNewsRead
from app.telegram_bots.work_request_source import cleanup_ready
from app.telegram_bots.work_result_identity import digest
from app.telegram_bots.work_view_sources import WorkViewReader, detail_read
from deerflow.persistence.telegram_bots.model import TelegramCallbackRow, TelegramOutboxRow, TelegramPersonSettingsRow
from deerflow.utils.file_io import await_drained


def prompt_intent(item):
    value = item.payload.get("management_prompt_intent")
    if (
        type(value) is not dict
        or set(value) != {"version", "purpose", "prompt_id", "draft_id", "parent_id", "origin"} | ({"reopen"} if value.get("version") == 2 else set())
        or type(value["version"]) is not int
        or value["version"] not in {1, 2}
        or value["purpose"] != "work_edit"
    ):
        raise ProductSourceRetired()
    if any(type(value[key]) is not str or len(value[key]) != 32 or any(ch not in "0123456789abcdef" for ch in value[key]) for key in ("prompt_id", "draft_id", "parent_id")):
        raise ProductSourceRetired()
    if value["version"] == 2:
        from app.telegram_bots.assignment_prompt_reopen import require_reopen

        reopened = require_reopen(value["reopen"])
        if value["prompt_id"] != digest(["assignment-prompt-reopen", item.link_id, item.binding_generation, reopened["callback_id"]])[:32]:
            raise ProductSourceRetired()
    return value


def editor_from(intent):
    return {"version": 1, **{key: intent[key] for key in ("prompt_id", "draft_id", "origin")}}


class WorkManagementPromptReader(WorkViewReader):
    def __init__(self, acts):
        super().__init__(work=acts.work, authority=acts.authority, links=acts.links, clock=acts.outbox.clock)
        self.acts, self.config = acts, acts.outbox.config

    async def read(self, link, item, *, allow_closed=False):
        async def perform():
            intent = prompt_intent(item)
            async with self.links.session_factory() as reader:
                parent = await reader.get(TelegramOutboxRow, intent["parent_id"])
                if parent is None:
                    raise ProductSourceRetired()
                work_id = parent.work_id
            detail = await self.detail(link, work_id)
            projection = WorkManagementPromptProjection(
                self,
                link,
                item,
                detail=detail,
                opening=parent.payload["work_management_intent"],
                history=parent.payload.get("work_management_replacements", []),
                dedupe_key=parent.dedupe_key,
                clock=await self.prepare_clock(link, item),
                allow_closed=allow_closed,
            )
            current = await prepare_attention(self, link)
            await current(projection=projection)
            return projection.source

        return await await_drained(perform())


class WorkManagementPromptProjection:
    def __init__(self, reader, link, item, source=None, *, detail=None, opening=None, history=None, dedupe_key=None, clock=None, allow_closed=False):
        self.reader, self.link, self.item, self.expected, self.clock = reader, link, item, source, clock
        self.intent = prompt_intent(item)
        self.allow_closed, self.reopen = allow_closed, None
        if self.intent["version"] == 2:
            from app.telegram_bots.assignment_prompt_reopen import AssignmentReopenProof

            self.reopen = AssignmentReopenProof(link, self.intent["reopen"], self.intent, completed=item.id, sqlite=reader.links.session_factory.kw["bind"].dialect.name == "sqlite")
        detail = detail_read(source.work["detail"]) if source is not None else detail
        self.opening = source.work["opening"] if source is not None else opening
        self.history = source.work["history"] if source is not None else history
        self.dedupe_key = source.work["owner_key"] if source is not None else dedupe_key
        self.frame = ManagementEditFrame(reader, link, self.intent["parent_id"], editor_from(self.intent), detail, opening_intent=self.opening, topic=item.message_thread_id, history=self.history, dedupe_key=self.dedupe_key)

    def extend(self, query):
        result = self.frame.extend(query)
        self.delivery_column = len(result.column_descriptions)
        delivery, settings, prompt, cleanup = aliased(TelegramOutboxRow), aliased(TelegramPersonSettingsRow), aliased(TelegramCallbackRow), aliased(TelegramOutboxRow)
        identity = self.item.payload.get("management_prompt_cleanup_id")
        result = (
            result.add_columns(delivery, settings, prompt, cleanup)
            .join(delivery, delivery.id == self.item.id)
            .join(settings, settings.owner_user_id == self.link.owner_user_id)
            .outerjoin(prompt, prompt.id == self.intent["prompt_id"])
            .outerjoin(cleanup, cleanup.id == identity if identity else false())
        )
        return self.reopen.extend(result) if self.reopen is not None else result

    def check(self, rows):
        raise ProductSourceRetired()

    def check_with_permissions(self, rows, held):
        self.frame.check_with_permissions(rows, held)
        if self.reopen is not None:
            self.reopen.check(rows)
        delivery, settings, prompt = rows[0][self.delivery_column : self.delivery_column + 3]
        if (
            _item(delivery) != self.item
            or (delivery.link_id, delivery.owner_user_id, delivery.binding_generation, delivery.chat_id, delivery.instance_id, delivery.work_id, delivery.class_, delivery.request_id, delivery.event_id)
            != (self.link.id, self.link.owner_user_id, self.link.binding_generation, self.link.chat_id, self.link.agent_ref, self.frame.root.work_id, "reply", None, None)
            or delivery.message_thread_id != self.frame.parent.message_thread_id
            or delivery.dedupe_key != "notice:management-prompt:" + self.intent["prompt_id"]
            or settings.timezone != self.clock.settings.timezone
        ):
            raise ProductSourceRetired()
        if delivery.parts:
            from app.telegram_bots.assignment_typing import validate_prompt

            validate_prompt(self.item, prompt, attached=True)
            if not self.allow_closed and (prompt.consumed_at is not None or "prompt_successor" in prompt.payload):
                raise ProductSourceRetired()
        cleanup = rows[0][self.delivery_column + 3]
        target = self.reopen.selected.message_id if self.reopen is not None else rows[0][self.frame.column + 2].message_id
        if cleanup is None or cleanup.payload.get("target_message_id") != target:
            raise ProductSourceRetired()
        if not cleanup_ready(_item(delivery), cleanup, identity=delivery.payload.get("management_prompt_cleanup_id")) and delivery.state == "sending":
            raise DeliveryRetry(5)
        data = thaw({"intent": self.intent, "root": private_facts(self.frame.root), "detail": asdict(self.frame.detail.source), "opening": self.opening, "history": self.history, "owner_key": self.dedupe_key})
        if self.intent["version"] == 2:
            from app.telegram_bots.assignment_typing import prompt_preview

            data["preview"] = prompt_preview(self.link, self.frame.root, "work_edit", self.clock)
        self.source = WorkNewsRead(delivery.work_id, "management.prompt", {"name": rows[0][1].name}, self.frame.detail.source.protected, data, digest([self.link.id, self.link.binding_generation, self.intent]), digest(data))
        if self.expected is not None and not same_body(asdict(self.source), asdict(self.expected)):
            raise ProductSourceRetired()
