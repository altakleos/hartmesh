"""Human-input presentation and host composition of saved request controls."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from html import escape

from app.telegram_bots.human_input_source import decode_request
from app.telegram_bots.render import render, render_plain
from app.telegram_bots.split import split_html, visible_text

CLARIFY_CHOICES_MAX = 48
CHOICE_BUTTONS_MAX = 24
NUMBER_BUTTONS_PER_ROW = 8
BUTTON_CHOICE_CUT = 24
BUTTON_LABEL_MAX_SINGLE = 32

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HumanInputPresentation:
    parts: tuple[dict, ...]
    keyboard: tuple[tuple[dict, ...], ...]


def _presentation(html, keyboard, *, reply_to_message_id, force_reply=False):
    if type(reply_to_message_id) is not int or reply_to_message_id <= 0 or force_reply and keyboard:
        raise ValueError("invalid-human-input-presentation")
    chunks = split_html(html)
    if not chunks:
        raise ValueError("empty-human-input-presentation")
    return HumanInputPresentation(
        tuple({"kind": "text", "html": chunk, "visible_text": visible_text(chunk), "reply_to_message_id": reply_to_message_id, "force_reply": force_reply and index == len(chunks) - 1} for index, chunk in enumerate(chunks)),
        tuple(tuple(row) for row in keyboard),
    )


def _button(kind, thread_id, payload, *, label="label.choice", values=None):
    value = {"label": label, "kind": kind, "target_kind": "thread", "target_id": thread_id, "payload": payload}
    if values is not None:
        value["label_values"] = values
    return value


def _choice_label(label, index):
    return label if len(label) <= BUTTON_LABEL_MAX_SINGLE else f"{index}. {label[:BUTTON_CHOICE_CUT]}…"


def _choice_rows(options, *, thread_id, kind, payload, selected=(), cap=CLARIFY_CHOICES_MAX):
    chosen = options[:cap]
    numbered = len(options) > CHOICE_BUTTONS_MAX
    buttons = []
    for index, option in enumerate(chosen, 1):
        label = str(index) if numbered else _choice_label(option["label"], index)
        if option["value"] in selected:
            label = "✓ " + label
        buttons.append(_button(kind, thread_id, {**payload, "option_id": option["id"]}, values={"label": label}))
    width = NUMBER_BUTTONS_PER_ROW if numbered else 1
    return [buttons[index : index + width] for index in range(0, len(buttons), width)]


def present_request(raw, *, thread_id, reply_to_message_id, locale):
    """Render an already source-qualified request; this function grants no read."""
    try:
        request = decode_request(raw)
    except ValueError:
        return _presentation(render("error.generic", locale), (), reply_to_message_id=reply_to_message_id)
    if request["input_mode"] == "form":
        raise ValueError("form-state-required")
    if request["source"] == "sandbox_network":
        details = request["details"]
        host = details["host"] + (f":{details['port']}" if details["port"] not in (80, 443) else "")
        minutes = details["ttl_seconds"] // 60
        payload = {"thread_id": thread_id, "request_id": request["request_id"], "host": details["host"], "port": details["port"]}
        keyboard = [
            [_button("network", thread_id, {**payload, "option_id": "allow_temporary"}, label="label.allow_site", values={"minutes": minutes})],
            [_button("network", thread_id, {**payload, "option_id": "deny"}, label="label.keep_blocked")],
        ]
        return _presentation(render("net.ask_plain", locale, host=host, minutes=minutes), keyboard, reply_to_message_id=reply_to_message_id)
    body = request["question"]
    if request.get("context"):
        body += "\n\n" + request["context"]
    options = request.get("options") or []
    if not options:
        return _presentation(escape(body), (), reply_to_message_id=reply_to_message_id, force_reply=True)
    body += "\n\n" + "\n".join(f"{index}. {option['label']}" for index, option in enumerate(options, 1))
    if len(options) > CLARIFY_CHOICES_MAX:
        body += "\n\n" + render_plain("clarify.more_choices", locale)
    payload = {"thread_id": thread_id, "request_id": request["request_id"]}
    keyboard = _choice_rows(options, thread_id=thread_id, kind="clarify_option", payload=payload)
    for row in keyboard:
        for button in row:
            selected = next(option for option in options if option["id"] == button["payload"]["option_id"])
            button["payload"]["value"] = selected["value"]
    keyboard.append([_button("clarify_other", thread_id, payload, label="label.something_else")])
    return _presentation(escape(body), keyboard, reply_to_message_id=reply_to_message_id)


def _display_value(value, locale):
    if type(value) is bool:
        return render_plain("clarify.form_yes" if value else "clarify.form_no", locale)
    if isinstance(value, (list, tuple)):
        return ", ".join(value)
    return str(value)


def present_form(raw, form, *, thread_id, reply_to_message_id, locale, view="field", include_question=False):
    request = decode_request(raw)
    if request["input_mode"] != "form" or view not in {"field", "review", "change"}:
        raise ValueError("invalid-form-presentation")
    fields = request["fields"]
    payload = {"form_id": form.id, "revision": form.revision}
    if view in {"review", "change"}:
        pairs = []
        for field in fields:
            value = form.values.get(field["name"], False if field["type"] == "checkbox" else None)
            if value is None or type(value) is str and not value.strip() or isinstance(value, (list, tuple)) and not value:
                continue
            pairs.append((field["label"], _display_value(value, locale)))
        if pairs:
            label, first = pairs[0]
            value = first + "".join(f"\n{label}: {value}" for label, value in pairs[1:])
            html = render("clarify.form_review", locale, label=label, value=value)
        else:
            html = render("clarify.form_review", locale, label="", value="").split("\n", 1)[0]
        if view == "review":
            keyboard = [[_button("form_send", thread_id, payload, label="label.send"), _button("form_change", thread_id, payload, label="label.change")]]
        else:
            keyboard = [[_button("form_field", thread_id, {**payload, "field": field["name"]}, label="label.form_field", values={"label": field["label"] if len(field["label"]) <= 32 else field["label"][:31] + "…"})] for field in fields]
            keyboard.append([_button("back", thread_id, {**payload, "group_id": form.id}, label="label.back")])
        return _presentation(html, keyboard, reply_to_message_id=reply_to_message_id)
    if type(form.field_index) is not int or not 0 <= form.field_index < len(fields):
        raise ValueError("invalid-form-field")
    field = fields[form.field_index]
    payload = {**payload, "field": field["name"]}
    html = render("clarify.form_field", locale, n=form.field_index + 1, total=len(fields), label=field["label"])
    if include_question:
        html = escape(request["question"]) + "\n\n" + html
    kind = field["type"]
    keyboard = []
    if kind in {"select", "multi_select"}:
        options = field["options"]
        html += "\n\n" + "\n".join(escape(f"{index}. {option['label']}") for index, option in enumerate(options, 1))
        keyboard = _choice_rows(options, thread_id=thread_id, kind="form_option", payload=payload, selected=form.values.get(field["name"], ()) if kind == "multi_select" else ())
        if kind == "multi_select":
            keyboard.append([_button("form_next", thread_id, payload, values={"label": render_plain("clarify.form_done", locale)})])
    elif kind == "checkbox":
        keyboard = [[_button("form_option", thread_id, {**payload, "option_id": option}, values={"label": render_plain(key, locale)}) for option, key in (("yes", "clarify.form_yes"), ("no", "clarify.form_no"))]]
    if not field["required"]:
        keyboard.append([_button("form_skip", thread_id, payload, label="label.skip")])
    return _presentation(html, keyboard, reply_to_message_id=reply_to_message_id, force_reply=not keyboard)


class _InputGuard:
    def __init__(self, service, channel, link, *, retained=False, original=None):
        self.service, self.channel, self.captured_link = service, channel, link
        self.retained, self.actual, self.original = retained, None, original

    def current(self):
        from app.telegram_bots.callbacks import CallbackRefused

        if not self.service.current(self.channel, self.captured_link, retained=self.retained) or self.original is not None and self.original.current() is not True:
            raise CallbackRefused() from None

    async def prepare(self):
        self.current()
        if self.original is not None:
            from app.telegram_bots.links import prepare_publication_guard

            await prepare_publication_guard(self.original)
            self.current()
        host, link = self.service.services, self.captured_link
        owner = await host.prepare_live_owner(link.owner_user_id)
        self.current()
        self.actual = host.links.publication_guard(link, check_live_owner=owner, check_attempt_current=lambda: self.service.current(self.channel, link, retained=self.retained))

    async def check(self, session):
        self.current()
        if self.actual is None:
            raise ValueError("human-input-unprepared") from None
        result = await self.actual.check(session)
        self.current()
        if self.original is not None:
            result = await self.original.check(session)
            self.current()
        return result


class HumanInputs:
    """No worker: the existing turn, callback and sender owners call these leaves."""

    def __init__(self, services, *, source=None):
        from app.telegram_bots.human_input_source import HumanInputSource

        self.services = services
        self.source = source or HumanInputSource(coordinator=services.publications, http_owner=services.thread_client, capture_attempt=services.publication_authority.capture)

    def current(self, channel, link, *, retained=False):
        if self.services.channel(link.id) is not channel or not channel._same_scope(link, channel.link) or channel.transport_revision != link.token_revision or not channel.application.running or channel._stopping:
            return False
        if retained:
            allowed = getattr(self.services, "publication_read_allowed", None)
            return allowed is None or allowed() is True
        return channel._callback_current(link)

    async def _source_for(self, link, item):
        from app.telegram_bots.human_input_source import validate_human_input_reply_basis

        checked = validate_human_input_reply_basis(item)
        source = await self.source.read_request(checked.record, link)
        if source is None or source.basis != item.payload["reply_basis"]["outcome"]["request_basis"]:
            raise ValueError("human-input-source-conflict") from None
        return source

    async def plan_terminal(self, record, link, *, guard=None):
        from deerflow.utils.file_io import await_drained

        return await await_drained(self._plan_terminal(record, link, original=guard))

    async def _plan_terminal(self, record, link, *, original=None):
        from hashlib import sha256

        from sqlalchemy import select

        from deerflow.persistence.telegram_bots.model import TelegramOutboxRow

        source = await self.source.read_request(record, link, guard=original) if original is not None else await self.source.read_request(record, link)
        if source is None:
            return None
        channel = self.services.channel(link.id)
        if channel is None or not self.current(channel, link, retained=True):
            return None
        guard = _InputGuard(self, channel, link, retained=True, original=original)
        guard.current()
        settings = await self.services.settings.get(link.owner_user_id)
        guard.current()
        if settings is None:
            raise ValueError("human-input-settings-missing") from None
        record = source.record
        anchor = record.origins[0].message_id
        try:
            request = decode_request(source.request)
        except ValueError:
            request = None
            source_name = source.request.get("source")
            classification = source_name if source_name in ("ask_clarification", "sandbox_network") else "unsupported"
            logger.warning("telegram_bots human_input_invalid source=%s link=%s", classification, link.id)
        file_plan = None
        if source.basis["version"] == 2:
            import asyncio

            from app.telegram_bots.mixed_source import prepare_mixed_files
            from app.telegram_bots.pages import page_address
            from deerflow.utils.file_io import await_drained

            guard.current()
            conversation_url = await await_drained(asyncio.to_thread(page_address, "web.conversation", link, thread_id=record.scope.thread_id))
            guard.current()
            file_plan = await prepare_mixed_files(
                record,
                source,
                http_owner=self.services.thread_client,
                guard=guard,
                capacity=self.services.file_bytes_budget.capacity,
                max_bytes=self.services.config.files.max_outbound_bytes,
                conversation_url=conversation_url,
            )
            guard.current()
        async with self.services.callbacks.writer(link, guard) as writer:
            # A lost result may be retried after the person already advanced a
            # form. The original primary wins before any UI state is rebuilt.
            identity = await writer.scalar(select(TelegramOutboxRow.id).where(TelegramOutboxRow.link_id == link.id, TelegramOutboxRow.dedupe_key == "reply:" + record.turn_id))
            if identity is not None:
                item = await self.services.outbox.get(link, identity, session=writer)
                if item is None:
                    raise ValueError("human-input-source-conflict") from None
                from app.telegram_bots.human_input_source import validate_human_input_reply_basis

                checked = validate_human_input_reply_basis(item)
                if checked.record.publication_id != record.publication_id or item.payload["reply_basis"]["outcome"]["request_basis"] != source.basis:
                    raise ValueError("human-input-source-conflict") from None
                return identity
            form = None
            if request is not None and request["input_mode"] == "form":
                form_id = sha256((record.publication_id + ":" + request["request_id"]).encode()).hexdigest()[:32]
                form = await self.services.forms.create(link, form_id=form_id, thread_id=record.scope.thread_id, request=request, guard=guard, session=writer, message_thread_id=record.scope.message_thread_id)
                view = present_form(request, form, thread_id=record.scope.thread_id, reply_to_message_id=anchor, locale=settings.locale, include_question=True)
            else:
                view = present_request(source.request, thread_id=record.scope.thread_id, reply_to_message_id=anchor, locale=settings.locale)
            extra = {"file_plan": file_plan} if file_plan is not None else {}
            identity = await self.services.outbox.plan_human_input_reply(record, source, parts=list(view.parts), keyboard=[list(row) for row in view.keyboard], callbacks=self.services.callbacks, guard=guard, session=writer, **extra)
            if form is not None:
                await self._field_prompt(link, source, form, identity, guard=guard, writer=writer)
            return identity

    async def _field_prompt(self, link, source, form, outbox_id, *, guard, writer, prompt_suffix=""):
        from hashlib import sha256

        if form.state != "field":
            return
        field = form.fields[form.field_index]
        if field["type"] not in {"text", "textarea", "number", "date"}:
            return
        prompt_id = sha256(f"{form.id}:{form.revision}:prompt:{prompt_suffix}".encode()).hexdigest()[:32]
        identity = await self.services.prompts.open(
            link,
            "form_field",
            {"form_id": form.id, "field": field["name"], "revision": form.revision, "thread_id": form.thread_id},
            chat_id=link.chat_id,
            message_thread_id=source.record.scope.message_thread_id,
            message_id=None,
            prompt_id=prompt_id,
            guard=guard,
            session=writer,
            defer_visibility=source.basis["version"] == 2,
        )
        await self.services.prompts.attach(link, identity, outbox_id=outbox_id, guard=guard, session=writer)

    async def _callback_source(self, channel, row):
        from copy import deepcopy

        from app.telegram_bots.callbacks import CallbackRefused

        link = deepcopy(channel.link)
        if not self.current(channel, link):
            raise CallbackRefused() from None
        identity = row.payload.get("anchor_outbox_id")
        item = await self.services.outbox.get(link, identity)
        if item is None:
            raise CallbackRefused() from None
        source = await self._source_for(link, item)
        return link, item, source, _InputGuard(self, channel, link)

    async def execute_callback(self, channel, row, *, update_id, query_id=None):
        from deerflow.utils.file_io import await_drained

        return await await_drained(self._execute_callback(channel, row, update_id=update_id))

    async def _execute_callback(self, channel, row, *, update_id):
        from app.telegram_bots.callbacks import CallbackRefused

        if row.kind.startswith("prompt_"):
            return await self._closed_choice(channel, row, update_id=update_id)
        link, item, source, guard = await self._callback_source(channel, row)
        request = decode_request(source.request)
        if row.kind in {"clarify_option", "network", "form_send"} and "publication_owner" in row.payload:
            saved = await self.services.outbox.inbox.read_callback_input(link, row.id, guard=guard)
            response = saved.human_message_kwargs.get("human_input_response")
            if response is None:
                raise CallbackRefused() from None
            stage = await self.services.outbox.inbox.stage_submission(
                link,
                source_outbox_id=item.id,
                response=response,
                callback_id=row.id,
                update_id=update_id,
                chat_id=row.chat_id,
                message_id=row.message_id,
                message_thread_id=row.message_thread_id,
                callbacks=self.services.callbacks,
                forms=self.services.forms,
                guard=guard,
            )
            guard.current()
            await self.services.turns.accept_staged(channel, stage)
            return
        settings = await self.services.settings.get(link.owner_user_id)
        stage = None
        async with self.services.callbacks.writer(link, guard) as writer:
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            row = group.selected
            if group.anchor_outbox_id != item.id or row.target_kind != "thread" or row.target_id != source.record.scope.thread_id:
                raise CallbackRefused() from None
            if any("publication_owner" in member.payload for member in group.rows):
                raise CallbackRefused() from None
            if row.kind == "clarify_other":
                if request["source"] != "ask_clarification" or not request.get("options"):
                    raise CallbackRefused() from None
                await self.services.callbacks.consume_group(group, session=writer)
                await self.services.prompts.close(link, row.chat_id, row.message_thread_id, reason="navigation", guard=guard, session=writer)
                view = _presentation(escape(request["question"]) + "\n\n" + render("clarify.other_prompt", settings.locale), (), reply_to_message_id=source.record.origins[0].message_id, force_reply=True)
                identity = await self._view(link, item, source, view, step_id="other:" + row.id, guard=guard, writer=writer)
                await self._other_prompt(link, source, identity, guard=guard, writer=writer, prompt_id=row.id)
            elif row.kind in {"form_option", "form_next", "form_skip", "form_change", "form_field", "back"}:
                await self._form_callback(link, item, source, group, update_id=update_id, guard=guard, writer=writer, locale=settings.locale)
            elif row.kind == "form_send":
                submitted = await self.services.forms.submission(link, row.id, guard=guard, session=writer)
                stage = await self.services.outbox.inbox.stage_submission(
                    link,
                    source_outbox_id=item.id,
                    response=dict(submitted.response),
                    callback_id=row.id,
                    update_id=update_id,
                    chat_id=row.chat_id,
                    message_id=row.message_id,
                    message_thread_id=row.message_thread_id,
                    callbacks=self.services.callbacks,
                    forms=self.services.forms,
                    guard=guard,
                    session=writer,
                )
                view = present_form(source.request, submitted.form, thread_id=source.record.scope.thread_id, reply_to_message_id=source.record.origins[0].message_id, locale=settings.locale, view="review")
                view = HumanInputPresentation(view.parts, ())
                await self._view(link, item, source, view, step_id="sent:" + row.id, guard=guard, writer=writer)
            elif row.kind in {"clarify_option", "network"}:
                option = next((option for option in request.get("options", ()) if option["id"] == row.payload.get("option_id")), None)
                if option is None:
                    raise CallbackRefused() from None
                response = {"version": 1, "kind": "human_input_response", "source": request["source"], "request_id": request["request_id"], "response_kind": "option", "option_id": option["id"], "value": option["value"]}
                if row.kind == "network":
                    await self.services.acts.write_network(link, callback_id=row.id, body={key: row.payload[key] for key in ("thread_id", "request_id", "option_id", "host", "port")}, guard=guard, session=writer)
                stage = await self.services.outbox.inbox.stage_submission(
                    link,
                    source_outbox_id=item.id,
                    response=response,
                    callback_id=row.id,
                    update_id=update_id,
                    chat_id=row.chat_id,
                    message_id=row.message_id,
                    message_thread_id=row.message_thread_id,
                    callbacks=self.services.callbacks,
                    guard=guard,
                    session=writer,
                )
                if row.kind == "network":
                    details = request["details"]
                    host = details["host"] + (f":{details['port']}" if details["port"] not in (80, 443) else "")
                    key = "net.allowed" if option["id"] == "allow_temporary" else "net.kept"
                    values = {"host": host, "minutes": details["ttl_seconds"] // 60} if option["id"] == "allow_temporary" else {"host": host}
                else:
                    key, values = "clarify.answer_line", {"label": option["label"]}
                await self._receipt(link, item, source, key, values, step_id="answer:" + row.id, guard=guard, writer=writer, locale=settings.locale)
            else:
                raise CallbackRefused() from None
        guard.current()
        if stage is not None:
            await self.services.turns.accept_staged(channel, stage)

    async def _other_prompt(self, link, source, outbox_id, *, guard, writer, prompt_id):
        from hashlib import sha256

        request = decode_request(source.request)
        identity = sha256(("other:" + prompt_id).encode()).hexdigest()[:32]
        await self.services.prompts.open(
            link,
            "clarify_other",
            {"thread_id": source.record.scope.thread_id, "request_id": request["request_id"]},
            chat_id=link.chat_id,
            message_thread_id=source.record.scope.message_thread_id,
            message_id=None,
            prompt_id=identity,
            guard=guard,
            session=writer,
            defer_visibility=source.basis["version"] == 2,
        )
        await self.services.prompts.attach(link, identity, outbox_id=outbox_id, guard=guard, session=writer)
        return identity

    async def _view(self, link, item, source, view, *, step_id, guard, writer, fallback_message_id=None):
        from app.telegram_bots.mixed_source import human_input_parts

        parts = [dict(part) for part in view.parts]
        old = [part for part in human_input_parts(item) if part.get("state") == "sent" and part.get("message_ids")]
        # Edit complete canonical parts only when each has a proven actual ACK.
        # A changed split sends a complete replacement, never a clipped preview.
        if not any(part.get("force_reply") for part in parts) and len(old) == len(parts):
            for index, (part, previous) in enumerate(zip(parts, old)):
                target = fallback_message_id if index == len(parts) - 1 and fallback_message_id is not None else previous["message_ids"][-1]
                part.update(kind="edit", target_message_id=target, target_protect_content=bool(previous.get("sent_protect_content", False)))
        elif not any(part.get("force_reply") for part in parts) and len(parts) == 1 and not old and fallback_message_id is not None:
            parts[0].update(kind="edit", target_message_id=fallback_message_id, target_protect_content=False)
        if any(part.get("force_reply") for part in parts) and item.payload["reply_basis"].get("keyboard") and old:
            cleared = [
                {
                    "kind": "edit",
                    "html": part["html"],
                    "visible_text": part["visible_text"],
                    "reply_to_message_id": source.record.origins[0].message_id,
                    "force_reply": False,
                    "remove_keyboard": True,
                    "target_message_id": old[-1]["message_ids"][-1],
                    "target_protect_content": bool(old[-1].get("sent_protect_content", False)),
                }
                for part in [item.payload["reply_basis"]["parts"][-1]]
            ]
            await self.services.outbox.plan_human_input_step(source.record, source, step_id="clear:" + step_id, parts=cleared, keyboard=[], callbacks=self.services.callbacks, guard=guard, session=writer)
        return await self.services.outbox.plan_human_input_step(source.record, source, step_id=step_id, parts=parts, keyboard=[list(row) for row in view.keyboard], callbacks=self.services.callbacks, guard=guard, session=writer)

    async def _receipt(self, link, item, source, key, values, *, step_id, guard, writer, locale):
        original = present_request(source.request, thread_id=source.record.scope.thread_id, reply_to_message_id=source.record.origins[0].message_id, locale=locale)
        html = "".join(part["html"] for part in original.parts) + "\n\n" + render(key, locale, **values)
        view = _presentation(html, (), reply_to_message_id=source.record.origins[0].message_id)
        return await self._view(link, item, source, view, step_id=step_id, guard=guard, writer=writer)

    async def _form_callback(self, link, item, source, group, *, update_id, guard, writer, locale):
        from app.telegram_bots.callbacks import CallbackRefused

        row = group.selected
        form = await self.services.forms.read(link, row.payload.get("form_id"), guard=guard, session=writer)
        if form.revision != row.payload.get("revision") or form.request["request_id"] != decode_request(source.request)["request_id"]:
            raise CallbackRefused() from None
        view = "field"
        kind = row.kind
        if kind in {"form_change", "back"}:
            if form.state != "review":
                raise CallbackRefused() from None
            view = "change" if kind == "form_change" else "review"
        else:
            field = row.payload.get("field")
            action, value = {"form_next": ("next", None), "form_skip": ("skip", None), "form_field": ("edit", None)}.get(kind, ("answer", row.payload.get("option_id")))
            if kind == "form_option":
                definition = form.fields[form.field_index]
                if definition["type"] == "multi_select":
                    action = "toggle"
                elif definition["type"] == "checkbox":
                    if value not in {"yes", "no"}:
                        raise CallbackRefused() from None
                    value = value == "yes"
            form = await self.services.forms.transition(link, form.id, expected_revision=form.revision, update_id=update_id, action=action, field=field, value=value, guard=guard, session=writer)
            view = "review" if form.state == "review" else "field"
        await self.services.callbacks.consume_group(group, session=writer)
        await self.services.prompts.close(link, row.chat_id, row.message_thread_id, reason="navigation", guard=guard, session=writer)
        presentation = present_form(source.request, form, thread_id=form.thread_id, reply_to_message_id=source.record.origins[0].message_id, locale=locale, view=view)
        identity = await self._view(link, item, source, presentation, step_id=f"form:{form.id}:{form.revision}:{view}:{row.id}", guard=guard, writer=writer, fallback_message_id=row.message_id)
        if view == "field":
            await self._field_prompt(link, source, form, identity, guard=guard, writer=writer)

    async def handle_text(self, channel, update, *, captured_link=None):
        from deerflow.utils.file_io import await_drained

        return await await_drained(self._handle_text(channel, update, captured_link=captured_link))

    async def _handle_text(self, channel, update, *, captured_link=None):
        from copy import deepcopy

        from app.telegram_bots.callbacks import normalize_callback_topic
        from app.telegram_bots.inbox import InboxRow
        from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

        message = update.message
        link = deepcopy(captured_link or channel.link)
        if message is None or type(message.text) is not str or not message.text.strip() or not self.current(channel, link):
            return False
        if message.forward_origin is not None or message.is_automatic_forward is True or any(message.api_kwargs.get(key) is not None for key in ("forward_from", "forward_from_chat", "forward_sender_name", "forward_date")):
            return False
        guard = _InputGuard(self, channel, link)
        async with self.services.callbacks.writer(link, guard) as writer:
            mark = await writer.get(TelegramInboundMarkRow, (link.id, message.message_id, message.chat_id))
            existing = InboxRow.from_row(mark) if mark is not None else None
        if existing is not None:
            if "submission" in existing.route_basis:
                await self.services.turns.accept_staged(channel, self.services.outbox.inbox.typed_submission_stage(existing))
                return True
            if existing.route_complete or existing.route_basis.get("human_input_ui") is not None:
                return True
            # An already admitted ordinary input remains ordinary even if a
            # different prompt appeared while it waited in the FIFO queue.
            if existing.route_kind is not None and not self.services.turns.unfinished(link, existing):
                return False
        topic = normalize_callback_topic(message.message_thread_id)
        quoted = message.reply_to_message.message_id if message.reply_to_message else None
        target = await self._text_target(link, guard, chat_id=message.chat_id, topic=topic, quoted=quoted, sent_at=None if existing is None else existing.accepted_at)
        if target is None:
            return False
        origin, _ = await self.services.outbox.inbox.accept(link, update, sanitized_input={"type": "text", "text": message.text})
        origin = await self.services.outbox.inbox.route(origin, kind="turn", target_id=link.agent_ref, basis={"input_profile": {"version": 1, "kind": "plain_text", "forwarded": False}}, operation_ids=[])
        return await self._apply_text(channel, link, guard, origin, target)

    async def _text_target(self, link, guard, *, chat_id, topic, quoted, sent_at=None):
        """What this text answers as (prompt, retired, rows); None leaves it ordinary.

        ``sent_at`` is set for text that arrives late. A prompt that appeared
        after the person spoke was not what they were answering.
        """
        prompt, rows, retired = None, (), False
        if quoted is not None:
            route = await self.services.callbacks.for_message(link.id, quoted, chat_id=chat_id, message_thread_id=topic, binding_generation=link.binding_generation)
            if route is not None:
                # Delivery-only warnings do not own a human-input answer, even
                # after their Send again control has been consumed.
                if all(row.kind == "file_retry" for row in route.rows):
                    return None
                rows, retired = route.rows, route.retired_presentation
                prompt = next((row for row in rows if row.kind == "prompt"), None)
                if prompt is None:
                    return (None, True, rows) if retired else None  # current quoted cards remain ordinary FIFO input
        else:
            prompt = await self.services.prompts.current(link, chat_id, topic, guard=guard)
            if prompt is not None and sent_at is not None and prompt.created_at > sent_at:
                return None
        if prompt is None or not self.services.prompts.is_visible(prompt) or prompt.consumed_at is None and prompt.payload.get("purpose") not in {"form_field", "clarify_other"}:
            return None
        return prompt, retired, rows

    async def _transcript_target(self, link, row, guard):
        return await self._text_target(link, guard, chat_id=row.chat_id, topic=row.message_thread_id, quoted=row.sanitized_input.get("reply_to_message_id"), sent_at=row.accepted_at)

    async def answers_prompt(self, channel, link, row):
        """Whether a transcript of this note would answer a prompt, for its audit."""
        target = None if row.route_basis.get("forwarded") else await self._transcript_target(link, row, _InputGuard(self, channel, link))
        return target is not None and target[0] is not None and not target[1] and target[0].consumed_at is None

    async def handle_transcript(self, channel, link, row):
        """A finished voice note answers exactly as the same typed text would."""
        from deerflow.utils.file_io import await_drained

        if row.route_basis["input_profile"]["forwarded"] or not self.current(channel, link):
            return False
        guard = _InputGuard(self, channel, link)
        target = await await_drained(self._transcript_target(link, row, guard))
        return False if target is None else await await_drained(self._apply_text(channel, link, guard, row, target))

    async def _apply_text(self, channel, link, guard, origin, target):
        from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

        prompt, retired, rows = target
        text, topic = origin.sanitized_input["text"], origin.message_thread_id
        if prompt is None:
            await self._retired_card_notice(link, origin, rows, guard=guard)
            return True
        if retired or prompt.consumed_at is not None:
            await self._closed_notice(link, origin, prompt, guard=guard)
            return True
        item = await self.services.outbox.get(link, prompt.payload.get("anchor_outbox_id"))
        if item is None:
            await self._closed_notice(link, origin, prompt, guard=guard)
            return True
        source = await self._source_for(link, item)
        settings = await self.services.settings.get(link.owner_user_id)
        stage = None
        async with self.services.callbacks.writer(link, guard) as writer:
            fresh = await self.services.prompts.get(link, prompt.id, guard=guard, session=writer)
            if fresh.consumed_at is not None:
                await self._closed_notice(link, origin, fresh, guard=guard, writer=writer)
                return True
            if fresh.payload["purpose"] == "clarify_other":
                request = decode_request(source.request)
                response = {"version": 1, "kind": "human_input_response", "source": request["source"], "request_id": request["request_id"], "response_kind": "text", "value": text}
                stage = await self.services.outbox.inbox.stage_submission(link, source_outbox_id=item.id, response=response, origin=origin, prompt_id=fresh.id, callbacks=self.services.callbacks, guard=guard, session=writer)
                await self._receipt(link, item, source, "clarify.answer_line", {"label": text}, step_id="typed:" + str(origin.message_id), guard=guard, writer=writer, locale=settings.locale)
            elif fresh.payload["purpose"] == "form_field":
                form = await self.services.forms.read(link, fresh.payload["form_id"], guard=guard, session=writer)
                form = await self.services.forms.transition(link, form.id, expected_revision=fresh.payload["revision"], update_id=origin.update_id, action="answer", field=fresh.payload["field"], value=text, guard=guard, session=writer)
                await self.services.prompts.close(link, origin.chat_id, topic, reason="answered", guard=guard, session=writer)
                view = "review" if form.state == "review" else "field"
                presentation = present_form(source.request, form, thread_id=form.thread_id, reply_to_message_id=source.record.origins[0].message_id, locale=settings.locale, view=view)
                identity = await self._view(link, item, source, presentation, step_id=f"form:{form.id}:{form.revision}:{view}", guard=guard, writer=writer)
                mark = await writer.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
                mark.route_basis = {**mark.route_basis, "human_input_ui": {"form_id": form.id, "revision": form.revision}}
                await writer.flush()
                await self.services.outbox.inbox.register_delivery([origin], outbox_id=identity, primary=True, finish=True, session=writer)
                await self._field_prompt(link, source, form, identity, guard=guard, writer=writer)
            else:
                return False
        if stage is not None:
            guard.current()
            await self.services.turns.accept_staged(channel, stage)
        return True

    async def _retired_card_notice(self, link, origin, rows, *, guard):
        from hashlib import sha256

        selected = next((row for row in rows if type(row.payload.get("anchor_outbox_id")) is str), None)
        if selected is None:
            raise ValueError("unresolved-quoted-presentation") from None
        item = await self.services.outbox.get(link, selected.payload["anchor_outbox_id"])
        if item is None:
            raise ValueError("unresolved-quoted-presentation") from None
        purpose, payload = "retired_presentation", {}
        if item.payload.get("reply_basis", {}).get("version") in {5, 7} and any(row.consumed_at is None for row in rows):
            source = await self._source_for(link, item)
            request = decode_request(source.request)
            payload = {"thread_id": source.record.scope.thread_id}
            if request["input_mode"] == "form":
                field = next((row for row in rows if "form_id" in row.payload), None)
                if field is not None:
                    form = await self.services.forms.read(link, field.payload["form_id"], guard=guard)
                    if form.state == "field":
                        purpose = "form_field"
                        payload.update(form_id=form.id, field=form.fields[form.field_index]["name"], revision=form.revision)
            else:
                purpose = "request_card"
        identity = sha256(f"retired:{selected.group_id}:{origin.message_id}".encode()).hexdigest()[:32]
        async with self.services.callbacks.writer(link, guard) as writer:
            await self.services.prompts.open(
                link,
                purpose,
                payload,
                chat_id=link.chat_id,
                message_thread_id=origin.message_thread_id,
                message_id=None,
                prompt_id=identity,
                guard=guard,
                session=writer,
                defer_visibility=item.payload.get("reply_basis", {}).get("outcome", {}).get("request_basis", {}).get("version") == 2,
            )
            await self.services.prompts.attach(link, identity, outbox_id=item.id, guard=guard, session=writer)
            await self.services.prompts.close(link, link.chat_id, origin.message_thread_id, reason="replaced", guard=guard, session=writer)
            prompt = await self.services.prompts.get(link, identity, guard=guard, session=writer)
            await self._closed_notice(link, origin, prompt, guard=guard, writer=writer)

    async def _closed_notice(self, link, origin, prompt, *, guard, writer=None, unavailable=False):
        from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

        async def save(session):
            payload = {"prompt_id": prompt.id, "message_id": origin.message_id}
            kinds = [("prompt_send_new", "label.send_new"), ("prompt_discard", "label.discard")]
            if not unavailable:
                kinds.insert(0, ("prompt_reopen", "label.reopen_answer"))
            keyboard = [[_button(kind, prompt.target_id or link.agent_ref, payload, label=label)] for kind, label in kinds]
            identity = await self.services.outbox.plan_product_keyboard(
                link,
                source_id=f"prompt-closed:{prompt.id}:{origin.message_id}:{int(unavailable)}",
                parts=[{"kind": "text", "source_ref": {"key": "prompt.unavailable" if unavailable else "prompt.closed", "values": {}}, "reply_to_message_id": origin.message_id}],
                keyboard=keyboard,
                callbacks=self.services.callbacks,
                guard=guard,
                inbox_rows=[origin],
                finish_route=False,
                message_thread_id=origin.message_thread_id,
                session=session,
            )
            mark = await session.get(TelegramInboundMarkRow, (origin.link_id, origin.message_id, origin.chat_id))
            mark.route_basis = {**mark.route_basis, "human_input_ui": {"held_prompt_id": prompt.id}}
            await session.flush()
            return identity

        if writer is not None:
            return await save(writer)
        async with self.services.callbacks.writer(link, guard) as session:
            return await save(session)

    async def _closed_choice(self, channel, row, *, update_id):
        from copy import deepcopy

        from app.telegram_bots.callbacks import CallbackRefused
        from app.telegram_bots.inbox import InboxRow
        from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

        link = deepcopy(channel.link)
        guard = _InputGuard(self, channel, link)
        # Read the request before entering the writer; source admission itself
        # still participates in each private AUX plan's final writer guard.
        prompt = await self.services.prompts.get(link, row.payload.get("prompt_id"), guard=guard)
        source = item = None
        if row.kind == "prompt_reopen":
            item = await self.services.outbox.get(link, prompt.payload.get("anchor_outbox_id"))
            if item is not None and prompt.payload["purpose"] != "retired_presentation":
                from app.channels.execution import ChannelExecutionProtocolError

                try:
                    source = await self._source_for(link, item)
                except (ValueError, ChannelExecutionProtocolError):
                    source = None
        settings = await self.services.settings.get(link.owner_user_id)
        stage = None
        async with self.services.callbacks.writer(link, guard) as writer:
            group = await self.services.callbacks.lock_current(link, row.id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, guard=guard, session=writer)
            row = group.selected
            if row.payload.get("prompt_id") != prompt.id:
                raise CallbackRefused() from None
            mark = await writer.get(TelegramInboundMarkRow, (link.id, row.payload.get("message_id"), link.chat_id), with_for_update=True, populate_existing=True)
            if mark is None or mark.message_thread_id != row.message_thread_id or mark.binding_generation != link.binding_generation or mark.route_basis.get("human_input_ui", {}).get("held_prompt_id") != prompt.id:
                raise CallbackRefused() from None
            origin = InboxRow.from_row(mark)
            pending = [member for member in group.rows if "publication_owner" in member.payload]
            if pending and (row.kind != "prompt_send_new" or not any(member.id == row.id for member in pending)):
                raise CallbackRefused() from None
            if row.kind == "prompt_send_new":
                stage = await self.services.outbox.inbox.stage_prompt_message(
                    link, callback_id=row.id, update_id=update_id, chat_id=row.chat_id, message_id=row.message_id, message_thread_id=row.message_thread_id, callbacks=self.services.callbacks, guard=guard, session=writer
                )
                await self.services.prompts.close(link, link.chat_id, row.message_thread_id, reason="navigation", guard=guard, session=writer)
            elif row.kind == "prompt_discard":
                await self.services.prompts.close(link, link.chat_id, row.message_thread_id, reason="discarded", guard=guard, session=writer)
                await self.services.callbacks.consume_group(group, session=writer)
                await self.services.outbox.plan_product(
                    link,
                    source_id="prompt-discard:" + row.id,
                    parts=[{"kind": "text", "source_ref": {"key": "prompt.discarded", "values": {}}, "reply_to_message_id": origin.message_id}],
                    inbox_rows=[origin],
                    finish_route=True,
                    message_thread_id=row.message_thread_id,
                    session=writer,
                )
            elif row.kind == "prompt_reopen":
                if source is None:
                    await self.services.callbacks.consume_group(group, session=writer)
                    await self._closed_notice(link, origin, prompt, guard=guard, writer=writer, unavailable=True)
                    return
                if prompt.payload["purpose"] == "form_field":
                    form = await self.services.forms.read(link, prompt.payload["form_id"], guard=guard, session=writer)
                    if form.state in {"submitted", "cancelled"}:
                        await self.services.callbacks.consume_group(group, session=writer)
                        await self._closed_notice(link, origin, prompt, guard=guard, writer=writer, unavailable=True)
                        return
                    view = "review" if form.state == "review" else "field"
                    presentation = present_form(source.request, form, thread_id=form.thread_id, reply_to_message_id=source.record.origins[0].message_id, locale=settings.locale, view=view)
                elif prompt.payload["purpose"] == "clarify_other":
                    request = decode_request(source.request)
                    presentation = _presentation(escape(request["question"]) + "\n\n" + render("clarify.other_prompt", settings.locale), (), reply_to_message_id=source.record.origins[0].message_id, force_reply=True)
                    form = None
                elif prompt.payload["purpose"] == "request_card":
                    presentation = present_request(source.request, thread_id=source.record.scope.thread_id, reply_to_message_id=source.record.origins[0].message_id, locale=settings.locale)
                    form = None
                else:
                    raise CallbackRefused() from None
                await self.services.callbacks.consume_group(group, session=writer)
                await self.services.prompts.close(link, link.chat_id, row.message_thread_id, reason="navigation", guard=guard, session=writer)
                identity = await self._view(link, item, source, presentation, step_id="reopen:" + row.id, guard=guard, writer=writer)
                if form is not None:
                    await self._field_prompt(link, source, form, identity, guard=guard, writer=writer, prompt_suffix=row.id)
                elif prompt.payload["purpose"] == "clarify_other":
                    await self._other_prompt(link, source, identity, guard=guard, writer=writer, prompt_id=row.id)
                # Reopening intentionally retires the held answer. A new human
                # gesture answers the current request; saved old text is not used.
                await self.services.outbox.inbox.register_delivery([origin], outbox_id=identity, primary=True, finish=True, session=writer)
            else:
                raise CallbackRefused() from None
        if stage is not None:
            guard.current()
            await self.services.turns.accept_staged(channel, stage)

    async def before_plain_publication(self, channel, link, origins):
        from deerflow.utils.file_io import await_drained

        if not origins:
            return
        return await await_drained(self._close_scope(channel, link, origins[0].message_thread_id, update_id=max(row.update_id for row in origins), reason="navigation"))

    async def command(self, channel, update, word, *, captured_link=None):
        from deerflow.utils.file_io import await_drained

        return await await_drained(self._command(channel, update, word, captured_link=captured_link))

    async def _command(self, channel, update, word, *, captured_link=None):
        from copy import deepcopy

        from app.telegram_bots.callbacks import CallbackRefused

        link = deepcopy(captured_link or channel.link)
        guard = _InputGuard(self, channel, link)
        guard.current()
        origin, _created = await self.services.outbox.inbox.accept(link, update, sanitized_input={"command": word})
        guard.current()
        if origin.binding_generation != link.binding_generation or origin.sanitized_input.get("command") != word or set(origin.sanitized_input) - {"command", "reply_to_message_id"}:
            raise CallbackRefused() from None
        if origin.route_complete or origin.route_basis.get("human_input_ui") == {"command_closed": word}:
            return
        if origin.route_kind is not None and (origin.route_kind, origin.route_target_id) != ("command", word):
            raise CallbackRefused() from None
        return await self._close_scope(channel, link, origin.message_thread_id, update_id=origin.update_id, reason="command", command_origin=origin, command_word=word)

    async def _close_scope(self, channel, link, topic, *, update_id, reason, command_origin=None, command_word=None):
        from app.channels.execution import ChannelExecutionProtocolError
        from app.telegram_bots.callbacks import CallbackRefused, normalize_callback_topic
        from deerflow.persistence.telegram_bots.model import TelegramInboundMarkRow

        topic = normalize_callback_topic(topic)
        guard = _InputGuard(self, channel, link)
        guard.current()
        items = await self.services.outbox.list_human_inputs(link, topic=topic)
        guard.current()
        settings = await self.services.settings.get(link.owner_user_id) if items else None
        guard.current()
        sources = []
        for item in items:
            guard.current()
            try:
                source = await self._source_for(link, item)
            except (ValueError, ChannelExecutionProtocolError):
                # Metadata cancellation needs no old private-read privilege.
                # Without a fresh proof no private cleanup part is planned.
                continue
            guard.current()
            sources.append((item, source))
        async with self.services.callbacks.writer(link, guard) as writer:
            command_mark = None
            if command_origin is not None:
                command_mark = await writer.get(TelegramInboundMarkRow, (command_origin.link_id, command_origin.message_id, command_origin.chat_id), with_for_update=True, populate_existing=True)
                if (
                    command_mark is None
                    or (command_mark.link_id, command_mark.chat_id, command_mark.message_thread_id, command_mark.binding_generation, command_mark.update_id) != (link.id, link.chat_id, topic, link.binding_generation, update_id)
                    or command_mark.sanitized_input != command_origin.sanitized_input
                    or command_mark.sanitized_input.get("command") != command_word
                    or command_mark.route_kind is not None
                    and (command_mark.route_kind, command_mark.route_target_id) != ("command", command_word)
                ):
                    raise CallbackRefused() from None
                if command_mark.route_complete or command_mark.route_basis.get("human_input_ui") == {"command_closed": command_word}:
                    return
                if "human_input_ui" in command_mark.route_basis:
                    raise CallbackRefused() from None
            forms = await self.services.callbacks.rows_in_scope(link, kinds={"form"}, message_thread_id=topic, guard=guard, session=writer)
            for row in forms:
                form = await self.services.forms.read(link, row.id, guard=guard, session=writer)
                if form.state not in {"submitted", "cancelled"}:
                    await self.services.forms.transition(link, form.id, expected_revision=form.revision, update_id=update_id, action="cancel", guard=guard, session=writer)
            form_ids = {row.id for row in forms}
            backs = await self.services.callbacks.rows_in_scope(link, kinds={"back"}, message_thread_id=topic, guard=guard, session=writer)
            for row in backs:
                if row.payload.get("form_id") in form_ids:
                    await self.services.callbacks.consume_group(row.group_id, link=link, guard=guard, reason=reason, session=writer)
            await self.services.prompts.close(link, link.chat_id, topic, reason=reason, guard=guard, session=writer)
            await self.services.callbacks.consume_scope(
                link, kinds={"clarify_option", "clarify_other", "network", "form_option", "form_next", "form_skip", "form_send", "form_change", "form_field"}, reason=reason, message_thread_id=topic, guard=guard, session=writer
            )
            for item, source in sources:
                request = decode_request(source.request)
                if request["source"] == "sandbox_network":
                    if settings is None:
                        raise ValueError("human-input-settings-missing") from None
                    locale = settings.locale
                    details = request["details"]
                    host = details["host"] + (f":{details['port']}" if details["port"] not in (80, 443) else "")
                    view = _presentation(render("net.cancelled", locale, host=host), (), reply_to_message_id=source.record.origins[0].message_id)
                    await self._view(link, item, source, view, step_id=f"closed:{reason}:{update_id}:{item.id}", guard=guard, writer=writer)
                else:
                    saved = item.payload["reply_basis"]["parts"]
                    from app.telegram_bots.mixed_source import human_input_parts

                    old = [part for part in human_input_parts(item) if part.get("state") == "sent" and part.get("message_ids")]
                    if old:
                        last = saved[-1]
                        clean = {
                            "kind": "edit",
                            "html": last["html"],
                            "visible_text": last["visible_text"],
                            "reply_to_message_id": source.record.origins[0].message_id,
                            "force_reply": False,
                            "remove_keyboard": True,
                            "target_message_id": old[-1]["message_ids"][-1],
                            "target_protect_content": bool(old[-1].get("sent_protect_content", False)),
                        }
                        await self.services.outbox.plan_human_input_step(source.record, source, step_id=f"closed:{reason}:{update_id}:{item.id}", parts=[clean], keyboard=[], callbacks=self.services.callbacks, guard=guard, session=writer)
            if command_mark is not None:
                command_mark.route_basis = {**command_mark.route_basis, "human_input_ui": {"command_closed": command_word}}
                await writer.flush()

    async def step_current(self, item, part=None):
        """Current durable UI revision; source qualification is a separate read."""
        from deerflow.utils.file_io import await_drained

        return await await_drained(self._step_current(item, part=part))

    async def _step_current(self, item, *, part=None):
        from datetime import UTC

        from sqlalchemy import select

        from deerflow.persistence.telegram_bots.model import TelegramCallbackRow

        basis = item.payload.get("reply_basis", {})
        if basis.get("version") not in {5, 7}:
            return True
        if part is not None:
            from app.telegram_bots.mixed_source import is_human_input_part

            if not is_human_input_part(item, part):
                return True
        controls = basis.get("keyboard", [])
        async with self.services.callbacks.sessions() as reader:
            rows = list(
                await reader.scalars(
                    select(TelegramCallbackRow).where(
                        TelegramCallbackRow.link_id == item.link_id,
                        TelegramCallbackRow.chat_id == item.chat_id,
                        TelegramCallbackRow.binding_generation == item.binding_generation,
                        TelegramCallbackRow.payload["anchor_outbox_id"].as_string() == item.id,
                    )
                )
            )
            kinds = {button.get("kind") for line in controls for button in line if "url" not in button}
            actionable = [row for row in rows if row.kind in kinds]
            if controls and (not actionable or all(row.consumed_at is not None for row in actionable)):
                return False
            prompts = [row for row in rows if row.kind == "prompt"]
            now = self.services.callbacks.clock()
            if prompts and all(row.consumed_at is not None or row.expires_at is not None and (row.expires_at.replace(tzinfo=UTC) if row.expires_at.tzinfo is None else row.expires_at) <= now for row in prompts):
                return False
            payloads = [button.get("payload", {}) for line in controls for button in line]
            for payload in payloads:
                if "form_id" in payload:
                    row = await reader.get(TelegramCallbackRow, payload["form_id"])
                    data = row.payload if row else {}
                    if data.get("state") in {"submitted", "cancelled"} or data.get("revision") != payload.get("revision"):
                        return False
            for prompt in prompts:
                if prompt.payload.get("purpose") == "form_field":
                    row = await reader.get(TelegramCallbackRow, prompt.payload.get("form_id"))
                    if row is None or row.payload.get("state") != "field" or row.payload.get("revision") != prompt.payload.get("revision"):
                        return False
        return True
