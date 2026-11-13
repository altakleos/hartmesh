"""Closed per-person message preference choices and localized chat forms."""

import re
from collections.abc import Mapping

from app.telegram_bots.callbacks import canonical_keyboard
from app.telegram_bots.render import render_plain


def parse_quiet(value):
    if type(value) is not str or (match := re.fullmatch(r"([0-2][0-9]):([0-5][0-9])-([0-2][0-9]):([0-5][0-9])", value.strip())) is None:
        raise ValueError("invalid-quiet-times")
    first, first_minute, last, last_minute = map(int, match.groups())
    if first > 23 or last > 23 or (first, first_minute) == (last, last_minute):
        raise ValueError("invalid-quiet-times")
    return first * 60 + first_minute, last * 60 + last_minute


def _minute(value):
    if type(value) is not int or not 0 <= value < 1440:
        raise ValueError("invalid-settings-act")
    return value


def setting_value(field, value):
    if field in {"locale", "notify"}:
        allowed = {"en-US", "zh-CN"} if field == "locale" else {"all", "questions", "none"}
        if type(value) is not str or value not in allowed:
            raise ValueError("invalid-settings-act")
        return value
    fields = {"enabled", "start", "end"} if field == "quiet" else {"enabled", "minute"} if field == "summary" else None
    if fields is None or not isinstance(value, Mapping) or set(value) != fields or type(value["enabled"]) is not bool:
        raise ValueError("invalid-settings-act")
    result = {"enabled": value["enabled"], **{key: _minute(value[key]) for key in fields - {"enabled"}}}
    if field == "quiet" and result["start"] == result["end"]:
        raise ValueError("invalid-settings-act")
    return result


def setting_body(value):
    if not isinstance(value, Mapping) or set(value) != {"field", "old", "new"} or type(value["field"]) is not str:
        raise ValueError("invalid-settings-act")
    return {"field": value["field"], **{key: setting_value(value["field"], value[key]) for key in ("old", "new")}}


def choose(settings, field, value):
    if type(field) is not str or type(value) is not str:
        raise ValueError("invalid-settings-choice")
    if field in {"locale", "notify"}:
        old, new = getattr(settings, field), value
    elif field == "quiet":
        old = {"enabled": settings.quiet_enabled, "start": settings.quiet_start_minute, "end": settings.quiet_end_minute}
        start, end = (old["start"], old["end"]) if value == "off" else parse_quiet(value)
        new = {"enabled": value != "off", "start": start, "end": end}
    elif field == "summary":
        old = {"enabled": settings.summary_enabled, "minute": settings.summary_minute}
        if value not in {"off", "07:00", "08:00", "09:00"}:
            raise ValueError("invalid-summary-choice")
        new = {"enabled": value != "off", "minute": old["minute"] if value == "off" else int(value[:2]) * 60}
    else:
        raise ValueError("invalid-settings-choice")
    return setting_body({"field": field, "old": old, "new": new})


def setting_changes(value):
    body = setting_body(value)
    field, new = body["field"], body["new"]
    if field == "quiet":
        return {"quiet_enabled": new["enabled"], "quiet_start_minute": new["start"], "quiet_end_minute": new["end"]}
    if field == "summary":
        return {"summary_enabled": new["enabled"], "summary_minute": new["minute"]}
    return {field: new}


def clock_text(minute):
    return f"{minute // 60:02d}:{minute % 60:02d}"


def present(settings, *, link, product, settings_url=None, page="state", notice=None):
    locale = settings.locale

    def text(key, **values):
        return render_plain("settings." + key, locale, **values)

    def button(label, kind, **payload):
        return {"label": "label.choice", "label_values": {"label": label}, "kind": kind, "target_kind": "link", "target_id": link.id, "payload": payload}

    def choice(label, field, value):
        return button(label, "settings_set", field=field, value=value)

    if page == "state":
        quiet = text("quiet_on", start=clock_text(settings.quiet_start_minute), end=clock_text(settings.quiet_end_minute), zone=settings.timezone) if settings.quiet_enabled else text("quiet_off")
        body = text(
            "overview",
            value=text("notify_" + settings.notify),
            quiet=quiet,
            time=clock_text(settings.summary_minute),
            enabled=text("on" if settings.summary_enabled else "off"),
            language="中文" if locale == "zh-CN" else "English",
            product=product,
        )
        if notice in {"questions", "none"}:
            body += "\n" + text("messages_questions" if notice == "questions" else "messages_off", product=product)
        keyboard = [[button(text("label_" + field), "settings_open", field=field) for field in fields] for fields in (("notify", "quiet"), ("summary", "locale"))]
        if settings_url is not None:
            keyboard.append([{"label": "label.open_telegram", "url": settings_url}])
        return {"key": "settings.state", "values": {"body": body}, "keyboard": canonical_keyboard(keyboard)}
    if page == "notify":
        keyboard = [[choice(text("notify_all"), page, "all")], [choice(text("label_questions"), page, "questions"), choice(text("label_none"), page, "none")]]
    elif page == "quiet":
        keyboard = [[choice(text("off"), page, "off"), choice("21:00–07:00", page, "21:00-07:00")], [choice("22:00–08:00", page, "22:00-08:00"), button(text("other_hours"), "settings_prompt", field="quiet")]]
    elif page == "summary":
        keyboard = [[choice(text("off"), page, "off"), choice("07:00", page, "07:00")], [choice("08:00", page, "08:00"), choice("09:00", page, "09:00")]]
    elif page == "locale":
        keyboard = [[choice("English", page, "en-US"), choice("中文", page, "zh-CN")]]
    else:
        raise ValueError("invalid-settings-page")
    keyboard.append([{"label": "label.back", "kind": "back", "target_kind": "link", "target_id": link.id, "payload": {"settings_page": "state"}}])
    key = {"notify": "messages", "quiet": "quiet", "summary": "summary", "locale": "language"}[page]
    return {"key": "settings." + key + "_choose", "values": {}, "keyboard": canonical_keyboard(keyboard)}
