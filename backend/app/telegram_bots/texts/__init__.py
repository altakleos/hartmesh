"""Immutable locale catalogs and current-stage stored keyboard descriptions."""

from collections.abc import Callable, Mapping
from types import MappingProxyType

from . import en, zh_cn

_CATALOGS = {"en": MappingProxyType(en.TEXTS), "zh-CN": MappingProxyType(zh_cn.TEXTS)}

FIRST_KEYS: frozenset[str] = frozenset(
    {
        "digest.header",
        "digest.header_news",
        "status.starting",
        "status.working",
        "status.waiting_you",
        "status.waiting_answer",
        "status.checking",
        "status.stopping",
        "status.stopped",
        "status.finished",
        "status.ended",
        "interrupted",
        "interrupted.title_not_started",
        "attempt.failed",
        "attempt.failed.other",
        "attempt.stopped_restart",
        "attempt.no_result",
        "attempt.no_result_excerpt",
        "request.answered",
        "decision.recorded",
        "result.accepted",
        "result.changes",
        "work.cancelled",
        "work.changed",
        "work.reopened",
        "attempt.stopped",
        "attempt.stopped_changed",
        "attempt.stopped_withdrawn",
        "work.proposed",
        "remind.ready_to_resume",
        "ask.info",
        "ask.decision",
        "review.ready",
        "result.done",
        "result.waiting_review",
        "hello.privacy",
        "agent.deleted",
        "link.conflict",
        "link.token_rejected_other",
        "link.conflict_over.plain",
        "link.conflict_over.chat",
        "link.conflict_over.agent",
        "link.conflict_over.custom_agent",
        "link.back_online",
        "link.removed",
        "access.off",
        "access.back",
        "delivery.failed",
        "policy.protected",
        "policy.unprotected",
        "policy.off",
        "policy.on",
        "policy.on_protected",
    }
)


def _language_keyboard(*, link):
    return [[{"label": label, "kind": "settings_set", "target_kind": "link", "target_id": link.id, "payload": {"field": "locale", "value": value}} for label, value in (("label.english", "en-US"), ("label.chinese", "zh-CN"))]]


def _new_keyboard(*, thread_id):
    return [[{"label": label, "kind": kind, "target_kind": "thread", "target_id": thread_id, "payload": {"thread_id": thread_id}} for label, kind in (("label.start_new", "new_confirm"), ("label.keep_conversation", "new_keep"))]]


def _help_keyboard(*, link):
    return [[{"label": "label.web_list", "kind": "web_list", "target_kind": "link", "target_id": link.id, "payload": {}}]]


def _voice_keyboard(*, payload):
    return [[{"label": label, "kind": kind, "target_kind": None, "target_id": None, "payload": payload}] for label, kind in (("label.use_voice", "voice_ok"), ("label.not_now", "voice_no"))]


KEYBOARDS: dict[str, Callable] = {
    "settings.state_basic": _language_keyboard,
    "new.confirm_plain": _new_keyboard,
    "help.chat": _help_keyboard,
    "help.custom_agent": _help_keyboard,
    "help.agent": _help_keyboard,
    "voice.first_notice": _voice_keyboard,
}


def _locale(locale: str) -> str:
    return "zh-CN" if locale.lower().replace("_", "-") == "zh-cn" else "en"


def catalog(locale: str) -> Mapping[str, str]:
    """Return the fixed catalog, falling back to English for unknown locales."""
    return _CATALOGS[_locale(locale)]


def forms(locale: str) -> Mapping[str, Mapping[str, str]]:
    return zh_cn.FORMS if _locale(locale) == "zh-CN" else en.FORMS


def words(locale: str) -> Mapping[str, str]:
    return zh_cn.WORDS if _locale(locale) == "zh-CN" else en.WORDS
