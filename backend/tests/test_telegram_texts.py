"""Telegram product wording and complete presentation contracts."""

import re
from string import Formatter

import pytest

from app.telegram_bots.render import render_plain
from app.telegram_bots.texts import FIRST_KEYS, catalog

STAGE_ONE_KEYS = frozenset(
    """
    hello.chat hello.custom_agent hello.privacy hello.link_expired hello.refused
    hello.account_taken hello.again stranger help.chat help.custom_agent privacy.full privacy.voice
    activity.read activity.write activity.edit activity.list activity.command activity.search activity.fetch
    activity.browser activity.image activity.subtask activity.present activity.work activity.other answer.thinking
    busy.followup busy.followup_full busy.queued busy.cancelled busy.gave_up busy.thread error.generic refused.interrupted
    chat.restarted chat.conversation_deleted chat.files_full stop.answer stop.nothing stop.failed file.saved_ask_plain file.saved_ask_photo
    file.saved_ask_many answer.on_web file.too_large_in file.not_saved file.own_full file.own_preparing file.own_no_seat file.own_unchecked file.own_unavailable
    file.too_large_out file.out_failed voice.unavailable voice.too_long voice.daily_limit voice.first_notice
    voice.agreed voice.not_used voice.writing voice.heard voice.failed voice.unclear media.unsupported
    update.refused edit.ignored token.pasted token.pasted_kept clarify.options clarify.more_choices clarify.answer_line
    clarify.other_prompt clarify.form_field clarify.form_yes clarify.form_no clarify.form_done clarify.form_review clarify.form_answer
    net.ask_plain net.allowed net.kept net.cancelled net.stale new.confirm_plain new.done new.kept new.in_topic new.files_full
    cmd.unknown cmd.agent cmd.skill_off cmd.skill_unavailable cmd.skill_failed stranger.waiting error.model_refused settings.state_basic link.conflict
    link.conflict_over.plain link.conflict_over.chat link.conflict_over.custom_agent link.limited link.back_online link.removed access.off
    access.back custom_agent.deleted unavailable.ask_admin unavailable.next unavailable.reference
    stale.other_person stale.not_waiting tap.failed form.required delivery.failed delivery.gone prompt.closed prompt.unavailable
    prompt.discarded profile.name.chat profile.short.chat profile.short.custom_agent
    profile.description.chat profile.description.custom_agent profile.removed
    web.list web.memory web.conversations web.conversation web.addAssistant web.addEmployee web.telegram
    web.timezone web.scheduled web.account web.fileTooLarge web.resultTooLarge
    """.split()
)
INSTANCE_FILE_KEYS = frozenset(
    {"file.saving", "file.waiting", "file.saved_ask", "file.saved", "file.saved_as", "file.same", "file.files_full", "file.files_full_ask", "file.files_busy", "file.unconfirmed", "file.not_active", "file.bad_name"}
)
INSTANCE_FILE_KEYS |= {"file.no_access"}
INSTANCE_AVAILABILITY_KEYS = frozenset("unavailable.off unavailable.paused unavailable.moved unavailable.files_unchecked unavailable.fix".split())
INSTANCE_FAILURE_KEYS = frozenset("busy.two_conversations busy.starting_elsewhere refused.restarting refused.files_busy start.ws_1 start.ws_30 start.ws_failed refused.files_full refused.files_full_ask".split())
INSTANCE_FAILURE_KEYS |= {"start.ws_failed_again", "refused.interrupted_manager", "refused.interrupted_recovery", "net.blocked_ask", "net.blocked"}
LIFECYCLE_KEYS = frozenset("access.agent_off agent.paused agent.retired agent.deleted".split())
INSTANCE_PRESENTATION_KEYS = frozenset("hello.assistant hello.employee help.agent privacy.company privacy.protected profile.short.agent profile.description.agent".split())
POLICY_KEYS = frozenset({"policy.off", "policy.on", "policy.on_protected", "policy.protected", "policy.unprotected"})


AGENT_LIST_KEYS = frozenset("cmd.other_chat assistants.list assistants.none assistants.colleagues employees.list employees.none agents.line_work agents.line_idle agents.line_plain agents.more agents.presentation".split())


WORK_REQUEST_KEYS = frozenset("ask.info ask.decision ask.comment_line review.ready concealed.question concealed.decision concealed.review".split())


def test_text_catalog_locales_have_equal_keys():
    assert set(catalog("en")) == set(catalog("zh-CN"))
    assert WORK_REQUEST_KEYS | AGENT_LIST_KEYS | STAGE_ONE_KEYS | INSTANCE_FILE_KEYS | POLICY_KEYS | INSTANCE_PRESENTATION_KEYS | LIFECYCLE_KEYS | INSTANCE_FAILURE_KEYS | INSTANCE_AVAILABILITY_KEYS == {
        key for key in catalog("en") if not key.startswith(("label.", "menu."))
    }
    assert catalog("zh_cn") is catalog("zh-CN")
    assert catalog("unrecognised") is catalog("en")
    assert all(text and "<br>" not in text for text in catalog("en").values())
    with pytest.raises(TypeError):
        catalog("en")["new.key"] = "changed"


def test_chat_texts_never_say_i():
    forbidden = re.compile(r"\b(?:I|I'm|I'll|I've|me|my)\b", re.I)
    assert not [(key, text) for key, text in catalog("en").items() if not key.startswith("label.") and forbidden.search(text)]


@pytest.mark.parametrize(
    "locale,expected",
    [("en", "Stopped. The text above is unfinished. Messages that were waiting were not answered."), ("zh-CN", "已停止。上面的文字尚未完成。等待中的消息未获回答。")],
)
def test_two_sentences_of_a_stop_join_as_the_language_joins_them(locale, expected):
    assert render_plain("stop.answer", locale, written=True, waiting=True) == expected


def test_chat_texts_use_no_pronoun_after_name():
    assert not [key for key, text in catalog("en").items() if "{name}" in text and re.search(r"\b(?:its|his|her|she|he)\b", text, re.I)]


def test_chat_texts_use_no_internal_term():
    assert not [key for key, text in catalog("en").items() if re.search(r"\b(?:attempt|attachment|inspect|sandbox|thread|container)\b", text, re.I)]
    for text in catalog("en").values():
        verbs_removed = re.sub(r"\b(?:is running|can run|chat runs without files)\b", "", text)
        assert not re.search(r"\bruns?\b", verbs_removed)


def test_localized_templates_keep_the_same_placeholders_and_current_commands():
    def placeholders(text):
        return {name for _, name, _, _ in Formatter().parse(text) if name}

    for key, english in catalog("en").items():
        assert placeholders(english) == placeholders(catalog("zh-CN")[key])
        assert "!" not in english
    for locale in ("en", "zh-CN"):
        for key in ("hello.chat", "help.chat"):
            assert "/assistants" in catalog(locale)[key] and "/employees" in catalog(locale)[key]
    assert "- /settings sets the language" in catalog("en")["help.chat"]
    for locale in ("en", "zh-CN"):
        for key in ("help.agent", "help.custom_agent"):
            assert "/assistants" not in catalog(locale)[key] and "/employees" not in catalog(locale)[key]


def test_chat_texts_name_no_storage_term_and_no_size():
    assert not [key for key, text in catalog("en").items() if re.search(r"\b(?:Space|slot|pool)\b|\b\d+\s*(?:GB|TB|GiB|MiB)\b", text, re.I)]
    assert {amount for text in catalog("en").values() for amount in re.findall(r"\b(\d+) MB\b", text)} == {"20", "50"}


def test_alert_texts_fit_200_characters():
    for locale in ("en", "zh-CN"):
        for key in ("link.limited", "stale.other_person", "stale.not_waiting"):
            assert len(render_plain(key, locale, product="P" * 64)) <= 200


def test_first_keys_are_exact_fixed_notice_subset():
    assert FIRST_KEYS == POLICY_KEYS | {"ask.info", "ask.decision", "review.ready"} | frozenset(
        {"agent.deleted", "hello.privacy", "link.conflict", "link.conflict_over.plain", "link.conflict_over.chat", "link.conflict_over.custom_agent", "link.back_online", "link.removed", "access.off", "access.back", "delivery.failed"}
    )


def test_no_text_sends_a_user_to_the_provider():
    for locale in ("en", "zh-CN"):
        for key in ("chat.files_full", "file.own_full", "file.own_preparing", "file.own_no_seat", "file.own_unchecked", "file.own_unavailable", "unavailable.next", "unavailable.reference"):
            values = {name: "value" for _, name, _, _ in Formatter().parse(catalog(locale)[key]) if name}
            values.update(provider="SERVICE-PROVIDER", product="HartMesh", file="notes.txt", code="storage-missing")
            text = render_plain(key, locale, **values)
            assert "SERVICE-PROVIDER" not in text
            assert "storage-missing" not in text


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
@pytest.mark.parametrize("custody", ["personal", "company"])
def test_instance_help_and_hello_hide_missing_roles_and_keep_kind(locale, custody):
    values = {"name": "A & B", "product": "HartMesh", "email": "reader@example.test", "role": None, "role_description": None, "custody": custody, "voice_enabled": False}
    hello = render_plain("hello.assistant" if custody == "personal" else "hello.employee", locale, **values)
    help_ = render_plain("help.agent", locale, **values)
    kind = ("AI assistant" if custody == "personal" else "AI employee") if locale == "en" else ("AI 助理" if custody == "personal" else "AI 员工")
    assert kind in hello and kind in help_
    assert all(text not in hello + help_ for text in ("None", "()", "（）", "/work", "/assign", "voice note", "语音消息"))
    shared = render_plain("hello.assistant" if custody == "personal" else "hello.employee", locale, read_by_others=True, **values)
    disclosure = render_plain("privacy.company", locale, **values)
    assert disclosure in shared and disclosure not in hello
    full = render_plain("privacy.full", locale, read_by_others=True, protected=True, **values)
    assert disclosure in full and render_plain("privacy.protected", locale, **values) in full


def test_colleague_hello_names_the_owner_without_an_empty_role():
    text = render_plain("hello.assistant", "en", name="Helper", owner="colleague@example.test", email="reader@example.test", product="HartMesh", role=None, read_by_others=True)
    assert "colleague@example.test's AI assistant" in text and "People who can see all" in text and "()" not in text
