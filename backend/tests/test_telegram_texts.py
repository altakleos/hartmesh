"""Stage-one Telegram product wording and complete presentation contracts."""

import re
from string import Formatter

import pytest

from app.telegram_bots.render import render_plain, render_preview
from app.telegram_bots.texts import FIRST_KEYS, catalog

STAGE_ONE_KEYS = frozenset(
    """
    hello.chat hello.custom_agent hello.privacy hello.link_expired hello.refused
    hello.account_taken hello.again stranger help.chat help.custom_agent privacy.full privacy.voice
    activity.read activity.write activity.edit activity.list activity.command activity.search activity.fetch
    activity.browser activity.image activity.subtask activity.present activity.work activity.other answer.thinking
    busy.followup busy.followup_full busy.queued busy.cancelled busy.gave_up busy.thread error.generic
    chat.restarted chat.files_full run.cancelled stop.answer stop.nothing stop.failed file.saved_ask_plain file.too_large_in
    file.not_saved file.own_full file.own_preparing file.own_no_seat file.own_unchecked file.own_unavailable
    file.too_large_out file.out_failed voice.unavailable voice.too_long voice.daily_limit voice.first_notice
    voice.agreed voice.not_used voice.writing voice.heard voice.failed voice.unclear media.unsupported
    edit.ignored token.pasted token.pasted_kept clarify.options clarify.more_choices clarify.answer_line
    clarify.other_prompt clarify.form_field clarify.form_yes clarify.form_no clarify.form_done clarify.form_review
    net.ask_plain net.allowed net.kept net.cancelled net.stale new.confirm_plain new.done new.kept
    cmd.unknown cmd.agent cmd.skill_off cmd.skill_unavailable settings.state_basic link.conflict
    link.conflict_over.chat link.conflict_over.custom_agent link.limited link.back_online link.removed access.off
    access.back custom_agent.deleted unavailable.ask_admin unavailable.next unavailable.reference
    stale.other_person stale.not_waiting tap.failed delivery.failed prompt.closed prompt.unavailable
    prompt.discarded preview.part_header profile.name.chat profile.short.chat profile.short.custom_agent
    profile.description.chat profile.description.custom_agent profile.removed
    web.list web.memory web.conversations web.conversation web.addAssistant web.addEmployee web.telegram
    web.timezone web.scheduled web.account web.fileTooLarge web.resultTooLarge
    """.split()
)


def test_text_catalog_locales_have_equal_keys():
    assert set(catalog("en")) == set(catalog("zh-CN"))
    assert STAGE_ONE_KEYS == {key for key in catalog("en") if not key.startswith(("label.", "menu."))}
    assert catalog("zh_cn") is catalog("zh-CN")
    assert catalog("unrecognised") is catalog("en")
    assert all(text and "<br>" not in text for text in catalog("en").values())
    with pytest.raises(TypeError):
        catalog("en")["new.key"] = "changed"


def test_chat_texts_never_say_i():
    forbidden = re.compile(r"\b(?:I|I'm|I'll|I've|me|my)\b", re.I)
    assert not [(key, text) for key, text in catalog("en").items() if not key.startswith("label.") and forbidden.search(text)]


@pytest.mark.parametrize("locale,expected", [("en", "This request was cancelled."), ("zh-CN", "此次请求已取消。")])
def test_sealed_cancellation_text_does_not_claim_queue_removal(locale, expected):
    assert render_plain("run.cancelled", locale) == expected
    assert catalog(locale)["run.cancelled"] != catalog(locale)["busy.cancelled"]


def test_chat_texts_use_no_pronoun_after_name():
    assert not [key for key, text in catalog("en").items() if "{name}" in text and re.search(r"\b(?:its|his|her|she|he)\b", text, re.I)]


def test_chat_texts_use_no_internal_term():
    assert not [key for key, text in catalog("en").items() if re.search(r"\b(?:attempt|attachment|inspect|sandbox|thread|container)\b", text, re.I)]
    for text in catalog("en").values():
        verbs_removed = re.sub(r"\b(?:is running|can run|chat runs without files)\b", "", text)
        assert not re.search(r"\bruns?\b", verbs_removed)


def test_localized_templates_keep_the_same_placeholders_and_stage_one_wording():
    def placeholders(text):
        return {name for _, name, _, _ in Formatter().parse(text) if name}

    for key, english in catalog("en").items():
        assert placeholders(english) == placeholders(catalog("zh-CN")[key])
        assert "!" not in english
    assert "/assistants" not in catalog("en")["hello.chat"]
    assert "/assistants" not in catalog("en")["help.chat"]
    assert "- /settings sets the language" in catalog("en")["help.chat"]


def test_chat_texts_name_no_storage_term_and_no_size():
    assert not [key for key, text in catalog("en").items() if re.search(r"\b(?:Space|slot|pool)\b|\b\d+\s*(?:GB|TB|GiB|MiB)\b", text, re.I)]
    assert {amount for text in catalog("en").values() for amount in re.findall(r"\b(\d+) MB\b", text)} == {"20", "50"}


def test_alert_texts_fit_200_characters():
    for locale in ("en", "zh-CN"):
        for key in ("link.limited", "stale.other_person", "stale.not_waiting"):
            assert len(render_plain(key, locale, product="P" * 64)) <= 200


def test_first_keys_are_exact_stage_one_proactive_subset():
    assert FIRST_KEYS == frozenset({"hello.privacy", "link.conflict", "link.conflict_over.chat", "link.conflict_over.custom_agent", "link.back_online", "link.removed", "access.off", "access.back", "delivery.failed"})


def test_no_text_sends_a_user_to_the_provider():
    for locale in ("en", "zh-CN"):
        for key in ("chat.files_full", "file.own_full", "file.own_preparing", "file.own_no_seat", "file.own_unchecked", "file.own_unavailable", "unavailable.next", "unavailable.reference"):
            values = {name: "value" for _, name, _, _ in Formatter().parse(catalog(locale)[key]) if name}
            values.update(provider="SERVICE-PROVIDER", product="HartMesh", file="notes.txt", code="storage-missing")
            text = render_plain(key, locale, **values)
            assert "SERVICE-PROVIDER" not in text
            assert "storage-missing" not in text


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
def test_complete_actionable_previews_split_with_controls_last(locale):
    from app.telegram_bots.split import utf16_units, visible_text

    question = "Question <&> 😀 " * 400
    full_options = "\n".join(f"{n}. complete option {n} " + "choice " * 150 for n in range(1, 5))
    canonical = question + "\n" + full_options
    plan = render_preview("clarify.options", locale, heading="A long title " * 20, basis={"revision": 7}, protected=True, text=canonical)
    assert len(plan.parts) >= 3
    assert "".join(visible_text(part).split("\n", 1)[1] for part in plan.parts) == canonical
    assert all(utf16_units(visible_text(part)) <= 4096 for part in plan.parts)
    for index in range(len(plan.parts)):
        part = plan.next_part(tuple(range(1, index + 1)))
        assert part.html == plan.parts[index]
        assert part.protected
        assert part.controls_allowed is (index == len(plan.parts) - 1)
        assert part.silent is (index != len(plan.parts) - 1)
        assert len(visible_text(part.html).split("\n", 1)[0]) <= 60
    assert plan.next_part(tuple(range(1, len(plan.parts) + 1))) is None
    assert plan.next_part(()) == plan.next_part(())
    assert plan.basis == {"revision": 7}
