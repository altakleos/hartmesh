"""Pure product renderer and fresh administrator lookup regressions."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.telegram_bots import authority
from app.telegram_bots.render import render, render_plain


@pytest.mark.parametrize("locale,administrator", [("en", "an administrator"), ("zh-CN", "管理员")])
def test_support_reads_an_administrator_for_a_user_and_the_provider_for_an_administrator(locale, administrator):
    values = {"file": "data.txt", "product": "HartMesh", "provider": "R&D <service>", "support": "injected provider"}
    assert administrator in render("file.own_full", locale, **values)
    assert "injected provider" not in render("file.own_full", locale, **values)
    assert "R&amp;D &lt;service&gt;" in render("file.own_full", locale, reader_is_administrator=True, **values)
    assert "R&D <service>" in render_plain("file.own_full", locale, reader_is_administrator=True, **values)


def test_text_key_values_are_escaped():
    assert render("file.saved_ask_plain", "en", file="R&D <b> 'quoted' &amp;") == "Received R&amp;D &lt;b&gt; &#x27;quoted&#x27; &amp;amp;. What should be done with it?"
    with pytest.raises(KeyError):
        render("no.such.key", "en")
    with pytest.raises(KeyError):
        render("file.saved_ask_plain", "en")


def test_plain_text_values_are_not_escaped():
    assert render_plain("profile.name.chat", "en", product="R&D <b>") == "R&D <b>"
    assert render_plain("clarify.answer_line", "en", label="R&D <b>") == "Your answer: R&D <b>"


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
def test_unavailable_endings_hide_reference_from_users(locale):
    values = {"file": "data.txt", "provider": "Service", "code": "storage-missing"}
    assert "Service" not in render("file.own_unavailable", locale, **values)
    assert "storage-missing" not in render("file.own_unavailable", locale, **values)
    assert "Service" in render("file.own_unavailable", locale, reader_is_administrator=True, **values)
    assert "storage-missing" in render("file.own_unavailable", locale, reader_is_administrator=True, **values)
    assert "files-unchecked" in render("file.own_unchecked", locale, reader_is_administrator=True, file="a.txt", provider="Service")
    assert "More accounts exist here than seats" in render("file.own_no_seat", "en", reader_is_administrator=True, **values)
    assert render("unavailable.reference", locale, code="storage-missing") == ""
    with pytest.raises(ValueError):
        render("file.own_unavailable", locale, reader_is_administrator=True, **(values | {"code": "internal-pool-code"}))


def test_optional_wording_has_safe_defaults_and_explicit_facts():
    assert render("stop.answer", "en", written=False, waiting=True) == "Stopped. Messages that were waiting were not answered."
    assert "voice note" not in render("help.chat", "en", product="HartMesh", voice_enabled=False)
    assert "voice notes" not in render("media.unsupported", "en", voice_enabled=False)
    assert "voice note" in render("help.chat", "en", product="HartMesh", voice_enabled=True)


def test_titles_are_clipped_but_canonical_preview_values_are_complete(monkeypatch):
    import app.telegram_bots.render as module

    templates = {"test.preview": "{objective}\n{objective_full}\n{success_criteria}\n{supervisor}", "test.first": "{person}: {objective}\n{name}"}
    monkeypatch.setattr(module, "catalog", lambda locale: templates)
    monkeypatch.setattr(module, "FIRST_KEYS", frozenset({"test.first"}))
    full = "long word " * 100
    text = render_plain("test.preview", "en", objective=full, objective_full=full, success_criteria=full, supervisor="Rosa", reader_is_supervisor=True)
    assert len(text.split("\n")[0]) <= 60
    assert text.split("\n")[1:3] == [full, full]
    assert text.split("\n")[-1] == "a manager"
    first = render_plain("test.first", "en", person="P" * 100, name="N" * 100, objective=full)
    assert len(first.split("\n")[0]) <= 60
    assert first.split("\n")[1] == "N" * 100


@pytest.mark.asyncio
async def test_administrator_lookup_reads_current_account_and_role_limit(monkeypatch):
    provider = SimpleNamespace(get_user=AsyncMock(return_value=SimpleNamespace(system_role="admin", role_limit=None)))
    monkeypatch.setattr(authority, "get_local_provider", lambda: provider)
    assert await authority.is_administrator("owner")
    provider.get_user.return_value.role_limit = "user"
    assert not await authority.is_administrator("owner")
    provider.get_user.return_value = SimpleNamespace(system_role="user", role_limit=None)
    assert not await authority.is_administrator("owner")
    provider.get_user.return_value = None
    assert not await authority.is_administrator("owner")
    provider.get_user.side_effect = LookupError("unavailable account")
    assert not await authority.is_administrator("owner")
    assert provider.get_user.await_count == 5
    assert all(call.args == ("owner",) for call in provider.get_user.await_args_list)


def test_forms_support_reader_custody_supervisor_and_topics_without_later_catalog_keys(monkeypatch):
    import app.telegram_bots.render as module

    monkeypatch.setattr(module, "catalog", lambda locale: {"test.form": "base"})
    monkeypatch.setattr(
        module,
        "forms",
        lambda locale: {
            "test.form": {
                "personal": "AI assistant: {supervisor}. Ask {support}.",
                "company": "AI employee: {supervisor}. Ask {support}.",
                "company.administrator": "AI employee, administrator: {supervisor}. Ask {support}.",
                "no_supervisor": "Ask an administrator.",
                "topics_hint": "\nTopics hint.",
            }
        },
    )
    assert render("test.form", "en", custody="personal", supervisor="Rosa") == "AI assistant: Rosa. Ask an administrator."
    assert render("test.form", "en", custody="company", reader_is_supervisor=True) == "AI employee: a manager. Ask an administrator."
    assert render("test.form", "en", custody="company", reader_is_administrator=True, supervisor="Rosa", provider="Service") == "AI employee, administrator: Rosa. Ask Service."
    assert render("test.form", "en", supervisor=None, topics_hint=True) == "Ask an administrator.\nTopics hint."
    with pytest.raises(ValueError):
        render("test.form", "en", custody="invalid")


@pytest.mark.parametrize("locale", ["en", "zh-CN"])
def test_every_proactive_first_line_fits_without_clipping_later_brand_values(locale):
    from string import Formatter

    from app.telegram_bots.texts import FIRST_KEYS, catalog

    for key in FIRST_KEYS:
        values = {name: "Very long product " * 20 for _, name, _, _ in Formatter().parse(catalog(locale)[key]) if name}
        text = render_plain(key, locale, **values)
        assert len(text.split("\n", 1)[0]) <= 60
        if key == "access.off":
            assert values["product"] in text.split("\n", 1)[1]


def test_delivery_failure_catalog_keeps_exact_sentences_and_renderer_only_adds_line_break():
    from app.telegram_bots.texts import catalog

    english = "A message could not be shown here. Retry delivery sends it again and starts nothing new."
    assert catalog("en")["delivery.failed"] == english
    assert render("delivery.failed", "en").replace("\n", " ") == english
    assert render("delivery.failed", "zh-CN").replace("\n", "") == catalog("zh-CN")["delivery.failed"]
