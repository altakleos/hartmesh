"""Extraction runs in-process, and never reaches for a package manager.

A released-profile qualification measured the defect: all 23 successful direct fetches called
``readabilipy``'s ``have_node()``, which shells out to ``node -v`` and then, with
no ``node_modules`` beside the installed package, calls ``run_npm_install()``.
The released Gateway is non-root on an immutable image, so each fetch logged

    npm error syscall mkdir
    npm error errno -13
    EACCES: permission denied, mkdir
    '/app/backend/.venv/lib/python3.12/site-packages/readabilipy/javascript/node_modules'

before the pure-Python extractor -- which was always the one doing the work --
produced the result.

The first test is the contract: no subprocess, for any input. The rest show the
pure-Python path is a deliberate choice that still extracts, on the three page
shapes the repair brief names.
"""

from __future__ import annotations

import subprocess

import pytest

from deerflow.utils.readability import ReadabilityExtractor

REPRESENTATIVE_HTML = """
<html><head><title>Great Lakes</title></head><body>
<nav><a href="/">home</a><a href="/about">about</a></nav>
<article>
  <h1>The Great Lakes</h1>
  <p>The Great Lakes hold about 21 per cent of the world's surface fresh water,
     and supply drinking water to roughly 30 million people.</p>
  <p>Shipping on the lakes carries iron ore, coal and grain between eight states
     and two provinces.</p>
</article>
<footer>copyright</footer>
</body></html>
"""

# A page that never closes its tags, with text after the damage.
MALFORMED_HTML = "<html><body><div><p>Muse is a personal agent.<div><span>It runs in a secure VM.<p>Pricing starts at twenty dollars a month."

# What a JS-heavy but publicly readable page looks like to a fetcher: a mount
# point, the payload in a script tag, and a <noscript> the reader can still use.
JS_HEAVY_HTML = """
<html><head><title>Muse Agent</title>
<script type="application/json" id="__DATA__">{"props":{"body":"hydrated later"}}</script>
</head><body>
<div id="root"></div>
<noscript><h1>Muse Agent</h1><p>Muse is a personal AI agent that runs tasks in a secure VM.</p></noscript>
<script>window.__hydrate(document.getElementById('root'));</script>
</body></html>
"""


@pytest.fixture
def no_subprocess(monkeypatch: pytest.MonkeyPatch) -> list[list[str]]:
    """Any attempt to leave the process is a failure, not a fallback.

    ``readabilipy`` reaches for ``node`` and ``npm`` through ``subprocess.run``
    and ``subprocess.check_call``; both are stubbed so a probe or an install
    shows up as a test failure naming the command, instead of an EACCES
    traceback in a tenant's Gateway log.
    """
    attempts: list[list[str]] = []

    def forbidden(args, *_rest, **_kwargs):  # noqa: ANN001, ANN202 - stub matches two stdlib signatures
        attempts.append(list(args) if isinstance(args, (list, tuple)) else [str(args)])
        raise AssertionError(f"extraction tried to run {args!r}; a request must not probe for or install anything")

    monkeypatch.setattr(subprocess, "run", forbidden)
    monkeypatch.setattr(subprocess, "check_call", forbidden)
    monkeypatch.setattr(subprocess, "Popen", forbidden)
    return attempts


@pytest.mark.parametrize(
    ("html", "shape"),
    [(REPRESENTATIVE_HTML, "representative"), (MALFORMED_HTML, "malformed"), (JS_HEAVY_HTML, "js-heavy"), ("", "empty"), ("<html></html>", "no content")],
)
def test_extraction_never_leaves_the_process(html: str, shape: str, no_subprocess: list[list[str]]) -> None:
    ReadabilityExtractor().extract_article(html)
    assert no_subprocess == [], f"{shape}: nothing should have been executed"


def test_a_representative_page_extracts_its_body_and_drops_the_furniture(no_subprocess: list[list[str]]) -> None:
    article = ReadabilityExtractor().extract_article(REPRESENTATIVE_HTML)
    markdown = article.to_markdown()

    assert "21 per cent" in markdown and "30 million people" in markdown
    assert "iron ore" in markdown
    assert article.title != "Untitled"


def test_malformed_html_still_yields_its_text(no_subprocess: list[list[str]]) -> None:
    # Unclosed tags all the way down: the extractor must return the text rather
    # than raise, because a real page like this is a fetch the model is waiting on.
    article = ReadabilityExtractor().extract_article(MALFORMED_HTML)
    markdown = article.to_markdown()

    assert "personal agent" in markdown
    assert "secure VM" in markdown
    assert "twenty dollars" in markdown


def test_a_js_heavy_page_yields_what_is_actually_in_the_html(no_subprocess: list[list[str]]) -> None:
    """Explicit about what this extraction can and cannot do.

    Nothing here executes JavaScript -- that is what the browser tool is for --
    and the simplifier also drops ``<noscript>``, so a page whose body arrives
    by hydration yields little beyond its title. Stated here rather than
    discovered on a tenant's turn. It is not a regression: the deployment's fetches
    were already taking this path, since the JS extractor never ran there
    either.
    """
    article = ReadabilityExtractor().extract_article(JS_HEAVY_HTML)
    markdown = article.to_markdown()

    assert "Muse Agent" in markdown, "the title and any served body are what a reader gets"
    assert "hydrated later" not in markdown, "a script payload is not page text"
    assert "window.__hydrate" not in markdown
    # Measured, not assumed: this extractor drops <noscript>, so a page whose
    # body arrives by hydration yields little more than its title. That is what
    # the tenant was already getting -- the JS path never ran there either --
    # so it is the behaviour to state, not a regression to hide. A page like
    # this is what the browser tool exists for.
    assert "personal AI agent" not in markdown


def test_an_empty_page_says_so_instead_of_returning_nothing(no_subprocess: list[list[str]]) -> None:
    article = ReadabilityExtractor().extract_article("<html><body></body></html>")
    assert "No content" in article.to_markdown()
    assert article.title == "Untitled"


def test_only_the_pure_python_path_is_ever_asked_for(monkeypatch: pytest.MonkeyPatch) -> None:
    """One call, with the JS path off -- not a JS attempt that falls back.

    The old behaviour asked twice: ``use_readability=True``, then ``False``
    after the first raised. That second call was the one doing the work, and
    the first was two subprocesses and a traceback. What replaced it has to be
    a single deliberate call, not a quieter fallback.
    """
    calls: list[bool] = []

    def fake(html: str, use_readability: bool = False) -> dict:
        calls.append(use_readability)
        return {"title": "T", "content": "<p>C</p>"}

    monkeypatch.setattr("deerflow.utils.readability.simple_json_from_html_string", fake)
    article = ReadabilityExtractor().extract_article("<html><body>x</body></html>")

    assert calls == [False], "asked once, for the path that does the work"
    assert article.title == "T"


def test_an_unexpected_failure_still_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    """Absorbing the empty-page case must not absorb everything else.

    A resource failure reported as "no content" would be indistinguishable in
    the logs from a page that genuinely had none, which is how a repeatable
    memory or recursion failure becomes invisible. This is the contract the
    module had before -- unexpected errors are surfaced, not swallowed -- and
    it survives the change.
    """
    for error in (RuntimeError("unexpected parser failure"), MemoryError(), RecursionError()):

        def fake(html: str, use_readability: bool = False, _error: BaseException = error) -> dict:
            raise _error

        monkeypatch.setattr("deerflow.utils.readability.simple_json_from_html_string", fake)
        with pytest.raises(type(error)):
            ReadabilityExtractor().extract_article("<html><body>x</body></html>")


def test_the_js_path_is_off_by_construction() -> None:
    """The one line that decides it, asserted directly.

    ``use_readability=True`` is what makes ``readabilipy`` probe for Node and
    attempt ``npm install``; nothing else in this module reaches outside the
    process. If this flips, every fetch pays two subprocesses again.
    """
    assert ReadabilityExtractor.USE_READABILITY_JS is False
