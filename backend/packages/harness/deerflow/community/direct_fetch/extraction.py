"""Turn a fetched page into readable text, without running a package manager.

``readabilipy`` offers a Readability.js path, and asking for it is not free:
``have_node()`` shells out to ``node -v`` and, finding no ``node_modules`` beside
the installed package, calls ``run_npm_install()`` to create one. A Gateway that
runs non-root on an immutable image cannot make that write, so every fetch would
spend two subprocesses and log a traceback before falling back to the
pure-Python extractor that then does the work.

The page reader therefore asks for the pure-Python path deliberately: no Node
probe, no install attempt, no request-time package management.
"""

from __future__ import annotations

import logging

from readabilipy import simple_json_from_html_string

from deerflow.utils.readability import Article, ReadabilityExtractor

logger = logging.getLogger(__name__)


class InProcessExtractor(ReadabilityExtractor):
    """Extraction that stays inside the process it runs in."""

    def extract_article(self, html: str, *, url: str | None = None) -> Article:
        try:
            article = simple_json_from_html_string(html, use_readability=False)
        except IndexError:
            # Exactly one failure is absorbed, and only because it is not a
            # failure: a page with nothing in it reaches an unguarded index
            # inside the simplifier's BeautifulSoup pass, and an empty body is
            # an ordinary thing for a fetch to meet -- a 204, a redirect stub,
            # a wrapper whose content never arrived. Raising would fail the
            # whole fetch for a page that simply had no article in it.
            #
            # Nothing else is caught, deliberately. A MemoryError or a
            # RecursionError from a pathological page is a resource failure,
            # and reporting it as "no content" would make it indistinguishable
            # in the logs from an empty page.
            logger.warning("Nothing to extract from a %d-character page; reporting it as empty", len(html), exc_info=True)
            article = {}

        html_content = article.get("content")
        if not html_content or not str(html_content).strip():
            html_content = "No content could be extracted from this page"

        title = article.get("title")
        if not title or not str(title).strip():
            title = "Untitled"

        return Article(title=title, html_content=html_content, url=url)
