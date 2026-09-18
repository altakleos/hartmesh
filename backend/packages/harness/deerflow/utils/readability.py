"""Turn a fetched page into readable text, without running a package manager.

``readabilipy`` offers a Readability.js path, and asking for it is not free:
``have_node()`` shells out to ``node -v`` and, finding no ``node_modules`` beside
the installed package, calls ``run_npm_install()`` to create one. The released
Gateway runs non-root on an immutable image, so that write cannot succeed. The
tenant class measured the result: all 23 successful direct fetches spent two
subprocesses and logged an EACCES traceback for
``/app/backend/.venv/.../readabilipy/javascript/node_modules`` before falling
back to the pure-Python extractor that then did the work.

The fallback was doing the extraction, so the JS path was never the behaviour
-- only its cost. This module now asks for the pure-Python path deliberately:
no Node probe, no install attempt, no request-time package management, and the
same output the tenant was already getting. Restoring the JS path is a
packaging decision (bake the dependency into the image), not something a
request may attempt.
"""

import logging
import re
from urllib.parse import urljoin

from markdownify import markdownify as md
from readabilipy import simple_json_from_html_string

logger = logging.getLogger(__name__)


class Article:
    url: str

    def __init__(self, title: str, html_content: str):
        self.title = title
        self.html_content = html_content

    def to_markdown(self, including_title: bool = True) -> str:
        markdown = ""
        if including_title:
            markdown += f"# {self.title}\n\n"

        if self.html_content is None or not str(self.html_content).strip():
            markdown += "*No content available*\n"
        else:
            markdown += md(self.html_content)

        return markdown

    def to_message(self) -> list[dict]:
        image_pattern = r"!\[.*?\]\((.*?)\)"

        content: list[dict[str, str]] = []
        markdown = self.to_markdown()

        if not markdown or not markdown.strip():
            return [{"type": "text", "text": "No content available"}]

        parts = re.split(image_pattern, markdown)

        for i, part in enumerate(parts):
            if i % 2 == 1:
                image_url = urljoin(self.url, part.strip())
                content.append({"type": "image_url", "image_url": {"url": image_url}})
            else:
                text_part = part.strip()
                if text_part:
                    content.append({"type": "text", "text": text_part})

        # If after processing all parts, content is still empty, provide a fallback message.
        if not content:
            content = [{"type": "text", "text": "No content available"}]

        return content


class ReadabilityExtractor:
    """Extraction that stays inside the process it runs in."""

    #: Never ``True``. ``use_readability=True`` makes ``readabilipy`` probe for
    #: Node and try ``npm install`` on a read-only tree; see the module
    #: docstring. Kept as a named constant so the choice is visible at the call
    #: site rather than looking like a forgotten default.
    USE_READABILITY_JS = False

    def extract_article(self, html: str) -> Article:
        try:
            article = simple_json_from_html_string(html, use_readability=self.USE_READABILITY_JS)
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
            # in the logs from an empty page -- so those still surface, as any
            # unexpected error always has. The raw HTML is not salvaged
            # either: if the parser could not read it, this is not the place
            # to guess.
            logger.warning("Nothing to extract from a %d-character page; reporting it as empty", len(html), exc_info=True)
            article = {}

        html_content = article.get("content")
        if not html_content or not str(html_content).strip():
            html_content = "No content could be extracted from this page"

        title = article.get("title")
        if not title or not str(title).strip():
            title = "Untitled"

        return Article(title=title, html_content=html_content)
