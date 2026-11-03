"""SearXNG result URLs are screened before they reach the model or a browser.

SearXNG is the Compose bundle's default search provider, and frontend-hm
renders result ``url`` and ``thumbnail_url`` values as links and images, so an
intranet or metadata address in an untrusted result must not pass through.
The screen (``url_safety.safe_public_result_url``, from upstream #6740/#6819)
does no DNS or network I/O.
"""

from unittest.mock import patch

import httpx
import pytest

from deerflow.community.searxng.tools import normalize_image_results, normalize_results
from deerflow.community.url_safety import safe_public_result_url

_NON_PUBLIC = [
    "http://metadata.google.internal/x",
    "http://169.254.169.254/latest/meta-data/",
    "http://100.100.100.200/latest/meta-data/",
    "http://[64:ff9b::7f00:1]/x",
    "http://[::ffff:127.0.0.1]/x",
    "http://[2002:7f00:1::]/x",
    "http://127.0.0.1/x",
    "http://0x7f000001/x",
    "http://0177.0.0.1/x",
    "http://3758096385/x",
    "http://10.0.0.5/x",
    "http://localhost./x",
    "http://intranet.localhost/x",
    "http://127。0。0。1/x",
    "file:///etc/passwd",
    "javascript:alert(1)",
]


@pytest.mark.parametrize("url", _NON_PUBLIC)
def test_non_public_result_urls_are_dropped(url):
    assert safe_public_result_url(url) == ""


@pytest.mark.parametrize("url", ["https://example.com/a", "http://8.8.8.8/a", "http://[2606:4700:4700::1111]/a", "https://images。example/full.jpg"])
def test_public_result_urls_are_kept_without_dns(url):
    with patch("socket.getaddrinfo", side_effect=AssertionError("result screening must not resolve DNS")):
        assert safe_public_result_url(url) == url


def test_idna_separator_spelling_matches_what_clients_connect_to():
    assert httpx.URL("http://127。0。0。1/x").host == "127.0.0.1"


def test_non_string_results_are_dropped():
    assert safe_public_result_url(None) == ""
    assert safe_public_result_url(["https://example.com"]) == ""


def test_web_results_keep_only_public_urls():
    results = [
        {"title": "metadata", "url": "http://169.254.169.254/latest/meta-data/", "content": "x"},
        {"title": "public", "url": "https://example.com/report", "content": "y"},
    ]
    assert normalize_results(results, max_results=5) == [{"title": "public", "url": "https://example.com/report", "snippet": "y"}]


def test_image_results_screen_image_thumbnail_and_source_urls():
    results = [
        {"title": "private image", "img_src": "http://10.0.0.5/a.png", "url": "https://example.com/page"},
        {
            "title": "public image",
            "img_src": "https://images.example/full.jpg",
            "thumbnail_src": "http://127.0.0.1/thumb.jpg",
            "url": "http://169.254.169.254/page",
        },
    ]
    assert normalize_image_results(results, max_results=5) == [
        {"title": "public image", "image_url": "https://images.example/full.jpg", "thumbnail_url": "", "source_url": ""},
    ]
