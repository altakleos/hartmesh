"""Browser observations for the opt-in candidate Compose rehearsal."""

import json
import re
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from playwright.sync_api import TimeoutError as BrowserTimeout
from playwright.sync_api import expect, sync_playwright


class AttachmentBrowser:
    def __init__(self, page, origin, conversation, evidence, image):
        self.page = page
        self.thread = conversation["thread_id"]
        self.agent = conversation["instance"]
        self.scope = f"/api/agent-instances/{self.agent['id']}/conversations/{self.thread}/files"
        self.evidence = evidence
        self.image = image
        self.requests = []
        self.responses = []
        self.chip = None
        self.interrupted = None
        self.origin = origin

    def relevant(self, method, url):
        path = urlsplit(url).path
        other_writes = (f"/api/threads/{self.thread}/uploads", f"/api/agent-instances/{self.agent['id']}/home/", f"/api/spaces/{self.agent['home_id']}/")
        return path.startswith(self.scope) or (method not in ("GET", "HEAD") and path.startswith(other_writes))

    def submit(self, endpoint, name, content):
        self.page.goto(f"{self.origin}/workspace/chats/{self.thread}", wait_until="domcontentloaded")
        attach = self.page.get_by_role("button", name=f"Attach a file for {self.agent['name']}", exact=True)
        expect(attach).to_be_enabled(timeout=30000)

        def request_seen(request):
            if self.relevant(request.method, request.url):
                url = urlsplit(request.url)
                # Chromium may omit bytes for multipart file parts in its
                # network observation. The server separately proves the file.
                has_body = request.post_data_buffer is not None or request.headers.get("content-type", "").startswith("multipart/form-data") or request.headers.get("content-length", "0") != "0"
                self.requests.append({"method": request.method, "origin": f"{url.scheme}://{url.netloc}", "path": url.path, "query": parse_qs(url.query), "has_body": has_body})

        def response_seen(response):
            if self.relevant(response.request.method, response.url):
                self.responses.append({"method": response.request.method, "path": urlsplit(response.url).path, "status": response.status, "server": response.headers.get("server")})

        self.page.on("request", request_seen)
        self.page.on("response", response_seen)
        with self.page.expect_file_chooser() as chooser:
            attach.click()
        with self.page.expect_request(lambda request: urlsplit(request.url).path == endpoint and request.method == "POST", timeout=20000) as sent:
            chooser.value.set_files({"name": name, "mimeType": "text/plain", "buffer": content})
        (operation,) = parse_qs(urlsplit(sent.value.url).query)["operation_id"]
        assert re.fullmatch("[0-9a-f]{32}", operation)
        self.chip = self.page.get_by_role("article").filter(has_text=name)
        return operation

    def observe_interruption(self):
        # Gateway is still stopped. The immediate GET must give the open chip
        # a visible, inconclusive result and leave a manual check available.
        notice = "The result is currently unavailable."
        expect(self.chip.get_by_role("status")).to_have_text(notice, timeout=20000)
        expect(self.chip.get_by_role("button", name="Check again")).to_be_visible()
        self.interrupted = self.chip.inner_text()
        self.chip.screenshot(path=self.evidence / "attachment-browser-interrupted.png")
        writes = [event for event in self.responses if event["method"] == "POST"]
        assert len(writes) == 1 and writes[0]["status"] == 502 and writes[0]["server"].startswith("nginx"), writes
        return writes[0]["status"]

    def confirm(self, outcome):
        complete = f"Saved in {self.agent['name']}'s files"
        # The independent storage check has observed a terminal receipt. Let
        # any in-flight automatic read finish, or exercise the manual GET.
        saved = self.chip.get_by_text(complete, exact=True)
        if not saved.is_visible():
            try:
                self.chip.get_by_role("button", name="Check again", exact=True).click(timeout=3000)
            except BrowserTimeout:
                # An automatic completion may already have removed the button.
                expect(saved).to_be_visible()
        expect(self.chip.get_by_role("status")).to_have_text(complete, timeout=30000)
        self.chip.screenshot(path=self.evidence / "attachment-browser-recovered.png")
        endpoint = self.scope + "/operations"
        post, *checks = self.requests
        assert post == {"method": "POST", "origin": self.origin, "path": endpoint, "query": {"operation_id": [outcome["operation"]]}, "has_body": True}
        expected = {"method": "GET", "origin": self.origin, "path": endpoint + "/" + outcome["operation"], "query": {"filename": [outcome["name"]]}, "has_body": False}
        assert checks and all(check == expected for check in checks), checks
        assert any(response["method"] == "GET" and response["status"] == 200 for response in self.responses)
        (self.evidence / "attachment-browser.json").write_text(
            json.dumps(
                {
                    "frontend": self.image,
                    "thread": self.thread,
                    "interrupted": self.interrupted,
                    "recovered": self.chip.inner_text(),
                    "requests": self.requests,
                    "responses": self.responses,
                },
                sort_keys=True,
            ),
            encoding="utf-8",
        )


@contextmanager
def open_attachment_browser(compose, session, conversation, image, evidence):
    frontend = compose.run("ps", "--no-trunc", "--quiet", "frontend").stdout.strip()
    observed = compose.invoke(["docker", "inspect", "--format", "{{.Config.Image}}", frontend]).stdout.strip()
    assert observed == image and image.startswith("localhost:80/hartmesh-frontend@sha256:")
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(locale="en-US")
            context.add_cookies([{"url": compose.url, "name": cookie.name, "value": cookie.value, "httpOnly": cookie.has_nonstandard_attr("HttpOnly"), "secure": cookie.secure, "sameSite": "Lax"} for cookie in session.client.cookies.jar])
            me = context.request.get(compose.url + "/api/v1/auth/me")
            assert me.ok and me.json()["id"] == session.user["id"]
            yield AttachmentBrowser(context.new_page(), compose.url, conversation, Path(evidence), image)
        finally:
            browser.close()
