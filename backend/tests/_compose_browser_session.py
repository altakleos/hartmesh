"""Authenticated Chromium pages and running image checks for mounted Compose rehearsals."""

from contextlib import contextmanager


def verify_image(compose, service, image):
    container = compose.run("ps", "-q", service).stdout.strip()
    running = compose.invoke(["docker", "inspect", "--format", "{{.Config.Image}} {{.Image}}", container]).stdout.strip().split()
    expected = compose.invoke(["docker", "image", "inspect", "--format", "{{.Id}}", image]).stdout.strip()
    assert running == [image, expected]
    return {"ref": image, "image_id": expected}


@contextmanager
def signed_in_browser(compose, session):
    from playwright.sync_api import sync_playwright

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            context = browser.new_context(locale="en-US")
            context.add_cookies([{"url": compose.url, "name": c.name, "value": c.value, "httpOnly": c.has_nonstandard_attr("HttpOnly"), "secure": c.secure, "sameSite": "Lax"} for c in session.client.cookies.jar])
            assert context.request.get(compose.url + "/api/v1/auth/me").json()["id"] == session.user["id"]
            yield context.new_page()
        finally:
            browser.close()
