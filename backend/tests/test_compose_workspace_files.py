"""Measure file browsing and a lost Home upload on the installed managed bundle."""

import hashlib
import json
import os
import platform
import re
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit

import pytest

if os.environ.get("HARTMESH_REQUIRE_BUNDLE_INSTANCES") != "1" or not os.environ.get("FILES_BROWSER_REVISION"):
    pytest.skip("requires the hosted workspace browser bundle", allow_module_level=True)

from _compose_gateway_restart import start_gateway, stop_gateway
from _compose_shared_reader import retire_reader
from _compose_workspace_file_timing import observe
from playwright.sync_api import TimeoutError as BrowserTimeout
from playwright.sync_api import expect
from test_compose_agent_connections import signed_in_browser, verify_image
from test_compose_instance_acceptance import await_execution_ready, eventually, required, sql
from test_compose_instance_acceptance import compose as compose
from test_compose_instance_acceptance import member as member
from test_compose_instance_acceptance import session as session

pytestmark = pytest.mark.live
TITLE = "Workspace browser assistant"
LIBRARY = "Browser library"
UPLOAD = "browser-upload.txt"
CONTENT = b"Managed browser upload with a lost response."


def image_pair(bundle):
    result = {}
    for name, service, variable in (("backend", "gateway", "BACKEND_REF"), ("frontend", "frontend", "HARTMESH_BROWSER_FRONTEND")):
        observed = verify_image(bundle, service, required(variable))
        labels = json.loads(bundle.invoke(["docker", "image", "inspect", "--format", "{{json .Config.Labels}}", observed["image_id"]]).stdout)
        revision = labels["org.opencontainers.image.revision"]
        source = labels["org.opencontainers.image.source"]
        assert revision == required("FILES_BROWSER_REVISION") and source == "https://github.com/" + required("GITHUB_REPOSITORY")
        result[name] = {**observed, "revision": revision, "source": source}
    return result


def prepare(bundle, user):
    user.request("POST", "/api/agents", json={"name": "files-assistant", "model": "acceptance-model", "skills": [], "mcp_plugins": [], "allowed_subagents": [], "soul": "Assist with files."}, expected=201)
    agent = user.request("POST", "/api/agent-instances", json={"creation_id": uuid.uuid4().hex, "name": TITLE, "custody": "personal", "definition_name": "files-assistant"}, expected=201).json()
    bindings = bundle.bindings()
    mine = bundle.role(user.user["id"])
    company = bundle.psql("SELECT space_id FROM hm_storage_feature_links WHERE namespace='hm.company-space' AND key='company'")

    def root(space, folder):
        (binding,) = [b for b in bindings.values() if b["space_id"] == space]
        return [binding["slot_id"], binding["root_inode"], folder]

    areas = {"personal": root(mine["space"], "files"), "shared": root(company, "shared"), "agent": root(agent["home_id"], "uploads")}
    script = Path(__file__).with_name("_compose_workspace_file_seed.py").resolve()
    counts = json.loads(bundle.invoke(["sudo", "-n", sys.executable, str(script), json.dumps(areas)]).stdout)
    return agent, counts


def region(page, name):
    return page.get_by_role("region", name=re.compile("^" + re.escape(name) + "$", re.I))


def location(agent, *, folder="uploads", view="browser"):
    return "/workspace/instances?" + urlencode({"instance": agent["id"], "tab": "files", "folder": folder, "view": view})


def http_sample(user, path, params, count):
    result = user.request("GET", path, params=params, timeout=12).json()
    if count is None:
        assert result["truncated"] is False
        assert {"path": LIBRARY, "kind": "directory"} in [{"path": row["path"], "kind": row["kind"]} for row in result["files"]]
    else:
        assert result["truncated"] and len(result["files"]) == count


def memory(bundle):
    values = bundle.run("exec", "-T", "gateway", "cat", "/sys/fs/cgroup/memory.current", "/sys/fs/cgroup/memory.peak").stdout.splitlines()
    current, peak = map(int, values)
    assert 0 < current and 0 < peak
    return {"current_cgroup_bytes": current, "peak_cgroup_bytes": peak}


def browse(page, base, path, name):
    started = time.monotonic()
    response = page.goto(base + path, wait_until="domcontentloaded")
    assert response.ok
    content = region(page, name)
    action = content.get_by_role("button", name=LIBRARY, exact=True)
    expect(action).to_be_visible(timeout=30000)
    assert action.is_enabled()
    return round(time.monotonic() - started, 5)


def measure(bundle, user, page, agent, evidence, record):
    surfaces = (("personal", "/workspace/files?view=browser", "My files"), ("shared", "/workspace/files?tab=shared&view=browser", "Company shared"), ("agent", location(agent), TITLE))
    for key, path, title in surfaces:
        record["ui"][key] = {}
        fresh = page.context.browser.new_context(locale="en-US", viewport={"width": 1280, "height": 800})
        try:
            fresh.add_cookies(page.context.cookies([bundle.url]))
            cold_page = fresh.new_page()
            traversed = []
            cold_page.on("request", lambda request: traversed.append(urlsplit(request.url).path))
            observe(record["ui"], key, lambda: browse(cold_page, bundle.url, path, title))
            record["ui"][key]["first_document_seconds"] = record["ui"][key].pop("seconds")[0]
            del record["ui"][key]["maximum_seconds"], record["ui"][key]["sample_count"]
            if key in ("personal", "shared"):
                direct = f"/api/file-browser/v1/{key}/folder"
                flat = "/api/files" if key == "personal" else "/api/shared"
                assert direct in traversed and flat not in traversed
                scope = region(cold_page, title)
                with cold_page.expect_response(lambda response: response.url.split("?", 1)[0] == bundle.url + flat) as pending:
                    scope.get_by_role("searchbox").fill("entry-")
                assert pending.value.status == 200
                expect(scope.get_by_role("link", name=re.compile(r"^entry-\d+\.txt$")).first).to_be_visible(timeout=30000)
                expect(scope.get_by_text(re.compile(r"only the first 10,?000 entries", re.I))).to_be_visible()
                assert traversed.count(flat) == 1
                record["ui"][key]["search_flat_requests"] = traversed.count(flat)
        finally:
            fresh.close()

    # Start in an authenticated workspace. Time each actual location link from
    # activation until the first actionable folder of its destination is usable.
    browse(page, bundle.url, "/workspace/files?view=browser", "My files")
    start = time.monotonic()
    window = record["measurement_window"] = {"budget_seconds": 300}
    previous = "My files"
    locations = (("shared", "Company shared", LIBRARY), ("agent", TITLE, "Uploads"), ("personal", "My files", LIBRARY))
    try:
        for _ in range(20):
            for key, title, first_folder in locations:
                source = region(page, previous)
                if key == "agent" or not source.get_by_role("link", name=title, exact=True).is_visible():
                    source.get_by_role("button", name="Locations", exact=True).click(timeout=10000)
                    source = page.get_by_role("dialog", name="Locations")
                link = source.get_by_role("link", name=title, exact=True)
                expect(link).to_be_visible(timeout=10000)

                def visit():
                    link.click(timeout=10000)
                    folder = region(page, title).get_by_role("button", name=first_folder, exact=True)
                    expect(folder).to_be_visible(timeout=10000)
                    assert folder.is_enabled()

                observe(record["ui"], key, visit)
                previous = title
                assert time.monotonic() - start < window["budget_seconds"]
        window["observations_per_surface"] = 20
    finally:
        window["elapsed_seconds"] = round(time.monotonic() - start, 5)

    controls = (
        ("personal-list", "/api/files", None, 10000),
        ("shared-list", "/api/shared", None, 10000),
        ("agent-legacy-folder", f"/api/spaces/{agent['home_id']}/files", {"path": "uploads/" + LIBRARY, "limit": 500}, 500),
        ("agent-current-folder", f"/api/file-browser/v1/agents/{agent['id']}/folder", {"path": "uploads/" + LIBRARY, "limit": 500}, 500),
    )
    folders = tuple((area + "-direct-" + label, f"/api/file-browser/v1/{area}/folder", {"path": folder}, count) for area in ("personal", "shared") for label, folder, count in (("root", "", None), ("child", LIBRARY, 500)))
    for key, path, params, count in (*controls, *folders):
        record["http"][key] = {"expected_folder": LIBRARY} if count is None else {"returned": count}
        for _ in range(20):
            observe(record["http"], key, lambda: http_sample(user, path, params, count))
    record["memory_after_browse"] = memory(bundle)

    # The existing scripted model can remain held for 180 seconds. The full
    # performance sample runs above. Only four bounded requests overlap this turn.
    thread = user.thread()
    token = uuid.uuid4().hex
    barrier = evidence / ("armed-" + token)
    waiting = evidence / ("waiting-" + token)
    user.upload(thread)
    barrier.touch(exist_ok=False)
    held = record["chat"] = {"thread": thread, "concurrent": {}}
    with ThreadPoolExecutor(max_workers=1) as executor:
        run = executor.submit(user.run, thread, message="acceptance:file:final:" + token)
        try:
            eventually(lambda: waiting.is_file() or run.done(), timeout=120, interval=0.25)
            assert waiting.is_file() and not run.done(), "normal chat did not reach its final model call"
            overlap = time.monotonic()
            for key, path, params, count in (folders[0], folders[2], controls[0], controls[3]):
                held["concurrent"][key] = {}
                observe(held["concurrent"], key, lambda: http_sample(user, path, params, count))
                assert not run.done() and time.monotonic() - overlap < 60
            held["overlap_seconds"] = round(time.monotonic() - overlap, 5)
        finally:
            barrier.unlink(missing_ok=True)
        held["final_status"] = run.result(timeout=150)["status"]
    sandbox = bundle.sandbox(thread)
    retire_reader(bundle, user, thread, sandbox["Id"], record, wait_ready=await_execution_ready)


def phone_geometry(page, base, evidence):
    context = page.context.browser.new_context(viewport={"width": 390, "height": 844}, locale="zh-CN", color_scheme="dark", has_touch=True, is_mobile=True)
    try:
        context.add_cookies(page.context.cookies([base]))
        context.add_cookies([{"url": base, "name": "locale", "value": "zh-CN"}])
        context.add_init_script("localStorage.setItem('theme', 'dark')")
        mobile = context.new_page()
        reply = mobile.goto(base + "/workspace/files?" + urlencode({"tab": "shared", "folder": LIBRARY, "view": "browser"}), wait_until="domcontentloaded")
        assert reply.ok
        source = region(mobile, "公司共享")
        control = source.get_by_role("button", name=re.compile(r"entry-.* 的更多操作")).first
        expect(control).to_be_visible(timeout=30000)
        expect(mobile.locator("html")).to_have_attribute("lang", "zh-CN")
        expect(mobile.locator("html")).to_have_class(re.compile(r"\bdark\b"))
        control.focus()
        expect(control).to_be_focused()
        rect = control.bounding_box()
        widths = mobile.evaluate("() => [innerWidth, document.documentElement.scrollWidth]")
        assert rect["width"] >= 44 and rect["height"] >= 44 and widths[1] <= widths[0]
        mobile.screenshot(path=evidence / "workspace-file-phone-zh-dark.png")
        return {"locale": "zh-CN", "theme": "dark", "viewport": widths[0], "document": widths[1], "menu_width": rect["width"], "menu_height": rect["height"], "keyboard_focus": True}
    finally:
        context.close()


def shared_cycle(user):
    source = "/mnt/user-data/files/" + LIBRARY + "/entry-00000.txt"
    submitted = {"path": source, "folder": "Browser proof"}
    receipt = user.request("POST", "/api/shared/publish", json=submitted, expected=201).json()
    duplicate = user.request("POST", "/api/shared/publish", json=submitted).json()
    assert receipt["publication_id"] and receipt["revision"] and not duplicate.get("revision")
    assert duplicate["publication_id"] == receipt["publication_id"]
    selection = user.request("GET", "/api/file-browser/v1/shared/selection", params={"path": receipt["path"]}).json()
    assert selection["can_remove"] and selection["revision"] == receipt["revision"] and selection["publication_id"] == receipt["publication_id"]
    request = {"path": receipt["path"], "expected_publication_id": receipt["publication_id"], "expected_revision": receipt["revision"], "operation_id": uuid.uuid4().hex}
    for _ in range(2):
        user.request("POST", "/api/file-browser/v1/shared/remove", json=request, expected=204)
    user.request("GET", "/api/file-browser/v1/shared/selection", params={"path": receipt["path"]}, expected=404)
    user.request("GET", "/api/files/" + LIBRARY + "/entry-00000.txt")
    return {"path": receipt["path"], "publication_id": receipt["publication_id"], "operation_id": request["operation_id"], "duplicate_reused": True, "confirmed_replay": True}


def verify_copies(session, agent, actual, digest):
    url = f"/api/file-browser/v1/agents/{agent['id']}/content"
    assert session.request("GET", url, params={"path": "uploads/" + UPLOAD}).content == b"Earlier file"
    assert hashlib.sha256(session.request("GET", url, params={"path": actual}).content).hexdigest() == digest


def check_upload(bundle, user, page, agent, evidence, result):
    prefix = f"/api/agent-instances/{agent['id']}/home/operations"
    actual, checks, writes, confirmed = {}, [], [], []
    accepting = False
    digest = hashlib.sha256(CONTENT).hexdigest()

    def observe(route):
        request = route.request
        parsed = urlsplit(request.url)
        if request.method == "PUT" and parsed.path == prefix + "/content":
            params = parse_qs(parsed.query)
            (nonce,) = params["operation_id"]
            assert re.fullmatch("[0-9a-f]{32}", nonce) and params["path"] == ["uploads/" + UPLOAD]
            assert not writes, "the original upload was replayed"
            writes.append(nonce)
            response = route.fetch(max_retries=0)
            assert response.ok
            actual.update(response.json())
            assert actual["sha256"] == digest and actual["size"] == len(CONTENT) and not actual["deduplicated"]
            assert actual["path"].startswith("uploads/") and actual["path"] != params["path"][0]
            route.abort("failed")
        elif request.method == "GET" and writes and parsed.path == prefix + "/" + writes[0]:
            assert request.post_data is None and not parsed.query
            checks.append(writes[0])
            if accepting:
                route.continue_()
            else:
                route.abort("failed")
        else:
            route.continue_()

    def receipt_response(response):
        if accepting and writes and response.request.method == "GET" and response.url == bundle.url + prefix + "/" + writes[0] and response.status == 200:
            body = response.json()
            if body.get("status") == "complete":
                assert body["file"] == actual
                confirmed.append(writes[0])

    route_pattern = bundle.url + prefix + "**"
    page.on("response", receipt_response)
    page.route(route_pattern, observe)
    try:
        browse(page, bundle.url, location(agent), TITLE)
        page.locator('input[type="file"]').first.set_input_files({"name": UPLOAD, "mimeType": "text/plain", "buffer": CONTENT})
        expect(page.get_by_text("The result is currently unavailable.", exact=True)).to_be_visible(timeout=30000)
        assert actual and len(writes) == 1
        page.get_by_role("link", name="Use classic file view", exact=True).click()
        expect(page.get_by_role("link", name="Use file browser", exact=True)).to_be_visible(timeout=20000)
        expect(page.get_by_text("The result is currently unavailable.", exact=True)).to_be_visible()
        result.update({"original_id": writes[0], "requested_path": "uploads/" + UPLOAD, "saved_path": actual["path"], "sha256": digest})
        page.screenshot(path=evidence / "workspace-file-interrupted.png")
        before = bundle.run("ps", "-q", "gateway").stdout.strip()
        started = bundle.invoke(["docker", "inspect", "--format", "{{.State.StartedAt}}", before]).stdout.strip()
        stop_gateway(bundle)
        live = start_gateway(bundle, user)
        assert live["State"]["StartedAt"] != started
        result["gateway_restart"] = {"previous_started_at": started, "new_started_at": live["State"]["StartedAt"]}
        await_execution_ready(user)
        accepting = True
        saved_name = "Saved as " + actual["path"].rsplit("/", 1)[-1]
        saved = page.get_by_role("status").filter(has_text=re.compile("^" + re.escape(saved_name) + "$"))
        if not saved.is_visible():
            check = page.get_by_role("button", name="Check again", exact=True)
            try:
                check.click(timeout=3000)
            except BrowserTimeout:
                expect(saved).to_be_visible(timeout=30000)
        expect(saved).to_be_visible(timeout=30000)
        assert checks and set(checks) == set(writes) == set(confirmed)
        status = user.request("GET", prefix + "/" + writes[0]).json()
        assert status["status"] == "complete" and status["file"] == actual
        matching = bundle.psql(
            "SELECT count(*) FROM storage_space_file_operations WHERE space_id=" + sql(agent["home_id"]) + " AND phase='complete' AND request->>'path'=" + sql("uploads/" + UPLOAD) + " AND request->>'sha256'=" + sql(digest)
        )
        assert matching == "1"
        verify_copies(user, agent, actual["path"], digest)
        page.screenshot(path=evidence / "workspace-file-recovered.png")
        result.update({"confirmed": True, "writes": len(writes), "checks": len(checks), "matching_sql_operations": int(matching), "terminal": status["status"]})
    finally:
        page.unroute(route_pattern, observe)
        page.remove_listener("response", receipt_response)


def test_installed_workspace_files(compose, session, member):
    evidence = Path(required("EVIDENCE"))
    record = {"complete": False, "revision": required("FILES_BROWSER_REVISION"), "workflow_sha": required("GITHUB_SHA"), "attempt": required("GITHUB_RUN_ATTEMPT"), "ui": {}, "http": {}, "transfer": {}}
    try:
        record["images_before"] = image_pair(compose)
        record["host"] = {"architecture": platform.machine(), "logical_cores": os.cpu_count()}
        record["memory_before"] = memory(compose)
        for service in ("gateway", "frontend"):
            container = compose.run("ps", "-q", service).stdout.strip()
            raw = compose.invoke(["docker", "inspect", "--format", "{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}", container]).stdout.split()
            record.setdefault("limits", {})[service] = {"nano_cpus": int(raw[0]), "memory_bytes": int(raw[1])}
        await_execution_ready(session)
        agent, record["counts"] = prepare(compose, session)
        source = f"/api/file-browser/v1/agents/{agent['id']}/folder"
        with signed_in_browser(compose, member) as denied:
            reply = denied.request.get(compose.url + source, params={"path": "uploads/" + LIBRARY})
            assert reply.status in (403, 404)
            record["access"] = {"refused": True, "status": reply.status, "distinct_users": member.user["id"] != session.user["id"]}
            assert record["access"]["distinct_users"]
        with signed_in_browser(compose, session) as page:
            feature = page.request.get(compose.url + "/api/features").json()["file_browser"]
            assert feature["storage_mode"] == "spaces" and all("folder" in feature["operations"][area] for area in ("agent", "personal", "shared")) and "remove" in feature["operations"]["shared"]
            measure(compose, session, page, agent, evidence, record)
            record["phone"] = phone_geometry(page, compose.url, evidence)
            record["shared"] = shared_cycle(session)
            check_upload(compose, session, page, agent, evidence, record["transfer"])
        record["images_after"] = image_pair(compose)
        assert record["images_after"] == record["images_before"]
        record["performance_scope"] = (
            "Fresh Chromium context per first document. Twenty signed-in workspace link activations per area and twenty public HTTP samples per route. OS caches are shared. Four bounded GETs also overlap a live ordinary chat."
        )
        record["ui_target_met"] = {key: value["p95_seconds"] <= 1 for key, value in record["ui"].items()}
        record["http_target_seconds"] = 1
        record["http_target_met"] = {key: value["p95_seconds"] <= 1 for key, value in record["http"].items()}
        assert all(record["ui_target_met"].values()), record["ui_target_met"]
        record["complete"] = True
    finally:
        (evidence / "workspace-files.json").write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
