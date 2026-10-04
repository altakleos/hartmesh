"""A failed report command cannot replace an earlier complete draft."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import sys
from pathlib import Path

import pytest

for _library in ("pandas", "docx", "pypdf", "jinja2", "matplotlib"):
    pytest.importorskip(_library)

ROOT = Path(__file__).resolve().parents[4]
SCRIPT = ROOT / "skills/public/business-report/scripts/report.py"
FIXTURE = Path(__file__).parent / "fixtures/example_services_export_small.csv"


@pytest.fixture
def report():
    spec = importlib.util.spec_from_file_location("business_report_publication", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    previous = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = previous
    return module


def current(root):
    # Also accepts the preceding fixed-path layout while establishing the
    # regression against that implementation.
    state = root / ".cache/business-report/out.current.json"
    if state.exists():
        return root / json.loads(state.read_text(encoding="utf-8"))["report"]
    return next(root.glob("*.report.json"))


def public_bytes(root):
    return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file() and ".cache" not in path.relative_to(root).parts}


@pytest.mark.parametrize("command", ["build", "prose", "render"])
@pytest.mark.parametrize("failure_target", ["docx", "xlsx"])
@pytest.mark.parametrize("interrupted", [False, True])
def test_renderer_failure_keeps_the_preceding_complete_bundle(report, tmp_path, monkeypatch, command, failure_target, interrupted):
    root = tmp_path / "out"
    assert report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--render", "docx,xlsx"]) == 0
    path = current(root)
    neighbor = root / "customer-notes.txt"
    neighbor.write_text("keep this", encoding="utf-8")
    before = public_bytes(root)
    real_render = report.render

    def fail(document, report_path, target, out_path, tenant):
        if target == failure_target:
            # A renderer can leave a partially written format before failing.
            destination = out_path or report_path.with_suffix(f".{target}")
            destination.write_bytes(b"incomplete render")
            if interrupted:
                raise KeyboardInterrupt
            raise RuntimeError("controlled renderer failure")
        return real_render(document, report_path, target, out_path, tenant)

    monkeypatch.setattr(report, "render", fail)
    if command == "build":
        argv = ["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--exclude", "category=Warranty", "--render", "docx,xlsx"]
    elif command == "prose":
        argv = ["prose", str(path), "--summary", "Revised words.", "--render", "docx,xlsx"]
    else:
        argv = ["render", str(path), "--to", "docx,xlsx"]
    with pytest.raises(KeyboardInterrupt if interrupted else RuntimeError):
        report.main(argv)
    assert public_bytes(root) == before
    assert current(root) == path


def build(report, root, identity="initial", formats="docx,xlsx"):
    return report.main(["build", str(FIXTURE), "--period", "2026-08", "--out", str(root), "--bundle-id", identity, "--render", formats])


def assert_complete(path):
    manifest = json.loads((path.parent / "renders.json").read_text(encoding="utf-8"))
    document = json.loads(path.read_text(encoding="utf-8"))
    assert manifest["draft"] == document["meta"]["draft"]
    assert json.loads((path.parent / "checks.json").read_text(encoding="utf-8")) == document["checks"]
    for name, digest in manifest["sha256"].items():
        assert hashlib.sha256((path.parent / name).read_bytes()).hexdigest() == digest
    assert all(chart["png"] in manifest["sha256"] for chart in document["charts"])
    assert ".cache" not in path.read_text(encoding="utf-8")
    assert path.stat().st_mode & 0o777 == 0o644
    assert path.parent.stat().st_mode & 0o777 == 0o755


def test_new_draft_is_complete_and_old_bundle_and_unowned_neighbors_are_retained(report, tmp_path, capsys):
    root = tmp_path / "out"
    assert build(report, root) == 0
    previous = current(root)
    before = public_bytes(previous.parent)
    neighbor = previous.parent / "out.pdf"
    neighbor.write_bytes(b"an unowned neighboring file")
    assert report.main(["prose", str(previous), "--summary", "A revised review.", "--bundle-id", "revised", "--render", "docx,xlsx"]) == 0
    published = current(root)
    assert published == root / "drafts/revised/out.report.json"
    assert_complete(published)
    assert {name: (previous.parent / name).read_bytes() for name in before} == before
    assert neighbor.read_bytes() == b"an unowned neighboring file"
    assert str(published) in capsys.readouterr().out


def test_empty_render_target_is_refused_before_any_publication(report, tmp_path):
    root = tmp_path / "out"
    assert build(report, root) == 0
    before = public_bytes(root)
    assert report.main(["render", str(current(root)), "--to", ","]) == 1
    assert public_bytes(root) == before


def test_published_chart_directories_are_readable_under_a_restrictive_umask(report, tmp_path):
    import os

    root = tmp_path / "out"
    previous = os.umask(0o077)
    try:
        assert build(report, root) == 0
    finally:
        os.umask(previous)
    assert_complete(current(root))
    assert (current(root).parent / "charts").stat().st_mode & 0o777 == 0o755


@pytest.mark.parametrize("identity", ["initial", "empty-neighbor", "../outside", "UPPER"])
def test_existing_or_invalid_bundle_id_never_overwrites_a_directory(report, tmp_path, identity):
    root = tmp_path / "out"
    assert build(report, root) == 0
    empty = root / "drafts/empty-neighbor"
    empty.mkdir()
    before = public_bytes(root)
    assert build(report, root, identity) == 1
    assert public_bytes(root) == before
    assert empty.is_dir() and not list(empty.iterdir())


@pytest.mark.parametrize("subdirectory", ["", "nested"])
def test_build_refuses_a_root_inside_a_published_bundle_before_writing_state_or_checks(report, tmp_path, monkeypatch, subdirectory):
    root = tmp_path / "out"
    assert build(report, root) == 0
    source = current(root)
    before = {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}
    # Even a withheld build must not overwrite the preceding bundle's checks.
    monkeypatch.setattr(report, "independent_amount_total", lambda *args, **kwargs: 1.0)
    assert build(report, source.parent / subdirectory, "nested-draft") == 1
    assert {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()} == before


@pytest.mark.parametrize("command", ["prose", "render"])
def test_stale_source_is_refused(report, tmp_path, command, capsys):
    root = tmp_path / "out"
    assert build(report, root) == 0
    stale = current(root)
    assert build(report, root, "newer") == 0
    before = public_bytes(root)
    argv = ["prose", str(stale), "--summary", "Stale edit."] if command == "prose" else ["render", str(stale), "--to", "xlsx"]
    assert report.main(argv) == 1
    assert "earlier report draft" in capsys.readouterr().err
    assert public_bytes(root) == before


@pytest.mark.parametrize("member", ["out.report.json", "out.docx", "charts/revenue_by_category.png", "checks.json"])
def test_outside_edit_refuses_reuse_but_an_explicit_build_can_recover(report, tmp_path, member):
    root = tmp_path / "out"
    assert build(report, root) == 0
    source = current(root)
    edited = source.parent / member
    edited.write_bytes(edited.read_bytes() + b"\n")
    before = public_bytes(root)
    assert report.main(["render", str(source), "--to", "xlsx"]) == 1
    assert public_bytes(root) == before
    assert build(report, root, "recovered") == 0
    assert_complete(current(root))
    assert {name: (root / name).read_bytes() for name in before} == before


def test_failure_before_rename_keeps_current_state_and_has_no_success_paths(report, tmp_path, monkeypatch, capsys):
    root = tmp_path / "out"
    assert build(report, root) == 0
    before = public_bytes(root)
    capsys.readouterr()
    publisher = sys.modules["business_report_publish"]

    def fail(*args):
        raise OSError("controlled publication failure")

    monkeypatch.setattr(publisher.os, "rename", fail)
    with pytest.raises(OSError):
        build(report, root, "failed")
    assert public_bytes(root) == before
    output = capsys.readouterr().out
    assert "Rendered" not in output and "Built" not in output


def test_pointer_failure_leaves_only_a_complete_unadopted_bundle(report, tmp_path, monkeypatch, capsys):
    root = tmp_path / "out"
    assert build(report, root) == 0
    previous = current(root)
    publisher = sys.modules["business_report_publish"]
    original = publisher.write_json

    def fail(path, value):
        if path.name.endswith(".current.json"):
            raise OSError("controlled pointer failure")
        return original(path, value)

    monkeypatch.setattr(publisher, "write_json", fail)
    assert build(report, root, "unadopted") == 1
    assert current(root) == previous
    assert_complete(root / "drafts/unadopted/out.report.json")
    assert "Complete report bundle created" in capsys.readouterr().err


def test_checks_and_explicit_exports_do_not_modify_any_published_bundle(report, tmp_path):
    root = tmp_path / "out"
    assert build(report, root) == 0
    source = current(root)
    before = public_bytes(source.parent)
    assert report.main(["checks", str(source)]) == 0
    assert (root / "checks.json").exists()
    assert report.main(["render", str(source), "--to", "xlsx", "--out", str(tmp_path / "export.xlsx")]) == 0
    for destination in [source, source.parent / "one-off.xlsx", source.parent / "charts/one-off.xlsx"]:
        assert report.main(["render", str(source), "--to", "xlsx", "--out", str(destination)]) == 1
    assert public_bytes(source.parent) == before


@pytest.mark.parametrize("interrupted", [False, True])
def test_failed_explicit_export_preserves_existing_destination_and_source(report, tmp_path, monkeypatch, interrupted):
    root = tmp_path / "out"
    assert build(report, root) == 0
    source = current(root)
    before = public_bytes(source.parent)
    destination = tmp_path / "export.xlsx"
    destination.write_bytes(b"keep previous export")

    def fail(document, path, target, out_path, tenant):
        out_path.write_bytes(b"partial export")
        if interrupted:
            raise KeyboardInterrupt
        raise RuntimeError("controlled export failure")

    monkeypatch.setattr(report, "render", fail)
    with pytest.raises(KeyboardInterrupt if interrupted else RuntimeError):
        report.main(["render", str(source), "--to", "xlsx", "--out", str(destination)])
    assert destination.read_bytes() == b"keep previous export"
    assert public_bytes(source.parent) == before


def test_legacy_import_renders_requested_formats_without_adopting_old_filename_only_ownership(report, tmp_path):
    root = tmp_path / "out"
    assert build(report, root) == 0
    generated = current(root)
    legacy = tmp_path / "imported"
    import shutil

    shutil.copytree(generated.parent, legacy)
    (legacy / "renders.json").write_text(json.dumps({"version": 1, "base": "out", "files": ["out.docx", "out.xlsx"]}), encoding="utf-8")
    (legacy / "out.docx").write_bytes(b"old unknown format")
    before = public_bytes(legacy)
    source = legacy / generated.name
    assert report.main(["render", str(source), "--to", "xlsx", "--bundle-id", "verified"]) == 0
    published = legacy / "drafts/verified/out.report.json"
    assert_complete(published)
    assert not (published.parent / "out.docx").exists()
    assert {name: (legacy / name).read_bytes() for name in before} == before


def test_interrupted_process_leaves_no_partial_bundle_in_delivery_scans(report, tmp_path):
    import multiprocessing

    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("Process interruption fixture needs POSIX fork")
    from deerflow.workspace_changes.scanner import scan_workspace_roots
    from deerflow.workspace_changes.types import WorkspaceRoot

    root = tmp_path / "out"
    assert build(report, root) == 0
    before = public_bytes(root)
    context = multiprocessing.get_context("fork")
    ready, hold = context.Event(), context.Event()

    def command():
        original = report.render

        def paused(document, path, target, out_path, tenant):
            if target == "xlsx":
                (path.parent / "out.xlsx").write_bytes(b"partial private bytes")
                ready.set()
                if not hold.wait(20):
                    raise RuntimeError("test synchronization expired")
            return original(document, path, target, out_path, tenant)

        report.render = paused
        build(report, root, "interrupted")

    process = context.Process(target=command)
    process.start()
    try:
        assert ready.wait(15), "renderer did not reach the controlled boundary"
        process.terminate()
        process.join(10)
        assert process.exitcode is not None
        assert public_bytes(root) == before
        snapshot = scan_workspace_roots([WorkspaceRoot("outputs", root, "/mnt/user-data/outputs")], include_text=False)
        assert not snapshot.truncated
        assert all(".cache" not in path and "interrupted" not in path for path in snapshot.files)
        # A dead writer releases the process lock; its abandoned private stage
        # does not prevent the next complete publication.
        assert build(report, root, "after-interruption") == 0
        assert_complete(current(root))
    finally:
        if process.is_alive():
            process.kill()
        process.join(10)


def test_concurrent_publishers_compare_the_exact_source_and_only_one_advances(report, tmp_path):
    import multiprocessing

    if "fork" not in multiprocessing.get_all_start_methods():
        pytest.skip("Concurrent publication fixture needs POSIX fork")
    root = tmp_path / "out"
    assert build(report, root) == 0
    original_path = current(root)
    before = public_bytes(original_path.parent)
    context = multiprocessing.get_context("fork")
    proceed = context.Event()
    ready = [context.Event(), context.Event()]
    results = context.Queue()

    def command(index):
        original = report.render

        def paused(document, path, target, out_path, tenant):
            ready[index].set()
            if not proceed.wait(20):
                raise RuntimeError("test synchronization expired")
            return original(document, path, target, out_path, tenant)

        report.render = paused
        code = report.main(["prose", str(original_path), "--summary", f"Writer {'one' if index == 0 else 'two'}.", "--render", "xlsx", "--bundle-id", f"writer-{index}"])
        results.put(code)

    processes = [context.Process(target=command, args=(index,)) for index in range(2)]
    for process in processes:
        process.start()
    try:
        assert all(event.wait(15) for event in ready), "the publication lock must not cover rendering"
        proceed.set()
        codes = sorted(results.get(timeout=15) for _ in processes)
        assert codes == [0, 1]
        for process in processes:
            process.join(10)
            assert process.exitcode == 0
        assert len(list((root / "drafts").iterdir())) == 2
        assert public_bytes(original_path.parent) == before
        assert_complete(current(root))
    finally:
        proceed.set()
        for process in processes:
            if process.is_alive():
                process.kill()
            process.join(10)


@pytest.mark.parametrize("profile", ["restricted", "slim"])
def test_sandbox_smoke_command_checks_the_pdf_that_its_build_publishes(report, tmp_path, profile):
    import re
    import shlex

    pytest.importorskip("weasyprint")
    workflow = (ROOT / ".github/workflows/sandbox-image-smoke.yml").read_text(encoding="utf-8")
    payloads = [json.loads(match) for match in re.findall(r"--data '(\{[^\n]+\})'", workflow) if "R=/mnt/smoke/business-report/scripts/report.py;" in match]
    output = "/tmp/smoke-report" if profile == "restricted" else "/tmp/slim-report"
    command = next(payload["command"] for payload in payloads if "report.py;" in payload["command"] and f"--out {output} " in payload["command"])
    parts = command.split(";")
    build = next(shlex.split(part) for part in parts if " $R build " in part)[2:]
    fixture = Path(__file__).parent / "fixtures/example_services_export_small.xls"
    destination = tmp_path / Path(output).name
    build[build.index("build") + 1] = str(fixture)
    build[build.index("--out") + 1] = str(destination)
    assert report.main(build) == 0
    check = next(shlex.split(part) for part in parts if "check_pdf_on_image.py " in part)
    checked_pdf = Path(check[-1].replace(output, str(destination), 1))
    assert checked_pdf.is_file(), f"smoke checks {checked_pdf}, but build published {list(destination.rglob('*.pdf'))}"
    from pypdf import PdfReader

    assert "Business Review" in PdfReader(checked_pdf).pages[0].extract_text()
