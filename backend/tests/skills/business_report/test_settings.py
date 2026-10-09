"""Consumer preferences: real files, retries, concurrent processes and CLI builds."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

SCRIPTS = Path(__file__).resolve().parents[4] / "skills/public/business-report/scripts"


@pytest.fixture
def settings():
    spec = importlib.util.spec_from_file_location("report_settings_test", SCRIPTS / "business_report_settings.py")
    module = importlib.util.module_from_spec(spec)
    old = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = old
    return module


def test_legacy_read_preserves_exact_bytes(settings, tmp_path):
    path = tmp_path / "preferences.json"
    raw = b'{"version": 1, "summary_length": "short", "brand": {"primary":"#012345"}}\n'
    path.write_bytes(raw)
    result = settings.read(path)
    assert result["preferences"]["summary_length"] == "short"
    assert result["revision"].startswith("legacy:")
    assert path.read_bytes() == raw


@pytest.mark.parametrize(
    "raw",
    [
        b'{"version":2}',
        b'{"version":true}',
        b'{"version":1,"summary_length":"short","currency":"bad"}',
        b'{"version":1,"brand":{"primary":"red"}}',
        b'{"version":1,"exclusions":[{"role":"category"}]}',
        b'{"version":1,"comparisons":["made_up"]}',
        b'{"version":1,"currency":"EUR","currency":"USD"}',
        b'{"version":1,"extra":true}',
        b'{"version":1,"charts":[NaN]}',
        b"[" * 1000 + b"]" * 1000,
        b" " * 65537,
        b"\xff",
    ],
)
def test_invalid_document_applies_nothing_and_preserves_bytes(settings, tmp_path, raw):
    path = tmp_path / "preferences.json"
    path.write_bytes(raw)
    with pytest.raises(settings.SettingsError):
        settings.read(path)
    assert path.read_bytes() == raw


def test_revision_reset_and_exact_retry(settings, tmp_path):
    path = tmp_path / "preferences.json"
    first = settings.mutate(path, "save", {"version": 1, "summary_length": "short", "currency": "USD"}, expected="missing", operation="save-1")
    request = {"summary_length": "standard"}
    second = settings.mutate(path, "patch", request, expected=first["revision"], operation="patch-1")
    assert second["preferences"]["currency"] == "USD"
    assert settings.mutate(path, "patch", request, expected=first["revision"], operation="patch-1") == second
    with pytest.raises(settings.SettingsError):
        settings.mutate(path, "patch", {"summary_length": "short"}, expected=first["revision"], operation="patch-1")
    reset = settings.mutate(path, "reset", {}, expected=second["revision"], operation="reset-1")
    assert reset["preferences"] == {"version": 1}
    assert reset["revision"] != first["revision"]
    with pytest.raises(settings.SettingsError):
        settings.mutate(path, "patch", request, expected=first["revision"], operation="stale")
    with pytest.raises(settings.SettingsError):
        settings.mutate(path, "save", {"version": 1}, expected="missing", operation="recreate")


def test_patch_removes_one_field_and_replaces_collections(settings, tmp_path):
    path = tmp_path / "preferences.json"
    first = settings.mutate(path, "save", {"version": 1, "currency": "EUR", "brand": {"primary": "#123456"}, "exclusions": ["category=Warranty"]}, expected="missing", operation="one")
    second = settings.mutate(path, "patch", {"currency": None, "exclusions": []}, expected=first["revision"], operation="two")
    assert second["preferences"] == {"version": 1, "brand": {"primary": "#123456"}, "exclusions": []}


def test_fail_before_replace_preserves_old_and_after_replace_reconciles(settings, tmp_path, monkeypatch):
    path = tmp_path / "preferences.json"
    first = settings.mutate(path, "save", {"version": 1}, expected="missing", operation="one")
    old = path.read_bytes()
    replace = settings.os.replace
    monkeypatch.setattr(settings.os, "replace", lambda *a: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(settings.SettingsError, match="before publication"):
        settings.mutate(path, "patch", {"currency": "EUR"}, expected=first["revision"], operation="two")
    assert path.read_bytes() == old
    monkeypatch.setattr(settings.os, "replace", replace)
    flush = settings._flush_directory
    replaced = False

    def observed_replace(*args):
        nonlocal replaced
        result = replace(*args)
        replaced = True
        return result

    monkeypatch.setattr(settings.os, "replace", observed_replace)

    def fail_after_replace(*args):
        if replaced:
            raise OSError("lost acknowledgment")
        return flush(*args)

    monkeypatch.setattr(settings, "_flush_directory", fail_after_replace)
    with pytest.raises(settings.SettingsError, match="uncertain"):
        settings.mutate(path, "patch", {"currency": "EUR"}, expected=first["revision"], operation="two")
    assert path.read_bytes() != old
    monkeypatch.setattr(settings, "_flush_directory", flush)
    result = settings.mutate(path, "patch", {"currency": "EUR"}, expected=first["revision"], operation="two")
    assert result["preferences"]["currency"] == "EUR"
    assert result == settings.read(path)


def test_same_content_has_new_revision_and_stable_lock(settings, tmp_path):
    path = tmp_path / "preferences.json"
    first = settings.mutate(path, "save", {"version": 1}, expected="missing", operation="one")
    inode = path.with_name(path.name + ".lock").stat().st_ino
    second = settings.mutate(path, "save", {"version": 1}, expected=first["revision"], operation="two")
    assert first["revision"] != second["revision"]
    assert path.with_name(path.name + ".lock").stat().st_ino == inode


def test_concurrent_processes_cannot_both_replace_same_revision(settings, tmp_path):
    import subprocess

    path = tmp_path / "preferences.json"
    first = settings.mutate(path, "save", {"version": 1}, expected="missing", operation="seed")
    jobs = []
    for index, currency in enumerate(("EUR", "USD")):
        payload = tmp_path / f"patch-{index}.json"
        payload.write_text(json.dumps({"currency": currency}), encoding="utf-8")
        jobs.append(
            subprocess.Popen(
                [sys.executable, "-B", str(SCRIPTS / "business_report_settings.py"), "patch", str(path), "--from", str(payload), "--expected", first["revision"], "--operation", f"writer-{index}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )
    results = [job.communicate(timeout=15) for job in jobs]
    assert sorted(job.returncode for job in jobs) == [0, 1], results
    assert any(b"revision conflict" in stderr for _, stderr in results)


def test_build_temporary_override_retains_saved_bytes_and_next_build_defaults(settings, tmp_path, capsys):
    from .test_report import LARGE_CSV, _load_script, _read_report, _report_path

    report = _load_script()
    path = tmp_path / "preferences.json"
    saved = settings.mutate(path, "save", {"version": 1, "summary_length": "short", "exclusions": ["category=Warranty"]}, expected="missing", operation="save")
    raw = path.read_bytes()
    override = tmp_path / "override.json"
    override.write_text(json.dumps({"version": 1, "summary_length": "standard", "exclusions": []}), encoding="utf-8")
    for name, extra in (("temporary", ["--prefs-override", str(override)]), ("later", [])):
        assert report.main(["build", str(LARGE_CSV), "--period", "2026-08", "--out", str(tmp_path / name), "--prefs", str(path), *extra]) == 0
        capsys.readouterr()
        assert path.read_bytes() == raw
    temporary = _read_report(tmp_path / "temporary")
    later = _read_report(tmp_path / "later")
    assert temporary["kpis"][1]["value"] > later["kpis"][1]["value"]
    evidence = json.loads((_report_path(tmp_path / "temporary").parent / "preferences-used.json").read_text(encoding="utf-8"))
    assert evidence["revision"] == saved["revision"]
    assert evidence["effective"]["summary_length"] == "standard"
    assert evidence["preferences"]["summary_length"] == "short"
    later_evidence = json.loads((_report_path(tmp_path / "later").parent / "preferences-used.json").read_text(encoding="utf-8"))
    assert later_evidence["effective"]["summary_length"] == "short"


def test_unsupported_chart_is_rejected(settings):
    with pytest.raises(settings.SettingsError):
        settings.validate({"version": 1, "charts": ["invented_chart"]})


def test_override_only_evidence_survives_prose_and_render(tmp_path, capsys):
    from .test_report import LARGE_CSV, _load_script, _report_path

    report = _load_script()
    override = tmp_path / "one-report.json"
    override.write_text('{"version":1,"summary_length":"short","charts":[]}', encoding="utf-8")
    out = tmp_path / "report"
    assert report.main(["build", str(LARGE_CSV), "--period", "2026-08", "--out", str(out), "--prefs-override", str(override)]) == 0
    original = (_report_path(out).parent / "preferences-used.json").read_bytes()
    assert json.loads(original)["override"]["preferences"]["charts"] == []
    override.write_text('{"version":1}', encoding="utf-8")
    assert report.main(["prose", str(_report_path(out)), "--summary", "Prepared from supplied records."]) == 0
    assert (_report_path(out).parent / "preferences-used.json").read_bytes() == original
    assert report.main(["render", str(_report_path(out)), "--to", "html"]) == 0
    assert (_report_path(out).parent / "preferences-used.json").read_bytes() == original
