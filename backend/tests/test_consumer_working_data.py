"""Execute two independent consumer packages in fresh processes on retained files.

Ordinary disk fixtures establish consumer behavior, not native quota/containment.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize("consumer,field,flag,source", [("procedure-summary", "include_caveats", "--include-caveats", "procedure.json"), ("supplier-comparison", "show_delivery", "--show-delivery", "quotes.json")])
def test_fresh_processes_use_retained_settings_and_temporary_overrides(tmp_path, consumer, field, flag, source):
    scripts = ROOT / "examples/skills" / consumer / "scripts"
    path = tmp_path / "home" / "workflow-data" / consumer / "settings.json"
    payload = tmp_path / "requested.json"
    payload.write_text(json.dumps({"version": 1, field: False}), encoding="utf-8")

    def run(script, *args, success=True):
        process = subprocess.run([sys.executable, "-B", str(scripts / script), *map(str, args)], capture_output=True, text=True, timeout=30)
        if success:
            assert process.returncode == 0, process.stderr
            return json.loads(process.stdout)
        assert process.returncode != 0
        return process.stderr

    first = run("working_data.py", "save", path, "--from", payload, "--expected", "missing", "--operation", "save-1")
    original = path.read_bytes()
    args = ["--input", ROOT / "examples/skills/inputs" / source, "--output", tmp_path / "home/outputs", "--settings", path]
    temporary = run("build.py", *args, flag, "yes")
    later = run("build.py", *args)
    assert temporary["effective"][field] is True and later["effective"][field] is False
    assert path.read_bytes() == original
    assert temporary["settings_used"]["revision"] == later["settings_used"]["revision"] == first["revision"]
    for result in (temporary, later):
        assert result["files"]
        for filename in result["files"]:
            assert (tmp_path / "home/outputs" / filename).is_file()

    def view(result):
        name = next(name for name in result["files"] if name.endswith(".view.json"))
        return json.loads((tmp_path / "home/outputs" / name).read_text(encoding="utf-8"))

    if consumer == "procedure-summary":
        assert any(block.get("heading") == "Supplied caveats" for block in view(temporary)["blocks"])
        assert not any(block.get("heading") == "Supplied caveats" for block in view(later)["blocks"])
    else:

        def columns(result):
            return next(block["columns"] for block in view(result)["blocks"] if block["type"] == "table")

        assert {"label": "Delivery"} in columns(temporary)
        assert {"label": "Delivery"} not in columns(later)
    reset = run("working_data.py", "reset", path, "--expected", first["revision"], "--operation", "reset-1")
    assert reset["settings"] == {"version": 1} and reset["revision"] != first["revision"]
    assert "revision conflict" in run("working_data.py", "save", path, "--from", payload, "--expected", first["revision"], "--operation", "late", success=False)
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"version": 1, field: "yes"}), encoding="utf-8")
    current = path.read_bytes()
    run("working_data.py", "save", path, "--from", bad, "--expected", reset["revision"], "--operation", "invalid", success=False)
    assert path.read_bytes() == current


@pytest.mark.parametrize("consumer,flag,label", [("procedure-summary", "include_caveats", "heading"), ("supplier-comparison", "show_delivery", "project")])
def test_each_consumer_mutation_contract(tmp_path, monkeypatch, consumer, flag, label):
    import importlib.util

    script = ROOT / "examples/skills" / consumer / "scripts/working_data.py"
    spec = importlib.util.spec_from_file_location("working_data_contract", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    path = tmp_path / "settings.json"
    first = module.mutate(path, "save", {"version": 1, flag: False, label: "Original"}, expected="missing", operation="one")
    patch = {label: "Updated"}
    second = module.mutate(path, "patch", patch, expected=first["revision"], operation="two")
    assert second["settings"][flag] is False
    assert module.mutate(path, "patch", patch, expected=first["revision"], operation="two") == second
    with pytest.raises(module.SettingsError):
        module.mutate(path, "patch", {label: "Other"}, expected=first["revision"], operation="two")
    third = module.mutate(path, "patch", {label: None}, expected=second["revision"], operation="three")
    assert label not in third["settings"] and third["settings"][flag] is False
    original = path.read_bytes()
    replace = module.os.replace
    monkeypatch.setattr(module.os, "replace", lambda *_: (_ for _ in ()).throw(OSError("full disk")))
    with pytest.raises(module.SettingsError, match="before publication"):
        module.mutate(path, "reset", {}, expected=third["revision"], operation="four")
    assert path.read_bytes() == original
    monkeypatch.setattr(module.os, "replace", replace)
    flush = module._flush_directory
    replaced = False

    def observed_replace(*args):
        nonlocal replaced
        result = replace(*args)
        replaced = True
        return result

    monkeypatch.setattr(module.os, "replace", observed_replace)

    def fail_after_replace(*args):
        if replaced:
            raise OSError("lost reply")
        return flush(*args)

    monkeypatch.setattr(module, "_flush_directory", fail_after_replace)
    with pytest.raises(module.SettingsError, match="uncertain"):
        module.mutate(path, "reset", {}, expected=third["revision"], operation="four")
    monkeypatch.setattr(module, "_flush_directory", flush)
    reset = module.mutate(path, "reset", {}, expected=third["revision"], operation="four")
    assert reset["settings"] == {"version": 1} and reset["revision"] != third["revision"]
    jobs = []
    for index in range(2):
        payload = tmp_path / f"writer-{index}.json"
        payload.write_text(json.dumps({label: str(index)}), encoding="utf-8")
        jobs.append(subprocess.Popen([sys.executable, "-B", str(script), "patch", str(path), "--from", str(payload), "--expected", reset["revision"], "--operation", f"writer-{index}"], stdout=subprocess.PIPE, stderr=subprocess.PIPE))
    results = [job.communicate(timeout=15) for job in jobs]
    assert sorted(job.returncode for job in jobs) == [0, 1], results


@pytest.mark.parametrize("relative", ["skills/public/business-report/scripts/business_report_settings.py", "examples/skills/procedure-summary/scripts/working_data.py", "examples/skills/supplier-comparison/scripts/working_data.py"])
def test_first_save_flushes_physical_ancestry_and_retries_partial_creation(tmp_path, monkeypatch, relative):
    import importlib.util

    spec = importlib.util.spec_from_file_location("directory_durability", ROOT / relative)
    module = importlib.util.module_from_spec(spec)
    old = sys.dont_write_bytecode
    sys.dont_write_bytecode = True
    try:
        spec.loader.exec_module(module)
    finally:
        sys.dont_write_bytecode = old
    directory = tmp_path / "home/workflow-data/consumer"
    path = directory / "settings.json"
    original_flush = module._flush_directory
    seen = []

    def interrupted(directory):
        seen.append(directory)
        if directory == tmp_path / "home":
            raise OSError("Interrupted parent flush")
        original_flush(directory)

    monkeypatch.setattr(module, "_flush_directory", interrupted)
    with pytest.raises(module.SettingsError, match="before publication"):
        module.mutate(path, "save", {"version": 1}, expected="missing", operation="first")
    assert directory.is_dir() and not path.exists()
    seen.clear()

    def record(directory):
        seen.append(directory)
        original_flush(directory)

    monkeypatch.setattr(module, "_flush_directory", record)
    module.mutate(path, "save", {"version": 1}, expected="missing", operation="first")
    assert seen[:4] == [directory, directory.parent, directory.parent.parent, tmp_path]
    assert seen[-1] == directory
