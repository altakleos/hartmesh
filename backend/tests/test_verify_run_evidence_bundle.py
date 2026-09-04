from __future__ import annotations

import hashlib
import io
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest
from test_run_evidence_archive import _snapshot

from app.gateway.artifact_archive import build_run_evidence_archive
from deerflow.runtime.run_evidence import (
    BUNDLE_REFERENCE_DOMAIN,
    EVIDENCE_ROOT_DOMAIN,
    MANIFEST_DIGEST_DOMAIN,
    RUN_EVIDENCE_MANIFEST_PATH,
    canonical_json_bytes,
)

_SCRIPT = Path(__file__).parents[2] / "scripts" / "verify_run_evidence_bundle.py"


def _valid_bundle(tmp_path: Path) -> bytes:
    outputs = tmp_path / "outputs"
    outputs.mkdir()
    (outputs / "report.txt").write_bytes(b"evidence artifact")
    path = "/mnt/user-data/outputs/report.txt"
    result = build_run_evidence_archive(
        outputs,
        [path],
        snapshot=_snapshot(paths=(path,)),
        user_data_dir=outputs.parent,
    )
    with result.file:
        return result.file.read()


def _rewrite_bundle(
    original: bytes,
    transform,
    *,
    extra: tuple[tuple[str, bytes, int], ...] = (),
) -> bytes:
    source = zipfile.ZipFile(io.BytesIO(original))
    output = io.BytesIO()
    with source, zipfile.ZipFile(output, "w", allowZip64=False) as target:
        for info in source.infolist():
            name, body, compression = transform(
                info.filename,
                source.read(info),
                info.compress_type,
            )
            target.writestr(name, body, compress_type=compression)
        for name, body, compression in extra:
            target.writestr(name, body, compress_type=compression)
    return output.getvalue()


def _run_verifier(tmp_path: Path, bundle: bytes) -> subprocess.CompletedProcess[str]:
    path = tmp_path / "bundle.zip"
    path.write_bytes(bundle)
    env = {"PATH": os.environ.get("PATH", "")}
    return subprocess.run(
        [sys.executable, "-I", str(_SCRIPT), str(path)],
        text=True,
        capture_output=True,
        check=False,
        env=env,
    )


def _redigest(document: dict[str, object]) -> bytes:
    document["manifest_digest"] = None
    bundle_projection = dict(document)
    bundle_projection.pop("bundle_ref")
    document["bundle_ref"] = (
        "bundle-"
        + hashlib.sha256(
            BUNDLE_REFERENCE_DOMAIN + canonical_json_bytes(bundle_projection),
        ).hexdigest()[:24]
    )
    document["manifest_digest"] = hashlib.sha256(
        MANIFEST_DIGEST_DOMAIN + canonical_json_bytes(document),
    ).hexdigest()
    return canonical_json_bytes(document)


def test_offline_verifier_accepts_valid_bundle_and_disclaims_authenticity(tmp_path) -> None:
    result = _run_verifier(tmp_path, _valid_bundle(tmp_path))

    assert result.returncode == 0, result.stderr
    output = json.loads(result.stdout)
    assert output["status"] == "valid"
    assert output["authenticity"] == "not_signed"
    assert output["artifact_count"] == 1
    assert output["bundle_ref"].startswith("bundle-")


@pytest.mark.parametrize(
    ("mutation", "expected_code"),
    [
        ("artifact", "artifact_digest_invalid"),
        ("manifest", "manifest_digest_invalid"),
        ("unsupported", "manifest_version_unsupported"),
        ("boolean_version", "manifest_version_unsupported"),
        ("event_count_bound", "bundle_limit_exceeded"),
        ("missing_completeness", "manifest_fields_invalid"),
        ("timestamp", "manifest_fields_invalid"),
        ("section_crosslink", "evidence_cross_link_invalid"),
        ("extra_section_anchor", "evidence_cross_link_invalid"),
        ("dangling_evidence_link", "evidence_cross_link_invalid"),
        ("public_reference_kind_swap", "manifest_fields_invalid"),
    ],
)
def test_offline_verifier_rejects_tampering_and_unsupported_contracts(
    tmp_path,
    mutation: str,
    expected_code: str,
) -> None:
    original = _valid_bundle(tmp_path)

    def transform(name: str, body: bytes, compression: int):
        if mutation == "artifact" and name == "report.txt":
            body = b"tampered artifact"
        if name == RUN_EVIDENCE_MANIFEST_PATH and mutation != "artifact":
            document = json.loads(body)
            if mutation == "manifest":
                document["terminal"]["stop_reason"] = "cancelled"
            elif mutation == "unsupported":
                document["schema_version"] = 2
            elif mutation == "boolean_version":
                document["schema_version"] = True
            elif mutation == "event_count_bound":
                document["lifecycle"]["event_count"] = 100_001
                document["lifecycle"]["safe_counts"] = {
                    "lifecycle": 100_001,
                }
            elif mutation == "missing_completeness":
                del document["completeness"]
            elif mutation == "timestamp":
                document["terminal"]["accepted_at"] = "2026-09-04T12:00:00+00:00"
            elif mutation == "section_crosslink":
                document["admission"]["subagent_catalog_digest"] = "1" * 64
            elif mutation == "extra_section_anchor":
                section = next(item for item in document["evidence_sections"] if item["name"] == "accepted_invocation")
                section["references"] = sorted([*section["references"], "f" * 64])
                section["item_count"] = len(section["references"])
                section["root_digest"] = hashlib.sha256(
                    EVIDENCE_ROOT_DOMAIN
                    + canonical_json_bytes(
                        {
                            "section": "accepted_invocation",
                            "references": section["references"],
                        }
                    )
                ).hexdigest()
            elif mutation == "dangling_evidence_link":
                document["evidence_links"] = [
                    {
                        "kind": "mcp_task_to_tool_receipt",
                        "subject_section": "mcp_tasks",
                        "subject_digest": "a" * 64,
                        "object_section": "tool_receipts",
                        "object_digest": "b" * 64,
                    }
                ]
            elif mutation == "public_reference_kind_swap":
                document["run_ref"], document["thread_ref"] = (
                    document["thread_ref"],
                    document["run_ref"],
                )
            body = (
                _redigest(document)
                if mutation
                in {
                    "boolean_version",
                    "event_count_bound",
                    "extra_section_anchor",
                    "dangling_evidence_link",
                    "public_reference_kind_swap",
                }
                else json.dumps(
                    document,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            )
        return name, body, compression

    result = _run_verifier(tmp_path, _rewrite_bundle(original, transform))

    assert result.returncode != 0
    assert json.loads(result.stdout)["code"] == expected_code


@pytest.mark.parametrize(
    ("name", "compression", "expected_code"),
    [
        ("undeclared.txt", zipfile.ZIP_STORED, "undeclared_entry"),
        ("../escape", zipfile.ZIP_STORED, "zip_path_unsafe"),
        ("compressed.txt", zipfile.ZIP_DEFLATED, "zip_compression_unsupported"),
    ],
)
def test_offline_verifier_rejects_undeclared_unsafe_and_compressed_entries(
    tmp_path,
    name: str,
    compression: int,
    expected_code: str,
) -> None:
    original = _valid_bundle(tmp_path)
    result = _run_verifier(
        tmp_path,
        _rewrite_bundle(
            original,
            lambda member, body, kind: (member, body, kind),
            extra=((name, b"x" * 1000, compression),),
        ),
    )

    assert result.returncode != 0
    assert json.loads(result.stdout)["code"] == expected_code


def test_offline_verifier_rejects_duplicate_manifest_path(tmp_path) -> None:
    original = _valid_bundle(tmp_path)
    with zipfile.ZipFile(io.BytesIO(original)) as archive:
        manifest = archive.read(RUN_EVIDENCE_MANIFEST_PATH)
    result = _run_verifier(
        tmp_path,
        _rewrite_bundle(
            original,
            lambda member, body, kind: (member, body, kind),
            extra=((RUN_EVIDENCE_MANIFEST_PATH, manifest, zipfile.ZIP_STORED),),
        ),
    )

    assert result.returncode != 0
    assert json.loads(result.stdout)["code"] == "zip_path_duplicate"
