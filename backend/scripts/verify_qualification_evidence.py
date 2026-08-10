"""Verify one externally supplied Kubernetes qualification evidence artifact."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from deerflow.qualification_evidence import (
    ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2,
    MAX_QUALIFICATION_EVIDENCE_BYTES,
    QUALIFICATION_VERIFICATION_API_VERSION,
    AcceptedSkillQualificationExpectationV2,
    QualificationEvidenceExpectation,
    QualificationVerificationError,
    verify_qualification_evidence,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=("Verify an operator-supplied qualification artifact against an independently supplied digest and exact deployment subjects."))
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--declared-digest", required=True)
    parser.add_argument("--qualification-id", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--provisioner-image-digest")
    parser.add_argument("--verifier-image-digest")
    parser.add_argument("--sandbox-image-digest")
    parser.add_argument("--chart-version", required=True)
    parser.add_argument("--chart-digest", required=True)
    parser.add_argument("--configuration-digest", required=True)
    parser.add_argument("--migration-head", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument(
        "--required-scenario",
        action="append",
        required=True,
        dest="required_scenarios",
    )
    return parser


def _read_bounded(path: Path) -> bytes:
    with path.open("rb") as artifact:
        payload = artifact.read(MAX_QUALIFICATION_EVIDENCE_BYTES + 1)
    if len(payload) > MAX_QUALIFICATION_EVIDENCE_BYTES:
        raise QualificationVerificationError("artifact_unreadable")
    return payload


def _failure(code: str) -> dict[str, str]:
    return {
        "api_version": QUALIFICATION_VERIFICATION_API_VERSION,
        "kind": "qualification.verification",
        "status": "failed",
        "code": code,
    }


def main(argv: list[str] | None = None) -> int:
    """Return zero only when digest, exact subjects, and scenarios verify."""

    args = _parser().parse_args(argv)
    try:
        if args.scope == ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2:
            if not all(
                (
                    args.provisioner_image_digest,
                    args.verifier_image_digest,
                    args.sandbox_image_digest,
                )
            ):
                raise ValueError(
                    "accepted-skill verification requires every runtime image digest",
                )
            expected = AcceptedSkillQualificationExpectationV2(
                qualification_id=args.qualification_id,
                gateway_image_digest=args.image_digest,
                provisioner_image_digest=args.provisioner_image_digest,
                verifier_image_digest=args.verifier_image_digest,
                sandbox_image_digest=args.sandbox_image_digest,
                chart_version=args.chart_version,
                chart_digest=args.chart_digest,
                configuration_digest=args.configuration_digest,
                migration_head=args.migration_head,
                scope=args.scope,
                namespace=args.namespace,
                required_scenarios=tuple(args.required_scenarios),
            )
        else:
            expected = QualificationEvidenceExpectation(
                qualification_id=args.qualification_id,
                image_digest=args.image_digest,
                chart_version=args.chart_version,
                chart_digest=args.chart_digest,
                configuration_digest=args.configuration_digest,
                migration_head=args.migration_head,
                scope=args.scope,
                namespace=args.namespace,
                required_scenarios=tuple(args.required_scenarios),
            )
        result = verify_qualification_evidence(
            _read_bounded(args.artifact),
            declared_digest=args.declared_digest,
            expected=expected,
        ).to_dict()
    except QualificationVerificationError as exc:
        result = _failure(exc.code)
        return_code = 1
    except (OSError, TypeError, ValueError):
        result = _failure("invalid_expectation_or_artifact")
        return_code = 1
    else:
        return_code = 0
    sys.stdout.write(json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n")
    return return_code


if __name__ == "__main__":
    raise SystemExit(main())
