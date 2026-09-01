"""Verify one externally supplied Kubernetes qualification evidence artifact."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from deerflow.deployment.topology import MULTI_GATEWAY_QUALIFICATION_SCOPE
from deerflow.multi_gateway_qualification import (
    MultiGatewayQualificationExpectationV1,
    verify_multi_gateway_qualification_evidence,
)
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
    parser.add_argument("--frontend-image-digest")
    parser.add_argument("--nginx-image-digest")
    parser.add_argument("--provisioner-image-digest")
    parser.add_argument("--verifier-image-digest")
    parser.add_argument("--sandbox-image-digest")
    parser.add_argument("--postgres-image-digest")
    parser.add_argument("--redis-image-digest")
    parser.add_argument("--git-revision")
    parser.add_argument("--chart-version", required=True)
    parser.add_argument("--chart-digest", required=True)
    parser.add_argument("--configuration-digest", required=True)
    parser.add_argument("--migration-head", required=True)
    parser.add_argument("--scope", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--gateway-service-uid")
    parser.add_argument("--gateway-pod-0-uid")
    parser.add_argument("--gateway-pod-1-uid")
    parser.add_argument("--provisioner-pod-uid")
    parser.add_argument("--sandbox-pvc-uid")
    parser.add_argument("--tenant-public-ref")
    parser.add_argument("--tenant-digest")
    parser.add_argument("--database-schema-ref")
    parser.add_argument("--redis-namespace-digest")
    parser.add_argument("--redis-acl-proof-digest")
    parser.add_argument("--extension-artifact-digest")
    parser.add_argument("--extension-configuration-digest")
    parser.add_argument("--capability-manifest-digest")
    parser.add_argument("--topology-digest")
    parser.add_argument("--max-age-seconds", type=int, default=604800)
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
        if args.scope == MULTI_GATEWAY_QUALIFICATION_SCOPE:
            required = (
                args.provisioner_image_digest,
                args.sandbox_image_digest,
                args.frontend_image_digest,
                args.nginx_image_digest,
                args.postgres_image_digest,
                args.redis_image_digest,
                args.git_revision,
                args.tenant_public_ref,
                args.tenant_digest,
                args.database_schema_ref,
                args.redis_namespace_digest,
                args.redis_acl_proof_digest,
                args.gateway_service_uid,
                args.gateway_pod_0_uid,
                args.gateway_pod_1_uid,
                args.provisioner_pod_uid,
                args.sandbox_pvc_uid,
                args.extension_artifact_digest,
                args.extension_configuration_digest,
                args.capability_manifest_digest,
                args.topology_digest,
            )
            if not all(required):
                raise ValueError("multi-Gateway verification requires all exact topology subjects")
            expected = MultiGatewayQualificationExpectationV1(
                qualification_id=args.qualification_id,
                git_revision=args.git_revision,
                chart_version=args.chart_version,
                chart_digest=args.chart_digest,
                image_digests={
                    "gateway": args.image_digest,
                    "frontend": args.frontend_image_digest,
                    "nginx": args.nginx_image_digest,
                    "provisioner": args.provisioner_image_digest,
                    "sandbox": args.sandbox_image_digest,
                    "postgres": args.postgres_image_digest,
                    "redis": args.redis_image_digest,
                },
                configuration_digest=args.configuration_digest,
                migration_head=args.migration_head,
                tenant_public_ref=args.tenant_public_ref,
                tenant_digest=args.tenant_digest,
                namespace=args.namespace,
                kubernetes_refs={
                    "gateway_service_uid": args.gateway_service_uid,
                    "gateway_pod_0_uid": args.gateway_pod_0_uid,
                    "gateway_pod_1_uid": args.gateway_pod_1_uid,
                    "provisioner_pod_uid": args.provisioner_pod_uid,
                    "sandbox_pvc_uid": args.sandbox_pvc_uid,
                },
                database_schema_ref=args.database_schema_ref,
                redis_namespace_digest=args.redis_namespace_digest,
                redis_acl_proof_digest=args.redis_acl_proof_digest,
                extension_artifact_digest=args.extension_artifact_digest,
                extension_configuration_digest=(args.extension_configuration_digest),
                capability_manifest_digest=args.capability_manifest_digest,
                topology_digest=args.topology_digest,
                scope=args.scope,
                required_scenarios=tuple(args.required_scenarios),
                max_age_seconds=args.max_age_seconds,
            )
        elif args.scope == ACCEPTED_SKILL_QUALIFICATION_SCOPE_V2:
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
        payload = _read_bounded(args.artifact)
        if isinstance(expected, MultiGatewayQualificationExpectationV1):
            result = verify_multi_gateway_qualification_evidence(
                payload,
                declared_digest=args.declared_digest,
                expected=expected,
            ).to_dict()
        else:
            result = verify_qualification_evidence(
                payload,
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
