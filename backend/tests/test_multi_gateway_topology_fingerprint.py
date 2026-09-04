from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from deerflow.deployment.topology import (
    DeploymentProfile,
    ReplicaRegistrationV1,
    TopologyFingerprintV1,
)


def _fingerprint(**overrides: object) -> TopologyFingerprintV1:
    values: dict[str, object] = {
        "profile": "durable_two_gateway_v1",
        "tenant_digest": "1" * 64,
        "image_digests": {
            "gateway": "sha256:" + ("2" * 64),
            "frontend": "sha256:" + ("3" * 64),
            "nginx": "sha256:" + ("4" * 64),
            "provisioner": "sha256:" + ("4" * 64),
            "sandbox": "sha256:" + ("3" * 64),
            "postgres": "sha256:" + ("b" * 64),
            "redis": "sha256:" + ("c" * 64),
        },
        "config_digest": "sha256:" + ("5" * 64),
        "database_schema_ref": "schema:sha256:" + ("6" * 64),
        "redis_namespace_digest": "sha256:" + ("7" * 64),
        "extension_artifact_digest": "sha256:" + ("8" * 64),
        "extension_configuration_digest": "sha256:" + ("9" * 64),
        "capability_manifest_digest": "a" * 64,
        "mcp_task_replay_keyring_confirmation_version": 1,
        "mcp_task_replay_keyring_confirmation_digest": "sha256:" + ("d" * 64),
        "execution_policy_keyring_confirmation_version": 1,
        "execution_policy_keyring_confirmation_digest": "sha256:" + ("e" * 64),
        "migration_head": "0030_run_delivery_owner_backfill",
        "accepted_materialization_profile": "rwx_verified_copy_v2",
    }
    values.update(overrides)
    return TopologyFingerprintV1.create(**values)


def test_fingerprint_is_canonical_complete_and_mapping_order_independent() -> None:
    first = _fingerprint()
    second = _fingerprint(
        image_digests={
            "redis": "sha256:" + ("c" * 64),
            "gateway": "sha256:" + ("2" * 64),
            "postgres": "sha256:" + ("b" * 64),
            "sandbox": "sha256:" + ("3" * 64),
            "provisioner": "sha256:" + ("4" * 64),
            "nginx": "sha256:" + ("4" * 64),
            "frontend": "sha256:" + ("3" * 64),
        }
    )

    assert first == second
    assert first.digest == "e0115ddcfb676b2aa61debb0fd1de42993da843e5dc3cef5c966f11e7721efba"
    assert first.to_dict() == {
        "version": 1,
        "profile": "durable_two_gateway_v1",
        "tenant_digest": "1" * 64,
        "image_digests": {
            "frontend": "sha256:" + ("3" * 64),
            "gateway": "sha256:" + ("2" * 64),
            "nginx": "sha256:" + ("4" * 64),
            "postgres": "sha256:" + ("b" * 64),
            "provisioner": "sha256:" + ("4" * 64),
            "redis": "sha256:" + ("c" * 64),
            "sandbox": "sha256:" + ("3" * 64),
        },
        "config_digest": "sha256:" + ("5" * 64),
        "database_schema_ref": "schema:sha256:" + ("6" * 64),
        "redis_namespace_digest": "sha256:" + ("7" * 64),
        "extension_artifact_digest": "sha256:" + ("8" * 64),
        "extension_configuration_digest": "sha256:" + ("9" * 64),
        "capability_manifest_digest": "a" * 64,
        "mcp_task_replay_keyring_confirmation_version": 1,
        "mcp_task_replay_keyring_confirmation_digest": "sha256:" + ("d" * 64),
        "execution_policy_keyring_confirmation_version": 1,
        "execution_policy_keyring_confirmation_digest": "sha256:" + ("e" * 64),
        "migration_head": "0030_run_delivery_owner_backfill",
        "accepted_materialization_profile": "rwx_verified_copy_v2",
        "digest": first.digest,
    }


def test_fingerprint_is_immutable_and_contains_no_raw_config_or_credentials() -> None:
    fingerprint = _fingerprint()

    with pytest.raises(TypeError):
        fingerprint.image_digests["gateway"] = "sha256:" + ("f" * 64)  # type: ignore[index]
    serialized = str(fingerprint.to_dict()).lower()
    assert "password" not in serialized
    assert "postgresql://" not in serialized
    assert "redis://" not in serialized

    with pytest.raises(ValueError, match="database_schema_ref"):
        _fingerprint(database_schema_ref="postgresql://user:password@db/tenant")


@pytest.mark.parametrize(
    "image_digests",
    [
        {
            "gateway": "sha256:" + ("1" * 64),
            "provisioner": "sha256:" + ("2" * 64),
            "sandbox": "sha256:" + ("3" * 64),
        },
        {
            "gateway": "sha256:" + ("1" * 64),
            "frontend": "sha256:" + ("2" * 64),
            "nginx": "sha256:" + ("3" * 64),
            "provisioner": "sha256:" + ("4" * 64),
            "sandbox": "sha256:" + ("5" * 64),
            "postgres": "sha256:" + ("6" * 64),
            "redis": "sha256:" + ("7" * 64),
            "unexpected": "sha256:" + ("8" * 64),
        },
    ],
)
def test_fingerprint_rejects_any_image_set_outside_exact_profile(
    image_digests: dict[str, str],
) -> None:
    with pytest.raises(ValueError, match="exact qualified image set"):
        _fingerprint(image_digests=image_digests)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("profile", "durable_production"),
        ("tenant_digest", "tenant-readable-name"),
        ("config_digest", "sha256:not-a-digest"),
        ("redis_namespace_digest", "7" * 64),
        ("capability_manifest_digest", "sha256:short"),
        ("mcp_task_replay_keyring_confirmation_version", 2),
        ("mcp_task_replay_keyring_confirmation_digest", "sha256:short"),
        ("execution_policy_keyring_confirmation_version", 2),
        ("execution_policy_keyring_confirmation_digest", "sha256:short"),
        ("migration_head", "../../secret"),
        ("accepted_materialization_profile", "disabled"),
    ],
)
def test_fingerprint_rejects_incomplete_or_unqualified_subjects(
    field: str,
    value: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        _fingerprint(**{field: value})


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (
            "mcp_task_replay_keyring_confirmation_digest",
            "sha256:" + ("e" * 64),
        ),
        ("mcp_task_replay_keyring_confirmation_version", 2),
    ],
)
def test_fingerprint_detects_mcp_replay_keyring_skew(
    field: str,
    value: object,
) -> None:
    baseline = _fingerprint()

    if field.endswith("version"):
        with pytest.raises(ValueError):
            _fingerprint(**{field: value})
        return

    skewed = _fingerprint(**{field: value})
    assert skewed.digest != baseline.digest


def test_fingerprint_detects_execution_policy_keyring_skew() -> None:
    baseline = _fingerprint()
    skewed = _fingerprint(
        execution_policy_keyring_confirmation_digest="sha256:" + ("f" * 64),
    )

    assert skewed.digest != baseline.digest


def test_fingerprint_reader_rejects_half_present_execution_policy_confirmation() -> None:
    payload = _fingerprint().to_dict()
    payload.pop("execution_policy_keyring_confirmation_digest")

    with pytest.raises(ValueError, match="fields are invalid"):
        TopologyFingerprintV1.from_dict(payload)


def test_fingerprint_reader_preserves_legacy_v1_without_keyring_confirmation() -> None:
    legacy = _fingerprint().to_dict()
    legacy.pop("mcp_task_replay_keyring_confirmation_version")
    legacy.pop("mcp_task_replay_keyring_confirmation_digest")
    legacy.pop("digest")
    legacy["digest"] = hashlib.sha256(
        json.dumps(
            legacy,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8"),
    ).hexdigest()

    parsed = TopologyFingerprintV1.from_dict(legacy)

    assert parsed.mcp_task_replay_keyring_confirmation_version is None
    assert parsed.mcp_task_replay_keyring_confirmation_digest is None
    assert parsed.to_dict() == legacy


def test_fingerprint_reader_rejects_half_present_keyring_confirmation() -> None:
    payload = _fingerprint().to_dict()
    payload.pop("mcp_task_replay_keyring_confirmation_digest")

    with pytest.raises(ValueError, match="fields are invalid"):
        TopologyFingerprintV1.from_dict(payload)


def test_replica_registration_is_bounded_aware_and_round_trips() -> None:
    started = datetime(2026, 9, 1, 12, 0, tzinfo=UTC)
    registration = ReplicaRegistrationV1(
        replica_id="gateway-a-7f9c",
        topology_fingerprint=_fingerprint(),
        started_at=started,
        heartbeat_at=started + timedelta(seconds=5),
    )

    assert ReplicaRegistrationV1.from_dict(registration.to_dict()) == registration

    with pytest.raises(ValueError, match="timezone-aware"):
        ReplicaRegistrationV1(
            replica_id="gateway-a",
            topology_fingerprint=_fingerprint(),
            started_at=started.replace(tzinfo=None),
            heartbeat_at=started,
        )
    with pytest.raises(ValueError, match="before started_at"):
        ReplicaRegistrationV1(
            replica_id="gateway-a",
            topology_fingerprint=_fingerprint(),
            started_at=started,
            heartbeat_at=started - timedelta(seconds=1),
        )


def test_central_profile_model_retains_existing_profiles_and_exact_new_one() -> None:
    assert tuple(item.value for item in DeploymentProfile) == (
        "local_development",
        "durable_production",
        "durable_two_gateway_v1",
    )
