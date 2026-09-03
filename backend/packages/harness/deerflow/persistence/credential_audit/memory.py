"""Process-local bounded credential audit for non-durable development mode."""

from __future__ import annotations

import hashlib
import json
from collections import OrderedDict
from typing import Any

from deerflow.persistence.credential_audit.contract import (
    normalize_audit_timestamp,
    validate_credential_audit_fields,
)
from deerflow.runtime.tenant_identity import TenantReferenceV1


class InMemoryCredentialAuditRepository:
    """Bounded development adapter; durable profiles use the SQL repository."""

    def __init__(
        self,
        *,
        tenant: TenantReferenceV1,
        max_aggregates: int = 1024,
    ) -> None:
        if not isinstance(tenant, TenantReferenceV1):
            raise TypeError("tenant must be TenantReferenceV1")
        if type(max_aggregates) is not int or max_aggregates < 1:
            raise ValueError("max_aggregates must be positive")
        self._tenant = tenant
        self._maximum = max_aggregates
        self._observations: OrderedDict[str, dict[str, object]] = OrderedDict()

    async def record(self, **values: Any) -> None:
        validate_credential_audit_fields(
            method=values.get("method"),
            action=values.get("action"),
            credential_ref=values.get("credential_ref"),
            actor_digest=values.get("actor_digest"),
            authority_digest=values.get("authority_digest"),
            route_category=values.get("route_category"),
            reason_code=values.get("reason_code"),
        )
        occurred = normalize_audit_timestamp(values.get("occurred_at"))
        bucket = occurred.date().isoformat()
        safe = {
            key: values.get(key)
            for key in (
                "method",
                "action",
                "credential_ref",
                "actor_digest",
                "authority_digest",
                "route_category",
                "reason_code",
            )
        }
        key = hashlib.sha256(
            json.dumps(
                {
                    "version": 1,
                    "tenant_digest": self._tenant.digest,
                    "bucket": bucket,
                    **safe,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        existing = self._observations.get(key)
        if existing is None:
            self._observations[key] = {
                **safe,
                "first_occurred_at": occurred,
                "last_occurred_at": occurred,
                "event_count": 1,
            }
        else:
            existing["first_occurred_at"] = min(
                existing["first_occurred_at"],
                occurred,
            )
            existing["last_occurred_at"] = max(
                existing["last_occurred_at"],
                occurred,
            )
            existing["event_count"] = int(existing["event_count"]) + 1
            self._observations.move_to_end(key)
        while len(self._observations) > self._maximum:
            self._observations.popitem(last=False)

    async def list_recent(self, *, limit: int = 50) -> list[dict[str, object]]:
        if type(limit) is not int or limit < 1 or limit > 100:
            raise ValueError("audit observation limit must be between 1 and 100")
        return [dict(item) for item in reversed(tuple(self._observations.values()))][:limit]


__all__ = ["InMemoryCredentialAuditRepository"]
