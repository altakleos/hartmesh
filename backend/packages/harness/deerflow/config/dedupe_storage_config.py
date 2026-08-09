"""Backward-compatible configuration for keyed inbound receipt storage.

The field name predates leased receipts and remains wire-compatible. ``auto``
uses PostgreSQL for durable receipt rows when the application database is
PostgreSQL; ``memory`` retains explicitly best-effort local delivery.
"""

from __future__ import annotations

from enum import StrEnum

from pydantic import BaseModel, Field

from deerflow.config.reload_boundary import format_field_description


class DedupeStorageBackend(StrEnum):
    AUTO = "auto"
    MEMORY = "memory"
    POSTGRES = "postgres"


class DedupeStorageConfig(BaseModel):
    """Where keyed native-ingress receipt state lives."""

    backend: DedupeStorageBackend = Field(
        default=DedupeStorageBackend.AUTO,
        description=format_field_description(
            "dedupe_storage",
            field_doc=(
                "Storage backend for keyed native-ingress receipts. "
                "'auto' uses the PostgreSQL application database when available; "
                "otherwise delivery remains explicitly best_effort. "
                "'memory' forces best_effort process-local delivery. "
                "'postgres' selects leased durable receipt storage."
            ),
        ),
    )
