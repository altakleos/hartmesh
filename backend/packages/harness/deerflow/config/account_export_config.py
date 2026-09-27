"""Limits for "Download all my data": one person's whole account as a prepared archive."""

from pydantic import BaseModel, Field

GIB = 1024 * 1024 * 1024


class AccountExportConfig(BaseModel):
    """Read when an export starts, so a change applies to the next one without a restart."""

    part_bytes: int = Field(
        default=2 * GIB,
        ge=64 * 1024 * 1024,
        description="The largest one archive part may grow before the next begins. An account larger than this downloads as numbered parts; a single file larger than this is a part of its own.",
    )
    min_free_bytes: int = Field(
        default=GIB,
        ge=0,
        description=(
            "Free space the data disk must keep while an export is prepared on it, after what the other exports being prepared still have to write. "
            "An export that would leave less is refused before it starts, or stopped and removed if the disk fills while it runs."
        ),
    )
    expires_after_seconds: int = Field(
        default=3600,
        ge=60,
        le=86400,
        description=(
            "How long a prepared export waits for its next download before it is deleted: counted from when it is ready and again from the end of each part's download, "
            "and never while a part is downloading. Once every part has been downloaded, it goes ten minutes later."
        ),
    )
    max_concurrent: int = Field(
        default=2,
        ge=1,
        le=8,
        description="How many exports this Gateway prepares at once. A person has one at a time; asking again returns the one in progress.",
    )
