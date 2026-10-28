"""Cold SDK MIME preparation and multipart streaming retain off-loop I/O."""

import mimetypes

import pytest
from test_telegram_sdk_upload_custody import test_real_sdk_upload_drops_leased_bytes_even_when_pool_retains_failed_response as exercise_upload


@pytest.mark.asyncio
async def test_cold_mime_sdk_upload_preparation_owns_its_io(monkeypatch):
    # A separately configured Gateway describes files in another process. Its
    # HEAD does not initialize this sender's standard-library MIME database.
    monkeypatch.setattr(mimetypes, "_db", None)
    await exercise_upload(False)
