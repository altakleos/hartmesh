"""Open a snapshot member by publication and serve only its verified bytes."""

from contextlib import nullcontext
from pathlib import Path

from fastapi import HTTPException, Request

from app.gateway.conversation_snapshots import _manifest_document, _open_shared
from app.gateway.deps import get_shared_publications_repo
from app.gateway.routers._file_http import DescriptorFileResponse
from app.gateway.routers.shared import _shared_mutation
from app.gateway.snapshot_manifest import member_record
from app.gateway.storage_features import current_feature_service, storage_feature
from deerflow.spaces.filesystem import UnsafeSpacePath
from deerflow.utils.file_io import await_drained, run_file_io


def _unavailable():
    return HTTPException(410, "This file is no longer available in this shared conversation.", headers={"Cache-Control": "private, no-store"})


class SnapshotMemberResponse(DescriptorFileResponse):
    def __init__(self, request: Request, snapshot_id: str, member_id: str, download: bool):
        super().__init__(Path(member_id), download=download)
        self.headers["cache-control"] = "private, no-store"
        self.request = request
        self.snapshot_id = snapshot_id
        self.member_id = member_id

    def _open_descriptor(self):
        return _open_shared(str(self.path))

    def capture(self, row):
        # The manifest bounds the size. The descriptor response captures the
        # file once, then serves headers and every range from that capture.
        self.path = Path(row["path"])
        self._hash_max_bytes = row["size"]
        try:
            self._prepare()
        except HTTPException as exc:
            if exc.status_code not in (400, 404, 409):
                raise
            raise _unavailable() from None
        except (NotADirectoryError, UnsafeSpacePath):
            raise _unavailable() from None
        if self._content is None or len(self._content) != row["size"] or self.headers["etag"] != f'"{row["sha256"]}"':
            raise _unavailable()

    async def _prepare_owned(self):
        admit = _download_member if self._download else _open_member
        await admit(request=self.request, response=self)


async def _capture_member(request: Request, response: SnapshotMemberResponse):
    repo = get_shared_publications_repo(request)
    # Managed reads already hold the Shared workflow lock. Ordinary reads
    # settle removals and hold the host lock through identity verification.
    managed = current_feature_service(request, "hm.shared")
    async with nullcontext() if managed else _shared_mutation(repo):
        manifest = await repo.publication(response.snapshot_id)
        member = await repo.publication(response.member_id)
        if not manifest or manifest["removed_at"] is not None or not member or member["removed_at"] is not None:
            raise _unavailable()
        doc = await run_file_io(_manifest_document, manifest)
        if not doc or not any(member_record(manifest, entry, member) for entry in (doc.transcript, *doc.files)):
            raise _unavailable()
        await await_drained(run_file_io(response.capture, member))
        current = await repo.live_publications(paths=[manifest["path"], member["path"]])
        if any(current.get(row["path"], {}).get("publication_id") != row["publication_id"] for row in (manifest, member)):
            raise _unavailable()


_open_member = storage_feature("hm.shared")(_capture_member)
_download_member = storage_feature("hm.shared", export=True)(_capture_member)
