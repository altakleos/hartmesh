"""Quota-counted ordinary uploads in a freshly admitted native User Space."""

import asyncio
import errno
import logging
from pathlib import Path

from fastapi import HTTPException

from app.gateway.chat_files import QualifiedUploadStage, files_full, files_unavailable
from deerflow.spaces.chat_paths import ChatFilesFull
from deerflow.spaces.filesystem import FilesystemUnavailable, SpaceFull, UnsafeSpacePath
from deerflow.utils.file_io import await_drained, run_file_io

logger = logging.getLogger(__name__)


class ChatUploadSession:
    """Own private stages through streaming, conversion and request settlement."""

    def __init__(self, owner, access):
        self.owner = owner
        self.access = access
        self.stages = []
        self.finalized = False
        self._gate = None
        self.relative = f"conversations/{owner._thread_id}/user-data/uploads"

    async def open(self):
        try:
            await self._open()
        except (ChatFilesFull, SpaceFull):
            raise files_full() from None
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise files_full() from None
            raise
        except FilesystemUnavailable:
            raise files_unavailable("space-unverified") from None
        except UnsafeSpacePath:
            raise HTTPException(400, "The uploads directory is not accessible within its qualified Space") from None

    async def _open(self):
        self.access = await self.access.refresh(self.owner._request)
        prepared = self.access.prepared
        if prepared.mounts.user_id != self.owner._user_id:
            raise files_unavailable("space-unverified")
        from deerflow.spaces.chat_gate import conversation_gate

        self._gate = conversation_gate(prepared.paths, self.owner._thread_id, user_id=self.owner._user_id)
        await self._gate.__aenter__()
        self.access = await self.access.refresh(self.owner._request)
        await await_drained(run_file_io(prepared.paths.ensure_thread_dirs, self.owner._thread_id, user_id=self.owner._user_id))
        self.access = await self.access.refresh(self.owner._request)
        listing = await self.access.list_uploads(self.owner._request)
        self.owner._seen_filenames.update(item["filename"] for item in listing["files"])
        self.owner._uploads_dir = prepared.paths.sandbox_uploads_dir(self.owner._thread_id, user_id=self.owner._user_id)

    async def _stage(self, *, expected_bytes=0, suffix=""):
        self.access = await self.access.refresh(self.owner._request)

        def create():
            stage = QualifiedUploadStage(self.access.prepared.mounts.user, self.relative, expected_bytes=expected_bytes, suffix=suffix)
            # The worker establishes ownership before returning. Cancellation
            # after acquisition cannot abandon the returned open descriptors.
            self.stages.append(stage)
            return stage

        return await await_drained(run_file_io(create))

    async def _publish(self, stage, name, uploads):
        while True:
            self.access = await self.access.refresh(self.owner._request)
            try:
                await await_drained(run_file_io(stage.publish, name))
                return name
            except FileExistsError:
                name = uploads.claim_unique_filename(name, self.owner._seen_filenames)

    async def ingest(self, chunks, *, display_name, expected_size, uploads):
        from app.gateway.upload_ingestion import UnsafeFilenameError

        try:
            original = uploads.normalize_filename(display_name)
        except ValueError as exc:
            raise UnsafeFilenameError(str(exc)) from exc
        name = uploads.claim_unique_filename(original, self.owner._seen_filenames)
        suffix = Path(name).suffix.lower()
        conversion = self.owner._auto_convert and suffix in uploads.CONVERTIBLE_EXTENSIONS
        stage = None
        try:
            if expected_size is not None:
                if type(expected_size) is not int or expected_size < 0:
                    raise HTTPException(400, "Invalid upload size")
                if expected_size > self.owner.limits.max_file_size or self.owner._total_size + expected_size > self.owner.limits.max_total_size:
                    raise HTTPException(413, "Upload size exceeds the configured limit")
            stage = await self._stage(expected_bytes=expected_size or 0, suffix=suffix if conversion else "")
            size = 0
            async for chunk in chunks:
                size += len(chunk)
                self.owner._total_size += len(chunk)
                if size > self.owner.limits.max_file_size:
                    raise HTTPException(413, f"File too large: {name}")
                if self.owner._total_size > self.owner.limits.max_total_size:
                    raise HTTPException(413, "Total upload size too large")

                def write():
                    stage.check_capacity(len(chunk), minimum_inodes=1)
                    stage.write(chunk)

                await await_drained(run_file_io(write))
            # Conversion reads the same still-private inode. Both originals
            # and companions are counted on this volume; no temporary copy on
            # a different disk or sandbox-writable source is used.
            markdown = await self._convert(stage) if conversion else None
            companion = None
            if markdown is not None:
                companion = await self._stage(expected_bytes=len(markdown))
                await await_drained(run_file_io(companion.write, markdown))
            name = await self._publish(stage, name, uploads)
            info = {"filename": name, "size": size, "path": str(self.owner._uploads_dir / name), "virtual_path": uploads.upload_virtual_path(name), "artifact_url": uploads.upload_artifact_url(self.owner._thread_id, name)}
            if name != original:
                info["original_filename"] = original
            if companion is not None:
                md_name = uploads.claim_unique_filename(Path(name).with_suffix(".md").name, self.owner._seen_filenames)
                md_name = await self._publish(companion, md_name, uploads)
                info.update(
                    markdown_file=md_name,
                    markdown_path=str(self.owner._uploads_dir / md_name),
                    markdown_virtual_path=uploads.upload_virtual_path(md_name),
                    markdown_artifact_url=uploads.upload_artifact_url(self.owner._thread_id, md_name),
                )
            return info
        except (ChatFilesFull, SpaceFull):
            raise files_full() from None
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise files_full() from None
            raise
        except FilesystemUnavailable:
            raise files_unavailable("space-unverified") from None
        except UnsafeSpacePath:
            raise HTTPException(400, "The uploads directory changed before publication") from None

    async def _convert(self, stage):
        from deerflow.utils.file_conversion import _do_convert, _get_pdf_converter

        self.access = await self.access.refresh(self.owner._request)
        try:
            text = await await_drained(asyncio.to_thread(lambda: _do_convert(stage.private_path, _get_pdf_converter())))
        except OSError as exc:
            if exc.errno in (errno.ENOSPC, errno.EDQUOT):
                raise
            logger.warning("Optional uploaded document conversion failed", exc_info=True)
            return None
        except Exception:
            logger.warning("Optional uploaded document conversion failed", exc_info=True)
            return None
        if len(text) > self.owner.limits.max_file_size:
            raise HTTPException(413, "Converted upload is too large")
        content = text.encode("utf-8")
        if len(content) > self.owner.limits.max_file_size:
            raise HTTPException(413, "Converted upload is too large")
        return content

    async def finalize(self):
        self.access = await self.access.refresh(self.owner._request)
        self.finalized = True

    async def cleanup_written(self):
        await await_drained(run_file_io(self._settle, tuple(reversed(self.stages)), "rollback_published"))

    async def aclose(self):
        stages, self.stages = self.stages, []

        def settle():
            try:
                if not self.finalized:
                    self._settle(tuple(reversed(stages)), "rollback_published")
            finally:
                self._settle(tuple(reversed(stages)), "close")

        async def close_all():
            try:
                await run_file_io(settle)
            finally:
                if self._gate is not None:
                    gate, self._gate = self._gate, None
                    await gate.__aexit__(None, None, None)

        await await_drained(close_all())

    @staticmethod
    def _settle(stages, method):
        first_error = None
        for stage in stages:
            try:
                getattr(stage, method)()
            except BaseException as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error
