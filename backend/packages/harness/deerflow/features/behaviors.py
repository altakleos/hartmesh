"""First-party business workflows supplied by the installed feature services."""

from contextlib import asynccontextmanager

from deerflow.features.publications import PublicationUnavailable, copy_and_record, recover_removals
from deerflow.files import manager, shared
from deerflow.files.store import normalize_relative_path
from deerflow.spaces.contract import SpaceConflict, SpaceDenied
from deerflow.utils.file_io import await_drained, run_file_io


class MyFilesBehavior:
    async def execute(self, operation, *, user_id, **values):
        if operation == "keep":
            return await run_file_io(manager.keep_file, user_id, **values)
        if operation == "list":
            return await run_file_io(manager.list_user_files, user_id)
        if operation == "remove":
            return await run_file_io(manager.delete_user_file, user_id, values["path"])
        raise ValueError("Unknown My Files workflow")


class SharedBehavior:
    @asynccontextmanager
    async def mutation(self, repo):
        state = shared.shared_mutation_state()
        try:
            await await_drained(run_file_io(state.acquire))
            await await_drained(recover_removals(repo, state))
            yield state
        finally:
            await await_drained(run_file_io(state.close))

    async def execute(self, operation, *, user_id, repo, **values):
        async with self.mutation(repo) as state:
            if operation == "list":
                entries, truncated = await run_file_io(shared.list_shared_files)
                return entries, truncated, await repo.live_publications()
            if operation == "publish":
                source = values["source"]
                folder = normalize_relative_path(values.get("folder") or "", allow_empty=True)
                digest = await run_file_io(shared.digest_of, source)
                for record in await repo.live_publications_holding(digest, folder=folder):
                    held = await run_file_io(shared.shared_file_holding, record["path"], digest)
                    if held is not None:
                        return held, record, False
                published, record = await copy_and_record(
                    repo, source, name=values["name"], folder=folder, user_id=user_id, thread_id=values.get("thread_id"), source_path=values["source_path"], copy=shared.publish_file, remove=shared.remove_shared_file
                )
                return published, record, True
            if operation == "remove":
                path = normalize_relative_path(values["path"])
                source = await run_file_io(shared.resolve_shared_file, path)
                await run_file_io(shared.digest_of, source)
                record = await repo.live_publication(path)
                publication_id = "" if record is None else record["publication_id"]
                expected = values.get("expected_publication_id")
                if expected is not None and expected != publication_id:
                    raise SpaceConflict("This Shared publication changed; refresh before removing it")
                if not values.get("admin", False) and (record is None or record.get("published_by") != user_id):
                    raise SpaceDenied("Only the publisher or an administrator can remove this publication")
                pending = await run_file_io(state.stage, path, publication_id or None)
                try:
                    if record is not None:
                        if await repo.record_removal(publication_id, removed_by=user_id) is None:
                            raise PublicationUnavailable("The exact publication changed during removal")
                    else:
                        await run_file_io(pending.mark_removed)
                    await run_file_io(pending.finish)
                except Exception:
                    await run_file_io(pending.close)
                    await recover_removals(repo, state)
                    raise
                finally:
                    await run_file_io(pending.close)
                return path
        raise ValueError("Unknown Shared workflow")


class ProjectsBehavior:
    def __init__(self):
        self.repository = None
        self.documents = None

    def bind(self, session_factory):
        from deerflow.persistence.projects import ProjectDocumentRepository, ProjectRepository

        self.repository = ProjectRepository(session_factory)
        self.documents = ProjectDocumentRepository(session_factory)

    async def execute(self, operation, *, user_id, **values):
        if operation in {"create", "get", "list", "patch", "delete"}:
            return await getattr(self.repository, operation)(user_id=user_id, **values)
        if operation in {"archive", "restore"}:
            return await self.repository.set_status(values["project_id"], "archived" if operation == "archive" else "active", user_id=user_id)
        if operation == "stage-document":
            from deerflow.config.paths import get_paths
            from deerflow.projects.documents import stage_document_bytes

            return await stage_document_bytes(get_paths(), user_id=user_id, **values)
        if operation == "commit-document":
            from deerflow.config.paths import get_paths
            from deerflow.projects.documents import add_staged_document

            return await add_staged_document(self.documents, get_paths(), user_id=user_id, **values)
        if operation == "attach-copy":
            from deerflow.config.paths import get_paths
            from deerflow.projects.documents import stage_document_copy_for_attach

            return await stage_document_copy_for_attach(self.documents, get_paths(), user_id=user_id, **values)
        raise ValueError("Unknown Projects workflow")


class ProjectRepositoryFacade:
    """Compatibility adapter to the installed organization's business service."""

    def __init__(self, service):
        self.service = service

    async def create(self, **values):
        return await self.service.execute("create", **values)

    async def get(self, project_id):
        return await self.service.execute("get", project_id=project_id)

    async def list(self, **values):
        return await self.service.execute("list", **values)

    async def patch(self, project_id, **values):
        return await self.service.execute("patch", project_id=project_id, **values)

    async def delete(self, project_id):
        return await self.service.execute("delete", project_id=project_id)

    async def set_status(self, project_id, status):
        if status not in {"active", "archived"}:
            raise ValueError("Unsupported project status")
        return await self.service.execute("restore" if status == "active" else "archive", project_id=project_id)
