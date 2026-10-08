"""Publication provenance, committed-outcome checks and controller recovery."""

import uuid

from deerflow.files.shared import shared_file_holding
from deerflow.spaces.workflows import record_fact
from deerflow.utils.file_io import run_file_io


class PublicationUnavailable(Exception):
    pass


async def copy_and_record(repo, source, *, name, folder, user_id, thread_id, source_path, copy, remove):
    publication_id = str(uuid.uuid4())
    published = await run_file_io(copy, source, name=name, folder=folder)
    fact = {"publication_id": publication_id, "path": published.path, "size": published.size, "sha256": published.sha256 or "", "published_by": user_id, "from_thread_id": thread_id, "from_path": source_path}
    await run_file_io(record_fact, "publication", fact)
    try:
        record = await repo.record_publication(**fact)
    except Exception as exc:
        try:
            committed = await repo.publication(publication_id)
        except Exception:
            # Unknown acknowledgement is never evidence for deleting the bytes.
            raise PublicationUnavailable("Publication outcome is unknown; its bytes and private receipt are retained") from exc
        if committed is not None:
            if any(committed.get(key) != value for key, value in fact.items()) or committed.get("removed_at") is not None:
                raise PublicationUnavailable("Publication identity changed; preserve bytes for controller recovery") from exc
            return published, committed
        held = await run_file_io(shared_file_holding, published.path, fact["sha256"])
        if held is None:
            raise PublicationUnavailable("Uncommitted publication bytes changed; preserve them for recovery") from exc
        await run_file_io(remove, published.path)
        await run_file_io(record_fact, "publication_rolled_back", publication_id)
        raise PublicationUnavailable("The publication record was not committed; the owned copy was removed") from exc
    return published, record


async def recover_removals(repo, state):
    from deerflow.files.store import StoreError
    from deerflow.spaces.workflows import _read_only, mark_effect

    names = await run_file_io(state.pending_names)
    if _read_only.get() and names:
        raise StoreError("Pending publication recovery requires the installed controller and mutation authority")
    for name in names:
        pending = await run_file_io(state.open_removal, name)
        try:
            mark_effect()
            removed = pending.journal is None or pending.journal["removed"]
            if pending.publication_id is not None:
                record = await repo.publication(pending.publication_id)
                if record is None:
                    raise StoreError("Shared removal recovery needs its publication record")
                if record.get("path") != pending.path:
                    raise StoreError("Shared removal publication does not match its staged path")
                removed = record.get("removed_at") is not None
            await run_file_io(pending.finish if removed else pending.restore)
        finally:
            await run_file_io(pending.close)


async def reconcile_publication(repo, receipt, intent):
    """Reconcile this operation's exact known receipt; never infer a new copy."""
    from deerflow.files.shared import shared_mutation_state
    from deerflow.spaces.workflows import mark_effect

    facts = receipt.value["facts"]
    if intent.request.get("operation") == "publish":
        fact = facts.get("publication")
        if not isinstance(fact, dict) or fact.get("published_by") != intent.actor_id or fact.get("from_path") != intent.request.get("source", {}).get("path"):
            raise PublicationUnavailable("The exact publication receipt is unavailable; retain unknown bytes")
        record = await repo.publication(fact["publication_id"])
        if facts.get("publication_rolled_back") == fact["publication_id"]:
            if record is not None:
                raise PublicationUnavailable("A rolled-back publication unexpectedly has a record")
            return {"publication_id": fact["publication_id"], "published": False}
        held = await run_file_io(shared_file_holding, fact["path"], fact["sha256"])
        if held is None:
            raise PublicationUnavailable("Publication bytes no longer match their receipt")
        if record is None:
            mark_effect()
            record = await repo.record_publication(**fact)
        if record.get("removed_at") is not None or any(record.get(key) != value for key, value in fact.items()):
            raise PublicationUnavailable("Publication identity changed; retain data")
        return {"publication_id": fact["publication_id"], "published": True}
    if intent.request.get("operation") == "remove_shared":
        fact = facts.get("removal")
        if not isinstance(fact, dict):
            raise PublicationUnavailable("The exact removal receipt is unavailable")
        expected = intent.request.get("parameters", {}).get("expected_publication_id")
        if expected is not None and expected != fact["publication_id"]:
            raise PublicationUnavailable("Removal receipt identifies a different publication")
        state = shared_mutation_state()
        try:
            await run_file_io(state.acquire)
            names = await run_file_io(state.pending_names)
            if any(name != fact["journal"] for name in names):
                raise PublicationUnavailable("Other removal journals require independent reconciliation")
            if fact["journal"] in names:
                pending = await run_file_io(state.open_removal, fact["journal"])
                try:
                    if pending.journal is None or pending.path != fact["path"] or pending.publication_id != fact["publication_id"]:
                        raise PublicationUnavailable("Removal journal does not match the exact operation receipt")
                finally:
                    await run_file_io(pending.close)
            await recover_removals(repo, state)
        finally:
            await run_file_io(state.close)
        return {"publication_id": fact["publication_id"], "reconciled": True}
    raise PublicationUnavailable("This workflow has no qualified publication reconciliation")
