"""Publication provenance, committed-outcome checks and controller recovery."""

import re
import uuid

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from deerflow.files.shared import shared_file_holding
from deerflow.spaces.workflows import record_fact
from deerflow.utils.file_io import run_file_io

FACT_BEFORE_NAME = "fact-before-name"


class PublicationUnavailable(Exception):
    pass


async def copy_and_record(repo, source, *, name, folder, user_id, thread_id, source_path, copy, remove, copy_async=None, rollback_async=None, ordered_copy=None):
    publication_id = str(uuid.uuid4())
    fact = None

    def remember(path, size, digest):
        nonlocal fact
        fact = {"publication_id": publication_id, "path": path, "size": size, "sha256": digest, "published_by": user_id, "from_thread_id": thread_id, "from_path": source_path}
        record_fact("publication", fact)

    if ordered_copy is not None:
        published = await ordered_copy(source, name=name, folder=folder, before_name=remember)
        if fact is None or (fact["path"], fact["size"], fact["sha256"]) != (published.path, published.size, published.sha256):
            raise PublicationUnavailable("The published file differs from its recorded candidate")
    else:
        published = await copy_async(source, name=name, folder=folder) if copy_async is not None else await run_file_io(copy, source, name=name, folder=folder)
        await run_file_io(remember, published.path, published.size, published.sha256 or "")
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
        if rollback_async is not None:
            await rollback_async(published)
        else:
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

    if _read_only.get():
        # A mounted reader holds the Space's shared host lock. It observes only public names.
        return
    for name in await run_file_io(state.pending_names):
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


def publication_destination(request):
    from deerflow.files.store import normalize_relative_path
    from deerflow.uploads.manager import normalize_filename

    source = request.get("source")
    if not isinstance(source, dict) or not isinstance(source.get("path"), str) or (source.get("folder") is not None and not isinstance(source["folder"], str)):
        raise ValueError("The publication intent has no recognized destination")
    folder = normalize_relative_path(source.get("folder") or "", allow_empty=True)
    return folder, normalize_filename(source["path"].rsplit("/", 1)[-1])


def _publication_source_thread(source):
    from deerflow.config.paths import USER_FILES_VIRTUAL_PREFIX, VIRTUAL_PATH_PREFIX
    from deerflow.utils.thread_id import validate_thread_id

    if source["path"].startswith(USER_FILES_VIRTUAL_PREFIX + "/"):
        return None
    if source["path"].startswith((VIRTUAL_PATH_PREFIX + "/uploads/", VIRTUAL_PATH_PREFIX + "/outputs/")):
        return validate_thread_id(source.get("thread_id"))
    raise PublicationUnavailable("The publication source is unrecognized")


class _PublicationFact(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid")

    publication_id: str = Field(min_length=1)
    published_by: str
    from_path: str
    from_thread_id: str | None
    path: str
    size: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


async def _settle_recorded_copy(repo, facts, intent, identity, holding):
    from deerflow.features.qualified_publications import is_publication_name
    from deerflow.spaces.workflows import WorkflowResolution, mark_effect

    try:
        fact = _PublicationFact.model_validate(facts["publication"])
    except ValidationError:
        raise PublicationUnavailable("The exact publication receipt is unavailable; retain unknown bytes") from None
    folder, name = publication_destination(intent.request)
    source = intent.request["source"]
    parent, _, leaf = fact.path.rpartition("/")
    if fact.published_by != intent.actor_id or fact.from_path != source["path"] or fact.from_thread_id != _publication_source_thread(source):
        raise PublicationUnavailable("The publication actor or source does not match the intent")
    if parent != folder or not is_publication_name(name, leaf):
        raise PublicationUnavailable("The publication destination does not match the intent")
    record = await repo.publication(fact.publication_id)
    if "publication_rolled_back" in facts:
        if record is not None or facts["publication_rolled_back"] != fact.publication_id:
            raise PublicationUnavailable("A rolled-back publication has inconsistent evidence")
        return {"publication_id": fact.publication_id, "published": False}
    fields = fact.model_dump()
    if record is not None and (record.get("removed_at") is not None or any(record.get(key) != value for key, value in fields.items())):
        raise PublicationUnavailable("Publication identity changed; retain data")
    if identity is None and facts.get("publication_order") == FACT_BEFORE_NAME:
        raise PublicationUnavailable("The staged publication identity is missing")
    held = await run_file_io(holding, fact.path, fact.sha256, expected_identity=identity)
    if held is None or held.size != fact.size:
        return WorkflowResolution.FAILED
    if record is None:
        mark_effect()
        await repo.record_publication(**fields)
    return {"publication_id": fact.publication_id, "published": True}


async def _reconcile_publish(repo, facts, intent, *, holding, volume):
    from deerflow.features.qualified_publications import publication_names
    from deerflow.spaces.workflows import WorkflowResolution, mark_effect

    identity = facts.get("publication_identity")
    if "publication_identity" in facts and (
        not isinstance(identity, dict) or set(identity) != {"device", "inode"} or type(identity["device"]) is not int or identity["device"] < 0 or type(identity["inode"]) is not int or identity["inode"] <= 0
    ):
        raise PublicationUnavailable("The staged publication identity is invalid")
    if "removal" in facts:
        raise PublicationUnavailable("The publication receipt contains a removal")
    if "publication" in facts:
        return await _settle_recorded_copy(repo, facts, intent, identity, holding)
    if "publication_rolled_back" in facts:
        raise PublicationUnavailable("A rolled-back publication has no publication fact")
    if facts.get("publication_order") == FACT_BEFORE_NAME:
        return WorkflowResolution.FAILED
    folder, name = publication_destination(intent.request)
    source = intent.request["source"]
    adopted = None
    for path in await run_file_io(publication_names, volume, folder, name):
        record = await repo.live_publication(path)
        if record is not None:
            held = await run_file_io(holding, path, record["sha256"])
            if held is not None and held.size == record["size"]:
                continue
        if identity is None:
            raise PublicationUnavailable("A possible publication has no matching record or staged identity")
        candidate = await run_file_io(holding, path, None, expected_identity=identity)
        if candidate is not None:
            if adopted is not None:
                raise PublicationUnavailable("Multiple names match the staged publication")
            adopted = candidate
    if adopted is None:
        return WorkflowResolution.FAILED
    mark_effect()
    record = await repo.record_publication(path=adopted.path, size=adopted.size, sha256=adopted.sha256, published_by=intent.actor_id, from_thread_id=_publication_source_thread(source), from_path=source["path"])
    return {"publication_id": record["publication_id"], "published": True}


async def _reconcile_removal(repo, facts, intent):
    from deerflow.files.shared import shared_mutation_state
    from deerflow.files.store import normalize_relative_path
    from deerflow.spaces.workflows import WorkflowResolution

    if "publication" in facts:
        raise PublicationUnavailable("The removal receipt contains a publication")
    if "removal" not in facts:
        return WorkflowResolution.FAILED
    fact = facts["removal"]
    if not isinstance(fact, dict) or set(fact) != {"journal", "path", "publication_id"}:
        raise PublicationUnavailable("The exact removal receipt is unavailable")
    parameters = intent.request.get("parameters", {})
    path, publication_id = fact["path"], fact["publication_id"]
    if not isinstance(path, str) or normalize_relative_path(path) != path or parameters.get("path", path) != path:
        raise PublicationUnavailable("Removal path does not match the exact operation receipt")
    expected = parameters.get("expected_publication_id")
    if expected is not None and (expected or None) != publication_id:
        raise PublicationUnavailable("Removal receipt identifies a different publication")
    if not isinstance(fact["journal"], str) or re.fullmatch(r"remove-[a-f0-9]{32}", fact["journal"]) is None:
        raise PublicationUnavailable("The exact removal journal is unavailable")
    state = shared_mutation_state()
    try:
        await run_file_io(state.acquire)
        names = await run_file_io(state.pending_names)
        if any(name != fact["journal"] for name in names):
            raise PublicationUnavailable("Other removal journals require independent reconciliation")
        pending = await run_file_io(state.open_removal, fact["journal"]) if names else None
        try:
            if pending is not None and pending.journal is not None and (pending.path != path or pending.publication_id != publication_id):
                raise PublicationUnavailable("Removal journal does not match the exact operation receipt")
            record = await repo.publication(publication_id) if publication_id is not None else None
            if publication_id is not None and (record is None or record["path"] != path):
                raise PublicationUnavailable("Shared removal record does not match the exact operation receipt")
            if pending is not None:
                if pending.journal is None:
                    await run_file_io(pending.finish_empty)
                else:
                    removed = record["removed_at"] is not None if record is not None else pending.journal["removed"]
                    await run_file_io(pending.finish if removed else pending.restore)
        finally:
            if pending is not None:
                await run_file_io(pending.close)
        if record is not None and record["removed_at"] is not None:
            return {"publication_id": publication_id, "reconciled": True}
        if await run_file_io(state.file_present, path):
            return WorkflowResolution.FAILED
        if record is not None:
            raise PublicationUnavailable("The live publication is missing from its recorded path")
        return {"publication_id": None, "reconciled": True}
    finally:
        await run_file_io(state.close)


async def reconcile_publication(repo, receipt, intent, *, holding=None, volume=None):
    """Reconcile this exact owned intent against the verified Company volume."""
    facts = receipt.value["facts"]
    operation = intent.request.get("operation")
    if operation == "publish":
        if volume is None or holding is None:
            raise PublicationUnavailable("Publication recovery requires its verified Company volume")
        return await _reconcile_publish(repo, facts, intent, holding=holding, volume=volume)
    if operation == "remove_shared":
        return await _reconcile_removal(repo, facts, intent)
    raise PublicationUnavailable("This workflow has no qualified publication reconciliation")
