"""Read exact admitted output evidence without creating or replaying execution."""

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse

from app.gateway.authz import require_permission
from app.gateway.channel_execution import CHANNEL_TURN_ID_KEY, receipt_from_run_kwargs
from app.gateway.channel_execution_reads import ChannelReadDenied, ChannelReadMissing, channel_read_headers, channel_read_scope, strict_channel_read
from app.gateway.deps import get_run_event_store, get_run_manager
from app.gateway.run_output import OUTPUT_INVENTORY_REQUIRE_HEADER, OUTPUT_INVENTORY_VERSION, OUTPUT_INVENTORY_VERSION_2, OUTPUT_INVENTORY_VERSION_HEADER, RunOutputProjection, RunOutputProjectionV2
from deerflow.runtime.runs.output import OutputEvidenceError, OutputEvidenceScopeError, TextOutputReceipt, capture_from_kwargs, receipt_from_kwargs, validate_receipt_status
from deerflow.runtime.runs.output_inventory import EventInventoryEvidence, inventory_evidence_from_kwargs, validate_inventory_pair
from deerflow.runtime.runs.output_inventory_source import read_durable_inventory_source
from deerflow.runtime.runs.output_source import read_durable_output_source
from deerflow.utils.file_io import await_drained
from deerflow.utils.thread_id import ThreadId

router = APIRouter(tags=["runs"])


class _ScopeDenied(HTTPException):
    """Only a fresh known scope refusal bypasses the proof error boundary."""


async def _refresh_scope(request, thread_id, owner):
    try:
        async with channel_read_scope(request, thread_id) as current:
            if current != owner:
                raise _ScopeDenied(404, "Run output is unavailable")
    except HTTPException as error:
        if error.status_code in {403, 404}:
            raise _ScopeDenied(error.status_code, "Run output is unavailable") from None
        raise OutputEvidenceError() from None


def _unavailable():
    return HTTPException(503, "Run output evidence is unavailable")


def _admission(snapshot, *, owner, thread_id, run_id):
    if (snapshot.owner_user_id, snapshot.thread_id, snapshot.run_id) != (owner, thread_id, run_id):
        raise ChannelReadDenied()
    if capture_from_kwargs(snapshot.kwargs) is None:
        raise OutputEvidenceError()
    admission = receipt_from_run_kwargs(snapshot.kwargs)
    if admission is None or (admission.owner_user_id, admission.thread_id, admission.assistant_id) != (owner, thread_id, "lead_agent"):
        raise OutputEvidenceError()
    alias = snapshot.assistant_id
    if alias not in (None, "lead_agent") and (type(alias) is not str or alias.strip().lower().replace("_", "-") != admission.agent_name):
        raise OutputEvidenceError()
    return admission


@require_permission("runs", "read", owner_check=True)
async def _read_owned(thread_id: str, run_id: str, request: Request, *, require_inventory: bool = False, inventory_version: str = OUTPUT_INVENTORY_VERSION):
    async with channel_read_scope(request, thread_id) as owner:
        try:
            manager = get_run_manager(request)
            events = get_run_event_store(request)
            if manager.supports_output_receipts is not True or getattr(events, "supports_durable_output_evidence", False) is not True:
                raise OutputEvidenceError()
            inventory_options = {"require_inventory": True} if require_inventory else {}
            if require_inventory and (getattr(manager, "supports_output_inventory", False) is not True or getattr(events, "supports_durable_output_inventory", False) is not True):
                raise OutputEvidenceError()
            snapshot = await manager.get_output_snapshot(run_id, user_id=owner, **inventory_options)
            if snapshot is None:
                await _refresh_scope(request, thread_id, owner)
                missing = ChannelReadMissing()
                if require_inventory:
                    missing.headers = {**channel_read_headers(), OUTPUT_INVENTORY_VERSION_HEADER: inventory_version}
                raise missing
            admission = _admission(snapshot, owner=owner, thread_id=thread_id, run_id=run_id)
            receipt = receipt_from_kwargs(snapshot.kwargs)
            inventory = None
            if require_inventory:
                intent = capture_from_kwargs(snapshot.kwargs)
                if intent.version != 2 or inventory_version == OUTPUT_INVENTORY_VERSION and intent.document_version != 1:
                    raise OutputEvidenceError()
                inventory = inventory_evidence_from_kwargs(snapshot.kwargs)
                if receipt is None:
                    if inventory is not None:
                        raise OutputEvidenceError()
                else:
                    receipt, inventory = validate_inventory_pair(receipt, inventory, status=snapshot.status)
            source = reason = None
            if receipt is None:
                if snapshot.status.value not in {"pending", "running"}:
                    raise OutputEvidenceError()
                state = "pending"
            else:
                receipt = validate_receipt_status(receipt, snapshot.status)
                if isinstance(receipt, TextOutputReceipt):
                    if receipt.input_message_id is None:
                        raise OutputEvidenceError()
                    audit = await read_durable_output_source(events, receipt=receipt)
                    if audit.state == "matched":
                        human = audit.input_message
                        additional = human.get("additional_kwargs", {}) if human is not None else {}
                        if additional.get(CHANNEL_TURN_ID_KEY) == admission.publication_id:
                            state, source = "complete", {"text": audit.text}
                        else:
                            state, reason = "unavailable", "source_changed"
                    else:
                        state, reason = "unavailable", "source_missing" if audit.state == "missing" else "source_changed"
                    # A response never upgrades an original immutable seal/scope.
                    if not require_inventory:
                        current = await manager.get_output_snapshot(run_id, user_id=owner)
                        if current is None or (current.status, current.kwargs, current.assistant_id) != (snapshot.status, snapshot.kwargs, snapshot.assistant_id):
                            raise OutputEvidenceError()
                        _admission(current, owner=owner, thread_id=thread_id, run_id=run_id)
                else:
                    state, reason = "unavailable", receipt.reason
            result = RunOutputProjection(version=1, owner_user_id=owner, thread_id=thread_id, run_id=run_id, run_status=snapshot.status.value, state=state, channel_execution=admission, output_evidence=receipt, source=source, reason=reason)
            headers = {**channel_read_headers(), "Cache-Control": "private, no-store"}
            if require_inventory:
                if receipt is None:
                    inventory_projection = {"state": "pending", "evidence": None, "document": None, "reason": None}
                elif isinstance(inventory, EventInventoryEvidence):
                    inventory_audit = await read_durable_inventory_source(events, evidence=inventory, output_receipt=receipt)
                    if inventory_audit.document is not None and inventory_audit.document.version != intent.document_version:
                        raise OutputEvidenceError()
                    inventory_projection = {
                        "state": "matched" if inventory_audit.state == "matched" else "unavailable",
                        "evidence": inventory,
                        "document": inventory_audit.document,
                        "reason": None if inventory_audit.state == "matched" else "source_missing" if inventory_audit.state == "missing" else "source_changed",
                    }
                else:
                    inventory_projection = {"state": "unavailable", "evidence": inventory, "document": None, "reason": inventory.reason}
                # Includes pending and pure unavailable: both immutable proofs,
                # status and admission are re-read AFTER all source work.
                current = await manager.get_output_snapshot(run_id, user_id=owner, **inventory_options)
                if current is None or (current.status, current.kwargs, current.assistant_id) != (snapshot.status, snapshot.kwargs, snapshot.assistant_id):
                    raise OutputEvidenceError()
                _admission(current, owner=owner, thread_id=thread_id, run_id=run_id)
                result = RunOutputProjectionV2(version=2, output=result, inventory=inventory_projection)
                headers[OUTPUT_INVENTORY_VERSION_HEADER] = inventory_version
            await _refresh_scope(request, thread_id, owner)
            return JSONResponse(result.model_dump(mode="json"), headers=headers)
        except (ChannelReadDenied, ChannelReadMissing, _ScopeDenied):
            raise
        except OutputEvidenceScopeError:
            raise ChannelReadDenied() from None
        except Exception:
            raise _unavailable() from None


@router.get("/{thread_id}/runs/{run_id}/output")
async def get_run_output(thread_id: ThreadId, run_id: str, request: Request):
    if not strict_channel_read(request):
        raise HTTPException(422, "Unsupported channel execution read version")
    versions = request.headers.getlist(OUTPUT_INVENTORY_REQUIRE_HEADER)
    if versions and (len(versions) != 1 or versions[0] not in {OUTPUT_INVENTORY_VERSION, OUTPUT_INVENTORY_VERSION_2}):
        raise HTTPException(422, "Unsupported output inventory read version")
    try:
        # Includes permission/ownership SQL, live scope and response construction.
        return await await_drained(_read_owned(thread_id=thread_id, run_id=run_id, request=request, **({"require_inventory": True, "inventory_version": versions[0]} if versions else {})))
    except (ChannelReadDenied, ChannelReadMissing):
        raise
    except HTTPException as error:
        # Scope/auth denials never borrow a backend's acknowledgement or details.
        if error.status_code in {401, 403, 404}:
            raise HTTPException(error.status_code, "Run output is unavailable") from None
        raise _unavailable() from None
    except Exception:
        raise _unavailable() from None
