"""Opt-in preparation; physical effects remain owned through cancellation."""

import asyncio
import logging
from uuid import uuid4

from deerflow.agent_instances.contract import AgentDenied, Refusal, ResidentFilesBusy, ResidentFilesFull, ResidentStartFailed, ResidentStartingElsewhere
from deerflow.persistence.spaces.lifecycle import SpaceAttachmentRow
from deerflow.runtime.turn_phases import TurnPhase, phase_span
from deerflow.spaces.attachments import AttachmentPending, ResourceMount, RetainedAttachment
from deerflow.spaces.backings import BackingUnavailable
from deerflow.spaces.provision import SPACE_MIN_FREE_BYTES, SPACE_MIN_FREE_INODES
from deerflow.spaces.service import SpaceOperationPending
from deerflow.utils.file_io import await_drained, run_file_io

logger = logging.getLogger(__name__)
RESUME_RETRIES = 3
RESUME_RETRY_SECONDS = 2


async def _validate(execution):
    try:
        await execution.authority.validate(execution)
    except SpaceOperationPending as exc:
        raise ResidentFilesBusy(execution.instance.name, cause=exc.cause) from exc
    except BackingUnavailable as exc:
        raise ResidentStartFailed(execution.instance.name) from exc


async def _retained(execution, residents, mounts, attached):
    facts = await mounts.retained_attachment(attached.id)
    if (
        not isinstance(facts, RetainedAttachment)
        or facts.id != attached.id
        or facts.actor != execution.instance.principal
        or facts.host_id != mounts.provider.host_id
        or facts.resources != (ResourceMount(execution.instance.home_id, execution.home_generation, "home", True),)
        or (getattr(attached, "container_id", None) is not None and facts.container_id != attached.container_id)
    ):
        raise AttachmentPending("Preparation does not match its detached immutable facts")
    return facts


async def _require_usable(residents, mounts, attached, facts):
    merged = residents.retained.get(attached.id)
    if facts.phase != "active" or merged is None or merged.phase != "active" or merged.container_id != attached.container_id:
        await mounts.contain_host(attached.id, "start-failed")
        raise AttachmentPending("The returned attachment is no longer confirmed active")


async def _claim_existing(execution, residents, mounts, attached, *, attempt_id=None, require_active=True):
    if not residents._restoration_started:
        residents.claimed(execution.instance, attached.id, attempt_id=attempt_id)
        return
    facts = await _retained(execution, residents, mounts, attached)
    residents.retain(facts)
    if require_active:
        await _require_usable(residents, mounts, attached, facts)
    if facts.phase == "active":
        residents.associate(execution.instance, residents.retained[facts.id], generation=execution.home_generation, attempt_id=attempt_id)


async def _transfer_preparation(execution, residents, mounts, preparation, attached, *, attempt_id=None, require_active=False):
    if not residents._restoration_started:
        return residents.preparation_claimed(execution.instance, preparation, attached.id, attempt_id=attempt_id)
    facts = await _retained(execution, residents, mounts, attached)
    transferred = residents.preparation_retained(execution.instance, preparation, facts, attempt_id=attempt_id)
    if transferred and require_active:
        await _require_usable(residents, mounts, attached, facts)
    return transferred


async def _resume(execution, residents, mounts, resources):
    instance = execution.instance
    for retry in range(RESUME_RETRIES + 1):
        try:
            # resume's filesystem offload is not itself shielded. The entire
            # preparation is owned, and this boundary also drains inner cancel.
            return await await_drained(mounts.resume(actor=instance.principal, resources=resources))
        except AttachmentPending:
            state = await mounts.unfenced(instance.home_id, actor=instance.principal, resources=resources, host_id=mounts.provider.host_id)
            if state is None:
                return None
            await _claim_existing(execution, residents, mounts, state, require_active=False)
            physical = await mounts.container_state(state.id) if state.container_id is not None else "unknown"
            if physical == "gone" and await mounts.container_state(state.id) == "gone":
                if await mounts.contain_host(state.id, "container-absent"):
                    return None
                raise ResidentStartFailed(instance.name)
            if retry < RESUME_RETRIES:
                await asyncio.sleep(RESUME_RETRY_SECONDS)
                continue
            if state.container_id is None:
                raise ResidentStartingElsewhere(instance.name)
            if state.phase in {"pending", "fence_pending"} or (state.phase == "active" and physical == "running"):
                if await mounts.contain_host(state.id, "container-absent"):
                    return None
            raise ResidentStartFailed(instance.name)


async def _recover_claim(execution, environment, residents, mounts, resources, claim, *, returned=None):
    """Only exact correlation authorizes cleanup of an invoked preparation."""
    instance = execution.instance
    try:
        state = await mounts.inspect_preparation(actor=instance.principal, incarnation=claim.incarnation, resources=resources, host_id=claim.host_id)
        if state is None:
            if returned is not None:
                raise AttachmentPending("Returned native attachment cannot be forgotten from a missing metadata read")
            # No SQL intent is insufficient if the qualified host disappeared.
            await await_drained(run_file_io(mounts.provider._same_host))
            residents.preparation_absent(instance.home_id, claim)
            return
        if returned is not None and (state.id != returned.id or (getattr(state, "container_id", None) is not None and state.container_id != returned.container_id)):
            raise AttachmentPending("Preparation recovery changed its returned immutable identity")
        if residents._restoration_started and state.phase == "fenced":
            confirmed = await mounts.confirm_fenced_preparation(
                actor=instance.principal,
                incarnation=claim.incarnation,
                resources=resources,
                host_id=claim.host_id,
                attachment_id=state.id,
                container_id=state.container_id,
            )
            if confirmed:
                try:
                    await mounts.contain_host(state.id, "start-failed")
                finally:
                    residents.preparation_absent(instance.home_id, claim)
            return
        if await _transfer_preparation(execution, residents, mounts, claim, state, attempt_id=getattr(execution.work, "attempt_id", None)):
            environment.attachment_id = state.id
            await mounts.contain_host(state.id, "start-failed")
    except Exception:
        logger.warning("Preparation outcome remains retained for Home %s", instance.home_id, exc_info=True)


async def _refresh_closed(execution, residents, mounts):
    """Observe an external closure without deriving absence from a missing row."""
    entry = residents.entry(execution.instance)
    if not residents._restoration_started or not residents.restoration_complete or residents._closing or entry.reconciling or entry.attachment_id is None or entry.preparation is not None:
        return
    claim = residents.retained.get(entry.attachment_id)
    if (
        claim is None
        or claim.actor != execution.instance.principal
        or claim.host_id != mounts.provider.host_id
        or claim.resources != (ResourceMount(execution.instance.home_id, execution.home_generation, "home", True),)
        or claim.container_id is None
    ):
        raise AttachmentPending("The previous resident lost its exact retained correlation")
    if await mounts.retained_attachment(claim.id) is not None:
        return
    # A standalone operator cannot notify this process. Historical SQL alone
    # cannot release the charge: canonical host containment must finish the
    # retained original graph and notify with its stored closure reason.
    state = await mounts.inspect_preparation(actor=claim.actor, incarnation=claim.incarnation, resources=claim.resources, host_id=claim.host_id)
    if state is None or (state.id, state.phase, state.host_id, state.container_id) != (claim.id, "fenced", claim.host_id, claim.container_id):
        raise AttachmentPending("The previous resident's exact closure is unconfirmed")

    async def observed_fenced(session, identities):
        row = await session.get(SpaceAttachmentRow, claim.id)
        if (
            identities != ((claim.id, claim.container_id),)
            or row is None
            or (row.id, row.incarnation, row.actor_kind, row.actor_id, row.host_id, row.container_id, row.phase) != (claim.id, claim.incarnation, claim.actor.kind, claim.actor.subject_id, claim.host_id, claim.container_id, "fenced")
            or mounts._closure_reason(row, None) is None
        ):
            raise AttachmentPending("The previous resident changed before observing its closure")
        scope = await mounts._mount_scope(session, (claim.id,))
        if scope[claim.id] != {(resource.space_id, resource.generation, resource.alias, resource.writable) for resource in claim.resources}:
            raise AttachmentPending("The previous resident's closed mount scope changed")

    if not await mounts.contain_host(claim.id, "container-absent", admission=observed_fenced) or entry.attachment_id is not None:
        raise AttachmentPending("The previous resident's closure was not observed")


async def _attachment(execution, environment, residents, mounts, run_id):
    instance = execution.instance
    entry = residents.entry(instance)
    resources = [ResourceMount(instance.home_id, execution.home_generation, "home", writable=True)]
    async with entry.lock:
        try:
            await _validate(execution)
        except AttachmentPending:
            # resume inspects that same durable intent and decides its outcome.
            pass
        try:
            await _refresh_closed(execution, residents, mounts)
            await residents.admit(instance, work=False, run_id=run_id)
            if entry.preparation is not None:
                raise ResidentStartingElsewhere(instance.name)
            attached = await _resume(execution, residents, mounts, resources)
            if attached is None and entry.attachment_id is not None:
                await _refresh_closed(execution, residents, mounts)
                if entry.attachment_id is not None:
                    raise AttachmentPending("The previous resident is not confirmed closed")
        except (AgentDenied, Refusal):
            raise
        except SpaceOperationPending as exc:
            raise ResidentFilesBusy(instance.name, cause=exc.cause) from exc
        except Exception as exc:
            raise ResidentStartFailed(instance.name) from exc
        if attached is not None:
            environment.attachment_id = attached.id
            try:
                await _claim_existing(execution, residents, mounts, attached, attempt_id=getattr(execution.work, "attempt_id", None))
            except BaseException as exc:
                environment.recycle_required = True
                if isinstance(exc, (asyncio.CancelledError, AgentDenied, Refusal)):
                    raise
                raise ResidentStartFailed(instance.name) from exc
            return attached
        claim = residents.preparing(instance, incarnation=uuid4().hex, generation=execution.home_generation, host_id=mounts.provider.host_id)
        invoked = False
        returned = None
        try:
            await residents.take_slot(instance, wait=True)
            # Capacity waiting releases the event loop; authority may change.
            await _validate(execution)
            quota = await execution.authority.instances.files.quota(actor=instance.principal, space_id=instance.home_id)
            if quota["available_bytes"] < SPACE_MIN_FREE_BYTES or quota["available_inodes"] < SPACE_MIN_FREE_INODES:
                raise ResidentFilesFull(instance.name)
            invoked = True
            with phase_span(TurnPhase.SANDBOX_CREATE):
                attached = await await_drained(mounts.attach(actor=instance.principal, incarnation=claim.incarnation, resources=resources))
            returned = attached
            environment.attachment_id = attached.id
            if not await _transfer_preparation(execution, residents, mounts, claim, attached, attempt_id=getattr(execution.work, "attempt_id", None), require_active=True):
                raise RuntimeError("The exact preparation claim changed")
            environment.attachment_id = attached.id
            return attached
        except BaseException as exc:
            if invoked:
                environment.recycle_required = True
                await await_drained(_recover_claim(execution, environment, residents, mounts, resources, claim, returned=returned))
            else:
                residents.preparation_absent(instance.home_id, claim)
            if isinstance(exc, (asyncio.CancelledError, AgentDenied, Refusal)):
                raise
            if isinstance(exc, SpaceOperationPending):
                raise ResidentFilesBusy(instance.name, cause=exc.cause) from exc
            raise ResidentStartFailed(instance.name) from exc


async def prepare_resident(execution, environment, residents, *, baseline_provider, app_config, run_id):
    from deerflow.agent_instances import runtime
    from deerflow.agent_instances.public_skills import capture_public_skills
    from deerflow.config.app_config import get_app_config

    try:
        app_config = app_config or await await_drained(run_file_io(get_app_config))
        capture = await await_drained(run_file_io(capture_public_skills, execution.definition, app_config))
        provider, mounts = await runtime.qualified_attachments(execution.authority.instances.files, baseline_provider=baseline_provider)
    except (AgentDenied, Refusal):
        raise
    except Exception as exc:
        raise ResidentStartFailed(execution.instance.name) from exc
    if residents.provider is not provider or residents.attachments is not mounts:
        raise ResidentStartFailed(execution.instance.name)
    attached = await _attachment(execution, environment, residents, mounts, run_id)
    try:
        client = await runtime._prepare_client(execution, provider, attached, capture, app_config, validate=False)
        environment.provider = client
    except BaseException as exc:
        environment.recycle_required = True
        await await_drained(residents.request_recycle(execution.instance.id, execution.instance.home_id, attached.id, "start-failed", exclude_run_id=run_id))
        if isinstance(exc, asyncio.CancelledError):
            raise
        raise ResidentStartFailed(execution.instance.name) from exc
    try:
        await _validate(execution)
    except BaseException as exc:
        try:
            await await_drained(run_file_io(client.close))
        except (Exception, asyncio.CancelledError):
            environment.recycle_required = True
            logger.warning("Client closure after denied preparation remains uncertain", exc_info=True)
        if isinstance(exc, AttachmentPending):
            raise ResidentStartFailed(execution.instance.name) from exc
        raise
    return client
