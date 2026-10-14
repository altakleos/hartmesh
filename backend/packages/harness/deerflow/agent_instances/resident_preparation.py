"""Opt-in preparation; physical effects remain owned through cancellation."""

import asyncio
import logging
from uuid import uuid4

from deerflow.agent_instances.contract import AgentDenied, Refusal, ResidentFilesBusy, ResidentFilesFull, ResidentStartFailed, ResidentStartingElsewhere
from deerflow.runtime.turn_phases import TurnPhase, phase_span
from deerflow.spaces.attachments import AttachmentPending, ResourceMount
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
            residents.claimed(instance, state.id)
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


async def _recover_claim(execution, environment, residents, mounts, resources, claim):
    """Only exact correlation authorizes cleanup of an invoked preparation."""
    instance = execution.instance
    try:
        state = await mounts.inspect_preparation(actor=instance.principal, incarnation=claim.incarnation, resources=resources, host_id=claim.host_id)
        if state is None:
            # No SQL intent is insufficient if the qualified host disappeared.
            await await_drained(run_file_io(mounts.provider._same_host))
            residents.preparation_absent(instance.home_id, claim)
            return
        if residents.preparation_claimed(instance, claim, state.id, attempt_id=getattr(execution.work, "attempt_id", None)):
            environment.attachment_id = state.id
            await mounts.contain_host(state.id, "start-failed")
    except Exception:
        logger.warning("Preparation outcome remains retained for Home %s", instance.home_id, exc_info=True)


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
        await residents.admit(instance, work=False, run_id=run_id)
        if entry.preparation is not None:
            raise ResidentStartingElsewhere(instance.name)
        try:
            attached = await _resume(execution, residents, mounts, resources)
        except (AgentDenied, Refusal):
            raise
        except SpaceOperationPending as exc:
            raise ResidentFilesBusy(instance.name, cause=exc.cause) from exc
        except Exception as exc:
            raise ResidentStartFailed(instance.name) from exc
        if attached is not None:
            residents.claimed(instance, attached.id, attempt_id=getattr(execution.work, "attempt_id", None))
            environment.attachment_id = attached.id
            return attached
        claim = residents.preparing(instance, incarnation=uuid4().hex, generation=execution.home_generation, host_id=mounts.provider.host_id)
        invoked = False
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
            if not residents.preparation_claimed(instance, claim, attached.id, attempt_id=getattr(execution.work, "attempt_id", None)):
                raise RuntimeError("The exact preparation claim changed")
            environment.attachment_id = attached.id
            return attached
        except BaseException as exc:
            if invoked:
                environment.recycle_required = True
                await await_drained(_recover_claim(execution, environment, residents, mounts, resources, claim))
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
