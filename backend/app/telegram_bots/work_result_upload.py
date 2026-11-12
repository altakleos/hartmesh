"""Bounded recipient exports and SDK uploads for one claimed result media part."""

import asyncio

from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError

from app.telegram_bots.adapter import LongRetryAfter, SendContext
from app.telegram_bots.files import OutputBatch, classify_output_photo
from app.telegram_bots.outbound import _hash_files, _logged, retry_export
from app.telegram_bots.outbound_upload import _forget_exception, upload_output
from app.telegram_bots.producer import DeliveryClaimRetired, DeliveryEligibility, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired, SourceUnavailable
from app.telegram_bots.work_file_exports import authorize_work_file_batch, prepare_work_file_export
from app.telegram_bots.work_result_delivery import change_result_part
from app.telegram_bots.work_result_files import classify_result_media, result_media
from deerflow.spaces.contract import SpaceDenied
from deerflow.spaces.filesystem import FileTooLarge
from deerflow.utils.file_io import await_drained


async def send_result_file(channel, saved, capture):
    services, expected = channel._services, saved
    source = services.result_notice_sources if "result_notice_basis" in saved.item.payload else services.work_result_sources
    require_write = getattr(source, "require_write", True)
    basis, read, recipe = await source._prepare(saved)
    part, files = saved.part, recipe["files"]
    original = saved.link

    def unchanged(value):
        return value.item == expected.item and value.part == expected.part and channel._same_scope(original, value.link)

    async def current():
        if not channel._callback_current(original):
            raise DeliveryClaimRetired()
        value = await services.outbox.capture_sending_part(**capture)
        if not unchanged(value):
            raise DeliveryClaimRetired()
        verdict = await services.eligibility(value.link, value.item, value.part)
        if not isinstance(verdict, DeliveryEligibility) or verdict.decision != "allow" or verdict.protect_content and not part.get("protect_content"):
            raise DeliveryClaimRetired()
        await source._prepare(value)
        if not channel._callback_current(original) or not unchanged(await services.outbox.capture_sending_part(**capture)):
            raise DeliveryClaimRetired()

    async def claimed(writer):
        from deerflow.persistence.telegram_bots.model import TelegramBotLinkRow, TelegramOutboxRow

        binding = await writer.get(TelegramBotLinkRow, original.id, with_for_update=True, populate_existing=True)
        row = await writer.get(TelegramOutboxRow, saved.item.id, with_for_update=True, populate_existing=True)
        value = services.outbox._sending(binding, row, **{key: value for key, value in capture.items() if key != "outbox_id"})
        if not unchanged(value):
            raise DeliveryClaimRetired()

    async def dispatch():
        await current()
        # All members, the request, exact claim and account/grants share the
        # last provider-final SELECT. Sequential permission reads are weaker.
        projection = await source.file_projection(expected, now=channel._now())
        await authorize_work_file_batch(exports, check_delivery=claimed, protect_content=bool(part.get("protect_content")), projection=projection, require_write=require_write)
        if not channel._callback_current(original):
            raise DeliveryClaimRetired()

    async def documents(reason):
        await current()
        await change_result_part(services.outbox, expected, templates=tuple(("document", [file]) for file in files), reason=reason)

    exports, failure, outcome = [], None, None
    reservation = batch = data = buffers = hash_inputs = None
    upload_started = False
    try:
        await current()
        ceiling = min(basis["result_files"]["max_bytes"], services.config.files.max_outbound_bytes)
        capacity = services.file_bytes_budget.capacity
        if len(files) * ceiling > capacity:
            templates = result_media(files, capacity=capacity, max_bytes=ceiling, protected=bool(part.get("protect_content")))
            await change_result_part(services.outbox, expected, templates=tuple((value["kind"], value["files"]) for value in templates), reason="capacity")
            raise DeliveryClaimRetired()
        for file in files:
            exports.append(
                await prepare_work_file_export(
                    source.reader,
                    original,
                    work_id=read.work["id"],
                    outcome_id=read.work["outcome_id"],
                    evidence_revision=read.work["outcome"]["evidence_revision"],
                    source_index=file["source_index"],
                    current=lambda: channel._callback_current(original),
                )
            )
        reservation = await services.file_bytes_budget.reserve_batch((ceiling,) * len(files), timeout=services.config.files.retry_max_seconds)
        batch = OutputBatch(reservation)
        await current()
        for index, export in enumerate(exports):
            closed = batch.begin_transport(index)
            try:
                projection = await source.file_projection(expected, now=channel._now())
                data = await retry_export(
                    lambda: export.read(max_bytes=ceiling, check_delivery=claimed, projection=projection, require_write=require_write),
                    current=current,
                    retry_seconds=services.config.files.retry_seconds,
                    retry_max_seconds=services.config.files.retry_max_seconds,
                )
                await batch.keep(index, data)
                data = None
            finally:
                data = None
                await await_drained(closed())
            hash_inputs = ((str(files[index]["source_index"]), batch.data(index)),)

            def hashed():
                nonlocal hash_inputs
                try:
                    return _hash_files(hash_inputs)
                finally:
                    hash_inputs = None

            hashes = await await_drained(asyncio.to_thread(hashed))
            if not await change_result_part(services.outbox, expected, hashes=hashes):
                failure = "file.out_failed"
                break
            expected = await services.outbox.capture_sending_part(**capture)
            await current()
        if failure is None and part["kind"] in {"photo", "album"}:
            photos = [await classify_output_photo(file["filename"], batch.data(index)) for index, file in enumerate(files)]
            if not all(photos):
                await current()
                await change_result_part(services.outbox, expected, templates=classify_result_media(files, photos), reason="photo-classification")
                raise DeliveryClaimRetired()
        if failure is None:
            buffers = tuple((file["filename"], batch.data(index)) for index, file in enumerate(files))
            upload_started = True
            outcome = await upload_output(
                channel.bot,
                kind=part["kind"],
                files=buffers,
                chat_id=saved.item.chat_id,
                message_thread_id=saved.item.message_thread_id,
                reply_to_message_id=None,
                silent=True,
                protect_content=bool(part.get("protect_content")),
                caption=None,
                plain=False,
                before_dispatch=dispatch,
            )
            buffers = None
    except (DeliveryClaimRetired, DeliveryRetry, PermanentDeliveryError, ProductSourceRetired):
        raise
    except asyncio.CancelledError as error:
        _forget_exception(error)
        raise
    except SpaceDenied as error:
        _forget_exception(error)
        failure = "file.no_access"
    except FileTooLarge as error:
        _forget_exception(error)
        failure = "file.too_large_out"
    except Exception as error:
        _forget_exception(error)
        failure = "ambiguous" if upload_started else "file.out_failed"
    finally:
        data = buffers = hash_inputs = None
        if batch is not None:
            await await_drained(batch.aclose())
        elif reservation is not None:
            await await_drained(reservation.aclose())
    if failure == "ambiguous":
        raise NetworkError("Result file acknowledgement unavailable")
    if failure is not None:
        if part["kind"] == "album":
            await documents("photo-classification")
            raise DeliveryClaimRetired()
        raise SourceUnavailable(source_ref={"key": failure, "values": {"file": files[0]["filename"]}})
    if outcome is None:
        raise PermanentDeliveryError()
    if outcome.failure is None:
        channel._chat_message_sent(saved.item.chat_id, saved.item.message_thread_id)
        await channel._after_send_success()
        return list(outcome.message_ids)
    channel._failure()
    context = SendContext(original.id, saved.item.chat_id, original.binding_generation, channel.transport_revision, "bound_message")
    method = {"document": "sendDocument", "photo": "sendPhoto", "album": "sendMediaGroup"}[part["kind"]]
    diagnostic = (
        LongRetryAfter(outcome.retry_after)
        if outcome.failure == "retry"
        else InvalidToken("Unauthorized")
        if outcome.failure == "token"
        else Forbidden("File send refused")
        if outcome.failure == "forbidden"
        else NetworkError("Result file delivery unavailable")
    )
    channel._log_part_failure(method, diagnostic, expected)
    if outcome.failure == "retry":
        await channel._send_refused_by_limit()
        raise DeliveryRetry(outcome.retry_after)
    if outcome.failure == "forbidden":
        await channel._forbidden(context)
        raise _logged(Forbidden("Bound result file delivery refused"))
    if outcome.failure == "token":
        verdict = await channel.verify_token()
        if verdict.status == "unverified":
            await channel._defer_invalid_token(expected, verdict, http_status=outcome.http_status)
        if verdict.status == "valid":
            raise _logged(PermanentDeliveryError())
        raise DeliveryClaimRetired()
    if outcome.failure in {"network", "ack"}:
        raise _logged(NetworkError("Result file acknowledgement unavailable"))
    if outcome.failure == "topic":
        raise _logged(BadRequest("message thread not found"))
    if part["kind"] in {"photo", "album"}:
        await documents("photo-rejected")
        raise DeliveryClaimRetired()
    raise SourceUnavailable(source_ref={"key": "file.out_failed", "values": {"file": files[0]["filename"]}})
