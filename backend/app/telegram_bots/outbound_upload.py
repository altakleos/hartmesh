"""One already-authorized upload through the adapter's pinned SDK transport.

The caller owns source qualification, the whole-batch byte lease and durable
acknowledgements. This leaf neither reads files nor retries durable delivery.
"""

from __future__ import annotations

import asyncio
import traceback
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from telegram import InputFile, InputMediaPhoto
from telegram.constants import ParseMode
from telegram.error import BadRequest, Forbidden, InvalidToken, NetworkError, RetryAfter, TelegramError

from app.telegram_bots.adapter import LiveRequestFence, LongRetryAfter, _invalid_status
from deerflow.utils.file_io import await_drained

# Telegram answers only after it has taken the whole file. Under the SDK's 5 seconds a large
# upload that succeeded reads as a failure, and the file is sent again.
UPLOAD_READ_SECONDS = 120


class _UploadReader:
    """A memory-only public SDK handle, emptied after its upload drains.

    HTTPX may retain multipart fields in response cycles or failed pool entries.
    A closed handle retains no leased body, and each emitted copy is <=64 KiB.
    """

    def __init__(self, data: bytes):
        self._data, self._position = data, 0

    @property
    def closed(self):
        return self._data is None

    def _open(self):
        if self.closed:
            raise ValueError("closed-output-upload")

    def tell(self):
        self._open()
        return self._position

    def seek(self, offset, whence=0):
        self._open()
        if type(offset) is not int or type(whence) is not int or whence not in {0, 1, 2}:
            raise ValueError("invalid-output-seek")
        position = offset if whence == 0 else self._position + offset if whence == 1 else len(self._data) + offset
        if position < 0:
            raise ValueError("invalid-output-seek")
        self._position = position
        return position

    def read(self, size=-1):
        self._open()
        if type(size) is not int:
            raise ValueError("invalid-output-read")
        size = 65_536 if size < 0 else min(size, 65_536)
        end = min(len(self._data), self._position + size)
        # Always copy only the bounded transport chunk. A small body's full
        # slice must not hand its original leased object to a retained writer.
        result = bytes(memoryview(self._data)[self._position : end])
        self._position = max(self._position, end)
        return result

    def close(self):
        self._data, self._position = None, 0


@dataclass(frozen=True)
class UploadOutcome:
    message_ids: tuple[int, ...] = ()
    failure: str | None = None
    retry_after: float | None = None
    http_status: int | None = None


def _forget_exception(error: BaseException) -> None:
    """Do not carry SDK multipart/request bodies into a durable retry exception."""
    seen = set()
    pending = [error]
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if current.__cause__ is not None:
            pending.append(current.__cause__)
        if current.__context__ is not None:
            pending.append(current.__context__)
        if current.__traceback__ is not None:
            traceback.clear_frames(current.__traceback__)
        current.__traceback__ = current.__cause__ = current.__context__ = None


def _make_uploads(names, readers):
    # InputFile may initialize the OS MIME database. Executor arguments contain
    # only handles; their cells are emptied after this native unit drains.
    return tuple(InputFile(reader, filename=name, attach=True, read_file_handle=False) for name, reader in zip(names, readers, strict=True))


async def upload_output(
    bot,
    *,
    kind: str,
    files: tuple[tuple[str, bytes], ...],
    chat_id: int,
    message_thread_id: int | None,
    reply_to_message_id: int | None,
    silent: bool,
    protect_content: bool,
    caption: str | None,
    plain: bool,
    before_dispatch: Callable[[], Awaitable[None]],
) -> UploadOutcome:
    if (
        kind not in {"document", "photo", "album"}
        or type(files) is not tuple
        or not (2 <= len(files) <= 10 if kind == "album" else len(files) == 1)
        or any(
            type(member) is not tuple or len(member) != 2 or type(member[0]) is not str or not member[0] or member[0] in {".", ".."} or any(char in member[0] for char in ("/", "\\", "\x00")) or type(member[1]) is not bytes
            for member in files
        )
        or caption is not None
        and (kind != "document" or type(caption) is not str)
        or not callable(before_dispatch)
    ):
        raise ValueError("invalid-output-upload")
    readers = ()
    uploads = media = message = kwargs = None
    try:
        readers = tuple(_UploadReader(data) for _name, data in files)
        uploads = await await_drained(asyncio.to_thread(_make_uploads, tuple(name for name, _data in files), readers))
        kwargs = dict(
            chat_id=chat_id,
            message_thread_id=message_thread_id,
            reply_to_message_id=reply_to_message_id,
            disable_notification=silent,
            protect_content=protect_content,
            read_timeout=UPLOAD_READ_SECONDS,
            rate_limit_args=LiveRequestFence(before_dispatch),
        )
        if kind == "document":
            message = await bot.send_document(document=uploads[0], caption=caption, parse_mode=None if plain else ParseMode.HTML, **kwargs)
        elif kind == "photo":
            message = await bot.send_photo(photo=uploads[0], **kwargs)
        else:
            media = tuple(InputMediaPhoto(upload) for upload in uploads)
            message = await bot.send_media_group(media=media, **kwargs)
        messages = message if kind == "album" else (message,)
        ids = tuple(getattr(member, "message_id", None) for member in messages)
        if len(ids) != len(files) or any(type(identity) is not int or identity <= 0 for identity in ids) or len(set(ids)) != len(ids):
            return UploadOutcome(failure="ack")
        return UploadOutcome(message_ids=ids)
    except (LongRetryAfter, RetryAfter) as error:
        seconds = error.seconds if isinstance(error, LongRetryAfter) else error._retry_after.total_seconds()
        _forget_exception(error)
        return UploadOutcome(failure="retry", retry_after=seconds)
    except InvalidToken as error:
        status = _invalid_status(error)
        _forget_exception(error)
        return UploadOutcome(failure="token", http_status=status)
    except Forbidden as error:
        _forget_exception(error)
        return UploadOutcome(failure="forbidden")
    except BadRequest as error:
        description = str(error).lower()
        failure = "entities" if "can't parse entities" in description else "topic" if "message thread not found" in description else "anchor" if "message to be replied" in description else "payload"
        description = None
        _forget_exception(error)
        return UploadOutcome(failure=failure)
    except (NetworkError, TelegramError) as error:
        _forget_exception(error)
        return UploadOutcome(failure="network")
    except BaseException as error:
        _forget_exception(error)
        raise
    finally:
        for reader in readers:
            reader.close()
        readers = files = uploads = media = message = kwargs = None
