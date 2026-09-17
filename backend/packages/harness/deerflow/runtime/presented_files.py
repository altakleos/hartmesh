"""The message-level record of a presentation.

A tool that hands files to the user returns the ``artifacts`` state update
the reducer merges, and tags the ``ToolMessage`` it answers with, under
:data:`PRESENTED_FILES_KEY`, with the virtual paths it presented. The tag is
what distinguishes a presentation -- an act the model asked for, through
``present_files`` or a producing tool's ``present`` argument -- from a file
that merely landed in ``artifacts`` as a side effect (a browser tool's
screenshot). The delivery fence, the archive and the evidence bundle count
only tagged presentations; readers that walk messages rather than state (the
IM channels, the browser) read the same tag.

The key is server-owned: the Gateway strips it from any client-supplied
message, so it is always a fact about what the host delivered. This module
is a leaf on purpose -- the journal, the tools and the Gateway all import it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Final

#: ``ToolMessage.additional_kwargs`` key carrying the virtual paths a tool
#: result presented.
PRESENTED_FILES_KEY: Final = "presented_files"


def presented_files_of(message: Any) -> list[str]:
    """The paths a tool message presented, in order, without repeats; empty when untagged.

    Reads the tag off a message object or a message dict, types checked: a
    string, a non-list or a list with non-string entries presents nothing.
    """
    if isinstance(message, Mapping):
        kwargs = message.get("additional_kwargs")
    else:
        kwargs = getattr(message, "additional_kwargs", None)
    presented = kwargs.get(PRESENTED_FILES_KEY) if isinstance(kwargs, Mapping) else None
    if not isinstance(presented, list):
        return []
    return list(dict.fromkeys(path for path in presented if isinstance(path, str) and path))
