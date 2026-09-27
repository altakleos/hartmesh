"""The tag that marks a tool as one that works in the sandbox.

The tag is *written* where the sandbox tools are defined (``tools.py``) and
*read* by policy that orders sandbox work, such as a skill's ``first-command``
(``SkillToolPolicyMiddleware``). Readers ask the tool, not a list of names, so
a sandbox tool added later is covered from the moment it is tagged.

A leaf module like ``deerflow.tools.mcp_metadata``: it imports nothing from the
sandbox, so the middleware can read the tag without loading the tools.
"""

from __future__ import annotations

from typing import Any

SANDBOX_TOOL_METADATA_KEY = "deerflow_sandbox"


def tag_sandbox_tool(tool: Any) -> Any:
    """Mark ``tool`` as working in the sandbox. Mutates in place and returns it."""
    tool.metadata = {**(tool.metadata or {}), SANDBOX_TOOL_METADATA_KEY: True}
    return tool


def is_sandbox_tool(tool: object) -> bool:
    """True when ``tool`` carries the tag written by :func:`tag_sandbox_tool`."""
    return (getattr(tool, "metadata", None) or {}).get(SANDBOX_TOOL_METADATA_KEY) is True
