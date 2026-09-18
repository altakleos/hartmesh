"""What the sandbox image guarantees is importable.

Why this exists
---------------
In a released-profile qualification run the model spent tool calls asking the sandbox what it had.
The successful "Create pdf about muse agent" run opened with::

    python3 -c "import reportlab; ..." ; python3 -c "import fpdf; ..." ;
    python3 -c "import weasyprint; ..."

and the failed run's very first act was the same question -- which is the call
that arrived with a shell command where its tool name belonged and ended the run
(DF23). A probe is a round trip through the sandbox and a model call to read the
answer, for a fact the image settles at build time.

So the fact is stated where the model plans, instead of being discovered per
turn. ``docker/sandbox/Dockerfile`` asserts these exact imports at build time --
the image does not publish if one is missing -- so this list is a promise the
build already keeps, and ``tests/test_sandbox_preinstalled.py`` fails if the two
ever disagree.

This is a statement about the image, not a menu: anything else the model needs
is still an ordinary import that may fail, and it should handle that rather than
check first.
"""

from __future__ import annotations

__all__ = ["GUARANTEED_IMPORTS", "preinstalled_libraries_section"]

#: Import names the sandbox image asserts at build time, in the Dockerfile's
#: own order. Import names, not distribution names -- what a model would type.
GUARANTEED_IMPORTS: tuple[str, ...] = (
    "docx",
    "duckdb",
    "xlrd",
    "weasyprint",
    "xlsxwriter",
    "matplotlib",
    "pandas",
    "openpyxl",
)

#: The distribution behind an import name, where they differ and the difference
#: is the kind of thing a model gets wrong.
_DISTRIBUTION_NOTES = {"docx": "python-docx"}


def preinstalled_libraries_section() -> str:
    """One line for the prompt, built from the list the image asserts."""
    names = ", ".join(f"{name} ({_DISTRIBUTION_NOTES[name]})" if name in _DISTRIBUTION_NOTES else name for name in GUARANTEED_IMPORTS)
    return (
        f"- Always importable in the sandbox: {names}. "
        "Do not run commands to check whether these exist — the image guarantees them. "
        "For anything else, import it and handle the failure if it is absent; a probe costs a sandbox round trip and a model call to read its answer."
    )
