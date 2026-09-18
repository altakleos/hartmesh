"""The prompt's promise about the sandbox is the image's own promise.

The model was told nothing about what the sandbox has, so it asked -- with tool
calls, every turn. That qualification's successful repetition opened by importing
reportlab, fpdf and weasyprint to see which existed; the failed run's first act
was the same question, and that is the call whose name arrived as a shell
command and ended the run.

Stating the answer in the prompt only helps if it is true, and a list typed into
a prompt is exactly the kind of thing that rots. ``docker/sandbox/Dockerfile``
already asserts these imports at build time -- the image does not publish if one
is missing -- so this test makes that assertion the authority and fails the
moment the two disagree.
"""

from __future__ import annotations

import re
from pathlib import Path

from deerflow.sandbox.preinstalled import GUARANTEED_IMPORTS, preinstalled_libraries_section

DOCKERFILE = Path(__file__).resolve().parents[2] / "docker" / "sandbox" / "Dockerfile"


def _imports_asserted_by_the_image() -> list[str]:
    """The import names the sandbox build proves, read from the build itself."""
    text = DOCKERFILE.read_text(encoding="utf-8")
    # The build asserts imports in more than one place (a later line re-imports
    # matplotlib.pyplot to prove the font cache). The library set is the one
    # that imports several names at once; requiring exactly one such line means
    # a second set added later fails here rather than being silently ignored.
    matches = [m for m in re.findall(r"""python3 -c ['"]import ([^'"]+)['"]""", text) if "," in m]
    assert len(matches) == 1, f"expected exactly one multi-library import assertion in {DOCKERFILE}, found {len(matches)}"
    return [name.strip() for name in matches[0].split(",")]


def test_the_prompt_promises_exactly_what_the_image_proves() -> None:
    assert list(GUARANTEED_IMPORTS) == _imports_asserted_by_the_image()


def test_the_section_names_every_library_and_says_not_to_probe() -> None:
    section = preinstalled_libraries_section()
    for name in GUARANTEED_IMPORTS:
        assert name in section
    assert "python-docx" in section, "the import name and the distribution name differ here, which is where a model guesses wrong"
    assert "Do not run commands to check whether these exist" in section


def test_the_section_reaches_the_lead_agent_prompt() -> None:
    from deerflow.agents.lead_agent.prompt import apply_prompt_template

    assert preinstalled_libraries_section() in apply_prompt_template()


def test_it_is_a_statement_about_the_image_not_a_menu() -> None:
    # A model that reads this as "these are the only libraries" would stop
    # trying anything else; the line has to leave the ordinary case open.
    section = preinstalled_libraries_section()
    assert "import it and handle the failure" in section
