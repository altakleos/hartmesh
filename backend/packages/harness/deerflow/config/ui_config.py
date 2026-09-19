"""What the workspace shows before anyone has asked for anything.

Two presentation settings the deployment owns: which starters Home offers, and
whether the developer-facing screens are offered to people who are not
administrators. `profile` hides those screens and changes no route. The
endpoints behind them allow exactly what they allowed before: `authorization`
has no permission covering these APIs, so it is not the lever that closes them.
What limits a person there is their `system_role`, which the API already checks.

Both fields are read through ``get_config`` on each request, so an edit to
``config.yaml`` reaches the next page load without a Gateway restart.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

#: How many starters Home will offer. Past this a grid stops being a choice.
MAX_STARTERS = 6

MAX_STARTER_TITLE_CHARS = 60
MAX_STARTER_PROMPT_CHARS = 2000

#: Characters that would make a tile read as something other than what it does.
#: Written as escapes because every one of them is invisible in a source file:
#: the bidi embeddings, overrides and isolates reorder the glyphs around them,
#: and the zero-width marks hide inside a word. Control characters go with them.
_REORDERING_CHARS = frozenset(
    "\u202a\u202b\u202c\u202d\u202e"  # LRE, RLE, PDF, LRO, RLO
    "\u2066\u2067\u2068\u2069"  # LRI, RLI, FSI, PDI
    "\u200b\u200e\u200f\ufeff"  # ZWSP, LRM, RLM, ZWNBSP
)


def first_control_or_reordering_character(value: str, *, allow_newlines: bool) -> str | None:
    """The first character that would make text read as something other than what it is, or None."""
    for char in value:
        if char == "\n" and allow_newlines:
            continue
        if ord(char) < 32 or ord(char) == 127 or char in _REORDERING_CHARS:
            return char
    return None


class StarterConfig(BaseModel):
    """One thing Home offers to someone who has not typed anything yet."""

    # Strip before the length constraint, so a `prompt: >-` block scalar's
    # trailing newline does not spend one of the 2000 characters. Frozen
    # because `DEFAULT_STARTERS` is shared by every `UiConfig` that takes it.
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True, frozen=True)

    id: str = Field(
        ...,
        pattern=r"^[a-z][a-z0-9-]*$",
        max_length=64,
        description="Stable identifier for this starter; it is the grid's key.",
    )
    title: str = Field(..., max_length=MAX_STARTER_TITLE_CHARS, description="The words on the tile.")
    prompt: str = Field(
        ...,
        max_length=MAX_STARTER_PROMPT_CHARS,
        description="What the message box is filled with when the tile is chosen. Nothing is sent; the person adds their files and words first.",
    )

    @field_validator("title", "prompt")
    @classmethod
    def _must_say_something(cls, value: str, info: ValidationInfo) -> str:
        if not value:
            raise ValueError("must not be blank")
        # A tile whose glyphs run in a different order than its prompt reads as
        # one action and performs another. A prompt may hold newlines; a title,
        # which is one line on a button, may not.
        char = first_control_or_reordering_character(value, allow_newlines=info.field_name == "prompt")
        if char is not None:
            raise ValueError(f"must not contain the control or reordering character {char!r}")
        return value


#: What `profile: business` opens on before an operator writes their own.
#: Deliberately generic: these name what the workspace can do, not who is
#: using it. A `developer` deployment resolves to no grid at all.
DEFAULT_STARTERS = (
    StarterConfig(
        id="business-review",
        title="Monthly business review",
        prompt="Build a monthly business review from the spreadsheet I am about to attach, and give me the PDF, Word and Excel versions.",
    ),
    StarterConfig(
        id="summarize-document",
        title="Summarize a document",
        prompt="Read the document I am about to attach and summarize it: what it says, what it asks of me, and anything I should check.",
    ),
    StarterConfig(
        id="ask-a-spreadsheet",
        title="Ask about a spreadsheet",
        prompt="Look at the spreadsheet I am about to attach and answer questions about it. Start by telling me what is in it.",
    ),
)


class UiConfig(BaseModel):
    """Deployment-owned presentation settings for the workspace."""

    model_config = ConfigDict(extra="forbid")

    profile: Literal["business", "developer"] = Field(
        default="developer",
        description=(
            "'developer' offers every screen to everyone, which is what a deployment that says nothing gets. "
            "'business' keeps the skills, tools, subagents and integrations screens, and the scheduled-task recipe chips, for administrators. "
            "Presentation only: it hides screens and changes no route; `system_role` is what limits a person."
        ),
    )
    starters: list[StarterConfig] | None = Field(
        default=None,
        max_length=MAX_STARTERS,
        description="What Home offers before anyone types. Unset takes the profile's default; an empty list shows no grid.",
    )

    @model_validator(mode="after")
    def _resolve_starters(self) -> "UiConfig":
        # Unset is not empty: a `developer` deployment that said nothing keeps
        # exactly the Home it had, and `business` gets a grid without being
        # asked twice.
        if self.starters is None:
            self.starters = list(DEFAULT_STARTERS) if self.profile == "business" else []
        seen = {starter.id for starter in self.starters}
        if len(seen) != len(self.starters):
            raise ValueError("every starter needs its own id")
        return self
