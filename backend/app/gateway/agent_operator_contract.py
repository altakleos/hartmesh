"""Operator command refusal, shared without importing Gateway startup."""


class CommandRefused(Exception):
    note = "refused; nothing was changed"

    def __init__(self, message: str, **fields):
        super().__init__(message)
        self.fields = fields


class CommandFailed(CommandRefused):
    """A known failure after actual or possible command effects."""

    note = "failed; run the verb again"
