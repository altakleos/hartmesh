class PermanentNotificationError(RuntimeError):
    """A notification cannot ever be delivered without external state changing."""


class McpTaskNotificationLineageConflictError(PermanentNotificationError):
    """A stable notification key was reused with different source evidence."""

    code = "mcp_task_notification_lineage_conflict"

    def __init__(self) -> None:
        super().__init__(self.code)
