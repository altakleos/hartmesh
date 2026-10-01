# The scheduler the Gateway runs is the service with the wall-clock bound on a
# scheduled run added (``run_time_bound``); ``service.ScheduledTaskService`` is
# the service without it.
from .run_time_bound import TimeBoundedScheduledTaskService as ScheduledTaskService

__all__ = ["ScheduledTaskService"]
