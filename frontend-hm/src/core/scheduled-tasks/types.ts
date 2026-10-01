export type ScheduledTask = {
  id: string;
  thread_id: string | null;
  context_mode: "fresh_thread_per_run" | "reuse_thread";
  title: string;
  prompt: string;
  schedule_type: "once" | "cron";
  schedule_spec: Record<string, unknown>;
  timezone: string;
  status:
    | "enabled"
    | "paused"
    | "running"
    | "completed"
    | "failed"
    | "cancelled";
  next_run_at: string | null;
  last_run_at: string | null;
  last_run_id: string | null;
  last_thread_id: string | null;
  last_error: string | null;
  run_count: number;
  created_at: string;
  updated_at: string;
};

export type ScheduledTaskRun = {
  id: string;
  task_id: string;
  thread_id: string;
  run_id: string | null;
  scheduled_for: string;
  trigger: "scheduled" | "manual";
  status:
    | "queued"
    | "launching"
    | "running"
    | "success"
    | "failed"
    | "skipped"
    | "interrupted";
  error: string | null;
  attempt_count: number;
  started_at: string | null;
  finished_at: string | null;
  created_at: string;
};

/**
 * Whether anything is actually going to run the schedules.
 *
 * A task row carries its own status and its next run time, and neither can say
 * that no scheduler is polling for it — which is how a workspace ends up
 * showing "enabled, next run" on a date that has already passed. `running` is
 * read from the Gateway's running service, `configured` from its configuration,
 * and they can legitimately disagree.
 */
export type SchedulerState = {
  version: number;
  running: boolean;
  configured: boolean;
  /** The single discriminator to branch on; `running` is it, spelled as a boolean. */
  state:
    | "running"
    | "disabled_by_configuration"
    | "not_running"
    | "unavailable";
};
