import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

/**
 * What the page says when nothing is going to run these schedules.
 *
 * A tenant-class upgrade found a task showing "enabled, next run
 * September 9" on September 17, on a Gateway whose scheduler had never
 * started. The row was accurate and the page was still misleading: a date with
 * no context reads as a promise. So the page now says the missing half — that
 * scheduling is off and who turns it back on — and marks a scheduled time that
 * has already passed.
 */
const schedulerState = rs.hoisted(() => ({
  value: {
    data: {
      version: 1,
      running: false,
      configured: false,
      state: "disabled_by_configuration" as const,
    },
    error: null,
  } as { data: unknown; error: null },
}));

const task = rs.hoisted(() => ({ value: null as unknown }));

const TASK = {
  id: "task-1",
  thread_id: null,
  context_mode: "fresh_thread_per_run" as const,
  title: "Monthly report",
  prompt: "Build last month's report",
  schedule_type: "cron" as const,
  schedule_spec: { cron: "0 9 1 * *" },
  timezone: "UTC",
  status: "enabled" as const,
  // Deliberately in the past: this is the shape the upgrade found.
  next_run_at: "2020-01-09T09:00:00Z",
  last_run_at: "2020-01-08T09:00:00Z",
  last_run_id: null,
  last_thread_id: null,
  last_error: null,
  run_count: 3,
  created_at: "2019-12-01T00:00:00Z",
  updated_at: "2020-01-08T09:00:00Z",
};

rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams(),
  usePathname: () => "/workspace/scheduled-tasks",
  useRouter: () => ({
    push: rs.fn(),
    replace: rs.fn(),
    back: rs.fn(),
    prefetch: rs.fn(),
  }),
}));
rs.mock("@/core/features", () => ({
  useDeveloperSurfacesVisible: () => false,
  useDocumentTitle: () => undefined,
}));
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceBody: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceHeader: () => null,
}));
rs.mock("@/core/scheduled-tasks/hooks", () => {
  const mutation = () => ({ mutate: rs.fn(), isPending: false });
  return {
    useScheduledTasks: () => ({ data: [task.value], error: null }),
    useSchedulerState: () => schedulerState.value,
    useThreadScheduledTasks: () => ({ data: [], error: null }),
    useScheduledTaskRuns: () => ({ data: [], error: null }),
    useCreateScheduledTask: mutation,
    useUpdateScheduledTask: mutation,
    useDeleteScheduledTask: mutation,
    usePauseScheduledTask: mutation,
    useResumeScheduledTask: mutation,
    useTriggerScheduledTask: mutation,
  };
});

import ScheduledTasksPage from "@/app/workspace/scheduled-tasks/page";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

function renderPage() {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <ScheduledTasksPage />
    </I18nContext.Provider>,
  );
}

beforeEach(() => {
  task.value = TASK;
});

afterEach(() => {
  cleanup();
  schedulerState.value = {
    data: {
      version: 1,
      running: false,
      configured: false,
      state: "disabled_by_configuration",
    },
    error: null,
  };
});

describe("scheduled tasks page when the scheduler is not running", () => {
  it("says scheduling is off, and that the saved schedules are still there", () => {
    renderPage();

    const notice = screen.getByTestId("scheduler-off-notice");
    expect(notice.textContent).toContain(
      enUS.scheduledTasks.scheduler.offTitle,
    );
    expect(notice.textContent).toContain(
      enUS.scheduledTasks.scheduler.offDescription,
    );
  });

  it("distinguishes a scheduler that is turned on but not running", () => {
    schedulerState.value = {
      data: {
        version: 1,
        running: false,
        configured: true,
        state: "not_running",
      },
      error: null,
    };
    renderPage();

    const notice = screen.getByTestId("scheduler-off-notice");
    // Title and body from the same branch: a page that exists to end a status
    // contradiction must not open one of its own.
    expect(notice.textContent).toContain(
      enUS.scheduledTasks.scheduler.stoppedTitle,
    );
    expect(notice.textContent).toContain(
      enUS.scheduledTasks.scheduler.stoppedDescription,
    );
    expect(notice.textContent).not.toContain(
      enUS.scheduledTasks.scheduler.offTitle,
    );
  });

  it("distinguishes a workspace with no scheduler at all", () => {
    schedulerState.value = {
      data: {
        version: 1,
        running: false,
        configured: false,
        state: "unavailable",
      },
      error: null,
    };
    renderPage();

    const notice = screen.getByTestId("scheduler-off-notice");
    expect(notice.textContent).toContain(
      enUS.scheduledTasks.scheduler.unavailableTitle,
    );
    expect(notice.textContent).toContain(
      enUS.scheduledTasks.scheduler.unavailableDescription,
    );
  });

  it("marks a next run that has already passed", () => {
    renderPage();

    expect(
      screen.getByTestId("scheduled-task-next-run-overdue").textContent,
    ).toContain(enUS.scheduledTasks.scheduler.overdue);
  });

  it("does not call a paused task overdue", () => {
    // Pausing leaves `next_run_at` where it was, so a task somebody paused on
    // Monday would otherwise be flagged in warning amber on Thursday for doing
    // exactly what they asked.
    schedulerState.value = {
      data: { version: 1, running: true, configured: true, state: "running" },
      error: null,
    };
    task.value = { ...TASK, status: "paused" as const };
    renderPage();

    expect(screen.queryByTestId("scheduled-task-next-run-overdue")).toBeNull();
  });

  it("stays quiet while the scheduler state is still loading", () => {
    // A banner that flashes "turned off" on every page load would be its own
    // lie, and the one it tells is about the product being broken.
    schedulerState.value = { data: undefined, error: null };
    renderPage();

    expect(screen.queryByTestId("scheduler-off-notice")).toBeNull();
  });

  it("shows nothing when the scheduler is running", () => {
    schedulerState.value = {
      data: { version: 1, running: true, configured: true, state: "running" },
      error: null,
    };
    renderPage();

    expect(screen.queryByTestId("scheduler-off-notice")).toBeNull();
  });
});
