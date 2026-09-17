import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { cleanup, render, screen } from "@testing-library/react";

/**
 * Who is offered the scheduled-task recipes.
 *
 * The page itself is not gated — recurring work is not a developer capability,
 * and "send me last week's numbers every Monday" is the point of it. The
 * recipes are: they are about repositories, issue triage and trending
 * releases. The end-to-end suite runs with authentication disabled, where
 * every session is an administrator, so only this can tell the two apart.
 */
const surfaces = rs.hoisted(() => ({ developerSurfacesVisible: true }));

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
  useDeveloperSurfacesVisible: () => surfaces.developerSurfacesVisible,
}));
// The page's chrome (sidebar, header) is not what this is about, and it
// needs providers the recipe row does not.
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
  const query = () => ({ data: [], error: null });
  const mutation = () => ({ mutate: rs.fn(), isPending: false });
  return {
    useScheduledTasks: query,
    // A running scheduler, so this test sees the page without its
    // scheduling-is-off banner.
    useSchedulerState: () => ({
      data: { version: 1, running: true, configured: true, state: "running" },
      error: null,
    }),
    useThreadScheduledTasks: query,
    useScheduledTaskRuns: query,
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

afterEach(() => {
  cleanup();
  surfaces.developerSurfacesVisible = true;
});

describe("ScheduledTasksPage recipes", () => {
  it("offers the recipes where the developer surfaces are visible", () => {
    renderPage();

    expect(screen.getByTestId("schedule-recipes")).toBeTruthy();
  });

  it("keeps the recipes for administrators without taking the page away", () => {
    surfaces.developerSurfacesVisible = false;

    renderPage();

    expect(screen.queryByTestId("schedule-recipes")).toBeNull();
    // The page is still the person's: they write their own prompt.
    expect(screen.getByTestId("scheduled-task-create-form")).toBeTruthy();
  });
});
