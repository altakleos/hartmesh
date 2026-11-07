import { afterEach, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
} from "@testing-library/react";
import { useEffect } from "react";

import { SubtaskCard } from "@/components/workspace/messages/subtask-card";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { fetchSubtaskSteps } from "@/core/tasks/api";
import {
  SubtasksProvider,
  useSubtaskContext,
  useUpdateSubtask,
} from "@/core/tasks/context";
import type { Subtask } from "@/core/tasks/types";

import { FlipDisplay as StaticFlipDisplay } from "../../../helpers/flip-display";

rs.mock("@/components/workspace/flip-display", () => ({
  FlipDisplay: (props: Parameters<typeof StaticFlipDisplay>[0]) => (
    <StaticFlipDisplay {...props} />
  ),
}));
rs.mock("@/core/tasks/api", () => ({ fetchSubtaskSteps: rs.fn() }));
afterEach(() => {
  cleanup();
  rs.clearAllMocks();
});

function setup(history = false) {
  let publish!: ReturnType<typeof useUpdateSubtask>;
  let read!: () => Subtask;
  function Messages() {
    const update = useUpdateSubtask();
    const { tasks, tasksRef, setTasks } = useSubtaskContext();
    useEffect(() => {
      setTasks({
        "task-1": {
          id: "task-1",
          description: "Research",
          prompt: "Find sources",
          subagent_type: "researcher",
          status: "in_progress",
        },
      });
    }, [setTasks]);
    useEffect(() => {
      publish = update;
      read = () => tasksRef.current["task-1"]!;
    });
    return tasks["task-1"] ? (
      <SubtaskCard
        taskId="task-1"
        threadId={history ? "thread-1" : undefined}
        runId={history ? "run-1" : undefined}
        isLoading
      />
    ) : null;
  }
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  client.setQueryData(["models"], {
    models: [],
    token_usage: { enabled: true },
  });
  const view = render(
    <QueryClientProvider client={client}>
      <I18nContext.Provider
        value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
      >
        <SubtasksProvider>
          <Messages />
        </SubtasksProvider>
      </I18nContext.Provider>
    </QueryClientProvider>,
  );
  return {
    ...view,
    publish: (task: Partial<Subtask> & { id: string }) => publish(task),
    read: () => read(),
    complete: () =>
      publish({
        id: "task-1",
        description: "Research the topic",
        prompt: "Find and compare reliable sources",
      }),
  };
}

it("merges a delayed history backfill with live steps after argument synchronization", async () => {
  let finishBackfill!: (steps: NonNullable<Subtask["steps"]>) => void;
  rs.mocked(fetchSubtaskSteps).mockImplementation(
    () =>
      new Promise((resolve) => {
        finishBackfill = resolve;
      }),
  );
  const view = setup(true);
  fireEvent.click(screen.getByRole("button"));
  expect(fetchSubtaskSteps).toHaveBeenCalledWith("thread-1", "run-1", "task-1");
  const live = {
    kind: "tool" as const,
    message_index: 2,
    text: "live",
    tool_name: "web_search",
  };
  const historical = {
    kind: "tool" as const,
    message_index: 1,
    text: "history",
    tool_name: "read_file",
  };
  await act(async () => view.publish({ id: "task-1", steps: [live] }));
  await act(async () => view.complete());
  await act(async () => finishBackfill([historical]));
  expect(view.read().steps).toEqual([historical, live]);
  expect(view.container.textContent).toContain("read_file");
  expect(view.container.textContent).toContain("web_search");
  expect(view.container.textContent).toContain(
    "Find and compare reliable sources",
  );
});

it("keeps full live steps when a delayed backfill contains truncated copies", async () => {
  let finishBackfill!: (steps: NonNullable<Subtask["steps"]>) => void;
  rs.mocked(fetchSubtaskSteps).mockImplementation(
    () =>
      new Promise((resolve) => {
        finishBackfill = resolve;
      }),
  );
  const view = setup(true);
  fireEvent.click(screen.getByRole("button"));
  expect(fetchSubtaskSteps).toHaveBeenCalledWith("thread-1", "run-1", "task-1");
  const historical = {
    kind: "tool" as const,
    message_index: 1,
    text: "history",
    tool_name: "read_file",
  };
  const live = {
    kind: "ai" as const,
    message_index: 2,
    text: `${"x".repeat(8192)} live reasoning tail`,
    tool_calls: [{ name: "web_search", args: { query: "sources" } }],
  };
  await act(async () => view.publish({ id: "task-1", steps: [live] }));
  expect(view.container.textContent).toContain("live reasoning tail");

  // Persisted steps are bounded; a late response must only fill missing indices.
  await act(async () =>
    finishBackfill([
      historical,
      { ...live, text: live.text.slice(0, 8192), truncated: true },
    ]),
  );
  expect(view.read().steps).toEqual([historical, live]);
  expect(screen.getAllByText("read_file")).toHaveLength(1);
  expect(view.container.textContent).toContain("live reasoning tail");
});
