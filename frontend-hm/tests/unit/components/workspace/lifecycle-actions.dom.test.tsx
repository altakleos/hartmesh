import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

const state = rs.hoisted(() => ({ refresh: rs.fn(), held: false }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "reader",
      system_role: "admin",
      permissions: ["agents:read", "agents:write"],
    },
  }),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => ({
    known: true,
    enabled: true,
    execution: state.held ? "unavailable" : "ready",
    reason: state.held ? "operator-hold" : null,
  }),
  useBranding: () => ({ providerName: "Provider", supportURL: null }),
  useStorage: () => null,
}));
rs.mock("@/components/workspace/person-name", () => ({
  usePersonName: () => "Colleague",
  usePersonNames: (ids: string[]) =>
    new Map(ids.map((id) => [id, "Colleague"])),
  useSupervisorName: () => "a manager",
}));
rs.mock("@/core/agent-instances/api", () => {
  const actual = rs.requireActual<typeof api>("@/core/agent-instances/api");
  return {
    ...actual,
    changeInstanceLifecycle: rs.fn(),
    getInstance: rs.fn(),
    abandonInstanceLifecycle: rs.fn(),
  };
});
import {
  LifecycleActionsProvider,
  LifecycleState,
  useLifecycleActions,
} from "@/components/workspace/instances/lifecycle-actions";
import * as api from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
const id = "a".repeat(32);
const agent = {
  id,
  principal: { kind: "nonhuman", subject_id: "ai" },
  owner_id: "reader",
  creator_id: "reader",
  definition_revision: "revision",
  home_id: "b".repeat(32),
  name: "Researcher",
  custody: "personal",
  status: "active",
  permissions: 7,
  generation: 1,
  supervisor: { kind: "human", subject_id: "reader" },
  activity: {
    running_runs: 0,
    running: [],
    last_active_at: null,
    open_conversations: 0,
  },
  home: { state: "available", workspace: "absent" },
  execution: { execution: "ready", reason: null },
} as api.AgentInstanceDetail;
function Controls() {
  const actions = useLifecycleActions();
  return (
    <>
      <button
        disabled={actions.busy || actions.pending}
        onClick={() =>
          void actions.change({
            action: "grant",
            member_id: "colleague",
            permissions: 3,
            only_if_idle: true,
          })
        }
      >
        Save grant
      </button>
      <LifecycleState />
    </>
  );
}
function tree(
  operations: api.LifecycleOperation[],
  instance = agent,
  observationEpoch = 1,
) {
  return (
    <I18nProvider initialLocale="en-US">
      <FileActionLifetimeProvider>
        <LifecycleActionsProvider
          instance={instance}
          observationEpoch={observationEpoch}
          operations={operations}
          refresh={state.refresh}
        >
          <Controls />
        </LifecycleActionsProvider>
      </FileActionLifetimeProvider>
    </I18nProvider>
  );
}
function show(operations: api.LifecycleOperation[] = []) {
  const view = render(tree(operations));
  return {
    ...view,
    replace: (
      operations: api.LifecycleOperation[],
      instance = agent,
      observationEpoch = 1,
    ) => view.rerender(tree(operations, instance, observationEpoch)),
  };
}
beforeEach(() => {
  state.held = false;
  state.refresh.mockReset().mockResolvedValue(undefined);
  rs.mocked(api.changeInstanceLifecycle).mockReset().mockResolvedValue({
    instance: agent,
    complete: true,
    operation_id: "token",
  });
  rs.mocked(api.getInstance).mockReset().mockResolvedValue(agent);
  rs.mocked(api.abandonInstanceLifecycle)
    .mockReset()
    .mockResolvedValue({ instance: agent, complete: true, abandoned: true });
});
afterEach(() => {
  cleanup();
  rs.useRealTimers();
});
test("sends a grant with only_if_idle and a captured operation identity", async () => {
  show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  await waitFor(() => expect(api.changeInstanceLifecycle).toHaveBeenCalled());
  expect(rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1]).toEqual({
    action: "grant",
    member_id: "colleague",
    permissions: 3,
    only_if_idle: true,
    generation: 1,
    operation_id: expect.stringMatching(/^[0-9a-f]{32}$/),
  });
  expect(state.refresh).toHaveBeenCalled();
});
test("an unanswered change retries the exact body and never shows transport prose", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new TypeError("private transport"),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  await screen.findByText(
    "It is not confirmed that the change was saved. Press Try again; nothing is done twice.",
  );
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(2),
  );
  expect(rs.mocked(api.changeInstanceLifecycle).mock.calls[1]![1]).toEqual(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1],
  );
  expect(screen.queryByText("private transport")).toBeNull();
});
test("a pending server operation appears on every tab with retry and explicit give-up confirmation", async () => {
  const body = {
    action: "grant" as const,
    member_id: "colleague",
    permissions: 3,
    generation: 1,
    operation_id: "c".repeat(32),
  };
  show([{ ...body, complete: false, retry: body }]);
  expect(
    screen.getByText(
      "This change is not finished: it is not yet confirmed that Researcher's workspace is closed. Researcher cannot work until the change is finished.",
    ),
  ).toBeDefined();
  fireEvent.click(screen.getByRole("button", { name: "Give up this change" }));
  expect(api.abandonInstanceLifecycle).not.toHaveBeenCalled();
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Give up this change",
    }),
  );
  await waitFor(() =>
    expect(api.abandonInstanceLifecycle).toHaveBeenCalledWith(
      id,
      { generation: 1, operation_id: body.operation_id },
      expect.any(AbortSignal),
    ),
  );
});
test("work in flight is named and the explicit stop-and-apply preserves the operation token", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private", "work-in-flight", undefined, {
      run_ids: ["run-one"],
    }),
  );
  rs.mocked(api.getInstance).mockResolvedValue({
    ...agent,
    activity: {
      ...agent.activity,
      running_runs: 1,
      running: [
        {
          run_id: "run-one",
          thread_id: "thread",
          requester_id: "colleague",
          work_id: null,
          objective: null,
        },
      ],
    },
  });
  show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  await screen.findByRole("dialog");
  expect(
    screen.getByText(
      /Changing who has access restarts Researcher's workspace.*a reply to Colleague/,
    ),
  ).toBeDefined();
  fireEvent.click(
    screen.getByRole("button", { name: "Stop the work and apply now" }),
  );
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(2),
  );
  const calls = rs.mocked(api.changeInstanceLifecycle).mock.calls;
  expect(calls[1]![1].operation_id).toBe(calls[0]![1].operation_id);
  expect(calls[1]![1]).not.toHaveProperty("only_if_idle");
});
test("empty run_ids retries once without a needless interruption dialog", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private", "work-in-flight", undefined, {
      run_ids: [],
    }),
  );
  show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  await waitFor(() =>
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(2),
  );
  expect(screen.queryByRole("dialog")).toBeNull();
  expect(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[1]![1].only_if_idle,
  ).toBe(true);
});
test("waiting for idle retains only_if_idle and does not stop work automatically", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private", "work-in-flight", undefined, {
      run_ids: ["run-one"],
    }),
  );
  rs.mocked(api.getInstance)
    .mockResolvedValueOnce({
      ...agent,
      activity: { ...agent.activity, running_runs: 1, running: [] },
    })
    .mockResolvedValue(agent);
  show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  const waitButton = await screen.findByRole("button", {
    name: "Apply when Researcher is idle",
  });
  await waitFor(() =>
    expect((waitButton as HTMLButtonElement).disabled).toBe(false),
  );
  fireEvent.click(waitButton);
  await screen.findByText(
    "This change will apply when Researcher is idle. Keep this page open.",
  );
  await waitFor(
    () => expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(2),
    { timeout: 6000 },
  );
  expect(
    rs.mocked(api.changeInstanceLifecycle).mock.calls[1]![1].only_if_idle,
  ).toBe(true);
}, 10_000);
test("unmount retires a held change without refresh or subsequent requests", async () => {
  let finish!: (
    value: Awaited<ReturnType<typeof api.changeInstanceLifecycle>>,
  ) => void;
  rs.mocked(api.changeInstanceLifecycle).mockImplementation(
    () =>
      new Promise((done) => {
        finish = done;
      }),
  );
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  await waitFor(() => expect(api.changeInstanceLifecycle).toHaveBeenCalled());
  view.unmount();
  finish({ instance: agent, complete: true, operation_id: "token" });
  await new Promise((done) => setTimeout(done, 10));
  expect(state.refresh).not.toHaveBeenCalled();
});

const operationA: api.LifecycleChange = {
  action: "grant",
  member_id: "colleague",
  permissions: 3,
  generation: 1,
  operation_id: "c".repeat(32),
};
test("give-up confirmation and unknown retry keep the exact operation that was confirmed", async () => {
  const view = show([{ ...operationA, complete: false, retry: operationA }]);
  fireEvent.click(screen.getByRole("button", { name: "Give up this change" }));
  const operationB = {
    ...operationA,
    operation_id: "d".repeat(32),
    generation: 2,
  };
  view.replace([{ ...operationB, complete: false, retry: operationB }], {
    ...agent,
    generation: 2,
  });
  rs.mocked(api.abandonInstanceLifecycle).mockRejectedValueOnce(
    new TypeError("private transport"),
  );
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Give up this change",
    }),
  );
  await within(screen.getByRole("dialog")).findByText(
    "It is not confirmed that the change was saved. Press Try again; nothing is done twice.",
  );
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Try again",
    }),
  );
  await waitFor(() =>
    expect(api.abandonInstanceLifecycle).toHaveBeenCalledTimes(2),
  );
  expect(
    rs.mocked(api.abandonInstanceLifecycle).mock.calls.map((call) => call[1]),
  ).toEqual([
    { generation: 1, operation_id: operationA.operation_id },
    { generation: 1, operation_id: operationA.operation_id },
  ]);
});
test("a new execution hold disables retry of an unanswered lifecycle action", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new TypeError("network"),
  );
  const view = show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  await screen.findByText(
    "It is not confirmed that the change was saved. Press Try again; nothing is done twice.",
  );
  state.held = true;
  view.replace([]);
  expect(
    screen.getByRole<HTMLButtonElement>("button", { name: "Try again" })
      .disabled,
  ).toBe(true);
  fireEvent.click(screen.getByRole("button", { name: "Try again" }));
  expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1);
});
test("an idle poll which observes permission loss cannot submit the held change", async () => {
  rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
    new api.InstanceApiError(409, "private", "work-in-flight", undefined, {
      run_ids: ["run-one"],
    }),
  );
  rs.mocked(api.getInstance)
    .mockResolvedValueOnce(agent)
    .mockResolvedValue({ ...agent, permissions: 3 });
  show();
  fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
  const button = await screen.findByRole<HTMLButtonElement>("button", {
    name: "Apply when Researcher is idle",
  });
  await waitFor(() => expect(button.disabled).toBe(false));
  fireEvent.click(button);
  await waitFor(() => expect(api.getInstance).toHaveBeenCalledTimes(2), {
    timeout: 6000,
  });
  await new Promise((resolve) => setTimeout(resolve, 25));
  expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1);
}, 10_000);
test.each([false, true])(
  "matching terminal proof clears a captured pending change, abandoned=%s",
  async (abandoned) => {
    rs.mocked(api.changeInstanceLifecycle).mockResolvedValueOnce({
      instance: agent,
      complete: false,
      operation_id: "token",
    });
    const view = show();
    fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
    await screen.findByRole("button", { name: "Try again" });
    const body = rs.mocked(api.changeInstanceLifecycle).mock.calls[0]![1];
    view.replace([{ ...body, complete: true, abandoned }]);
    await waitFor(() =>
      expect(
        screen.getByRole<HTMLButtonElement>("button", { name: "Save grant" })
          .disabled,
      ).toBe(false),
    );
    expect(screen.queryByRole("button", { name: "Try again" })).toBeNull();
    if (abandoned)
      expect(
        screen.getByText(
          "An earlier change was given up. Nothing was changed.",
        ),
      ).toBeDefined();
  },
);
test("a known non-conflict give-up refusal is an operation error, not an unknown outcome", async () => {
  rs.mocked(api.abandonInstanceLifecycle).mockRejectedValueOnce(
    new api.InstanceApiError(403, "private refusal"),
  );
  show([{ ...operationA, complete: false, retry: operationA }]);
  fireEvent.click(screen.getByRole("button", { name: "Give up this change" }));
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", {
      name: "Give up this change",
    }),
  );
  await within(screen.getByRole("dialog")).findByText(
    "That did not work. Refresh the page, then try again.",
  );
  expect(screen.queryByText("private refusal")).toBeNull();
});

test.each([{ run_ids: [] as string[] }, { run_ids: ["run-one"] }])(
  "fresh unavailability after idle refusal cannot replay or offer force controls, run_ids=%j",
  async ({ run_ids }) => {
    rs.mocked(api.changeInstanceLifecycle).mockRejectedValueOnce(
      new api.InstanceApiError(409, "private", "work-in-flight", undefined, {
        run_ids,
      }),
    );
    rs.mocked(api.getInstance).mockResolvedValue({
      ...agent,
      execution: { execution: "unavailable", reason: "operator-hold" },
    });
    const view = show();
    fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
    await screen.findByText(
      "AI assistants and AI employees are paused by Provider.",
    );
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1);
    expect(screen.queryByRole("dialog")).toBeNull();
    state.refresh.mockRejectedValue(new TypeError("refresh failed"));
    view.replace([]);
    fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
    await new Promise((resolve) => setTimeout(resolve, 20));
    expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(1);
    view.replace([], agent, 2);
    fireEvent.click(screen.getByRole("button", { name: "Save grant" }));
    await waitFor(() =>
      expect(api.changeInstanceLifecycle).toHaveBeenCalledTimes(2),
    );
  },
);
