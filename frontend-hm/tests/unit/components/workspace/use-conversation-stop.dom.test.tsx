import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";

const auth = rs.hoisted(() => ({ id: "reader", permissions: ["runs:cancel"] }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { ...auth, system_role: "user" } }),
}));
rs.mock("@/core/agent-instances/api", () => ({
  ...rs.requireActual<typeof api>("@/core/agent-instances/api"),
  getWork: rs.fn(),
  commandWork: rs.fn(),
}));

import {
  useConversationStop,
  type ConversationStopOptions,
} from "@/components/workspace/chats/use-conversation-stop";
import * as api from "@/core/agent-instances/api";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";

const instance = "a".repeat(32);
const work = "c".repeat(32);
const binding = {
  id: instance,
  name: "Researcher",
  custody: "personal" as const,
  status: "active" as const,
  requester_id: "reader",
};
function detail(): api.AgentInstanceDetail {
  return {
    id: instance,
    name: "Researcher",
    principal: { kind: "nonhuman", subject_id: "ai" },
    custody: "personal",
    owner_id: "reader",
    creator_id: "reader",
    supervisor: { kind: "human", subject_id: "reader" },
    definition_revision: "revision",
    generation: 1,
    home_id: "b".repeat(32),
    status: "active",
    permissions: 3,
    activity: {
      running_runs: 1,
      running: [
        {
          run_id: "run",
          thread_id: "thread",
          requester_id: "reader",
          work_id: work,
          objective: "Review guide",
        },
      ],
      last_active_at: null,
      open_conversations: 1,
    },
    home: { state: "available", workspace: "running" },
    execution: { execution: "ready", reason: null },
  };
}
function record(): api.WorkRecord {
  return {
    id: work,
    instance_id: instance,
    creator_id: "reader",
    definition_revision: "revision",
    objective: "Review guide",
    success_criteria: "List gaps",
    priority: "normal",
    review_required: true,
    assignment_revision: 2,
    revision: 5,
    status: "open",
    progress: "",
    next_action: "",
    sources: [],
    review_state: "none",
    outcome: null,
    review: null,
    blocker: null,
    attempt: {
      id: "d".repeat(32),
      status: "running",
      thread_id: "thread",
      run_id: "run",
      requester_id: "reader",
    },
    execution_available: true,
    availability: "explicit_activation",
    work_enabled: true,
    needs_mandate_reconciliation: false,
    current_contents: "not_checked",
    created_at: "2026-10-16T07:00:00Z",
    updated_at: "2026-10-16T07:00:00Z",
  };
}
const stopStream = rs.fn<() => Promise<void>>();
const changed = rs.fn();
function options(): ConversationStopOptions {
  return {
    threadId: "thread",
    binding,
    detail: detail(),
    legacy: false,
    streaming: true,
    sentHere: false,
    stopStream,
    changed,
  };
}
function show(initialProps = options()) {
  return renderHook(useConversationStop, {
    initialProps,
    wrapper: ({ children }) => (
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>{children}</FileActionLifetimeProvider>
      </I18nProvider>
    ),
  });
}
beforeEach(() => {
  auth.id = "reader";
  auth.permissions = ["runs:cancel"];
  stopStream.mockReset().mockResolvedValue();
  changed.mockReset();
  rs.mocked(api.getWork)
    .mockReset()
    .mockImplementation(async () => record());
  rs.mocked(api.commandWork)
    .mockReset()
    .mockImplementation(async () => {
      const value = record();
      return { ...value, attempt: { ...value.attempt!, status: "stopping" } };
    });
});
afterEach(cleanup);
test("an assignment reply uses the Work command and stays stopping until its stream ends", async () => {
  const props = options();
  const view = show(props);
  expect(view.result.current.allowed).toBe(true);
  await act(() => view.result.current.stop());
  expect(api.getWork).toHaveBeenCalledWith(
    instance,
    work,
    expect.any(AbortSignal),
  );
  expect(rs.mocked(api.commandWork).mock.calls[0]![2]).toMatchObject({
    action: "stop",
    expected_revision: 5,
    expected_assignment_revision: 2,
    attempt_id: "d".repeat(32),
  });
  expect(stopStream).not.toHaveBeenCalled();
  expect(changed).toHaveBeenCalledTimes(1);
  expect(view.result.current.pending).toBe(true);
  expect(view.result.current.message).toBe(
    "Stopping Researcher. This can take up to a minute.",
  );
  view.rerender({ ...props, streaming: false });
  await waitFor(() => expect(view.result.current.pending).toBe(false));
});
test("ordinary replies use stream cancellation, and a colleague needs current sender or manager authority", async () => {
  const props = options();
  props.detail!.activity.running[0]!.work_id = null;
  const view = show(props);
  await act(() => view.result.current.stop());
  expect(stopStream).toHaveBeenCalledTimes(1);
  expect(api.getWork).not.toHaveBeenCalled();
  auth.id = "colleague";
  view.rerender({ ...props });
  expect(view.result.current.allowed).toBe(false);
  await act(() => view.result.current.stop());
  expect(stopStream).toHaveBeenCalledTimes(1);
});
test("Work stopping requires both Use and Inspect for its starter and the cancellation ceiling", async () => {
  const props = options();
  props.detail!.permissions = 1;
  const view = show(props);
  expect(view.result.current.allowed).toBe(false);
  props.detail = detail();
  auth.permissions = ["runs:read"];
  view.rerender({ ...props });
  expect(view.result.current.allowed).toBe(false);
  await act(() => view.result.current.stop());
  expect(api.getWork).not.toHaveBeenCalled();
});
test("a manager may stop a colleague's assignment", async () => {
  const props = options();
  props.detail!.permissions = 6;
  auth.id = "manager";
  const view = show(props);
  expect(view.result.current.allowed).toBe(true);
  await act(() => view.result.current.stop());
  expect(api.commandWork).toHaveBeenCalledTimes(1);
});
test.each([4, 5, 3])(
  "mask %s cannot stop a colleague's assignment",
  async (permissions) => {
    const props = options();
    props.detail!.permissions = permissions;
    auth.id = "colleague";
    const view = show(props);
    expect(view.result.current.allowed).toBe(false);
    await act(() => view.result.current.stop());
    expect(api.getWork).not.toHaveBeenCalled();
    expect(api.commandWork).not.toHaveBeenCalled();
    expect(stopStream).not.toHaveBeenCalled();
  },
);
test("the exact Work starter must still match after the fresh record read", async () => {
  const value = record();
  rs.mocked(api.getWork).mockResolvedValue({
    ...value,
    attempt: { ...value.attempt!, requester_id: "other" },
  });
  const view = show();
  expect(view.result.current.allowed).toBe(true);
  await act(() => view.result.current.stop());
  expect(api.commandWork).not.toHaveBeenCalled();
  expect(view.result.current.message).toBeTruthy();
});
test("a settled attempt reports nothing to stop without issuing cancellation", async () => {
  rs.mocked(api.getWork).mockResolvedValue({ ...record(), attempt: null });
  const view = show();
  await act(() => view.result.current.stop());
  expect(view.result.current.message).toBe(
    "There is nothing to stop: Researcher has already stopped working on this.",
  );
  expect(api.commandWork).not.toHaveBeenCalled();
  expect(stopStream).not.toHaveBeenCalled();
});
test("a replacement conversation is never stopped from the original conversation", async () => {
  const value = record();
  rs.mocked(api.getWork).mockResolvedValue({
    ...value,
    attempt: { ...value.attempt!, thread_id: "replacement", run_id: "new-run" },
  });
  const view = show();
  await act(() => view.result.current.stop());
  expect(api.commandWork).not.toHaveBeenCalled();
  expect(stopStream).not.toHaveBeenCalled();
  expect(view.result.current.message).toBe(
    "There is nothing to stop: Researcher has already stopped working on this.",
  );
});
test("an unknown Work stop retries the exact intent manually without another read or fallback cancellation", async () => {
  rs.mocked(api.commandWork).mockRejectedValueOnce(
    new TypeError("private transport diagnostic"),
  );
  const view = show();
  await act(() => view.result.current.stop());
  expect(view.result.current.message).toBe(
    "The work could not be stopped. Try again.",
  );
  expect(api.commandWork).toHaveBeenCalledTimes(1);
  const body = rs.mocked(api.commandWork).mock.calls[0]![2];
  await act(() => view.result.current.stop());
  expect(rs.mocked(api.commandWork).mock.calls[1]![2]).toEqual(body);
  expect(api.getWork).toHaveBeenCalledTimes(1);
  expect(stopStream).not.toHaveBeenCalled();
});
test("repeated clicks share one in-flight Work read and retirement prohibits its later command", async () => {
  let resolve!: (value: api.WorkRecord) => void;
  rs.mocked(api.getWork).mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const props = options();
  const view = show(props);
  let pending!: Promise<void>;
  act(() => {
    pending = view.result.current.stop();
    void view.result.current.stop();
  });
  expect(api.getWork).toHaveBeenCalledTimes(1);
  expect(view.result.current.busy).toBe(true);
  const signal = rs.mocked(api.getWork).mock.calls[0]![2]!;
  view.rerender({ ...props, threadId: "other" });
  expect(signal.aborted).toBe(true);
  await act(async () => {
    resolve(record());
    await pending;
  });
  expect(api.commandWork).not.toHaveBeenCalled();
  expect(view.result.current.message).toBeNull();
});
test("permission loss and unmount retire a pending Stop before any follow-up command", async () => {
  let resolve!: (value: api.WorkRecord) => void;
  rs.mocked(api.getWork).mockImplementation(
    () =>
      new Promise((done) => {
        resolve = done;
      }),
  );
  const props = options();
  const view = show(props);
  let pending!: Promise<void>;
  act(() => {
    pending = view.result.current.stop();
  });
  view.rerender({ ...props, detail: { ...props.detail!, permissions: 1 } });
  await act(async () => {
    resolve(record());
    await pending;
  });
  expect(api.commandWork).not.toHaveBeenCalled();
  expect(view.result.current.allowed).toBe(false);
  view.unmount();
});
test("ordinary chats retain their Stop while an unresolved binding never gains it", async () => {
  const props = { ...options(), binding: null, detail: null, legacy: true };
  const view = show(props);
  expect(view.result.current.allowed).toBe(true);
  await act(() => view.result.current.stop());
  expect(stopStream).toHaveBeenCalledTimes(1);
  view.rerender({ ...props, legacy: false });
  expect(view.result.current.allowed).toBe(false);
});

test("unmount retires a held Work read and a late error stays out of the next view", async () => {
  let reject!: (error: Error) => void;
  rs.mocked(api.getWork).mockImplementation(
    () =>
      new Promise((_resolve, fail) => {
        reject = fail;
      }),
  );
  const view = show();
  let pending!: Promise<void>;
  act(() => {
    pending = view.result.current.stop();
  });
  const signal = rs.mocked(api.getWork).mock.calls[0]![2]!;
  view.unmount();
  expect(signal.aborted).toBe(true);
  await act(async () => {
    reject(new Error("private late diagnostic"));
    await pending;
  });
  expect(api.commandWork).not.toHaveBeenCalled();
  expect(changed).not.toHaveBeenCalled();
});
test("an uncertain Work command keeps its exact manual retry when activity temporarily disappears", async () => {
  rs.mocked(api.commandWork).mockRejectedValueOnce(new TypeError("private"));
  const props = options();
  const view = show(props);
  await act(() => view.result.current.stop());
  const original = rs.mocked(api.commandWork).mock.calls[0]![2];
  view.rerender({
    ...props,
    detail: {
      ...props.detail!,
      activity: { ...props.detail!.activity, running: [], running_runs: 0 },
    },
  });
  await act(() => view.result.current.stop());
  expect(rs.mocked(api.commandWork).mock.calls[1]![2]).toEqual(original);
  expect(stopStream).not.toHaveBeenCalled();
  expect(api.getWork).toHaveBeenCalledTimes(1);
});
