import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";

const state = rs.hoisted(() => ({
  user: { id: "reader", system_role: "user", permissions: ["runs:read"] },
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: state.user }),
}));
rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "/backend" }));

import {
  readNewestAgentRun,
  useNewestAgentRun,
} from "@/core/agent-instances/newest-run";
import { fetch as fetcher } from "@/core/api/fetcher";

const clients: QueryClient[] = [];
const interrupted = {
  run_id: "run",
  thread_id: "thread",
  status: "interrupted",
  stop_reason: "gateway_stopped",
};
function show(change = {}) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  const props = {
    threadId: "thread",
    enabled: true,
    streaming: false,
    finalAnswer: false,
    ...change,
  };
  return renderHook((props) => useNewestAgentRun(props), {
    initialProps: props,
    wrapper: ({ children }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    ),
  });
}
beforeEach(() => {
  state.user = {
    id: "reader",
    system_role: "user",
    permissions: ["runs:read"],
  };
  rs.mocked(fetcher)
    .mockReset()
    .mockImplementation(async () => Response.json({ data: [interrupted] }));
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.useRealTimers();
});
test("opens exactly one newest-run read and displays only its fixed restart notice", async () => {
  const view = show();
  await waitFor(() =>
    expect(view.result.current.notice).toEqual({ kind: "service" }),
  );
  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(rs.mocked(fetcher).mock.calls[0]![0]).toBe(
    "/backend/api/threads/thread/runs/page?limit=1",
  );
});
test("feature and run-read authority keep unavailable observations inert", () => {
  show({ enabled: false });
  state.user.permissions = [];
  show();
  expect(fetcher).not.toHaveBeenCalled();
});
test("a send retires an in-flight open read and never resurrects its old notice", async () => {
  let finish!: (response: Response) => void;
  rs.mocked(fetcher).mockImplementationOnce(
    () =>
      new Promise((resolve) => {
        finish = resolve;
      }),
  );
  const view = show();
  await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
  act(() => view.result.current.sent());
  await act(async () => {
    finish(Response.json({ data: [interrupted] }));
  });
  expect(view.result.current.notice).toBeNull();
  expect(rs.mocked(fetcher).mock.calls[0]![1]!.signal!.aborted).toBe(true);
});
test("stream completion without an answer refreshes the admitted run instead of showing an older run's ending", async () => {
  const view = show();
  await waitFor(() => expect(view.result.current.notice?.kind).toBe("service"));
  act(() => {
    view.result.current.sent();
    view.result.current.admitted("thread", "new-run");
  });
  expect(view.result.current.notice).toBeNull();
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: true,
    finalAnswer: false,
  });
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: false,
    finalAnswer: false,
  });
  await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(2));
  expect(view.result.current.notice).toBeNull();
  rs.mocked(fetcher).mockImplementation(async () =>
    Response.json({
      data: [
        {
          ...interrupted,
          run_id: "new-run",
          stop_reason: "start-failed",
          status: "error",
        },
      ],
    }),
  );
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: true,
    finalAnswer: false,
  });
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: false,
    finalAnswer: false,
  });
  await waitFor(() =>
    expect(view.result.current.notice).toEqual({
      kind: "refusal",
      refusal: { reason: "start-failed", extra: {} },
    }),
  );
});
test("a final answer does not request another read solely because its stream settled", async () => {
  const view = show({ streaming: true });
  await waitFor(() => expect(fetcher).toHaveBeenCalledTimes(1));
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: false,
    finalAnswer: true,
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  expect(fetcher).toHaveBeenCalledTimes(1);
});
test("running observations poll every fifteen seconds for at most two minutes and stop on unmount", async () => {
  rs.useFakeTimers();
  rs.mocked(fetcher).mockImplementation(async () =>
    Response.json({
      data: [{ ...interrupted, status: "running", stop_reason: null }],
    }),
  );
  const view = show();
  await act(async () => rs.advanceTimersByTimeAsync(1));
  expect(fetcher).toHaveBeenCalledTimes(1);
  await act(async () => rs.advanceTimersByTimeAsync(120_000));
  const calls = rs.mocked(fetcher).mock.calls.length;
  expect(calls).toBeGreaterThanOrEqual(8);
  expect(calls).toBeLessThanOrEqual(9);
  await act(async () => rs.advanceTimersByTimeAsync(120_000));
  expect(fetcher).toHaveBeenCalledTimes(calls);
  view.unmount();
  await act(async () => rs.advanceTimersByTimeAsync(60_000));
  expect(fetcher).toHaveBeenCalledTimes(calls);
});
test("does not poll while the live stream is present", async () => {
  rs.useFakeTimers();
  rs.mocked(fetcher).mockImplementation(async () =>
    Response.json({
      data: [{ ...interrupted, status: "running", stop_reason: null }],
    }),
  );
  show({ streaming: true });
  await act(async () => rs.advanceTimersByTimeAsync(60_000));
  expect(fetcher).toHaveBeenCalledTimes(1);
});
test("a long live stream starts a fresh two-minute recovery budget only after disconnecting", async () => {
  rs.useFakeTimers();
  rs.mocked(fetcher).mockImplementation(async () =>
    Response.json({
      data: [{ ...interrupted, status: "running", stop_reason: null }],
    }),
  );
  const view = show({ streaming: true });
  await act(async () => rs.advanceTimersByTimeAsync(180_000));
  expect(fetcher).toHaveBeenCalledTimes(1);
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: false,
    finalAnswer: false,
  });
  await act(async () => rs.advanceTimersByTimeAsync(1));
  expect(fetcher).toHaveBeenCalledTimes(2);
  await act(async () => rs.advanceTimersByTimeAsync(15_000));
  expect(fetcher).toHaveBeenCalledTimes(3);
  await act(async () => rs.advanceTimersByTimeAsync(120_000));
  const calls = rs.mocked(fetcher).mock.calls.length;
  await act(async () => rs.advanceTimersByTimeAsync(120_000));
  expect(fetcher).toHaveBeenCalledTimes(calls);
});
test("an earlier held read that finishes during streaming cannot consume the next recovery budget", async () => {
  rs.useFakeTimers();
  let finish!: (response: Response) => void;
  const running = { ...interrupted, status: "running", stop_reason: null };
  rs.mocked(fetcher)
    .mockImplementation(async () => Response.json({ data: [running] }))
    .mockImplementationOnce(
      () =>
        new Promise((resolve) => {
          finish = resolve;
        }),
    );
  const view = show();
  await act(async () => rs.advanceTimersByTimeAsync(1));
  expect(fetcher).toHaveBeenCalledTimes(1);
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: true,
    finalAnswer: false,
  });
  await act(async () => {
    finish(Response.json({ data: [running] }));
    await rs.advanceTimersByTimeAsync(1);
  });
  await act(async () => rs.advanceTimersByTimeAsync(180_000));
  expect(fetcher).toHaveBeenCalledTimes(1);
  view.rerender({
    threadId: "thread",
    enabled: true,
    streaming: false,
    finalAnswer: false,
  });
  await act(async () => rs.advanceTimersByTimeAsync(1));
  expect(fetcher).toHaveBeenCalledTimes(2);
  await act(async () => rs.advanceTimersByTimeAsync(15_000));
  expect(fetcher).toHaveBeenCalledTimes(3);
});
test("invalid or foreign run data fails without inventing a notice", async () => {
  rs.mocked(fetcher).mockImplementation(async () =>
    Response.json({ data: [{ ...interrupted, thread_id: "other" }] }),
  );
  await expect(readNewestAgentRun("thread")).rejects.toThrow(
    "Invalid latest run record",
  );
  rs.mocked(fetcher).mockImplementation(
    async () => new Response("private proxy diagnostic", { status: 503 }),
  );
  await expect(readNewestAgentRun("thread")).rejects.toThrow(
    "Latest run could not be read",
  );
});
