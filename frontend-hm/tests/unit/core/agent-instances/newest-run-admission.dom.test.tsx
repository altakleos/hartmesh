import { afterEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook, waitFor } from "@testing-library/react";
import { useRef } from "react";

const sdk = rs.hoisted(() => ({
  loading: false,
  options: null as {
    onCreated: (meta: { thread_id: string; run_id: string }) => void;
  } | null,
  submit: rs.fn(),
}));
rs.mock("@langchain/langgraph-sdk/react", () => ({
  useStream: (options: typeof sdk.options) => {
    sdk.options = options;
    return {
      isLoading: sdk.loading,
      messages: [],
      values: { messages: [], artifacts: [], todos: [], title: "" },
      submit: sdk.submit,
      stop: async () => undefined,
    };
  },
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "reader",
      system_role: "user",
      permissions: ["runs:read", "agents:read"],
    },
  }),
}));
rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));

import { useNewestAgentRun } from "@/core/agent-instances/newest-run";
import { fetch as fetcher } from "@/core/api/fetcher";
import { submissionCreated } from "@/core/api/submission-lifecycle";
import { I18nProvider } from "@/core/i18n/context";
import { DEFAULT_LOCAL_SETTINGS } from "@/core/settings/local";
import { useThreadStream } from "@/core/threads/hooks";

afterEach(() => {
  cleanup();
  rs.mocked(fetcher).mockReset();
  sdk.submit.mockReset();
  sdk.loading = false;
});
test("actual consecutive stream submissions each admit newest-run recovery while conversation start stays once", async () => {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const start = rs.fn();
  let durable = {
    run_id: "old",
    thread_id: "thread",
    status: "interrupted",
    stop_reason: "gateway_stopped",
  };
  rs.mocked(fetcher).mockImplementation(async () =>
    Response.json({ data: [durable] }),
  );
  sdk.submit.mockImplementation(async () => {
    const call = sdk.submit.mock.calls.length - 1;
    const metadata = (
      sdk.submit.mock.calls[call] as unknown as [unknown, { metadata: object }]
    )[1].metadata;
    const meta = { thread_id: "thread", run_id: `run-${call + 1}` };
    sdk.loading = true;
    submissionCreated(metadata, meta);
    sdk.options!.onCreated(meta);
  });
  const view = renderHook(
    () => {
      const callbacks = useRef<ReturnType<typeof useNewestAgentRun> | null>(
        null,
      );
      const stream = useThreadStream({
        threadId: "thread",
        displayThreadId: "thread",
        agentInstanceId: "a".repeat(32),
        context: DEFAULT_LOCAL_SETTINGS.context,
        isMock: true,
        onSend: () => callbacks.current?.sent(),
        onStart: (thread, run) => {
          start();
          callbacks.current?.admitted(thread, run);
        },
        onAdmitted: (thread, run) => callbacks.current?.admitted(thread, run),
      });
      const latest = useNewestAgentRun({
        threadId: "thread",
        enabled: true,
        streaming: stream.thread.isLoading,
        finalAnswer: false,
      });
      callbacks.current = latest;
      return { stream, latest };
    },
    {
      wrapper: ({ children }) => (
        <QueryClientProvider client={client}>
          <I18nProvider initialLocale="en-US">{children}</I18nProvider>
        </QueryClientProvider>
      ),
    },
  );
  await waitFor(() =>
    expect(view.result.current.latest.notice?.kind).toBe("service"),
  );
  for (const [index, reason] of ["busy", "start-failed"].entries()) {
    await act(async () => {
      await view.result.current.stream.sendMessage("thread", {
        text: `Question ${index}`,
        files: [],
      });
    });
    durable = {
      ...durable,
      run_id: `run-${index + 1}`,
      status: "error",
      stop_reason: reason,
    };
    sdk.loading = false;
    view.rerender();
    await waitFor(() =>
      expect(view.result.current.latest.notice).toEqual({
        kind: "refusal",
        refusal: { reason, extra: {} },
      }),
    );
  }
  expect(start).toHaveBeenCalledTimes(1);
  client.clear();
});
