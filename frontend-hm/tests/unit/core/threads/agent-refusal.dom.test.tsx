import type { Message } from "@langchain/langgraph-sdk";
import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";

const sdk = rs.hoisted(() => ({
  messages: [] as Message[],
  options: null as {
    onError: (
      error: unknown,
      meta?: { thread_id: string; run_id: string },
    ) => void;
    onCreated?: (meta: { thread_id: string; run_id: string }) => void;
    onFinish?: (
      state: {
        values: {
          messages: never[];
          artifacts: never[];
          title: string;
          todos: never[];
        };
      },
      meta?: { thread_id: string; run_id: string },
    ) => void;
  } | null,
  submit: rs.fn(),
  stop: rs.fn(async (): Promise<void> => undefined),
}));
const toasts = rs.hoisted(() => ({ error: rs.fn() }));
rs.mock("@langchain/langgraph-sdk/react", () => ({
  useStream: (options: typeof sdk.options) => {
    sdk.options = options;
    return {
      isLoading: false,
      messages: sdk.messages,
      values: { messages: sdk.messages, artifacts: [], todos: [], title: "" },
      submit: sdk.submit,
      stop: sdk.stop,
    };
  },
}));
rs.mock("sonner", () => ({ toast: toasts }));
rs.mock("@/core/uploads/api", () => ({ uploadFiles: rs.fn() }));

import { rememberRefusal, takeRefusal } from "@/core/agent-instances/errors";
import {
  submissionCreated,
  submissionFailed,
} from "@/core/api/submission-lifecycle";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { DEFAULT_LOCAL_SETTINGS } from "@/core/settings/local";
import { useThreadStream } from "@/core/threads/hooks";
import { uploadFiles } from "@/core/uploads/api";

const clients: QueryClient[] = [];
function metadata(call = sdk.submit.mock.calls.length - 1) {
  return (
    sdk.submit.mock.calls[call] as unknown as [unknown, { metadata: object }]
  )[1].metadata;
}
function fail(
  error: unknown,
  call = sdk.submit.mock.calls.length - 1,
  meta?: { thread_id: string; run_id: string },
) {
  const failure = error === full ? { ...full } : error;
  submissionFailed(metadata(call), failure);
  sdk.options!.onError(failure, meta);
}
function created(
  meta: { thread_id: string; run_id: string },
  call = sdk.submit.mock.calls.length - 1,
) {
  submissionCreated(metadata(call), meta);
  sdk.options!.onCreated!(meta);
}
const full = {
  status: 507,
  message:
    'HTTP 507: {"detail":{"code":"files-full","message":"raw server text"}}',
};
function start(agentInstanceId: string | undefined = "a".repeat(32)) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>
      <I18nContext.Provider
        value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
      >
        {children}
      </I18nContext.Provider>
    </QueryClientProvider>
  );
  return {
    client,
    ...renderHook(
      ({ id, agentInstanceId }) =>
        useThreadStream({
          context: DEFAULT_LOCAL_SETTINGS.context,
          displayThreadId: id,
          threadId: id,
          agentInstanceId,
          isMock: true,
        }),
      { wrapper, initialProps: { id: "thread", agentInstanceId } },
    ),
  };
}
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  sdk.submit.mockReset();
  sdk.stop.mockReset();
  toasts.error.mockReset();
  rs.mocked(uploadFiles).mockReset();
});

beforeEach(() => {
  sdk.messages = [];
  sdk.submit.mockResolvedValue(undefined);
  sdk.stop.mockResolvedValue(undefined);
  takeRefusal("thread");
});
afterEach(() => {
  takeRefusal("thread");
});
const saved = {
  filename: "statement.pdf",
  size: 12,
  path: "/mnt/user-data/uploads/statement.pdf",
  status: "uploaded" as const,
};
const refusal = {
  status: 409,
  message:
    'HTTP 409: {"reason":"run-limit","detail":"private server diagnostic"}',
};
test.each(["another conversation", "return to original", "unmount"])(
  "a held Stop failure retires after %s without consuming another refusal",
  async (destination) => {
    let reject!: (reason: Error) => void;
    sdk.stop.mockImplementation(
      () =>
        new Promise<void>((_resolve, fail) => {
          reject = fail;
        }),
    );
    const view = start();
    let stopping!: Promise<void>;
    act(() => {
      stopping = view.result.current.thread.stop();
    });
    if (destination === "unmount") view.unmount();
    else {
      view.rerender({ id: "other", agentInstanceId: "b".repeat(32) });
      if (destination === "return to original")
        view.rerender({ id: "thread", agentInstanceId: "a".repeat(32) });
    }
    const target = destination === "another conversation" ? "other" : "thread";
    rememberRefusal(target, { reason: "busy", extra: {} });
    await act(async () => {
      reject(new Error("private Stop diagnostic"));
      await stopping;
    });
    if (destination !== "unmount")
      expect(view.result.current.agentRunRefusal).toBeNull();
    expect(takeRefusal(target)?.reason).toBe("busy");
    expect(toasts.error).not.toHaveBeenCalled();
  },
);
test("a known refused send keeps structured agent metadata and the exact unsent draft without a toast", async () => {
  sdk.submit.mockImplementation(async () => {
    fail(refusal);
  });
  const { result } = start();
  await act(async () => {
    await result.current
      .sendMessage(
        "thread",
        { text: "  Research this  ", files: [] },
        undefined,
        { additionalKwargs: { files: [saved] } },
      )
      .catch(() => undefined);
  });
  expect(result.current.agentRunRefusal).toMatchObject({
    threadId: "thread",
    refusal: { reason: "run-limit" },
    restore: { text: "  Research this  ", files: [saved] },
  });
  expect(toasts.error).not.toHaveBeenCalled();
});
test("raw captured frame metadata wins over the SDK's stripped error", async () => {
  const { result } = start();
  rememberRefusal("thread", {
    name: "ResidentInterrupted",
    reason: "storage-recovery",
    extra: {},
  });
  await act(async () => {
    sdk.options!.onError(
      Object.assign(new Error("private"), { name: "ResidentInterrupted" }),
    );
  });
  expect(result.current.agentRunRefusal?.refusal.reason).toBe(
    "storage-recovery",
  );
  expect(toasts.error).not.toHaveBeenCalled();
  expect(takeRefusal("thread")).toBeNull();
});
test("an admitted network answer retains only its request identity for a managers-only card refusal", async () => {
  const { result } = start();
  let sending!: Promise<void>;
  act(() => {
    sending = result.current.sendMessage(
      "thread",
      { text: "Hidden answer", files: [] },
      undefined,
      {
        additionalKwargs: {
          hide_from_ui: true,
          human_input_response: {
            source: "sandbox_network",
            request_id: "request",
          },
        },
      },
    );
  });
  await act(async () => {
    await Promise.resolve();
    created({ thread_id: "thread", run_id: "run" });
    await sending;
  });
  await act(async () => {
    rememberRefusal("thread", {
      name: "Refusal",
      reason: "managers-only",
      extra: {},
    });
    fail({ name: "Refusal", message: "private" }, 0, {
      thread_id: "thread",
      run_id: "run",
    });
  });
  expect(result.current.agentRunRefusal).toMatchObject({
    requestId: "request",
    refusal: { reason: "managers-only" },
  });
  expect(result.current.agentRunRefusal?.restore).toBeUndefined();
  expect(toasts.error).not.toHaveBeenCalled();
  sdk.submit.mockImplementation(async () => {
    created({ thread_id: "thread", run_id: "next-run" });
  });
  await act(async () => {
    await result.current.sendMessage("thread", {
      text: "Another question",
      files: [],
    });
  });
  expect(result.current.agentRunRefusal).toBeNull();
  expect(result.current.refusedNetworkRequests).toEqual(new Set(["request"]));
});
test("a start failure before output restores admitted text and saved chips despite an earlier answer", async () => {
  sdk.messages = [
    { type: "ai", id: "prior-answer", content: "Earlier answer" },
  ];
  const { result } = start();
  let sending!: Promise<void>;
  act(() => {
    sending = result.current.sendMessage(
      "thread",
      { text: "New question", files: [] },
      undefined,
      { additionalKwargs: { files: [saved] } },
    );
  });
  await act(async () => {
    await Promise.resolve();
    created({ thread_id: "thread", run_id: "run" });
    await sending;
  });
  await act(async () => {
    sdk.options!.onError(
      Object.assign(new Error("private"), { name: "ResidentStartFailed" }),
      { thread_id: "thread", run_id: "run" },
    );
  });
  expect(result.current.agentRunRefusal?.restore).toEqual({
    text: "New question",
    files: [saved],
  });
  expect(toasts.error).not.toHaveBeenCalled();
});
test("an interruption after actual model output does not put the admitted question back into the composer", async () => {
  const view = start();
  let sending!: Promise<void>;
  act(() => {
    sending = view.result.current.sendMessage("thread", {
      text: "New question",
      files: [],
    });
  });
  await act(async () => {
    await Promise.resolve();
    created({ thread_id: "thread", run_id: "run" });
    await sending;
  });
  sdk.messages = [
    { type: "human", id: "question", content: "New question" },
    { type: "ai", id: "answer", content: "Partial answer" },
  ];
  view.rerender({ id: "thread", agentInstanceId: "a".repeat(32) });
  await act(async () => {
    sdk.options!.onError(
      Object.assign(new Error("private"), { name: "ResidentInterrupted" }),
      { thread_id: "thread", run_id: "run" },
    );
  });
  expect(view.result.current.agentRunRefusal?.restore).toBeUndefined();
  expect(toasts.error).not.toHaveBeenCalled();
});
test("an unknown agent error retains ordinary error handling", async () => {
  start();
  await act(async () => {
    sdk.options!.onError(
      Object.assign(new Error("ordinary unknown"), {
        name: "UnknownRuntimeError",
      }),
    );
  });
  expect(toasts.error).toHaveBeenCalled();
});
test("a refused Stop on an agent is a generic inline refusal", async () => {
  sdk.stop.mockRejectedValueOnce({
    status: 403,
    message: "private stop refusal",
  });
  const { result } = start();
  await act(async () => {
    await result.current.thread.stop();
  });
  expect(result.current.agentRunRefusal?.refusal.reason).toBe(
    "operation-failed",
  );
  expect(toasts.error).not.toHaveBeenCalled();
});
