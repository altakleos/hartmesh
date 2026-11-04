import { afterEach, beforeEach, expect, rs, test } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook } from "@testing-library/react";
import { createElement, type ReactNode } from "react";

import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { DEFAULT_LOCAL_SETTINGS } from "@/core/settings/local";
import { useThreadStream } from "@/core/threads/hooks";
import {
  runInputsKey,
  type RunInput,
  type RunInputMode,
} from "@/core/threads/run-inputs";

const stream = rs.hoisted(() => ({
  isLoading: true,
  joinStream: rs.fn(async (_runId: string) => undefined),
  options: undefined as
    | {
        onFinish?: (
          state: unknown,
          meta?: { run_id: string; thread_id: string },
        ) => void;
      }
    | undefined,
}));

rs.mock("@/core/api", () => ({
  cancelActiveThreadRun: async () => undefined,
  getAPIClient: () => ({
    runs: { cancel: async () => undefined, list: async () => [] },
    threads: { update: async () => ({}) },
  }),
}));

rs.mock("@langchain/langgraph-sdk/react", () => ({
  useStream: (options: typeof stream.options) => {
    stream.options = options;
    return {
      isLoading: stream.isLoading,
      joinStream: stream.joinStream,
      messages: [],
      stop: async () => undefined,
      submit: async () => undefined,
      values: { artifacts: [], messages: [], title: "", todos: [] },
    };
  },
}));

type SubmittedInput = {
  runId: string;
  body: {
    request_id: string;
    text: string;
    mode: RunInputMode;
    context: Record<string, unknown>;
  };
};

const queryClients: QueryClient[] = [];
let activeRunId: string | null;
let receipts: RunInput[];
let inputReads: number;
let submittedInputs: SubmittedInput[];
let firstPostFailure:
  | "hang"
  | "conflict"
  | "network"
  | "server"
  | "malformed"
  | "accepted";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function receiptFor({ runId, body }: SubmittedInput): RunInput {
  return {
    id: body.request_id,
    target_run_id: runId,
    message_id: `message-${body.request_id}`,
    text: body.text,
    mode: body.mode,
    status: "pending",
    created_at: "2026-11-04T00:00:00Z",
  };
}

function createWrapper(queryClient: QueryClient) {
  return function RunInputSendTestWrapper({
    children,
  }: {
    children: ReactNode;
  }) {
    return createElement(
      QueryClientProvider,
      { client: queryClient },
      createElement(
        I18nContext.Provider,
        { value: { locale: "en-US", setLocale: () => undefined, t: enUS } },
        children,
      ),
    );
  };
}

async function flushFrames() {
  for (let index = 0; index < 8; index += 1) {
    await act(async () => {
      await rs.advanceTimersByTimeAsync(1);
    });
  }
}

async function renderThread({
  account = "account-1" as string | null,
  cached,
}: {
  account?: string | null;
  /** An answer cached by an earlier visit to the same chat. */
  cached?: { items: RunInput[]; active_run_id: string | null };
} = {}) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  queryClients.push(queryClient);
  if (cached)
    queryClient.setQueryData(
      [...runInputsKey("thread-1"), account ?? null],
      cached,
    );
  const rendered = renderHook(
    ({
      modelName,
      owner = account,
    }: {
      modelName: string;
      owner?: string | null;
    }) =>
      useThreadStream({
        context: { ...DEFAULT_LOCAL_SETTINGS.context, model_name: modelName },
        threadId: "thread-1",
        runInputs: owner ? { account: owner } : null,
      }),
    {
      initialProps: {
        modelName: "model-original",
        owner: account,
      } as { modelName: string; owner?: string | null },
      wrapper: createWrapper(queryClient),
    },
  );
  await flushFrames();
  return { queryClient, ...rendered };
}

async function advanceToRun(queryClient: QueryClient, runId: string) {
  activeRunId = runId;
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: runInputsKey("thread-1") });
  });
  await flushFrames();
}

beforeEach(() => {
  rs.useFakeTimers({
    toFake: ["setTimeout", "clearTimeout", "setInterval", "clearInterval"],
  });
  window.sessionStorage.clear();
  stream.isLoading = true;
  stream.joinStream.mockClear();
  activeRunId = "run-1";
  receipts = [];
  inputReads = 0;
  submittedInputs = [];
  firstPostFailure = "conflict";
  rs.stubGlobal(
    "fetch",
    rs.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(
        typeof input === "string"
          ? input
          : input instanceof URL
            ? input.href
            : input.url,
        "http://localhost",
      );
      if (url.pathname === "/api/threads/thread-1/inputs") {
        inputReads += 1;
        return jsonResponse({ items: receipts, active_run_id: activeRunId });
      }
      const match = /^\/api\/threads\/thread-1\/runs\/([^/]+)\/inputs$/.exec(
        url.pathname,
      );
      if (match && init?.method === "POST") {
        if (typeof init.body !== "string")
          throw new Error("Expected a JSON run-input request body");
        const submission: SubmittedInput = {
          runId: match[1]!,
          body: JSON.parse(init.body) as SubmittedInput["body"],
        };
        submittedInputs.push(submission);
        const receipt = receiptFor(submission);
        if (submittedInputs.length === 1) {
          if (firstPostFailure === "hang")
            return new Promise<Response>((_resolve, reject) => {
              init.signal?.addEventListener("abort", () =>
                reject(new DOMException("aborted", "AbortError")),
              );
            });
          if (firstPostFailure === "conflict")
            return jsonResponse(
              { detail: "Run is finishing; this input was not accepted" },
              409,
            );
          if (firstPostFailure === "server")
            return jsonResponse({ detail: "Gateway unavailable" }, 500);
          if (firstPostFailure === "malformed")
            return new Response("{", {
              status: 200,
              headers: { "Content-Type": "application/json" },
            });
          if (firstPostFailure === "accepted") receipts.push(receipt);
          throw new TypeError("Failed to fetch");
        }
        receipts.push(receipt);
        return jsonResponse(receipt);
      }
      // History and other thread reads are not under test.
      if ((init?.method ?? "GET") === "GET")
        return jsonResponse({
          data: [],
          has_more: false,
          next_before_seq: null,
        });
      throw new Error(`Unexpected request: ${init?.method} ${url}`);
    }),
  );
});

afterEach(() => {
  cleanup();
  for (const queryClient of queryClients.splice(0)) queryClient.clear();
  rs.useRealTimers();
  rs.unstubAllGlobals();
});

test("offers no run input without an account-owned capability", async () => {
  const { result } = await renderThread({ account: null });
  expect(result.current.canSendInput).toBe(false);
  expect(result.current.activeRunId).toBeUndefined();
  expect(result.current.runInputs).toEqual([]);
  const fetchMock = globalThis.fetch as ReturnType<typeof rs.fn>;
  expect(
    fetchMock.mock.calls.some(([input]) =>
      String(input).includes("/threads/thread-1/inputs"),
    ),
  ).toBe(false);
});

test.each(["queue", "steer"] as const)(
  "sends a rejected %s input to the next active run with a new request ID",
  async (mode) => {
    const { result, queryClient } = await renderThread();
    expect(result.current.canSendInput).toBe(true);
    expect(result.current.activeRunId).toBe("run-1");

    await act(async () => {
      await expect(result.current.sendInput("Continue", mode)).rejects.toThrow(
        "Run is finishing; this input was not accepted",
      );
    });
    expect(submittedInputs).toHaveLength(1);

    await advanceToRun(queryClient, "run-2");
    expect(result.current.activeRunId).toBe("run-2");
    await act(async () => {
      await result.current.sendInput("Continue", mode);
    });

    expect(submittedInputs).toHaveLength(2);
    expect(submittedInputs[1]?.runId).toBe("run-2");
    expect(submittedInputs[1]?.body.request_id).not.toBe(
      submittedInputs[0]?.body.request_id,
    );
  },
);

test.each(["network", "server", "malformed"] as const)(
  "keeps the original run, request ID and context after an uncertain %s failure",
  async (failure) => {
    firstPostFailure = failure;
    const { result, queryClient, rerender } = await renderThread();

    await act(async () => {
      await expect(
        result.current.sendInput("Continue", "queue"),
      ).rejects.toThrow();
    });
    const first = submittedInputs[0]!;
    expect(first.body.context.model_name).toBe("model-original");
    expect(first.body.context.thread_id).toBe("thread-1");

    await advanceToRun(queryClient, "run-2");
    rerender({ modelName: "model-new" });
    await act(async () => {
      await result.current.sendInput("Continue", "queue");
    });

    expect(submittedInputs).toHaveLength(2);
    expect(submittedInputs[1]).toEqual(first);
  },
);

test("recovers an accepted input when the POST response was lost", async () => {
  firstPostFailure = "accepted";
  const { result } = await renderThread();

  await act(async () => {
    await expect(
      result.current.sendInput("Continue", "queue"),
    ).resolves.toEqual(
      expect.objectContaining({
        target_run_id: "run-1",
        text: "Continue",
        mode: "queue",
        status: "pending",
      }),
    );
  });
  expect(submittedInputs).toHaveLength(1);
  await flushFrames();
  expect(result.current.runInputs.map((input) => input.id)).toEqual([
    submittedInputs[0]?.body.request_id,
  ]);
});

test("a steer input carries no settings of its own", async () => {
  firstPostFailure = "accepted";
  const { result } = await renderThread();
  await act(async () => {
    await result.current.sendInput("Use the shorter outline", "steer");
  });
  expect(submittedInputs[0]?.body.context).toEqual({});
});

async function refetchInputs(queryClient: QueryClient) {
  await act(async () => {
    await queryClient.invalidateQueries({ queryKey: runInputsKey("thread-1") });
  });
  await flushFrames();
}

function queueInput(overrides: Partial<RunInput>): RunInput {
  return {
    id: "queued-1",
    target_run_id: "run-1",
    message_id: "message-queued-1",
    text: "Then summarise it",
    mode: "queue",
    status: "pending",
    created_at: "2026-11-04T00:00:00Z",
    ...overrides,
  };
}

test("follows a run it is not streaming once, and records it for Stop and reload", async () => {
  stream.isLoading = false;
  const { result, rerender } = await renderThread();

  expect(stream.joinStream).toHaveBeenCalledTimes(1);
  expect(stream.joinStream).toHaveBeenCalledWith("run-1");
  expect(window.sessionStorage.getItem("lg:stream:thread-1")).toBe("run-1");
  rerender({ modelName: "model-original" });
  await flushFrames();
  expect(stream.joinStream).toHaveBeenCalledTimes(1);
  // Once joined, the SDK's own loading state rules the composer.
  expect(result.current.thread.isLoading).toBe(false);
});

test("does not join when the Gateway reports no run taking inputs", async () => {
  stream.isLoading = false;
  activeRunId = null;
  const { result } = await renderThread();
  expect(stream.joinStream).not.toHaveBeenCalled();
  expect(result.current.canSendInput).toBe(false);
  expect(result.current.thread.isLoading).toBe(false);
});

test("never rejoins the run this tab streamed once it settles", async () => {
  // The SDK names the tab's own run in its reconnect pointer while streaming.
  window.sessionStorage.setItem("lg:stream:thread-1", "run-1");
  const { queryClient, rerender } = await renderThread();
  expect(stream.joinStream).not.toHaveBeenCalled();

  // It settles while the Gateway still reports the run for a moment.
  window.sessionStorage.removeItem("lg:stream:thread-1");
  act(() => {
    stream.options?.onFinish?.(
      { values: {} },
      { run_id: "run-1", thread_id: "thread-1" },
    );
  });
  stream.isLoading = false;
  rerender({ modelName: "model-original" });
  await refetchInputs(queryClient);
  expect(stream.joinStream).not.toHaveBeenCalled();
});

test("follows the next run even when it is reported while this one finishes", async () => {
  window.sessionStorage.setItem("lg:stream:thread-1", "run-1");
  const { queryClient, rerender } = await renderThread();

  // A queued input started run-2 while the SDK still settles run-1.
  await advanceToRun(queryClient, "run-2");
  expect(stream.joinStream).not.toHaveBeenCalled();

  window.sessionStorage.removeItem("lg:stream:thread-1");
  stream.isLoading = false;
  rerender({ modelName: "model-original" });
  await flushFrames();
  expect(stream.joinStream).toHaveBeenCalledTimes(1);
  expect(stream.joinStream).toHaveBeenCalledWith("run-2");
});

test("a cached answer from an earlier visit is not followed", async () => {
  stream.isLoading = false;
  activeRunId = null;
  const { result } = await renderThread({
    cached: { items: [], active_run_id: "run-old" },
  });
  expect(stream.joinStream).not.toHaveBeenCalled();
  expect(result.current.thread.isLoading).toBe(false);
});

test("follows a queued input's run before that run takes inputs", async () => {
  stream.isLoading = false;
  activeRunId = null;
  receipts = [queueInput({ status: "pending" })];
  const { queryClient } = await renderThread();
  expect(stream.joinStream).not.toHaveBeenCalled();

  // Dispatched: the input names its run, which has not reopened inputs yet.
  receipts = [queueInput({ status: "applied", run_id: "run-2" })];
  await refetchInputs(queryClient);
  expect(stream.joinStream).toHaveBeenCalledTimes(1);
  expect(stream.joinStream).toHaveBeenCalledWith("run-2");
});

test("an input applied before the chat opened is not followed", async () => {
  stream.isLoading = false;
  activeRunId = null;
  receipts = [queueInput({ status: "applied", run_id: "run-old" })];
  await renderThread();
  expect(stream.joinStream).not.toHaveBeenCalled();
});

test("polling stops once nothing is running or waiting", async () => {
  stream.isLoading = false;
  activeRunId = null;
  await renderThread();
  const reads = inputReads;
  await act(async () => {
    await rs.advanceTimersByTimeAsync(10_000);
  });
  expect(inputReads).toBe(reads);
});

test("a queued input carries the composer's resolved model settings", async () => {
  firstPostFailure = "accepted";
  const { result } = await renderThread();
  await act(async () => {
    await result.current.sendInput("Then summarise it", "queue", {
      model_name: "model-picked",
      mode: "flash",
      reasoning_effort: "low",
    });
  });
  expect(submittedInputs[0]?.body.context).toMatchObject({
    model_name: "model-picked",
    mode: "flash",
    thread_id: "thread-1",
  });
});

test("an account switch retires a send still in flight", async () => {
  firstPostFailure = "hang";
  const { result, rerender } = await renderThread();
  let sending!: Promise<unknown>;
  act(() => {
    sending = result.current.sendInput("Continue", "queue");
  });
  await flushFrames();
  expect(submittedInputs).toHaveLength(1);

  rerender({ modelName: "model-original", owner: "account-2" });
  await act(async () => {
    await expect(sending).rejects.toMatchObject({ name: "AbortError" });
  });
  await flushFrames();
  // The retired attempt is gone: the next send is a new request.
  firstPostFailure = "accepted";
  await act(async () => {
    await result.current.sendInput("Continue", "queue");
  });
  expect(submittedInputs).toHaveLength(2);
  expect(submittedInputs[1]?.body.request_id).not.toBe(
    submittedInputs[0]?.body.request_id,
  );
});
