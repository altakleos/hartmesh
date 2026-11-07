import type { Run } from "@langchain/langgraph-sdk";
import { afterEach, beforeEach, expect, rs, test } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook } from "@testing-library/react";
import { createElement, type ReactNode } from "react";

import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { DEFAULT_LOCAL_SETTINGS } from "@/core/settings/local";
import { useThreadStream } from "@/core/threads/hooks";

const api = rs.hoisted(() => ({ list: rs.fn(async () => [] as Run[]) }));
const stream = rs.hoisted(() => ({
  isLoading: false,
  joinStream: rs.fn(async (_runId: string) => undefined),
  onFinish: undefined as
    | ((
        state: {
          values: { messages: never[]; artifacts: never[]; title: string };
        },
        meta: { thread_id: string; run_id: string },
      ) => void)
    | undefined,
}));
rs.mock("@/core/api", () => ({
  getAPIClient: () => ({ runs: { list: api.list } }),
}));
rs.mock("@langchain/langgraph-sdk/react", () => ({
  useStream: (options: { onFinish: typeof stream.onFinish }) => {
    stream.onFinish = options.onFinish;
    return {
      isLoading: stream.isLoading,
      joinStream: stream.joinStream,
      messages: [],
      values: { messages: [], artifacts: [], title: "", todos: [] },
      stop: async () => undefined,
      submit: async () => undefined,
    };
  },
}));

const pending = {
  run_id: "batch-followup",
  status: "pending",
  created_at: "2026-11-07T01:02:03",
} as Run;
const clients: QueryClient[] = [];

function renderThread(cached?: Run[]) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  if (cached) client.setQueryData(["thread", "thread-1"], cached);
  const wrapper = ({ children }: { children: ReactNode }) =>
    createElement(
      QueryClientProvider,
      { client },
      createElement(
        I18nContext.Provider,
        { value: { locale: "en-US", setLocale: () => undefined, t: enUS } },
        children,
      ),
    );
  return {
    client,
    ...renderHook(
      ({ threadId }) =>
        useThreadStream({
          threadId,
          context: DEFAULT_LOCAL_SETTINGS.context,
        }),
      { wrapper, initialProps: { threadId: "thread-1" } },
    ),
  };
}

async function flush() {
  for (let index = 0; index < 8; index += 1) {
    await act(async () => {
      await rs.advanceTimersByTimeAsync(1);
    });
  }
}

beforeEach(() => {
  rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
  window.sessionStorage.clear();
  api.list.mockReset().mockResolvedValue([]);
  stream.joinStream.mockReset().mockResolvedValue(undefined);
  stream.isLoading = false;
  rs.stubGlobal(
    "fetch",
    rs.fn(
      async () =>
        new Response(
          JSON.stringify({
            data: [],
            has_more: false,
            next_before_seq: null,
            items: [],
            active_run_id: null,
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ),
    ),
  );
});
afterEach(() => {
  cleanup();
  for (const client of clients.splice(0)) client.clear();
  rs.unstubAllGlobals();
  rs.useRealTimers();
});

test("discovers a pending followup after its batch receipt, before run inputs open", async () => {
  const { client, result, rerender } = renderThread();
  await flush();
  expect(stream.joinStream).not.toHaveBeenCalled();
  api.list.mockResolvedValue([pending]);
  await act(async () => {
    await client.invalidateQueries({ queryKey: ["thread", "thread-1"] });
  });
  await flush();
  expect(stream.joinStream).toHaveBeenCalledExactlyOnceWith("batch-followup");
  expect(window.sessionStorage.getItem("lg:stream:thread-1")).toBe(
    "batch-followup",
  );
  stream.isLoading = true;
  rerender({ threadId: "thread-1" });
  expect(result.current.activeRunStartedAt).toBe(
    Date.parse("2026-11-07T01:02:03Z"),
  );
});

test("does not follow a cached active run until a fresh server read confirms it", async () => {
  let finish!: (runs: Run[]) => void;
  api.list.mockImplementation(
    () =>
      new Promise<Run[]>((resolve) => {
        finish = resolve;
      }),
  );
  renderThread([pending]);
  await flush();
  expect(stream.joinStream).not.toHaveBeenCalled();
  await act(async () => {
    finish([{ ...pending, status: "success" }]);
  });
  await flush();
  expect(stream.joinStream).not.toHaveBeenCalled();
});

test("leaves an existing reconnect pointer to the SDK", async () => {
  window.sessionStorage.setItem("lg:stream:thread-1", "batch-followup");
  api.list.mockResolvedValue([pending]);
  renderThread();
  await flush();
  expect(stream.joinStream).not.toHaveBeenCalled();
});

test("a late run read for the previous thread cannot attach to the new view", async () => {
  let finish!: (runs: Run[]) => void;
  api.list.mockImplementationOnce(
    () =>
      new Promise<Run[]>((resolve) => {
        finish = resolve;
      }),
  );
  const { rerender } = renderThread();
  await flush();
  rerender({ threadId: "thread-2" });
  await act(async () => {
    finish([pending]);
  });
  await flush();
  expect(stream.joinStream).not.toHaveBeenCalled();
  expect(window.sessionStorage.getItem("lg:stream:thread-2")).toBeNull();
});

test("a completed stream is not rejoined from a stale pending run row", async () => {
  api.list.mockResolvedValue([pending]);
  const { client } = renderThread();
  await flush();
  expect(stream.joinStream).toHaveBeenCalledTimes(1);
  act(() =>
    stream.onFinish?.(
      { values: { messages: [], artifacts: [], title: "" } },
      {
        thread_id: "thread-1",
        run_id: "batch-followup",
      },
    ),
  );
  await act(async () => {
    await client.invalidateQueries({ queryKey: ["thread", "thread-1"] });
  });
  await flush();
  expect(stream.joinStream).toHaveBeenCalledTimes(1);
});
