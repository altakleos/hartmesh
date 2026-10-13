import { afterEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook } from "@testing-library/react";
import type { ReactNode } from "react";

const sdk = rs.hoisted(() => ({
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
      messages: [],
      values: { messages: [], artifacts: [], todos: [], title: "" },
      submit: sdk.submit,
      stop: sdk.stop,
    };
  },
}));
rs.mock("sonner", () => ({ toast: toasts }));
rs.mock("@/core/uploads/api", () => ({ uploadFiles: rs.fn() }));

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
function start() {
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
      ({ id }) =>
        useThreadStream({
          context: DEFAULT_LOCAL_SETTINGS.context,
          displayThreadId: id,
          isMock: true,
        }),
      { wrapper, initialProps: { id: "new-thread" } },
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

describe("storage refusal at the send boundary", () => {
  it("retains admission context when the SDK resolves submit before the error callback", async () => {
    sdk.submit.mockResolvedValue(undefined);
    const { result } = start();
    const message = { text: "preserve this draft", files: [] };
    let sending!: Promise<void>;
    act(() => {
      sending = result.current.sendMessage("new-thread", message);
    });
    await act(async () => {
      await Promise.resolve();
    });
    expect(result.current.runRefusal).toBeNull();
    await act(async () => {
      fail(full);
      await sending.catch(() => undefined);
    });
    expect(result.current.runRefusal).toMatchObject({
      threadId: "new-thread",
      message,
      newThread: true,
      phase: "send",
      refusal: { code: "files-full" },
    });
  });

  it("retiring a held admission never reports success or clears its saved draft", async () => {
    sdk.submit.mockResolvedValue(undefined);
    const { result, unmount } = start();
    const onSent = rs.fn();
    let sending!: Promise<void>;
    act(() => {
      sending = result.current.sendMessage(
        "new-thread",
        { text: "unsent saved draft", files: [] },
        undefined,
        { onSent },
      );
    });
    void sending.catch(() => undefined);
    await act(async () => {
      await Promise.resolve();
    });
    unmount();
    await expect(sending).rejects.toMatchObject({
      name: "ThreadSendNotAdmittedError",
    });
    expect(onSent).not.toHaveBeenCalled();
  });
  it("captures the original draft when the SDK reports an error but resolves submit", async () => {
    // StreamManager catches errors and invokes onError; its submit resolves.
    sdk.submit.mockImplementation(async () => {
      fail(full);
    });
    const { result } = start();
    const message = { text: "  untouched draft  ", files: [] };
    await act(async () => {
      await result.current
        .sendMessage("new-thread", message)
        .catch(() => undefined);
    });
    expect(result.current.runRefusal).toMatchObject({
      threadId: "new-thread",
      phase: "send",
      message,
      newThread: true,
      refusal: { code: "files-full", status: 507 },
    });
    expect(result.current.thread.messages).toEqual([]);
    expect(toasts.error).not.toHaveBeenCalled();
  });

  it.each(["new-thread", "other-thread"])(
    "keeps the admission guard before a second send in %s and rejects only the original files",
    async (id) => {
      sdk.submit.mockResolvedValue(undefined);
      const { result, rerender } = start();
      let original!: Promise<void>;
      act(() => {
        original = result.current.sendMessage("new-thread", {
          text: "original draft",
          files: [],
        });
      });
      await act(async () => {
        await Promise.resolve();
      });
      rerender({ id });
      await act(async () => {
        await expect(
          result.current.sendMessage(id, { text: "new draft", files: [] }),
        ).rejects.toMatchObject({ name: "ThreadSendNotAdmittedError" });
      });
      expect(sdk.submit).toHaveBeenCalledTimes(1);
      await act(async () => {
        fail(full);
        await original.catch(() => undefined);
      });
      if (id === "other-thread") expect(result.current.runRefusal).toBeNull();
      else
        expect(result.current.runRefusal?.message?.text).toBe("original draft");
    },
  );

  it("does not attribute an admitted older run's error to a later same-thread admission", async () => {
    sdk.submit.mockResolvedValue(undefined);
    const { result } = start();
    let original!: Promise<void>;
    act(() => {
      original = result.current.sendMessage("new-thread", {
        text: "first",
        files: [],
      });
    });
    await act(async () => {
      await Promise.resolve();
      created({ thread_id: "new-thread", run_id: "run-A" });
      await original;
    });
    let second!: Promise<void>;
    act(() => {
      second = result.current.sendMessage("new-thread", {
        text: "second",
        files: [],
      });
    });
    await act(async () => {
      await Promise.resolve();
    });
    act(() =>
      sdk.options!.onError(full, { thread_id: "new-thread", run_id: "run-A" }),
    );
    expect(result.current.runRefusal).toBeNull();
    await act(async () => {
      created({ thread_id: "new-thread", run_id: "run-B" });
      await second;
    });
    expect(result.current.runRefusal).toBeNull();
    expect(sdk.submit).toHaveBeenCalledTimes(2);
  });

  it("keeps genuine current-thread reconnect callbacks after a locally admitted run", async () => {
    sdk.submit.mockResolvedValue(undefined);
    const { result } = start();
    let sending!: Promise<void>;
    act(() => {
      sending = result.current.sendMessage("new-thread", {
        text: "first",
        files: [],
      });
    });
    await act(async () => {
      await Promise.resolve();
      created({ thread_id: "new-thread", run_id: "run-A" });
      await sending;
    });
    act(() =>
      sdk.options!.onError(
        { ...full },
        { thread_id: "new-thread", run_id: "joined-run-B" },
      ),
    );
    expect(result.current.runRefusal).toMatchObject({
      threadId: "new-thread",
      newThread: false,
      refusal: { code: "files-full" },
    });
    expect(result.current.runRefusal?.message).toBeUndefined();
  });

  it("does not settle a local admission from an untagged reconnect ending", async () => {
    sdk.submit.mockResolvedValue(undefined);
    const { result } = start();
    let sending!: Promise<void>;
    let settled = false;
    act(() => {
      sending = result.current.sendMessage("new-thread", {
        text: "pending local draft",
        files: [],
      });
    });
    void sending.then(() => {
      settled = true;
    });
    await act(async () => {
      await Promise.resolve();
    });
    act(() =>
      sdk.options!.onFinish!(
        { values: { messages: [], artifacts: [], todos: [], title: "" } },
        { thread_id: "new-thread", run_id: "joined-run-C" },
      ),
    );
    await act(async () => {
      await Promise.resolve();
    });
    expect(settled).toBe(false);
    await act(async () => {
      created({ thread_id: "new-thread", run_id: "local-run-B" });
      await sending;
    });
    expect(settled).toBe(true);
  });

  it("holds Stop's original attempt across admission and navigation before permitting another send", async () => {
    sdk.submit.mockResolvedValue(undefined);
    let finishStop!: () => void;
    sdk.stop.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          finishStop = resolve;
        }),
    );
    const { result, rerender } = start();
    let first!: Promise<void>;
    act(() => {
      first = result.current.sendMessage("new-thread", {
        text: "first",
        files: [],
      });
    });
    await act(async () => {
      await Promise.resolve();
    });
    let stopping!: Promise<void>;
    act(() => {
      stopping = Promise.resolve(result.current.thread.stop());
    });
    await act(async () => {
      await Promise.resolve();
      created({ thread_id: "new-thread", run_id: "run-A" });
      await first;
    });
    rerender({ id: "other-thread" });
    await act(async () => {
      await expect(
        result.current.sendMessage("other-thread", {
          text: "keep this unsent draft",
          files: [],
        }),
      ).rejects.toMatchObject({ name: "ThreadSendNotAdmittedError" });
    });
    expect(sdk.submit).toHaveBeenCalledTimes(1);
    await act(async () => {
      finishStop();
      await stopping;
    });
    let second!: Promise<void>;
    act(() => {
      second = result.current.sendMessage("other-thread", {
        text: "keep this unsent draft",
        files: [],
      });
    });
    await act(async () => {
      await Promise.resolve();
      created({ thread_id: "other-thread", run_id: "run-B" }, 1);
      await second;
    });
    act(() => {
      fail(full, 0);
    });
    expect(result.current.runRefusal).toBeNull();
    expect(sdk.submit).toHaveBeenCalledTimes(2);
  });

  it.each([true, false])(
    "Stop success=%s retains or retires the originating unresolved admission",
    async (success) => {
      sdk.submit.mockResolvedValue(undefined);
      sdk.stop.mockImplementation(async () => {
        if (!success) throw new Error("Stop unavailable");
      });
      const { result } = start();
      let original!: Promise<void>;
      act(() => {
        original = result.current.sendMessage("new-thread", {
          text: "original",
          files: [],
        });
      });
      void original.catch(() => undefined);
      await act(async () => {
        await Promise.resolve();
      });
      await act(async () => {
        await result.current.thread.stop();
      });
      if (success) await original.catch(() => undefined);
      let next!: Promise<void>;
      act(() => {
        next = result.current.sendMessage("new-thread", {
          text: "later",
          files: [],
        });
      });
      const nextOutcome = next.catch((error: unknown) => error);
      await act(async () => {
        await Promise.resolve();
      });
      expect(sdk.submit).toHaveBeenCalledTimes(success ? 2 : 1);
      // Late first-request metadata and a metadata-less refusal retain A's origin.
      act(() => {
        if (success)
          created({ thread_id: "new-thread", run_id: "old-run-A" }, 0);
        fail(full, 0);
      });
      if (success) {
        expect(result.current.runRefusal).toBeNull();
        await act(async () => {
          created({ thread_id: "new-thread", run_id: "new-run-B" }, 1);
          await next;
        });
        expect(result.current.runRefusal).toBeNull();
      } else {
        await act(async () => {
          await original.catch(() => undefined);
          expect(await nextOutcome).toMatchObject({
            name: "ThreadSendNotAdmittedError",
          });
        });
        expect(result.current.runRefusal?.message?.text).toBe("original");
      }
    },
  );

  it("also captures a direct submit rejection and refreshes feature discovery", async () => {
    sdk.submit.mockRejectedValue(full);
    const { result, client } = start();
    const invalidate = rs.spyOn(client, "invalidateQueries");
    await act(async () => {
      await result.current
        .sendMessage("new-thread", { text: "draft", files: [] })
        .catch(() => undefined);
    });
    expect(result.current.runRefusal?.refusal.code).toBe("files-full");
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ["features"] });
  });

  it("keeps upload refusal separate and never starts a model run", async () => {
    rs.mocked(uploadFiles).mockRejectedValue(full);
    const { result } = start();
    const file = new File(["small fixture"], "statement.txt", {
      type: "text/plain",
    });
    const message = {
      text: "keep this text",
      files: [
        {
          type: "file" as const,
          url: "blob:fixture",
          mediaType: "text/plain",
          filename: file.name,
          file,
        },
      ],
    };
    await act(async () => {
      await result.current
        .sendMessage("new-thread", message)
        .catch(() => undefined);
    });
    expect(result.current.runRefusal).toMatchObject({
      phase: "upload",
      message,
      refusal: { code: "files-full" },
    });
    expect(sdk.submit).not.toHaveBeenCalled();
    expect(toasts.error).not.toHaveBeenCalled();
  });

  it("keeps uploaded attachments pending until a later run admission refusal is known", async () => {
    rs.mocked(uploadFiles).mockResolvedValue({
      success: true,
      files: [
        {
          filename: "statement.txt",
          size: 7,
          path: "uploads/statement.txt",
          virtual_path: "/mnt/user-data/uploads/statement.txt",
          artifact_url: "/api/artifact",
        },
      ],
      message: "",
      skipped_files: [],
    });
    sdk.submit.mockResolvedValue(undefined);
    const { result } = start();
    const file = new File(["fixture"], "statement.txt");
    const message = {
      text: "retain the attachment",
      files: [
        {
          type: "file" as const,
          mediaType: "text/plain",
          filename: file.name,
          file,
          url: "blob:fixture",
        },
      ],
    };
    let sending!: Promise<void>;
    let settled = false;
    act(() => {
      sending = result.current.sendMessage("new-thread", message);
    });
    const outcome = sending.then(
      () => {
        settled = true;
      },
      () => {
        settled = true;
      },
    );
    await act(async () => {
      await Promise.resolve();
      await Promise.resolve();
    });
    expect(settled).toBe(false);
    expect(sdk.submit).toHaveBeenCalledTimes(1);
    await act(async () => {
      fail(full);
      await outcome;
    });
    expect(result.current.runRefusal).toMatchObject({ phase: "send", message });
    expect(settled).toBe(true);
  });

  it("does not apply a late refusal to another displayed conversation", async () => {
    let fail!: () => void;
    sdk.submit.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          const options = sdk.options!;
          fail = () => {
            submissionFailed(metadata(0), full);
            options.onError(full);
            resolve();
          };
        }),
    );
    const { result, rerender } = start();
    let sending!: Promise<void>;
    act(() => {
      sending = result.current.sendMessage("new-thread", {
        text: "old draft",
        files: [],
      });
    });
    rerender({ id: "other-thread" });
    await act(async () => {
      fail();
      await sending.catch(() => undefined);
    });
    expect(result.current.runRefusal).toBeNull();
    expect(toasts.error).not.toHaveBeenCalled();
  });
});
