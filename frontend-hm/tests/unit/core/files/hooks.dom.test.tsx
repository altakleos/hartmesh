import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  render,
  renderHook,
  waitFor,
} from "@testing-library/react";
import type { PropsWithChildren } from "react";

const toast = rs.hoisted(() => ({
  success: rs.fn(),
  error: rs.fn(),
  dismiss: rs.fn(),
}));
const router = rs.hoisted(() => ({ push: rs.fn() }));

rs.mock("sonner", () => ({ toast }));
rs.mock("next/navigation", () => ({ useRouter: () => router }));
rs.mock("@/core/files/api", () => ({
  keepInMyFiles: rs.fn(),
  listMyFiles: rs.fn(),
  deleteMyFile: rs.fn(),
}));

import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { keepInMyFiles } from "@/core/files/api";
import { useSaveToMyFiles } from "@/core/files/hooks";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

const mockedKeep = rs.mocked(keepInMyFiles);

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={queryClient}>
        <FileActionLifetimeProvider>
          <I18nContext.Provider
            value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
          >
            {children}
          </I18nContext.Provider>
        </FileActionLifetimeProvider>
      </QueryClientProvider>
    );
  };
}

function kept(name: string) {
  return {
    path: name,
    name,
    size: 1,
    modified: 0,
    virtual_path: `/mnt/user-data/files/${name}`,
    url: `/api/files/${name}`,
  };
}

describe("useSaveToMyFiles", () => {
  beforeEach(() => {
    mockedKeep.mockReset();
    toast.success.mockReset();
    toast.error.mockReset();
    toast.dismiss.mockReset();
    router.push.mockReset();
  });

  afterEach(cleanup);

  it("finishes a save batch during same-account navigation away from its card", async () => {
    let resolve!: (value: ReturnType<typeof kept>) => void;
    mockedKeep
      .mockReturnValueOnce(
        new Promise((done) => {
          resolve = done;
        }),
      )
      .mockResolvedValue(kept("second.pdf"));
    const Wrapper = createWrapper();
    let save!: ReturnType<typeof useSaveToMyFiles>["save"];
    function Card() {
      save = useSaveToMyFiles("alice-thread").save;
      return null;
    }
    const mounted = render(
      <Wrapper>
        <Card />
      </Wrapper>,
    );
    let saving!: Promise<unknown[]>;
    act(() => {
      saving = save([
        "/mnt/user-data/outputs/first.pdf",
        "/mnt/user-data/outputs/second.pdf",
      ]);
    });
    await waitFor(() => expect(mockedKeep).toHaveBeenCalledTimes(1));
    mounted.rerender(
      <Wrapper>
        <div>Files page</div>
      </Wrapper>,
    );
    await act(async () => {
      resolve(kept("first.pdf"));
      await saving;
    });
    expect(mockedKeep).toHaveBeenCalledTimes(2);
    expect(toast.success).toHaveBeenCalledTimes(1);
  });

  it.each(["success", "failure"])(
    "stops a retired account's save batch after a pending %s",
    async (outcome) => {
      let resolve!: (value: ReturnType<typeof kept>) => void;
      let reject!: (error: Error) => void;
      const response = new Promise<ReturnType<typeof kept>>((done, fail) => {
        resolve = done;
        reject = fail;
      });
      mockedKeep
        .mockReturnValueOnce(response)
        .mockResolvedValue(kept("second.pdf"));
      const { result, unmount } = renderHook(
        () => useSaveToMyFiles("alice-thread"),
        { wrapper: createWrapper() },
      );
      let saving!: Promise<unknown[]>;
      act(() => {
        saving = result.current.save([
          "/mnt/user-data/outputs/alice.pdf",
          "/mnt/user-data/outputs/second.pdf",
        ]);
      });
      await waitFor(() => expect(mockedKeep).toHaveBeenCalledTimes(1));
      unmount(); // AuthProvider retires this subtree when the identity changes.
      await act(async () => {
        if (outcome === "success") resolve(kept("alice.pdf"));
        else reject(new Error("old account request failed"));
        await saving;
      });
      expect(mockedKeep).toHaveBeenCalledTimes(1);
      expect(toast.success).not.toHaveBeenCalled();
      expect(toast.error).not.toHaveBeenCalled();
    },
  );

  it("dismisses its private filename toast and retires its action on unmount", async () => {
    mockedKeep.mockResolvedValue(kept("alice-private.pdf"));
    toast.success.mockReturnValue("saved-toast");
    const { result, unmount } = renderHook(
      () => useSaveToMyFiles("alice-thread"),
      { wrapper: createWrapper() },
    );
    await act(async () => {
      await result.current.save(["/mnt/user-data/outputs/alice-private.pdf"]);
    });
    const options = toast.success.mock.calls[0]![1] as {
      action: { onClick: () => void };
    };
    unmount();
    options.action.onClick();
    expect(router.push).not.toHaveBeenCalled();
    expect(toast.dismiss).toHaveBeenCalledWith("saved-toast");
  });

  it("keeps each path and says so once, with a way to the files", async () => {
    mockedKeep.mockImplementation(async (_threadId, { path }) =>
      kept(path.split("/").pop()!),
    );
    const { result } = renderHook(() => useSaveToMyFiles("thread-1"), {
      wrapper: createWrapper(),
    });

    let saved: unknown[] = [];
    await act(async () => {
      saved = await result.current.save([
        "/mnt/user-data/outputs/a.pdf",
        "/mnt/user-data/outputs/a.xlsx",
      ]);
    });

    expect(saved).toHaveLength(2);
    expect(mockedKeep).toHaveBeenCalledTimes(2);
    expect(mockedKeep).toHaveBeenCalledWith("thread-1", {
      path: "/mnt/user-data/outputs/a.pdf",
      folder: undefined,
    });
    expect(toast.success).toHaveBeenCalledTimes(1);
    const [message, options] = toast.success.mock.calls[0]!;
    expect(message).toBe("Saved 2 files to My files");
    (options as { action: { onClick: () => void } }).action.onClick();
    expect(router.push).toHaveBeenCalledWith("/workspace/files");
  });

  it("names one kept file by its name", async () => {
    mockedKeep.mockResolvedValue(kept("august.pdf"));
    const { result } = renderHook(() => useSaveToMyFiles("thread-1"), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.save(["/mnt/user-data/outputs/august.pdf"]);
    });

    expect(toast.success).toHaveBeenCalledWith(
      "Saved august.pdf to My files",
      expect.anything(),
    );
  });

  it("says which folder it went into, because the person did not choose it", async () => {
    mockedKeep.mockResolvedValue(kept("august.pdf"));
    const { result } = renderHook(() => useSaveToMyFiles("thread-1"), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.save(
        ["/mnt/user-data/outputs/august.pdf"],
        "Reports",
      );
    });

    expect(toast.success).toHaveBeenCalledWith(
      "Saved august.pdf to My files, in Reports",
      expect.anything(),
    );
  });

  it("reports a failure in the person's words and keeps what it can", async () => {
    // The failing render is in the middle: what comes after it is still kept.
    mockedKeep
      .mockResolvedValueOnce(kept("a.pdf"))
      .mockRejectedValueOnce(new Error("File not found: b.xlsx"))
      .mockResolvedValueOnce(kept("c.docx"));
    const { result } = renderHook(() => useSaveToMyFiles("thread-1"), {
      wrapper: createWrapper(),
    });

    let saved: unknown[] = [];
    await act(async () => {
      saved = await result.current.save([
        "/mnt/user-data/outputs/a.pdf",
        "/mnt/user-data/outputs/b.xlsx",
        "/mnt/user-data/outputs/c.docx",
      ]);
    });

    expect(saved).toHaveLength(2);
    expect(mockedKeep).toHaveBeenCalledTimes(3);
    // Not the Gateway's reason, which names paths and rules in system words.
    expect(toast.error).toHaveBeenCalledWith(
      "Saved 2 of 3. Couldn't save the rest. Try again.",
    );
    expect(toast.success).not.toHaveBeenCalled();
  });

  it("says nothing landed when the first keep fails", async () => {
    mockedKeep.mockRejectedValueOnce(new Error("Path traversal detected"));
    const { result } = renderHook(() => useSaveToMyFiles("thread-1"), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.save(["/mnt/user-data/outputs/a.pdf"]);
    });

    expect(toast.error).toHaveBeenCalledWith(
      "Couldn't save to My files. Try again.",
    );
  });
});
