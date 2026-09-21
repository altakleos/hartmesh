import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook } from "@testing-library/react";
import type { PropsWithChildren } from "react";

const toast = rs.hoisted(() => ({
  success: rs.fn(),
  error: rs.fn(),
  info: rs.fn(),
}));
const router = rs.hoisted(() => ({ push: rs.fn() }));

rs.mock("sonner", () => ({ toast }));
rs.mock("next/navigation", () => ({ useRouter: () => router }));
rs.mock("@/core/shared/api", () => ({
  publishToShared: rs.fn(),
  removeSharedFile: rs.fn(),
  listSharedFiles: rs.fn(),
}));

import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { publishToShared, removeSharedFile } from "@/core/shared/api";
import { useShareWithEveryone } from "@/core/shared/hooks";

const mockedPublish = rs.mocked(publishToShared);
const mockedRemove = rs.mocked(removeSharedFile);

function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  });
  return function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={queryClient}>
        <I18nContext.Provider
          value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
        >
          {children}
        </I18nContext.Provider>
      </QueryClientProvider>
    );
  };
}

function published(name: string, alreadyShared = false) {
  return {
    alreadyShared,
    file: {
      path: name,
      name,
      size: 1,
      modified: 0,
      virtual_path: `/mnt/user-data/shared/${name}`,
      url: `/api/shared/${name}`,
      published_by: "alex@example.com",
      published_at: "2026-09-19T10:00:00+00:00",
      from_thread_id: null,
      can_remove: true,
    },
  };
}

type ToastOptions = { action: { label: string; onClick: () => void } };

describe("useShareWithEveryone", () => {
  beforeEach(() => {
    mockedPublish.mockReset();
    mockedRemove.mockReset();
    toast.success.mockReset();
    toast.error.mockReset();
    toast.info.mockReset();
    router.push.mockReset();
  });

  afterEach(cleanup);

  it("shares each path and offers to take back what it put there", async () => {
    mockedPublish.mockImplementation(async ({ path }) =>
      published(path.split("/").pop()!),
    );
    mockedRemove.mockResolvedValue(undefined);
    const { result } = renderHook(() => useShareWithEveryone("thread-1"), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.share(
        ["/mnt/user-data/outputs/a.pdf", "/mnt/user-data/outputs/a.xlsx"],
        "Reports",
      );
    });

    expect(mockedPublish).toHaveBeenCalledWith({
      path: "/mnt/user-data/outputs/a.pdf",
      thread_id: "thread-1",
      folder: "Reports",
    });
    expect(toast.success).toHaveBeenCalledTimes(1);
    const [message, options] = toast.success.mock.calls[0]!;
    expect(message).toBe(
      "Shared 2 files with everyone at the company, in Reports",
    );
    expect((options as ToastOptions).action.label).toBe("Undo");
    expect(toast.info).not.toHaveBeenCalled();
    await act(async () => {
      (options as ToastOptions).action.onClick();
    });
    expect(mockedRemove).toHaveBeenNthCalledWith(1, "a.pdf");
    expect(mockedRemove).toHaveBeenNthCalledWith(2, "a.xlsx");
  });

  it("says which folder it went into, because the person did not choose it", async () => {
    mockedPublish.mockResolvedValue(published("august.pdf"));
    const { result } = renderHook(() => useShareWithEveryone("thread-1"), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.share(
        ["/mnt/user-data/outputs/august.pdf"],
        "Reports",
      );
    });

    expect(toast.success).toHaveBeenCalledWith(
      "Shared august.pdf with everyone at the company, in Reports",
      expect.anything(),
    );
  });

  it("says which name the same bytes already carry when it is not the one clicked", async () => {
    mockedPublish.mockResolvedValue(published("august.pdf", true));
    const { result } = renderHook(() => useShareWithEveryone(), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.share(["/mnt/user-data/files/august-final.pdf"]);
    });

    expect(toast.info).toHaveBeenCalledWith(
      "august-final.pdf is already shared with everyone, as august.pdf",
      expect.anything(),
    );
  });

  it("counts a whole batch that was already there", async () => {
    mockedPublish
      .mockResolvedValueOnce(published("a.pdf", true))
      .mockResolvedValueOnce(published("b.xlsx", true));
    const { result } = renderHook(() => useShareWithEveryone("thread-1"), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.share([
        "/mnt/user-data/outputs/a.pdf",
        "/mnt/user-data/outputs/b.xlsx",
      ]);
    });

    expect(toast.success).not.toHaveBeenCalled();
    expect(toast.info).toHaveBeenCalledWith(
      "All 2 files are already shared with everyone",
      expect.anything(),
    );
  });

  it("says a file was already shared, on the server's word, and takes nothing back", async () => {
    // The page keeps no memory of its own clicks: a second share from any
    // tab or session is the server answering with what is already there.
    mockedPublish.mockResolvedValue(published("august.pdf", true));
    const { result } = renderHook(() => useShareWithEveryone(), {
      wrapper: createWrapper(),
    });

    let shared: unknown[] = [];
    await act(async () => {
      shared = await result.current.share(["/mnt/user-data/files/august.pdf"]);
    });

    expect(shared).toHaveLength(1);
    expect(toast.success).not.toHaveBeenCalled();
    expect(toast.info).toHaveBeenCalledTimes(1);
    const [message, options] = toast.info.mock.calls[0]!;
    expect(message).toBe("august.pdf is already shared with everyone");
    expect((options as ToastOptions).action.label).toBe("Open Shared");
    (options as ToastOptions).action.onClick();
    expect(router.push).toHaveBeenCalledWith("/workspace/files?tab=shared");
    expect(mockedRemove).not.toHaveBeenCalled();
  });

  it("undoes only what this click put there when some were already shared", async () => {
    mockedPublish
      .mockResolvedValueOnce(published("a.pdf", true))
      .mockResolvedValueOnce(published("b.xlsx"));
    mockedRemove.mockResolvedValue(undefined);
    const { result } = renderHook(() => useShareWithEveryone("thread-1"), {
      wrapper: createWrapper(),
    });

    await act(async () => {
      await result.current.share([
        "/mnt/user-data/outputs/a.pdf",
        "/mnt/user-data/outputs/b.xlsx",
      ]);
    });

    expect(toast.info).not.toHaveBeenCalled();
    const [message, options] = toast.success.mock.calls[0]!;
    expect(message).toBe("Shared b.xlsx with everyone at the company");
    await act(async () => {
      (options as ToastOptions).action.onClick();
    });
    expect(mockedRemove).toHaveBeenCalledTimes(1);
    expect(mockedRemove).toHaveBeenCalledWith("b.xlsx");
  });

  it("reports a failure in the person's words and keeps what landed", async () => {
    mockedPublish
      .mockResolvedValueOnce(published("a.pdf"))
      .mockRejectedValueOnce(new Error("File not found: b.xlsx"));
    const { result } = renderHook(() => useShareWithEveryone("thread-1"), {
      wrapper: createWrapper(),
    });

    let shared: unknown[] = [];
    await act(async () => {
      shared = await result.current.share([
        "/mnt/user-data/outputs/a.pdf",
        "/mnt/user-data/outputs/b.xlsx",
      ]);
    });

    expect(shared).toHaveLength(1);
    expect(toast.error).toHaveBeenCalledWith(
      "Shared 1 of 2. Couldn't share the rest. Try again.",
    );
    expect(toast.success).not.toHaveBeenCalled();
  });
});
