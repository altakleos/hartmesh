import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, cleanup, renderHook } from "@testing-library/react";
import type { PropsWithChildren } from "react";

const toast = rs.hoisted(() => ({ success: rs.fn(), error: rs.fn() }));
const router = rs.hoisted(() => ({ push: rs.fn() }));

rs.mock("sonner", () => ({ toast }));
rs.mock("next/navigation", () => ({ useRouter: () => router }));
rs.mock("@/core/files/api", () => ({
  keepInMyFiles: rs.fn(),
  listMyFiles: rs.fn(),
  deleteMyFile: rs.fn(),
}));

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
        <I18nContext.Provider
          value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
        >
          {children}
        </I18nContext.Provider>
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
    router.push.mockReset();
  });

  afterEach(cleanup);

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

  it("reports a failure by its reason and keeps what already landed", async () => {
    mockedKeep
      .mockResolvedValueOnce(kept("a.pdf"))
      .mockRejectedValueOnce(new Error("File not found: b.xlsx"));
    const { result } = renderHook(() => useSaveToMyFiles("thread-1"), {
      wrapper: createWrapper(),
    });

    let saved: unknown[] = [];
    await act(async () => {
      saved = await result.current.save([
        "/mnt/user-data/outputs/a.pdf",
        "/mnt/user-data/outputs/b.xlsx",
      ]);
    });

    expect(saved).toHaveLength(1);
    expect(toast.error).toHaveBeenCalledWith("File not found: b.xlsx");
    expect(toast.success).not.toHaveBeenCalled();
  });
});
