import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";

/**
 * The Files page: what the person kept, from every conversation.
 *
 * The page is a list and a delete; what it fetches is mocked at the hook so
 * the test is about what is shown and what a click does, not about React
 * Query.
 */
const files = rs.hoisted(() => ({
  data: undefined as
    | {
        files: Array<{
          path: string;
          name: string;
          size: number;
          modified: number;
          virtual_path: string;
          url: string;
        }>;
        count: number;
        truncated: boolean;
      }
    | undefined,
  error: null as Error | null,
  isPending: false,
  refetch: rs.fn(),
  deleteMutate: rs.fn(),
  deletePending: false,
}));

const routerReplace = rs.hoisted(() => rs.fn());
rs.mock("next/navigation", () => ({
  usePathname: () => "/workspace/files",
  useRouter: () => ({ push: rs.fn(), replace: routerReplace, prefetch: rs.fn() }),
  useSearchParams: () => new URLSearchParams(window.location.search),
}));
rs.mock("sonner", () => ({ toast: { success: rs.fn(), error: rs.fn() } }));
// The tab title asks the deployment whose workspace this is; not what this is about.
rs.mock("@/core/features", () => ({ useDocumentTitle: () => undefined }));
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceBody: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceHeader: () => null,
}));
const shared = rs.hoisted(() => ({
  data: undefined as
    | {
        files: Array<{
          path: string;
          name: string;
          size: number;
          modified: number;
          virtual_path: string;
          url: string;
          published_by: string | null;
          published_at: string | null;
          from_thread_id: string | null;
          can_remove: boolean;
        }>;
        count: number;
        truncated: boolean;
      }
    | undefined,
  error: null as Error | null,
  isPending: false,
  refetch: rs.fn(),
  removeMutate: rs.fn(),
  removePending: false,
}));

const shareWithEveryone = rs.hoisted(() => rs.fn());
rs.mock("@/core/shared", () => ({
  urlOfSharedFile: (path: string, { download = false } = {}) =>
    `/api/shared/${path}${download ? "?download=true" : ""}`,
  useSharedFiles: () => ({
    data: shared.data,
    error: shared.error,
    isPending: shared.isPending,
    refetch: shared.refetch,
  }),
  useRemoveSharedFile: () => ({
    mutate: shared.removeMutate,
    isPending: shared.removePending,
  }),
  useShareWithEveryone: () => ({
    share: shareWithEveryone,
    isPending: false,
    hasShared: () => false,
    openShared: rs.fn(),
  }),
}));
rs.mock("@/core/files", () => ({
  MY_FILES_VIRTUAL_PREFIX: "/mnt/user-data/files",
  urlOfMyFile: (path: string, { download = false } = {}) =>
    `/api/files/${path}${download ? "?download=true" : ""}`,
  useMyFiles: () => ({
    data: files.data,
    error: files.error,
    isPending: files.isPending,
    refetch: files.refetch,
  }),
  useDeleteMyFile: () => ({
    mutate: files.deleteMutate,
    isPending: files.deletePending,
  }),
}));

import FilesPage from "@/app/workspace/files/page";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";

function renderPage() {
  return render(
    <I18nContext.Provider
      value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
    >
      <FilesPage />
    </I18nContext.Provider>,
  );
}

const AUGUST = {
  path: "Reports/august.pdf",
  name: "august.pdf",
  size: 48_213,
  modified: Date.now() / 1000 - 3600,
  virtual_path: "/mnt/user-data/files/Reports/august.pdf",
  url: "/api/files/Reports/august.pdf",
};
const NOTES = {
  path: "notes.txt",
  name: "notes.txt",
  size: 12,
  modified: Date.now() / 1000 - 86_400,
  virtual_path: "/mnt/user-data/files/notes.txt",
  url: "/api/files/notes.txt",
};

const PUBLISHED = {
  path: "Reports/august.pdf",
  name: "august.pdf",
  size: 48_213,
  modified: Date.now() / 1000 - 3600,
  virtual_path: "/mnt/user-data/shared/Reports/august.pdf",
  url: "/api/shared/Reports/august.pdf",
  published_by: "owner-1",
  published_at: new Date(Date.now() - 3600_000).toISOString(),
  from_thread_id: "11111111-1111-1111-1111-111111111111",
  can_remove: true,
};
const PLACED = {
  path: "Exports/jobs.xlsx",
  name: "jobs.xlsx",
  size: 1024,
  modified: Date.now() / 1000 - 86_400,
  virtual_path: "/mnt/user-data/shared/Exports/jobs.xlsx",
  url: "/api/shared/Exports/jobs.xlsx",
  published_by: null,
  published_at: null,
  from_thread_id: null,
  can_remove: false,
};

function openSharedTab() {
  // A tab trigger activates on pointer down, the way the tab primitive does.
  fireEvent.mouseDown(screen.getByTestId("files-tab-shared"), { button: 0 });
}

describe("FilesPage", () => {
  beforeEach(() => {
    files.data = { files: [AUGUST, NOTES], count: 2, truncated: false };
    files.error = null;
    files.isPending = false;
    files.deleteMutate.mockReset();
    files.refetch.mockReset();
    shared.data = { files: [PUBLISHED, PLACED], count: 2, truncated: false };
    shared.error = null;
    shared.isPending = false;
    shared.removeMutate.mockReset();
    shared.refetch.mockReset();
    window.history.replaceState(null, "", "/workspace/files");
  });

  afterEach(cleanup);

  it("opens on the person's own files, and a link can ask for Shared", () => {
    renderPage();
    expect(screen.getByTestId("my-files-list")).toBeTruthy();
    expect(screen.queryByTestId("shared-files-list")).toBeNull();
    cleanup();

    window.history.replaceState(null, "", "/workspace/files?tab=shared");
    renderPage();
    expect(screen.getByTestId("shared-files-list")).toBeTruthy();
    // One page, two tabs: the heading names the page, so clicking "My files"
    // in the sidebar never lands on a page headed something else.
    expect(screen.getByRole("heading", { level: 1 }).textContent).toBe("Files");
  });

  it("puts the open tab in the URL, so a copied link lands where the person was", () => {
    renderPage();
    routerReplace.mockClear();

    openSharedTab();
    expect(routerReplace).toHaveBeenCalledWith("/workspace/files?tab=shared");

    fireEvent.mouseDown(screen.getByTestId("files-tab-mine"), { button: 0 });
    expect(routerReplace).toHaveBeenCalledWith("/workspace/files");
  });

  it("lists what the company shared, with who put it there", () => {
    renderPage();
    openSharedTab();

    const list = screen.getByTestId("shared-files-list");
    const august = within(list).getByTestId("shared-file-Reports/august.pdf");
    expect(
      within(august)
        .getByRole("link", { name: "august.pdf" })
        .getAttribute("href"),
    ).toBe("/api/shared/Reports/august.pdf");
    expect(august.textContent).toContain("Reports");
    // Who shared it is a column, not a tooltip: it is the first thing asked of
    // a company folder, and a tooltip is unreachable on a phone.
    expect(august.textContent).toContain("owner-1");
    expect(
      within(august)
        .getByRole("link", { name: "Download august.pdf" })
        .getAttribute("href"),
    ).toBe("/api/shared/Reports/august.pdf?download=true");
    // A file an operator placed by hand says so, and is nobody's to remove.
    const jobs = within(list).getByTestId("shared-file-Exports/jobs.xlsx");
    expect(jobs.textContent).toContain("No record of who shared this");
    expect(within(jobs).queryByRole("button", { name: /Remove/ })).toBeNull();
  });

  it("offers Remove only where the server said the person may, and asks first", () => {
    renderPage();
    openSharedTab();

    fireEvent.click(screen.getByRole("button", { name: "Remove august.pdf" }));
    expect(
      screen.getByText(/Remove august.pdf from Shared\?/).textContent,
    ).toContain("Your own copy, if you have one, stays.");
    expect(shared.removeMutate).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: /^Remove$/ }));

    expect(shared.removeMutate).toHaveBeenCalledWith(
      "Reports/august.pdf",
      expect.anything(),
    );
  });

  it("lets a person hand one of their own files to the whole company", () => {
    renderPage();

    const list = screen.getByTestId("my-files-list");
    const august = within(list).getByTestId("my-file-Reports/august.pdf");
    fireEvent.click(
      within(august).getByRole("button", {
        name: "Share with everyone august.pdf",
      }),
    );

    // The path the sandbox knows it by, so the server can find the caller's
    // own copy; no conversation is involved, so no thread.
    expect(shareWithEveryone).toHaveBeenCalledWith([
      "/mnt/user-data/files/Reports/august.pdf",
    ]);
  });

  it("says when nothing was shared yet, and how something gets here", () => {
    shared.data = { files: [], count: 0, truncated: false };
    renderPage();
    openSharedTab();

    const empty = screen.getByTestId("shared-files-empty");
    expect(empty.textContent).toContain("Nothing shared yet");
    expect(empty.textContent).toContain("Share with everyone");
  });

  it("offers to try again when Shared could not be loaded", () => {
    shared.data = undefined;
    shared.error = new Error("HTTP 503");
    renderPage();
    openSharedTab();

    const failure = screen.getByTestId("shared-files-load-error");
    expect(failure.textContent).toContain("Couldn't load what was shared");
    fireEvent.click(within(failure).getByRole("button", { name: "Try again" }));
    expect(shared.refetch).toHaveBeenCalled();
  });

  it("lists what was kept, with its folder, size and a download", () => {
    renderPage();

    const list = screen.getByTestId("my-files-list");
    const august = within(list).getByTestId("my-file-Reports/august.pdf");
    expect(
      within(august)
        .getByRole("link", { name: "august.pdf" })
        .getAttribute("href"),
    ).toBe("/api/files/Reports/august.pdf");
    expect(august.textContent).toContain("Reports");
    expect(august.textContent).toContain("47.1 KiB");
    expect(
      within(august)
        .getByRole("link", { name: "Download august.pdf" })
        .getAttribute("href"),
    ).toBe("/api/files/Reports/august.pdf?download=true");
    // A file at the root has no folder to name.
    const notes = within(list).getByTestId("my-file-notes.txt");
    expect(notes.textContent).toContain("—");
  });

  it("asks before deleting, then deletes that file", () => {
    renderPage();

    fireEvent.click(screen.getByRole("button", { name: "Delete august.pdf" }));
    expect(
      screen.getByText("Delete august.pdf? This cannot be undone."),
    ).toBeTruthy();
    expect(files.deleteMutate).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: /^Delete$/ }));

    expect(files.deleteMutate).toHaveBeenCalledWith(
      "Reports/august.pdf",
      expect.anything(),
    );
  });

  it("says when there is nothing yet, and how something gets here", () => {
    files.data = { files: [], count: 0, truncated: false };
    renderPage();

    const empty = screen.getByTestId("my-files-empty");
    expect(empty.textContent).toContain("Nothing kept yet");
    expect(empty.textContent).toContain("Save a report or an upload");
  });

  it("says when the list stopped short", () => {
    files.data = { files: [AUGUST], count: 1, truncated: true };
    renderPage();

    expect(
      screen.getByText(
        "Showing the first 1 files. Delete some to see the rest.",
      ),
    ).toBeTruthy();
  });

  it("offers to try again when the list could not be loaded", () => {
    files.data = undefined;
    files.error = new Error("HTTP 503");
    renderPage();

    const failure = screen.getByTestId("my-files-load-error");
    expect(failure.textContent).toContain("Couldn't load your files");
    fireEvent.click(within(failure).getByRole("button", { name: "Try again" }));
    expect(files.refetch).toHaveBeenCalled();
  });
});
