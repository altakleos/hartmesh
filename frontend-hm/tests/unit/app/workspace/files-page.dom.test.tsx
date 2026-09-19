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

rs.mock("next/navigation", () => ({
  usePathname: () => "/workspace/files",
  useRouter: () => ({ push: rs.fn(), replace: rs.fn(), prefetch: rs.fn() }),
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
rs.mock("@/core/files", () => ({
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

describe("FilesPage", () => {
  beforeEach(() => {
    files.data = { files: [AUGUST, NOTES], count: 2, truncated: false };
    files.error = null;
    files.isPending = false;
    files.deleteMutate.mockReset();
    files.refetch.mockReset();
  });

  afterEach(cleanup);

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
