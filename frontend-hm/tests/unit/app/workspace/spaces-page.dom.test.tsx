import { afterEach, beforeEach, expect, it, rs } from "@rstest/core";
import {
  cleanup,
  act,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  enabled: false,
  readOnly: false,
  exportable: true,
  spaceError: null as Error | null,
  navigate: rs.fn(),
  write: rs.fn(),
  files: [
    {
      name: "page.html",
      path: "page.html",
      kind: "file",
      size: 12,
      modified: 0,
      accessible: true,
      target_kind: undefined as "file" | "directory" | undefined,
    },
  ],
}));
rs.mock("@/core/features", () => ({
  useStorageSpacesEnabled: () => ({ enabled: state.enabled, isLoading: false }),
  useDocumentTitle: () => undefined,
}));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({ user: { id: "alice" } }),
}));
rs.mock("next/navigation", () => ({
  useSearchParams: () => new URLSearchParams("space=" + "a".repeat(32)),
  useRouter: () => ({ replace: state.navigate }),
}));
rs.mock("@/core/spaces/hooks", () => ({
  useSpaces: () => ({ data: { spaces: [] }, refetch: rs.fn() }),
  useSpace: () => ({
    error: state.spaceError,
    data: {
      id: "a".repeat(32),
      name: "Home",
      generation: 1,
      mode: "native",
      status: "active",
      permissions: state.readOnly ? (state.exportable ? 17 : 1) : 27,
    },
    refetch: rs.fn(),
  }),
  useSpaceFiles: () => ({
    data: { files: state.files, truncated: false },
    refetch: rs.fn(),
  }),
}));
rs.mock("@/core/spaces/api", { spy: true });
rs.mock("@/components/workspace/workspace-container", () => ({
  WorkspaceContainer: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceBody: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
  WorkspaceHeader: () => null,
}));

import SpacesPage from "@/app/workspace/spaces/page";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
import { createSpace, readSpaceText, writeSpaceFile } from "@/core/spaces/api";

function show() {
  return render(
    <I18nProvider initialLocale="en-US">
      <FileActionLifetimeProvider>
        <SpacesPage />
      </FileActionLifetimeProvider>
    </I18nProvider>,
  );
}

beforeEach(() => {
  state.enabled = false;
  state.readOnly = false;
  state.exportable = true;
  state.spaceError = null;
  state.navigate.mockReset();
  state.files = [
    {
      name: "page.html",
      path: "page.html",
      kind: "file",
      size: 12,
      modified: 0,
      accessible: true,
      target_kind: undefined,
    },
  ];
  rs.mocked(createSpace).mockReset();
  rs.mocked(writeSpaceFile).mockReset();
  rs.mocked(readSpaceText).mockResolvedValue({
    text: "<script>safe as text</script>",
    sha256: "b".repeat(64),
    concurrency: "exclusive-host-window",
  });
});

it("READ without EXPORT or WRITE can preview literal text", async () => {
  state.enabled = true;
  state.readOnly = true;
  state.exportable = false;
  show();
  expect(screen.queryByRole("link", { name: "Download page.html" })).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Preview page.html" }));
  await waitFor(() =>
    expect(
      screen.getByLabelText<HTMLTextAreaElement>("File contents").value,
    ).toContain("safe as text"),
  );
  expect(
    screen.getByLabelText<HTMLTextAreaElement>("File contents").readOnly,
  ).toBe(true);
  expect(screen.queryByRole("button", { name: "Save" })).toBeNull();
});

it("navigates legitimate directory links", () => {
  state.enabled = true;
  state.files = [
    {
      name: "alias",
      path: "alias",
      kind: "symlink",
      size: 5,
      modified: 0,
      accessible: true,
      target_kind: "directory",
    },
  ];
  show();
  fireEvent.click(screen.getByRole("button", { name: "alias/" }));
  expect(state.navigate).toHaveBeenCalledWith(
    `/workspace/spaces?space=${"a".repeat(32)}&path=alias`,
  );
});

it("late creation cannot navigate after leaving the page within the same account", async () => {
  state.enabled = true;
  let finish!: (value: Awaited<ReturnType<typeof createSpace>>) => void;
  let signal: AbortSignal | undefined;
  rs.mocked(createSpace).mockImplementationOnce(
    (_name, _custody, requestSignal) => {
      signal = requestSignal;
      return new Promise((resolve) => {
        finish = resolve;
      });
    },
  );
  function Account({ visible }: { visible: boolean }) {
    return (
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          {visible ? <SpacesPage /> : <span>Another page</span>}
        </FileActionLifetimeProvider>
      </I18nProvider>
    );
  }
  const view = render(<Account visible />);
  fireEvent.click(screen.getByRole("button", { name: "New space" }));
  fireEvent.change(screen.getByLabelText("Space name"), {
    target: { value: "New" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Create" }));
  await waitFor(() => expect(createSpace).toHaveBeenCalledTimes(1));
  view.rerender(<Account visible={false} />);
  await act(async () => {
    finish({
      id: "b".repeat(32),
      backing_handle: "c".repeat(32),
      name: "New",
      custody: {
        kind: "personal",
        principal: { kind: "human", subject_id: "alice" },
      },
      generation: 1,
      status: "active",
      mode: "native",
      permissions: 27,
    });
  });
  expect(state.navigate).not.toHaveBeenCalled();
  expect(signal?.aborted).toBe(true);
});
afterEach(cleanup);

it("known resource failures hide cached file mutation controls", () => {
  state.enabled = true;
  state.spaceError = new Error("Access retired");
  show();
  expect(screen.queryByRole("button", { name: "Edit page.html" })).toBeNull();
  expect(screen.queryByLabelText("Upload file")).toBeNull();
  expect(
    screen.getByText(
      "Unable to load this space. Reload to check current access.",
    ),
  ).toBeTruthy();
});

it("shows unsupported capability without offering storage writes", () => {
  show();
  expect(screen.getByText("Spaces are unavailable")).toBeTruthy();
  expect(screen.queryByText("Upload file")).toBeNull();
});

it("read-only members can download without being offered an editor or deletion", () => {
  state.enabled = true;
  state.readOnly = true;
  show();
  expect(screen.getByRole("link", { name: "Download page.html" })).toBeTruthy();
  expect(screen.queryByRole("button", { name: "Edit page.html" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Delete page.html" })).toBeNull();
});

it("renders active text safely and preserves the draft on stale save", async () => {
  state.enabled = true;
  rs.mocked(writeSpaceFile).mockRejectedValueOnce(
    new Error("File changed; reload"),
  );
  const { container } = show();
  fireEvent.click(screen.getByRole("button", { name: "Edit page.html" }));
  await waitFor(() =>
    expect(
      screen.getByLabelText<HTMLTextAreaElement>("File contents").value,
    ).toContain("safe as text"),
  );
  expect(container.querySelector("script")).toBeNull();
  fireEvent.change(screen.getByLabelText("File contents"), {
    target: { value: "my draft" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText("File changed; reload");
  expect(
    screen.getByLabelText<HTMLTextAreaElement>("File contents").value,
  ).toBe("my draft");
  expect(writeSpaceFile).toHaveBeenCalledTimes(1);
});
