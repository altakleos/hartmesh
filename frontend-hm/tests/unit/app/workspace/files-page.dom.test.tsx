import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  within,
} from "@testing-library/react";
import { type ComponentProps, useSyncExternalStore } from "react";

const pluginState = rs.hoisted(() => ({
  data: [] as LoadedContribution[],
  isPending: false,
  isError: false,
}));
rs.mock("@/core/extensions/hooks", () => ({
  useFrontendExtensions: () => pluginState,
}));
// These existing file scenarios exercise the host before conversation publications.
rs.mock("@/core/shared-conversations/hooks", () => ({
  useSharing: () => ({ scope: "legacy", can: () => false }),
  useSnapshotList: () => ({ data: undefined, isError: false }),
}));
const lifetime = rs.hoisted(() => ({
  active: true,
  signal: new AbortController().signal,
  toasts: new Set<string | number>(),
}));
rs.mock("@/core/file-areas/file-action-lifetime", () => ({
  useFileActionLifetime: () => lifetime,
}));

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
rs.mock("next/link", () => ({
  default: ({ href, onClick, ...props }: ComponentProps<"a">) => (
    <a
      {...props}
      href={href}
      onClick={(event) => {
        event.preventDefault();
        onClick?.(event);
        routerReplace(href);
      }}
    />
  ),
}));
rs.mock("next/navigation", () => ({
  usePathname: () => "/workspace/files",
  useRouter: () => ({
    push: routerReplace,
    replace: routerReplace,
    prefetch: rs.fn(),
  }),
  useSearchParams: () =>
    new URLSearchParams(
      useSyncExternalStore(
        (onChange) => {
          window.addEventListener("popstate", onChange);
          return () => window.removeEventListener("popstate", onChange);
        },
        () => window.location.search,
      ),
    ),
}));
rs.mock("sonner", () => ({ toast: { success: rs.fn(), error: rs.fn() } }));
// The tab title asks the deployment whose workspace this is; not what this is about.
const storage = rs.hoisted(() => ({
  known: true,
  enabled: false,
  state: "off",
  reason: null as string | null,
  own: "ready",
  userSpaces: false,
  isLoading: false,
  dataUpdatedAt: 0,
}));
const viewer = rs.hoisted(() => ({
  user: {
    id: "default",
    system_role: "user" as "user" | "admin",
    permissions: ["threads:read", "threads:write"],
  },
}));
rs.mock("@/core/auth/AuthProvider", () => ({ useAuth: () => viewer }));
const discovery = rs.hoisted(() => ({ folders: true, legacy: false }));
rs.mock("@/core/features", () => ({
  useFeatures: () => ({
    data: discovery.legacy
      ? undefined
      : {
          personal: discovery.folders ? ["select", "folder"] : ["select"],
          shared: discovery.folders ? ["select", "folder"] : ["select"],
        },
    isPending: false,
    isError: false,
  }),
  useAgentInstances: () => ({ known: true, enabled: false }),
  useDocumentTitle: () => undefined,
  useStorage: () => storage,
  useBranding: () => ({
    providerName: "Acme",
    supportURL: "https://support.example.com/help",
  }),
}));
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
          publication_id?: string | null;
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
const personalRead = rs.hoisted(() => rs.fn());
const sharedRead = rs.hoisted(() => rs.fn());
// Only the hooks are stood in for. Where a file is addressed and which
// folder it belongs in are the module's own rules and stay real, so this
// page's tests read the folder the product would share into. (The backend
// base URL is empty in a test, so the real address is the one asserted.)
rs.mock("@/core/shared/hooks", () => ({
  useSharedFiles: (path?: string) => {
    sharedRead(path);
    return {
      data: shared.data,
      error: shared.error,
      isPending: shared.isPending,
      refetch: shared.refetch,
    };
  },
  useRemoveSharedFile: () => ({
    mutate: shared.removeMutate,
    isPending: shared.removePending,
  }),
  useShareWithEveryone: () => ({
    share: shareWithEveryone,
    available: true,
    isPending: false,
    openShared: rs.fn(),
  }),
}));
rs.mock("@/core/files/hooks", () => ({
  useMyFiles: (path?: string) => {
    personalRead(path);
    return {
      data: files.data,
      error: files.error,
      isPending: files.isPending,
      refetch: files.refetch,
    };
  },
  useDeleteMyFile: () => ({
    mutate: files.deleteMutate,
    isPending: files.deletePending,
  }),
}));

import FilesPage from "@/app/workspace/files/page";
import type { LoadedContribution } from "@/core/extensions/registry";
import { readStorageRefusal } from "@/core/file-areas/storage-state";
import { MyFilesRequestError } from "@/core/files/api";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import { SharedRequestError } from "@/core/shared/api";

import { legacyReportContribution } from "../../../helpers/legacy-report";

const clients: QueryClient[] = [];
function renderPage() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  clients.push(client);
  return render(<FilesPage />, {
    wrapper: ({ children }) => (
      <QueryClientProvider client={client}>
        <I18nContext.Provider
          value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
        >
          {children}
        </I18nContext.Provider>
      </QueryClientProvider>
    ),
  });
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
  publication_id: "publication-owned-1",
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

function openLocation(name: string) {
  const link = screen.queryByRole("link", { name });
  if (link) fireEvent.click(link);
  else {
    fireEvent.click(screen.getByRole("button", { name: "Locations" }));
    fireEvent.click(
      within(screen.getByRole("dialog", { name: "Locations" })).getByRole(
        "link",
        { name },
      ),
    );
  }
}

function openSharedTab() {
  openLocation("Company shared");
}

function enterFolder(name: string) {
  fireEvent.click(
    within(screen.getByRole("list")).getByRole("button", { name }),
  );
}

function details(name: string) {
  fireEvent.click(
    screen.getByRole("button", { name: `More actions for ${name}` }),
  );
  return within(screen.getByRole("dialog", { name }));
}

function search(value: string) {
  fireEvent.change(screen.getByRole("searchbox"), { target: { value } });
}

describe("FilesPage", () => {
  beforeEach(() => {
    Object.assign(storage, {
      known: true,
      enabled: false,
      state: "off",
      reason: null,
      own: "ready",
      userSpaces: false,
      isLoading: false,
      dataUpdatedAt: 0,
    });
    discovery.legacy = false;
    discovery.folders = false;
    personalRead.mockClear();
    sharedRead.mockClear();
    shareWithEveryone.mockClear();
    viewer.user.permissions = ["threads:read", "threads:write"];
    viewer.user.id = "default";
    viewer.user.system_role = "user";
    pluginState.data = [legacyReportContribution()];
    pluginState.isPending = false;
    pluginState.isError = false;
    routerReplace.mockImplementation((url: string) => {
      window.history.replaceState(null, "", url);
      window.dispatchEvent(new PopStateEvent("popstate"));
    });
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

  afterEach(() => {
    cleanup();
    clients.splice(0).forEach((client) => client.clear());
    rs.restoreAllMocks();
  });

  it.each(["not-assigned", "no-seat", "unverified", "fault"])(
    "keeps Shared usable when personal files are %s",
    (own) => {
      Object.assign(storage, { enabled: true, state: "incomplete", own });
      renderPage();
      const text =
        own === "not-assigned"
          ? enUS.chat.filesPreparing("an administrator")
          : own === "no-seat"
            ? enUS.chat.filesNoSeat(false, "Acme")
            : own === "fault"
              ? enUS.chat.filesFault("an administrator")
              : enUS.chat.myFilesUnchecked;
      expect(screen.getByText(text)).toBeTruthy();
      expect(screen.queryByRole("list", { name: "My files" })).toBeNull();
      openSharedTab();
      expect(screen.getByRole("list", { name: "Company shared" })).toBeTruthy();
    },
  );

  it("replaces both lists for deployment storage failure without hiding the tabs", () => {
    Object.assign(storage, {
      enabled: true,
      state: "unavailable",
      reason: "storage-pool-missing",
    });
    renderPage();
    expect(screen.getByText(enUS.chat.filesUnavailable)).toBeTruthy();
    expect(screen.queryByRole("list", { name: "My files" })).toBeNull();
    openSharedTab();
    expect(screen.getByText(enUS.chat.filesUnavailable)).toBeTruthy();
    expect(screen.queryByRole("list", { name: "Company shared" })).toBeNull();
    expect(screen.getByRole("button", { name: "Locations" })).toBeTruthy();
    expect(screen.queryByRole("link", { name: /support/ })).toBeNull();
  });

  it("gives an administrator the provider and safe reference in place of raw list error text", () => {
    viewer.user.system_role = "admin";
    const refusal = readStorageRefusal({
      status: 503,
      code: "storage-unavailable",
      reason: "storage-pool-missing",
      message: "raw pool geometry",
    });
    files.error = new MyFilesRequestError(503, "raw pool geometry", refusal);
    renderPage();
    expect(
      screen.getByText(enUS.agentInstances.unavailable.storageMissing("Acme")),
    ).toBeTruthy();
    expect(
      screen.getByText(
        enUS.agentInstances.unavailable.reference("storage-missing"),
      ),
    ).toBeTruthy();
    expect(screen.queryByText("raw pool geometry")).toBeNull();
  });

  it("retains a deployment-wide list refusal across tabs until fresh discovery", () => {
    files.error = new MyFilesRequestError(
      503,
      "raw",
      readStorageRefusal({
        status: 503,
        code: "storage-unavailable",
        reason: "disk-too-small",
      }),
    );
    renderPage();
    expect(screen.getByText(enUS.chat.filesUnavailable)).toBeTruthy();
    openSharedTab();
    expect(screen.queryByRole("list", { name: "Company shared" })).toBeNull();
    expect(screen.getByText(enUS.chat.filesUnavailable)).toBeTruthy();
  });

  it("clears a retained deployment refusal after fresh discovery", () => {
    files.error = new MyFilesRequestError(
      503,
      "raw",
      readStorageRefusal({
        status: 503,
        code: "storage-unavailable",
        reason: "disk-too-small",
      }),
    );
    const view = renderPage();
    openSharedTab();
    expect(screen.queryByRole("list", { name: "Company shared" })).toBeNull();
    storage.dataUpdatedAt = 1;
    view.rerender(<FilesPage />);
    expect(screen.getByRole("list", { name: "Company shared" })).toBeTruthy();
  });

  it("clears a deployment refusal when the account changes without inheriting its cached files", () => {
    files.error = new MyFilesRequestError(
      503,
      "raw",
      readStorageRefusal({
        status: 503,
        code: "storage-unavailable",
        reason: "disk-too-small",
      }),
    );
    const view = renderPage();
    openSharedTab();
    expect(screen.queryByRole("list", { name: "Company shared" })).toBeNull();
    viewer.user.id = "another-account";
    shared.data = { files: [], count: 0, truncated: false };
    view.rerender(<FilesPage />);
    expect(screen.getByText(enUS.shared.emptyHint)).toBeTruthy();
    expect(screen.queryByText(enUS.chat.filesUnavailable)).toBeNull();
  });

  it("retries a retained preparing list after newer readiness and bounds retries when discovery disagrees", () => {
    Object.assign(storage, {
      enabled: true,
      state: "incomplete",
      own: "not-assigned",
    });
    files.error = new MyFilesRequestError(
      503,
      "preparing",
      readStorageRefusal({
        status: 503,
        code: "storage-unavailable",
        reason: "space-not-assigned",
      }),
    );
    const view = renderPage();
    files.refetch.mockClear();
    Object.assign(storage, { state: "ready", own: "ready", dataUpdatedAt: 1 });
    view.rerender(<FilesPage />);
    expect(files.refetch).toHaveBeenCalledTimes(1);
    for (let revision = 2; revision < 8; revision++) {
      storage.dataUpdatedAt = revision;
      files.error = new MyFilesRequestError(
        503,
        "still refused",
        readStorageRefusal({
          status: 503,
          code: "storage-unavailable",
          reason: "disk-too-small",
        }),
      );
      view.rerender(<FilesPage />);
    }
    expect(files.refetch).toHaveBeenCalledTimes(1);
    fireEvent.click(screen.getByRole("button", { name: enUS.files.retry }));
    expect(files.refetch).toHaveBeenCalledTimes(2);
    files.error = null;
    storage.dataUpdatedAt = 8;
    view.rerender(<FilesPage />);
    expect(screen.getByRole("list", { name: "My files" })).toBeTruthy();
  });

  it("schedules five-second busy retries and clears the actual timer on tab retirement", () => {
    const scheduled = rs.spyOn(globalThis, "setInterval");
    const cleared = rs.spyOn(globalThis, "clearInterval");
    files.error = new MyFilesRequestError(
      409,
      "raw",
      readStorageRefusal({ status: 409, code: "storage-busy" }),
    );
    renderPage();
    const index = scheduled.mock.calls.findIndex((call) => call[1] === 5_000);
    expect(index).toBeGreaterThanOrEqual(0);
    const callback = scheduled.mock.calls[index]![0] as () => void;
    act(() => callback());
    expect(files.refetch).toHaveBeenCalledTimes(1);
    openSharedTab();
    expect(cleared).toHaveBeenCalledWith(scheduled.mock.results[index]!.value);
  });

  it("keeps Shared unverified and busy errors local to their tab", () => {
    const refusal = readStorageRefusal({
      status: 503,
      code: "storage-unavailable",
      reason: "space-unverified",
    });
    shared.error = new SharedRequestError(503, "raw", refusal);
    renderPage();
    expect(screen.getByRole("list", { name: "My files" })).toBeTruthy();
    openSharedTab();
    expect(screen.getByText(enUS.shared.unchecked)).toBeTruthy();
    expect(screen.queryByRole("list", { name: "Company shared" })).toBeNull();
  });

  it("shows busy and Retry in place of the affected personal list", () => {
    files.error = new MyFilesRequestError(
      409,
      "raw",
      readStorageRefusal({ status: 409, code: "storage-busy" }),
    );
    renderPage();
    expect(screen.getByText(enUS.files.busy)).toBeTruthy();
    fireEvent.click(screen.getByRole("button", { name: enUS.files.retry }));
    expect(files.refetch).toHaveBeenCalledTimes(1);
    openSharedTab();
    expect(screen.getByRole("list", { name: "Company shared" })).toBeTruthy();
  });

  it.each(["", "?view=classic", "?view=browser"])(
    "opens the same personal browser for a saved URL %s on an older Gateway",
    (query) => {
      discovery.legacy = true;
      window.history.replaceState(null, "", "/workspace/files" + query);
      renderPage();
      expect(screen.getByRole("list", { name: "My files" })).toBeTruthy();
      expect(screen.queryByRole("tab")).toBeNull();
      expect(
        screen.queryByRole("link", {
          name: /Use classic file view|Use file browser/i,
        }),
      ).toBeNull();
      expect(personalRead).toHaveBeenCalledWith(undefined);
      enterFolder("Reports");
      expect(
        screen.getByRole("link", { name: "august.pdf" }).getAttribute("href"),
      ).toBe("/api/files/Reports/august.pdf");
    },
  );

  it.each(["", "?tab=shared&view=classic"])(
    "does not list either location without thread read access at %s",
    (query) => {
      viewer.user.permissions = ["threads:write"];
      window.history.replaceState(null, "", "/workspace/files" + query);
      renderPage();
      expect(screen.getByRole("alert")).toBeTruthy();
      expect(screen.queryByRole("list")).toBeNull();
      expect(personalRead).not.toHaveBeenCalled();
      expect(sharedRead).not.toHaveBeenCalled();
    },
  );

  it("uses the advertised direct folder read on normal entry and the bounded collection for a search", () => {
    discovery.folders = true;
    files.data = { files: [NOTES], count: 1, truncated: false };
    renderPage();
    expect(personalRead).toHaveBeenCalledWith("");
    expect(personalRead).not.toHaveBeenCalledWith(undefined);
    search("notes");
    expect(personalRead).toHaveBeenCalledWith(undefined);
    expect(screen.getByRole("link", { name: "notes.txt" })).toBeTruthy();
    openSharedTab();
    expect(sharedRead).toHaveBeenCalledWith("");
  });

  it("follows the saved location and reacts to history changes without remounting", () => {
    renderPage();
    for (const [query, name] of [
      ["?tab=shared", "Company shared"],
      ["", "My files"],
      ["?tab=shared&view=classic", "Company shared"],
      ["?tab=unknown", "My files"],
    ] as const) {
      act(() => {
        window.history.replaceState(null, "", "/workspace/files" + query);
        window.dispatchEvent(new PopStateEvent("popstate"));
      });
      expect(screen.getByRole("list", { name })).toBeTruthy();
    }
    openSharedTab();
    expect(window.location.search).toBe("?tab=shared");
    openLocation("My files");
    expect(window.location.pathname + window.location.search).toBe(
      "/workspace/files",
    );
  });

  it("shows the original publication and the publisher in the selected file’s actions", () => {
    window.history.replaceState(
      null,
      "",
      "/workspace/files?tab=shared&folder=Reports",
    );
    renderPage();
    expect(
      screen.getByRole("link", { name: "august.pdf" }).getAttribute("href"),
    ).toBe("/api/shared/Reports/august.pdf");
    const item = details("august.pdf");
    expect(item.getByText("Reports/august.pdf")).toBeTruthy();
    expect(item.getByText(/owner-1/)).toBeTruthy();
    expect(
      item.getByRole("link", { name: "Download" }).getAttribute("href"),
    ).toBe("/api/shared/Reports/august.pdf?download=true");
    // A published file still needs an authorized physical selection to be removable.
    expect(item.queryByRole("button", { name: /Remove/ })).toBeNull();
    expect(shared.removeMutate).not.toHaveBeenCalled();
  });

  it("identifies an unrecorded Shared file and withholds removal", () => {
    window.history.replaceState(
      null,
      "",
      "/workspace/files?tab=shared&folder=Exports&view=classic",
    );
    discovery.legacy = true;
    renderPage();
    const item = details("jobs.xlsx");
    expect(item.getByText(/No record of who shared this/)).toBeTruthy();
    expect(item.queryByRole("button", { name: /Remove/ })).toBeNull();
  });

  it.each(["pending", "discovery failure", "module failure"])(
    "withholds publishing when filing is %s",
    (failure) => {
      if (failure === "pending") pluginState.isPending = true;
      else if (failure === "discovery failure") pluginState.isError = true;
      else
        pluginState.data = [
          {
            ...legacyReportContribution(),
            extension: undefined,
            error: "Unavailable",
          },
        ];
      renderPage();
      enterFolder("Reports");
      const item = details("august.pdf");
      expect(
        item.queryByRole("button", { name: "Share with everyone" }),
      ).toBeNull();
      expect(shareWithEveryone).not.toHaveBeenCalled();
    },
  );

  it.each([true, false])(
    "publishes a selected file to the installed collection when enabled=%s",
    (installed) => {
      if (!installed) pluginState.data = [];
      renderPage();
      enterFolder("Reports");
      const item = details("august.pdf");
      expect(
        item.getByText(/Everyone at the company can open it/),
      ).toBeTruthy();
      fireEvent.click(
        item.getByRole("button", { name: "Share with everyone" }),
      );
      expect(shareWithEveryone).toHaveBeenCalledWith(
        [AUGUST.virtual_path],
        installed ? "Reports" : undefined,
      );
    },
  );

  it("shares a file without disclosing the name of the person's private folder", () => {
    const path = "Acme-acquisition/terms.pdf";
    files.data = {
      files: [
        {
          ...AUGUST,
          name: "terms.pdf",
          path,
          url: "/api/files/" + path,
          virtual_path: "/mnt/user-data/files/" + path,
        },
      ],
      count: 1,
      truncated: false,
    };
    renderPage();
    enterFolder("Acme-acquisition");
    fireEvent.click(
      details("terms.pdf").getByRole("button", { name: "Share with everyone" }),
    );
    expect(shareWithEveryone).toHaveBeenCalledWith(
      ["/mnt/user-data/files/" + path],
      undefined,
    );
  });

  it("keeps sharing live across a round trip to Shared", () => {
    renderPage();
    for (let attempt = 0; attempt < 2; attempt++) {
      enterFolder("Reports");
      const item = details("august.pdf");
      fireEvent.click(
        item.getByRole("button", { name: "Share with everyone" }),
      );
      fireEvent.click(item.getByRole("button", { name: "Close" }));
      openSharedTab();
      openLocation("My files");
    }
    expect(shareWithEveryone).toHaveBeenCalledTimes(2);
    expect(shareWithEveryone).toHaveBeenNthCalledWith(
      2,
      [AUGUST.virtual_path],
      "Reports",
    );
  });

  it("allows readers to download and withholds file mutations", () => {
    viewer.user.permissions = ["threads:read"];
    renderPage();
    enterFolder("Reports");
    expect(
      screen.getByRole("link", { name: "august.pdf" }).getAttribute("href"),
    ).toBe("/api/files/Reports/august.pdf");
    const item = details("august.pdf");
    expect(item.getByText("Reports/august.pdf")).toBeTruthy();
    expect(item.getByText("47.1 KiB")).toBeTruthy();
    expect(
      item.getByRole("link", { name: "Download" }).getAttribute("href"),
    ).toBe("/api/files/Reports/august.pdf?download=true");
    expect(
      item.queryByRole("button", { name: /Share with everyone|Delete/ }),
    ).toBeNull();
    expect(files.deleteMutate).not.toHaveBeenCalled();
    expect(shareWithEveryone).not.toHaveBeenCalled();
  });

  it.each(["personal", "shared"] as const)(
    "shows the empty and failed states for %s",
    (area) => {
      const location = area === "shared" ? shared : files;
      location.data = { files: [], count: 0, truncated: false };
      if (area === "shared")
        window.history.replaceState(null, "", "/workspace/files?tab=shared");
      const view = renderPage();
      const words = area === "shared" ? enUS.shared : enUS.files;
      expect(screen.getByText(words.emptyHint)).toBeTruthy();
      location.data = undefined;
      location.error = new Error("private diagnostic");
      view.rerender(<FilesPage />);
      expect(screen.getByRole("alert").textContent).toContain(words.loadFailed);
      expect(screen.queryByText("private diagnostic")).toBeNull();
      fireEvent.click(screen.getByRole("button", { name: enUS.files.retry }));
      expect(location.refetch).toHaveBeenCalled();
    },
  );

  it("searches the bounded file paths, reports no results, and restores the folder on clearing", () => {
    files.data = { files: [AUGUST, NOTES], count: 2, truncated: true };
    renderPage();
    expect(
      screen.getByText("Only the first 2 entries are available here."),
    ).toBeTruthy();
    search("REPORTS/");
    expect(screen.getByRole("link", { name: "august.pdf" })).toBeTruthy();
    expect(screen.queryByRole("link", { name: "notes.txt" })).toBeNull();
    search("absent");
    expect(screen.getByText(/No matches among the loaded files/)).toBeTruthy();
    expect(screen.queryByText(enUS.files.emptyHint)).toBeNull();
    expect(
      screen.getByText("Only the first 2 entries are available here."),
    ).toBeTruthy();
    fireEvent.click(
      screen.getByRole("button", { name: enUS.files.clearFilter }),
    );
    expect(screen.getByRole("button", { name: "Reports" })).toBeTruthy();
    expect(screen.getByRole("link", { name: "notes.txt" })).toBeTruthy();
  });

  it("sorts duplicate names by newest without losing the original paths", () => {
    files.data = {
      files: [
        { ...AUGUST, path: "A/report.pdf", name: "report.pdf", modified: 1 },
        { ...AUGUST, path: "B/report.pdf", name: "report.pdf", modified: 3 },
      ],
      count: 2,
      truncated: false,
    };
    renderPage();
    search("report.pdf");
    const links = () =>
      screen
        .getAllByRole("link", { name: "report.pdf" })
        .map((el) => el.getAttribute("href"));
    expect(links()).toEqual([
      "/api/files/A/report.pdf",
      "/api/files/B/report.pdf",
    ]);
    fireEvent.change(
      screen.getByRole("combobox", { name: enUS.files.sortLabel }),
      { target: { value: "newest" } },
    );
    expect(links()).toEqual([
      "/api/files/B/report.pdf",
      "/api/files/A/report.pdf",
    ]);
  });

  it("matches canonically equivalent Unicode without carrying a private filter into Shared", () => {
    files.data = {
      files: [{ ...NOTES, path: "Résumé.txt", name: "Résumé.txt" }],
      count: 1,
      truncated: false,
    };
    renderPage();
    search("RE\u0301SUME\u0301");
    expect(screen.getByRole("link", { name: "Résumé.txt" })).toBeTruthy();
    openSharedTab();
    expect(screen.getByRole<HTMLInputElement>("searchbox").value).toBe("");
    search("Exports");
    expect(screen.getByRole("link", { name: "jobs.xlsx" })).toBeTruthy();
    expect(screen.queryByRole("link", { name: "august.pdf" })).toBeNull();
  });
});
