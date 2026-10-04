import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { type QueryClient, useQueryClient } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";
import { StrictMode, useState } from "react";

const router = rs.hoisted(() => ({ push: rs.fn() }));
const mode = rs.hoisted(() => ({ static: false }));
rs.mock("next/navigation", () => ({
  useRouter: () => router,
  usePathname: () => "/workspace/files",
}));
rs.mock("@/core/static-mode", () => ({
  isStaticWebsiteOnly: () => mode.static,
}));
rs.mock("@/core/files/api", () => ({
  listMyFiles: rs.fn(),
  deleteMyFile: rs.fn(),
  keepInMyFiles: rs.fn(),
}));

import { QueryClientProvider } from "@/components/query-client-provider";
import { AuthProvider, useAuth, type User } from "@/core/auth/AuthProvider";
import { listMyFiles, type MyFilesListResponse } from "@/core/files/api";
import { MY_FILES_QUERY_KEY, useMyFiles } from "@/core/files/hooks";

const alice: User = {
  id: "alice",
  email: "alice@example.com",
  system_role: "user",
  needs_setup: false,
};
const bob: User = { ...alice, id: "bob", email: "bob@example.com" };
let auth: ReturnType<typeof useAuth>;
let client: QueryClient;
const clients = new Set<QueryClient>();
let displayed: string[];

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

function listing(name: string): MyFilesListResponse {
  return {
    files: [
      {
        name,
        path: name,
        size: 123,
        modified: 0,
        virtual_path: name,
        url: name,
      },
    ],
    count: 1,
    truncated: false,
  };
}

function Files() {
  auth = useAuth();
  client = useQueryClient();
  clients.add(client);
  const [initialOwner] = useState(auth.user?.id);
  const [draft, setDraft] = useState("");
  const files = useMyFiles();
  const names =
    files.data?.files.map((file) => file.name).join(",") ?? "loading";
  displayed.push(names);
  return (
    <div data-testid="files" data-owner={initialOwner}>
      {names}
      <input
        aria-label="Unsaved draft"
        value={draft}
        onChange={(event) => setDraft(event.target.value)}
      />
    </div>
  );
}

function App({ user = alice }: { user?: User | null }) {
  return (
    <AuthProvider initialUser={user}>
      <QueryClientProvider>
        <Files />
      </QueryClientProvider>
    </AuthProvider>
  );
}

beforeEach(() => {
  displayed = [];
  mode.static = false;
  router.push.mockReset();
  rs.mocked(listMyFiles).mockReset();
});

afterEach(() => {
  cleanup();
  clients.forEach((queryClient) => queryClient.clear());
  clients.clear();
  rs.useRealTimers();
  rs.restoreAllMocks();
});

describe("authenticated query cache isolation", () => {
  it("bounds an unresponsive check without discarding the draft", async () => {
    rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    rs.mocked(listMyFiles).mockResolvedValue(listing("account.pdf"));
    let signal: AbortSignal | undefined;
    rs.spyOn(globalThis, "fetch").mockImplementation(
      (_url, options) =>
        new Promise((_resolve, reject) => {
          signal = options?.signal ?? undefined;
          signal?.addEventListener("abort", () =>
            reject(new DOMException("owned fixture timeout", "AbortError")),
          );
        }),
    );
    const view = render(<App />);
    fireEvent.change(screen.getByLabelText("Unsaved draft"), {
      target: { value: "unfinished edits" },
    });
    let refresh: Promise<void>;
    act(() => {
      refresh = auth.refreshUser();
    });
    await act(async () => {
      await rs.advanceTimersByTimeAsync(10_000);
      await refresh;
    });
    expect(signal?.aborted).toBe(true);
    expect(auth.user?.id).toBe("alice");
    expect(auth.isLoading).toBe(false);
    expect(screen.getByLabelText<HTMLInputElement>("Unsaved draft").value).toBe(
      "unfinished edits",
    );
    view.unmount();
  });

  it("can recover on visibility after an uncertain check", async () => {
    rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    rs.mocked(listMyFiles).mockResolvedValue(listing("account.pdf"));
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockRejectedValueOnce(new Error("owned fixture offline"))
      .mockResolvedValue(Response.json(alice));
    rs.spyOn(document, "visibilityState", "get").mockReturnValue("visible");
    render(<App />);
    const previousClient = client;
    await act(async () => auth.refreshUser());
    await act(async () => {
      document.dispatchEvent(new Event("visibilitychange"));
    });
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(client).toBe(previousClient);
    await act(async () => rs.advanceTimersByTimeAsync(60_000));
    expect(fetch).toHaveBeenCalledTimes(2);
  });
  it.each(["network", "unavailable", "invalid-json", "invalid-user"])(
    "keeps the draft and cache after an uncertain %s refresh",
    async (failure) => {
      rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
      rs.mocked(listMyFiles).mockResolvedValue(listing("alice-private.pdf"));
      const fetch = rs.spyOn(globalThis, "fetch");
      if (failure === "network")
        fetch.mockRejectedValue(new Error("owned fixture offline"));
      else if (failure === "unavailable")
        fetch.mockResolvedValue(new Response(null, { status: 503 }));
      else if (failure === "invalid-json")
        fetch.mockResolvedValue(new Response("invalid JSON"));
      else fetch.mockResolvedValue(Response.json({ id: "unverified" }));
      render(<App />);
      const previousClient = client;
      fireEvent.change(screen.getByLabelText("Unsaved draft"), {
        target: { value: "unfinished edits" },
      });

      await act(async () => auth.refreshUser());

      expect(auth.user?.id).toBe("alice");
      expect(auth.isLoading).toBe(false);
      expect(client).toBe(previousClient);
      expect(
        screen.getByLabelText<HTMLInputElement>("Unsaved draft").value,
      ).toBe("unfinished edits");
    },
  );

  it("retries a bounded number of times and a later manual refresh can recover", async () => {
    rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    rs.mocked(listMyFiles).mockResolvedValue(listing("alice-private.pdf"));
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockRejectedValue(new Error("owned fixture offline"));
    render(<App />);
    await act(async () => auth.refreshUser());
    for (const delay of [2_000, 5_000, 15_000]) {
      await act(async () => rs.advanceTimersByTimeAsync(delay));
    }
    expect(fetch).toHaveBeenCalledTimes(4);
    await act(async () => rs.advanceTimersByTimeAsync(60_000));
    expect(fetch).toHaveBeenCalledTimes(4);
    fetch.mockResolvedValue(Response.json(alice));
    await act(async () => auth.refreshUser());
    expect(auth.user?.id).toBe("alice");
    expect(fetch).toHaveBeenCalledTimes(5);
  });

  it("recovers on a retry without replacing unsaved state", async () => {
    rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
    rs.mocked(listMyFiles).mockResolvedValue(listing("alice-private.pdf"));
    const fetch = rs
      .spyOn(globalThis, "fetch")
      .mockRejectedValueOnce(new Error("owned fixture offline"))
      .mockResolvedValue(Response.json(alice));
    render(<App />);
    const previousClient = client;
    fireEvent.change(screen.getByLabelText("Unsaved draft"), {
      target: { value: "unfinished edits" },
    });
    await act(async () => auth.refreshUser());
    await act(async () => rs.advanceTimersByTimeAsync(2_000));
    expect(fetch).toHaveBeenCalledTimes(2);
    expect(client).toBe(previousClient);
    expect(screen.getByLabelText<HTMLInputElement>("Unsaved draft").value).toBe(
      "unfinished edits",
    );
    await act(async () => rs.advanceTimersByTimeAsync(60_000));
    expect(fetch).toHaveBeenCalledTimes(2);
  });

  it.each(["unauthorized", "permissions", "account"])(
    "retires drafts and queries on a confirmed %s change",
    async (change) => {
      rs.mocked(listMyFiles).mockResolvedValue(listing("account.pdf"));
      const response =
        change === "unauthorized"
          ? new Response(null, { status: 401 })
          : Response.json(
              change === "account"
                ? bob
                : { ...alice, permissions: ["files:read"] },
            );
      rs.spyOn(globalThis, "fetch").mockResolvedValue(response);
      render(<App />);
      const previousClient = client;
      fireEvent.change(screen.getByLabelText("Unsaved draft"), {
        target: { value: "unfinished edits" },
      });
      await act(async () => auth.refreshUser());
      expect(client).not.toBe(previousClient);
      expect(
        screen.getByLabelText<HTMLInputElement>("Unsaved draft").value,
      ).toBe("");
      expect(previousClient.getQueryCache().getAll()).toHaveLength(0);
      if (change === "unauthorized")
        expect(router.push).toHaveBeenCalledWith(
          "/login?next=%2Fworkspace%2Ffiles",
        );
    },
  );

  it.each(["account", "logout", "unmount"])(
    "cancels queued recovery on %s retirement",
    async (retirement) => {
      rs.useFakeTimers({ toFake: ["setTimeout", "clearTimeout", "Date"] });
      rs.mocked(listMyFiles).mockResolvedValue(listing("account.pdf"));
      const fetch = rs
        .spyOn(globalThis, "fetch")
        .mockRejectedValueOnce(new Error("owned fixture offline"))
        .mockResolvedValue(new Response(null, { status: 204 }));
      const view = render(<App />);
      await act(async () => auth.refreshUser());
      if (retirement === "account") act(() => auth.applyUser(bob));
      else if (retirement === "logout") await act(async () => auth.logout());
      else view.unmount();
      await act(async () => rs.advanceTimersByTimeAsync(60_000));
      expect(fetch).toHaveBeenCalledTimes(retirement === "logout" ? 2 : 1);
      if (retirement === "account") expect(auth.user?.id).toBe("bob");
    },
  );
  it("starts a fresh cache when the workspace remounts after SPA sign-in", async () => {
    rs.mocked(listMyFiles).mockResolvedValueOnce(listing("alice-private.pdf"));
    const first = render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("alice-private.pdf"),
    );
    const previousClient = client;
    first.unmount();

    const response = deferred<MyFilesListResponse>();
    rs.mocked(listMyFiles).mockReturnValue(response.promise);
    displayed = [];
    render(<App user={bob} />);

    expect(displayed).not.toContain("alice-private.pdf");
    expect(client).not.toBe(previousClient);
    expect(previousClient.getQueryData(MY_FILES_QUERY_KEY)).toBeUndefined();
    await act(async () => response.resolve(listing("bob.pdf")));
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("bob.pdf"),
    );
  });

  it("replaces every query cache and local child state when the identity changes", async () => {
    rs.mocked(listMyFiles).mockResolvedValueOnce(listing("alice-private.pdf"));
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("alice-private.pdf"),
    );
    client.setQueryData(["private-preferences"], { owner: "alice" });
    const previousClient = client;
    const response = deferred<MyFilesListResponse>();
    rs.mocked(listMyFiles).mockReturnValue(response.promise);
    displayed = [];

    act(() => auth.applyUser(bob));

    expect(displayed).not.toContain("alice-private.pdf");
    expect(screen.getByTestId("files").getAttribute("data-owner")).toBe("bob");
    expect(client).not.toBe(previousClient);
    expect(client.getQueryData(["private-preferences"])).toBeUndefined();
    expect(previousClient.getQueryCache().getAll()).toHaveLength(0);
    await act(async () => response.resolve(listing("bob.pdf")));
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("bob.pdf"),
    );
  });

  it("ignores a previous account's request even when its transport cannot abort", async () => {
    const previous = deferred<MyFilesListResponse>();
    const next = deferred<MyFilesListResponse>();
    rs.mocked(listMyFiles)
      .mockReturnValueOnce(previous.promise)
      .mockReturnValue(next.promise);
    render(<App />);
    act(() => auth.applyUser(bob));
    displayed = [];

    await act(async () => previous.resolve(listing("alice-late.pdf")));
    expect(displayed).not.toContain("alice-late.pdf");
    expect(screen.getByTestId("files").textContent).toBe("loading");

    await act(async () => next.resolve(listing("bob.pdf")));
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("bob.pdf"),
    );
  });

  it("does not restore a previous identity from an older auth refresh", async () => {
    rs.mocked(listMyFiles).mockResolvedValue(listing("account.pdf"));
    const response = deferred<Response>();
    rs.spyOn(globalThis, "fetch").mockReturnValue(response.promise);
    render(<App />);
    let refresh: Promise<void>;
    act(() => {
      refresh = auth.refreshUser();
    });
    act(() => auth.applyUser(bob));

    await act(async () => {
      response.resolve(Response.json(alice));
      await refresh;
    });

    expect(auth.user?.id).toBe("bob");
  });

  it("discards the workspace immediately on logout while the cookie request is pending", async () => {
    rs.mocked(listMyFiles).mockResolvedValue(listing("alice-private.pdf"));
    const response = deferred<Response>();
    rs.spyOn(globalThis, "fetch").mockReturnValue(response.promise);
    render(<App />);
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("alice-private.pdf"),
    );
    const previousClient = client;
    let logout: Promise<void>;
    act(() => {
      logout = auth.logout();
    });

    expect(screen.queryByTestId("files")).toBeNull();
    expect(previousClient.getQueryCache().getAll()).toHaveLength(0);
    await act(async () => {
      response.resolve(new Response(null, { status: 204 }));
      await logout;
    });
    expect(router.push).toHaveBeenCalledWith("/login");
  });

  it("keeps the cache during a successful refresh of the same identity", async () => {
    rs.mocked(listMyFiles).mockResolvedValue(listing("alice-private.pdf"));
    rs.spyOn(globalThis, "fetch").mockResolvedValue(Response.json(alice));
    render(
      <StrictMode>
        <App />
      </StrictMode>,
    );
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("alice-private.pdf"),
    );
    const previousClient = client;

    await act(async () => auth.refreshUser());

    expect(client).toBe(previousClient);
    expect(screen.getByTestId("files").textContent).toBe("alice-private.pdf");
  });

  it("supports initially unknown identity recovery and static demo rendering", async () => {
    rs.mocked(listMyFiles).mockResolvedValue(listing("fixture.pdf"));
    mode.static = true;
    const fetch = rs.spyOn(globalThis, "fetch");
    render(<App user={null} />);
    await act(async () => auth.refreshUser());
    expect(fetch).not.toHaveBeenCalled();
    act(() => auth.applyUser(bob));
    await waitFor(() =>
      expect(screen.getByTestId("files").textContent).toBe("fixture.pdf"),
    );
    expect(screen.getByTestId("files").getAttribute("data-owner")).toBe("bob");
  });

  it("keeps static demo content mounted when logout returns to its workspace", async () => {
    rs.mocked(listMyFiles).mockResolvedValue(listing("fixture.pdf"));
    mode.static = true;
    const fetch = rs.spyOn(globalThis, "fetch");
    render(<App />);
    await act(async () => auth.logout());
    expect(fetch).not.toHaveBeenCalled();
    expect(router.push).toHaveBeenCalledWith("/");
    expect(screen.getByTestId("files")).not.toBeNull();
    expect(auth.user).toEqual(alice);
  });
});
