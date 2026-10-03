import { afterEach, beforeEach, describe, expect, it, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

const api = rs.hoisted(() => ({
  threads: { search: rs.fn(), delete: rs.fn() },
}));
const localDelete = rs.hoisted(() => rs.fn());
const navigation = rs.hoisted(() => ({
  replace: rs.fn(),
  path: "/workspace/chats/parent",
  params: { thread_id: "parent", agent_name: undefined as string | undefined },
}));
const reset = rs.hoisted(() => rs.fn());
rs.mock("@/core/api", () => ({
  getAPIClient: () => api,
  cancelActiveThreadRun: rs.fn(),
}));
rs.mock("@/core/api/fetcher", () => ({ fetch: localDelete }));
rs.mock("@/core/config", () => ({ getBackendBaseURL: () => "" }));
rs.mock("next/navigation", () => ({
  useRouter: () => navigation,
  usePathname: () => navigation.path,
  useParams: () => navigation.params,
  useSearchParams: () => new URLSearchParams(),
}));

import {
  THREAD_CHAT_RESET_EVENT,
  useThreadChat,
} from "@/components/workspace/chats/use-thread-chat";
import {
  ThreadDeleteDialogProvider,
  useThreadDeleteDialog,
} from "@/components/workspace/thread-delete-dialog";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nContext } from "@/core/i18n/context";
import { enUS } from "@/core/i18n/locales/en-US";
import type { AgentThread } from "@/core/threads/types";

const clients = new Set<QueryClient>();
const thread = {
  thread_id: "parent",
  metadata: {},
  values: { title: "Selected conversation" },
} as AgentThread;
function Target({ selected = thread }: { selected?: AgentThread }) {
  const request = useThreadDeleteDialog();
  return (
    <button
      onClick={() => request({ thread: selected, recentThreadId: "parent" })}
    >
      Request deletion
    </button>
  );
}
function mount() {
  const client = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  clients.add(client);
  function Draft() {
    const chat = useThreadChat();
    return <output data-testid="draft-thread-id">{chat.threadId}</output>;
  }
  function App({
    show = true,
    draft = false,
  }: {
    show?: boolean;
    draft?: boolean;
  }) {
    return (
      <QueryClientProvider client={client}>
        <FileActionLifetimeProvider>
          <I18nContext.Provider
            value={{ locale: "en-US", setLocale: () => undefined, t: enUS }}
          >
            <ThreadDeleteDialogProvider>
              {show && <Target />}
              {draft && <Draft />}
            </ThreadDeleteDialogProvider>
          </I18nContext.Provider>
        </FileActionLifetimeProvider>
      </QueryClientProvider>
    );
  }
  return { App, view: render(<App />) };
}
function route(path: string, threadId: string, agent?: string) {
  navigation.path = path;
  navigation.params = { thread_id: threadId, agent_name: agent };
  window.history.replaceState(null, "", path);
}
function gate() {
  let resolve!: () => void;
  let reject!: (error: Error) => void;
  const promise = new Promise<void>((yes, no) => {
    resolve = yes;
    reject = no;
  });
  return { promise, resolve, reject };
}
async function open() {
  fireEvent.click(screen.getByRole("button", { name: "Request deletion" }));
  await screen.findByRole("alertdialog", { name: "Delete conversation?" });
}
function confirm() {
  fireEvent.click(screen.getByTestId("thread-delete-confirm-button"));
}

beforeEach(() => {
  api.threads.search.mockReset().mockResolvedValue([]);
  api.threads.delete.mockReset().mockResolvedValue(undefined);
  localDelete
    .mockReset()
    .mockResolvedValue(new Response(null, { status: 204 }));
  navigation.replace.mockReset();
  reset.mockReset();
  window.addEventListener(THREAD_CHAT_RESET_EVENT, reset);
  route("/workspace/chats/parent", "parent");
});
afterEach(() => {
  cleanup();
  window.removeEventListener(THREAD_CHAT_RESET_EVENT, reset);
  clients.forEach((client) => client.clear());
  clients.clear();
});

describe("conversation deletion confirmation", () => {
  it("preserves a newer logical draft on the same new-chat route", async () => {
    route("/workspace/chats/new", "new");
    const pending = gate();
    api.threads.delete.mockReturnValueOnce(pending.promise);
    const { App, view } = mount();
    view.rerender(<App draft />);
    const draftId = screen.getByTestId("draft-thread-id").textContent;
    await open();
    confirm();
    await waitFor(() => expect(api.threads.delete).toHaveBeenCalledTimes(1));
    await act(async () => {
      pending.resolve();
    });
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    expect(screen.getByTestId("draft-thread-id").textContent).toBe(draftId);
  });
  it("opens with Cancel focused and makes no request on open or cancel", async () => {
    mount();
    await open();
    expect(screen.getByText(/Selected conversation/)).toBeTruthy();
    expect(document.activeElement).toBe(
      screen.getByRole("button", { name: "Cancel" }),
    );
    expect(api.threads.search).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    expect(api.threads.delete).not.toHaveBeenCalled();
    expect(localDelete).not.toHaveBeenCalled();
  });

  it("submits once and blocks dismissal while pending", async () => {
    const pending = gate();
    api.threads.delete.mockReturnValueOnce(pending.promise);
    mount();
    await open();
    act(() => {
      confirm();
      confirm();
    });
    await waitFor(() => expect(api.threads.delete).toHaveBeenCalledTimes(1));
    expect(
      screen.getByRole("button", { name: "Cancel" }).hasAttribute("disabled"),
    ).toBe(true);
    expect(
      screen
        .getByTestId("thread-delete-confirm-button")
        .hasAttribute("disabled"),
    ).toBe(true);
    fireEvent.keyDown(document, { key: "Escape" });
    fireEvent.pointerDown(document.body);
    expect(screen.getByRole("alertdialog")).toBeTruthy();
    await act(async () => {
      pending.resolve();
    });
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    expect(navigation.replace).toHaveBeenCalledWith("/workspace/chats/new");
  });

  it("keeps the selected title and retry after the list disappears and cleanup fails", async () => {
    localDelete.mockResolvedValueOnce(
      new Response(JSON.stringify({ detail: "Cleanup failed" }), {
        status: 500,
      }),
    );
    const { App, view } = mount();
    await open();
    view.rerender(<App show={false} />);
    confirm();
    await screen.findByRole("alert");
    expect(screen.getByText("Cleanup failed")).toBeTruthy();
    expect(screen.getByText(/Selected conversation/)).toBeTruthy();
    api.threads.delete.mockRejectedValueOnce(
      Object.assign(new Error("Gone"), { status: 404 }),
    );
    confirm();
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    expect(localDelete).toHaveBeenCalledTimes(2);
  });

  it.each([
    ["/workspace/chats/parent", "parent", undefined, "/workspace/chats/new"],
    ["/workspace/chats/new", "new", undefined, "/workspace/chats/new"],
    [
      "/workspace/agents/Agent%20One/chats/parent",
      "parent",
      "Agent One",
      "/workspace/agents/Agent%20One/chats/new",
    ],
    ["/workspace/chats/other", "other", undefined, undefined],
  ])("preserves redirect behavior at %s", async (path, id, agent, expected) => {
    route(path, id, agent);
    mount();
    await open();
    confirm();
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    if (expected) expect(navigation.replace).toHaveBeenCalledWith(expected);
    else expect(navigation.replace).not.toHaveBeenCalled();
  });

  it("does not reset a newer route when the remote delete resolves late", async () => {
    const pending = gate();
    api.threads.delete.mockReturnValueOnce(pending.promise);
    const { App, view } = mount();
    await open();
    confirm();
    await waitFor(() => expect(api.threads.delete).toHaveBeenCalledTimes(1));
    route("/workspace/chats/other", "other");
    view.rerender(<App />);
    await act(async () => {
      pending.resolve();
    });
    await waitFor(() => expect(screen.queryByRole("alertdialog")).toBeNull());
    expect(navigation.replace).not.toHaveBeenCalled();
    expect(reset).not.toHaveBeenCalled();
  });

  it("ignores completion after the account subtree unmounts", async () => {
    const pending = gate();
    api.threads.delete.mockReturnValueOnce(pending.promise);
    const { view } = mount();
    await open();
    confirm();
    await waitFor(() => expect(api.threads.delete).toHaveBeenCalledTimes(1));
    view.unmount();
    await act(async () => {
      pending.resolve();
    });
    expect(localDelete).not.toHaveBeenCalled();
    expect(navigation.replace).not.toHaveBeenCalled();
    expect(reset).not.toHaveBeenCalled();
  });
});
