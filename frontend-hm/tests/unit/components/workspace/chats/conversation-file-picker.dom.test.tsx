import { afterEach, expect, rs, test } from "@rstest/core";
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from "@testing-library/react";

import { ConversationFilePicker } from "@/components/workspace/chats/conversation-file-picker";
import { enUS } from "@/core/i18n/locales/en-US";
import type {
  ImportDestination,
  PresentedFile,
} from "@/core/threads/conversation-file-import";

rs.mock("@/core/i18n/hooks", () => ({
  useI18n: () => ({ t: enUS, locale: "en-US" }),
}));
const digest = "b".repeat(64);
const home = { id: "a".repeat(32), name: "Library" };
const note: PresentedFile = {
  filename: "notes.txt",
  run_id: "answer",
  path: "/mnt/user-data/outputs/notes.txt",
  size: 11,
  modified: 1740000000,
  available: true,
};
const saved = {
  filename: "notes_1.txt",
  size: 11,
  virtual_path: "/mnt/user-data/uploads/notes_1.txt",
  sha256: digest,
};
const receipt = {
  filename: saved.filename,
  path: saved.virtual_path,
  size: 11,
  status: "uploaded",
};
const rows = [
  {
    thread_id: "source",
    values: { title: "Planning" },
    owner_user_id: "reader",
    agent_instance: null,
  },
  {
    thread_id: "agent-chat",
    values: { title: "Indexing" },
    owner_user_id: "reader",
    agent_instance: home,
  },
  {
    thread_id: "destination",
    values: { title: "Current chat" },
    owner_user_id: "reader",
  },
  {
    thread_id: "foreign",
    values: { title: "Other chat" },
    owner_user_id: "another",
  },
  {
    thread_id: "side",
    values: { title: "Sidebar" },
    owner_user_id: "reader",
    metadata: { deerflow_sidecar: true },
  },
];
function page(files: PresentedFile[]) {
  return {
    files,
    has_more: false,
    next_before_created_at: null,
    next_before_run_id: null,
  };
}
function mount(
  destination: ImportDestination,
  handler: (url: string, init: RequestInit) => Promise<Response> | Response,
) {
  const calls: { url: string; init: RequestInit }[] = [];
  rs.spyOn(globalThis, "fetch").mockImplementation(async (input, init = {}) => {
    const url =
      typeof input === "string"
        ? input
        : input instanceof URL
          ? input.href
          : input.url;
    calls.push({ url, init });
    if (url.endsWith("/threads/search")) return Response.json(rows);
    return handler(url, init);
  });
  const onSaved = rs.fn(),
    close = rs.fn();
  const view = render(
    <ConversationFilePicker
      destination={destination}
      account="reader"
      onSaved={onSaved}
      close={close}
    />,
  );
  return { calls, onSaved, close, view };
}
afterEach(() => {
  cleanup();
  rs.restoreAllMocks();
});
test("own chats page past empty deliveries; missing files stay disabled and a saved receipt has no source path", async () => {
  const { calls, onSaved } = mount(
    { threadId: "destination", newThread: true },
    (url) => {
      if (url.includes("presented-files"))
        return Response.json(
          url.includes("before_run_id")
            ? page([
                note,
                {
                  ...note,
                  run_id: "past",
                  filename: "old.txt",
                  available: false,
                  size: null,
                  modified: null,
                },
              ])
            : {
                ...page([]),
                has_more: true,
                next_before_created_at: "2026-01-02T00:00:00Z",
                next_before_run_id: "last",
              },
        );
      return Response.json(saved);
    },
  );
  fireEvent.click(await screen.findByRole("button", { name: "Planning" }));
  fireEvent.click(await screen.findByRole("button", { name: "Load more" }));
  expect(
    (
      await screen.findByRole("button", {
        name: /old.txt/,
      })
    ).hasAttribute("disabled"),
  ).toBe(true);
  fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
  await waitFor(() =>
    expect(onSaved).toHaveBeenCalledWith(receipt, "Planning"),
  );
  const write = calls.filter((x) => x.url.includes("/from-conversation"));
  expect(write).toHaveLength(1);
  expect(write[0]?.url).toContain(
    "/threads/destination/uploads/from-conversation",
  );
  expect(JSON.parse(write[0]?.init.body as string)).toEqual({
    source_thread_id: "source",
    source_run_id: "answer",
    path: note.path,
    create_destination: true,
  });
});
test("a Home lists personal sources, discloses both audiences, and cancellation writes nothing", async () => {
  const { calls, onSaved, close } = mount(
    { threadId: "destination", instance: home },
    () => Response.json(page([note])),
  );
  await screen.findByRole("button", { name: "Planning" });
  expect(
    screen.queryByRole("button", {
      name: /Indexing|Current chat|Other chat|Sidebar/,
    }),
  ).toBeNull();
  fireEvent.click(screen.getByRole("button", { name: "Planning" }));
  fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
  expect(screen.getByRole("dialog").textContent).toContain(
    "Anyone who can chat with Library can ask for it",
  );
  expect(screen.getByRole("dialog").textContent).toContain(
    "anyone who can see all of Library’s work can open it",
  );
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(close).toHaveBeenCalledOnce();
  expect(onSaved).not.toHaveBeenCalled();
  expect(calls.some((x) => x.url.includes("from-conversation"))).toBe(false);
});
test("a dropped Home write reconciles its original ID and picked filename, returning the stored numbered copy", async () => {
  let checks = 0;
  const { calls, onSaved } = mount(
    { threadId: "destination", instance: home },
    (url) => {
      if (url.includes("presented-files")) return Response.json(page([note]));
      if (url.includes("from-conversation"))
        throw new TypeError("Connection interrupted");
      if (url.includes("/files/operations/")) {
        checks++;
        return Response.json(
          checks === 1
            ? { status: "pending" }
            : {
                status: "complete",
                file: {
                  path: "uploads/notes_1.txt",
                  size: 11,
                  sha256: digest,
                  sandbox_path: saved.virtual_path,
                  deduplicated: false,
                },
              },
        );
      }
      throw new Error("Unexpected request");
    },
  );
  fireEvent.click(await screen.findByRole("button", { name: "Planning" }));
  fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
  fireEvent.click(screen.getByRole("button", { name: "Give the file" }));
  fireEvent.click(await screen.findByRole("button", { name: "Check again" }));
  await waitFor(() => expect(onSaved).toHaveBeenCalledWith(receipt, undefined));
  const writes = calls.filter((x) => x.url.includes("from-conversation"));
  expect(writes).toHaveLength(1);
  const write = writes[0]!;
  const id = new URL(write.url, "https://test.invalid").searchParams.get(
    "operation_id",
  );
  expect(write.url).toContain(
    `/agent-instances/${home.id}/conversations/destination/files/from-conversation`,
  );
  expect(id).toMatch(/^[a-f0-9]{32}$/);
  expect(JSON.parse(write.init.body as string)).toEqual({
    source_thread_id: "source",
    source_run_id: "answer",
    path: note.path,
    acknowledge_disclosure: true,
  });
  expect(
    calls.filter((x) =>
      x.url.includes(`/files/operations/${id}?filename=notes.txt`),
    ),
  ).toHaveLength(2);
});
test("an instance source unavailable to the viewer shows the access requirement without a filename", async () => {
  mount({ threadId: "destination" }, () =>
    Response.json({ detail: "Not found" }, { status: 404 }),
  );
  fireEvent.click(await screen.findByRole("button", { name: /Indexing/ }));
  await screen.findByText(
    /Opening Library’s files requires “Can see all work”/,
  );
  expect(screen.queryByText("notes.txt")).toBeNull();
});

test("an uncertain ordinary copy shows the upload check without repeating the write", async () => {
  const { calls, onSaved } = mount({ threadId: "destination" }, (url) => {
    if (url.includes("presented-files")) return Response.json(page([note]));
    throw new TypeError("Connection interrupted");
  });
  fireEvent.click(await screen.findByRole("button", { name: "Planning" }));
  fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
  await screen.findByText(/Check this conversation’s uploads/);
  expect(screen.queryByRole("button", { name: /retry|try again/i })).toBeNull();
  expect(calls.filter((x) => x.url.includes("from-conversation"))).toHaveLength(
    1,
  );
  expect(onSaved).not.toHaveBeenCalled();
});
test("a retired picker ignores a late copy receipt and aborts its request", async () => {
  let complete!: (r: Response) => void;
  const { calls, onSaved, view } = mount({ threadId: "destination" }, (url) =>
    url.includes("presented-files")
      ? Response.json(page([note]))
      : new Promise<Response>((resolve) => {
          complete = resolve;
        }),
  );
  fireEvent.click(await screen.findByRole("button", { name: "Planning" }));
  fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
  await waitFor(() =>
    expect(calls.some((x) => x.url.includes("from-conversation"))).toBe(true),
  );
  const write = calls.find((x) => x.url.includes("from-conversation"))!;
  view.unmount();
  expect(write.init.signal?.aborted).toBe(true);
  complete(Response.json(saved));
  await new Promise<void>((r) => setTimeout(r, 0));
  expect(onSaved).not.toHaveBeenCalled();
});

test.each(["post", "lookup"])(
  "a terminal unconfirmed %s has no retry or repeated check",
  async (via) => {
    const { calls } = mount(
      { threadId: "destination", instance: home },
      (url) => {
        if (url.includes("presented-files")) return Response.json(page([note]));
        if (url.includes("from-conversation")) {
          if (via === "post")
            return Response.json(
              { reason: "file-write-failed", file_outcome: "unconfirmed" },
              { status: 503 },
            );
          throw new TypeError("Connection interrupted");
        }
        return Response.json({ status: "unconfirmed" });
      },
    );
    fireEvent.click(await screen.findByRole("button", { name: "Planning" }));
    fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
    fireEvent.click(screen.getByRole("button", { name: "Give the file" }));
    await screen.findByText(/Ask Library whether it has the file/);
    expect(
      screen.queryByRole("button", {
        name: /retry|check again|check copy status/i,
      }),
    ).toBeNull();
    expect(
      calls.filter((x) => x.url.includes("from-conversation")),
    ).toHaveLength(1);
  },
);
test("only a confirmed clean failure permits a fresh Home operation ID", async () => {
  const { calls, onSaved } = mount(
    { threadId: "destination", instance: home },
    (url) => {
      if (url.includes("presented-files")) return Response.json(page([note]));
      if (calls.filter((x) => x.url.includes("from-conversation")).length === 1)
        return Response.json(
          { reason: "file-write-failed", file_outcome: "nothing-changed" },
          { status: 503 },
        );
      return Response.json(saved);
    },
  );
  fireEvent.click(await screen.findByRole("button", { name: "Planning" }));
  fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
  fireEvent.click(screen.getByRole("button", { name: "Give the file" }));
  fireEvent.click(await screen.findByRole("button", { name: "Try again" }));
  await waitFor(() => expect(onSaved).toHaveBeenCalledWith(receipt, undefined));
  const writes = calls.filter((x) => x.url.includes("from-conversation"));
  expect(writes).toHaveLength(2);
  expect(writes[0]?.url).not.toBe(writes[1]?.url);
});

test("closing a pending Home copy keeps its scoped receipt available without another write", async () => {
  let checks = 0;
  const { calls, onSaved, close } = mount(
    { threadId: "destination", instance: home },
    (url) => {
      if (url.includes("presented-files")) return Response.json(page([note]));
      if (url.includes("from-conversation"))
        throw new TypeError("Connection interrupted");
      checks++;
      return Response.json(
        checks === 1
          ? { status: "pending" }
          : {
              status: "complete",
              file: {
                path: "uploads/notes_1.txt",
                size: 11,
                sha256: digest,
                sandbox_path: saved.virtual_path,
                deduplicated: false,
              },
            },
      );
    },
  );
  fireEvent.click(await screen.findByRole("button", { name: "Planning" }));
  fireEvent.click(await screen.findByRole("button", { name: /notes.txt/ }));
  fireEvent.click(screen.getByRole("button", { name: "Give the file" }));
  await screen.findByRole("button", { name: "Check again" });
  fireEvent.click(screen.getAllByRole("button", { name: "Close" }).at(-1)!);
  expect(close).not.toHaveBeenCalled();
  expect(screen.queryByRole("dialog")).toBeNull();
  fireEvent.click(
    screen.getByRole("button", { name: /Check copy status.*notes.txt/ }),
  );
  fireEvent.click(await screen.findByRole("button", { name: "Check again" }));
  await waitFor(() => expect(onSaved).toHaveBeenCalledWith(receipt, undefined));
  expect(calls.filter((x) => x.url.includes("from-conversation"))).toHaveLength(
    1,
  );
});
