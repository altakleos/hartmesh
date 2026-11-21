import { afterEach, beforeEach, expect, test, rs } from "@rstest/core";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import {
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
  within,
} from "@testing-library/react";

const state = rs.hoisted(() => ({
  changed: rs.fn(),
  feature: {
    known: true,
    enabled: true,
    execution: "ready",
    reason: null as string | null,
  },
}));
rs.mock("@/core/api/fetcher", () => ({ fetch: rs.fn() }));
rs.mock("@/core/auth/AuthProvider", () => ({
  useAuth: () => ({
    user: {
      id: "reader",
      system_role: "admin",
      permissions: ["agents:read", "agents:write"],
    },
  }),
}));
rs.mock("@/core/agent-instances/home-transfer-access", () => ({
  checkHomeTransferAccess: rs.fn(async () => true),
}));
rs.mock("@/core/features", () => ({
  useAgentInstances: () => state.feature,
  useStorage: () => null,
  useBranding: () => ({ providerName: "Provider", supportURL: null }),
}));
rs.mock("@/components/workspace/person-name", () => ({
  useSupervisorName: () => "a manager",
}));
rs.mock("@/core/spaces/api", () => {
  const actual = rs.requireActual<typeof spaces>("@/core/spaces/api");
  return {
    ...actual,
    getSpace: rs.fn(),
    listSpaceFiles: rs.fn(),
    readSpaceText: rs.fn(),
  };
});
rs.mock("@/core/agent-instances/agent-files", () => ({
  putAgentFile: rs.fn(),
  changeAgentFile: rs.fn(),
}));
import { FilesTab } from "@/components/workspace/instances/files-tab";
import * as transfers from "@/core/agent-instances/agent-files";
import {
  InstanceApiError,
  type AgentInstanceDetail,
} from "@/core/agent-instances/api";
import { HomeFileTransferProvider } from "@/core/agent-instances/home-transfers";
import { fetch as fetcher } from "@/core/api/fetcher";
import { FileActionLifetimeProvider } from "@/core/file-areas/file-action-lifetime";
import { I18nProvider } from "@/core/i18n/context";
import * as spaces from "@/core/spaces/api";
const clients: QueryClient[] = [];
const id = "a".repeat(32),
  home = "b".repeat(32);
const file = {
  name: "note.txt",
  path: "note.txt",
  kind: "file",
  size: 5,
  modified: 1720000000,
  accessible: true,
} as spaces.SpaceFile;
const instance: AgentInstanceDetail = {
  id,
  name: "Researcher",
  custody: "personal",
  owner_id: "reader",
  creator_id: "reader",
  principal: { kind: "nonhuman", subject_id: "agent" },
  supervisor: { kind: "human", subject_id: "reader" },
  definition_revision: "d".repeat(64),
  home_id: home,
  home_generation: 1,
  status: "active",
  permissions: 7,
  generation: 1,
  activity: {
    running_runs: 0,
    running: [],
    last_active_at: null,
    open_conversations: 0,
  },
  home: { state: "available", workspace: "absent" },
  execution: { execution: "ready", reason: null },
};
function show(change: Partial<AgentInstanceDetail> = {}, initialFolder = "") {
  const client = new QueryClient();
  clients.push(client);
  const ui = (current: Partial<AgentInstanceDetail>) => (
    <QueryClientProvider client={client}>
      <I18nProvider initialLocale="en-US">
        <FileActionLifetimeProvider>
          <HomeFileTransferProvider>
            <FilesTab
              instance={{ ...instance, ...current }}
              initialFolder={initialFolder}
              changed={state.changed}
            />
          </HomeFileTransferProvider>
        </FileActionLifetimeProvider>
      </I18nProvider>
    </QueryClientProvider>
  );
  const view = render(ui(change));
  return {
    ...view,
    update: (current: Partial<AgentInstanceDetail>) =>
      view.rerender(ui(current)),
  };
}
async function choose(fileToSend = new File(["hello"], "note.txt")) {
  const input = await screen.findByLabelText("Choose files");
  fireEvent.change(input, { target: { files: [fileToSend] } });
}
beforeEach(() => {
  rs.mocked(fetcher).mockReset();
  state.changed.mockReset();
  state.feature = {
    known: true,
    enabled: true,
    execution: "ready",
    reason: null,
  };
  rs.mocked(spaces.getSpace)
    .mockReset()
    .mockResolvedValue({
      id: home,
      generation: 1,
      status: "active",
      permissions: 19,
      quota: { max_bytes: 999999999, max_inodes: 5000 },
    } as spaces.StorageSpace);
  rs.mocked(spaces.listSpaceFiles)
    .mockReset()
    .mockResolvedValue({ files: [], truncated: false });
  rs.mocked(spaces.readSpaceText).mockReset().mockResolvedValue({
    text: "hello",
    sha256: "old-revision",
    concurrency: "sha256",
  });
  rs.mocked(transfers.putAgentFile).mockReset().mockResolvedValue({
    path: "note.txt",
    size: 5,
    sha256: "hash",
    deduplicated: false,
  });
  rs.mocked(transfers.changeAgentFile)
    .mockReset()
    .mockResolvedValue({ complete: true, path: "new" });
});
afterEach(() => {
  cleanup();
  clients.splice(0).forEach((client) => client.clear());
  rs.useRealTimers();
});

const recoveryOp = "c".repeat(32);
const acceptLabel = "Accept the files as they are";
const pendingHome = {
  id: home,
  generation: 3,
  permissions: 31,
  mode: "native",
  status: "active",
  storage_state: "recovery-pending",
} as spaces.StorageSpace;
function unfinished(action = "put", age = 360_000) {
  return {
    operation_id: recoveryOp,
    generation: 3,
    phase: "pending",
    created_at: new Date(Date.now() - age).toISOString(),
    request:
      action === "copy"
        ? { action, destination_path: "uploads/example.txt" }
        : {
            action,
            path: "uploads/example.txt",
            destination: "outputs/moved.txt",
          },
    result: null,
  };
}
test.each([
  ["put", /saving uploads\/example.txt/i],
  ["write", /saving uploads\/example.txt/i],
  ["rename", /renaming uploads\/example.txt to outputs\/moved.txt/i],
  ["mkdir", /making the folder uploads\/example.txt/i],
  ["remove", /deleting uploads\/example.txt/i],
  ["copy", /copying a file to uploads\/example.txt/i],
] as const)(
  "manager can inspect an aged %s change on a readable Home",
  async (action, description) => {
    rs.mocked(spaces.getSpace).mockResolvedValue(pendingHome);
    rs.mocked(fetcher).mockResolvedValue(
      Response.json({ operations: [unfinished(action)], can_fence: true }),
    );
    show({ home_generation: 3 });
    await screen.findByText(description);
    expect(screen.getByRole("button", { name: acceptLabel })).toBeDefined();
  },
);

test("a changed recovery listing retires the confirmation for the old path", async () => {
  const row = unfinished();
  rs.mocked(spaces.getSpace).mockResolvedValue(pendingHome);
  rs.mocked(fetcher)
    .mockResolvedValueOnce(Response.json({ operations: [row] }))
    .mockResolvedValueOnce(
      Response.json({
        operations: [
          { ...row, request: { ...row.request, path: "outputs/new.txt" } },
        ],
      }),
    );
  show({ home_generation: 3 });
  fireEvent.click(await screen.findByRole("button", { name: acceptLabel }));
  fireEvent.click(within(screen.getByRole("dialog")).getByRole("checkbox"));
  await act(async () => {
    await clients
      .at(-1)!
      .invalidateQueries({ queryKey: ["home-file-recovery"] });
  });
  await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
  fireEvent.click(screen.getByRole("button", { name: acceptLabel }));
  expect(
    within(screen.getByRole("dialog"))
      .getByRole("button", { name: acceptLabel })
      .hasAttribute("disabled"),
  ).toBe(true);
});

test("a manager without current Home administration cannot read recovery rows", async () => {
  rs.mocked(spaces.getSpace).mockResolvedValue({
    ...pendingHome,
    permissions: 19,
  });
  show({ home_generation: 3 });
  await screen.findByRole("navigation");
  expect(fetcher).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: acceptLabel })).toBeNull();
});

test("filters fresh rows, stale generations and lifecycle operations", async () => {
  rs.mocked(spaces.getSpace).mockResolvedValue(pendingHome);
  rs.mocked(fetcher).mockResolvedValue(
    Response.json({
      operations: [
        unfinished("put", 30_000),
        { ...unfinished(), generation: 2, operation_id: "d".repeat(32) },
        { ...unfinished("restore"), operation_id: "e".repeat(32) },
        { ...unfinished("backup"), operation_id: "f".repeat(32) },
      ],
    }),
  );
  show({ home_generation: 3 });
  expect(
    (await screen.findAllByText(/backup or a restore.*Ask Provider/i)).length,
  ).toBeGreaterThan(0);
  expect(screen.queryByRole("button", { name: acceptLabel })).toBeNull();
});

test("an inspector receives only supervisor guidance after a second pending Home reading a minute later", async () => {
  rs.useFakeTimers({ toFake: ["Date", "setInterval", "clearInterval"] });
  rs.mocked(spaces.getSpace).mockResolvedValue(pendingHome);
  show({ home_generation: 3, permissions: 2 });
  await screen.findByRole("navigation");
  expect(screen.queryByText(/waiting for a change to be settled/i)).toBeNull();
  await act(async () => {
    await rs.advanceTimersByTimeAsync(61_000);
  });
  await screen.findByText(/waiting for a change to be settled.*ask a manager/i);
  expect(rs.mocked(spaces.getSpace).mock.calls.length).toBeGreaterThan(1);
  expect(fetcher).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: acceptLabel })).toBeNull();
});

test.each([
  ["accepted", /current files were accepted/i, true],
  ["access concealed", null, false],
  ["manager revoked", null, false],
  ["host-unconfirmed", /service.*could not confirm what happened/i, false],
  ["lost", /current files were accepted/i, true],
  ["closing", /current files were accepted/i, false],
  ["another pending change", /current files were accepted/i, false],
  ["unavailable execution", /current files were accepted/i, false],
  ["failed refresh", /current files were accepted/i, false],
  ["stale instance", /current files were accepted/i, false],
  ["unverified home", /current files were accepted/i, false],
  ["wrong operation", /no result is recorded/i, false],
  ["changed path", /no result is recorded/i, false],
  ["changed creation time", /no result is recorded/i, false],
  ["wrong generation", /no result is recorded/i, false],
  ["host-settled", /service settled/i, false],
  ["pending", /change is still pending/i, false],
  ["absent", /no result is recorded/i, false],
] as const)(
  "checks the exact result and fresh availability after %s",
  async (condition, message, ready) => {
    const row = unfinished();
    const posts: unknown[] = [];
    const generations: (string | null)[] = [];
    const reads: string[] = [];
    rs.mocked(spaces.getSpace).mockImplementation(async () => {
      if (!posts.length) return pendingHome;
      reads.push("home");
      if (condition === "failed refresh") throw Error("offline");
      return {
        ...pendingHome,
        storage_state:
          condition === "another pending change"
            ? "recovery-pending"
            : "available",
      };
    });
    rs.mocked(fetcher).mockImplementation(async (url, init) => {
      const route = new URL(
        url instanceof Request ? url.url : url,
        "http://localhost",
      );
      if (route.pathname.endsWith("/recovery") && init?.method === "POST") {
        posts.push(
          typeof init.body === "string" ? JSON.parse(init.body) : null,
        );
        if (condition === "lost") throw new TypeError("connection closed");
        return Response.json(
          {},
          {
            status: ["host-settled", "pending", "absent"].includes(condition)
              ? 409
              : 200,
          },
        );
      }
      if (route.pathname.endsWith("/recovery"))
        return Response.json({
          operations: posts.length ? [] : [row],
          can_fence: true,
        });
      if (route.pathname.endsWith("/recovery/" + recoveryOp)) {
        generations.push(route.searchParams.get("generation"));
        reads.push("operation");
        if (condition === "absent") return Response.json({}, { status: 404 });
        if (condition === "pending") return Response.json(row);
        return Response.json({
          ...row,
          operation_id:
            condition === "wrong operation" ? "d".repeat(32) : row.operation_id,
          generation: condition === "wrong generation" ? 2 : row.generation,
          request:
            condition === "changed path"
              ? { ...row.request, path: "outputs/other.txt" }
              : row.request,
          created_at:
            condition === "changed creation time"
              ? new Date().toISOString()
              : row.created_at,
          phase: condition === "host-settled" ? "complete" : "failed",
          result:
            condition === "host-unconfirmed"
              ? { resolution: "host-unconfirmed" }
              : condition === "host-settled"
                ? { resolution: "host-settled" }
                : {
                    resolution: "owner-accepted-current-state",
                    actor_kind: "human",
                    actor_id: "reader",
                  },
        });
      }
      if (route.pathname.endsWith("/agent-instances/" + id)) {
        reads.push("execution");
        if (condition === "access concealed")
          return Response.json({}, { status: 404 });
        return Response.json({
          ...instance,
          permissions:
            condition === "manager revoked" ? 2 : instance.permissions,
          home_generation: condition === "stale instance" ? 2 : 3,
          home: {
            state: condition === "unverified home" ? "unverified" : "available",
            workspace: condition === "closing" ? "starting" : "absent",
          },
          execution: {
            execution:
              condition === "unavailable execution" ? "unavailable" : "ready",
            reason: null,
          },
        });
      }
      throw new Error("Unexpected route " + route.pathname);
    });
    const view = show({ home_generation: 3 });
    fireEvent.click(await screen.findByRole("button", { name: acceptLabel }));
    const dialog = await screen.findByRole("dialog");
    const confirm = within(dialog).getByRole("button", {
      name: acceptLabel,
    });
    expect(confirm.hasAttribute("disabled")).toBe(true);
    expect(dialog.textContent).toMatch(/numbered|number in its name/i);
    expect(dialog.textContent).toContain(
      "A reply already in progress may still arrive.",
    );
    expect(dialog.textContent).toContain(
      "For an assignment, check its status before continuing.",
    );
    fireEvent.click(within(dialog).getByRole("checkbox"));
    fireEvent.click(confirm);
    if (message) await screen.findByText(message);
    else {
      await waitFor(() => expect(reads).toContain("execution"));
      await waitFor(() =>
        expect(screen.queryByText(/saving uploads\/example.txt/i)).toBeNull(),
      );
      expect(screen.queryByText(/current files were accepted/i)).toBeNull();
    }
    expect(posts).toEqual([
      {
        generation: 3,
        operation_id: recoveryOp,
        file_change_only: true,
        acknowledge_uncertain_outcome: true,
      },
    ]);
    expect(generations).toEqual(["3"]);
    expect(new Set(reads)).toEqual(new Set(["home", "execution", "operation"]));
    expect(!!screen.queryByText(/Researcher can work again/i)).toBe(ready);
    if (condition === "accepted") {
      view.update({
        home_generation: 3,
        home: { state: "available", workspace: "starting" },
      });
      expect(screen.queryByText(/Researcher can work again/i)).toBeNull();
    }
  },
);
test("without Inspect no Space route is requested and the named guidance is shown", () => {
  show({ permissions: 1 });
  expect(fetcher).not.toHaveBeenCalled();
  expect(spaces.getSpace).not.toHaveBeenCalled();
  expect(spaces.listSpaceFiles).not.toHaveBeenCalled();
  expect(
    screen.getByText(
      "To see Researcher's files you need “Can see all work”. Ask a manager.",
    ),
  ).toBeDefined();
  expect(
    screen.getByText(
      "To give Researcher a file, attach it in a conversation with Researcher.",
    ),
  ).toBeDefined();
});
test("a reader without Space WRITE sees files and the conversation hint without write controls", async () => {
  rs.mocked(spaces.getSpace).mockResolvedValue({
    id: home,
    permissions: 17,
  } as spaces.StorageSpace);
  rs.mocked(spaces.listSpaceFiles).mockResolvedValue({
    files: [file],
    truncated: false,
  });
  show();
  await screen.findByRole("button", { name: "note.txt" });
  expect(screen.queryByRole("button", { name: "Add files" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Delete" })).toBeNull();
  expect(
    screen.getByText(
      "To give Researcher a file, attach it in a conversation with Researcher.",
    ),
  ).toBeDefined();
});
test("busy files replace the list and expose no recovery or write action", () => {
  show({ home: { state: "busy", workspace: "absent" } });
  expect(
    screen.getByText(
      "Researcher's files are being changed right now and cannot be used. Try again in a few minutes.",
    ),
  ).toBeDefined();
  expect(spaces.listSpaceFiles).not.toHaveBeenCalled();
  expect(screen.queryByRole("button", { name: "Add files" })).toBeNull();
  expect(screen.queryByText(/backup|restore|repair/i)).toBeNull();
});
test("legacy missing Space access produces the named access-save guidance", async () => {
  rs.mocked(spaces.getSpace).mockRejectedValue(
    new spaces.StorageRequestError(403, "private diagnostic"),
  );
  show();
  await screen.findByText(
    "Your access to Researcher's files has not been saved yet. Ask a manager to save your access on Researcher's Access tab.",
  );
  expect(screen.queryByText("private diagnostic")).toBeNull();
});
test("an interrupted upload is checked without publishing the bytes again", async () => {
  rs.mocked(transfers.putAgentFile).mockRejectedValueOnce(
    new TypeError("offline"),
  );
  rs.mocked(fetcher)
    .mockResolvedValueOnce(Response.json({ status: "unknown" }))
    .mockResolvedValueOnce(
      Response.json({
        status: "complete",
        file: {
          path: "note.txt",
          size: 5,
          sha256: "hash",
          deduplicated: false,
        },
      }),
    );
  show({ home: { state: "available", workspace: "starting" } });
  await choose();
  await screen.findByText("No result is recorded yet.");
  fireEvent.click(screen.getByRole("button", { name: "Check again" }));
  await screen.findByText("Saved in Researcher's files");
  const calls = rs.mocked(transfers.putAgentFile).mock.calls;
  expect(calls[0]![0]).toBe(id);
  expect(calls[0]![1]).toBe("note.txt");
  expect(calls).toHaveLength(1);
  for (const [url, options] of rs.mocked(fetcher).mock.calls) {
    expect(url).toEqual(
      expect.stringContaining(`/home/operations/${calls[0]![3]}`),
    );
    expect(options?.body).toBeUndefined();
    expect(options?.method ?? "GET").toBe("GET");
  }
  expect(calls[0]![3]).toMatch(/^[0-9a-f]{32}$/);
});
test("shows progress and cancel retires an individual transfer", async () => {
  let options: transfers.TransferOptions | undefined;
  rs.mocked(transfers.putAgentFile).mockImplementation(
    (_id, _path, _bytes, _operation, captured) => {
      options = captured;
      return new Promise(() => undefined);
    },
  );
  show();
  await choose();
  options?.onProgress?.(25);
  await screen.findByText("Uploading… 25%");
  fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
  expect(options?.signal?.aborted).toBe(true);
  expect(screen.queryByText("Uploading… 25%")).toBeNull();
});
test("a proxy's plain-text access denial retires the poll without exposing a repeat write", async () => {
  rs.mocked(transfers.putAgentFile).mockRejectedValue(
    new Error("disconnected"),
  );
  rs.mocked(fetcher).mockResolvedValue(new Response("denied", { status: 403 }));
  show();
  await choose();
  await screen.findByText("You no longer have access to check this change.");
  expect(screen.queryByRole("button", { name: "Check again" })).toBeNull();
  expect(screen.queryByRole("button", { name: "Try again" })).toBeNull();
  expect(fetcher).toHaveBeenCalledTimes(1);
  expect(transfers.putAgentFile).toHaveBeenCalledTimes(1);
});
test("refuses a file over sixty-four MiB before any write", async () => {
  const oversized = new File(["x"], "large.dat");
  Object.defineProperty(oversized, "size", { value: 64 * 1024 * 1024 + 1 });
  show();
  await choose(oversized);
  await screen.findByText(
    /large.dat is larger than .*the most one file can be/,
  );
  expect(transfers.putAgentFile).not.toHaveBeenCalled();
});
test("shows the returned collision-safe name", async () => {
  rs.mocked(transfers.putAgentFile).mockResolvedValue({
    path: "note (2).txt",
    size: 5,
    sha256: "hash",
    deduplicated: false,
  });
  show();
  await choose();
  await screen.findByText("Saved as note (2).txt");
});
test("directory rename and deletion use only audited agent routes", async () => {
  rs.mocked(spaces.listSpaceFiles).mockResolvedValue({
    files: [file],
    truncated: false,
  });
  show();
  fireEvent.click(await screen.findByRole("button", { name: "Rename" }));
  fireEvent.change(screen.getByLabelText("New name"), {
    target: { value: "renamed.txt" },
  });
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Rename" }),
  );
  await waitFor(() =>
    expect(transfers.changeAgentFile).toHaveBeenCalledWith(
      id,
      expect.objectContaining({
        action: "rename",
        path: "note.txt",
        destination: "renamed.txt",
        operation_id: expect.stringMatching(/^[0-9a-f]{32}$/),
      }),
      expect.any(Object),
    ),
  );
});
test.each(["edit", "rename"])(
  "a busy %s keeps its original ID across manual checks and never resends the change",
  async (action) => {
    rs.mocked(spaces.listSpaceFiles).mockResolvedValue({
      files: [file],
      truncated: false,
    });
    const busy = new InstanceApiError(409, "busy", "storage-busy");
    rs.mocked(transfers.putAgentFile).mockRejectedValue(busy);
    rs.mocked(transfers.changeAgentFile).mockRejectedValue(busy);
    rs.mocked(fetcher)
      .mockResolvedValueOnce(Response.json({ status: "pending" }))
      .mockResolvedValueOnce(Response.json({ status: "unknown" }));
    show();
    if (action === "edit") {
      fireEvent.click(await screen.findByRole("button", { name: "note.txt" }));
      fireEvent.change(await screen.findByLabelText("File contents"), {
        target: { value: "draft retained" },
      });
      fireEvent.click(screen.getByRole("button", { name: "Save" }));
    } else {
      fireEvent.click(await screen.findByRole("button", { name: "Rename" }));
      fireEvent.change(screen.getByLabelText("New name"), {
        target: { value: "renamed.txt" },
      });
      fireEvent.click(
        within(screen.getByRole("dialog")).getByRole("button", {
          name: "Rename",
        }),
      );
    }
    await screen.findByText("This change is still being checked.");
    fireEvent.click(screen.getByRole("button", { name: "Check again" }));
    await screen.findByText("No result is recorded yet.");
    const edits = rs.mocked(transfers.putAgentFile).mock.calls;
    const actions = rs.mocked(transfers.changeAgentFile).mock.calls;
    expect(edits.length + actions.length).toBe(1);
    const handle = edits[0]?.[3] ?? actions[0]?.[1].operation_id;
    expect(fetcher).toHaveBeenCalledTimes(2);
    for (const [url, options] of rs.mocked(fetcher).mock.calls) {
      expect(url).toEqual(
        expect.stringContaining(`/home/operations/${handle}`),
      );
      expect(options?.method ?? "GET").toBe("GET");
      expect(options?.body).toBeUndefined();
    }
  },
);
test("text editing sends the loaded revision and keeps the draft on a changed-file refusal", async () => {
  rs.mocked(spaces.listSpaceFiles).mockResolvedValue({
    files: [file],
    truncated: false,
  });
  rs.mocked(transfers.putAgentFile).mockRejectedValue(
    new InstanceApiError(409, "private", "file-changed"),
  );
  show();
  fireEvent.click(await screen.findByRole("button", { name: "note.txt" }));
  fireEvent.change(await screen.findByLabelText("File contents"), {
    target: { value: "my draft" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText(
    "This file changed after you opened it. Reload it; your text stays here.",
  );
  expect(
    screen.getByLabelText<HTMLTextAreaElement>("File contents").value,
  ).toBe("my draft");
  expect(
    rs.mocked(transfers.putAgentFile).mock.calls[0]![4]?.expectedSha256,
  ).toBe("old-revision");
});
test("the root prioritizes uploads and outputs, hides tool packages, and reports their shared empty state", async () => {
  const folder = (name: string): spaces.SpaceFile => ({
    ...file,
    name,
    path: name,
    kind: "directory",
    size: 0,
  });
  rs.mocked(spaces.listSpaceFiles).mockImplementation(async (_id, path) => ({
    files: path
      ? []
      : [folder("outputs"), folder(".agent-skill-packages"), folder("uploads")],
    truncated: false,
  }));
  show();
  await screen.findByText(
    "Nothing here yet. Drop files to give them to Researcher.",
  );
  const names = screen
    .getAllByRole("button")
    .map((button) => button.textContent);
  expect(names.indexOf("uploads")).toBeLessThan(names.indexOf("outputs"));
  expect(screen.queryByText(".agent-skill-packages")).toBeNull();
  expect(screen.queryByText(/capacity|room left|Space|Home|MiB/)).toBeNull();
});

test.each([
  {
    reason: "files-full",
    status: 507,
    text: "Researcher's files are full. Delete files on Researcher's Files tab, or ask Provider for more room.",
  },
  { reason: "invalid-name", status: 400, text: "That name cannot be used." },
  {
    reason: "reserved-path",
    status: 409,
    text: "That folder is kept for Researcher's own tools.",
  },
  {
    reason: "name-collision",
    status: 409,
    text: "Something with that name already exists here.",
  },
  {
    reason: "not-found",
    status: 404,
    text: "That file or folder is no longer there. Refresh the list.",
  },
  {
    reason: "agent-not-active",
    status: 409,
    text: "Researcher is paused or retired, so Researcher's files cannot be changed.",
  },
  {
    reason: "file-write-failed",
    status: 503,
    text: "note.txt could not be saved. Try again.",
  },
  {
    reason: "unknown-reason",
    status: 409,
    text: "That did not work. Refresh the page, then try again.",
  },
  {
    reason: "execution-unavailable",
    status: 409,
    text: "AI assistants and AI employees are paused by Provider.",
  },
])(
  "an upload refusal $reason uses localized copy without diagnostics",
  async (row) => {
    rs.mocked(transfers.putAgentFile).mockRejectedValue(
      new InstanceApiError(
        row.status,
        "private diagnostic",
        row.reason,
        "operator-hold",
      ),
    );
    show();
    await choose();
    await screen.findByText(row.text);
    expect(screen.queryByText("private diagnostic")).toBeNull();
    expect(screen.queryByRole("button", { name: "Try again" }) !== null).toBe(
      row.reason === "file-write-failed",
    );
    expect(screen.getByRole("button", { name: "Remove" })).toBeDefined();
  },
);
test("reloading a conflicted edit preserves the draft and uses the newly read revision", async () => {
  rs.mocked(spaces.listSpaceFiles).mockResolvedValue({
    files: [file],
    truncated: false,
  });
  rs.mocked(transfers.putAgentFile).mockRejectedValueOnce(
    new InstanceApiError(409, "private", "file-changed"),
  );
  show();
  fireEvent.click(await screen.findByRole("button", { name: "note.txt" }));
  fireEvent.change(await screen.findByLabelText("File contents"), {
    target: { value: "my draft" },
  });
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await screen.findByText(
    "This file changed after you opened it. Reload it; your text stays here.",
  );
  rs.mocked(spaces.readSpaceText).mockResolvedValue({
    text: "someone else's version",
    sha256: "new-revision",
    concurrency: "sha256",
  });
  fireEvent.click(
    within(screen.getByRole("dialog")).getByRole("button", { name: "Refresh" }),
  );
  await waitFor(() => expect(spaces.readSpaceText).toHaveBeenCalledTimes(2));
  expect(
    screen.getByLabelText<HTMLTextAreaElement>("File contents").value,
  ).toBe("my draft");
  fireEvent.click(screen.getByRole("button", { name: "Save" }));
  await waitFor(() => expect(transfers.putAgentFile).toHaveBeenCalledTimes(2));
  expect(
    rs.mocked(transfers.putAgentFile).mock.calls[1]![4]?.expectedSha256,
  ).toBe("new-revision");
});

test("the actual nested busy envelope replaces the list and recovers through read polling", async () => {
  const actual = rs.requireActual<typeof spaces>("@/core/spaces/api");
  rs.mocked(spaces.listSpaceFiles).mockImplementation(actual.listSpaceFiles);
  rs.mocked(fetcher)
    .mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          detail: {
            code: "storage-busy",
            cause: "pending-recovery",
            message: "private storage diagnostic",
          },
        }),
        { status: 409, headers: { "Content-Type": "application/json" } },
      ),
    )
    .mockResolvedValue(
      new Response(JSON.stringify({ files: [file], truncated: false }), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      }),
    );
  show();
  await screen.findByText(
    "Researcher's files are being changed right now and cannot be used. Try again in a few minutes.",
  );
  expect(screen.queryByText("private storage diagnostic")).toBeNull();
  expect(screen.queryByRole("button", { name: "Add files" })).toBeNull();
  await screen.findByRole("button", { name: "note.txt" }, { timeout: 4500 });
  expect(fetcher).toHaveBeenCalledTimes(2);
});
test("an actual non-UTF8 preview response gives fixed download guidance", async () => {
  const actual = rs.requireActual<typeof spaces>("@/core/spaces/api");
  rs.mocked(spaces.readSpaceText).mockImplementation(actual.readSpaceText);
  rs.mocked(spaces.listSpaceFiles).mockResolvedValue({
    files: [file],
    truncated: false,
  });
  rs.mocked(fetcher).mockResolvedValue(
    new Response(JSON.stringify({ detail: "private decoder traceback" }), {
      status: 415,
      headers: { "Content-Type": "application/json" },
    }),
  );
  show();
  fireEvent.click(await screen.findByRole("button", { name: "note.txt" }));
  await screen.findByText(
    "Binary and unsupported text files can be downloaded.",
  );
  expect(screen.queryByText("private decoder traceback")).toBeNull();
  expect(screen.queryByLabelText("File contents")).toBeNull();
});

test("a conversation attachment link opens the uploads folder directly", async () => {
  show({}, "uploads");
  await waitFor(() => expect(spaces.listSpaceFiles).toHaveBeenCalled());
  expect(rs.mocked(spaces.listSpaceFiles).mock.calls[0]![1]).toBe("uploads");
});
